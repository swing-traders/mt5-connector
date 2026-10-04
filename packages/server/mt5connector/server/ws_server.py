"""The push hub: each symbol's EA's frames fanned out to their subscribers in true UTC, its relays
to the HTTP server, and the charts the spawner opens and closes for the symbols in use.

State: the connections that said hello and their roles; each symbol's publishing EA and the wanted
frame last told it; the spawner; the chart requests held for symbols no EA publishes, and those the
spawner was sent; the spawner's reason for each chart it failed to open, standing until that chart
opens or its symbol's EA says hello; when each symbol was last in use, and the symbols whose charts
the spawner was told to close; each consumer's subscriptions and its queue of frames not yet sent;
and the broker hour of the last repeated-hour warning. Nothing is persisted, and nothing is replayed
to a consumer that reconnects."""

import asyncio
import http.client
import json
import logging
import math
import os
import time
import urllib.request
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import timedelta
from http import HTTPMethod, HTTPStatus
from urllib.parse import quote, unquote

from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed
from websockets.frames import CloseCode
from websockets.http11 import Request, Response

from mt5connector.server.push_frames import Published, hold, published
from mt5connector.server.settings import HubSettings, read_hub_settings
from mt5connector.server.wire.broker_clock import BrokerClock
from mt5connector.server.wire.history_wire import BAR_TIMEFRAME
from mt5connector.server.wire.push_wire import (
    CHARTS_PATH,
    CLOSE_TIMEOUT_S,
    MISSED_PONGS,
    PING_INTERVAL_S,
    PROTOCOL_VERSION,
    ChartState,
    FrameType,
    Role,
    Stream,
    Subscription,
    subscription_of,
)

SERVER_TIME_RELAY_PATH = "/relay/server_time"
COMMISSIONS_RELAY_PATH = "/relay/commissions"
# The most frames a consumer's queue holds; the next one closes it with the overflow code.
QUEUE_BOUND = 10_000
OVERFLOW = 4008
_RELAY_TIMEOUT_S = 5
_IDLE_SWEEP_S = 5
_LOOPBACK = "127.0.0.1"
_PUBLISHED = frozenset({FrameType.TICK, FrameType.BAR, FrameType.TRADE_TRANSACTION})
_RELAYED = frozenset({FrameType.SERVER_TIME, FrameType.COMMISSIONS})
_OPS = frozenset({FrameType.SUBSCRIBE, FrameType.UNSUBSCRIBE})
_CHART_ANSWERS = frozenset({FrameType.CHART_OPENED, FrameType.CHART_FAILED, FrameType.CHART_KEPT})
_EA_HELLO = ("v", "type", "role", "symbol", "spawner")
_CHART_OPENED = ("v", "type", "symbol")
_CHART_REASON = ("v", "type", "symbol", "reason")
_ADAPTER_HELLO = ("v", "type", "role")
_HOUR_S = 3_600

logger = logging.getLogger(__name__)


@dataclass
class _Consumer:
    """A consumer of the hub."""

    queue: asyncio.Queue
    sender: asyncio.Task
    subscriptions: set[Subscription] = field(default_factory=set)


@dataclass(frozen=True)
class _Ea:
    """An EA that said hello: its chart's symbol, and whether it opens the charts the hub
    requests."""

    symbol: str
    spawner: bool


class Hub:
    """Routes each symbol's EA's frames to the consumers subscribed to them, and has the spawner
    keep a chart open for each symbol in use."""

    def __init__(self, clock: BrokerClock, relay: Callable[[str, dict], None], *, idle_s: int):
        self._clock = clock
        self._relay = relay
        self._idle_s = idle_s
        self._roles: dict[object, Role] = {}
        self._eas: dict[object, _Ea] = {}
        self._publishers: dict[str, object] = {}
        self._told: dict[str, dict] = {}
        self._spawner: object | None = None
        self._requested: set[str] = set()
        self._sent: set[str] = set()
        self._failed: dict[str, str] = {}
        self._used: dict[str, float] = {}
        self._close_sent: set[str] = set()
        self._consumers: dict[object, _Consumer] = {}
        self._warned_hour: int | None = None
        self._closing: set[asyncio.Task] = set()

    async def handler(self, ws) -> None:
        """Serves one connection until it closes; a frame the hub cannot handle is answered with an
        error frame and closes it."""
        try:
            async for raw in ws:
                await self._on_frame(ws, raw)
        except ConnectionClosed as closed:
            logger.info("connection %s closed: %s", ws.remote_address, closed)
        except Exception as failure:
            logger.warning(
                "closing connection %s on a frame the hub cannot handle: %s",
                ws.remote_address,
                failure,
            )
            await self._refuse(ws, str(failure))
        finally:
            await self._drop(ws)

    async def process_request(self, connection, request: Request) -> Response | None:
        """Answers the chart route — a POST from loopback naming a symbol in its path — over plain
        HTTP; any other request goes on to the WebSocket handshake."""
        prefix = f"{CHARTS_PATH}/"
        if not request.path.startswith(prefix):
            return None
        raw = request.path.removeprefix(prefix)
        if connection.remote_address[0] != _LOOPBACK:
            refusal = (HTTPStatus.FORBIDDEN, f"{connection.remote_address[0]} is not loopback")
        elif request.method != HTTPMethod.POST:
            refusal = (HTTPStatus.METHOD_NOT_ALLOWED, f"{request.method} is not POST")
        elif raw == "" or "/" in raw or "?" in raw:
            refusal = (HTTPStatus.NOT_FOUND, f"{request.path} names no symbol")
        else:
            refusal = None
        if refusal is None:
            symbol = unquote(raw)
            state = await self.use_chart(symbol)
            body = {"ok": True, "result": state}
            if state == ChartState.FAILED:
                body["reason"] = self._failed[symbol]
            return _json(connection, HTTPStatus.OK, body)
        else:
            status, message = refusal
            return _json(connection, status, {"ok": False, "error": message})

    async def use_chart(self, symbol: str) -> ChartState:
        """Records a use of the symbol, and requests its chart while no EA publishes it and the
        spawner has not failed to open it."""
        self._used[symbol] = time.monotonic()
        if symbol in self._publishers:
            return ChartState.PUBLISHED
        elif symbol in self._failed:
            return ChartState.FAILED
        else:
            await self._request_chart(symbol)
            return ChartState.REQUESTED

    async def close_idle_charts(self) -> None:
        """Tells the spawner, which opened them, to close the chart of each symbol out of use for
        the idle period, once per spawner connection; the spawner's own chart never closes."""
        if self._spawner is None:
            return
        now = time.monotonic()
        for symbol, ws in sorted(self._publishers.items()):
            idle = not self._in_use(symbol, now)
            if idle and ws is not self._spawner and symbol not in self._close_sent:
                self._close_sent.add(symbol)
                logger.info("%s out of use for %d s: closing its chart", symbol, self._idle_s)
                await self._send(self._spawner, _addressed(FrameType.CLOSE_CHART, symbol))

    async def _on_frame(self, ws, raw) -> None:
        frame = json.loads(raw)
        if not isinstance(frame, dict):
            raise ValueError(f"not a JSON object: {frame!r}")
        elif not (_is_integer(frame.get("v")) and frame["v"] == PROTOCOL_VERSION):
            raise ValueError(f"unsupported frame version: {frame.get('v')!r}")
        try:
            kind = FrameType(frame.get("type"))
        except ValueError:
            logger.info("unknown kind %r", frame.get("type"))
        else:
            await self._on_kind(ws, kind, frame)

    async def _on_kind(self, ws, kind: FrameType, frame: dict) -> None:
        role = self._roles.get(ws)
        if kind == FrameType.HELLO:
            await self._hello(ws, role, frame)
        elif kind in _OPS and role == Role.ADAPTER:
            await self._op(ws, kind, frame)
        elif kind in _PUBLISHED and role == Role.EA:
            await self._publish(ws, frame)
        elif kind in _RELAYED and role == Role.EA:
            self._relay_frame(ws, kind, frame)
        elif kind in _CHART_ANSWERS and role == Role.EA:
            self._chart_answer(ws, kind, frame)
        elif role is None:
            raise ValueError(f"a {kind} frame before the connection's hello")
        else:
            raise ValueError(f"a {kind} frame from an {role}")

    async def _hello(self, ws, role: Role | None, frame: dict) -> None:
        if role is not None:
            raise ValueError(f"a second hello from an {role}")
        declared = Role(frame.get("role"))
        if declared == Role.EA:
            hold(frame, _EA_HELLO, FrameType.HELLO)
            if not (isinstance(frame["symbol"], str) and frame["symbol"]):
                raise ValueError(f"{FrameType.HELLO}: not a symbol: {frame['symbol']!r}")
            elif not isinstance(frame["spawner"], bool):
                raise ValueError(
                    f"{FrameType.HELLO}: spawner is not a boolean: {frame['spawner']!r}"
                )
            await self._ea_hello(ws, _Ea(frame["symbol"], frame["spawner"]))
        else:
            hold(frame, _ADAPTER_HELLO, FrameType.HELLO)
            self._roles[ws] = declared
            logger.info("%s connected: %s", declared, ws.remote_address)
            queue = asyncio.Queue(maxsize=QUEUE_BOUND)
            sender = asyncio.create_task(self._send_queued(ws, queue))
            self._consumers[ws] = _Consumer(queue, sender)

    async def _ea_hello(self, ws, ea: _Ea) -> None:
        """Takes an EA as its symbol's publisher, and as the spawner when it says it is one; an EA
        for a symbol another EA publishes is told it is a duplicate and closed."""
        self._roles[ws] = Role.EA
        self._eas[ws] = ea
        publisher = self._publishers.get(ea.symbol)
        if publisher is not None:
            logger.warning(
                "refusing a second EA for %s from %s: %s publishes it",
                ea.symbol,
                ws.remote_address,
                publisher.remote_address,
            )
            await self._send(ws, _addressed(FrameType.DUPLICATE, ea.symbol))
            await ws.close(CloseCode.NORMAL_CLOSURE, "duplicate")
        else:
            logger.info("EA for %s connected: %s", ea.symbol, ws.remote_address)
            self._publishers[ea.symbol] = ws
            self._used[ea.symbol] = time.monotonic()
            self._requested.discard(ea.symbol)
            self._sent.discard(ea.symbol)
            self._failed.pop(ea.symbol, None)
            self._told[ea.symbol] = self._wanted(ea.symbol)
            await self._send(ws, self._told[ea.symbol])
            if ea.spawner:
                if self._spawner is not None:
                    logger.warning(
                        "the EA for %s replaces the EA for %s as the spawner",
                        ea.symbol,
                        self._eas[self._spawner].symbol,
                    )
                self._spawner = ws
                self._sent.clear()
                self._close_sent.clear()
                for symbol in sorted(self._requested):
                    await self._request_chart(symbol)

    async def _op(self, ws, kind: FrameType, frame: dict) -> None:
        """Applies a consumer's subscribe or unsubscribe and queues its ack ahead of any frame the
        change routes to it; a subscription to a symbol no EA publishes requests its chart."""
        if not _is_integer(frame.get("id")):
            raise ValueError(f"not an op id: {frame.get('id')!r}")
        subscription = subscription_of(frame)
        # None while the hub closes it for overflow.
        consumer = self._consumers.get(ws)
        if consumer is not None:
            if kind == FrameType.SUBSCRIBE:
                consumer.subscriptions.add(subscription)
            else:
                consumer.subscriptions.discard(subscription)
            self._mark_used([subscription])
            ack = {"v": PROTOCOL_VERSION, "type": FrameType.ACK, "id": frame["id"]}
            self._enqueue(ws, consumer, json.dumps(ack))
            await self._tell_publishers()
            if (
                kind == FrameType.SUBSCRIBE
                and subscription.symbol is not None
                and subscription.symbol not in self._publishers
            ):
                await self._request_chart(subscription.symbol)

    async def _publish(self, ws, frame: dict) -> None:
        """Fans a frame out to the consumers subscribed to it when the EA may send it: one of its
        own symbol, or, from the current spawner, one of no symbol. A refused duplicate's frame is
        dropped silently."""
        ea = self._eas[ws]
        if self._publishers.get(ea.symbol) is ws:
            passed = published(frame, self._clock)
            if passed.symbol == ea.symbol or (passed.symbol == "" and ws is self._spawner):
                await self._fan_out(passed)
            else:
                logger.warning(
                    "dropping a %s frame for %r from the EA for %s",
                    frame["type"],
                    passed.symbol,
                    ea.symbol,
                )

    async def _fan_out(self, passed: Published) -> None:
        if passed.ambiguous is not None:
            self._warn_ambiguous(*passed.ambiguous)
        raw = json.dumps(passed.frame)
        overflowed = False
        for ws, consumer in list(self._consumers.items()):
            if passed.subscription in consumer.subscriptions:
                overflowed |= not self._enqueue(ws, consumer, raw)
        if overflowed:
            await self._tell_publishers()

    def _relay_frame(self, ws, kind: FrameType, frame: dict) -> None:
        """Hands the server the EA's commission schedule, or a server-time sample naming its own
        symbol, off the loop and not awaited, so a slow server never holds back the stream."""
        ea = self._eas[ws]
        if self._publishers.get(ea.symbol) is ws:
            loop = asyncio.get_running_loop()
            if kind == FrameType.COMMISSIONS:
                # Posted under the EA's symbol, so the server records a refused frame against it.
                path = f"{COMMISSIONS_RELAY_PATH}/{quote(ea.symbol, safe='')}"
                loop.run_in_executor(None, self._relay, path, frame)
            elif frame.get("symbol") == ea.symbol:
                loop.run_in_executor(None, self._relay, SERVER_TIME_RELAY_PATH, frame)
            else:
                logger.warning(
                    "dropping a %s frame for %r from the EA for %s",
                    kind,
                    frame.get("symbol"),
                    ea.symbol,
                )

    def _chart_answer(self, ws, kind: FrameType, frame: dict) -> None:
        """Takes the spawner's answer to an open_chart or a close_chart: records or clears the
        failure to open a chart, or counts a chart it kept open in use for another idle period. A
        refused duplicate's answer is dropped silently."""
        if kind == FrameType.CHART_OPENED:
            hold(frame, _CHART_OPENED, kind)
        else:
            hold(frame, _CHART_REASON, kind)
            if not isinstance(frame["reason"], str):
                raise ValueError(f"{kind}: reason is not a string: {frame['reason']!r}")
        symbol = frame["symbol"]
        if not (isinstance(symbol, str) and symbol):
            raise ValueError(f"{kind}: not a symbol: {symbol!r}")
        ea = self._eas[ws]
        if self._publishers.get(ea.symbol) is ws:
            if ws is not self._spawner:
                logger.warning(
                    "dropping a %s frame for %r from the EA for %s", kind, symbol, ea.symbol
                )
            elif kind == FrameType.CHART_OPENED:
                logger.info("the spawner opened a chart of %s", symbol)
                self._failed.pop(symbol, None)
            elif kind == FrameType.CHART_FAILED:
                logger.warning("the spawner cannot open a chart of %s: %s", symbol, frame["reason"])
                self._failed[symbol] = frame["reason"]
            else:
                logger.info("the spawner keeps the chart of %s open: %s", symbol, frame["reason"])
                self._used[symbol] = time.monotonic()
                self._close_sent.discard(symbol)

    def _enqueue(self, ws, consumer: _Consumer, raw: str) -> bool:
        """Queues a frame for a consumer; whether its queue had room. A consumer whose queue is full
        is closed with the overflow code."""
        try:
            consumer.queue.put_nowait(raw)
        except asyncio.QueueFull:
            logger.warning(
                "closing consumer %s: %d frames queued and not sent", ws.remote_address, QUEUE_BOUND
            )
            del self._consumers[ws]
            consumer.sender.cancel()
            self._mark_used(consumer.subscriptions)
            # Not awaited: the close waits on a peer that is not reading.
            closing = asyncio.create_task(ws.close(OVERFLOW, "queue overflow"))
            self._closing.add(closing)
            closing.add_done_callback(self._closing.discard)
            return False
        else:
            return True

    async def _send_queued(self, ws, queue: asyncio.Queue) -> None:
        """Sends a consumer the frames queued for it, in the order they were queued."""
        try:
            while True:
                await ws.send(await queue.get())
        except ConnectionClosed:
            # The consumer's handler logs the close and drops it.
            pass

    async def _tell_publishers(self) -> None:
        """Tells each symbol's EA its symbol's wanted frame when it differs from the one last
        told."""
        for symbol, ws in list(self._publishers.items()):
            wanted = self._wanted(symbol)
            if wanted != self._told[symbol]:
                self._told[symbol] = wanted
                await self._send(ws, wanted)

    def _wanted(self, symbol: str) -> dict[str, object]:
        """The wanted frame of a symbol: whether some consumer subscribes to its ticks, and the
        timeframes whose bars some consumer subscribes to."""
        ticks = False
        timeframes = set()
        for consumer in self._consumers.values():
            for item in consumer.subscriptions:
                if item.symbol == symbol and item.stream == Stream.TICKS:
                    ticks = True
                elif item.symbol == symbol and item.stream == Stream.BARS:
                    timeframes.add(BAR_TIMEFRAME[item.timeframe])
        return _addressed(FrameType.WANTED, symbol) | {
            "ticks": ticks,
            "timeframes": sorted(timeframes),
        }

    async def _request_chart(self, symbol: str) -> None:
        """Holds a chart request for a symbol no EA publishes until its EA says hello, sending it to
        the spawner once per spawner connection."""
        self._requested.add(symbol)
        if self._spawner is not None and symbol not in self._sent:
            self._sent.add(symbol)
            await self._send(self._spawner, _addressed(FrameType.OPEN_CHART, symbol))

    def _in_use(self, symbol: str, now: float) -> bool:
        """Whether a consumer subscribes to the symbol, or it was used within the idle period."""
        subscribed = any(
            item.symbol == symbol
            for consumer in self._consumers.values()
            for item in consumer.subscriptions
        )
        return subscribed or now - self._used.get(symbol, -math.inf) < self._idle_s

    def _mark_used(self, subscriptions: Iterable[Subscription]) -> None:
        now = time.monotonic()
        for item in subscriptions:
            if item.symbol is not None:
                self._used[item.symbol] = now

    def _warn_ambiguous(self, field: str, broker_epoch: int) -> None:
        """Warns once per repeated broker hour that its epochs read as their first occurrence."""
        hour = broker_epoch // _HOUR_S
        if hour != self._warned_hour:
            self._warned_hour = hour
            logger.warning(
                "%s %d is in the broker's repeated hour, read as its first occurrence",
                field,
                broker_epoch,
            )

    async def _send(self, ws, frame: dict) -> None:
        try:
            await ws.send(json.dumps(frame))
        except ConnectionClosed as closed:
            logger.info(
                "connection %s closed before a %s frame: %s",
                ws.remote_address,
                frame["type"],
                closed,
            )

    async def _refuse(self, ws, message: str) -> None:
        """Answers a frame the hub cannot handle with an error frame, then closes the connection."""
        await self._drop(ws)
        await self._send(ws, {"v": PROTOCOL_VERSION, "type": FrameType.ERROR, "message": message})
        await ws.close(CloseCode.PROTOCOL_ERROR)

    async def _drop(self, ws) -> None:
        """Forgets a connection. A consumer's subscriptions leave the wanted frames with it; a
        publishing EA leaves its symbol unpublished, and a symbol still in use has its chart
        requested again."""
        self._roles.pop(ws, None)
        consumer = self._consumers.pop(ws, None)
        ea = self._eas.pop(ws, None)
        if consumer is not None:
            consumer.sender.cancel()
            self._mark_used(consumer.subscriptions)
            await self._tell_publishers()
        elif ea is not None and self._publishers.get(ea.symbol) is ws:
            logger.info("EA for %s disconnected: %s", ea.symbol, ws.remote_address)
            del self._publishers[ea.symbol]
            del self._told[ea.symbol]
            self._close_sent.discard(ea.symbol)
            if ws is self._spawner:
                self._spawner = None
                self._sent.clear()
                self._close_sent.clear()
            if self._in_use(ea.symbol, time.monotonic()):
                await self._request_chart(ea.symbol)


def serve_hub(hub: Hub, host: str, port: int):
    """The hub served on `host`:`port` with its chart route, pinging every peer at the protocol's
    interval with one ping outstanding, failing a connection whose ping goes the ping timeout
    without its pong, and dropping it within the close timeout; the ping and the close frame wait
    on the hub's writes to the peer draining."""
    return serve(
        hub.handler,
        host,
        port,
        process_request=hub.process_request,
        ping_interval=PING_INTERVAL_S,
        ping_timeout=PING_INTERVAL_S * MISSED_PONGS,
        close_timeout=CLOSE_TIMEOUT_S,
    )


def post_to(base_url: str) -> Callable[[str, dict], None]:
    """A relay that POSTs each frame as JSON to the route at its path under `base_url`, and logs a
    failed post as a warning."""
    # No proxy: the server refuses a relayed frame that does not arrive over loopback.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def post(path: str, frame: dict) -> None:
        url = f"{base_url}{path}"
        request = urllib.request.Request(
            url,
            data=json.dumps(frame).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with opener.open(request, timeout=_RELAY_TIMEOUT_S):
                pass
        except (OSError, http.client.HTTPException) as failure:
            logger.warning("relay to %s failed: %s", url, failure)

    return post


async def _serve(settings: HubSettings) -> None:
    clock = BrokerClock(settings.broker_tz, timedelta(hours=settings.broker_offset_hours))
    relay = post_to(f"http://127.0.0.1:{settings.api_port}")
    hub = Hub(clock, relay, idle_s=settings.chart_idle_seconds)
    async with serve_hub(hub, "0.0.0.0", settings.hub_port):
        while True:
            await asyncio.sleep(_IDLE_SWEEP_S)
            await hub.close_idle_charts()


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    settings = read_hub_settings(os.environ)
    logger.info(
        "push hub on port %d, broker clock %s %+d h, charts closed after %d s out of use",
        settings.hub_port,
        settings.broker_tz.key,
        settings.broker_offset_hours,
        settings.chart_idle_seconds,
    )
    asyncio.run(_serve(settings))


def _addressed(kind: FrameType, symbol: str) -> dict[str, object]:
    return {"v": PROTOCOL_VERSION, "type": kind, "symbol": symbol}


def _json(connection, status: HTTPStatus, body: dict[str, object]) -> Response:
    response = connection.respond(status, json.dumps(body))
    del response.headers["Content-Type"]
    response.headers["Content-Type"] = "application/json"
    return response


def _is_integer(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


if __name__ == "__main__":
    main()
