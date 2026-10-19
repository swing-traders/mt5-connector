"""The hub, the one endpoint the EAs and the consumers connect to: it fans the EAs' frames out in
true UTC, routes or answers every request, and has the spawner open and close the charts in use.

State: the connections that said hello — each consumer's subscriptions, each EA's chart role and
symbol — and each one's queue of frames not yet sent; the EA serving each role and symbol, and the
wanted frame last told each publisher; the spawner; the chart requests held for a role and symbol no
EA serves, and those the spawner was sent; the MQL5 error the spawner met on each chart it failed to
open, standing until that chart opens or its EA says hello; when each chart was last in use, and
when the spawner was last told to close each chart whose close is pending, which outlives the
chart's EA; the true-UTC open of the last bar fanned out of each series; the latest commission
schedule and trading sessions relayed for each symbol; the floors learned from ranges answers; the
pending requests and the clock gate. Nothing is persisted, and nothing is replayed to a consumer
that reconnects."""

import asyncio
import dataclasses
import json
import logging
import math
import os
import time
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import timedelta
from http import HTTPMethod, HTTPStatus

from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed
from websockets.frames import CloseCode
from websockets.http11 import Request, Response

from mt5connector.server.commissions import (
    CommissionStore,
    RelayRefusal,
    commission_schedule,
    commissions_refusal,
)
from mt5connector.server.floors import FloorStore
from mt5connector.server.hub_gate import ClockGate
from mt5connector.server.hub_health import health
from mt5connector.server.hub_requests import Pending, Requests
from mt5connector.server.hub_utc import (
    WINDOWS,
    Conversion,
    Results,
    broker_arguments,
    published,
    window_refusal,
)
from mt5connector.server.repeated_hours import RepeatedHours
from mt5connector.server.sessions import SessionStore, sessions_refusal, trading_sessions
from mt5connector.server.settings import HubSettings, read_hub_settings
from mt5connector.server.wire import binary, calls, mirror
from mt5connector.server.wire.binary import ErrorKind
from mt5connector.server.wire.broker_clock import BrokerClock
from mt5connector.server.wire.history_wire import BAR_TIMEFRAME, Series, bar_series
from mt5connector.server.wire.push_wire import (
    CLOSE_TIMEOUT_S,
    HEALTH_PATH,
    PING_INTERVAL_S,
    PING_TIMEOUT_INTERVALS,
    ChartRole,
    ChartState,
    Stream,
    Subscription,
)

OVERFLOW = 4008
_IDLE_SWEEP_S = 5
_CLOSE_BOUND_S = 30
_HOUR_S = 3_600

logger = logging.getLogger(__name__)

ChartKey = tuple[ChartRole, str]


@dataclass
class _Outbound:
    """A connection's frames not yet sent, and the task sending them in the order queued."""

    queue: asyncio.Queue
    sender: asyncio.Task


@dataclass
class _Consumer:
    outbound: _Outbound
    subscriptions: set[Subscription] = field(default_factory=set)


@dataclass
class _Ea:
    chart_role: ChartRole
    symbol: str
    outbound: _Outbound

    @property
    def key(self) -> tuple[ChartRole, str | None]:
        """What the EA serves: its chart's role, and its symbol but for the spawner, which serves
        the account."""
        if self.chart_role is ChartRole.SPAWNER:
            return ChartRole.SPAWNER, None
        else:
            return self.chart_role, self.symbol


class Hub:
    """Routes each publisher's frames to their subscribers and each request to the EA serving it,
    and has the spawner keep a chart open for each role and symbol in use."""

    def __init__(self, clock: BrokerClock, settings: HubSettings) -> None:
        self._clock = clock
        self._settings = settings
        self._gate = ClockGate(
            clock,
            max_age_s=settings.clock_sample_max_age_seconds,
            check_s=settings.clock_check_seconds,
            bootstrap_s=settings.clock_bootstrap_seconds,
        )
        self._requests = Requests(expire=self._expired)
        self._consumers: dict[object, _Consumer] = {}
        self._eas: dict[object, _Ea] = {}
        self._served: dict[tuple[ChartRole, str | None], object] = {}
        self._told: dict[str, binary.Wanted] = {}
        self._spawner: object | None = None
        self._requested: set[ChartKey] = set()
        self._sent: set[ChartKey] = set()
        self._failed: dict[ChartKey, int] = {}
        self._used: dict[ChartKey, float] = {}
        self._close_sent: dict[ChartKey, float] = {}
        self._bar_opens: dict[Subscription, int] = {}
        self._commissions = CommissionStore()
        self._sessions = SessionStore()
        self._floors = FloorStore()
        self._repeated_hours = RepeatedHours()
        self._closing: set[asyncio.Task] = set()

    def start(self) -> None:
        """Arms the clock gate's bootstrap window."""
        self._gate.start()

    async def handler(self, ws) -> None:
        """Serves one connection until it closes; a frame the hub cannot handle is answered with a
        refusal and closes it."""
        keepalive = asyncio.create_task(self._keepalive(ws))
        try:
            async for raw in ws:
                self._on_frame(ws, raw)
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
            keepalive.cancel()
            self._drop(ws)

    async def process_request(self, connection, request: Request) -> Response | None:
        """Answers /health over plain HTTP; any other request goes on to the WebSocket handshake."""
        if request.path == HEALTH_PATH and request.method == HTTPMethod.GET:
            labels = {ws: (ea.chart_role.value, ea.symbol) for ws, ea in self._eas.items()}
            status, body = health(
                self._gate, self._spawner is not None, self._requests.all(), labels
            )
            return _json(connection, status, body)
        elif request.path == HEALTH_PATH:
            message = f"{request.method} is not GET"
            return _json(connection, HTTPStatus.METHOD_NOT_ALLOWED, {"ok": False, "error": message})
        else:
            return None

    def use_chart(self, key: ChartKey) -> ChartState:
        """Records a use of the chart of a role and symbol, and requests it while no EA serves it
        and the spawner has not failed to open it."""
        self._used[key] = time.monotonic()
        if key in self._served:
            return ChartState.PUBLISHED
        elif key in self._failed:
            return ChartState.FAILED
        else:
            self._request_chart(key)
            return ChartState.REQUESTED

    def close_idle_charts(self) -> None:
        """Tells the spawner to close each chart out of use for the idle period, and settles each
        close pending for the bound: sent again while an EA serves the chart out of use, else ended.
        The spawner's own chart never closes."""
        now = time.monotonic()
        # The spawner is read per key: a close that overflows its queue drops it mid-sweep.
        for key, sent in list(self._close_sent.items()):
            if self._spawner is not None and now - sent >= _CLOSE_BOUND_S:
                if key not in self._served or self._in_use(key, now):
                    del self._close_sent[key]
                else:
                    chart_role, symbol = key
                    logger.warning(
                        "the %s chart of %s neither closed nor was kept within %d s: closing it "
                        "again",
                        chart_role,
                        symbol,
                        _CLOSE_BOUND_S,
                    )
                    self._close_chart(key, now)
        for key in sorted(self._served, key=str):
            chart_role, symbol = key
            if (
                self._spawner is not None
                and chart_role is not ChartRole.SPAWNER
                and key not in self._close_sent
                and not self._in_use(key, now)
            ):
                logger.info(
                    "the %s chart of %s out of use for %d s: closing it",
                    chart_role,
                    symbol,
                    self._settings.chart_idle_seconds,
                )
                self._close_chart(key, now)

    # ── Frames ────────────────────────────────────────────────────────────────

    def _on_frame(self, ws, raw: str | bytes) -> None:
        if isinstance(raw, str):
            raise ValueError("a text frame")
        elif ws in self._eas:
            self._decoded(ws, raw, binary.Sender.EA)
        elif ws in self._consumers:
            self._decoded(ws, raw, binary.Sender.ADAPTER)
        else:
            self._hello(ws, binary.decode_hello(raw))

    def _decoded(self, ws, raw: bytes, sender: binary.Sender) -> None:
        try:
            frame = binary.decode(raw, sender)
        except binary.UnknownKind as unknown:
            logger.info("skipping a frame from %s: %s", ws.remote_address, unknown)
        else:
            if sender is binary.Sender.EA:
                self._on_ea_frame(ws, self._eas[ws], frame)
            else:
                self._on_consumer_frame(ws, self._consumers[ws], frame)

    def _hello(self, ws, hello: binary.AdapterHello | binary.EaHello) -> None:
        if isinstance(hello, binary.EaHello):
            ea = _Ea(hello.chart_role, hello.symbol, self._outbound(ws))
            self._eas[ws] = ea
            self._ea_hello(ws, ea)
        else:
            logger.info("consumer connected: %s", ws.remote_address)
            self._consumers[ws] = _Consumer(self._outbound(ws))

    def _ea_hello(self, ws, ea: _Ea) -> None:
        """Takes an EA as the one serving its role and symbol, and as the spawner when its role is;
        an EA for a role and symbol another EA serves is told it is a duplicate and closed."""
        holder = self._served.get(ea.key)
        if holder is not None:
            logger.warning(
                "refusing a second %s EA for %s from %s: %s serves it",
                ea.chart_role,
                ea.symbol,
                ws.remote_address,
                holder.remote_address,
            )
            self._to_ea(ws, binary.Duplicate(ea.chart_role, ea.symbol))
            self._close_later(ws, CloseCode.NORMAL_CLOSURE, "duplicate")
        else:
            logger.info("%s EA for %s connected: %s", ea.chart_role, ea.symbol, ws.remote_address)
            self._served[ea.key] = ws
            if ea.chart_role is ChartRole.SPAWNER:
                self._spawner = ws
                self._sent.clear()
                self._close_sent.clear()
                for key in sorted(self._requested):
                    self._request_chart(key)
            else:
                self._requested.discard(ea.key)
                self._sent.discard(ea.key)
                self._failed.pop(ea.key, None)
                # The terminal drops a closing chart's EA's socket before it unloads the EA, which
                # says hello again in between: that hello is no use.
                if ea.key not in self._close_sent:
                    self._used[ea.key] = time.monotonic()
            if ea.chart_role is ChartRole.PUBLISHER:
                self._told[ea.symbol] = self._wanted(ea.symbol)
                self._to_ea(ws, self._told[ea.symbol])

    def _on_ea_frame(self, ws, ea: _Ea, frame: binary.Frame) -> None:
        """Takes a frame from an EA, by what its role sends; a refused duplicate's frames are
        dropped silently."""
        serving = self._served.get(ea.key) is ws
        publisher = ea.chart_role is ChartRole.PUBLISHER
        spawner = ea.chart_role is ChartRole.SPAWNER
        if isinstance(frame, binary.EaHello):
            raise ValueError(f"a second hello from the {ea.chart_role} EA for {ea.symbol}")
        elif not serving:
            pass
        elif isinstance(frame, binary.Answer):
            self._answer(ws, ea, frame)
        elif isinstance(frame, binary.Busy):
            self._busy(ws, ea, frame)
        elif isinstance(frame, binary.Ticks | binary.RatesBar) and publisher:
            self._publish(ea, frame)
        elif isinstance(frame, binary.TradeTransaction) and (publisher or spawner):
            self._publish(ea, frame)
        elif isinstance(frame, binary.ServerTime) and spawner:
            self._server_time(ea, frame)
        elif isinstance(frame, binary.Commissions) and publisher:
            self._relayed_commissions(ea, frame)
        elif isinstance(frame, binary.Sessions) and publisher:
            self._relayed_sessions(ea, frame)
        elif (
            isinstance(frame, binary.ChartOpened | binary.ChartFailed | binary.ChartKept)
            and spawner
        ):
            self._chart_answer(frame)
        else:
            raise ValueError(f"a {type(frame).__name__} frame from a {ea.chart_role} EA")

    def _on_consumer_frame(self, ws, consumer: _Consumer, frame: binary.Frame) -> None:
        if isinstance(frame, binary.AdapterHello):
            raise ValueError("a second hello from a consumer")
        elif isinstance(frame, binary.Subscribe | binary.Unsubscribe):
            self._op(ws, consumer, frame)
        else:
            self._request(ws, frame)

    def _op(self, ws, consumer: _Consumer, frame: binary.Subscribe | binary.Unsubscribe) -> None:
        """Applies a consumer's subscribe or unsubscribe and queues its ack ahead of any frame the
        change routes to it; a subscription to a symbol no publisher serves requests its chart."""
        if isinstance(frame, binary.Subscribe):
            consumer.subscriptions.add(frame.subscription)
        else:
            consumer.subscriptions.discard(frame.subscription)
        self._mark_used([frame.subscription])
        if self._enqueue(ws, consumer, binary.encode(binary.Ack(frame.op_id))):
            self._tell_publishers()
            symbol = frame.subscription.symbol
            if isinstance(frame, binary.Subscribe) and symbol is not None:
                key = (ChartRole.PUBLISHER, symbol)
                if key not in self._served:
                    self._request_chart(key)

    # ── Streams ───────────────────────────────────────────────────────────────

    def _publish(
        self, ea: _Ea, frame: binary.Ticks | binary.RatesBar | binary.TradeTransaction
    ) -> None:
        """Fans a frame out to the consumers subscribed to it when the EA may send it: a publisher's
        of its own symbol, or the spawner's transaction of no symbol."""
        passed = published(frame, self._clock, self._repeated_hours)
        own = ea.chart_role is ChartRole.PUBLISHER and passed.symbol == ea.symbol
        if not (own or (passed.symbol == "" and ea.chart_role is ChartRole.SPAWNER)):
            logger.warning(
                "dropping a %s frame for %r from the %s EA for %s",
                type(frame).__name__,
                passed.symbol,
                ea.chart_role,
                ea.symbol,
            )
        elif isinstance(frame, binary.RatesBar):
            self._publish_bar(passed.subscription, passed.frame, frame.rates["time"])
        else:
            self._fan_out(passed.subscription, binary.encode(passed.frame))

    def _publish_bar(self, subscription: Subscription, bar: binary.Bar, broker_open: int) -> None:
        """Fans a bar out unless the last bar of its series fanned out opened at the same true-UTC
        second; that first bar keeps the period, as a history read answers it."""
        opened = bar.bar["time"]
        if self._bar_opens.get(subscription) == opened:
            self._repeated_hours.warn_dropped_bar(
                binary.FrameKind.BAR.name.lower(), "time", broker_open, broker_open // _HOUR_S
            )
        else:
            self._bar_opens[subscription] = opened
            self._fan_out(subscription, binary.encode(bar))

    def _fan_out(self, subscription: Subscription, raw: bytes) -> None:
        overflowed = False
        for ws, consumer in list(self._consumers.items()):
            if subscription in consumer.subscriptions:
                overflowed |= not self._enqueue(ws, consumer, raw)
        if overflowed:
            self._tell_publishers()

    def _tell_publishers(self) -> None:
        """Tells each publisher its symbol's wanted frame when it differs from the one last told."""
        for (chart_role, symbol), ws in list(self._served.items()):
            if chart_role is ChartRole.PUBLISHER:
                wanted = self._wanted(symbol)
                if wanted != self._told[symbol]:
                    self._told[symbol] = wanted
                    self._to_ea(ws, wanted)

    def _wanted(self, symbol: str) -> binary.Wanted:
        """Whether some consumer subscribes to the symbol's ticks, and the timeframes whose bars
        some consumer subscribes to."""
        ticks = False
        timeframes = set()
        for consumer in self._consumers.values():
            for item in consumer.subscriptions:
                if item.symbol == symbol and item.stream == Stream.TICKS:
                    ticks = True
                elif item.symbol == symbol and item.stream == Stream.BARS:
                    timeframes.add(BAR_TIMEFRAME[item.timeframe])
        return binary.Wanted(symbol, ticks, tuple(sorted(timeframes)))

    # ── Relays ────────────────────────────────────────────────────────────────

    def _server_time(self, ea: _Ea, frame: binary.ServerTime) -> None:
        """Takes the spawner's server-time sample naming its own symbol into the gate."""
        if frame.symbol == ea.symbol:
            self._gate.write(frame)
        else:
            logger.warning(
                "dropping a server-time sample for %r from the spawner for %s",
                frame.symbol,
                ea.symbol,
            )

    def _relayed_commissions(self, ea: _Ea, frame: binary.Commissions) -> None:
        """Keeps a publisher's commission schedule under its own symbol, or the reason it is
        refused."""
        refusal = commissions_refusal(frame, ea.symbol)
        if refusal is None:
            self._commissions.write(ea.symbol, commission_schedule(frame.schedule))
        else:
            logger.warning("refusing the commission schedule for %s: %s", ea.symbol, refusal)
            self._commissions.write(ea.symbol, RelayRefusal(refusal))

    def _relayed_sessions(self, ea: _Ea, frame: binary.Sessions) -> None:
        """Keeps a publisher's trading sessions under its own symbol, or the reason they are
        refused."""
        refusal = sessions_refusal(frame, ea.symbol)
        if refusal is None:
            self._sessions.write(ea.symbol, trading_sessions(frame.sessions))
        else:
            logger.warning("refusing the trading sessions for %s: %s", ea.symbol, refusal)
            self._sessions.write(ea.symbol, RelayRefusal(refusal))

    # ── Charts ────────────────────────────────────────────────────────────────

    def _chart_answer(
        self, frame: binary.ChartOpened | binary.ChartFailed | binary.ChartKept
    ) -> None:
        """Takes the spawner's answer to an open_chart or a close_chart: clears or records the
        failure to open a chart, or counts a chart it kept open in use for another idle period."""
        key = (frame.chart_role, frame.symbol)
        if isinstance(frame, binary.ChartOpened):
            logger.info("the spawner opened the %s chart of %s", frame.chart_role, frame.symbol)
            self._failed.pop(key, None)
        elif isinstance(frame, binary.ChartFailed):
            logger.warning(
                "the spawner cannot open the %s chart of %s: MQL5 error %d",
                frame.chart_role,
                frame.symbol,
                frame.error,
            )
            self._failed[key] = frame.error
        else:
            logger.info(
                "the spawner keeps the %s chart of %s open: %d positions, %d pending orders",
                frame.chart_role,
                frame.symbol,
                frame.positions,
                frame.orders,
            )
            self._used[key] = time.monotonic()
            self._close_sent.pop(key, None)

    def _close_chart(self, key: ChartKey, now: float) -> None:
        # Recorded first: a send that overflows the spawner's queue drops it, clearing every close.
        self._close_sent[key] = now
        self._to_ea(self._spawner, binary.CloseChart(*key))

    def _request_chart(self, key: ChartKey) -> None:
        """Holds a chart request until its EA says hello, sending it to the spawner once per spawner
        connection."""
        self._requested.add(key)
        if self._spawner is not None and key not in self._sent:
            self._sent.add(key)
            self._to_ea(self._spawner, binary.OpenChart(*key))

    def _in_use(self, key: ChartKey, now: float) -> bool:
        """Whether a consumer subscribes to the symbol of a publisher chart, a request waits on the
        chart's EA, or the chart was used within the idle period."""
        chart_role, symbol = key
        subscribed = chart_role is ChartRole.PUBLISHER and any(
            item.symbol == symbol
            for consumer in self._consumers.values()
            for item in consumer.subscriptions
        )
        ea = self._served.get(key)
        waited_on = ea is not None and len(self._requests.of_ea(ea)) > 0
        recent = now - self._used.get(key, -math.inf) < self._settings.chart_idle_seconds
        return subscribed or waited_on or recent

    def _mark_used(self, subscriptions: Iterable[Subscription]) -> None:
        now = time.monotonic()
        for item in subscriptions:
            if item.symbol is not None:
                self._used[(ChartRole.PUBLISHER, item.symbol)] = now

    # ── Requests ──────────────────────────────────────────────────────────────

    def _request(self, ws, frame: binary.Request) -> None:
        """Routes a consumer's request: answered at once while the gate is shut, at the pending
        bound, for an invalid window and with no EA to serve it; answered by the hub for a call it
        serves; else sent to the EA of its call's role and symbol."""
        try:
            call = calls.Call(frame.call)
        except ValueError:
            raise ValueError(f"request {frame.request_id}: unknown call {frame.call}") from None
        row = calls.CALLS[call]
        arguments = calls.decode_arguments(row, frame)
        if row.shape is calls.Shape.RATES:
            bar_series(arguments["timeframe"])
        if self._gate.verification is None:
            message = "the broker clock is not verified"
            self._deferred(ws, frame.request_id, ErrorKind.NOT_READY, message)
        elif self._requests.asked_by(ws) >= self._settings.pending_bound:
            message = f"{self._settings.pending_bound} requests pending"
            self._error(ws, frame.request_id, 0, ErrorKind.OVERFLOW, message)
        elif call is calls.Call.COMMISSION_SCHEDULE:
            self._relay_read(ws, frame.request_id, row, arguments["symbol"], self._commissions)
        elif call is calls.Call.TRADING_SESSIONS:
            self._relay_read(ws, frame.request_id, row, arguments["symbol"], self._sessions)
        elif call in WINDOWS:
            self._window_read(ws, frame.request_id, row, arguments)
        else:
            symbol = calls.routed_symbol(row, arguments)
            self._send(ws, frame.request_id, row, arguments, symbol)

    def _window_read(
        self, ws, client_id: int, row: calls.Row, arguments: dict[str, object]
    ) -> None:
        """Sends a range read to its EA, or answers it invalid_window when its true-UTC window
        starts after it ends or after the terminal's time."""
        refusal = window_refusal(arguments, self._gate.terminal_time())
        if refusal is None:
            symbol = calls.routed_symbol(row, arguments)
            self._send(ws, client_id, row, arguments, symbol)
        else:
            self._error(ws, client_id, 0, ErrorKind.INVALID_WINDOW, refusal)

    def _send(
        self, ws, client_id: int, row: calls.Row, arguments: dict[str, object], symbol: str | None
    ) -> None:
        """Sends a request to the EA serving its role and symbol; with none, has the spawner open
        its chart and defers it, or refuses it once that chart failed to open."""
        if row.role is ChartRole.SPAWNER:
            key = (ChartRole.SPAWNER, None)
            state = None
        else:
            key = (row.role, symbol)
            state = self.use_chart(key)
        ea = self._served.get(key)
        if ea is not None and self._eas[ea].outbound.queue.qsize() < self._settings.ea_queue_bound:
            results = Results(row, arguments, self._clock, self._repeated_hours)
            pending = self._requests.add(ws, client_id, row, symbol, ea, results)
            count, body = calls.encode_arguments(row, broker_arguments(row, arguments, self._clock))
            self._to_ea(ea, binary.Request(pending.hub_id, row.call, count, body))
        elif ea is not None:
            message = f"{self._settings.ea_queue_bound} frames queued for the EA"
            self._error(ws, client_id, 0, ErrorKind.OVERFLOW, message)
        elif state is ChartState.FAILED:
            self._chart_failed(ws, client_id, key)
        elif row.role is ChartRole.SPAWNER:
            self._deferred(ws, client_id, ErrorKind.NO_PUBLISHER, "no spawner is connected")
        else:
            message = f"no {row.role} EA for {symbol}; chart requested"
            self._deferred(ws, client_id, ErrorKind.NO_PUBLISHER, message)

    def _relay_read(
        self,
        ws,
        client_id: int,
        row: calls.Row,
        symbol: str,
        store: CommissionStore | SessionStore,
    ) -> None:
        """Answers a commission schedule or trading sessions read from the latest relay for its
        symbol, deferring it while no publisher serves the symbol or none was relayed."""
        key = (ChartRole.PUBLISHER, symbol)
        state = self.use_chart(key)
        relay = store.read(symbol)
        if state is ChartState.REQUESTED:
            message = f"no publisher for {symbol}; chart requested"
            self._deferred(ws, client_id, ErrorKind.NO_PUBLISHER, message)
        elif state is ChartState.FAILED:
            self._chart_failed(ws, client_id, key)
        elif relay is None:
            message = f"no {row.call.name.lower()} relayed for {symbol}"
            self._deferred(ws, client_id, ErrorKind.SYNCING, message)
        elif isinstance(relay, RelayRefusal):
            message = f"the {row.call.name.lower()} relay for {symbol} was refused: {relay.reason}"
            self._error(ws, client_id, 0, ErrorKind.EA_ERROR, message)
        else:
            count, result = calls.write_result(row, dataclasses.asdict(relay))
            self._to_consumer(ws, binary.Answer(client_id, 0, True, 0, count, result))

    def _answer(self, ws, ea: _Ea, answer: binary.Answer) -> None:
        """Passes an EA's answer part on to the consumer that asked, its results in true UTC; a part
        for a request not pending on this EA is dropped with a warning."""
        pending = self._requests.get(ws, answer.request_id)
        if pending is None:
            logger.warning(
                "dropping an answer to request %d from the %s EA for %s: none is pending",
                answer.request_id,
                ea.chart_role,
                ea.symbol,
            )
        elif answer.error is not None:
            self._requests.remove(pending)
            passed = dataclasses.replace(
                answer, request_id=pending.client_id, part=pending.next_part
            )
            self._to_consumer(pending.consumer, passed)
        elif answer.part != pending.next_part:
            raise ValueError(
                f"part {answer.part} of the answer to request {answer.request_id}, "
                f"not {pending.next_part}"
            )
        else:
            # Composed before the request leaves: a part the hub cannot pass on refuses its EA, and
            # the request, still pending on it, is answered ea_gone.
            value = calls.read_ea_result(pending.served, answer)
            if pending.served.shape is calls.Shape.RANGES:
                passed = self._measured(pending, answer, value)
            else:
                count, result = calls.write_result(pending.served, pending.results.part(value))
                passed = binary.Answer(
                    pending.client_id, answer.part, answer.last, answer.last_error, count, result
                )
            pending.next_part += 1
            if answer.last:
                self._requests.remove(pending)
            self._to_consumer(pending.consumer, passed)

    def _busy(self, ws, ea: _Ea, busy: binary.Busy) -> None:
        """Moves a request's deadline and its EA's keepalive tolerance to the busy's bound, and
        tells the consumer that asked."""
        pending = self._requests.get(ws, busy.request_id)
        if pending is None:
            logger.warning(
                "dropping a busy for request %d from the %s EA for %s: none is pending",
                busy.request_id,
                ea.chart_role,
                ea.symbol,
            )
        else:
            self._requests.busy(pending, busy.bound_s)
            self._to_consumer(pending.consumer, binary.Busy(pending.client_id, busy.bound_s))

    def _expired(self, pending: Pending) -> None:
        ea = self._eas[pending.ea]
        logger.error(
            "request %d, %s, to the %s EA for %s went past its deadline unanswered",
            pending.hub_id,
            pending.served.call.name.lower(),
            ea.chart_role,
            ea.symbol,
        )
        message = f"the {ea.chart_role} EA for {ea.symbol} did not answer in time"
        self._error(
            pending.consumer, pending.client_id, pending.next_part, ErrorKind.TIMEOUT, message
        )

    def _measured(
        self, pending: Pending, answer: binary.Answer, measured: calls.MeasuredFloors
    ) -> binary.Answer:
        """Takes a history EA's ranges answer as the symbol's floors, and answers the consumer the
        symbol's ranges from the store."""
        self._floors.replace(pending.symbol, measured.floors, int(time.time()))
        ranges = self._ranges(pending.symbol, measured.maxbars)
        count, result = calls.write_result(pending.served, ranges)
        return binary.Answer(pending.client_id, 0, True, answer.last_error, count, result)

    def _ranges(self, symbol: str, maxbars: int) -> calls.Ranges:
        """The terminal's MaxBars and every floor held for the symbol, in true UTC."""
        conversion = Conversion(self._clock, self._repeated_hours)
        floors = self._floors.of_symbol(symbol)
        series = {}
        for name in Series:
            if name in floors:
                floor = floors[name]
                advertised = conversion.utc(
                    mirror.EpochUnit.SECONDS, floor.value, f"{symbol} {name}", "floor"
                )
                series[name] = calls.SeriesRange(advertised, floor.measured_at, floor.generation)
        return calls.Ranges(maxbars, series)

    def _deferred(self, ws, client_id: int, kind: ErrorKind, message: str) -> None:
        error = binary.AnswerError(kind, message, int(self._settings.retry_after_s * 1000))
        self._to_consumer(ws, binary.Answer(client_id, 0, True, 0, error=error))

    def _error(self, ws, client_id: int, part: int, kind: ErrorKind, message: str) -> None:
        error = binary.AnswerError(kind, message)
        self._to_consumer(ws, binary.Answer(client_id, part, True, 0, error=error))

    def _chart_failed(self, ws, client_id: int, key: ChartKey) -> None:
        """Answers a request ea_error naming its chart that failed to open, with the MQL5 error the
        spawner met."""
        chart_role, symbol = key
        mql5_error = self._failed[key]
        message = f"the {chart_role} chart of {symbol} failed to open: MQL5 error {mql5_error}"
        error = binary.AnswerError(ErrorKind.EA_ERROR, message)
        self._to_consumer(ws, binary.Answer(client_id, 0, True, mql5_error, error=error))

    # ── Sending ───────────────────────────────────────────────────────────────

    def _outbound(self, ws) -> _Outbound:
        queue = asyncio.Queue()
        return _Outbound(queue, asyncio.create_task(self._send_queued(ws, queue)))

    def _to_consumer(self, ws, frame: binary.Frame) -> None:
        """Queues a frame for a consumer still connected."""
        consumer = self._consumers.get(ws)
        if consumer is not None:
            self._enqueue(ws, consumer, binary.encode(frame))

    def _enqueue(self, ws, consumer: _Consumer, raw: bytes) -> bool:
        """Queues a frame for a consumer; whether its queue had room. A consumer whose queue is full
        is closed with the overflow code."""
        if consumer.outbound.queue.qsize() < self._settings.consumer_queue_bound:
            consumer.outbound.queue.put_nowait(raw)
            return True
        else:
            logger.warning(
                "closing consumer %s: %d frames queued and not sent",
                ws.remote_address,
                self._settings.consumer_queue_bound,
            )
            self._drop(ws)
            self._close_later(ws, OVERFLOW, "queue overflow")
            return False

    def _to_ea(self, ws, frame: binary.Frame) -> None:
        """Queues a frame for an EA; an EA whose queue is full is closed with the overflow code."""
        ea = self._eas.get(ws)
        if ea is not None and ea.outbound.queue.qsize() < self._settings.ea_queue_bound:
            ea.outbound.queue.put_nowait(binary.encode(frame))
        elif ea is not None:
            logger.warning(
                "closing the %s EA for %s: %d frames queued and not sent",
                ea.chart_role,
                ea.symbol,
                self._settings.ea_queue_bound,
            )
            self._drop(ws)
            self._close_later(ws, OVERFLOW, "queue overflow")

    async def _send_queued(self, ws, queue: asyncio.Queue) -> None:
        """Sends a connection the frames queued for it, in the order they were queued."""
        try:
            while True:
                await ws.send(await queue.get())
        except ConnectionClosed:
            # The connection's handler logs the close and drops it.
            pass

    def _close_later(self, ws, code: int, reason: str) -> None:
        # Not awaited: the close waits on a peer that may not be reading.
        closing = asyncio.create_task(ws.close(code, reason))
        self._closing.add(closing)
        closing.add_done_callback(self._closing.discard)

    async def _keepalive(self, ws) -> None:
        """Pings the connection every interval, one ping outstanding, and fails it once a pong is
        overdue: past the ping timeout, or for an EA past its latest busy's bound when later."""
        while True:
            await asyncio.sleep(PING_INTERVAL_S)
            try:
                pong = await ws.ping()
            except ConnectionClosed:
                return
            sent = time.monotonic()
            while not pong.done():
                deadline = sent + PING_INTERVAL_S * PING_TIMEOUT_INTERVALS
                busy_until = self._requests.busy_until(ws)
                if busy_until is not None:
                    deadline = max(deadline, busy_until)
                remaining = deadline - time.monotonic()
                if remaining > 0:
                    await asyncio.wait({pong}, timeout=remaining)
                else:
                    logger.warning("closing connection %s: its pong is overdue", ws.remote_address)
                    self._drop(ws)
                    self._close_later(ws, CloseCode.INTERNAL_ERROR, "keepalive ping timeout")
                    return

    async def _refuse(self, ws, message: str) -> None:
        """Answers a frame the hub cannot handle with a refusal, then closes the connection."""
        self._drop(ws)
        try:
            await ws.send(binary.encode(binary.Refusal(message)))
        except ConnectionClosed as closed:
            logger.info("connection %s closed before its refusal: %s", ws.remote_address, closed)
        await ws.close(CloseCode.PROTOCOL_ERROR)

    def _drop(self, ws) -> None:
        """Forgets a connection. A consumer's subscriptions leave the wanted frames with it and its
        pending requests are answered to no one; an EA that served its role and symbol leaves them
        unserved, each of its pending requests answered ea_gone, a close pending on its chart still
        pending, and a chart still in use is requested again."""
        consumer = self._consumers.pop(ws, None)
        ea = self._eas.pop(ws, None)
        if consumer is not None:
            consumer.outbound.sender.cancel()
            self._requests.consumer_left(ws)
            self._mark_used(consumer.subscriptions)
            self._tell_publishers()
        elif ea is not None:
            ea.outbound.sender.cancel()
            if self._served.get(ea.key) is ws:
                logger.info(
                    "%s EA for %s disconnected: %s", ea.chart_role, ea.symbol, ws.remote_address
                )
                del self._served[ea.key]
                message = f"the {ea.chart_role} EA for {ea.symbol} left"
                for pending in self._requests.of_ea(ws):
                    self._requests.remove(pending)
                    self._error(
                        pending.consumer,
                        pending.client_id,
                        pending.next_part,
                        ErrorKind.EA_GONE,
                        message,
                    )
                if ea.chart_role is ChartRole.SPAWNER:
                    self._spawner = None
                    self._sent.clear()
                    self._close_sent.clear()
                else:
                    if ea.chart_role is ChartRole.PUBLISHER:
                        del self._told[ea.symbol]
                    if self._in_use(ea.key, time.monotonic()):
                        self._request_chart(ea.key)


def serve_hub(hub: Hub, host: str, port: int):
    """The hub served on `host`:`port` with /health; the hub keeps every connection alive itself,
    and drops one within the close timeout."""
    return serve(
        hub.handler,
        host,
        port,
        process_request=hub.process_request,
        ping_interval=None,
        close_timeout=CLOSE_TIMEOUT_S,
    )


async def _serve(settings: HubSettings) -> None:
    clock = BrokerClock(settings.broker_tz, timedelta(hours=settings.broker_offset_hours))
    hub = Hub(clock, settings)
    hub.start()
    async with serve_hub(hub, "0.0.0.0", settings.hub_port):
        while True:
            await asyncio.sleep(_IDLE_SWEEP_S)
            hub.close_idle_charts()


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    settings = read_hub_settings(os.environ)
    logger.info(
        "hub on port %d, broker clock %s %+d h, charts closed after %d s out of use",
        settings.hub_port,
        settings.broker_tz.key,
        settings.broker_offset_hours,
        settings.chart_idle_seconds,
    )
    asyncio.run(_serve(settings))


def _json(connection, status: HTTPStatus, body: dict[str, object]) -> Response:
    response = connection.respond(status, json.dumps(body))
    del response.headers["Content-Type"]
    response.headers["Content-Type"] = "application/json"
    return response


if __name__ == "__main__":
    main()
