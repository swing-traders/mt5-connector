"""A consumer's channel to the hub on NT's WebSocketClient, sending its wanted subscriptions whole
again on every reconnect: the hub keeps nothing for a consumer that left.

State: the subscriptions wanted, in the order they were first wanted, and the next op id. A change
to what is wanted and the resend of the whole set hold one lock across their sends, so the hub
receives them in the order the set changed."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from typing import TYPE_CHECKING

from nautilus_trader.core.nautilus_pyo3 import (
    WebSocketClient,
    WebSocketClientError,
    WebSocketConfig,
)

from mt5connector.client.constants import RECONNECT_MULTIPLIER
from mt5connector.wire.push_wire import (
    PING_INTERVAL_S,
    PROTOCOL_VERSION,
    FrameType,
    Role,
    Subscription,
)

if TYPE_CHECKING:
    from nautilus_trader.common.component import Logger

    from mt5connector.client.config import MT5Config

_DATA = frozenset({FrameType.TICK, FrameType.BAR, FrameType.TRADE_TRANSACTION})


class PushClient:
    """One consumer's connection to the hub."""

    def __init__(
        self,
        config: MT5Config,
        loop: asyncio.AbstractEventLoop,
        on_frame: Callable[[dict], None],
        on_reconnect: Callable[[], None] | None,
        log: Logger,
    ) -> None:
        self._config = config
        self._loop = loop
        self._on_frame = on_frame
        self._on_reconnect = on_reconnect
        self._log = log
        self._ws: WebSocketClient | None = None
        self._wanted: list[Subscription] = []
        self._wanting = asyncio.Lock()
        self._next_op_id = 1
        self._tasks: set[asyncio.Task] = set()

    async def connect(self) -> None:
        """Connects to the hub, says hello and subscribes everything wanted; NT's client reconnects
        with backoff from then on. Raises RuntimeError on a connected client."""
        if self._ws is not None:
            raise RuntimeError("push channel: already connected")
        config = WebSocketConfig(
            url=self._config.ws_url,
            headers=[],
            heartbeat=PING_INTERVAL_S,
            reconnect_delay_initial_ms=int(self._config.reconnect_initial_delay_s * 1000),
            reconnect_delay_max_ms=int(self._config.reconnect_max_delay_s * 1000),
            reconnect_backoff_factor=RECONNECT_MULTIPLIER,
        )
        self._ws = await WebSocketClient.connect(
            self._loop,
            config,
            self._on_message,
            post_reconnection=self._on_reconnected_off_loop,
        )
        await self._send_wanted()

    async def disconnect(self) -> None:
        """Closes the connection; the subscriptions stay wanted. Raises RuntimeError on a client
        never connected."""
        if self._ws is None:
            raise RuntimeError("push channel: not connected")
        for task in self._tasks:
            task.cancel()
        await self._ws.disconnect()
        self._ws = None

    async def subscribe(self, subscription: Subscription) -> None:
        """Wants a stream, and subscribes it while connected."""
        async with self._wanting:
            if subscription not in self._wanted:
                self._wanted.append(subscription)
                await self._send_op(FrameType.SUBSCRIBE, subscription)

    async def unsubscribe(self, subscription: Subscription) -> None:
        """Stops wanting a stream, and unsubscribes it while connected."""
        async with self._wanting:
            if subscription in self._wanted:
                self._wanted.remove(subscription)
                await self._send_op(FrameType.UNSUBSCRIBE, subscription)

    async def _send_op(self, frame_type: FrameType, subscription: Subscription) -> None:
        """Sends an op under the next op id while connected; the connect or reconnect sends the
        wanted set otherwise."""
        if self._ws is not None and self._ws.is_active():
            await self._send(subscription.op(frame_type, self._op_id()))

    def _op_id(self) -> int:
        op_id = self._next_op_id
        self._next_op_id += 1
        return op_id

    async def _send_wanted(self) -> None:
        async with self._wanting:
            await self._send({"v": PROTOCOL_VERSION, "type": FrameType.HELLO, "role": Role.ADAPTER})
            for subscription in self._wanted:
                await self._send(subscription.op(FrameType.SUBSCRIBE, self._op_id()))

    async def _send(self, frame: dict[str, object]) -> None:
        """Sends a frame; one the connection drops is sent again with the whole wanted set when NT's
        client reconnects."""
        try:
            await self._ws.send_text(json.dumps(frame).encode())
        except (WebSocketClientError, RuntimeError) as exc:
            self._log.info(f"push: {frame['type']} not sent, sent again on reconnect: {exc}")

    def _on_message(self, raw: bytes) -> None:
        """Hands a data frame to the owner; logs a refusal, a frame of another version, and a frame
        the owner fails on, so the stream goes on."""
        frame = json.loads(raw)
        if frame.get("v") != PROTOCOL_VERSION:
            self._log.error(f"push: a frame of version {frame.get('v')!r}: {raw!r}")
        elif frame.get("type") == FrameType.ERROR:
            self._log.error(f"push: the hub refused a frame: {frame.get('message')}")
        elif frame.get("type") == FrameType.ACK:
            self._log.debug(f"push: op {frame.get('id')} acknowledged")
        elif frame.get("type") in _DATA:
            try:
                self._on_frame(frame)
            except Exception as exc:
                self._log.exception(f"push: a {frame['type']} frame failed", exc)
        else:
            self._log.error(f"push: a frame of an unknown kind: {raw!r}")

    def _on_reconnected_off_loop(self) -> None:
        # NT calls this on its own thread.
        self._loop.call_soon_threadsafe(self._on_reconnected)

    def _on_reconnected(self) -> None:
        """Tells the owner, then sends the hello and the whole wanted set again."""
        self._log.info("push: reconnected, subscribing again what is wanted")
        if self._on_reconnect is not None:
            self._on_reconnect()
        task = self._loop.create_task(self._send_wanted())
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
