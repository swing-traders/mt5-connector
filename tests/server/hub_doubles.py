"""Doubles of the hub's peers: its settings for a test, a consumer on a real connection, a
connection double driving the handler directly, and a spawner relaying live server-time samples."""

import asyncio
import contextlib
import json
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from zoneinfo import ZoneInfo

import websockets
from ea_wire import FakeEa, encode_ea
from mirror_samples import CLOCK

from mt5connector.server.settings import HubSettings
from mt5connector.server.wire import binary, calls
from mt5connector.server.wire.push_wire import ChartRole
from mt5connector.server.ws_server import Hub, serve_hub

# How long a sample stays fresh in a test, and how often the spawner relays one.
MAX_AGE_S = 0.2
SAMPLE_EVERY_S = 0.04


def settings(**overrides) -> HubSettings:
    """The hub's default settings but for a short sample max age, and what a test overrides."""
    defaults = {
        "hub_port": 9000,
        "broker_tz": ZoneInfo("America/New_York"),
        "broker_offset_hours": 7,
        "chart_idle_seconds": 900,
        "retry_after_s": 5.0,
        "clock_sample_max_age_seconds": MAX_AGE_S,
        "clock_bootstrap_seconds": 120.0,
        "clock_check_seconds": 300.0,
        "consumer_queue_bound": 10_000,
        "ea_queue_bound": 10_000,
        "pending_bound": 1_000,
    }
    return HubSettings(**(defaults | overrides))


async def run_hub(**overrides):
    """The hub served on a free loopback port."""
    hub = Hub(CLOCK, settings(**overrides))
    server = await serve_hub(hub, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    return hub, server, port


@dataclass
class Consumer:
    """A consumer on a real connection to the hub, speaking the session's frames."""

    ws: object

    @classmethod
    async def connect(cls, port: int) -> "Consumer":
        ws = await websockets.connect(f"ws://127.0.0.1:{port}")
        await ws.send(binary.encode(binary.AdapterHello()))
        return cls(ws)

    async def send(self, frame) -> None:
        await self.ws.send(binary.encode(frame))

    async def ask(self, request_id: int, call: calls.Call, **arguments) -> None:
        count, body = calls.encode_arguments(calls.CALLS[call], arguments)
        await self.send(binary.Request(request_id, call, count, body))

    async def received(self, timeout=1.0):
        return binary.decode(await asyncio.wait_for(self.ws.recv(), timeout), binary.Sender.HUB)

    async def subscribed(self, op_id: int, subscription) -> None:
        await self.send(binary.Subscribe(op_id, subscription))
        assert await self.received() == binary.Ack(op_id)

    async def close(self) -> None:
        await self.ws.close()


def sample(symbol: str, *, connected=True, skew_s=0) -> binary.ServerTime:
    """A server-time sample of now, the trade server `skew_s` ahead of the true clock."""
    now = int(time.time())
    broker = CLOCK.to_broker(now + skew_s)
    return binary.ServerTime(symbol, broker, broker, now, connected)


@contextlib.asynccontextmanager
async def verified(port: int, symbol="EURUSD"):
    """A spawner relaying a live sample every SAMPLE_EVERY_S, held until the hub's clock is
    verified; yields the spawner."""
    spawner = await FakeEa.connect(port, ChartRole.SPAWNER, symbol)

    async def relay():
        with contextlib.suppress(websockets.ConnectionClosed):
            while True:
                await spawner.send(sample(symbol))
                await asyncio.sleep(SAMPLE_EVERY_S)

    task = asyncio.create_task(relay())
    try:
        await eventually(lambda: health(port)[0] == 200, timeout=MAX_AGE_S * 10)
        yield spawner
    finally:
        task.cancel()


def health(port: int) -> tuple[int, dict]:
    request = urllib.request.Request(f"http://127.0.0.1:{port}/health")
    try:
        with urllib.request.urlopen(request, timeout=1) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as refused:
        return refused.code, json.loads(refused.read())


async def health_of(port: int) -> tuple[int, dict]:
    return await asyncio.get_running_loop().run_in_executor(None, health, port)


async def eventually(predicate, timeout=2.0):
    """Waits for the predicate, run off the loop so it may ask the hub over HTTP."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not await loop.run_in_executor(None, predicate):
        if loop.time() > deadline:
            raise AssertionError("the condition never held")
        await asyncio.sleep(0.01)


class Peer:
    """A connection double for the hub's handler."""

    def __init__(self, stuck=False, deaf=False):
        self.incoming = asyncio.Queue()
        self.sent = []
        self.closed = None
        # No send of the hub's ever completes.
        self.stuck = stuck
        # Its pings are never answered.
        self.deaf = deaf
        self.remote_address = ("127.0.0.1", 0)

    def put(self, raw: bytes) -> None:
        self.incoming.put_nowait(raw)

    def put_ea(self, sent) -> None:
        self.put(encode_ea(sent))

    def put_frame(self, frame) -> None:
        self.put(binary.encode(frame))

    def __aiter__(self):
        return self

    async def __anext__(self):
        raw = await self.incoming.get()
        if raw is None:
            raise StopAsyncIteration
        return raw

    async def send(self, raw):
        if self.stuck:
            await asyncio.Event().wait()
        self.sent.append(raw)

    async def ping(self):
        pong = asyncio.get_running_loop().create_future()
        if not self.deaf:
            pong.set_result(0.0)
        return pong

    async def close(self, code=1000, reason=""):
        self.closed = (code, reason)
        self.incoming.put_nowait(None)


async def settle():
    for _ in range(20):
        await asyncio.sleep(0)
