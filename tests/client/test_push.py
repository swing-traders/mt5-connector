"""The client's push channel on NT's WebSocketClient against a hub double."""

import asyncio
import json
from unittest.mock import MagicMock

import pytest
from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

from mt5connector.client.config import MT5Config
from mt5connector.client.push import PushClient
from mt5connector.wire.history_wire import Series
from mt5connector.wire.push_wire import Stream, Subscription

HELLO = {"v": 1, "type": "hello", "role": "adapter"}
TICK = {
    "v": 1,
    "type": "tick",
    "symbol": "EURUSD",
    "time": 1_752_570_000,
    "bid": 1.085,
    "ask": 1.08512,
    "last": 0.0,
    "volume": 0,
    "time_msc": 1_752_570_000_123,
    "flags": 6,
    "volume_real": 0.0,
}


class HubDouble:
    """A hub on a free loopback port that records what each connection sends, acknowledges every
    op, and pushes what a test hands it to the latest connection."""

    def __init__(self):
        self.received: list[list[dict]] = []
        self.connections = []

    async def handler(self, ws):
        self.connections.append(ws)
        frames = []
        self.received.append(frames)
        try:
            async for raw in ws:
                frame = json.loads(raw)
                frames.append(frame)
                if frame["type"] in ("subscribe", "unsubscribe"):
                    await ws.send(json.dumps({"v": 1, "type": "ack", "id": frame["id"]}))
        except ConnectionClosed:
            pass

    async def push(self, frame):
        await self.connections[-1].send(json.dumps(frame))


async def eventually(predicate, timeout=3.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("the condition never held")
        await asyncio.sleep(0.01)


@pytest.fixture
async def hub():
    double = HubDouble()
    server = await serve(double.handler, "127.0.0.1", 0)
    double.port = server.sockets[0].getsockname()[1]
    yield double
    server.close()


def config(port):
    return MT5Config(
        account=12345678,
        password="p",
        server="Broker-Demo",
        symbols=["EURUSD"],
        server_url="http://127.0.0.1:5000",
        ws_url=f"ws://127.0.0.1:{port}",
        reconnect_initial_delay_s=0.05,
        reconnect_max_delay_s=0.2,
    )


class Owner:
    """What a push channel's owner sees: the frames handed to it and its reconnects."""

    def __init__(self, fails=False):
        self.frames = []
        self.reconnects = 0
        self.fails = fails
        self.log = MagicMock()

    def on_frame(self, frame):
        if self.fails:
            raise ValueError("the owner cannot handle it")
        self.frames.append(frame)

    def on_reconnect(self):
        self.reconnects += 1


def channel(hub, owner):
    return PushClient(
        config(hub.port),
        asyncio.get_running_loop(),
        owner.on_frame,
        owner.on_reconnect,
        owner.log,
    )


async def test_connect_says_hello_and_subscribes_what_is_wanted_under_op_ids(hub):
    owner = Owner()
    push = channel(hub, owner)
    await push.subscribe(Subscription(Stream.TICKS, "EURUSD"))

    await push.connect()
    await push.subscribe(Subscription(Stream.TRADE_TRANSACTIONS))
    await eventually(lambda: hub.received and len(hub.received[0]) == 3)

    assert hub.received == [
        [
            HELLO,
            {"v": 1, "type": "subscribe", "id": 1, "stream": "ticks", "symbol": "EURUSD"},
            {"v": 1, "type": "subscribe", "id": 2, "stream": "trade_transactions"},
        ]
    ]
    await asyncio.sleep(0.1)
    assert owner.frames == []
    await push.disconnect()


async def test_a_pushed_data_frame_is_handed_to_the_owner(hub):
    owner = Owner()
    push = channel(hub, owner)
    await push.connect()
    await eventually(lambda: hub.connections)

    await hub.push(TICK)

    await eventually(lambda: owner.frames)
    assert owner.frames == [TICK]
    await push.disconnect()


async def test_an_unsubscribe_is_sent_and_leaves_the_wanted_set(hub):
    owner = Owner()
    push = channel(hub, owner)
    ticks = Subscription(Stream.TICKS, "EURUSD")
    await push.connect()
    await push.subscribe(ticks)

    await push.unsubscribe(ticks)
    await eventually(lambda: len(hub.received[0]) == 3)

    assert hub.received[0][2] == {
        "v": 1,
        "type": "unsubscribe",
        "id": 2,
        "stream": "ticks",
        "symbol": "EURUSD",
    }
    await push.disconnect()


async def test_a_reconnect_resends_the_hello_and_the_whole_wanted_set_after_telling_the_owner(hub):
    owner = Owner()
    push = channel(hub, owner)
    await push.subscribe(Subscription(Stream.TICKS, "EURUSD"))
    await push.subscribe(Subscription(Stream.BARS, "XAUUSD", Series.H1))
    await push.subscribe(Subscription(Stream.TICKS, "GBPUSD"))
    await push.unsubscribe(Subscription(Stream.TICKS, "GBPUSD"))
    await push.connect()
    await eventually(lambda: hub.received and len(hub.received[0]) == 3)

    await hub.connections[0].close(4008, "queue overflow")
    await eventually(lambda: len(hub.received) == 2 and len(hub.received[1]) == 3)

    assert owner.reconnects == 1
    assert hub.received[1] == [
        HELLO,
        {"v": 1, "type": "subscribe", "id": 3, "stream": "ticks", "symbol": "EURUSD"},
        {
            "v": 1,
            "type": "subscribe",
            "id": 4,
            "stream": "bars",
            "symbol": "XAUUSD",
            "timeframe": "H1",
        },
    ]
    await push.disconnect()


async def test_an_error_frame_is_logged_and_not_handed_to_the_owner(hub):
    owner = Owner()
    push = channel(hub, owner)
    await push.connect()
    await eventually(lambda: hub.connections)

    await hub.push({"v": 1, "type": "error", "message": "not an op id: True"})
    await hub.push(TICK)

    await eventually(lambda: owner.frames)
    assert owner.frames == [TICK]
    assert [call.args[0] for call in owner.log.error.call_args_list] == [
        "push: the hub refused a frame: not an op id: True"
    ]
    await push.disconnect()


async def test_a_frame_of_another_version_is_logged_and_not_handed_to_the_owner(hub):
    owner = Owner()
    push = channel(hub, owner)
    await push.connect()
    await eventually(lambda: hub.connections)

    await hub.push(TICK | {"v": 2})
    await hub.push(TICK)

    await eventually(lambda: owner.frames)
    assert owner.frames == [TICK]
    assert len(owner.log.error.call_args_list) == 1
    assert owner.log.error.call_args.args[0].startswith("push: a frame of version 2: ")
    await push.disconnect()


async def test_a_frame_the_owner_fails_on_is_logged_and_the_channel_goes_on(hub):
    owner = Owner(fails=True)
    push = channel(hub, owner)
    await push.connect()
    await eventually(lambda: hub.connections)

    await hub.push(TICK)
    await hub.push(TICK)

    await eventually(lambda: len(owner.log.exception.call_args_list) == 2)
    message, failure = owner.log.exception.call_args.args
    assert message == "push: a tick frame failed"
    assert isinstance(failure, ValueError)
    await push.disconnect()


async def test_a_second_connect_and_a_disconnect_never_connected_raise(hub):
    owner = Owner()
    push = channel(hub, owner)
    with pytest.raises(RuntimeError):
        await push.disconnect()
    await push.connect()
    with pytest.raises(RuntimeError):
        await push.connect()
    await push.disconnect()


@pytest.mark.parametrize(
    ("stream", "symbol", "timeframe"),
    [
        ("nonsense", None, None),
        ("ticks", "EURUSD", None),
        (Stream.BARS, "EURUSD", "M1"),
        (Stream.TICKS, "", None),
        (Stream.TICKS, "EURUSD", Series.M1),
        (Stream.BARS, "EURUSD", Series.TICKS),
        (Stream.TRADE_TRANSACTIONS, "EURUSD", None),
    ],
    ids=[
        "unknown-stream",
        "stream-name",
        "timeframe-name",
        "empty-symbol",
        "ticks-with-a-timeframe",
        "bars-of-ticks",
        "transactions-of-a-symbol",
    ],
)
def test_a_subscription_naming_no_stream_is_refused_when_built(stream, symbol, timeframe):
    with pytest.raises(ValueError):
        Subscription(stream, symbol, timeframe)
