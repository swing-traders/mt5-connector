"""The client's hub session on NT's WebSocketClient against a hub double: its streams, its calls
under their correlation ids, their typed errors, its deadlines, and what a reconnect resends."""

import asyncio
import inspect
from unittest.mock import MagicMock

import numpy as np
import pytest
from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

from mt5connector.client import hub as hub_module
from mt5connector.client.config import MT5Config
from mt5connector.client.errors import (
    EaError,
    EaGone,
    HubTimeout,
    InvalidWindow,
    NoPublisher,
    NotReady,
    Overflow,
    ProtocolError,
    ResponseLost,
    ServerUnreachable,
    Syncing,
)
from mt5connector.client.hub import HubSession, Outcomes
from mt5connector.wire import binary, calls, mirror
from mt5connector.wire.binary import ErrorKind, Sender
from mt5connector.wire.history_wire import Series
from mt5connector.wire.push_wire import Stream, Subscription, TransactionType

Call = calls.Call
HELLO = binary.AdapterHello()
_TICKS = np.dtype(list(mirror.TICKS.dtype))


def zeroed(layout: binary.Layout, **values) -> dict:
    record = {}
    for member in layout.members:
        if member.declared is mirror.MqlType.STRING:
            record[member.name] = ""
        elif member.declared is mirror.MqlType.DOUBLE:
            record[member.name] = 0.0
        elif member.declared is mirror.MqlType.BOOL:
            record[member.name] = False
        elif member.declared is not None:
            record[member.name] = 0
    return record | values


def tick(msc=0):
    return {
        "time": 1_752_570_000,
        "bid": 1.085,
        "ask": 1.08512,
        "last": 0.0,
        "volume": 0,
        "time_msc": 1_752_570_000_000 + msc,
        "flags": 6,
        "volume_real": 0.0,
    }


class HubDouble:
    """A hub on a free loopback port that records what each connection sends, acknowledges every op,
    and sends what a test hands it to the latest connection."""

    def __init__(self):
        self.received: list[list] = []
        self.connections = []

    async def handler(self, ws):
        self.connections.append(ws)
        frames = []
        self.received.append(frames)
        try:
            async for raw in ws:
                if not frames:
                    frame = binary.decode_hello(raw)
                else:
                    frame = binary.decode(raw, Sender.ADAPTER)
                frames.append(frame)
                if isinstance(frame, binary.Subscribe | binary.Unsubscribe):
                    await ws.send(binary.encode(binary.Ack(frame.op_id)))
        except ConnectionClosed:
            pass

    async def push(self, frame):
        await self.connections[-1].send(binary.encode(frame))

    def requests(self, connection=-1) -> list[binary.Request]:
        return [frame for frame in self.received[connection] if isinstance(frame, binary.Request)]

    async def answer(self, request: binary.Request, value, *, part=0, last=True, last_error=0):
        count, body = calls.write_result(calls.CALLS[calls.Call(request.call)], value)
        await self.push(binary.Answer(request.request_id, part, last, last_error, count, body))

    async def fail(self, request: binary.Request, error: binary.AnswerError, last_error=0):
        await self.push(binary.Answer(request.request_id, 0, True, last_error, error=error))


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
        server_url="http://127.0.0.1:5000",
        ws_url=f"ws://127.0.0.1:{port}",
        reconnect_initial_delay_s=0.05,
        reconnect_max_delay_s=0.2,
    )


class Owner:
    """What a session's owner sees: the frames handed to it and its reconnects."""

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


def session(hub, owner):
    return HubSession(
        config(hub.port), asyncio.get_running_loop(), owner.on_frame, owner.on_reconnect, owner.log
    )


async def asked(hub, count=1, connection=-1) -> list[binary.Request]:
    await eventually(lambda: len(hub.received) > 0 and len(hub.requests(connection)) >= count)
    return hub.requests(connection)


# ── Streams ───────────────────────────────────────────────────────────────────


async def test_connect_says_hello_and_subscribes_what_is_wanted_under_op_ids(hub):
    owner = Owner()
    hub_session = session(hub, owner)
    await hub_session.subscribe(Subscription(Stream.TICKS, "EURUSD"))

    await hub_session.connect()
    await hub_session.subscribe(Subscription(Stream.TRADE_TRANSACTIONS))
    await eventually(lambda: hub.received and len(hub.received[0]) == 3)

    assert hub.received == [
        [
            HELLO,
            binary.Subscribe(1, Subscription(Stream.TICKS, "EURUSD")),
            binary.Subscribe(2, Subscription(Stream.TRADE_TRANSACTIONS)),
        ]
    ]
    await asyncio.sleep(0.1)
    assert owner.frames == []
    await hub_session.disconnect()


@pytest.mark.parametrize(
    "frame",
    [
        binary.Ticks("EURUSD", (tick(1), tick(2))),
        binary.Bar(
            "EURUSD",
            Series.M1,
            {
                "time": 60,
                "open": 1.0,
                "high": 1.1,
                "low": 0.9,
                "close": 1.05,
                "tick_volume": 3,
                "spread": 1,
                "real_volume": 0,
                "time_close": 120,
            },
        ),
        binary.TradeTransaction(
            zeroed(binary.TRADE_TRANSACTION, type=TransactionType.DEAL_ADD),
            zeroed(binary.TRADE_REQUEST),
            zeroed(binary.TRADE_RESULT),
        ),
    ],
    ids=["ticks", "bar", "transaction"],
)
async def test_a_pushed_stream_frame_is_handed_to_the_owner(hub, frame):
    owner = Owner()
    hub_session = session(hub, owner)
    await hub_session.connect()
    await eventually(lambda: hub.connections)

    await hub.push(frame)

    await eventually(lambda: owner.frames)
    assert owner.frames == [frame]
    await hub_session.disconnect()


async def test_an_unsubscribe_is_sent_and_leaves_the_wanted_set(hub):
    hub_session = session(hub, Owner())
    ticks = Subscription(Stream.TICKS, "EURUSD")
    await hub_session.connect()
    await hub_session.subscribe(ticks)

    await hub_session.unsubscribe(ticks)
    await eventually(lambda: len(hub.received[0]) == 3)

    assert hub.received[0][2] == binary.Unsubscribe(2, ticks)
    await hub_session.disconnect()


async def test_a_reconnect_resends_the_hello_and_the_whole_wanted_set_after_telling_the_owner(hub):
    owner = Owner()
    hub_session = session(hub, owner)
    await hub_session.subscribe(Subscription(Stream.TICKS, "EURUSD"))
    await hub_session.subscribe(Subscription(Stream.BARS, "XAUUSD", Series.H1))
    await hub_session.subscribe(Subscription(Stream.TICKS, "GBPUSD"))
    await hub_session.unsubscribe(Subscription(Stream.TICKS, "GBPUSD"))
    await hub_session.connect()
    await eventually(lambda: hub.received and len(hub.received[0]) == 3)

    await hub.connections[0].close(4008, "queue overflow")
    await eventually(lambda: len(hub.received) == 2 and len(hub.received[1]) == 3)

    assert owner.reconnects == 1
    assert hub.received[1] == [
        HELLO,
        binary.Subscribe(3, Subscription(Stream.TICKS, "EURUSD")),
        binary.Subscribe(4, Subscription(Stream.BARS, "XAUUSD", Series.H1)),
    ]
    await hub_session.disconnect()


class HeldSocket:
    """NT's WebSocketClient once connected, recording each frame as it is sent and holding the send
    of the first frame `hold` matches until released."""

    def __init__(self, hold):
        self.sent = []
        self.hold = hold
        self.holding = asyncio.Event()
        self.released = asyncio.Event()

    def is_active(self):
        return True

    async def send(self, data):
        if not self.sent:
            frame = binary.decode_hello(data)
        else:
            frame = binary.decode(data, Sender.ADAPTER)
        self.sent.append(frame)
        if self.hold(frame) and not self.holding.is_set():
            self.holding.set()
            await self.released.wait()


def held_by_the_hub(frames) -> set:
    """The streams a hub holds after the frames, in order."""
    held = set()
    for frame in frames:
        if isinstance(frame, binary.Subscribe):
            held.add(frame.subscription)
        elif isinstance(frame, binary.Unsubscribe):
            held.discard(frame.subscription)
    return held


async def test_an_unsubscribe_while_the_reconnect_resends_is_not_undone_by_it():
    owner = Owner()
    hub_session = HubSession(config(1), asyncio.get_running_loop(), owner.on_frame, None, owner.log)
    eurusd = Subscription(Stream.TICKS, "EURUSD")
    gbpusd = Subscription(Stream.TICKS, "GBPUSD")
    await hub_session.subscribe(eurusd)
    await hub_session.subscribe(gbpusd)
    socket = HeldSocket(lambda frame: getattr(frame, "subscription", None) == eurusd)
    hub_session._ws = socket

    hub_session._on_reconnected()
    await socket.holding.wait()
    unsubscribe = asyncio.ensure_future(hub_session.unsubscribe(gbpusd))
    await asyncio.wait({unsubscribe}, timeout=0.05)
    socket.released.set()
    await asyncio.gather(unsubscribe, *hub_session._tasks)

    assert held_by_the_hub(socket.sent) == {eurusd}


async def test_after_a_reconnect_a_subscribe_waits_for_the_hello_and_rides_the_resend():
    owner = Owner()
    hub_session = HubSession(config(1), asyncio.get_running_loop(), owner.on_frame, None, owner.log)
    await hub_session.subscribe(Subscription(Stream.TICKS, "EURUSD"))
    socket = HeldSocket(lambda frame: False)
    hub_session._ws = socket

    hub_session._on_reconnected()
    await hub_session.subscribe(Subscription(Stream.TICKS, "GBPUSD"))
    await hub_session.unsubscribe(Subscription(Stream.TICKS, "EURUSD"))
    await asyncio.gather(*hub_session._tasks)

    assert socket.sent == [HELLO, binary.Subscribe(1, Subscription(Stream.TICKS, "GBPUSD"))]
    await hub_session.subscribe(Subscription(Stream.TICKS, "USDJPY"))
    assert socket.sent[-1] == binary.Subscribe(2, Subscription(Stream.TICKS, "USDJPY"))


async def test_a_refusal_and_an_undecodable_frame_are_logged_and_the_stream_goes_on(hub):
    owner = Owner()
    hub_session = session(hub, owner)
    await hub_session.connect()
    await eventually(lambda: hub.connections)
    frame = binary.Ticks("EURUSD", (tick(),))

    await hub.push(binary.Refusal("a text frame"))
    await hub.connections[-1].send('{"v": 1, "type": "tick"}')
    await hub.push(frame)

    await eventually(lambda: owner.frames)
    assert owner.frames == [frame]
    errors = [call.args[0] for call in owner.log.error.call_args_list]
    assert errors[0] == "hub session: the hub refused a frame: a text frame"
    assert errors[1].startswith("hub session: a frame it cannot decode: offset 0: version 123")
    await hub_session.disconnect()


async def test_a_frame_the_owner_fails_on_is_logged_and_the_session_goes_on(hub):
    owner = Owner(fails=True)
    hub_session = session(hub, owner)
    await hub_session.connect()
    await eventually(lambda: hub.connections)

    await hub.push(binary.Ticks("EURUSD", (tick(),)))
    await hub.push(binary.Ticks("EURUSD", (tick(),)))

    await eventually(lambda: len(owner.log.exception.call_args_list) == 2)
    message, failure = owner.log.exception.call_args.args
    assert message == "hub session: a Ticks frame failed"
    assert isinstance(failure, ValueError)
    await hub_session.disconnect()


async def test_a_second_connect_and_a_disconnect_never_connected_raise(hub):
    hub_session = session(hub, Owner())
    with pytest.raises(RuntimeError):
        await hub_session.disconnect()
    await hub_session.connect()
    with pytest.raises(RuntimeError):
        await hub_session.connect()
    await hub_session.disconnect()


# ── Calls ─────────────────────────────────────────────────────────────────────


def test_each_call_is_a_method_with_its_registry_signature():
    signature = inspect.signature(HubSession.copy_rates_range)
    optional = inspect.signature(HubSession.history_deals_get)

    assert list(signature.parameters) == ["self", "symbol", "timeframe", "date_from", "date_to"]
    assert all(
        parameter.default is inspect.Parameter.empty for parameter in signature.parameters.values()
    )
    assert [parameter.default for parameter in optional.parameters.values()][1:] == [None] * 5


async def test_a_call_resolves_with_its_typed_result_and_the_error_its_answer_folds_in(hub):
    hub_session = session(hub, Owner())
    await hub_session.connect()

    selected = asyncio.create_task(hub_session.symbol_select("EURUSD", False))
    (request,) = await asked(hub)
    await hub.answer(request, True, last_error=4301)
    answered = await asyncio.wait_for(selected, 1)

    assert calls.decode_arguments(calls.CALLS[Call.SYMBOL_SELECT], request) == {
        "symbol": "EURUSD",
        "enable": False,
    }
    assert (answered.result, answered.last_error) == (True, 4301)
    await hub_session.disconnect()


async def test_two_calls_answered_in_reverse_order_resolve_each_with_its_own_answer(hub):
    hub_session = session(hub, Owner())
    await hub_session.connect()

    total = asyncio.create_task(hub_session.symbols_total())
    tick_read = asyncio.create_task(hub_session.symbol_info_tick("EURUSD"))
    first, second = await asked(hub, 2)
    await hub.answer(second, tick())
    await hub.answer(first, 1_234)

    assert (await asyncio.wait_for(total, 1)).result == 1_234
    assert (await asyncio.wait_for(tick_read, 1)).result == calls.Tick(**tick())
    assert first.request_id != second.request_id
    await hub_session.disconnect()


async def test_an_answer_in_parts_is_assembled(hub):
    hub_session = session(hub, Owner())
    await hub_session.connect()

    read = asyncio.create_task(
        hub_session.copy_ticks_range("EURUSD", 1_752_570_000, 1_752_570_060, 1)
    )
    (request,) = await asked(hub)
    first = np.array([tuple(tick(1).values())], dtype=_TICKS)
    rest = np.array([tuple(tick(2).values()), tuple(tick(3).values())], dtype=_TICKS)
    await hub.answer(request, first, last=False)
    await hub.answer(request, rest, part=1)
    answered = await asyncio.wait_for(read, 1)

    assert answered.result["time_msc"].tolist() == [
        1_752_570_000_001,
        1_752_570_000_002,
        1_752_570_000_003,
    ]
    await hub_session.disconnect()


@pytest.mark.parametrize(
    ("error", "raised", "retry_after_s"),
    [
        (binary.AnswerError(ErrorKind.SYNCING, "unproven", 5_000), Syncing, 5.0),
        (binary.AnswerError(ErrorKind.NOT_READY, "clock", 250), NotReady, 0.25),
        (binary.AnswerError(ErrorKind.NO_PUBLISHER, "chart", 1_000), NoPublisher, 1.0),
        (binary.AnswerError(ErrorKind.INVALID_WINDOW, "future"), InvalidWindow, None),
        (binary.AnswerError(ErrorKind.OVERFLOW, "full"), Overflow, None),
        (binary.AnswerError(ErrorKind.EA_GONE, "left"), EaGone, None),
        (binary.AnswerError(ErrorKind.TIMEOUT, "late"), HubTimeout, None),
        (binary.AnswerError(ErrorKind.EA_ERROR, ""), EaError, None),
    ],
    ids=lambda value: value.__name__ if isinstance(value, type) else "",
)
async def test_each_error_kind_raises_its_own_error(hub, error, raised, retry_after_s):
    hub_session = session(hub, Owner())
    await hub_session.connect()

    read = asyncio.create_task(hub_session.symbol_info("EURUSD"))
    (request,) = await asked(hub)
    await hub.fail(request, error, last_error=4301)

    with pytest.raises(raised, match=f"^symbol_info: {error.message}$") as failed:
        await asyncio.wait_for(read, 1)
    assert failed.value.last_error == 4301
    assert getattr(failed.value, "retry_after_s", None) == retry_after_s
    await hub_session.disconnect()


async def test_deferring_asks_again_after_each_deferring_answer_until_it_is_answered(hub):
    hub_session = session(hub, Owner())
    await hub_session.connect()

    read = asyncio.create_task(hub_session.deferring(Call.SYMBOLS_TOTAL))
    for count, kind in enumerate(
        (ErrorKind.NOT_READY, ErrorKind.NO_PUBLISHER, ErrorKind.SYNCING), start=1
    ):
        requests = await asked(hub, count)
        await hub.fail(requests[-1], binary.AnswerError(kind, "", 20))
    requests = await asked(hub, 4)
    await hub.answer(requests[-1], 7)

    assert (await asyncio.wait_for(read, 1)).result == 7
    await hub_session.disconnect()


async def test_deferring_raises_an_answer_that_does_not_defer(hub):
    hub_session = session(hub, Owner())
    await hub_session.connect()

    read = asyncio.create_task(hub_session.deferring(Call.SYMBOLS_TOTAL))
    (request,) = await asked(hub)
    await hub.fail(request, binary.AnswerError(ErrorKind.EA_GONE, "left"))

    with pytest.raises(EaGone):
        await asyncio.wait_for(read, 1)
    await hub_session.disconnect()


async def test_deferring_stops_when_its_caller_cancels(hub):
    hub_session = session(hub, Owner())
    await hub_session.connect()

    read = asyncio.create_task(hub_session.deferring(Call.SYMBOLS_TOTAL))
    (request,) = await asked(hub)
    await hub.fail(request, binary.AnswerError(ErrorKind.NOT_READY, "", 60_000))
    await asyncio.sleep(0.05)
    read.cancel()

    with pytest.raises(asyncio.CancelledError):
        await read
    assert len(hub.requests()) == 1
    await hub_session.disconnect()


async def test_a_call_unanswered_past_the_sessions_deadline_raises_and_a_late_answer_is_dropped(
    hub, monkeypatch
):
    monkeypatch.setattr(hub_module, "_DEADLINES", {calls.Wait.READ: 0.1})
    monkeypatch.setattr(hub_module, "_MARGIN_S", 0.05)
    owner = Owner()
    hub_session = session(hub, owner)
    await hub_session.connect()
    loop = asyncio.get_running_loop()

    started = loop.time()
    read = asyncio.create_task(hub_session.symbols_total())
    (request,) = await asked(hub)
    with pytest.raises(ResponseLost):
        await asyncio.wait_for(read, 1)
    waited = loop.time() - started
    await hub.answer(request, 1)
    await eventually(lambda: owner.log.warning.called)

    assert 0.15 <= waited < 0.3
    assert owner.log.warning.call_args.args[0] == (
        f"hub session: dropping an answer to request {request.request_id}"
    )
    await hub_session.disconnect()


async def test_a_busy_moves_the_sessions_deadline_to_its_bound(hub, monkeypatch):
    monkeypatch.setattr(hub_module, "_DEADLINES", {calls.Wait.HISTORY: 0.1})
    monkeypatch.setattr(hub_module, "_MARGIN_S", 0.05)
    monkeypatch.setattr(calls, "BUSY_MARGIN_S", 0.1)
    hub_session = session(hub, Owner())
    await hub_session.connect()

    read = asyncio.create_task(hub_session.copy_ticks_from("EURUSD", 1, 1, 1))
    (request,) = await asked(hub)
    await hub.push(binary.Busy(request.request_id, 1))
    await asyncio.sleep(0.5)
    await hub.answer(request, np.zeros(0, dtype=_TICKS))

    assert len((await asyncio.wait_for(read, 1)).result) == 0
    await hub_session.disconnect()


async def test_a_call_in_flight_when_the_connection_drops_fails_at_once(hub):
    hub_session = session(hub, Owner())
    await hub_session.connect()

    read = asyncio.create_task(hub_session.account_info())
    await asked(hub)
    await hub.connections[0].close()

    with pytest.raises(ResponseLost, match="the hub connection was lost"):
        await asyncio.wait_for(read, 2)
    await hub_session.disconnect()


async def test_arguments_a_call_does_not_take_are_refused_before_anything_is_sent(hub):
    hub_session = session(hub, Owner())
    await hub_session.connect()

    with pytest.raises(ValueError, match="symbol_info.symbol: 7 is not a string"):
        await hub_session.symbol_info(7)
    with pytest.raises(TypeError):
        await hub_session.symbol_info()
    await asyncio.sleep(0.05)
    assert hub.requests() == []
    await hub_session.disconnect()


# ── Commands ──────────────────────────────────────────────────────────────────


def sent(request_id, retcode=mirror.TRADE_RETCODE_PLACED):
    return zeroed(binary.TRADE_RESULT, retcode=retcode, request_id=request_id)


async def test_an_order_carries_its_whole_request_and_resolves_with_the_result_that_it_was_sent(
    hub,
):
    hub_session = session(hub, Owner())
    await hub_session.connect()

    order = asyncio.create_task(hub_session.order_send({"symbol": "EURUSD", "volume": 0.1}))
    (request,) = await asked(hub)
    await hub.answer(request, sent(41))
    answered = await asyncio.wait_for(order, 1)

    assert calls.decode_arguments(calls.CALLS[Call.ORDER_SEND], request) == {
        "request": calls.trade_request({"symbol": "EURUSD", "volume": 0.1})
    }
    assert (answered.result.retcode, answered.result.request_id) == (10008, 41)
    await hub_session.disconnect()


async def test_a_reconnect_asks_the_outcomes_of_the_commands_no_request_transaction_settled(hub):
    owner = Owner()
    hub_session = session(hub, owner)
    await hub_session.connect()
    for request_id, symbol in ((41, "EURUSD"), (42, "EURUSD"), (43, "XAUUSD"), (44, "EURUSD")):
        order = asyncio.create_task(hub_session.order_send({"symbol": symbol}))
        requests = await asked(hub, request_id - 40)
        await hub.answer(requests[-1], sent(request_id))
        await asyncio.wait_for(order, 1)
    settled = binary.TradeTransaction(
        zeroed(binary.TRADE_TRANSACTION, type=TransactionType.REQUEST),
        zeroed(binary.TRADE_REQUEST, symbol="EURUSD"),
        zeroed(binary.TRADE_RESULT, retcode=10009, request_id=42),
    )
    await hub.push(settled)
    await eventually(lambda: owner.frames)

    await hub.connections[0].close()
    outcomes = await asked(hub, 2, connection=1)
    by_symbol = {}
    asked_ids = {}
    for request in outcomes:
        arguments = calls.decode_arguments(calls.CALLS[Call.OUTCOMES], request)
        by_symbol[arguments["symbol"]] = request
        asked_ids[arguments["symbol"]] = arguments["request_ids"]
    await hub.answer(by_symbol["EURUSD"], [sent(41, 10009)])
    await hub.answer(by_symbol["XAUUSD"], [])
    await eventually(lambda: len(owner.frames) == 3)

    assert asked_ids == {"EURUSD": (41, 44), "XAUUSD": (43,)}
    handed = sorted(owner.frames[1:], key=lambda frame: frame.symbol)
    assert [(frame.symbol, [r.request_id for r in frame.results]) for frame in handed] == [
        ("EURUSD", [41]),
        ("XAUUSD", []),
    ]
    assert all(isinstance(frame, Outcomes) for frame in handed)
    await hub_session.disconnect()


async def test_a_resend_a_reconnect_overtakes_does_not_open_the_next_connection_to_requests():
    owner = Owner()
    hub_session = HubSession(config(1), asyncio.get_running_loop(), owner.on_frame, None, owner.log)
    await hub_session.subscribe(Subscription(Stream.TICKS, "EURUSD"))
    socket = HeldSocket(lambda frame: isinstance(frame, binary.Subscribe))
    hub_session._ws = socket

    hub_session._on_reconnected()
    await socket.holding.wait()
    hub_session._reconnects += 1
    hub_session._greeting.clear()
    socket.released.set()
    await asyncio.gather(*hub_session._tasks)

    assert not hub_session._greeting.is_set()


async def test_an_answer_of_one_record_in_two_parts_fails_its_call():
    hub_double = HubDouble()
    server = await serve(hub_double.handler, "127.0.0.1", 0)
    hub_double.port = server.sockets[0].getsockname()[1]
    hub_session = session(hub_double, Owner())
    await hub_session.connect()

    total = asyncio.create_task(hub_session.symbols_total())
    (request,) = await asked(hub_double)
    await hub_double.answer(request, 1, last=False)
    await hub_double.answer(request, 2, part=1)

    with pytest.raises(ServerUnreachable, match="symbols_total: a integer answer in 2 parts"):
        await asyncio.wait_for(total, 1)
    await hub_session.disconnect()
    server.close()


@pytest.mark.parametrize("call", [Call.COPY_RATES_RANGE, Call.COPY_RATES_FROM])
async def test_a_bar_read_of_a_timeframe_no_series_serves_is_refused_before_it_is_sent(hub, call):
    hub_session = session(hub, Owner())
    await hub_session.connect()
    if call is Call.COPY_RATES_RANGE:
        arguments = {"date_from": 1, "date_to": 2}
    else:
        arguments = {"date_from": 1, "count": 2}

    with pytest.raises(ValueError, match="timeframe 999 has no history series"):
        await asyncio.wait_for(
            hub_session.request(call, symbol="EURUSD", timeframe=999, **arguments), 1
        )
    await asyncio.sleep(0.05)
    assert hub.requests() == []
    await hub_session.disconnect()


def raw_header(kind, payload=b"", *, version=2, flags=0, correlation=0, count=0):
    return binary.HEADER.pack(version, kind, flags, correlation, count, len(payload)) + payload


async def test_an_answer_whose_payload_runs_short_fails_its_request_at_once_and_no_other(hub):
    owner = Owner()
    hub_session = session(hub, owner)
    await hub_session.connect()
    loop = asyncio.get_running_loop()
    total = asyncio.create_task(hub_session.symbols_total())
    tick_read = asyncio.create_task(hub_session.symbol_info_tick("EURUSD"))
    first, second = await asked(hub, 2)

    started = loop.time()
    # An answer's payload opens with its part and last_error, eight bytes; this one carries three.
    await hub.connections[-1].send(
        raw_header(binary.FrameKind.ANSWER, b"\x00\x00\x00", flags=3, correlation=first.request_id)
    )

    with pytest.raises(ProtocolError, match="answer frame: offset 20: part runs past the payload"):
        await asyncio.wait_for(total, 1)
    assert loop.time() - started < 0.5
    assert not tick_read.done()
    await hub.answer(second, tick())
    assert (await asyncio.wait_for(tick_read, 1)).result == calls.Tick(**tick())
    assert owner.log.error.call_args.args[0].startswith("hub session: a frame it cannot decode")
    await hub_session.disconnect()


@pytest.mark.parametrize(
    "raw",
    [b"\x02" * 10, raw_header(binary.FrameKind.ACK, version=1)],
    ids=["ten-bytes", "version-1"],
)
async def test_a_frame_whose_header_does_not_decode_fails_every_request_in_flight(hub, raw):
    owner = Owner()
    hub_session = session(hub, owner)
    await hub_session.connect()
    total = asyncio.create_task(hub_session.symbols_total())
    tick_read = asyncio.create_task(hub_session.symbol_info_tick("EURUSD"))
    await asked(hub, 2)

    await hub.connections[-1].send(raw)

    for read in (total, tick_read):
        with pytest.raises(ProtocolError, match="offset 0: "):
            await asyncio.wait_for(read, 1)
    assert len(owner.log.error.call_args_list) == 1
    await hub_session.disconnect()


async def test_an_undecodable_stream_frame_fails_nothing_and_the_session_answers_on(hub):
    owner = Owner()
    hub_session = session(hub, owner)
    await hub_session.connect()
    total = asyncio.create_task(hub_session.symbols_total())
    (request,) = await asked(hub)

    # A tick frame counting one tick and carrying none.
    await hub.connections[-1].send(raw_header(binary.FrameKind.TICK, b"\x01\x00X", count=1))
    await hub.answer(request, 3)

    assert (await asyncio.wait_for(total, 1)).result == 3
    assert owner.frames == []
    assert owner.log.error.call_args.args[0].startswith("hub session: a frame it cannot decode")
    await hub_session.disconnect()
