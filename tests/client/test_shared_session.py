"""A node's one hub session: the data client, the instrument provider and the execution client of
one connection reading and subscribing over it, its frames and reconnects handed to the client each
concerns, and the trade transactions it holds while the execution client is away, against a hub
double on a real socket."""

import asyncio
import logging
from dataclasses import dataclass

import numpy as np
import pandas as pd
import pytest
from exec_harness import STRATEGY_ID, TRADER_ID, RecordedExecClient, ours_deal
from hub_double import HubDouble, eventually, zeroed
from nautilus_trader.common.component import LiveClock, MessageBus
from nautilus_trader.common.factories import OrderFactory
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.data.engine import DataEngine
from nautilus_trader.data.messages import RequestBars, SubscribeBars, SubscribeQuoteTicks
from nautilus_trader.execution.messages import SubmitOrder
from nautilus_trader.model.data import BarType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.identifiers import ClientId, InstrumentId
from nautilus_trader.model.objects import Price, Quantity
from nautilus_trader.portfolio.portfolio import Portfolio
from nautilus_trader.test_kit.stubs.component import TestComponentStubs
from session_double import deal_added, verdict_of
from venue_doubles import (
    account_info,
    listing,
    relayed_sessions,
    symbol_info,
    symbol_margin_rates,
    tick,
    trade_result,
)
from websockets.asyncio.server import serve

from mt5connector.client import execution
from mt5connector.client import hub as hub_module
from mt5connector.client.config import MT5Config
from mt5connector.client.connection import MT5Connection
from mt5connector.client.constants import MT5_VENUE
from mt5connector.client.data import MT5DataClient
from mt5connector.client.errors import EaError, ProtocolError, ResponseLost
from mt5connector.client.events import DealsRecovery, deals_topic, recovery_topic
from mt5connector.client.execution import MT5LiveExecutionClient, SendOutcome
from mt5connector.client.parsing import venue_series
from mt5connector.client.providers import MT5InstrumentProvider
from mt5connector.wire import binary, calls, mirror
from mt5connector.wire.binary import ErrorKind
from mt5connector.wire.history_wire import BARS
from mt5connector.wire.push_wire import Stream, Subscription

Call = calls.Call
EURUSD = InstrumentId.from_str("EURUSD.MT5")
EURUSD_ONLY = InstrumentProviderConfig(load_ids=frozenset({EURUSD}))
MINUTE_BARS = BarType.from_str("EURUSD.MT5-1-MINUTE-BID-EXTERNAL")
TICKS = Subscription(Stream.TICKS, "EURUSD")
TRANSACTIONS = Subscription(Stream.TRADE_TRANSACTIONS)
MINUTES = Subscription(Stream.BARS, "EURUSD", venue_series(MINUTE_BARS))
STOP_REQUEST = 41
STOP_TICKET = 9401
NOW_S = 1_760_000_000
CONNECTION_LOG = "mt5connector.client.connection"


class NodeHub(HubDouble):
    """A hub serving one logged-in hedging account trading EURUSD, each request answered by its call
    and arguments: a call in `refused` with that error, one in `garbled` with an answer no session
    can decode, one in `unanswered` never, and one in `withheld` only once its event is set."""

    def __init__(self) -> None:
        super().__init__()
        self.deals: dict[int, object] = {}
        self.outcomes: list[binary.TradeTransaction] = []
        self.order_results: list = []
        self.refused: dict[Call, binary.AnswerError] = {}
        self.garbled: set[Call] = set()
        self.unanswered: set[Call] = set()
        self.withheld: dict[Call, asyncio.Event] = {}

    def answer_for(self, call: Call, arguments: dict) -> object:
        if call is Call.ACCOUNT_INFO:
            return account_info()._asdict()
        elif call is Call.TERMINAL_INFO:
            return zeroed(calls.CALLS[call].layout, trade_allowed=True, connected=True)
        elif call is Call.SYMBOLS_GET:
            return [listing(symbol_info())._asdict()]
        elif call is Call.SYMBOL_INFO:
            return symbol_info()._asdict()
        elif call is Call.SYMBOL_INFO_TICK:
            return tick(bid=1.085, ask=1.0852)._asdict()
        elif call is Call.COMMISSION_SCHEDULE:
            return {"ret": 0, "last_error": 0, "rules": []}
        elif call is Call.TRADING_SESSIONS:
            return relayed_sessions()
        elif call is Call.SYMBOL_MARGIN_RATE:
            return symbol_margin_rates()._asdict()
        elif call in (Call.ORDERS_GET, Call.POSITIONS_GET, Call.HISTORY_ORDERS_GET):
            return []
        elif call is Call.HISTORY_DEALS_GET:
            ticket = arguments.get("ticket")
            if ticket in self.deals:
                return [self.deals[ticket]._asdict()]
            else:
                return []
        elif call is Call.ORDER_SEND:
            return self.order_results.pop(0)._asdict()
        elif call is Call.OUTCOMES:
            return list(self.outcomes)
        elif call is Call.HISTORY_DEALS_RECOVER:
            return [ours_deal(ticket=8771, position_id=arguments["position"])._asdict()]
        elif call is Call.RANGES:
            return calls.Ranges(100_000, {})
        elif call is Call.COPY_RATES_RANGE:
            return np.array(
                [(NOW_S - 120, 1.085, 1.09, 1.08, 1.088, 10, 2, 0, NOW_S - 60)],
                dtype=np.dtype(list(BARS.dtype)),
            )
        else:
            raise AssertionError(f"no answer for {call.name}")

    async def handler(self, ws):
        self.connections.append(ws)
        frames = []
        self.received.append(frames)
        try:
            async for raw in ws:
                if not frames:
                    frame = binary.decode_hello(raw)
                else:
                    frame = binary.decode(raw, binary.Sender.ADAPTER)
                frames.append(frame)
                if isinstance(frame, binary.Subscribe | binary.Unsubscribe):
                    await ws.send(binary.encode(binary.Ack(frame.op_id)))
                elif isinstance(frame, binary.Request):
                    asyncio.get_running_loop().create_task(self._served(ws, frame))
        except Exception:  # noqa: BLE001 — a dropped socket ends what it carried
            pass

    async def _served(self, ws, request: binary.Request) -> None:
        call = Call(request.call)
        row = calls.CALLS[call]
        if call in self.unanswered:
            return
        elif call in self.refused:
            answer = binary.Answer(request.request_id, 0, True, 0, error=self.refused[call])
        elif call in self.garbled:
            answer = binary.Answer(request.request_id, 0, True, 0, 1, b"\x00")
        else:
            value = self.answer_for(call, calls.decode_arguments(row, request))
            count, body = calls.write_result(row, value)
            answer = binary.Answer(request.request_id, 0, True, 0, count, body)
        if call in self.withheld:
            await self.withheld[call].wait()
        await ws.send(binary.encode(answer))

    def asked(self, call: Call, connection: int | None = None) -> list[binary.Request]:
        """Every request of the call, over the connection named, else over every one."""
        if connection is None:
            return super().asked(call)
        else:
            return [frame for frame in self.requests(connection) if frame.call == call]

    def subscribed(self, connection: int) -> list[Subscription]:
        return [
            frame.subscription
            for frame in self.received[connection]
            if isinstance(frame, binary.Subscribe)
        ]

    def unsubscribed(self, connection: int) -> list[Subscription]:
        return [
            frame.subscription
            for frame in self.received[connection]
            if isinstance(frame, binary.Unsubscribe)
        ]

    def hellos(self) -> int:
        return sum(1 for frames in self.received if frames)


@pytest.fixture
async def node_hub():
    """A NodeHub on a loopback port, which a test can drop the live connection of, or stop and
    start again on the same port."""
    double = NodeHub()
    double.server = await serve(double.handler, "127.0.0.1", 0)
    double.port = double.server.sockets[0].getsockname()[1]
    yield double
    double.server.close()


async def drop(hub: NodeHub) -> None:
    """The hub drops the live connection and keeps listening: NT's client reconnects."""
    await hub.connections[-1].close()


async def outage(hub: NodeHub) -> None:
    """The hub drops the live connection and stops listening."""
    hub.server.close()
    for ws in list(hub.connections):
        await ws.close()
    await hub.server.wait_closed()


async def back(hub: NodeHub) -> None:
    hub.server = await serve(hub.handler, "127.0.0.1", hub.port)


def _config(port: int) -> MT5Config:
    return MT5Config(
        account=12345678,
        server="Broker-Demo",
        ws_url=f"ws://127.0.0.1:{port}",
        reconnect_initial_delay_s=0.1,
        reconnect_max_delay_s=0.2,
    )


@dataclass
class Node:
    """One connection and the data client, provider and execution client sharing it, over NT's
    data engine and portfolio; what the execution client emits is in `ledger`."""

    hub: NodeHub
    conn: MT5Connection
    data: MT5DataClient
    exec: MT5LiveExecutionClient
    cache: object
    ledger: list
    factory: OrderFactory

    def names(self) -> list[str]:
        return [type(item).__name__ for item in self.ledger]

    def quotes(self) -> int:
        return len(self.cache.quote_ticks(EURUSD))

    async def submit(self, order, *, request_id: int, ticket: int) -> None:
        self.hub.order_results.append(trade_result(request_id=request_id, order=ticket))
        self.cache.add_order(order)
        await self.exec._submit_order(
            SubmitOrder(
                trader_id=TRADER_ID,
                strategy_id=STRATEGY_ID,
                order=order,
                command_id=UUID4(),
                ts_init=0,
            )
        )

    async def stop_placed(self):
        """A buy stop of EURUSD the terminal answered PLACED with request 41."""
        order = self.factory.stop_market(
            EURUSD, OrderSide.BUY, Quantity.from_str("0.01"), Price.from_str("1.09000")
        )
        await self.submit(order, request_id=STOP_REQUEST, ticket=STOP_TICKET)
        return order

    def last_request(self) -> dict:
        sent = self.hub.asked(Call.ORDER_SEND)[-1]
        return calls.decode_arguments(calls.CALLS[Call.ORDER_SEND], sent)["request"]

    def refusal(self) -> binary.TradeTransaction:
        """The terminal's refusal of the stop sent under request 41."""
        return verdict_of(
            self.last_request(),
            STOP_REQUEST,
            mirror.TRADE_RETCODE_INVALID_PRICE,
            order=STOP_TICKET,
        )

    async def filled_market(self, *, request_id: int = 7, ticket: int = 9001) -> None:
        """A market buy sent, done and filled by its deal."""
        order = self.factory.market(EURUSD, OrderSide.BUY, Quantity.from_str("0.01"))
        await self.submit(order, request_id=request_id, ticket=ticket)
        await self.hub.push(
            verdict_of(
                self.last_request(),
                request_id,
                mirror.TRADE_RETCODE_DONE,
                order=ticket,
                deal=ticket + 1,
            )
        )
        deal = ours_deal(ticket=ticket + 1, order=ticket)
        self.hub.deals[ticket + 1] = deal
        await self.hub.push(deal_added(deal))
        await eventually(lambda: "OrderFilled" in self.names())

    async def ticks(self) -> None:
        """A tick of EURUSD pushed on the live connection."""
        await self.hub.push(
            binary.Ticks(
                "EURUSD",
                np.array(
                    [(NOW_S, 1.085, 1.0852, 0.0, 0, NOW_S * 1000, 6, 0.0)],
                    dtype=np.dtype(list(mirror.TICKS.dtype)),
                ),
            )
        )


async def _node(hub: NodeHub) -> Node:
    """A node connected to `hub` as NT's factories and lifecycle connect one: the connection first,
    then the data client quoting EURUSD, then the execution client."""
    config = _config(hub.port)
    clock = LiveClock()
    msgbus = MessageBus(trader_id=TRADER_ID, clock=clock)
    cache = TestComponentStubs.cache()
    DataEngine(msgbus, cache, clock)
    Portfolio(msgbus, cache, clock)
    ledger: list = []
    msgbus.register(endpoint="ExecEngine.process", handler=ledger.append)
    conn = MT5Connection(config)
    await conn.connect()
    provider = MT5InstrumentProvider(conn, venue=MT5_VENUE, clock=clock, config=EURUSD_ONLY)
    loop = asyncio.get_running_loop()
    data = MT5DataClient(
        loop=loop,
        connection=conn,
        msgbus=msgbus,
        cache=cache,
        clock=clock,
        instrument_provider=provider,
        config=config,
    )
    exec_client = RecordedExecClient(
        loop=loop,
        connection=conn,
        account=conn.account,
        msgbus=msgbus,
        cache=cache,
        clock=clock,
        instrument_provider=provider,
        config=config,
        loads=EURUSD_ONLY,
    )
    await data._connect()
    await data._subscribe_quote_ticks(
        SubscribeQuoteTicks(
            instrument_id=EURUSD,
            client_id=ClientId("MT5"),
            venue=None,
            command_id=UUID4(),
            ts_init=0,
        )
    )
    await exec_client._connect()
    factory = OrderFactory(trader_id=TRADER_ID, strategy_id=STRATEGY_ID, clock=clock)
    return Node(hub, conn, data, exec_client, cache, ledger, factory)


@pytest.fixture
async def node(node_hub):
    built = await _node(node_hub)
    yield built
    if built.exec._account_task is not None:
        await built.exec._disconnect()
    await built.conn.disconnect()


async def _subscribe_minute_bars(node: Node) -> None:
    await node.data._subscribe_bars(
        SubscribeBars(
            bar_type=MINUTE_BARS,
            client_id=ClientId("MT5"),
            venue=None,
            command_id=UUID4(),
            ts_init=0,
        )
    )


def _failed_frames(caplog, kind: str) -> list[logging.LogRecord]:
    return [
        record
        for record in caplog.records
        if record.name == CONNECTION_LOG and f"a {kind} frame failed" in record.getMessage()
    ]


# ── One connection ───────────────────────────────────────────────────────────


async def test_a_node_trading_one_order_rides_one_connection_and_no_client_builds_a_session(
    node_hub, monkeypatch
):
    built: list = []
    real = hub_module.HubSession.__init__

    def counted(self, *args, **kwargs):
        built.append(self)
        real(self, *args, **kwargs)

    monkeypatch.setattr(hub_module.HubSession, "__init__", counted)
    node = await _node(node_hub)
    try:
        await _subscribe_minute_bars(node)
        await node.data._request_bars(
            RequestBars(
                bar_type=MINUTE_BARS,
                start=pd.Timestamp(NOW_S - 300, unit="s", tz="UTC"),
                end=pd.Timestamp(NOW_S, unit="s", tz="UTC"),
                limit=0,
                client_id=ClientId("MT5"),
                venue=None,
                callback=lambda response: None,
                request_id=UUID4(),
                ts_init=0,
                params=None,
            )
        )
        await node.exec._refresh_account()
        await node.filled_market()

        assert len(built) == 1
        assert len(node_hub.connections) == 1
        asked = [Call(frame.call) for frame in node_hub.requests(0)]
        # The login's confirming read opens the connection.
        assert asked[0] is Call.ACCOUNT_INFO
        assert {Call.COPY_RATES_RANGE, Call.ORDER_SEND, Call.HISTORY_DEALS_GET} <= set(asked)
        assert node_hub.subscribed(0) == [TICKS, TRANSACTIONS, MINUTES]
    finally:
        await node.exec._disconnect()
        await node.conn.disconnect()


# ── Frames by kind ───────────────────────────────────────────────────────────


async def test_each_frame_reaches_the_client_it_concerns_and_one_no_client_takes_is_an_error(
    node, caplog
):
    with caplog.at_level(logging.ERROR, logger=CONNECTION_LOG):
        await node.ticks()
        await eventually(lambda: node.quotes() == 1)
        await node.filled_market()

        assert node.quotes() == 1
        assert _failed_frames(caplog, "Ticks") == []
        assert _failed_frames(caplog, "TradeTransaction") == []

        await node.data._disconnect()
        await node.ticks()
        await eventually(lambda: _failed_frames(caplog, "Ticks") != [])

    (failed,) = _failed_frames(caplog, "Ticks")
    assert "no data client takes" in str(failed.exc_info[1])
    assert node.quotes() == 1


# ── An outage before the reconnect ───────────────────────────────────────────


async def test_a_read_and_an_order_sent_in_an_outage_end_at_their_deadlines_and_are_never_resent(
    node, monkeypatch, caplog
):
    ended: list[tuple[Call, str, float]] = []
    loop = asyncio.get_running_loop()
    real_request = node.conn.session.request

    async def timed(call, **arguments):
        issued = loop.time()
        try:
            return await real_request(call, **arguments)
        except Exception as exc:
            ended.append((call, type(exc).__name__, loop.time() - issued))
            raise

    monkeypatch.setattr(node.conn.session, "request", timed)
    classified = []
    real_sent = node.exec._sent

    async def sent(request):
        outcome = await real_sent(request)
        classified.append(outcome.outcome)
        return outcome

    monkeypatch.setattr(node.exec, "_sent", sent)
    order = node.factory.stop_market(
        EURUSD, OrderSide.BUY, Quantity.from_str("0.01"), Price.from_str("1.09000")
    )
    node.cache.add_order(order)
    await outage(node.hub)
    await asyncio.sleep(1.0)
    with caplog.at_level(logging.INFO, logger=CONNECTION_LOG):
        refresh = loop.create_task(node.exec._refresh_account())
        submitting = loop.create_task(
            node.exec._submit_order(
                SubmitOrder(
                    trader_id=TRADER_ID,
                    strategy_id=STRATEGY_ID,
                    order=order,
                    command_id=UUID4(),
                    ts_init=0,
                )
            )
        )
        with pytest.raises(ResponseLost):
            await refresh
        await submitting

    # A command's deadline is 1 s and a read's 5 s, each with the session's 5 s margin.
    [(name, after)] = [(name, after) for call, name, after in ended if call is Call.ORDER_SEND]
    assert name == "ResponseLost" and 5.5 < after < 7.5
    reads = [(name, after) for call, name, after in ended if call is Call.ACCOUNT_INFO]
    assert reads != []
    assert all(name == "ResponseLost" and 9.5 < after < 11.5 for name, after in reads)
    assert any("a frame not sent" in record.getMessage() for record in caplog.records)
    assert classified == [SendOutcome.LOST]
    assert node.exec._account_owed
    # Left to reconciliation: neither accepted nor rejected.
    assert [name for name in node.names() if name.startswith("Order")] == ["OrderSubmitted"]

    due_before = node.exec._account_due_ns
    await back(node.hub)
    await eventually(lambda: len(node.hub.connections) >= 2, timeout=10)
    # A refresh clears the debt as it starts; its next due time moves only once it lands.
    await eventually(
        lambda: node.exec._account_due_ns > due_before and not node.exec._account_owed,
        timeout=15,
    )
    reconnected = range(1, len(node.hub.connections))
    assert any(node.hub.asked(Call.ACCOUNT_INFO, connection=i) != [] for i in reconnected)
    assert node.hub.asked(Call.ORDER_SEND) == []


# ── A reconnect ──────────────────────────────────────────────────────────────


async def test_one_reconnect_resends_the_wanted_set_starts_the_bar_recovery_and_asks_outcomes_once(
    node,
):
    await _subscribe_minute_bars(node)
    await node.stop_placed()
    node.hub.unanswered.add(Call.ACCOUNT_INFO)
    loop = asyncio.get_running_loop()
    refresh = loop.create_task(node.exec._refresh_account())
    await eventually(lambda: node.hub.asked(Call.ACCOUNT_INFO, connection=0)[-1:] != [])

    await drop(node.hub)
    await eventually(lambda: len(node.hub.connections) == 2, timeout=10)
    await eventually(lambda: node.hub.asked(Call.OUTCOMES, connection=1) != [], timeout=10)

    with pytest.raises(ResponseLost):
        await refresh
    assert node.exec._account_owed
    assert node.hub.hellos() == 2
    assert sorted(node.hub.subscribed(1), key=str) == sorted(
        [TICKS, TRANSACTIONS, MINUTES], key=str
    )
    assert MINUTE_BARS in node.data._recoveries
    (outcomes,) = node.hub.asked(Call.OUTCOMES, connection=1)
    asked = calls.decode_arguments(calls.CALLS[Call.OUTCOMES], outcomes)
    assert asked["request_ids"] == (STOP_REQUEST,)

    node.hub.unanswered.discard(Call.ACCOUNT_INFO)
    await eventually(lambda: not node.exec._account_owed, timeout=10)


# ── The execution client away ────────────────────────────────────────────────


async def test_an_execution_client_attached_again_asks_the_outcome_it_missed_and_takes_its_refusal(
    node,
):
    await node.stop_placed()
    await node.exec._disconnect()
    node.hub.outcomes = [node.refusal()]
    await node.ticks()
    await eventually(lambda: node.quotes() == 1)

    await node.exec._connect()
    await eventually(lambda: "OrderRejected" in node.names())

    (outcomes,) = node.hub.asked(Call.OUTCOMES)
    asked = calls.decode_arguments(calls.CALLS[Call.OUTCOMES], outcomes)
    assert asked["request_ids"] == (STOP_REQUEST,)
    await node.ticks()
    await eventually(lambda: node.quotes() == 2)
    assert len(node.hub.connections) == 1


async def test_an_outcome_recovered_while_the_execution_client_is_away_is_held_and_handed_first(
    node,
):
    await node.stop_placed()
    await node.exec._disconnect()
    node.hub.outcomes = [node.refusal()]

    await drop(node.hub)
    await eventually(lambda: len(node.hub.connections) == 2, timeout=10)
    await eventually(lambda: node.conn._held_trades != [], timeout=10)
    await node.ticks()
    await eventually(lambda: node.quotes() == 1)
    assert "OrderRejected" not in node.names()

    await node.exec._connect()

    # Handed at the attach itself, before the connect asks anything more.
    assert "OrderRejected" in node.names()
    assert node.conn._held_trades == []
    assert node.hub.asked(Call.OUTCOMES, connection=1)[1:] == []
    await node.ticks()
    await eventually(lambda: node.quotes() == 2)


# ── Each caller's failures ───────────────────────────────────────────────────


async def test_each_failure_reaches_its_own_caller_on_the_shared_session(node, monkeypatch, caplog):
    monkeypatch.setattr(hub_module, "SESSION_MARGIN_S", 0.2)
    monkeypatch.setattr(hub_module, "_DEADLINES", {**hub_module._DEADLINES, calls.Wait.READ: 0.3})
    node.hub.unanswered.add(Call.ACCOUNT_INFO)
    with pytest.raises(ResponseLost):
        await node.conn.account_info()
    node.hub.unanswered.discard(Call.ACCOUNT_INFO)

    node.hub.refused[Call.SYMBOL_INFO] = binary.AnswerError(ErrorKind.EA_ERROR, "no such symbol")
    with pytest.raises(EaError, match="no such symbol"):
        await node.exec._provider.load_symbol("EURUSD")
    del node.hub.refused[Call.SYMBOL_INFO]

    node.hub.garbled.add(Call.HISTORY_DEALS_GET)
    deals_answer, account_answer = asyncio.Event(), asyncio.Event()
    node.hub.withheld[Call.HISTORY_DEALS_GET] = deals_answer
    node.hub.withheld[Call.ACCOUNT_INFO] = account_answer
    loop = asyncio.get_running_loop()
    reads_before = len(node.hub.asked(Call.ACCOUNT_INFO))
    deals = loop.create_task(node.conn.session.request(Call.HISTORY_DEALS_GET, ticket=1))
    account = loop.create_task(node.conn.account_info())
    await eventually(
        lambda: node.hub.asked(Call.HISTORY_DEALS_GET) != []
        and len(node.hub.asked(Call.ACCOUNT_INFO)) > reads_before
    )
    deals_answer.set()
    with pytest.raises(ProtocolError):
        await deals
    assert not account.done()
    account_answer.set()
    assert (await account).login == account_info().login
    del node.hub.withheld[Call.HISTORY_DEALS_GET]
    del node.hub.withheld[Call.ACCOUNT_INFO]
    node.hub.garbled.discard(Call.HISTORY_DEALS_GET)

    late = asyncio.Event()
    node.hub.withheld[Call.ACCOUNT_INFO] = late
    with caplog.at_level(logging.WARNING, logger=CONNECTION_LOG):
        with pytest.raises(ResponseLost):
            await node.conn.account_info()
        late.set()
        await eventually(
            lambda: any("dropping an answer" in r.getMessage() for r in caplog.records)
        )
    del node.hub.withheld[Call.ACCOUNT_INFO]
    assert (await node.conn.account_info()).login == account_info().login
    assert [r.getMessage() for r in caplog.records].count(
        next(r.getMessage() for r in caplog.records if "dropping an answer" in r.getMessage())
    ) == 1


# ── Stopping a client ────────────────────────────────────────────────────────


async def test_stopping_either_client_leaves_the_session_and_the_others_streams(node):
    await _subscribe_minute_bars(node)
    await node.data._disconnect()

    await eventually(lambda: len(node.hub.unsubscribed(0)) == 2)
    assert sorted(node.hub.unsubscribed(0), key=str) == sorted([TICKS, MINUTES], key=str)
    assert node.conn.session.connected
    assert node.conn.session._wanted == [TRANSACTIONS]

    await node.data._connect()
    await node.exec._disconnect()
    await node.ticks()
    await eventually(lambda: node.quotes() == 1)
    assert node.conn.session._wanted == [TICKS, MINUTES]
    assert len(node.hub.connections) == 1


# ── A session that never reconnects ──────────────────────────────────────────


async def test_a_session_built_not_to_reconnect_never_dials_again_and_fails_its_read(
    node_hub, monkeypatch
):
    monkeypatch.setattr(hub_module, "SESSION_MARGIN_S", 0.5)
    monkeypatch.setattr(hub_module, "_DEADLINES", {**hub_module._DEADLINES, calls.Wait.READ: 1.0})
    conn = MT5Connection(_config(node_hub.port), reconnects=False)
    await conn.connect()
    node_hub.unanswered.add(Call.ACCOUNT_INFO)
    reading = asyncio.get_running_loop().create_task(conn.account_info())
    await eventually(lambda: len(node_hub.asked(Call.ACCOUNT_INFO)) == 2)

    await drop(node_hub)

    with pytest.raises(ResponseLost):
        await reading
    # Many reconnect delays later, still the one socket and its one hello.
    await asyncio.sleep(1.0)
    assert len(node_hub.connections) == 1
    assert node_hub.hellos() == 1
    await conn.disconnect()


async def test_a_connect_cancelled_during_its_confirming_read_leaves_no_socket_open(node_hub):
    node_hub.unanswered.add(Call.ACCOUNT_INFO)
    conn = MT5Connection(_config(node_hub.port), reconnects=False)
    connecting = asyncio.get_running_loop().create_task(conn.connect())
    await eventually(lambda: node_hub.asked(Call.ACCOUNT_INFO) != [])

    connecting.cancel()
    with pytest.raises(asyncio.CancelledError):
        await connecting

    await eventually(lambda: node_hub.connections[0].state.name == "CLOSED")
    assert len(node_hub.connections) == 1


async def test_the_nodes_session_reconnects(node):
    await drop(node.hub)

    await eventually(lambda: len(node.hub.connections) == 2, timeout=10)
    await eventually(lambda: node.hub.subscribed(1) != [], timeout=10)


# ── A withheld answer delays nothing else ────────────────────────────────────


async def test_an_account_answer_withheld_at_the_transport_delays_no_order_and_no_fill(node):
    released = asyncio.Event()
    node.hub.withheld[Call.ACCOUNT_INFO] = released
    loop = asyncio.get_running_loop()
    loop.call_later(5.0, released.set)
    started = loop.time()
    refresh = loop.create_task(node.exec._refresh_account())
    await eventually(lambda: node.hub.asked(Call.ACCOUNT_INFO)[-1:] != [])

    await node.filled_market()

    filled_at = loop.time() - started
    assert not refresh.done()
    assert node.names() == ["OrderSubmitted", "OrderAccepted", "OrderFilled"]
    await refresh
    assert filled_at < 5.0 <= loop.time() - started
    assert not node.exec._account_owed


# ── A recovery past its deadline ─────────────────────────────────────────────


async def test_a_recovery_past_its_deadline_is_logged_once_and_one_asked_30_s_later_publishes(
    node,
):
    def not_recovered() -> list[str]:
        return [
            call.args[0]
            for call in node.exec.recorded_log.warning.call_args_list
            if "not recovered" in call.args[0]
        ]

    figures: list = []
    node.exec._msgbus.subscribe(topic=deals_topic(node.exec.venue), handler=figures.append)
    late = asyncio.Event()
    node.hub.withheld[Call.HISTORY_DEALS_RECOVER] = late
    loop = asyncio.get_running_loop()
    started = loop.time()

    node.exec._msgbus.publish(
        topic=recovery_topic(node.exec.venue), msg=DealsRecovery(position=771, symbol="EURUSD")
    )
    await eventually(lambda: not_recovered() != [], timeout=15)

    assert execution._RECOVERY_DEADLINE_S == 10
    assert 9.5 < loop.time() - started < 11
    late.set()
    del node.hub.withheld[Call.HISTORY_DEALS_RECOVER]
    await asyncio.sleep(30)
    node.exec._msgbus.publish(
        topic=recovery_topic(node.exec.venue), msg=DealsRecovery(position=771, symbol="EURUSD")
    )
    await eventually(lambda: figures != [])

    assert [figure.ticket for figure in figures] == [8771]
    assert not_recovered() == ["position 771: its deals were not recovered: TimeoutError()"]
