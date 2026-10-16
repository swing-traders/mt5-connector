"""The hub and its EAs in a running image, through the client's hub session; runs only when
MT5_LIVE_HUB_URL names the hub and MT5_LIVE_HUB_SYMBOL a symbol its account trades."""

import asyncio
import logging
import os
import random
import time

import pytest

from mt5connector.client.config import MT5Config
from mt5connector.client.errors import NoPublisher
from mt5connector.client.hub import Answered, HubSession
from mt5connector.wire import binary, calls, mirror
from mt5connector.wire.history_wire import Series
from mt5connector.wire.push_wire import Stream, Subscription, TransactionType

HUB_URL = os.environ.get("MT5_LIVE_HUB_URL")
SYMBOL = os.environ.get("MT5_LIVE_HUB_SYMBOL")

pytestmark = pytest.mark.skipif(
    HUB_URL is None or SYMBOL is None,
    reason="MT5_LIVE_HUB_URL and MT5_LIVE_HUB_SYMBOL are not both set",
)

Call = calls.Call
# How long a call may stay deferred while the clock verifies or a chart opens.
ANSWERED_S = 120
# The magic every order of this tier carries, by which a resting one is found and removed.
MAGIC = 7_114_001
DAY_S = 86_400
HOUR_S = 3_600
# The account history a deal read by ticket finds an order's deals in.
HISTORY_DAYS = 90
_SETTLED = frozenset(
    {mirror.TRADE_RETCODE_PLACED, mirror.TRADE_RETCODE_DONE, mirror.TRADE_RETCODE_DONE_PARTIAL}
)

logger = logging.getLogger(__name__)


class Log:
    """The session's logger: NT's Logger methods onto the test's logging."""

    def debug(self, message: str) -> None:
        logger.debug(message)

    def info(self, message: str) -> None:
        logger.info(message)

    def warning(self, message: str) -> None:
        logger.warning(message)

    def error(self, message: str) -> None:
        logger.error(message)

    def exception(self, message: str, failure: BaseException) -> None:
        logger.error("%s: %r", message, failure)


class Owner:
    """What a session hands its owner, in order."""

    def __init__(self) -> None:
        self.frames: list = []

    def on_frame(self, frame) -> None:
        self.frames.append(frame)

    async def first(self, matches, timeout_s: float):
        """The first frame handed that matches, waiting at most `timeout_s` for one."""
        deadline = time.monotonic() + timeout_s
        while True:
            found = [frame for frame in self.frames if matches(frame)]
            if found:
                return found[0]
            elif time.monotonic() > deadline:
                raise AssertionError(f"no matching frame within {timeout_s} s")
            await asyncio.sleep(0.1)


def config() -> MT5Config:
    # The session reads the hub's URL and its reconnect delays alone; the account is the image's.
    return MT5Config(
        account=1,
        password="unused",
        server="unused",
        server_url="http://127.0.0.1",
        ws_url=HUB_URL,
    )


async def connected(owner: Owner) -> HubSession:
    session = HubSession(config(), asyncio.get_running_loop(), owner.on_frame, None, Log())
    await session.connect()
    return session


async def ask(session: HubSession, call: Call, **arguments) -> Answered:
    """A call's answer, asked again on each deferring answer, within ANSWERED_S."""
    return await asyncio.wait_for(session.deferring(call, **arguments), ANSWERED_S)


def is_struct(value, layout: str) -> bool:
    return isinstance(value, calls.STRUCT_TYPES[layout])


def refused_order(info, price: float) -> dict:
    """A buy limit at the smallest volume whose expiration passed an hour ago: the venue refuses
    it, so nothing ever rests."""
    return {
        "action": mirror.TRADE_ACTION_PENDING,
        "symbol": info.name,
        "volume": info.volume_min,
        "type": mirror.ORDER_TYPE_BUY_LIMIT,
        "price": round(price, info.digits),
        "type_time": mirror.ORDER_TIME_SPECIFIED,
        "expiration": int(time.time()) - HOUR_S,
        "type_filling": mirror.ORDER_FILLING_RETURN,
        "magic": MAGIC,
    }


async def assert_flat(session: HubSession) -> None:
    """Fails when an order of this tier rests, removing it first."""
    resting = [
        order for order in (await ask(session, Call.ORDERS_GET)).result if order.magic == MAGIC
    ]
    for order in resting:
        removal = {
            "action": mirror.TRADE_ACTION_REMOVE,
            "order": order.ticket,
            "symbol": order.symbol,
        }
        await ask(session, Call.ORDER_SEND, request=removal)
    assert resting == []


@pytest.fixture
async def session():
    owner = Owner()
    hub_session = await connected(owner)
    yield hub_session, owner
    await hub_session.disconnect()


async def test_every_spawner_read_answers_as_its_row_types_it(session):
    hub_session, _ = session
    now = int(time.time())

    account = (await ask(hub_session, Call.ACCOUNT_INFO)).result
    terminal = (await ask(hub_session, Call.TERMINAL_INFO)).result
    total = (await ask(hub_session, Call.SYMBOLS_TOTAL)).result
    symbols = (await ask(hub_session, Call.SYMBOLS_GET)).result
    positions = (await ask(hub_session, Call.POSITIONS_GET)).result
    orders = (await ask(hub_session, Call.ORDERS_GET)).result
    window = {"date_from": now - DAY_S, "date_to": now}
    history_orders = (await ask(hub_session, Call.HISTORY_ORDERS_GET, **window)).result
    history_deals = (await ask(hub_session, Call.HISTORY_DEALS_GET, **window)).result

    assert is_struct(account, "AccountInfo") and account.currency != ""
    assert isinstance(account.trade_allowed, bool)
    assert is_struct(terminal, "TerminalInfo") and terminal.connected is True
    assert terminal.maxbars > 0
    assert isinstance(total, int) and total > 0
    assert len(symbols) > 0 and all(is_struct(symbol, "SymbolInfo") for symbol in symbols)
    assert SYMBOL in {symbol.name for symbol in symbols}
    assert all(is_struct(position, "TradePosition") for position in positions)
    assert all(is_struct(order, "TradeOrder") for order in orders)
    assert all(is_struct(order, "TradeOrder") for order in history_orders)
    assert all(is_struct(deal, "TradeDeal") for deal in history_deals)
    assert all(now - DAY_S - HOUR_S <= deal.time <= now + HOUR_S for deal in history_deals)


async def test_every_publisher_read_of_the_symbol_answers_as_its_row_types_it(session):
    hub_session, _ = session

    selected = (await ask(hub_session, Call.SYMBOL_SELECT, symbol=SYMBOL, enable=True)).result
    info = (await ask(hub_session, Call.SYMBOL_INFO, symbol=SYMBOL)).result
    tick = (await ask(hub_session, Call.SYMBOL_INFO_TICK, symbol=SYMBOL)).result

    assert selected is True
    assert is_struct(info, "SymbolInfo")
    assert (info.name, info.digits > 0) == (SYMBOL, True)
    assert isinstance(tick, calls.Tick)
    assert abs(tick.time_msc / 1000 - time.time()) < HOUR_S


async def test_a_tick_subscription_delivers_ticks_in_true_utc_within_a_minute(session):
    hub_session, owner = session

    await hub_session.subscribe(Subscription(Stream.TICKS, SYMBOL))
    frame = await owner.first(lambda frame: isinstance(frame, binary.Ticks), 60)
    received = time.time()

    assert frame.symbol == SYMBOL and len(frame.ticks) > 0
    assert abs(max(tick["time_msc"] for tick in frame.ticks) / 1000 - received) < 60


async def test_an_m1_bar_subscription_delivers_a_closed_bar_in_true_utc_within_two_minutes(
    session,
):
    hub_session, owner = session

    await hub_session.subscribe(Subscription(Stream.BARS, SYMBOL, Series.M1))
    frame = await owner.first(lambda frame: isinstance(frame, binary.Bar), 120)
    received = time.time()

    assert (frame.symbol, frame.series) == (SYMBOL, Series.M1)
    assert frame.bar["time_close"] - frame.bar["time"] == 60
    assert abs(received - frame.bar["time_close"]) < 120


async def test_a_refused_order_answers_sent_its_verdict_arrives_and_a_new_connect_replays_it_once(
    session,
):
    watcher, watched = session
    await watcher.subscribe(Subscription(Stream.TRADE_TRANSACTIONS))
    sent = Owner()
    sender = await connected(sent)
    try:
        info = (await ask(sender, Call.SYMBOL_INFO, symbol=SYMBOL)).result
        tick = (await ask(sender, Call.SYMBOL_INFO_TICK, symbol=SYMBOL)).result

        answered = await ask(sender, Call.ORDER_SEND, request=refused_order(info, tick.bid / 2))
        request_id = answered.result.request_id
        assert (answered.result.retcode, request_id > 0) == (mirror.TRADE_RETCODE_PLACED, True)

        def verdict(frame) -> bool:
            return (
                isinstance(frame, binary.TradeTransaction)
                and frame.transaction["type"] == TransactionType.REQUEST
                and frame.result["request_id"] == request_id
            )

        live = await watched.first(verdict, 30)
        await sender.disconnect()
        await sender.connect()
        replayed = await sent.first(verdict, 30)
        await sender.disconnect()
        await sender.connect()
        await asyncio.sleep(5)

        assert live.result["retcode"] not in _SETTLED
        assert replayed == live
        assert [frame for frame in sent.frames if verdict(frame)] == [replayed]
    finally:
        await assert_flat(sender)
        await sender.disconnect()


async def test_a_deal_read_by_ticket_answers_every_deal_of_the_order_the_ticket_names(session):
    hub_session, _ = session
    now = int(time.time())
    window = {"date_from": now - HISTORY_DAYS * DAY_S, "date_to": now}
    deals = (await ask(hub_session, Call.HISTORY_DEALS_GET, **window)).result
    orders = [deal.order for deal in deals if deal.order != 0]
    if not orders:
        pytest.skip(f"the account's last {HISTORY_DAYS} days hold no deal of an order")

    by_ticket = (await ask(hub_session, Call.HISTORY_DEALS_GET, ticket=orders[-1])).result

    assert {deal.ticket for deal in by_ticket} == {
        deal.ticket for deal in deals if deal.order == orders[-1]
    }


async def test_a_command_for_a_symbol_no_publisher_serves_answers_once_its_chart_opened(session):
    hub_session, _ = session
    symbols = (await ask(hub_session, Call.SYMBOLS_GET)).result
    others = [
        symbol
        for symbol in symbols
        if symbol.name != SYMBOL
        and symbol.trade_mode == mirror.SYMBOL_TRADE_MODE_FULL
        and symbol.volume_min > 0
    ]
    random.shuffle(others)
    # A symbol out of Market Watch has no chart, so no publisher; it is asked first.
    others.sort(key=lambda symbol: symbol.select)
    deferred = None
    try:
        # A publisher's symbol answers at once, and only a chart's symbol has one.
        for other in others:
            order = refused_order(other, other.point)
            try:
                await hub_session.order_send(order)
            except NoPublisher as failure:
                deferred = (order, failure.retry_after_s)
                break
        assert deferred is not None, f"each of the {len(others)} symbols asked has a publisher"
        order, retry_after_s = deferred
        await asyncio.sleep(retry_after_s)
        answered = await ask(hub_session, Call.ORDER_SEND, request=order)

        assert is_struct(answered.result, "MqlTradeResult")
        assert answered.result.retcode > 0
    finally:
        await assert_flat(hub_session)
