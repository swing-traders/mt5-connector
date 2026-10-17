"""The hub and its EAs in a running image, through the client's hub session; runs only when
MT5_LIVE_HUB_URL names the hub and MT5_LIVE_HUB_SYMBOL a symbol its account trades, weekends
included."""

import asyncio
import logging
import os
import time

import numpy as np
import pytest

from mt5connector.client.config import MT5Config
from mt5connector.client.errors import InvalidWindow, NoPublisher
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
# How long a history read may stay deferred while its window syncs: a tick read's busy bound.
SYNCED_S = calls.TICK_BUSY_S
# The magic every order of this tier carries, by which a resting one is found and removed.
MAGIC = 7_114_001
DAY_S = 86_400
HOUR_S = 3_600
MINUTE_S = 60
# The account history a deal read by its ticket is picked from.
HISTORY_DAYS = 90
# The longest a publisher's read may take while its symbol's ticks download.
TICK_READ_S = 1.0
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


async def synced(session: HubSession, call: Call, **arguments) -> Answered:
    """A history read's answer, asked again on each deferring answer, within SYNCED_S."""
    return await asyncio.wait_for(session.deferring(call, **arguments), SYNCED_S)


async def m1_bars(session: HubSession, start: int, end: int) -> np.ndarray:
    """The symbol's M1 bars opened in [start, end], asked again while the window syncs."""
    window = {"date_from": start, "date_to": end}
    answered = await synced(
        session, Call.COPY_RATES_RANGE, symbol=SYMBOL, timeframe=mirror.TIMEFRAME_M1, **window
    )
    return answered.result


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
    assert len(symbols) > 0 and all(is_struct(symbol, "SymbolListing") for symbol in symbols)
    listed = {symbol.name: symbol for symbol in symbols}
    assert type(symbols[0])._fields == binary.SYMBOL_LISTING.fields
    assert {"name", "trade_calc_mode", "trade_mode", "currency_base", "currency_profit"} <= set(
        type(symbols[0])._fields
    )
    assert SYMBOL in listed and listed[SYMBOL].currency_profit != ""
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


async def test_a_deal_read_by_its_ticket_answers_that_deal(session):
    hub_session, _ = session
    now = int(time.time())
    window = {"date_from": now - HISTORY_DAYS * DAY_S, "date_to": now}
    deals = (await ask(hub_session, Call.HISTORY_DEALS_GET, **window)).result
    if not deals:
        pytest.skip(f"the account's last {HISTORY_DAYS} days hold no deal")

    by_ticket = (await ask(hub_session, Call.HISTORY_DEALS_GET, ticket=deals[-1].ticket)).result

    assert by_ticket == (deals[-1],)


async def test_a_cold_m1_window_a_week_back_answers_its_bars_and_the_m1_floor(session):
    hub_session, _ = session
    start = int(time.time()) - 7 * DAY_S
    end = start + DAY_S

    bars = await m1_bars(hub_session, start, end)
    ranges = (await ask(hub_session, Call.RANGES, symbol=SYMBOL)).result

    assert len(bars) > 0
    assert np.all(bars["time"] % MINUTE_S == 0)
    assert np.all((bars["time"] >= start) & (bars["time"] <= end))
    assert Series.M1 in ranges.series


async def test_a_window_before_the_m1_floor_answers_empty_and_writes_no_floor(session):
    hub_session, _ = session
    now = int(time.time())
    # The read the floor is measured on, should the EA's not be fresh.
    await m1_bars(hub_session, now - HOUR_S, now)
    floor = (await ask(hub_session, Call.RANGES, symbol=SYMBOL)).result.series[Series.M1]

    bars = await m1_bars(hub_session, floor.floor - 2 * DAY_S, floor.floor - DAY_S)
    ranges = (await ask(hub_session, Call.RANGES, symbol=SYMBOL)).result

    assert len(bars) == 0
    assert ranges.series[Series.M1] == floor


async def test_a_window_starting_after_the_terminals_time_raises_invalid_window(session):
    hub_session, _ = session
    now = int(time.time())

    with pytest.raises(InvalidWindow):
        await synced(
            hub_session,
            Call.COPY_RATES_RANGE,
            symbol=SYMBOL,
            timeframe=mirror.TIMEFRAME_M1,
            date_from=now + DAY_S,
            date_to=now + 2 * DAY_S,
        )


async def test_a_bar_window_one_period_over_the_span_cap_answers_every_bar_once(session):
    hub_session, _ = session
    maxbars = (await ask(hub_session, Call.TERMINAL_INFO)).result.maxbars
    # Ending a whole hour back, so no bar of either read is still forming.
    end = int(time.time()) // HOUR_S * HOUR_S - HOUR_S
    over, under = end - (maxbars - 10) * MINUTE_S, end - (maxbars - 11) * MINUTE_S
    if over < DAY_S:
        pytest.skip(f"MaxBars {maxbars} spans an M1 window from before 1970-01-02")
    # The bars the window holds, read in thirds, each well within the cap.
    cuts = [over + (end - over) * third // 3 for third in range(4)]
    thirds = []
    for k in range(2):
        thirds.append(await m1_bars(hub_session, cuts[k], cuts[k + 1] - 1))
    thirds.append(await m1_bars(hub_session, cuts[2], end))
    expected = np.concatenate(thirds)

    over_cap = await m1_bars(hub_session, over, end)
    at_cap = await m1_bars(hub_session, under, end)

    assert len(expected) > 0
    assert np.all(np.diff(expected["time"]) > 0)
    assert np.array_equal(over_cap, expected)
    assert np.array_equal(at_cap, expected[expected["time"] >= under])


async def test_a_tick_window_one_day_wide_a_month_back_answers_its_ticks_in_time_order(session):
    hub_session, _ = session
    start = int(time.time()) - 30 * DAY_S
    end = start + DAY_S

    ticks = (
        await synced(
            hub_session,
            Call.COPY_TICKS_RANGE,
            symbol=SYMBOL,
            date_from=start,
            date_to=end,
            flags=mirror.COPY_TICKS_INFO,
        )
    ).result

    assert len(ticks) > 0
    assert np.all(np.diff(ticks["time_msc"]) >= 0)
    assert np.all((ticks["time_msc"] >= start * 1000) & (ticks["time_msc"] <= end * 1000 + 999))


async def test_a_week_of_ticks_answers_within_five_minutes_while_the_publishers_reads_stay_quick(
    session,
):
    hub_session, _ = session
    end = int(time.time()) - 30 * DAY_S
    week = asyncio.create_task(
        synced(
            hub_session,
            Call.COPY_TICKS_RANGE,
            symbol=SYMBOL,
            date_from=end - 7 * DAY_S,
            date_to=end,
            flags=mirror.COPY_TICKS_INFO,
        )
    )
    durations = []
    # The publisher's own reads, every second, for as long as the week syncs and streams.
    while not week.done():
        asked = time.monotonic()
        await hub_session.request(Call.SYMBOL_INFO_TICK, symbol=SYMBOL)
        took = time.monotonic() - asked
        durations.append(took)
        logger.info("symbol_info_tick %d during the week's read took %.3f s", len(durations), took)
        await asyncio.sleep(max(0.0, 1.0 - took))

    answered = week.result()

    logger.info(
        "the week answered %d ticks; %d reads, the slowest %.3f s",
        len(answered.result),
        len(durations),
        max(durations),
    )
    assert isinstance(answered, Answered)
    assert max(durations) <= TICK_READ_S


async def test_a_command_for_a_symbol_no_publisher_serves_answers_once_its_chart_opened(session):
    hub_session, _ = session
    symbols = (await ask(hub_session, Call.SYMBOLS_GET)).result
    other = next(
        symbol
        for symbol in symbols
        if symbol.name != SYMBOL
        and symbol.trade_mode == mirror.SYMBOL_TRADE_MODE_FULL
        and symbol.volume_min > 0
    )
    order = refused_order(other, other.point)
    try:
        with pytest.raises(NoPublisher) as deferred:
            await hub_session.order_send(order)
        await asyncio.sleep(deferred.value.retry_after_s)
        answered = await ask(hub_session, Call.ORDER_SEND, request=order)

        assert is_struct(answered.result, "MqlTradeResult")
        assert answered.result.retcode > 0
    finally:
        await assert_flat(hub_session)
