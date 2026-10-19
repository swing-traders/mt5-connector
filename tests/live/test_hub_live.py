"""The hub and its EAs in two running images, through the client's hub session; runs only when
MT5_LIVE_HUB_SYMBOL names a symbol their account trades, weekends included. MT5_LIVE_HUB_URL names
the hub of a terminal whose MaxBars is the ceiling, MT5_LIVE_CAPPED_HUB_URL one whose MaxBars is
capped; the server README's live-tier playbook starts both."""

import asyncio
import logging
import os
import time
from datetime import UTC, datetime

import numpy as np
import pytest
from nautilus_trader.core.nautilus_pyo3 import WebSocketClientError

from mt5connector.client.config import MT5Config
from mt5connector.client.errors import InvalidWindow, NoPublisher
from mt5connector.client.hub import Answered, HubSession
from mt5connector.wire import binary, calls, mirror
from mt5connector.wire.history_wire import BAR_TIMEFRAME, Series
from mt5connector.wire.push_wire import Stream, Subscription, TransactionType

HUB_URL = os.environ.get("MT5_LIVE_HUB_URL", "ws://127.0.0.1:9000")
CAPPED_HUB_URL = os.environ.get("MT5_LIVE_CAPPED_HUB_URL", "ws://127.0.0.1:9001")
SYMBOL = os.environ.get("MT5_LIVE_HUB_SYMBOL")

pytestmark = pytest.mark.skipif(SYMBOL is None, reason="MT5_LIVE_HUB_SYMBOL is not set")

Call = calls.Call
# How long a call may stay deferred while the clock verifies or a chart opens.
ANSWERED_S = 120
# How long a history request may stay deferred while its history EA measures the symbol and reads:
# two history bounds.
SYNCED_S = 2 * calls.HISTORY_BUSY_S
# The magic every order of this tier carries, by which a resting one is found and removed.
MAGIC = 7_114_001
DAY_S = 86_400
HOUR_S = 3_600
MINUTE_S = 60
# The offsets in a minute, in milliseconds, of the four ticks the kline-quote rule makes of its bar.
KLINE_OFFSETS_MS = (0, 20_000, 40_000, 59_999)
# How far before a read began a bar must have opened to be the same in every read of it.
STABLE_S = 15 * MINUTE_S
# The account history a deal read by its ticket is picked from.
HISTORY_DAYS = 90
# The longest a publisher's read may take while its symbol's ticks download.
TICK_READ_S = 1.0
# The sessions that command at once.
SENDERS = 10
# The symbols tried for one no publisher serves: a rerun inside the hub's idle period finds the
# charts the last run opened still serving.
UNSERVED_CANDIDATES = 5
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


class Stamped(Owner):
    """What a session hands its owner, in order, and when each was handed."""

    def __init__(self) -> None:
        super().__init__()
        self.handed: list[float] = []

    def on_frame(self, frame) -> None:
        self.handed.append(time.monotonic())
        super().on_frame(frame)

    def when(self, matches) -> float:
        """When the first frame that matches was handed."""
        return next(
            at for at, frame in zip(self.handed, self.frames, strict=True) if matches(frame)
        )


def verdict_of(request_id: int):
    """Whether a frame is the REQUEST transaction carrying `request_id`."""

    def matches(frame) -> bool:
        return (
            isinstance(frame, binary.TradeTransaction)
            and frame.transaction["type"] == TransactionType.REQUEST
            and frame.result["request_id"] == request_id
        )

    return matches


def config(url: str) -> MT5Config:
    # The session reads the hub's URL and its reconnect delays alone; the account is the image's.
    return MT5Config(
        account=1,
        server="unused",
        ws_url=url,
    )


async def connected(owner: Owner, url: str = HUB_URL) -> HubSession:
    session = HubSession(config(url), asyncio.get_running_loop(), owner.on_frame, None, Log())
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


async def info_ticks(session: HubSession, start: int, end: int) -> np.ndarray:
    """The symbol's quote ticks in seconds [start, end], asked again while the window syncs."""
    window = {"date_from": start, "date_to": end, "flags": mirror.COPY_TICKS_INFO}
    answered = await synced(session, Call.COPY_TICKS_RANGE, symbol=SYMBOL, **window)
    return answered.result


async def ranges(session: HubSession) -> calls.Ranges:
    """The symbol's ranges, asked again while its history EA measures it."""
    return (await synced(session, Call.RANGES, symbol=SYMBOL)).result


def kline_quotes(bar, digits: int) -> list[tuple]:
    """The four ticks the kline-quote rule makes of a bar, inside its minute: the bid its open, low,
    high and close, or open, high, low and close for a bar closing below its open, the ask the bid
    plus the bar's own spread in points, zero included, and no volume."""
    if bar["close"] < bar["open"]:
        bids = (bar["open"], bar["high"], bar["low"], bar["close"])
    else:
        bids = (bar["open"], bar["low"], bar["high"], bar["close"])
    opened = int(bar["time"]) * 1000
    flags = mirror.TICK_FLAG_BID | mirror.TICK_FLAG_ASK
    quotes = []
    for offset, bid in zip(KLINE_OFFSETS_MS, bids, strict=True):
        ask = round(float(bid) + int(bar["spread"]) * 10.0**-digits, digits)
        stamp = opened + offset
        quotes.append((stamp // 1000, float(bid), ask, 0.0, 0, stamp, flags, 0.0))
    return quotes


def minute_quotes(ticks: np.ndarray, opened: int, digits: int) -> list[tuple]:
    """The ticks of the minute opened at `opened`, true-UTC seconds, their asks at the digits."""
    minute = ticks[
        (ticks["time_msc"] >= opened * 1000) & (ticks["time_msc"] < opened * 1000 + 60_000)
    ]
    return [
        (
            int(tick["time"]),
            float(tick["bid"]),
            round(float(tick["ask"]), digits),
            float(tick["last"]),
            int(tick["volume"]),
            int(tick["time_msc"]),
            int(tick["flags"]),
            float(tick["volume_real"]),
        )
        for tick in minute
    ]


async def spread_day(session: HubSession, m1_floor: int) -> int | None:
    """The first UTC day from the M1 floor's on whose M1 bars one records a spread, found by
    bisection, the predicate monotone from the first such bar on; None when today holds none."""

    async def recorded(day: int) -> bool:
        bars = await m1_bars(session, day, day + DAY_S - 1)
        return bool(np.any(bars["spread"] > 0))

    lo = m1_floor - m1_floor % DAY_S
    hi = int(time.time()) // DAY_S * DAY_S
    if not await recorded(hi):
        return None
    elif await recorded(lo):
        return lo
    while hi - lo > DAY_S:
        middle = lo + (hi - lo) // DAY_S // 2 * DAY_S
        if await recorded(middle):
            hi = middle
        else:
            lo = middle
    return hi


async def symbol_digits(session: HubSession) -> int:
    return (await ask(session, Call.SYMBOL_INFO, symbol=SYMBOL)).result.digits


def minute_times(ticks: np.ndarray, bar) -> list[int]:
    """The time_msc of each tick of the bar's minute."""
    opened = int(bar["time"]) * 1000
    return [int(stamp) for stamp in ticks["time_msc"] if opened <= stamp < opened + 60_000]


def at_the_rules_offsets(ticks: np.ndarray, bar) -> bool:
    """Whether the bar's minute holds exactly four ticks at the kline-quote rule's offsets, whatever
    their prices: the synthesis's signature, which no venue tick history carries."""
    opened = int(bar["time"]) * 1000
    return [stamp - opened for stamp in minute_times(ticks, bar)] == list(KLINE_OFFSETS_MS)


def _iso(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, UTC).isoformat(timespec="minutes")


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


async def deferred_command(
    session: HubSession, candidates: list
) -> tuple[dict, NoPublisher] | None:
    """The first candidate's refused order whose command no publisher serves yet, with its
    deferral; None when a publisher already serves each."""
    for symbol in candidates:
        order = refused_order(symbol, symbol.point)
        try:
            with pytest.raises(NoPublisher) as deferred:
                await session.order_send(order)
        except pytest.fail.Exception:
            logger.info("%s: a publisher already serves it", symbol.name)
        else:
            return order, deferred.value
    return None


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

    info = (await ask(hub_session, Call.SYMBOL_INFO, symbol=SYMBOL)).result
    tick = (await ask(hub_session, Call.SYMBOL_INFO_TICK, symbol=SYMBOL)).result

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
    held = await ranges(hub_session)

    assert len(bars) > 0
    assert np.all(bars["time"] % MINUTE_S == 0)
    assert np.all((bars["time"] >= start) & (bars["time"] <= end))
    assert Series.M1 in held.series


async def test_the_ranges_report_every_series_the_m1_floor_a_read_from_1970_begins_at(session):
    hub_session, _ = session

    held = await ranges(hub_session)
    m1 = held.series[Series.M1].floor
    ticks = held.series[Series.TICKS].floor
    bars = await m1_bars(hub_session, DAY_S, m1 + DAY_S)
    logger.info("%s: the M1 floor %s, the ticks floor %s", SYMBOL, _iso(m1), _iso(ticks))

    assert set(held.series) == set(BAR_TIMEFRAME) | {Series.TICKS}
    assert int(bars["time"][0]) == m1
    assert int(bars["time"][1] - bars["time"][0]) == MINUTE_S
    assert ticks >= m1


async def test_a_bar_window_ending_a_period_before_the_m1_floor_answers_empty_at_once(session):
    hub_session, _ = session
    m1 = (await ranges(hub_session)).series[Series.M1].floor

    asked = time.monotonic()
    bars = await m1_bars(hub_session, m1 - DAY_S, m1 - MINUTE_S)
    took = time.monotonic() - asked

    assert len(bars) == 0
    assert took < 1.0


async def test_a_day_of_m1_bars_in_the_daily_lead_in_answers_empty(session):
    hub_session, _ = session
    held = await ranges(hub_session)
    d1, m1 = held.series[Series.D1].floor, held.series[Series.M1].floor
    if d1 + DAY_S > m1:
        pytest.skip(
            f"{SYMBOL}: the D1 floor {_iso(d1)} leaves no day before the M1 floor {_iso(m1)}"
        )

    bars = await m1_bars(hub_session, d1, d1 + DAY_S - 1)

    assert len(bars) == 0


async def test_a_day_of_ticks_from_the_ticks_floor_begins_in_that_floors_minute(session):
    hub_session, _ = session
    floor = (await ranges(hub_session)).series[Series.TICKS].floor

    ticks = await info_ticks(hub_session, floor, floor + DAY_S - 1)

    assert len(ticks) > 0
    assert int(ticks["time_msc"][0]) // 60_000 == floor // MINUTE_S


async def test_a_day_of_ticks_ending_before_the_ticks_floor_answers_no_row_at_once(session):
    hub_session, _ = session
    floor = (await ranges(hub_session)).series[Series.TICKS].floor

    asked = time.monotonic()
    ticks = await info_ticks(hub_session, floor - DAY_S, floor - MINUTE_S)
    took = time.monotonic() - asked

    assert len(ticks) == 0
    assert took < 1.0


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


async def capped() -> HubSession:
    try:
        return await connected(Owner(), CAPPED_HUB_URL)
    except WebSocketClientError as exc:
        pytest.fail(f"no hub answers at {CAPPED_HUB_URL}: {exc}")


async def test_the_one_window_over_the_span_cap_a_capped_frame_allows_answers_whole_as_in_thirds():
    hub_session = await capped()
    try:
        maxbars = (await ask(hub_session, Call.TERMINAL_INFO)).result.maxbars
        floor = (await ranges(hub_session)).series[Series.M1].floor
        # Read clamped to the floor, a gapless capped frame's one window over the span cap runs from
        # the floor to the present.
        began = int(time.time())
        whole = await m1_bars(hub_session, floor, began)
        cuts = [floor + (began - floor) * third // 3 for third in range(4)]
        thirds = []
        for k in range(2):
            thirds.append(await m1_bars(hub_session, cuts[k], cuts[k + 1] - 1))
        thirds.append(await m1_bars(hub_session, cuts[2], began))
        joined = np.concatenate(thirds)
    finally:
        await hub_session.disconnect()

    # The forming bar, bars arriving between reads, and the floor sliding a bar a minute between
    # them fall outside the comparison.
    first = max(int(whole["time"][0]), int(joined["time"][0]))

    def stable(rows: np.ndarray) -> np.ndarray:
        return rows[(rows["time"] >= first) & (rows["time"] < began - STABLE_S)]

    assert len(whole) >= maxbars - 11
    assert np.all(np.diff(whole["time"]) > 0)
    assert np.all(np.diff(joined["time"]) > 0)
    assert len(stable(whole)) > 0
    assert np.array_equal(stable(whole), stable(joined))


async def test_a_wide_m1_read_on_the_capped_terminal_begins_at_the_m1_floor_ranges_reports():
    hub_session = await capped()
    try:
        floor = (await ranges(hub_session)).series[Series.M1].floor
        bars = await m1_bars(hub_session, floor - DAY_S, floor + DAY_S)
    finally:
        await hub_session.disconnect()

    assert int(bars["time"][0]) in (floor, floor + MINUTE_S)


async def test_a_day_of_ticks_between_the_capped_terminals_floors_holds_no_synthesised_minute():
    hub_session = await capped()
    try:
        held = (await ranges(hub_session)).series
        ticks_floor, m1 = held[Series.TICKS].floor, held[Series.M1].floor
        if m1 < ticks_floor + 2 * DAY_S:
            pytest.skip(
                f"{SYMBOL}: the M1 floor {_iso(m1)} is not two days after the ticks floor "
                f"{_iso(ticks_floor)}"
            )
        asked = time.monotonic()
        ticks = await info_ticks(hub_session, ticks_floor + DAY_S, ticks_floor + 2 * DAY_S - 1)
        took = time.monotonic() - asked
    finally:
        await hub_session.disconnect()

    minutes = ticks["time_msc"] // 60_000
    opened, counts = np.unique(minutes, return_counts=True)
    synthesised = [
        minute
        for minute in opened[counts == len(KLINE_OFFSETS_MS)]
        if list(ticks["time_msc"][minutes == minute] - minute * 60_000) == list(KLINE_OFFSETS_MS)
    ]

    logger.info(
        "%s: %d ticks in the day after the ticks floor on the capped terminal", SYMBOL, len(ticks)
    )
    assert synthesised == []
    assert took < 60.0


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


async def test_a_tickless_minute_from_the_spread_floor_on_holds_its_bars_kline_quotes(session):
    hub_session, _ = session
    m1 = (await ranges(hub_session)).series[Series.M1].floor
    digits = await symbol_digits(hub_session)
    day = await spread_day(hub_session, m1)
    if day is None:
        pytest.skip(f"{SYMBOL}: no M1 bar from the M1 floor to today records a spread")

    ticks = await info_ticks(hub_session, day, day + DAY_S - 1)
    bars = await m1_bars(hub_session, day, day + DAY_S - 1)
    synthesised = next((bar for bar in bars if at_the_rules_offsets(ticks, bar)), None)
    if synthesised is None:
        pytest.skip(f"{SYMBOL}: no minute of {_iso(day)} holds four quotes at the rule's offsets")

    assert minute_quotes(ticks, int(synthesised["time"]), digits) == kline_quotes(
        synthesised, digits
    )


async def test_a_command_for_a_symbol_no_publisher_serves_answers_once_its_chart_opened(session):
    hub_session, _ = session
    symbols = (await ask(hub_session, Call.SYMBOLS_GET)).result
    candidates = [
        symbol
        for symbol in symbols
        if symbol.name != SYMBOL
        and symbol.trade_mode == mirror.SYMBOL_TRADE_MODE_FULL
        and symbol.volume_min > 0
    ][:UNSERVED_CANDIDATES]
    try:
        first = await deferred_command(hub_session, candidates)
        if first is not None:
            order, deferred = first
            await asyncio.sleep(deferred.retry_after_s)
            answered = await ask(hub_session, Call.ORDER_SEND, request=order)

            assert is_struct(answered.result, "MqlTradeResult")
            assert answered.result.retcode > 0
        else:
            pytest.skip(
                f"a publisher already serves each of {[symbol.name for symbol in candidates]}"
            )
    finally:
        await assert_flat(hub_session)


async def test_sessions_commanding_at_once_each_hear_their_answer_then_their_verdict(session):
    watcher, _ = session
    info = (await ask(watcher, Call.SYMBOL_INFO, symbol=SYMBOL)).result
    tick = (await ask(watcher, Call.SYMBOL_INFO_TICK, symbol=SYMBOL)).result
    owners = [Stamped() for _ in range(SENDERS)]
    senders = [await connected(owner) for owner in owners]

    async def commanded(sender: HubSession, owner: Stamped) -> tuple[float, float]:
        """The time to the command's answer, and to its verdict, from when it was asked."""
        asked = time.monotonic()
        answered = await ask(sender, Call.ORDER_SEND, request=refused_order(info, tick.bid / 2))
        answered_s = time.monotonic() - asked
        assert answered.result.retcode == mirror.TRADE_RETCODE_PLACED
        assert answered.result.request_id > 0
        await owner.first(verdict_of(answered.result.request_id), 30)
        return answered_s, owner.when(verdict_of(answered.result.request_id)) - asked

    try:
        for sender in senders:
            await sender.subscribe(Subscription(Stream.TRADE_TRANSACTIONS))
        timings = await asyncio.gather(
            *(commanded(sender, owner) for sender, owner in zip(senders, owners, strict=True))
        )
        for name, durations in (
            ("answer", [answered for answered, _ in timings]),
            ("verdict", [verdict for _, verdict in timings]),
        ):
            p50, p95 = np.percentile(durations, [50, 95])
            print(f"{name}: p50 {p50:.3f} s, p95 {p95:.3f} s, max {max(durations):.3f} s")

        assert len(timings) == SENDERS
    finally:
        await assert_flat(watcher)
        for sender in senders:
            await sender.disconnect()
