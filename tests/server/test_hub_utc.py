"""The hub's epoch conversion: published frames and answer results from the broker's clock to true
UTC, request arguments the other way, each at the era of the timestamp itself."""

import calendar
import logging
from datetime import timedelta
from zoneinfo import ZoneInfo

import numpy as np
import pytest
from ea_wire import zeroed
from mirror_samples import BROKER_EPOCH, CLOCK, UTC_EPOCH

from mt5connector.server.encoding import bar_close
from mt5connector.server.hub_utc import Conversion, Results, broker_arguments, published
from mt5connector.server.repeated_hours import RepeatedHours
from mt5connector.server.windows import reading_into
from mt5connector.server.wire import binary, calls, mirror
from mt5connector.server.wire.broker_clock import BrokerClock
from mt5connector.server.wire.history_wire import Series, bar_series
from mt5connector.server.wire.push_wire import Stream, Subscription, TransactionType

Call = calls.Call
# 2025-11-02 01:30 in New York happens twice; the broker's clock shows 08:30 both times.
REPEATED_BROKER_EPOCH = calendar.timegm((2025, 11, 2, 8, 30, 0))
# Mid-January the broker's clock runs two hours ahead of true UTC.
WINTER_UTC = calendar.timegm((2025, 1, 15, 10, 0, 0))
WINTER_BROKER = calendar.timegm((2025, 1, 15, 12, 0, 0))
_RATES = np.dtype(list(mirror.RATES.dtype))
_TICKS = np.dtype(list(mirror.TICKS.dtype))


def tick(epoch, msc=123):
    return {
        "time": epoch,
        "bid": 1.085,
        "ask": 1.08512,
        "last": 0.0,
        "volume": 0,
        "time_msc": epoch * 1000 + msc,
        "flags": 6,
        "volume_real": 0.0,
    }


def rates(epoch):
    return {
        "time": epoch,
        "open": 1.085,
        "high": 1.0852,
        "low": 1.0849,
        "close": 1.0851,
        "tick_volume": 42,
        "spread": 12,
        "real_volume": 0,
    }


def rates_rows(*epochs):
    return np.array([tuple(rates(epoch).values()) for epoch in epochs], dtype=_RATES)


def test_a_published_ticks_epochs_reach_its_subscribers_in_true_utc():
    passed = published(binary.Ticks("EURUSD", (tick(BROKER_EPOCH),)), CLOCK, RepeatedHours())

    assert passed.subscription == Subscription(Stream.TICKS, "EURUSD")
    assert passed.frame == binary.Ticks("EURUSD", (tick(UTC_EPOCH),))


def test_a_published_bar_reaches_its_series_with_its_open_and_close_in_true_utc():
    bar = binary.RatesBar("EURUSD", mirror.TIMEFRAME_H4, rates(BROKER_EPOCH))

    passed = published(bar, CLOCK, RepeatedHours())

    assert passed.subscription == Subscription(Stream.BARS, "EURUSD", Series.H4)
    assert passed.frame == binary.Bar(
        "EURUSD", Series.H4, rates(UTC_EPOCH) | {"time_close": UTC_EPOCH + 4 * 3_600}
    )


def test_a_published_bar_of_a_timeframe_no_series_serves_is_refused():
    bar = binary.RatesBar("EURUSD", mirror.TIMEFRAME_MN1, rates(BROKER_EPOCH))

    with pytest.raises(ValueError, match=f"timeframe {mirror.TIMEFRAME_MN1} has no history"):
        published(bar, CLOCK, RepeatedHours())


@pytest.mark.parametrize(
    ("kind", "symbol", "request_symbol", "of"),
    [
        (TransactionType.DEAL_ADD, "EURUSD", "", "EURUSD"),
        (TransactionType.REQUEST, "", "GBPUSD", "GBPUSD"),
        (TransactionType.ORDER_DELETE, "", "", ""),
    ],
    ids=["deal", "request", "unnamed"],
)
def test_a_transaction_is_of_its_own_symbol_but_a_request_of_its_requests(
    kind, symbol, request_symbol, of
):
    frame = binary.TradeTransaction(
        zeroed(binary.TRADE_TRANSACTION, symbol=symbol, type=int(kind)),
        zeroed(binary.TRADE_REQUEST, symbol=request_symbol),
        zeroed(binary.TRADE_RESULT),
    )

    passed = published(frame, CLOCK, RepeatedHours())

    assert (passed.subscription, passed.symbol) == (Subscription(Stream.TRADE_TRANSACTIONS), of)


def test_a_transactions_expirations_reach_true_utc_and_zero_stays_zero():
    frame = binary.TradeTransaction(
        zeroed(binary.TRADE_TRANSACTION, symbol="EURUSD", time_expiration=BROKER_EPOCH),
        zeroed(binary.TRADE_REQUEST, symbol="EURUSD", expiration=0),
        zeroed(binary.TRADE_RESULT),
    )

    passed = published(frame, CLOCK, RepeatedHours()).frame

    assert (passed.transaction["time_expiration"], passed.request["expiration"]) == (UTC_EPOCH, 0)


def test_an_epoch_in_the_brokers_repeated_hour_reads_as_its_first_occurrence_with_one_warning(
    caplog,
):
    caplog.set_level(logging.WARNING)
    frame = binary.Ticks("EURUSD", (tick(REPEATED_BROKER_EPOCH), tick(REPEATED_BROKER_EPOCH + 60)))

    passed = published(frame, CLOCK, RepeatedHours())

    # New York's first 01:30 that day is EDT, 05:30Z.
    first = calendar.timegm((2025, 11, 2, 5, 30, 0))
    assert [item["time"] for item in passed.frame.ticks] == [first, first + 60]
    assert [record.getMessage() for record in caplog.records] == [
        f"tick: time {REPEATED_BROKER_EPOCH} is in the broker's repeated hour, read as its first "
        "occurrence"
    ]


def test_a_requests_windows_and_a_trade_requests_expiration_reach_the_ea_at_their_own_era():
    window = calls.CALLS[Call.HISTORY_DEALS_GET]
    order = calls.CALLS[Call.ORDER_SEND]
    request = calls.trade_request({"symbol": "EURUSD", "expiration": WINTER_UTC})

    assert broker_arguments(
        window, {"date_from": UTC_EPOCH, "date_to": WINTER_UTC, "group": "*"}, CLOCK
    ) == {"date_from": BROKER_EPOCH, "date_to": WINTER_BROKER, "group": "*"}
    assert broker_arguments(order, {"request": request}, CLOCK) == {
        "request": request | {"expiration": WINTER_BROKER}
    }
    assert broker_arguments(window, {"ticket": 7}, CLOCK) == {"ticket": 7}


def test_a_struct_answers_epochs_reach_true_utc_and_zero_stays_zero():
    row = calls.CALLS[Call.POSITIONS_GET]
    position = zeroed(
        row.layout,
        ticket=7,
        time=BROKER_EPOCH,
        time_msc=BROKER_EPOCH * 1000 + 5,
        time_update=0,
        time_update_msc=0,
    )

    converted = Results(row, {}, CLOCK, RepeatedHours()).part([position])

    assert converted == [position | {"time": UTC_EPOCH, "time_msc": UTC_EPOCH * 1000 + 5}]


def test_tick_rows_reach_true_utc():
    row = calls.CALLS[Call.COPY_TICKS_RANGE]
    ticks = np.array([tuple(tick(BROKER_EPOCH).values())], dtype=_TICKS)

    window = {"date_from": UTC_EPOCH, "date_to": UTC_EPOCH + 60}

    converted = Results(row, window, CLOCK, RepeatedHours()).part(ticks)

    assert converted[["time", "time_msc"]].tolist() == [(UTC_EPOCH, UTC_EPOCH * 1000 + 123)]


def test_bar_rows_reach_true_utc_with_their_close():
    row = calls.CALLS[Call.COPY_RATES_RANGE]
    arguments = {
        "timeframe": mirror.TIMEFRAME_D1,
        "date_from": UTC_EPOCH - 9 * 3_600,
        "date_to": UTC_EPOCH,
    }
    results = Results(row, arguments, CLOCK, RepeatedHours())

    converted = results.part(rates_rows(BROKER_EPOCH - 9 * 3_600))

    # D1 is on the broker day's grid: it closes at the next broker midnight.
    assert converted[["time", "time_close"]].tolist() == [
        (UTC_EPOCH - 9 * 3_600, UTC_EPOCH - 9 * 3_600 + 86_400)
    ]


# On a symbol that trades weekends the H1 bars stamped broker 09:00 and 10:00 on 2026-03-08 both
# open at 07:00Z.
SKIPPED = calendar.timegm((2026, 3, 8, 9, 0, 0))
AFTER_SKIPPED = calendar.timegm((2026, 3, 8, 10, 0, 0))
SPRING_OPEN = calendar.timegm((2026, 3, 8, 7, 0, 0))


def test_of_two_bars_opening_in_one_true_utc_period_across_parts_the_first_is_kept(caplog):
    caplog.set_level(logging.WARNING)
    row = calls.CALLS[Call.COPY_RATES_RANGE]
    arguments = {
        "timeframe": mirror.TIMEFRAME_H1,
        "date_from": SPRING_OPEN,
        "date_to": SPRING_OPEN + 3_600,
    }
    results = Results(row, arguments, CLOCK, RepeatedHours())

    first = results.part(rates_rows(SKIPPED))
    second = results.part(rates_rows(AFTER_SKIPPED, AFTER_SKIPPED + 3_600))

    assert first["time"].tolist() == [SPRING_OPEN]
    assert second["time"].tolist() == [SPRING_OPEN + 3_600]
    assert any(
        f"time {AFTER_SKIPPED} opens in the true-UTC period of the bar before it" in record.message
        for record in caplog.records
    )


def test_a_range_reads_records_outside_its_window_are_not_passed_on():
    row = calls.CALLS[Call.COPY_TICKS_RANGE]
    window = {"date_from": UTC_EPOCH, "date_to": UTC_EPOCH + 60}
    ticks = np.array(
        [tuple(tick(BROKER_EPOCH - 1).values()), tuple(tick(BROKER_EPOCH + 61).values())],
        dtype=_TICKS,
    )

    assert len(Results(row, window, CLOCK, RepeatedHours()).part(ticks)) == 0


def test_a_range_reads_window_reaches_the_ea_as_the_broker_bounds_it_reads_across():
    window = {"symbol": "EURUSD", "date_from": UTC_EPOCH, "date_to": UTC_EPOCH + 60, "flags": 1}

    # Mid-July holds no clock change within a day: the window is its broker seconds alone.
    assert broker_arguments(calls.CALLS[Call.COPY_TICKS_RANGE], window, CLOCK) == window | {
        "date_from": BROKER_EPOCH,
        "date_to": BROKER_EPOCH + 60,
    }


class CountingClock:
    """The broker clock, counting every question asked of it."""

    def __init__(self):
        self.asked = 0

    def __getattr__(self, name):
        method = getattr(CLOCK, name)

        def counted(*args):
            self.asked += 1
            return method(*args)

        return counted


def broker_hours(seconds: np.ndarray) -> int:
    return len(np.unique(seconds // 3_600))


# How many questions of the clock one broker hour of rows may cost, however many rows it holds.
PER_HOUR = 16


def test_a_100_000_tick_part_asks_the_clock_per_broker_hour_not_per_row():
    clock = CountingClock()
    row = calls.CALLS[Call.COPY_TICKS_RANGE]
    # One tick every 100 ms from broker 07:00 on the autumn change's day, through its repeated hour.
    start = calendar.timegm((2026, 11, 1, 7, 0, 0))
    milliseconds = start * 1000 + np.arange(100_000, dtype=np.int64) * 100
    ticks = np.zeros(100_000, dtype=_TICKS)
    ticks["time"] = milliseconds // 1000
    ticks["time_msc"] = milliseconds
    window = {"date_from": start - 6 * 3_600, "date_to": start}

    Results(row, window, clock, RepeatedHours()).part(ticks)

    assert clock.asked <= PER_HOUR * broker_hours(ticks["time"])


def test_a_100_000_bar_part_asks_the_clock_per_broker_hour_not_per_row():
    clock = CountingClock()
    row = calls.CALLS[Call.COPY_RATES_RANGE]
    opens = calendar.timegm((2026, 1, 5, 0, 0, 0)) + np.arange(100_000, dtype=np.int64) * 60
    bars = np.zeros(100_000, dtype=_RATES)
    bars["time"] = opens
    window = {"timeframe": mirror.TIMEFRAME_M1, "date_from": 1, "date_to": 2_000_000_000}

    Results(row, window, clock, RepeatedHours()).part(bars)

    assert clock.asked <= PER_HOUR * broker_hours(opens)


# Broker spans of one row per second: ±2 h about New York's 2026 spring-forward (the broker skips
# 09:00–10:00 on 2026-03-08) and autumn-back (it repeats 08:00–09:00 on 2026-11-01), and a summer
# span with no change near it.
SPANS = {
    "spring": calendar.timegm((2026, 3, 8, 7, 30, 0)),
    "autumn": calendar.timegm((2026, 11, 1, 6, 30, 0)),
    "summer": calendar.timegm((2026, 7, 15, 10, 0, 0)),
}
# Seconds about the epoch, where zero is no time and stays zero.
EDGES = np.array([0, 1, 3_599, 3_600, 86_400], dtype=np.int64)


def per_second(span: str) -> np.ndarray:
    return SPANS[span] + np.arange(4 * 3_600, dtype=np.int64)


@pytest.mark.parametrize("span", list(SPANS))
def test_converted_seconds_and_milliseconds_equal_the_clock_read_element_by_element(span):
    seconds = np.concatenate([EDGES, per_second(span)])
    milliseconds = np.concatenate([np.array([0, 1, 999, 1_000]), seconds * 1000 + 437])
    ticks = np.zeros(len(milliseconds), dtype=_TICKS)
    ticks["time"] = np.concatenate([np.zeros(4, dtype=np.int64), seconds])[: len(ticks)]
    ticks["time_msc"] = milliseconds
    conversion = ws_conversion()

    converted = conversion.rows(mirror.TICKS.epochs, ticks, "test")

    assert converted["time"].tolist() == [CLOCK.to_utc(value) for value in ticks["time"].tolist()]
    assert converted["time_msc"].tolist() == [
        CLOCK.to_utc_msc(value) for value in milliseconds.tolist()
    ]


def reading_into_one_by_one(seconds, start, end):
    inside = []
    for second in seconds.tolist():
        first, last = CLOCK.occurrences(second)
        inside.append(start <= first <= end or start <= last <= end)
    return inside


AUTUMN_UTC = calendar.timegm((2026, 11, 1, 0, 0, 0))
SPRING_UTC = calendar.timegm((2026, 3, 8, 0, 0, 0))


@pytest.mark.parametrize(
    ("span", "start", "end"),
    [
        # Inside the repeated hour's second pass, 06:10–06:40Z, and its first, 05:10–05:40Z.
        ("autumn", AUTUMN_UTC + 6 * 3_600 + 600, AUTUMN_UTC + 6 * 3_600 + 2_400),
        ("autumn", AUTUMN_UTC + 5 * 3_600 + 600, AUTUMN_UTC + 5 * 3_600 + 2_400),
        # Across the skipped hour, 06:30–07:30Z.
        ("spring", SPRING_UTC + 6 * 3_600 + 1_800, SPRING_UTC + 7 * 3_600 + 1_800),
        ("summer", SPANS["summer"] - 3 * 3_600, SPANS["summer"] - 2 * 3_600),
    ],
    ids=["second-pass", "first-pass", "skipped", "summer"],
)
def test_the_window_filter_equals_the_clock_read_element_by_element(span, start, end):
    seconds = np.concatenate([EDGES, per_second(span)])

    assert reading_into(CLOCK, seconds, start, end).tolist() == reading_into_one_by_one(
        seconds, start, end
    )


def window_about(timeframe, opens):
    """A bar read's arguments, its window a day wider than the broker opens on either side."""
    return {"timeframe": timeframe, "date_from": opens[0] - 86_400, "date_to": opens[-1] + 86_400}


def bars_one_by_one(series, opens):
    """Each bar's true-UTC open and close as the clock reads its broker open, of two opening at one
    true-UTC second the first."""
    answered = []
    for broker_open in opens:
        opened = CLOCK.to_utc(broker_open)
        if not answered or answered[-1][0] != opened:
            answered.append((opened, bar_close(CLOCK, series, broker_open)))
    return answered


@pytest.mark.parametrize("span", list(SPANS))
@pytest.mark.parametrize("timeframe", [mirror.TIMEFRAME_M1, mirror.TIMEFRAME_H1])
def test_converted_bars_equal_the_clock_read_bar_by_bar(span, timeframe):
    series = bar_series(timeframe)
    period = 60 if timeframe == mirror.TIMEFRAME_M1 else 3_600
    first = SPANS[span] - SPANS[span] % period
    opens = first + np.arange(0, 4 * 3_600, period, dtype=np.int64)
    rates = rates_rows(*opens.tolist())
    row = calls.CALLS[Call.COPY_RATES_RANGE]
    arguments = window_about(timeframe, opens.tolist())

    converted = Results(row, arguments, CLOCK, RepeatedHours()).part(rates)

    assert converted[["time", "time_close"]].tolist() == bars_one_by_one(series, opens.tolist())


def test_each_repeated_and_skipped_hour_is_warned_of_once_naming_its_first_row(caplog):
    caplog.set_level(logging.WARNING)
    seconds = np.concatenate([per_second("spring"), per_second("autumn")])
    ticks = np.zeros(len(seconds), dtype=_TICKS)
    ticks["time"] = seconds
    ticks["time_msc"] = seconds * 1000

    ws_conversion().rows(mirror.TICKS.epochs, ticks, "test")

    skipped = calendar.timegm((2026, 3, 8, 9, 0, 0))
    repeated = calendar.timegm((2026, 11, 1, 8, 0, 0))
    assert [record.getMessage() for record in caplog.records] == [
        f"test: time {skipped} is in the hour the broker's clock skips, read at the offset before "
        "the change",
        f"test: time {repeated} is in the broker's repeated hour, read as its first occurrence",
    ]


def ws_conversion():
    return Conversion(CLOCK, RepeatedHours())


# Lord Howe falls back half an hour at 02:00 on 2026-04-05: at +7 h the broker's 08:30–08:59 repeats
# while 08:00–08:29 does not, so the readings change within the broker hour.
LORD_HOWE = BrokerClock(ZoneInfo("Australia/Lord_Howe"), timedelta(hours=7))


def test_a_repeated_half_hour_is_warned_of_at_its_first_repeated_bar(caplog):
    caplog.set_level(logging.WARNING)
    opens = calendar.timegm((2026, 4, 5, 8, 0, 0)) + np.arange(60, dtype=np.int64) * 60
    first_repeated = next(value for value in opens.tolist() if LORD_HOWE.is_ambiguous(value))
    row = calls.CALLS[Call.COPY_RATES_RANGE]
    arguments = window_about(mirror.TIMEFRAME_M1, opens.tolist())

    converted = Results(row, arguments, LORD_HOWE, RepeatedHours()).part(
        rates_rows(*opens.tolist())
    )

    assert converted["time"].tolist() == [LORD_HOWE.to_utc(value) for value in opens.tolist()]
    assert [record.getMessage() for record in caplog.records] == [
        f"copy_rates_range: time {first_repeated} is in the broker's repeated hour, read as its "
        "first occurrence"
    ]
