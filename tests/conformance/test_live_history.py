"""The history protocol against the installed MetaTrader5 package and a running terminal.
Gated by MT5_LIVE_CONFORMANCE=1; the broker clock is MT5_BROKER_TZ plus MT5_BROKER_OFFSET_HOURS."""

import os
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import numpy as np
import pytest

from mt5connector.server.history import FloorStore, History, Syncing
from mt5connector.server.repeated_hours import RepeatedHours
from mt5connector.server.terminal import Answered, Failed, Terminal
from mt5connector.server.wire import mirror
from mt5connector.server.wire.broker_clock import BrokerClock
from mt5connector.server.wire.history_wire import Series, TickFlags

pytestmark = pytest.mark.skipif(
    os.environ.get("MT5_LIVE_CONFORMANCE") != "1", reason="MT5_LIVE_CONFORMANCE is not 1"
)

HOUR = 3_600
DAY = 86_400
BEFORE_HISTORY = DAY


@pytest.fixture(scope="module")
def clock() -> BrokerClock:
    return BrokerClock(
        ZoneInfo(os.environ.get("MT5_BROKER_TZ", "America/New_York")),
        timedelta(hours=int(os.environ.get("MT5_BROKER_OFFSET_HOURS", "7"))),
    )


@pytest.fixture(scope="module")
def history(terminal, clock) -> History:
    return History(
        Terminal(terminal), clock, RepeatedHours(), FloorStore(), retry_s=5, floor_ttl_s=900
    )


def answered(read: Callable[[], Answered | Failed | Syncing]) -> list:
    """The rows a history read answers, asked again after each syncing answer's delay."""
    outcome = read()
    while isinstance(outcome, Syncing):
        time.sleep(outcome.retry_after_s)
        outcome = read()
    assert isinstance(outcome, Answered), outcome
    return outcome.value


def advertised_floor(history: History, symbol: str, series: Series) -> int:
    ranges = history.ranges(symbol)
    assert isinstance(ranges, Answered), ranges
    return ranges.value["ranges"][series.value]["floor"]


@pytest.fixture(scope="module")
def floor(history, symbol) -> int:
    answered(lambda: history.bars(symbol, Series.H1, BEFORE_HISTORY, BEFORE_HISTORY + DAY))
    return advertised_floor(history, symbol, Series.H1)


def test_a_range_one_period_over_the_cap_answers_nothing(terminal, symbol, clock):
    end = clock.to_broker(int(time.time()) - DAY)
    start = end - (terminal.terminal_info().maxbars - 10) * HOUR

    rates = terminal.copy_rates_range(
        symbol,
        mirror.TIMEFRAME_H1,
        datetime.fromtimestamp(start, UTC),
        datetime.fromtimestamp(end, UTC),
    )

    assert rates is not None and len(rates) == 0, terminal.last_error()


def test_a_window_before_any_history_is_answered_empty(history, symbol, floor):
    rows = answered(lambda: history.bars(symbol, Series.H1, BEFORE_HISTORY, BEFORE_HISTORY + DAY))

    assert rows == []
    assert floor > BEFORE_HISTORY + DAY


def test_a_window_straddling_the_floor_is_answered_from_it(history, symbol, floor):
    rows = answered(lambda: history.bars(symbol, Series.H1, floor - DAY, floor + 2 * DAY))

    assert rows[0]["time"] == floor


def test_a_window_over_the_cap_is_answered_on_the_grid(terminal, history, symbol):
    end = int(time.time()) - DAY
    start = end - (terminal.terminal_info().maxbars - 10) * HOUR

    rows = answered(lambda: history.bars(symbol, Series.H1, start, end))

    assert len(rows) > 0


def test_the_bar_from_a_date_is_the_last_opened_at_or_before_it(terminal, history, symbol, clock):
    end = int(time.time()) - DAY
    rows = answered(lambda: history.bars(symbol, Series.H1, end - 7 * DAY, end))
    opened = clock.to_broker(rows[-2]["time"])

    last = terminal.copy_rates_from(
        symbol, mirror.TIMEFRAME_H1, datetime.fromtimestamp(opened + HOUR // 2, UTC), 1
    )

    assert last is not None and last["time"].tolist() == [opened], terminal.last_error()


def test_a_day_of_ticks_is_answered_in_order(history, symbol):
    day = int(time.time()) // DAY * DAY - DAY

    rows = answered(lambda: history.ticks(symbol, day, day + DAY - 1, TickFlags.INFO))

    times = np.array([row["time_msc"] for row in rows])
    assert np.all(np.diff(times) >= 0)


def test_a_window_before_the_first_tick_is_answered_empty(history, symbol):
    rows = answered(
        lambda: history.ticks(symbol, BEFORE_HISTORY, BEFORE_HISTORY + HOUR, TickFlags.INFO)
    )

    assert rows == []
    assert advertised_floor(history, symbol, Series.TICKS) > BEFORE_HISTORY + HOUR
