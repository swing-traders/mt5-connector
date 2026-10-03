"""The broker clock against reference conversions and New York's changeovers: New York plus seven
hours runs 10,800 s ahead of UTC under EDT and 7,200 s under EST."""

import calendar
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from mt5connect.broker_clock import BrokerClock, NonexistentWallTime

CLOCK = BrokerClock(ZoneInfo("America/New_York"), timedelta(hours=7))
EDT_S = 10_800
EST_S = 7_200


def broker_epoch(year: int, month: int, day: int, hour: int = 0, minute: int = 0, second: int = 0):
    """The broker epoch of a broker wall time: the wall time read as if it were UTC."""
    return calendar.timegm((year, month, day, hour, minute, second))


@pytest.mark.parametrize(
    ("broker", "utc", "instant", "offset_s"),
    [
        (1_752_580_800, 1_752_570_000, datetime(2025, 7, 15, 9, tzinfo=UTC), EDT_S),
        (1_736_942_400, 1_736_935_200, datetime(2025, 1, 15, 10, tzinfo=UTC), EST_S),
    ],
    ids=["edt", "est"],
)
def test_reference_conversions(broker, utc, instant, offset_s):
    assert datetime.fromtimestamp(utc, UTC) == instant
    assert CLOCK.to_utc(broker) == utc
    assert CLOCK.offset_at(utc) == timedelta(seconds=offset_s)
    assert CLOCK.to_broker(utc) == broker
    assert CLOCK.to_broker(CLOCK.to_utc(broker)) == broker


def test_milliseconds_keep_their_remainder():
    assert CLOCK.to_utc_msc(1_752_580_800_123) == 1_752_570_000_123


def test_zero_is_no_time_both_ways():
    assert CLOCK.to_utc(0) == 0
    assert CLOCK.to_utc_msc(0) == 0
    assert CLOCK.to_broker(0) == 0


# Each changeover day as (year, month, day), the broker hour its changeover takes, and the offsets
# before and after it.
CHANGEOVERS = [
    ((2025, 3, 9), 9, EST_S, EDT_S),
    ((2025, 11, 2), 8, EDT_S, EST_S),
    ((2026, 3, 8), 9, EST_S, EDT_S),
]


@pytest.mark.parametrize(("day", "changeover_hour", "before_s", "after_s"), CHANGEOVERS, ids=str)
def test_hourly_sweep_across_a_changeover_day(day, changeover_hour, before_s, after_s):
    for hour in range(24):
        broker = broker_epoch(*day, hour)
        if hour < changeover_hour:
            assert CLOCK.to_utc(broker) == broker - before_s
            assert CLOCK.to_broker(broker - before_s) == broker
        elif hour > changeover_hour:
            assert CLOCK.to_utc(broker) == broker - after_s
            assert CLOCK.to_broker(broker - after_s) == broker


@pytest.mark.parametrize(
    "wall",
    [(2026, 3, 8, 9, 0, 0), (2026, 3, 8, 9, 30, 0), (2026, 3, 8, 9, 59, 59)],
    ids=["09:00:00", "09:30:00", "09:59:59"],
)
def test_the_skipped_hour_raises(wall):
    with pytest.raises(NonexistentWallTime):
        CLOCK.to_utc(broker_epoch(*wall))
    with pytest.raises(NonexistentWallTime):
        CLOCK.to_utc_msc(broker_epoch(*wall) * 1000 + 500)


def test_the_skipped_hour_is_bounded():
    assert CLOCK.to_utc(broker_epoch(2026, 3, 8, 8, 59, 59)) == broker_epoch(2026, 3, 8, 6, 59, 59)
    assert CLOCK.to_utc(broker_epoch(2026, 3, 8, 10)) == broker_epoch(2026, 3, 8, 7)


def test_the_repeated_hour_reads_as_its_first_occurrence():
    broker = broker_epoch(2025, 11, 2, 8, 30)
    assert CLOCK.is_ambiguous(broker)
    assert CLOCK.to_utc(broker) == broker - EDT_S
    assert CLOCK.to_utc_msc(broker * 1000 + 7) == (broker - EDT_S) * 1000 + 7


@pytest.mark.parametrize(
    "wall",
    [(2025, 11, 2, 7, 59, 59), (2025, 11, 2, 9, 0, 0), (2025, 7, 15, 12, 0, 0)],
    ids=["before", "after", "july"],
)
def test_only_the_repeated_hour_is_ambiguous(wall):
    assert not CLOCK.is_ambiguous(broker_epoch(*wall))
