"""True-UTC windows on the broker's clock: the broker bounds a window is read across, and the broker
seconds that read into it at either occurrence of a repeated hour."""

import numpy as np

from mt5connector.server.eras import Eras
from mt5connector.server.wire.broker_clock import BrokerClock

_DAY_S = 86_400


def broker_window(clock: BrokerClock, start: int, end: int) -> tuple[int, int]:
    """The inclusive broker bounds true-UTC [start, end] is read across: the start shifted by the
    least offset the clock runs at within a day of it and the end by the greatest, so a clock change
    about either end stays inside."""
    lo = start + min(_offset_s(clock, edge) for edge in (start - _DAY_S, start, start + _DAY_S))
    hi = end + max(_offset_s(clock, edge) for edge in (end - _DAY_S, end, end + _DAY_S))
    return lo, hi


def _offset_s(clock: BrokerClock, utc_epoch: int) -> int:
    return int(clock.offset_at(utc_epoch).total_seconds())


def reading_into(clock: BrokerClock, seconds: np.ndarray, start: int, end: int) -> np.ndarray:
    """Which broker seconds read into true-UTC [start, end] at either occurrence, so a window in a
    repeated hour's second pass holds the records stamped in it."""
    eras = Eras(clock)
    first = eras.to_utc(seconds)
    last = eras.last_occurrence(seconds)
    return ((start <= first) & (first <= end)) | ((start <= last) & (last <= end))
