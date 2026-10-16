"""True-UTC windows on the broker's clock: the broker bounds a window is read across, and the broker
seconds that read into it at either occurrence of a repeated hour."""

from dataclasses import dataclass

import numpy as np

from mt5connector.server.eras import Eras
from mt5connector.server.wire.broker_clock import BrokerClock

_DAY_S = 86_400


@dataclass(frozen=True)
class BrokerWindow:
    """A true-UTC window on the broker's clock: the bounds read for it, and the intervals of the
    seconds that read into it."""

    # Broker seconds, each pair inclusive.
    fetch: tuple[int, int]
    proof: list[tuple[int, int]]


def broker_window(clock: BrokerClock, start: int, end: int) -> BrokerWindow:
    """The broker window about true-UTC [start, end]: the fetch bounds, the start shifted by the
    least offset the clock runs at within a day of it and the end by the greatest, so a clock change
    about either end stays inside; and the intervals of the broker seconds that read into the window
    at either occurrence, which alone a gap proof covers."""
    offsets = {}
    # The broker seconds where reading into the window can change: a clock change's two readings,
    # and each end's reading at every offset about it.
    cuts = set()
    for edge in (start, end):
        samples = [edge - _DAY_S, edge, edge + _DAY_S]
        offsets[edge] = [_offset_s(clock, sample) for sample in samples]
        for left, right in zip(samples, samples[1:], strict=False):
            before, after = _offset_s(clock, left), _offset_s(clock, right)
            if before != after:
                change = _clock_change(clock, left, right)
                cuts.update((change + before, change + after))
    lo = start + min(offsets[start])
    hi = end + max(offsets[end])
    for offset in {*offsets[start], *offsets[end]}:
        cuts.update((start + offset, end + offset + 1))
    bounds = sorted({lo, hi + 1, *(cut for cut in cuts if lo < cut <= hi)})
    inside = reading_into(clock, np.array(bounds[:-1]), start, end)
    proof = []
    for left, right, belongs in zip(bounds[:-1], bounds[1:], inside, strict=True):
        if belongs:
            if proof and proof[-1][1] + 1 == left:
                proof[-1] = (proof[-1][0], right - 1)
            else:
                proof.append((left, right - 1))
    return BrokerWindow((lo, hi), proof)


def _clock_change(clock: BrokerClock, left: int, right: int) -> int:
    """The first true-UTC second after `left` at the offset the clock runs at on `right`, for the
    one clock change between them."""
    before = _offset_s(clock, left)
    while right - left > 1:
        middle = (left + right) // 2
        if _offset_s(clock, middle) == before:
            left = middle
        else:
            right = middle
    return right


def _offset_s(clock: BrokerClock, utc_epoch: int) -> int:
    return int(clock.offset_at(utc_epoch).total_seconds())


def reading_into(clock: BrokerClock, seconds: np.ndarray, start: int, end: int) -> np.ndarray:
    """Which broker seconds read into true-UTC [start, end] at either occurrence, so a window in a
    repeated hour's second pass holds the records stamped in it."""
    eras = Eras(clock)
    first = eras.to_utc(seconds)
    last = eras.last_occurrence(seconds)
    return ((start <= first) & (first <= end)) | ((start <= last) & (last <= end))
