"""Arrays of broker epochs read on the broker's clock, BrokerClock asked once per broker hour they
span: each second's true UTC at its first and its last occurrence, exactly as BrokerClock reads it
alone, and whether its hour is one the clock repeats or skips.

State: each broker hour an Eras met, with its offsets from true UTC at either occurrence and whether
the clock repeats or skips it — or nothing, for an hour whose readings change within it, whose
seconds are each asked of BrokerClock. Nothing is persisted."""

from dataclasses import dataclass

import numpy as np

from mt5connector.server.wire.broker_clock import BrokerClock

_HOUR_S = 3_600


@dataclass(frozen=True)
class _Hour:
    """A broker hour every second of which the clock reads alike."""

    # Broker seconds less true-UTC seconds, at the first and at the last occurrence.
    first: int
    last: int
    repeated: bool
    skipped: bool


class Eras:
    """The broker clock's readings of whole arrays of broker epochs."""

    def __init__(self, clock: BrokerClock) -> None:
        self._clock = clock
        self._hours: dict[int, _Hour | None] = {}

    def to_utc(self, seconds: np.ndarray) -> np.ndarray:
        """BrokerClock.to_utc of each broker second."""
        return self._read(seconds, seconds, 1, first=True)

    def to_utc_msc(self, milliseconds: np.ndarray) -> np.ndarray:
        """BrokerClock.to_utc_msc of each broker millisecond."""
        return self._read(milliseconds, np.asarray(milliseconds) // 1000, 1000, first=True)

    def last_occurrence(self, seconds: np.ndarray) -> np.ndarray:
        """The true UTC of each broker second at its last occurrence, as BrokerClock.occurrences
        gives it."""
        return self._read(seconds, seconds, 1, first=False)

    def first_flagged(self, seconds: np.ndarray) -> tuple[int, bool, bool] | None:
        """Of the broker seconds of one hour, in order, the position of the first the clock repeats
        or skips, and which; None for none. Each second is judged where the hour's readings change
        within it."""
        hour = self._hour(int(seconds[0]) // _HOUR_S)
        if hour is not None and (hour.repeated or hour.skipped):
            return 0, hour.repeated, hour.skipped
        elif hour is None:
            for position, second in enumerate(seconds.tolist()):
                repeated = self._clock.is_ambiguous(second)
                skipped = self._clock.is_skipped(second)
                if repeated or skipped:
                    return position, repeated, skipped
        return None

    def _read(
        self, values: np.ndarray, seconds: np.ndarray, scale: int, *, first: bool
    ) -> np.ndarray:
        values = np.asarray(values, dtype=np.int64)
        hours, index = np.unique(
            np.asarray(seconds, dtype=np.int64) // _HOUR_S, return_inverse=True
        )
        offsets = np.zeros(len(hours), dtype=np.int64)
        alike = np.ones(len(hours), dtype=bool)
        for position, hour_index in enumerate(hours.tolist()):
            hour = self._hour(hour_index)
            if hour is None:
                alike[position] = False
            elif first:
                offsets[position] = hour.first
            else:
                offsets[position] = hour.last
        read = values - offsets[index] * scale
        for position in np.flatnonzero(~alike[index]).tolist():
            read[position] = self._one(int(values[position]), scale, first=first)
        # Zero is the venue's "no time", and converts to zero.
        read[values == 0] = 0
        return read

    def _one(self, value: int, scale: int, *, first: bool) -> int:
        if first and scale == 1:
            return self._clock.to_utc(value)
        elif first:
            return self._clock.to_utc_msc(value)
        else:
            return self._clock.occurrences(value)[1]

    def _hour(self, hour_index: int) -> _Hour | None:
        """The readings of a broker hour, the clock asked at its first and its last second; None
        where they differ."""
        if hour_index not in self._hours:
            start = hour_index * _HOUR_S
            # The hour's first second but for broker zero, which BrokerClock reads as no time.
            opening = start if start != 0 else 1
            closing = start + _HOUR_S - 1
            readings = []
            for second in (opening, closing):
                first, last = self._clock.occurrences(second)
                readings.append((second - first, second - last, self._clock.is_skipped(second)))
            if readings[0] == readings[1]:
                first, last, skipped = readings[0]
                self._hours[hour_index] = _Hour(first, last, first != last, skipped)
            else:
                self._hours[hour_index] = None
        return self._hours[hour_index]
