"""Conversion between true UTC and the broker's clock — a zone's wall time plus a fixed offset,
written as if it were UTC — by tzdata's era at each instant converted."""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

_EPOCH = datetime(1970, 1, 1)
_EPOCH_UTC = datetime(1970, 1, 1, tzinfo=UTC)
_SECOND = timedelta(seconds=1)


@dataclass(frozen=True)
class BrokerClock:
    """The broker's clock: the wall time of `tz` plus `offset`. Zero is the venue's "no time" and
    converts to zero both ways."""

    tz: ZoneInfo
    offset: timedelta

    def to_utc(self, broker_epoch: int) -> int:
        """The true-UTC epoch of a broker epoch in seconds; a repeated wall time reads as its first
        occurrence, and a skipped one at the offset before the change, the broker's clock moving
        after the zone's."""
        if broker_epoch == 0:
            return 0
        else:
            return (self._local(broker_epoch) - _EPOCH_UTC) // _SECOND

    def to_utc_near(self, broker_epoch: int, utc_epoch: int) -> int:
        """The true-UTC epoch of a broker epoch in seconds; a repeated wall time reads as its
        occurrence nearer the true-UTC `utc_epoch`, a skipped one as to_utc reads it."""
        first, last = self.occurrences(broker_epoch)
        if abs(last - utc_epoch) < abs(first - utc_epoch):
            return last
        else:
            return first

    def occurrences(self, broker_epoch: int) -> tuple[int, int]:
        """The true-UTC epochs of a broker epoch in seconds at its first and its last occurrence:
        one instant but for a wall time its zone repeats, and a skipped one as to_utc reads it."""
        first = self.to_utc(broker_epoch)
        if self.is_ambiguous(broker_epoch):
            return first, (self._local(broker_epoch).replace(fold=1) - _EPOCH_UTC) // _SECOND
        else:
            return first, first

    def to_utc_msc(self, broker_epoch_msc: int) -> int:
        """The true-UTC epoch of a broker epoch in milliseconds."""
        if broker_epoch_msc == 0:
            return 0
        else:
            seconds, milliseconds = divmod(broker_epoch_msc, 1000)
            return (self._local(seconds) - _EPOCH_UTC) // _SECOND * 1000 + milliseconds

    def to_broker(self, utc_epoch: int) -> int:
        """The broker epoch in seconds of a true-UTC epoch."""
        if utc_epoch == 0:
            return 0
        else:
            local = (_EPOCH_UTC + timedelta(seconds=utc_epoch)).astimezone(self.tz)
            return (local.replace(tzinfo=None) + self.offset - _EPOCH) // _SECOND

    def offset_at(self, utc_epoch: int) -> timedelta:
        """How far the broker's clock runs ahead of true UTC at a true-UTC epoch."""
        local = (_EPOCH_UTC + timedelta(seconds=utc_epoch)).astimezone(self.tz)
        return local.utcoffset() + self.offset

    def is_ambiguous(self, broker_epoch: int) -> bool:
        """Whether a broker epoch in seconds names a wall time its zone repeats."""
        # fold=0 takes the offset before a transition and fold=1 the one after, so the offset falls
        # across a repeated wall time and rises across a skipped one.
        local = self._local(broker_epoch)
        return local.utcoffset() > local.replace(fold=1).utcoffset()

    def is_skipped(self, broker_epoch: int) -> bool:
        """Whether a broker epoch in seconds names a wall time its zone skips."""
        local = self._local(broker_epoch)
        return local.utcoffset() < local.replace(fold=1).utcoffset()

    def _local(self, broker_epoch: int) -> datetime:
        """The zone's wall time a broker epoch names, under the offset before any transition at
        it."""
        wall = _EPOCH + timedelta(seconds=broker_epoch) - self.offset
        return wall.replace(tzinfo=self.tz)
