"""Conversion between true UTC and the broker's clock — a zone's wall time plus a fixed offset,
written as if it were UTC — by tzdata's era at each instant converted."""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

_EPOCH = datetime(1970, 1, 1)
_EPOCH_UTC = datetime(1970, 1, 1, tzinfo=UTC)
_SECOND = timedelta(seconds=1)


class NonexistentWallTime(ValueError):
    """Raised when a broker epoch names a wall time its zone skips."""


@dataclass(frozen=True)
class BrokerClock:
    """The broker's clock: the wall time of `tz` plus `offset`. Zero is the venue's "no time" and
    converts to zero both ways."""

    tz: ZoneInfo
    offset: timedelta

    def to_utc(self, broker_epoch: int) -> int:
        """The true-UTC epoch of a broker epoch in seconds; a repeated wall time reads as its first
        occurrence."""
        if broker_epoch == 0:
            return 0
        else:
            return (self._local(broker_epoch) - _EPOCH_UTC) // _SECOND

    def to_utc_near(self, broker_epoch: int, utc_epoch: int) -> int:
        """The true-UTC epoch of a broker epoch in seconds; a repeated wall time reads as its
        occurrence nearer the true-UTC `utc_epoch`."""
        if broker_epoch == 0:
            return 0
        else:
            # Both occurrences are one instant for a wall time the zone does not repeat.
            local = self._local(broker_epoch)
            first = (local - _EPOCH_UTC) // _SECOND
            second = (local.replace(fold=1) - _EPOCH_UTC) // _SECOND
            if abs(second - utc_epoch) < abs(first - utc_epoch):
                return second
            else:
                return first

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
        local = self._local(broker_epoch)
        return local.utcoffset() > local.replace(fold=1).utcoffset()

    def _local(self, broker_epoch: int) -> datetime:
        """The zone's wall time a broker epoch names, at its first occurrence; raises
        NonexistentWallTime for a wall time the zone skips."""
        # fold=0 takes the offset before a transition and fold=1 the one after, so the offset rises
        # across a skipped wall time and falls across a repeated one.
        wall = _EPOCH + timedelta(seconds=broker_epoch) - self.offset
        local = wall.replace(tzinfo=self.tz)
        if local.utcoffset() < local.replace(fold=1).utcoffset():
            raise NonexistentWallTime(
                f"broker epoch {broker_epoch} names {wall.isoformat()}, which {self.tz.key} skips"
            )
        return local
