"""Warnings, given once per broker hour across a process, of an epoch in an hour the broker's clock
repeats or skips, and of a bar dropped for opening in the true-UTC period of the bar before it.

State: the broker hours already warned of; nothing is persisted."""

import logging
import threading

logger = logging.getLogger(__name__)


class RepeatedHours:
    """The broker hours a process has warned of, shared by its threads."""

    def __init__(self) -> None:
        self._warned: set[int] = set()
        self._lock = threading.Lock()

    def warn(self, where: str, field: str, epoch: int, hour: int) -> None:
        """Warns that the epoch of `field` in what `where` names lies in the repeated broker hour
        `hour`, unless that hour was warned of."""
        if self._mark_warned(hour):
            logger.warning(
                "%s: %s %d is in the broker's repeated hour, read as its first occurrence",
                where,
                field,
                epoch,
            )

    def warn_skipped(self, where: str, field: str, epoch: int, hour: int) -> None:
        """Warns that the epoch of `field` in what `where` names lies in the broker hour `hour` its
        clock skips, unless that hour was warned of."""
        if self._mark_warned(hour):
            logger.warning(
                "%s: %s %d is in the hour the broker's clock skips, read at the offset before the "
                "change",
                where,
                field,
                epoch,
            )

    def warn_dropped_bar(self, where: str, field: str, epoch: int, hour: int) -> None:
        """Warns that the bar whose `field` in what `where` names is `epoch`, in the broker hour
        `hour`, opens in the true-UTC period of the bar before it and is dropped, unless that hour
        was warned of."""
        if self._mark_warned(hour):
            logger.warning(
                "%s: %s %d opens in the true-UTC period of the bar before it and is dropped",
                where,
                field,
                epoch,
            )

    def _mark_warned(self, hour: int) -> bool:
        """Marks `hour` warned of; whether it was not already."""
        with self._lock:
            warned = hour in self._warned
            self._warned.add(hour)
        return not warned
