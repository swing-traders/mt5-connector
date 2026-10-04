"""The warning that an epoch in the broker's repeated hour reads as its first occurrence, given once
per broker hour across everything a process converts.

State: the repeated broker hours already warned of; nothing is persisted."""

import logging
import threading

logger = logging.getLogger(__name__)


class RepeatedHours:
    """The broker's repeated hours a process has warned of, shared by its threads."""

    def __init__(self) -> None:
        self._warned: set[int] = set()
        self._lock = threading.Lock()

    def warn(self, where: str, field: str, epoch: int, hour: int) -> None:
        """Warns that the epoch of `field` in what `where` names lies in the repeated broker hour
        `hour`, unless that hour was warned of."""
        with self._lock:
            warned = hour in self._warned
            self._warned.add(hour)
        if not warned:
            logger.warning(
                "%s: %s %d is in the broker's repeated hour, read as its first occurrence",
                where,
                field,
                epoch,
            )
