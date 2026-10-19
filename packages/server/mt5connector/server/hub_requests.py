"""The requests the hub has sent an EA and not yet answered, each under its deadline.

State: each pending request under the hub's own id — the consumer that asked it, none once that
consumer left; the consumer's id for it; the call it asks; the EA; its deadline and the timer armed
for it; how long a busy said its EA would wait; the next answer part expected, and the conversion
its parts go through. A request leaves on its last part, on an error, on its deadline and when its
EA leaves; whatever its EA sends for it after that is dropped with a warning. Nothing is
persisted."""

import asyncio
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass

from mt5connector.server.hub_utc import Results
from mt5connector.server.wire import calls

logger = logging.getLogger(__name__)


@dataclass
class Pending:
    """A request sent to an EA and not yet answered."""

    hub_id: int
    consumer: object | None
    client_id: int
    served: calls.Row
    symbol: str | None
    ea: object
    results: Results
    deadline: float
    timer: asyncio.TimerHandle | None = None
    busy_until: float | None = None
    next_part: int = 0


class Requests:
    """The pending requests, each answered once: by its EA, or on its deadline by `expire`."""

    def __init__(self, *, expire: Callable[[Pending], None]) -> None:
        self._expire = expire
        self._pending: dict[int, Pending] = {}
        self._asked_by: dict[object, int] = {}
        self._next_id = 1

    def add(
        self,
        consumer: object,
        client_id: int,
        served: calls.Row,
        symbol: str | None,
        ea: object,
        results: Results,
    ) -> Pending:
        """Takes a request as pending under the next hub id, its deadline the one its served call
        waits under."""
        hub_id = self._next_id
        self._next_id += 1
        deadline = time.monotonic() + _deadline_s(served.wait)
        pending = Pending(hub_id, consumer, client_id, served, symbol, ea, results, deadline)
        self._pending[hub_id] = pending
        self._asked_by[consumer] = self._asked_by.get(consumer, 0) + 1
        self._arm(pending)
        return pending

    def get(self, ea: object, hub_id: int) -> Pending | None:
        """The pending request of this id that EA was sent; None for any other."""
        pending = self._pending.get(hub_id)
        if pending is not None and pending.ea is ea:
            return pending
        else:
            return None

    def busy(self, pending: Pending, bound_s: float) -> None:
        """Moves a request's deadline to the busy's bound past now, with the margin, and holds its
        EA's keepalive tolerance to it."""
        pending.busy_until = time.monotonic() + bound_s + calls.BUSY_MARGIN_S
        pending.deadline = pending.busy_until
        self._arm(pending)

    def remove(self, pending: Pending) -> None:
        """Takes a request off the pending ones, its deadline disarmed."""
        if self._pending.pop(pending.hub_id, None) is not None and pending.consumer is not None:
            self._asked_by[pending.consumer] -= 1
            if self._asked_by[pending.consumer] == 0:
                del self._asked_by[pending.consumer]
        if pending.timer is not None:
            pending.timer.cancel()

    def asked_by(self, consumer: object) -> int:
        """How many of a consumer's requests are pending."""
        return self._asked_by.get(consumer, 0)

    def consumer_left(self, consumer: object) -> None:
        """Keeps a leaving consumer's requests pending on their EAs with no one to answer."""
        self._asked_by.pop(consumer, None)
        for pending in self._pending.values():
            if pending.consumer is consumer:
                pending.consumer = None

    def of_ea(self, ea: object) -> list[Pending]:
        return [pending for pending in self._pending.values() if pending.ea is ea]

    def busy_until(self, ea: object) -> float | None:
        """The latest a busy request of this EA may still wait; None while it has none."""
        bounds = [
            pending.busy_until
            for pending in self._pending.values()
            if pending.ea is ea and pending.busy_until is not None
        ]
        return max(bounds, default=None)

    def all(self) -> list[Pending]:
        return list(self._pending.values())

    def _arm(self, pending: Pending) -> None:
        if pending.timer is not None:
            pending.timer.cancel()
        delay = max(pending.deadline - time.monotonic(), 0.0)
        pending.timer = asyncio.get_running_loop().call_later(delay, self._expired, pending)

    def _expired(self, pending: Pending) -> None:
        if self._pending.get(pending.hub_id) is pending:
            self.remove(pending)
            self._expire(pending)


def _deadline_s(wait: calls.Wait) -> float:
    """How long a request of a call that waits so may go unanswered."""
    if wait is calls.Wait.READ:
        return calls.READ_DEADLINE_S
    elif wait is calls.Wait.COMMAND:
        return calls.COMMAND_DEADLINE_S
    else:
        return calls.HISTORY_BUSY_S
