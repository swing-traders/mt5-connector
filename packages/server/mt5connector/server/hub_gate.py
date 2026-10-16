"""The hub's clock gate: the broker clock verified against the trade-server time the spawner relays,
under the HTTP server's rules — a sample fresh for its max-age, from a terminal connected without a
break for that long, measured within the tolerance — and re-verified on its interval. A missed
bootstrap window or a sample the broker clock contradicts never ends the process: the gate stays
shut until a sample verifies it.

State: the latest sample, the monotonic time it arrived and when its unbroken connected run began;
the sample last measured; the latest verification, held while the latest sample is verifiable and
cleared when it is not; when the next re-verification is due; whether the bootstrap window passed
with none; and the wake-up armed for the next time verifiability can change. A sample is measured
once until its re-verification is due, so a contradicted one is measured again only once a newer
sample arrives. Nothing is persisted."""

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime

from mt5connector.server.wire import binary
from mt5connector.server.wire.broker_clock import BrokerClock

logger = logging.getLogger(__name__)

# The most the trade server's time may stand from the hub's clock.
TOLERANCE_S = 120


@dataclass(frozen=True)
class ClockVerification:
    """A sample the broker clock agreed with, in true UTC, and what its verification measured."""

    symbol: str
    trade_server: int
    current: int
    gmt: int
    skew_s: int
    offset_s: int


@dataclass(frozen=True)
class _Received:
    sample: binary.ServerTime
    arrived: float
    connected_since: float | None


class ClockGate:
    """Opens on a verifying sample and shuts when the latest sample stops being verifiable."""

    def __init__(
        self, clock: BrokerClock, *, max_age_s: float, check_s: float, bootstrap_s: float
    ) -> None:
        self._clock = clock
        self._max_age_s = max_age_s
        self._check_s = check_s
        self._bootstrap_s = bootstrap_s
        self._latest: _Received | None = None
        self._measured: _Received | None = None
        self.verification: ClockVerification | None = None
        self._due = 0.0
        self._contradicted = False
        self.bootstrap_missed = False
        self._wake: asyncio.TimerHandle | None = None

    def start(self) -> None:
        """Arms the bootstrap window."""
        asyncio.get_running_loop().call_later(self._bootstrap_s, self._bootstrap_ends)

    def write(self, sample: binary.ServerTime) -> None:
        """Takes a sample as the latest: one disconnected, or arriving a max-age or more after the
        one before, starts a new connected run."""
        arrived = time.monotonic()
        previous = self._latest
        if not sample.connected:
            connected_since = None
        elif (
            previous is not None
            and previous.connected_since is not None
            and arrived - previous.arrived < self._max_age_s
        ):
            connected_since = previous.connected_since
        else:
            connected_since = arrived
        self._latest = _Received(sample, arrived, connected_since)
        self._evaluate()

    def sample_age_s(self) -> float | None:
        """How long ago the latest sample arrived; None before the first."""
        if self._latest is None:
            return None
        else:
            return time.monotonic() - self._latest.arrived

    def is_fresh(self) -> bool:
        """Whether the latest sample says the terminal is connected and arrived within its
        max-age."""
        return self._is_fresh(time.monotonic())

    def _evaluate(self) -> None:
        """Verifies the latest sample when it becomes verifiable or its re-verification is due,
        clears the verification when it stops being verifiable, and arms the wake-up for the next
        change."""
        now = time.monotonic()
        if self._is_verifiable(now):
            if (self.verification is None and self._measured is not self._latest) or (
                now >= self._due
            ):
                self._measured = self._latest
                self._verify(self._latest.sample)
                self._due = now + self._check_s
        elif self.verification is not None:
            self.verification = None
            logger.info(
                "broker clock unverified: the latest server-time sample arrived %d s ago, "
                "connected=%s",
                now - self._latest.arrived,
                self._latest.sample.connected,
            )
        self._arm(now)

    def _arm(self, now: float) -> None:
        if self._wake is not None:
            self._wake.cancel()
            self._wake = None
        if self.verification is not None:
            when = min(self._latest.arrived + self._max_age_s, self._due)
        elif self._is_fresh(now) and not self._is_verifiable(now):
            when = self._latest.connected_since + self._max_age_s
        else:
            # Unverifiable for good, or contradicted: only a newer sample changes the answer.
            when = None
        if when is not None:
            self._wake = asyncio.get_running_loop().call_later(max(when - now, 0.0), self._evaluate)

    def _verify(self, sample: binary.ServerTime) -> None:
        """Measures the sample's trade-server time against the hub's clock: within the tolerance it
        verifies the clock, past it the gate shuts with one critical log until a sample agrees."""
        now = int(time.time())
        offset_s = int(self._clock.offset_at(now).total_seconds())
        # The sample reads the terminal's clock live, so in the broker's repeated hour its times
        # name the occurrence nearer the hub's clock, not a record's first occurrence.
        trade_server = self._clock.to_utc_near(sample.trade_server, now)
        skew_s = trade_server - now
        if abs(skew_s) > TOLERANCE_S:
            self.verification = None
            if not self._contradicted:
                self._contradicted = True
                logger.critical(
                    "broker clock: %s trade server at broker epoch %d reads %s, %+d s from the "
                    "hub's clock's %s, under offset %+d s",
                    sample.symbol,
                    sample.trade_server,
                    _iso(trade_server),
                    skew_s,
                    _iso(now),
                    offset_s,
                )
        else:
            if self.verification is None:
                logger.info(
                    "broker clock verified on %s: trade server at %s, %+d s from the hub's clock; "
                    "offset %+d s",
                    sample.symbol,
                    _iso(trade_server),
                    skew_s,
                    offset_s,
                )
            self._contradicted = False
            self.verification = ClockVerification(
                symbol=sample.symbol,
                trade_server=trade_server,
                current=self._clock.to_utc_near(sample.current, now),
                gmt=sample.gmt,
                skew_s=skew_s,
                offset_s=offset_s,
            )

    def _bootstrap_ends(self) -> None:
        if self.verification is None and not self.bootstrap_missed:
            self.bootstrap_missed = True
            if self._is_fresh(time.monotonic()):
                logger.critical(
                    "broker clock: the terminal was not connected for %s s without a break in %s s",
                    self._max_age_s,
                    self._bootstrap_s,
                )
            else:
                logger.critical(
                    "broker clock: no fresh server-time sample in %s s", self._bootstrap_s
                )

    def _is_fresh(self, now: float) -> bool:
        return (
            self._latest is not None
            and self._latest.sample.connected
            and now - self._latest.arrived < self._max_age_s
        )

    def _is_verifiable(self, now: float) -> bool:
        return self._is_fresh(now) and now - self._latest.connected_since >= self._max_age_s


def _iso(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, UTC).isoformat(timespec="seconds")
