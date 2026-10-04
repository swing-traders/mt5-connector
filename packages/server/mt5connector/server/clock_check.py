"""The broker clock's verification against the trade-server time the terminal's EAs relay.

State: `status`, the latest verification, held while the latest relayed sample is fresh and the
terminal has been connected without a break for the maximum age, and cleared while it is not; the
server exits on a sample the broker clock contradicts, and when none verifies it at connect."""

import logging
import os
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime

from mt5connector.server.server_time import Received, ServerTimeSample, ServerTimeSink
from mt5connector.server.wire.broker_clock import BrokerClock

logger = logging.getLogger(__name__)

_TOLERANCE_S = 120


class ClockCheckFailed(Exception):
    """Raised when a sample contradicts the broker clock's schedule, or none is fresh at connect."""


@dataclass(frozen=True)
class ClockVerification:
    """A sample the broker clock agreed with, in true UTC, and what its verification measured."""

    symbol: str
    trade_server: int
    current: int
    gmt: int
    skew_s: int
    offset_s: int


class ClockStatus:
    """The broker clock's latest verification, or None while the clock is not verified."""

    def __init__(self) -> None:
        self._verification: ClockVerification | None = None
        self._lock = threading.Lock()

    def read(self) -> ClockVerification | None:
        with self._lock:
            return self._verification

    def set(self, verification: ClockVerification) -> None:
        with self._lock:
            self._verification = verification

    def clear(self) -> None:
        with self._lock:
            self._verification = None


class ClockCheck:
    """Verifies the broker clock against the relayed trade-server time, keeping `status` current."""

    def __init__(
        self,
        server_times: ServerTimeSink,
        clock: BrokerClock,
        *,
        max_age_s: int,
        check_s: int,
        bootstrap_s: int,
    ) -> None:
        self._server_times = server_times
        self._clock = clock
        self._max_age_s = max_age_s
        self._check_s = check_s
        self._bootstrap_s = bootstrap_s
        self.status = ClockStatus()

    def run(self) -> None:
        """Verifies the first verifiable sample, then follows the samples until the process ends."""
        try:
            received = self._bootstrap()
            self._watch(received)
        except ClockCheckFailed as failure:
            logger.critical("broker clock: %s", failure)
            os._exit(1)
        except Exception:
            logger.critical("broker clock: the verification failed", exc_info=True)
            os._exit(1)

    def _bootstrap(self) -> Received:
        """Verifies the first sample to become verifiable within the bootstrap window."""
        deadline = time.monotonic() + self._bootstrap_s
        received = None
        while not self._is_verifiable(received, time.monotonic()):
            now = time.monotonic()
            remaining = deadline - now
            fresh = self._is_fresh(received, now)
            if remaining <= 0:
                if fresh:
                    raise ClockCheckFailed(
                        f"the terminal was not connected for {self._max_age_s} s without a break "
                        f"in {self._bootstrap_s} s"
                    )
                else:
                    raise ClockCheckFailed(f"no fresh server-time sample in {self._bootstrap_s} s")
            elif fresh:
                timeout = min(remaining, received.connected_since + self._max_age_s - now)
            else:
                timeout = remaining
            received = self._server_times.wait_newer(received, timeout)
        self.status.set(self._verify(received.sample))
        logger.info("broker clock verified")
        return received

    def _watch(self, received: Received) -> None:
        """Wakes when a sample arrives, when the latest goes stale, when its connected run matures,
        and when a re-verification of it is due."""
        due = time.monotonic() + self._check_s
        while True:
            now = time.monotonic()
            if self.status.read() is not None:
                timeout = min(due, received.arrived + self._max_age_s) - now
            elif self._is_fresh(received, now):
                timeout = received.connected_since + self._max_age_s - now
            else:
                timeout = None
            received = self._server_times.wait_newer(received, timeout)
            now = time.monotonic()
            if not self._is_verifiable(received, now):
                if self.status.read() is not None:
                    self.status.clear()
                    logger.info(
                        "broker clock unverified: the latest server-time sample arrived %d s ago, "
                        "connected=%s",
                        now - received.arrived,
                        received.sample.connected,
                    )
            elif self.status.read() is None:
                self.status.set(self._verify(received.sample))
                logger.info("broker clock verified")
                due = now + self._check_s
            elif now >= due:
                self.status.set(self._verify(received.sample))
                due = now + self._check_s

    def _is_fresh(self, received: Received | None, now: float) -> bool:
        """Whether a sample says the terminal is connected and arrived less than the maximum age
        ago."""
        return (
            received is not None
            and received.sample.connected
            and now - received.arrived < self._max_age_s
        )

    def _is_verifiable(self, received: Received | None, now: float) -> bool:
        """Whether a sample is fresh and the terminal has been connected without a break for the
        maximum age."""
        return self._is_fresh(received, now) and now - received.connected_since >= self._max_age_s

    def _verify(self, sample: ServerTimeSample) -> ClockVerification:
        """Measures the sample's trade-server time against the server's clock; raises
        ClockCheckFailed past the tolerance."""
        now = int(time.time())
        offset_s = int(self._clock.offset_at(now).total_seconds())
        trade_server = self._clock.to_utc(sample.trade_server)
        skew_s = trade_server - now
        logger.info(
            "broker clock measured on %s: trade server at %s, %+d s from the server clock; "
            "offset %+d s",
            sample.symbol,
            _iso(trade_server),
            skew_s,
            offset_s,
        )
        if abs(skew_s) > _TOLERANCE_S:
            raise ClockCheckFailed(
                f"{sample.symbol} trade server at broker epoch {sample.trade_server} reads "
                f"{_iso(trade_server)}, {skew_s:+d} s from the server clock's {_iso(now)}, "
                f"under offset {offset_s:+d} s"
            )
        else:
            return ClockVerification(
                symbol=sample.symbol,
                trade_server=trade_server,
                current=self._clock.to_utc(sample.current),
                gmt=sample.gmt,
                skew_s=skew_s,
                offset_s=offset_s,
            )


def _iso(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, UTC).isoformat(timespec="seconds")
