"""The broker clock's verification against the live tick of the clock symbol.

State: `ready`, set once the first verification has passed or found the market closed, and never
cleared; the server exits on any failed verification."""

import logging
import os
import threading
import time
from datetime import UTC, datetime

from mt5connect import mirror
from mt5connect.broker_clock import BrokerClock
from mt5server.app.terminal import Failed, Terminal

logger = logging.getLogger(__name__)

_SYMBOL_INFO_TICK = mirror.FUNCTIONS[mirror.FunctionName.SYMBOL_INFO_TICK]
# The time between a verification's two reads: a tick whose time_msc moved across it is fresh.
_READ_GAP_S = 5
_TOLERANCE_S = 120


class ClockCheckFailed(Exception):
    """Raised when the clock symbol's tick contradicts the broker clock's schedule or is missing at
    connect."""


class ClockCheck:
    """Verifies the broker clock's schedule against the clock symbol's live tick."""

    def __init__(self, terminal: Terminal, clock: BrokerClock, symbol: str) -> None:
        self._terminal = terminal
        self._clock = clock
        self._symbol = symbol
        self.ready = threading.Event()

    def run(self, interval_s: int) -> None:
        """Verifies at once, then every interval; a failed verification exits the process."""
        try:
            self.verify(at_connect=True)
            self.ready.set()
            while True:
                time.sleep(interval_s)
                self.verify(at_connect=False)
        except ClockCheckFailed as failure:
            logger.critical("broker clock: %s", failure)
            os._exit(1)
        except Exception:
            logger.critical("broker clock: the verification failed", exc_info=True)
            os._exit(1)

    def verify(self, at_connect: bool) -> None:
        """Reads the clock symbol's tick twice, each read under the terminal's lock, and measures a
        fresh one against the server's clock; a stale one defers to the next verification."""
        first = self._terminal.call(_SYMBOL_INFO_TICK, {"symbol": self._symbol})
        time.sleep(_READ_GAP_S)
        second = self._terminal.call(_SYMBOL_INFO_TICK, {"symbol": self._symbol})
        now = int(time.time())
        offset_s = int(self._clock.offset_at(now).total_seconds())
        failures = [
            outcome.last_error for outcome in (first, second) if isinstance(outcome, Failed)
        ]
        if failures:
            if at_connect:
                raise ClockCheckFailed(f"no {self._symbol} tick: {failures[0]}")
            else:
                logger.warning(
                    "broker clock verification deferred: no %s tick: %s", self._symbol, failures[0]
                )
        elif second.value.time_msc == first.value.time_msc:
            logger.info(
                "broker clock verification deferred: %s did not tick in %s s, the market is "
                "closed; offset %+d s",
                self._symbol,
                _READ_GAP_S,
                offset_s,
            )
        else:
            tick_utc = self._clock.to_utc(second.value.time)
            skew_s = tick_utc - now
            logger.info(
                "broker clock measured on %s: tick at %s, %+d s from the server clock; "
                "offset %+d s",
                self._symbol,
                _iso(tick_utc),
                skew_s,
                offset_s,
            )
            if abs(skew_s) > _TOLERANCE_S:
                raise ClockCheckFailed(
                    f"{self._symbol} tick at broker epoch {second.value.time} reads "
                    f"{_iso(tick_utc)}, {skew_s:+d} s from the server clock's {_iso(now)}, "
                    f"under offset {offset_s:+d} s"
                )


def _iso(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, UTC).isoformat(timespec="seconds")
