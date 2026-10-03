"""The history protocol: bar and tick windows read from the terminal and answered with their rows
once the terminal's answers prove them, and the floors measured on the way.

State: FloorStore, each (symbol, series)'s floor on the broker's clock — seconds for bars,
milliseconds for ticks — with the true-UTC time it was measured, a generation that steps on every
change of value, and whether the stub or a coarse prefix at the series' start set it; a stub
measured again never moves a coarse floor back. Nothing is persisted, so floors are measured again
after a restart."""

import logging
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum

import numpy as np

from mt5connect import mirror
from mt5connect.broker_clock import BrokerClock
from mt5connect.history_wire import (
    BAR_PERIOD_S,
    BAR_TIMEFRAME,
    SPAN_MARGIN,
    TICK_FLAGS,
    Series,
    ServerCode,
    TickFlags,
)
from mt5server.app.encoding import broker_datetime, encode
from mt5server.app.terminal import Answered, Failed, Terminal

logger = logging.getLogger(__name__)

_COPY_RATES_FROM = mirror.FUNCTIONS[mirror.FunctionName.COPY_RATES_FROM]
_COPY_RATES_RANGE = mirror.FUNCTIONS[mirror.FunctionName.COPY_RATES_RANGE]
_COPY_TICKS_RANGE = mirror.FUNCTIONS[mirror.FunctionName.COPY_TICKS_RANGE]
_COPY_TICKS_FROM = mirror.FUNCTIONS[mirror.FunctionName.COPY_TICKS_FROM]
_SYMBOL_INFO_TICK = mirror.FUNCTIONS[mirror.FunctionName.SYMBOL_INFO_TICK]
_SYMBOL_SELECT = mirror.FUNCTIONS[mirror.FunctionName.SYMBOL_SELECT]
_TERMINAL_INFO = mirror.FUNCTIONS[mirror.FunctionName.TERMINAL_INFO]
_NO_RATES = np.zeros(0, dtype=np.dtype(list(mirror.RATES.dtype)))
_NO_TICKS = np.zeros(0, dtype=np.dtype(list(mirror.TICKS.dtype)))

_DAY_S = 86_400
_WEEK_S = 7 * _DAY_S
# UTC weeks start on Monday; 1970-01-05 was the first.
_FIRST_MONDAY_S = 4 * _DAY_S
# Before any plausible history, where the terminal answers a window with the series' first row.
_BEFORE_HISTORY_S = _DAY_S


class FloorOrigin(StrEnum):
    """What set a floor: the stub, the one row the terminal answers for a window before any
    plausible history, or the first authentic row past a coarse prefix at the series' start."""

    STUB = "stub"
    COARSE = "coarse"


@dataclass(frozen=True)
class Floor:
    """Where a series' rows begin, as the store holds it."""

    # The broker's clock: seconds for bars, milliseconds for ticks.
    value: int
    # True UTC seconds.
    measured_at: int
    generation: int
    origin: FloorOrigin


class FloorStore:
    """The floor measured for each (symbol, series)."""

    def __init__(self) -> None:
        self._floors: dict[tuple[str, Series], Floor] = {}
        self._lock = threading.Lock()

    def read(self, symbol: str, series: Series) -> Floor | None:
        with self._lock:
            return self._floors.get((symbol, series))

    def of_symbol(self, symbol: str) -> dict[Series, Floor]:
        with self._lock:
            return {
                series: floor for (owner, series), floor in self._floors.items() if owner == symbol
            }

    def write(
        self, symbol: str, series: Series, value: int, measured_at: int, origin: FloorOrigin
    ) -> Floor:
        """Records a measurement and answers the floor it leaves in force, whose generation steps
        when the value changes."""
        with self._lock:
            last = self._floors.get((symbol, series))
            if last is None:
                floor = Floor(value, measured_at, 1, origin)
            elif (
                last.origin is FloorOrigin.COARSE
                and origin is FloorOrigin.STUB
                and value <= last.value
            ):
                # The stub of a series served with a coarse prefix is that prefix's first row.
                floor = Floor(last.value, measured_at, last.generation, last.origin)
            elif last.value != value:
                floor = Floor(value, measured_at, last.generation + 1, origin)
            else:
                floor = Floor(value, measured_at, last.generation, origin)
            self._floors[(symbol, series)] = floor
            return floor


@dataclass(frozen=True)
class Syncing:
    """The answer to a window the terminal's answers do not prove yet."""

    last_error: tuple[int, str]
    retry_after_s: int


@dataclass(frozen=True)
class _Gap:
    """A stretch of a window that no read answered a row for."""

    # Broker seconds.
    lo: int
    hi: int
    rows_before: bool
    rows_after: bool


class _Unanswerable(Exception):
    """Raised when a request fails, with the error its failure envelope reports."""

    def __init__(self, code: int, message: str) -> None:
        super().__init__(message)
        self.last_error = (code, message)


class _Unproven(Exception):
    """Raised when the terminal's answers leave a window unproven, naming the answer that did."""


class _Calls:
    """One request's package calls, and the last_error() the latest of them left."""

    def __init__(self, terminal: Terminal) -> None:
        self._terminal = terminal
        self.last_error = mirror.SUCCESS

    def call(self, function: mirror.Function, arguments: dict[str, object]) -> Answered | Failed:
        outcome = self._terminal.call(function, arguments)
        self.last_error = outcome.last_error
        return outcome


class History:
    """The protocol run against the terminal, keeping the floors it measures in the store."""

    def __init__(
        self,
        terminal: Terminal,
        clock: BrokerClock,
        floors: FloorStore,
        *,
        retry_s: int,
        floor_ttl_s: int,
    ) -> None:
        self._terminal = terminal
        self._clock = clock
        self._floors = floors
        self._retry_s = retry_s
        self._floor_ttl_s = floor_ttl_s

    def bars(
        self, symbol: str, series: Series, start: int, end: int
    ) -> Answered | Failed | Syncing:
        """The bars opened in [start, end], true-UTC epochs inclusive at both ends, once the
        terminal's answers prove them."""
        calls = _Calls(self._terminal)
        try:
            rows = self._bars(calls, symbol, series, start, end)
        except _Unanswerable as failure:
            outcome = Failed(failure.last_error)
        except _Unproven as unproven:
            outcome = self._syncing(f"{symbol} {series} {start}..{end}", unproven)
        else:
            outcome = Answered(encode(_COPY_RATES_RANGE, rows, self._clock), calls.last_error)
        return outcome

    def ticks(
        self, symbol: str, start: int, end: int, flags: TickFlags
    ) -> Answered | Failed | Syncing:
        """The ticks from the start of second `start` through the end of second `end`, true UTC,
        once the terminal's answers prove them."""
        calls = _Calls(self._terminal)
        try:
            rows = self._ticks(calls, symbol, start, end, flags)
        except _Unanswerable as failure:
            outcome = Failed(failure.last_error)
        except _Unproven as unproven:
            outcome = self._syncing(f"{symbol} {Series.TICKS} {start}..{end}", unproven)
        else:
            outcome = Answered(encode(_COPY_TICKS_RANGE, rows, self._clock), calls.last_error)
        return outcome

    def ranges(self, symbol: str) -> Answered | Failed:
        """The terminal's MaxBars and every floor measured for the symbol, in true UTC."""
        outcome = self._terminal.call(_TERMINAL_INFO, {})
        if isinstance(outcome, Failed):
            return outcome
        else:
            floors = self._floors.of_symbol(symbol)
            ranges = {}
            for series in Series:
                if series in floors:
                    ranges[series.value] = {
                        "floor": self._utc_floor(series, floors[series]),
                        "measured_at": floors[series].measured_at,
                        "generation": floors[series].generation,
                    }
            return Answered(
                {"maxbars": outcome.value.maxbars, "ranges": ranges}, outcome.last_error
            )

    def _bars(self, calls: _Calls, symbol: str, series: Series, start: int, end: int) -> np.ndarray:
        self._select(calls, symbol)
        span_s = self._span_s(calls, series)
        lo = self._clock.to_broker(start)
        hi = self._clock.to_broker(end)
        stored = self._floors.read(symbol, series)
        kept = self._is_fresh(stored)
        if kept:
            floor = stored
        else:
            floor = self._measure_bar_floor(calls, symbol, series)
        if floor is not None and hi < floor.value:
            rows = _NO_RATES
        elif floor is None:
            rows = self._read_bars(calls, symbol, series, lo, hi, span_s, floor, kept)
        else:
            rows = self._read_bars(
                calls, symbol, series, max(lo, floor.value), hi, span_s, floor, kept
            )
        return rows

    def _read_bars(
        self,
        calls: _Calls,
        symbol: str,
        series: Series,
        lo: int,
        hi: int,
        span_s: int,
        floor: Floor | None,
        kept: bool,
    ) -> np.ndarray:
        """The bars opened in [lo, hi] on the broker's clock, read chunk by chunk, on the series'
        grid and from its floor on; raises _Unproven for a stretch no chunk answered a bar for that
        nothing proves empty."""
        period = BAR_PERIOD_S[series]
        chunks = _chunks(lo, hi, span_s)
        parts = []
        for chunk_lo, chunk_hi in chunks:
            parts.append(self._rates(calls, symbol, series, chunk_lo, chunk_hi))
        rows = np.concatenate(parts)
        off_grid = _first_off_grid(rows["time"], period)
        if off_grid is not None:
            at = self._clock.to_utc(int(rows["time"][off_grid]))
            raise _Unanswerable(mirror.RES_E_FAIL, f"{symbol} {series}: row {at} is off the grid")
        for gap in _gaps(chunks, parts):
            floor = self._prove_no_bars(calls, symbol, series, gap, span_s, floor, kept)
        # A coarse prefix can only lead a series, so it is sought only in an answer beginning at the
        # floor the gap proofs leave in force.
        if floor is not None and len(rows) > 0 and int(rows["time"][0]) == floor.value:
            prefix = _coarse_prefix(rows["time"], period)
            if prefix > 0:
                floor = self._move_floor(symbol, series, int(rows["time"][prefix]), prefix)
        if floor is None:
            answer = rows
        else:
            answer = rows[rows["time"] >= floor.value]
        return answer

    def _prove_no_bars(
        self,
        calls: _Calls,
        symbol: str,
        series: Series,
        gap: _Gap,
        span_s: int,
        floor: Floor | None,
        kept: bool,
    ) -> Floor | None:
        """Raises _Unproven unless no bar lies in the gap, and answers the floor then in force: a
        kept floor is measured again for a gap no row precedes, since the terminal answers a window
        between a stale floor and its own with its own first bar alone."""
        if kept and not gap.rows_before and gap.hi >= floor.value:
            measured = self._measure_bar_floor(calls, symbol, series)
            if measured is not None:
                floor = measured
        if floor is not None and gap.hi < floor.value:
            reason = None
        else:
            reason = self._unproven(
                calls,
                symbol,
                gap,
                rows_before=lambda: self._bar_before(calls, symbol, series, gap),
                record_after=lambda: self._bar_after(calls, symbol, series, gap, span_s),
            )
        if reason is not None:
            raise _Unproven(f"no {series} bar for broker {gap.lo}..{gap.hi}: {reason}")
        return floor

    def _ticks(
        self, calls: _Calls, symbol: str, start: int, end: int, flags: TickFlags
    ) -> np.ndarray:
        self._select(calls, symbol)
        last = self._clock.to_broker(end) * 1000 + 999
        stored = self._floors.read(symbol, Series.TICKS)
        kept = self._is_fresh(stored)
        if kept:
            floor = stored
        else:
            floor = self._measure_tick_floor(calls, symbol, flags)
        if floor is not None and last < floor.value:
            rows = _NO_TICKS
        elif floor is None:
            rows = self._read_ticks(calls, symbol, start, end, flags, floor, kept)
        else:
            read_from = max(start, self._utc_floor(Series.TICKS, floor))
            rows = self._read_ticks(calls, symbol, read_from, end, flags, floor, kept)
        return rows

    def _read_ticks(
        self,
        calls: _Calls,
        symbol: str,
        start: int,
        end: int,
        flags: TickFlags,
        floor: Floor | None,
        kept: bool,
    ) -> np.ndarray:
        """The ticks from the start of true-UTC second `start` through the end of `end`, in time
        order; raises _Unproven when there are none and nothing proves the window empty."""
        gap = _Gap(
            self._clock.to_broker(start),
            self._clock.to_broker(end),
            rows_before=False,
            rows_after=False,
        )
        read = self._read(
            calls,
            f"{symbol} ticks broker {gap.lo}..{gap.hi}",
            _COPY_TICKS_RANGE,
            self._ticks_arguments(symbol, start, end, flags),
        )
        times = read["time_msc"]
        rows = read[(times >= gap.lo * 1000) & (times <= gap.hi * 1000 + 999)]
        if len(rows) == 0:
            self._prove_no_ticks(calls, symbol, start, end, flags, gap, floor, kept)
        out_of_order = _first_decrease(rows["time_msc"])
        if out_of_order is not None:
            at = self._clock.to_utc_msc(int(rows["time_msc"][out_of_order]))
            raise _Unanswerable(mirror.RES_E_FAIL, f"{symbol} ticks: row {at} is out of order")
        logger.debug(
            "%s ticks %d..%d: %d of %d rows carry sub-second time_msc",
            symbol,
            start,
            end,
            np.count_nonzero(rows["time_msc"] % 1000),
            len(rows),
        )
        return rows

    def _prove_no_ticks(
        self,
        calls: _Calls,
        symbol: str,
        start: int,
        end: int,
        flags: TickFlags,
        gap: _Gap,
        floor: Floor | None,
        kept: bool,
    ) -> None:
        """Raises _Unproven unless no tick lies in the window: a kept floor is measured again first,
        since the terminal answers a window between a stale floor and its own with nothing."""
        if kept:
            measured = self._measure_tick_floor(calls, symbol, flags)
            if measured is not None:
                floor = measured
        if floor is not None and gap.hi * 1000 + 999 < floor.value:
            reason = None
        else:
            reason = self._unproven(
                calls,
                symbol,
                gap,
                rows_before=lambda: self._ticks_before(calls, symbol, start, end, flags, gap),
                record_after=lambda: self._tick_after(calls, symbol, end, flags, gap),
            )
        if reason is not None:
            raise _Unproven(f"no tick for broker {gap.lo}..{gap.hi}: {reason}")

    def _unproven(
        self,
        calls: _Calls,
        symbol: str,
        gap: _Gap,
        *,
        rows_before: Callable[[], bool],
        record_after: Callable[[], bool],
    ) -> str | None:
        """Why the terminal's answers do not prove the gap empty, or None when they do: it lies
        after the second of the symbol's last quote, or rows lie before it and a record after it.
        The last quote is itself a record, so a gap holding its second is unproven."""
        if gap.rows_after:
            last_quote = None
        else:
            last_quote = self._last_quote(calls, symbol)
        if last_quote is not None and last_quote < gap.lo:
            reason = None
        elif last_quote is not None and last_quote <= gap.hi:
            reason = f"the last quote, broker {last_quote}, lies in it"
        elif not (gap.rows_after or record_after()):
            reason = "no record follows it"
        elif not (gap.rows_before or rows_before()):
            reason = "no row precedes it"
        else:
            reason = None
        return reason

    def _select(self, calls: _Calls, symbol: str) -> None:
        outcome = calls.call(_SYMBOL_SELECT, {"symbol": symbol, "enable": True})
        if outcome.value is False:
            raise _Unanswerable(*outcome.last_error)

    def _span_s(self, calls: _Calls, series: Series) -> int:
        """The widest window one read of the series spans: the terminal answers a wider one with
        nothing."""
        outcome = calls.call(_TERMINAL_INFO, {})
        if not _is_success(outcome):
            raise _Unproven(f"terminal_info: {_unanswered(outcome)}")
        elif outcome.value.maxbars <= SPAN_MARGIN:
            raise _Unanswerable(
                mirror.RES_E_FAIL, f"the terminal's maxbars {outcome.value.maxbars} spans nothing"
            )
        else:
            return (outcome.value.maxbars - SPAN_MARGIN) * BAR_PERIOD_S[series]

    def _last_quote(self, calls: _Calls, symbol: str) -> int | None:
        """The broker second of the symbol's last quote, or None for the terminal's zero tick, which
        dates nothing."""
        outcome = calls.call(_SYMBOL_INFO_TICK, {"symbol": symbol})
        if not _is_success(outcome):
            raise _Unproven(f"symbol_info_tick: {_unanswered(outcome)}")
        elif outcome.value.time == 0:
            return None
        else:
            return int(outcome.value.time)

    def _is_fresh(self, floor: Floor | None) -> bool:
        return floor is not None and time.time() - floor.measured_at < self._floor_ttl_s

    def _measure_bar_floor(self, calls: _Calls, symbol: str, series: Series) -> Floor | None:
        """Measures and records the series' stub, the one row the terminal answers for a window
        before any plausible history, and answers the floor then in force; None when it answers
        none, which proves nothing."""
        probe_from = self._clock.to_broker(_BEFORE_HISTORY_S)
        probe = self._read(
            calls,
            f"{symbol} {series} floor",
            _COPY_RATES_RANGE,
            self._rates_arguments(symbol, series, probe_from, probe_from + BAR_PERIOD_S[series]),
        )
        if len(probe) == 0:
            floor = None
        else:
            floor = self._floors.write(
                symbol, series, int(probe["time"][0]), int(time.time()), FloorOrigin.STUB
            )
        return floor

    def _measure_tick_floor(self, calls: _Calls, symbol: str, flags: TickFlags) -> Floor | None:
        """Measures and records the ticks' floor, the first tick the terminal answers from before
        any plausible history; None when it answers none, which proves nothing."""
        probe = self._read(
            calls,
            f"{symbol} ticks floor",
            _COPY_TICKS_FROM,
            self._first_tick_arguments(symbol, _BEFORE_HISTORY_S, flags),
        )
        if len(probe) == 0:
            floor = None
        else:
            floor = self._floors.write(
                symbol, Series.TICKS, int(probe["time_msc"][0]), int(time.time()), FloorOrigin.STUB
            )
        return floor

    def _move_floor(self, symbol: str, series: Series, value: int, dropped: int) -> Floor:
        """Records the floor past a daily-spaced prefix of a series' rows."""
        moved = self._floors.write(symbol, series, value, int(time.time()), FloorOrigin.COARSE)
        logger.info(
            "%s %s: %d daily-spaced rows dropped; the floor is %d, generation %d",
            symbol,
            series,
            dropped,
            self._clock.to_utc(moved.value),
            moved.generation,
        )
        return moved

    def _rates(self, calls: _Calls, symbol: str, series: Series, lo: int, hi: int) -> np.ndarray:
        """The rates opened in [lo, hi]; an answer row outside it is not the window's."""
        read = self._read(
            calls,
            f"{symbol} {series} broker {lo}..{hi}",
            _COPY_RATES_RANGE,
            self._rates_arguments(symbol, series, lo, hi),
        )
        times = read["time"]
        return read[(times >= lo) & (times <= hi)]

    def _bar_before(self, calls: _Calls, symbol: str, series: Series, gap: _Gap) -> bool:
        """Whether the last bar the terminal answers as opened at or before the gap's start opened
        before it."""
        read = self._read(
            calls,
            f"{symbol} {series} last bar from broker {gap.lo}",
            _COPY_RATES_FROM,
            self._last_bar_arguments(symbol, series, gap.lo),
        )
        return len(read) > 0 and int(read["time"][-1]) < gap.lo

    def _bar_after(
        self, calls: _Calls, symbol: str, series: Series, gap: _Gap, span_s: int
    ) -> bool:
        """Whether the first bar the widest read from the gap's end answers lies after the gap."""
        read = self._read(
            calls,
            f"{symbol} {series} broker {gap.hi}..{gap.hi + span_s}",
            _COPY_RATES_RANGE,
            self._rates_arguments(symbol, series, gap.hi, gap.hi + span_s),
        )
        return len(read) > 0 and int(read["time"][0]) > gap.hi

    def _ticks_before(
        self, calls: _Calls, symbol: str, start: int, end: int, flags: TickFlags, gap: _Gap
    ) -> bool:
        """Whether the window's UTC day, or failing that its UTC week, answers a tick before the gap
        and none in it."""
        for widened in _widenings(start, end):
            read = self._read(
                calls,
                f"{symbol} ticks {widened[0]}..{widened[1]}",
                _COPY_TICKS_RANGE,
                self._ticks_arguments(symbol, *widened, flags),
            )
            times = read["time_msc"]
            if np.any((times >= gap.lo * 1000) & (times <= gap.hi * 1000 + 999)):
                return False
            elif np.any(times < gap.lo * 1000):
                return True
        return False

    def _tick_after(
        self, calls: _Calls, symbol: str, end: int, flags: TickFlags, gap: _Gap
    ) -> bool:
        """Whether the first tick from the window's last second lies after the gap."""
        read = self._read(
            calls,
            f"{symbol} first tick from {end}",
            _COPY_TICKS_FROM,
            self._first_tick_arguments(symbol, end, flags),
        )
        return len(read) > 0 and int(read["time_msc"][0]) > gap.hi * 1000 + 999

    def _read(
        self, calls: _Calls, what: str, function: mirror.Function, arguments: dict[str, object]
    ) -> np.ndarray:
        """A read's rows; raises _Unproven unless the terminal answered it with success."""
        outcome = calls.call(function, arguments)
        if _is_success(outcome):
            return outcome.value
        else:
            raise _Unproven(f"{what}: {_unanswered(outcome)}")

    def _syncing(self, what: str, unproven: _Unproven) -> Syncing:
        message = f"{what}: syncing"
        logger.info("%s; %s", message, unproven)
        return Syncing((ServerCode.SYNCING, message), self._retry_s)

    def _utc_floor(self, series: Series, floor: Floor) -> int:
        if series is Series.TICKS:
            return self._clock.to_utc_msc(floor.value) // 1000
        else:
            return self._clock.to_utc(floor.value)

    def _rates_arguments(self, symbol: str, series: Series, lo: int, hi: int) -> dict[str, object]:
        return {
            "symbol": symbol,
            "timeframe": BAR_TIMEFRAME[series],
            "date_from": broker_datetime(lo),
            "date_to": broker_datetime(hi),
        }

    def _last_bar_arguments(self, symbol: str, series: Series, at: int) -> dict[str, object]:
        # copy_rates_from reads back from its date: it answers the bars opened at or before it.
        return {
            "symbol": symbol,
            "timeframe": BAR_TIMEFRAME[series],
            "date_from": broker_datetime(at),
            "count": 1,
        }

    def _ticks_arguments(
        self, symbol: str, start: int, end: int, flags: TickFlags
    ) -> dict[str, object]:
        # The package ends a tick range at the first millisecond of its last second, so the read
        # runs to the broker's second after it.
        return {
            "symbol": symbol,
            "date_from": broker_datetime(self._clock.to_broker(start)),
            "date_to": broker_datetime(self._clock.to_broker(end) + 1),
            "flags": TICK_FLAGS[flags],
        }

    def _first_tick_arguments(self, symbol: str, start: int, flags: TickFlags) -> dict[str, object]:
        return {
            "symbol": symbol,
            "date_from": broker_datetime(self._clock.to_broker(start)),
            "count": 1,
            "flags": TICK_FLAGS[flags],
        }


def bars_refusal(body: object, now: int) -> str | None:
    """Why a body is not a bars request the terminal can answer at true-UTC `now`, or None when it
    is."""
    refusal = _window_refusal(body, ("symbol", "timeframe", "start", "end"), (), now)
    if refusal is not None:
        return refusal
    elif not (isinstance(body["timeframe"], str) and body["timeframe"] in BAR_PERIOD_S):
        return f"not a bar timeframe: {body['timeframe']!r}"
    else:
        return None


def ticks_refusal(body: object, now: int) -> str | None:
    """Why a body is not a ticks request the terminal can answer at true-UTC `now`, or None when it
    is."""
    refusal = _window_refusal(body, ("symbol", "start", "end"), ("flags",), now)
    if refusal is not None:
        return refusal
    elif "flags" in body and not (isinstance(body["flags"], str) and body["flags"] in TICK_FLAGS):
        return f"not a tick selection: {body['flags']!r}"
    else:
        return None


def _window_refusal(
    body: object, required: tuple[str, ...], optional: tuple[str, ...], now: int
) -> str | None:
    if not isinstance(body, dict):
        return "request body is not a JSON object"
    unknown = sorted(name for name in body if name not in required + optional)
    missing = [name for name in required if name not in body]
    not_epochs = [name for name in ("start", "end") if name in body and not _is_integer(body[name])]
    if unknown:
        return f"unknown parameter: {', '.join(unknown)}"
    elif missing:
        return f"missing parameter: {', '.join(missing)}"
    elif not (isinstance(body["symbol"], str) and body["symbol"]):
        return f"not a symbol: {body['symbol']!r}"
    elif not_epochs:
        return f"not an integer epoch: {', '.join(not_epochs)}"
    elif body["start"] > body["end"]:
        return "start is after end"
    elif body["start"] > now:
        return f"start {body['start']} is after the terminal's time {now}"
    else:
        return None


def _is_success(outcome: Answered | Failed) -> bool:
    return isinstance(outcome, Answered) and outcome.last_error[0] == mirror.RES_S_OK


def _unanswered(outcome: Answered | Failed) -> str:
    code, message = outcome.last_error
    if isinstance(outcome, Failed):
        return f"no answer, last_error {code} {message!r}"
    else:
        return f"last_error {code} {message!r}"


def _chunks(lo: int, hi: int, span_s: int) -> list[tuple[int, int]]:
    """[lo, hi] split oldest-first into the fewest equal ranges spanning at most span_s each, so no
    range is a sliver."""
    count = max(1, -(-(hi - lo) // span_s))
    bounds = [lo + (hi - lo) * index // count for index in range(count + 1)]
    chunks = [(bounds[index], bounds[index + 1] - 1) for index in range(count - 1)]
    chunks.append((bounds[count - 1], hi))
    return chunks


def _gaps(chunks: list[tuple[int, int]], parts: list[np.ndarray]) -> list[_Gap]:
    """The stretches of a window its chunk reads answered no row for: the chunks before the first
    that answered one and those after the last, or the whole window when none did. A chunk between
    two that answered rows is bracketed by them."""
    answered = [index for index, part in enumerate(parts) if len(part) > 0]
    gaps = []
    if not answered:
        gaps.append(_Gap(chunks[0][0], chunks[-1][1], rows_before=False, rows_after=False))
    else:
        if answered[0] > 0:
            leading_hi = chunks[answered[0] - 1][1]
            gaps.append(_Gap(chunks[0][0], leading_hi, rows_before=False, rows_after=True))
        if answered[-1] < len(chunks) - 1:
            trailing_lo = chunks[answered[-1] + 1][0]
            gaps.append(_Gap(trailing_lo, chunks[-1][1], rows_before=True, rows_after=False))
    return gaps


def _containing(start: int, end: int, unit: int, origin: int) -> tuple[int, int]:
    """The whole units, counted from origin, that [start, end] lies in."""
    return start - (start - origin) % unit, end - (end - origin) % unit + unit - 1


def _widenings(start: int, end: int) -> list[tuple[int, int]]:
    """The UTC day, then the UTC week, that [start, end] lies in, each only where it is wider than
    the read before it."""
    day = _containing(start, end, _DAY_S, 0)
    week = _containing(start, end, _WEEK_S, _FIRST_MONDAY_S)
    widenings = []
    if day != (start, end):
        widenings.append(day)
    if week != day:
        widenings.append(week)
    return widenings


def _first_off_grid(times: np.ndarray, period: int) -> int | None:
    """The first row off the grid the first row sets, or not after the row before it."""
    if len(times) == 0:
        return None
    off = (times - times[0]) % period != 0
    off[1:] |= np.diff(times) <= 0
    indices = np.flatnonzero(off)
    if len(indices) == 0:
        return None
    else:
        return int(indices[0])


def _first_decrease(times: np.ndarray) -> int | None:
    indices = np.flatnonzero(np.diff(times) < 0)
    if len(indices) == 0:
        return None
    else:
        return int(indices[0]) + 1


def _coarse_prefix(times: np.ndarray, period: int) -> int:
    """How many leading rows an intraday series serves a whole number of days apart, ahead of its
    first two rows one period apart."""
    deltas = np.diff(times)
    spaced = np.flatnonzero(deltas == period)
    prefix = 0
    if period < _DAY_S and len(spaced) > 0:
        first = int(spaced[0])
        if first > 0 and np.all(deltas[:first] % _DAY_S == 0):
            prefix = first
    return prefix


def _is_integer(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)
