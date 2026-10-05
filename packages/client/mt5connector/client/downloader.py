"""Downloads a symbol's bars and ticks through the MT5 server's history routes into a NautilusTrader
Parquet catalog, walking back from the end of a range until its start or the series' floor."""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from nautilus_trader.model.data import BarType
from nautilus_trader.persistence.catalog import ParquetDataCatalog

from mt5connector.client import history
from mt5connector.client.errors import (
    MT5InstrumentError,
    MT5SymbolNotFoundError,
    ServerUnreachable,
)
from mt5connector.client.history import HistoryRanges
from mt5connector.client.parsing import (
    InstrumentAny,
    parse_quote_tick,
    venue_bar,
    venue_bar_type,
)
from mt5connector.wire import mirror
from mt5connector.wire.history_wire import BAR_PERIOD_S, SPAN_MARGIN, Series, bar_series

if TYPE_CHECKING:
    import numpy as np

    from mt5connector.client.connection import MT5Connection
    from mt5connector.client.providers import MT5InstrumentProvider

logger = logging.getLogger(__name__)

_DAY_S = 86_400


# ─────────────────────────────────────────────────────────────────────────────
# RESULT DATACLASS
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class DownloadResult:
    """What one download wrote, the windows it asked the server for, and what failed."""

    symbol: str
    data_type: str
    total_written: int = 0
    chunks_processed: int = 0
    # Windows the server answered with no rows.
    chunks_empty: int = 0
    errors: list[str] = field(default_factory=list)
    start: datetime | None = None
    end: datetime | None = None
    # Where the series begins, once the walk reached it.
    floor: datetime | None = None

    @property
    def success(self) -> bool:
        return len(self.errors) == 0

    def __str__(self) -> str:
        status = "OK" if self.success else f"{len(self.errors)} errors"
        return (
            f"DownloadResult({self.symbol} {self.data_type} | "
            f"{self.total_written:,} rows | "
            f"{self.chunks_processed} chunks | "
            f"status={status})"
        )


# ─────────────────────────────────────────────────────────────────────────────
# DOWNLOADER
# ─────────────────────────────────────────────────────────────────────────────


class MT5DataDownloader:
    """Downloads a symbol's history from the MT5 server into a Parquet catalog."""

    def __init__(
        self,
        connection: MT5Connection,
        provider: MT5InstrumentProvider,
        catalog: ParquetDataCatalog,
    ) -> None:
        self._conn = connection
        self._provider = provider
        self._catalog = catalog

    # ── Public API ────────────────────────────────────────────────────────────

    def download_ticks(self, symbol: str, start: datetime, end: datetime) -> DownloadResult:
        """Writes the symbol's quote ticks between start and end, one UTC day per request, each day
        as it arrives."""
        symbol = symbol.strip()  # preserve broker casing (EURUSDm, EURUSD, etc.)
        start = _ensure_utc(start)
        end = _ensure_utc(end)
        result = DownloadResult(symbol=symbol, data_type="ticks", start=start, end=end)

        self._conn.ensure_connected()

        instrument = self._ensure_instrument(symbol, result)
        if instrument is not None:
            self._walk_ticks(result, instrument, start, end)
        logger.info(f"Downloader: {result}")
        return result

    def download_bars(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        timeframe: int | None = None,
    ) -> DownloadResult:
        """Writes the symbol's bars of an MT5 timeframe, H1 by default, closing between start and
        end, stamped at their close and typed by the price the venue charts them on; each request
        spans as many periods as the terminal answers in one read."""
        symbol = symbol.strip()  # preserve broker casing (EURUSDm, EURUSD, etc.)
        start = _ensure_utc(start)
        end = _ensure_utc(end)
        if timeframe is None:
            timeframe = mirror.TIMEFRAME_H1
        series = bar_series(timeframe)
        result = DownloadResult(symbol=symbol, data_type="bars", start=start, end=end)

        self._conn.ensure_connected()

        instrument = self._ensure_instrument(symbol, result)
        if instrument is not None:
            bar_type = venue_bar_type(instrument, timeframe)
            self._walk_bars(result, instrument, series, bar_type, start, end)
        logger.info(f"Downloader: {result}")
        return result

    def download_all(
        self,
        symbols: list[str],
        start: datetime,
        end: datetime,
        include_ticks: bool = True,
        include_bars: bool = True,
        timeframes: list[int] | None = None,
    ) -> dict[str, list[DownloadResult]]:
        """Downloads each symbol's ticks, then its bars of each MT5 timeframe, H1 and D1 by default;
        answers each symbol's results in that order."""
        if timeframes is None:
            timeframes = [mirror.TIMEFRAME_H1, mirror.TIMEFRAME_D1]
        results: dict[str, list[DownloadResult]] = {}

        for symbol in symbols:
            symbol_results = []

            if include_ticks:
                r = self.download_ticks(symbol, start, end)
                symbol_results.append(r)

            if include_bars:
                for tf in timeframes:
                    r = self.download_bars(symbol, start, end, timeframe=tf)
                    symbol_results.append(r)

            results[symbol] = symbol_results
            logger.info(
                f"Downloader: {symbol} complete — "
                f"{sum(r.total_written for r in symbol_results):,} total rows"
            )

        return results

    # ── Private ───────────────────────────────────────────────────────────────

    def _walk_ticks(
        self, result: DownloadResult, instrument: InstrumentAny, start: datetime, end: datetime
    ) -> None:
        """Walks the symbol's ticks back from end, a UTC day per window."""
        ranges = history.ranges(self._conn, result.symbol)
        if ranges is None:
            _record_error(result, f"Ranges failed: {self._conn.mt5.last_error()}")
        else:
            logger.info(f"Downloader: downloading ticks for {result.symbol} from {start} to {end}")
            first = int(start.timestamp())
            windows = []
            window_end = int(end.timestamp())
            while window_end >= first:
                window_start = max(first, window_end - window_end % _DAY_S)
                windows.append((window_start, window_end))
                window_end = window_start - 1
            self._walk_back(
                result,
                Series.TICKS,
                first,
                windows,
                _advertised_floor(ranges, Series.TICKS),
                lambda lo, hi: history.ticks(self._conn, result.symbol, lo, hi),
                lambda rows: [parse_quote_tick(row, instrument) for row in rows],
            )

    def _walk_bars(
        self,
        result: DownloadResult,
        instrument: InstrumentAny,
        series: Series,
        bar_type: BarType,
        start: datetime,
        end: datetime,
    ) -> None:
        """Walks the symbol's bars of `series` back from the one closing at end, as many periods per
        window as the terminal answers in one read."""
        ranges = history.ranges(self._conn, result.symbol)
        if ranges is None:
            _record_error(result, f"Ranges failed: {self._conn.mt5.last_error()}")
        else:
            logger.info(
                f"Downloader: downloading {series} bars for {result.symbol} closing from {start} "
                f"to {end}"
            )
            # The range names closes; the server serves bars by their open.
            period = BAR_PERIOD_S[series]
            first = int(start.timestamp()) - period
            span = (ranges.maxbars - SPAN_MARGIN) * period
            windows = []
            window_end = int(end.timestamp()) - period
            while window_end >= first:
                window_start = max(first, window_end - span)
                windows.append((window_start, window_end))
                window_end = window_start - 1
            self._walk_back(
                result,
                series,
                first,
                windows,
                _advertised_floor(ranges, series),
                lambda lo, hi: history.bars(self._conn, result.symbol, series, lo, hi),
                lambda rows: [venue_bar(row, bar_type, instrument) for row in rows],
            )

    def _walk_back(
        self,
        result: DownloadResult,
        series: Series,
        first: int,
        windows: list[tuple[int, int]],
        floor: int | None,
        read: Callable[[int, int], np.ndarray | None],
        convert: Callable[[np.ndarray], list],
    ) -> None:
        """Requests the windows newest-first, writing the rows each answers, until the next lies
        wholly before the series' floor, and records the floor when it lies after `first`. A window
        the server fails or leaves unanswered is recorded as an error and the walk goes on. The
        floor is read again after a window answered no rows and once the walk ends, since the server
        measures one while answering."""
        for lo, hi in windows:
            if floor is not None and hi < floor:
                break
            result.chunks_processed += 1
            label = f"{_iso(lo)}..{_iso(hi)}"
            try:
                rows = read(lo, hi)
            except ServerUnreachable as exc:
                _record_error(result, f"Window {label} failed: {exc}")
                continue
            if rows is None:
                _record_error(result, f"Window {label} failed: {self._conn.mt5.last_error()}")
            elif len(rows) == 0:
                result.chunks_empty += 1
                floor = self._refresh_floor(result, series, floor)
            else:
                data = convert(rows)
                self._catalog.write_data(data)
                result.total_written += len(data)
                logger.debug(f"Downloader: {result.symbol} {label} → {len(data):,} written")
        floor = self._refresh_floor(result, series, floor)
        if floor is not None and floor > first:
            result.floor = datetime.fromtimestamp(floor, UTC)

    def _refresh_floor(
        self, result: DownloadResult, series: Series, floor: int | None
    ) -> int | None:
        """Reads the series' floor the server advertises now; a failed read is recorded as an error
        and leaves the floor known before."""
        ranges = history.ranges(self._conn, result.symbol)
        if ranges is None:
            _record_error(result, f"Ranges failed: {self._conn.mt5.last_error()}")
            return floor
        else:
            return _advertised_floor(ranges, series)

    def _ensure_instrument(self, symbol: str, result: DownloadResult) -> InstrumentAny | None:
        """The symbol's instrument, loaded when the provider holds none; a definition the venue
        refuses is recorded as the symbol's error, by its type, and answers None."""
        instrument = self._provider.get_instrument(symbol)
        if instrument is None:
            try:
                instrument = self._provider.load_symbol(symbol)
            except (MT5SymbolNotFoundError, MT5InstrumentError) as exc:
                _record_error(result, f"{type(exc).__name__}: {exc}")
        return instrument


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────


def _ensure_utc(dt: datetime) -> datetime:
    """Make a datetime timezone-aware (UTC) if it isn't already."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt


def _advertised_floor(ranges: HistoryRanges, series: Series) -> int | None:
    """The series' floor among the ranges the server advertises, or None before it measures one."""
    if series in ranges.series:
        return ranges.series[series].floor
    else:
        return None


def _record_error(result: DownloadResult, message: str) -> None:
    logger.error(f"Downloader: {result.symbol} {message}")
    result.errors.append(message)


def _iso(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, UTC).isoformat(timespec="seconds")
