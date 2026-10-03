"""Downloads a symbol's bars and ticks through the MT5 server's history routes into a NautilusTrader
Parquet catalog, walking back from the end of a range until its start or the series' floor."""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING

try:
    import MetaTrader5 as mt5
except ImportError:  # pragma: no cover - Windows-only dependency
    mt5 = None  # bound to the real backend by mt5connect.backend.set_backend()

from nautilus_trader.persistence.catalog import ParquetDataCatalog

from mt5connect import history
from mt5connect.errors import MT5SymbolNotFoundError
from mt5connect.history import HistoryRanges
from mt5connect.history_wire import BAR_PERIOD_S, SPAN_MARGIN, Series, bar_series
from mt5connect.parsing import parse_bar, parse_quote_tick

if TYPE_CHECKING:
    import numpy as np

    from mt5connect.connection import MT5Connection
    from mt5connect.providers import MT5InstrumentProvider

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
        """Writes the symbol's quote ticks between start and end, one UTC day per request, each
        day as it arrives."""
        symbol = symbol.strip()  # preserve broker casing (EURUSDm, EURUSD, etc.)
        start = _ensure_utc(start)
        end = _ensure_utc(end)
        result = DownloadResult(symbol=symbol, data_type="ticks", start=start, end=end)

        self._conn.ensure_connected()

        # Ensure instrument is loaded
        instrument = self._ensure_instrument(symbol, result)
        if instrument is None:
            return result

        ranges = history.ranges(symbol)
        if ranges is None:
            _record_error(result, f"Ranges failed: {mt5.last_error()}")
        else:
            logger.info(f"Downloader: downloading ticks for {symbol} from {start} to {end}")
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
                lambda lo, hi: history.ticks(symbol, lo, hi),
                lambda rows: [parse_quote_tick(row, instrument) for row in rows],
            )
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
        end, stamped at their close; each request spans as many periods as the terminal answers in
        one read."""
        symbol = symbol.strip()  # preserve broker casing (EURUSDm, EURUSD, etc.)
        start = _ensure_utc(start)
        end = _ensure_utc(end)
        timeframe = timeframe or mt5.TIMEFRAME_H1
        series = bar_series(timeframe)
        result = DownloadResult(symbol=symbol, data_type="bars", start=start, end=end)

        self._conn.ensure_connected()

        instrument = self._ensure_instrument(symbol, result)
        if instrument is None:
            return result

        ranges = history.ranges(symbol)
        if ranges is None:
            _record_error(result, f"Ranges failed: {mt5.last_error()}")
        else:
            logger.info(
                f"Downloader: downloading {series} bars for {symbol} closing from {start} to {end}"
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
                lambda lo, hi: history.bars(symbol, series, lo, hi),
                lambda rows: [parse_bar(row, instrument, timeframe) for row in rows],
            )
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
        """
        Download tick and/or bar data for multiple symbols.

        Parameters
        ----------
        symbols : list[str]
        start, end : datetime
        include_ticks : bool
            Whether to download tick data.
        include_bars : bool
            Whether to download bar data.
        timeframes : list[int], optional
            MT5 timeframe constants. Defaults to [H1, D1].

        Returns
        -------
        dict[str, list[DownloadResult]]
            Keyed by symbol, value is list of DownloadResult (one per data type).
        """
        timeframes = timeframes or [mt5.TIMEFRAME_H1, mt5.TIMEFRAME_D1]
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
        wholly before the series' floor, and records the floor when it lies after `first`. The
        floor is read again after a window answered no rows and once the walk ends, since the server
        measures one while answering."""
        for lo, hi in windows:
            if floor is not None and hi < floor:
                break
            result.chunks_processed += 1
            label = f"{_iso(lo)}..{_iso(hi)}"
            try:
                rows = read(lo, hi)
            except Exception as exc:
                _record_error(result, f"Window {label} failed: {exc}")
                continue
            if rows is None:
                _record_error(result, f"Window {label} failed: {mt5.last_error()}")
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
        ranges = history.ranges(result.symbol)
        if ranges is None:
            _record_error(result, f"Ranges failed: {mt5.last_error()}")
            return floor
        else:
            return _advertised_floor(ranges, series)

    def _ensure_instrument(self, symbol: str, result: DownloadResult):
        """
        Get the loaded instrument for a symbol.
        Tries to load it if not already loaded.
        Returns None and records error in result if it fails.
        """
        instrument = self._provider.get_instrument(symbol)
        if instrument is not None:
            return instrument

        # Try loading it now
        try:
            instrument = self._provider.load_symbol(symbol)
            return instrument
        except MT5SymbolNotFoundError:
            msg = f"Symbol '{symbol}' not found on broker — skipping"
            logger.error(f"Downloader: {msg}")
            result.errors.append(msg)
            return None
        except Exception as exc:
            msg = f"Failed to load instrument '{symbol}': {exc}"
            logger.error(f"Downloader: {msg}")
            result.errors.append(msg)
            return None


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
