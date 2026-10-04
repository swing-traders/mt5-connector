"""MT5DataDownloader: its backward walks over the server's bar windows and UTC days, its instrument
loading, and download_all."""

from contextlib import contextmanager
from datetime import UTC, datetime
from enum import Enum, auto
from unittest.mock import MagicMock, patch

import numpy as np
import pytest
from nautilus_trader.model.data import Bar, QuoteTick
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments import CurrencyPair
from nautilus_trader.model.objects import Price, Quantity

from mt5connector.client.downloader import DownloadResult, MT5DataDownloader, _ensure_utc
from mt5connector.client.errors import (
    MT5ConnectionError,
    MT5InstrumentError,
    MT5SymbolNotFoundError,
    ServerUnreachable,
)
from mt5connector.client.history import HistoryRanges, SeriesRange
from mt5connector.wire import mirror
from mt5connector.wire.history_wire import Series

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────


def dt(year, month, day, hour=0, minute=0):
    return datetime(year, month, day, hour, minute, tzinfo=UTC)


RATES = np.dtype(list(mirror.RATES.dtype))
TICKS = np.dtype(list(mirror.TICKS.dtype))
HOUR = 3_600
DAY = 86_400


def rate_rows(*opens):
    return np.array([(t, 1.085, 1.09, 1.08, 1.088, 1000, 2, 0) for t in opens], dtype=RATES)


def tick_rows(*times_msc):
    return np.array([(t // 1000, 1.085, 1.0852, 0.0, 0, t, 6, 0.0) for t in times_msc], dtype=TICKS)


class Answer(Enum):
    ROWS = auto()
    EMPTY = auto()
    FAILED = auto()


class Server:
    """The history client's answers: one scripted answer per window asked, every window recorded,
    and the floors the ranges advertise once `measured_after` windows have been asked."""

    def __init__(self, *answers, floors=None, measured_after=0):
        self.answers = list(answers)
        self.floors = floors or {}
        self.measured_after = measured_after
        self.asked = []
        self.ranges_read = 0

    def bars(self, symbol, series, lo, hi):
        self.asked.append((symbol, series, lo, hi))
        return self._answer(rate_rows(hi - HOUR, hi))

    def ticks(self, symbol, lo, hi):
        self.asked.append((symbol, lo, hi))
        return self._answer(tick_rows(hi * 1000))

    def ranges(self, symbol):
        self.ranges_read += 1
        series = {}
        if len(self.asked) >= self.measured_after:
            series = {name: SeriesRange(floor, 0, 1) for name, floor in self.floors.items()}
        # MaxBars 111 lets a window span 100 periods.
        return HistoryRanges(maxbars=111, series=series)

    def _answer(self, rows):
        answer = self.answers.pop(0)
        if answer is Answer.FAILED:
            return None
        elif answer is Answer.EMPTY:
            return rows[:0]
        else:
            return rows


@contextmanager
def serving(server):
    """The history client answering from the server."""
    with (
        patch("mt5connector.client.history.bars", side_effect=server.bars),
        patch("mt5connector.client.history.ticks", side_effect=server.ticks),
        patch("mt5connector.client.history.ranges", side_effect=server.ranges),
    ):
        yield


def make_eurusd_instrument(info=None):
    """Build a real CurrencyPair instrument for EURUSD, its definition's venue facts `info`."""
    from decimal import Decimal

    from nautilus_trader.model.currencies import Currency

    return CurrencyPair(
        instrument_id=InstrumentId.from_str("EURUSD.MT5"),
        raw_symbol=from_str_sym("EURUSD"),
        base_currency=Currency.from_str("EUR"),
        quote_currency=Currency.from_str("USD"),
        price_precision=5,
        size_precision=2,
        price_increment=Price(0.00001, 5),
        size_increment=Quantity(0.01, 2),
        lot_size=Quantity(100000, 0),
        max_quantity=Quantity(1000.0, 2),
        min_quantity=Quantity(0.01, 2),
        max_notional=None,
        min_notional=None,
        max_price=None,
        min_price=None,
        margin_init=Decimal("0.03"),
        margin_maint=Decimal("0.03"),
        maker_fee=Decimal("0"),
        taker_fee=Decimal("0"),
        ts_event=0,
        ts_init=0,
        info=info,
    )


def from_str_sym(s):
    from nautilus_trader.model.identifiers import Symbol

    return Symbol(s)


def make_conn(connected=True):
    conn = MagicMock()
    if connected:
        conn.ensure_connected = MagicMock()  # no-op
    else:
        conn.ensure_connected = MagicMock(side_effect=MT5ConnectionError("Not connected"))
    return conn


def make_provider(instrument=None):
    provider = MagicMock()
    eurusd = instrument or make_eurusd_instrument()
    provider.get_instrument.return_value = eurusd
    provider.load_symbol.return_value = eurusd
    return provider


def make_catalog():
    catalog = MagicMock()
    catalog.write_data = MagicMock()
    return catalog


@pytest.fixture
def conn():
    return make_conn()


@pytest.fixture
def provider():
    return make_provider()


@pytest.fixture
def catalog():
    return make_catalog()


@pytest.fixture
def downloader(conn, provider, catalog):
    return MT5DataDownloader(connection=conn, provider=provider, catalog=catalog)


@pytest.fixture
def package():
    with patch("mt5connector.client.downloader.mt5") as mt5:
        mt5.TIMEFRAME_H1 = mirror.TIMEFRAME_H1
        mt5.TIMEFRAME_D1 = mirror.TIMEFRAME_D1
        mt5.last_error.return_value = (-4, "Terminal: Not found")
        yield mt5


# ═════════════════════════════════════════════════════════════════════════════
# 1. _ensure_utc()
# ═════════════════════════════════════════════════════════════════════════════


class TestEnsureUtc:

    def test_naive_datetime_gets_utc(self):
        d = datetime(2024, 1, 1)
        result = _ensure_utc(d)
        assert result.tzinfo == UTC

    def test_aware_datetime_unchanged(self):
        d = datetime(2024, 1, 1, tzinfo=UTC)
        result = _ensure_utc(d)
        assert result.tzinfo == UTC
        assert result == d

    def test_naive_date_preserved(self):
        d = datetime(2024, 6, 15, 12, 30)
        result = _ensure_utc(d)
        assert result.year == 2024
        assert result.month == 6
        assert result.day == 15
        assert result.hour == 12

    def test_returns_datetime(self):
        result = _ensure_utc(datetime(2024, 1, 1))
        assert isinstance(result, datetime)


# ═════════════════════════════════════════════════════════════════════════════
# 2. DownloadResult
# ═════════════════════════════════════════════════════════════════════════════


class TestDownloadResult:

    def test_success_when_no_errors(self):
        r = DownloadResult(symbol="EURUSD", data_type="ticks")
        assert r.success is True

    def test_not_success_when_errors(self):
        r = DownloadResult(symbol="EURUSD", data_type="ticks", errors=["chunk failed"])
        assert r.success is False

    def test_default_total_written_zero(self):
        r = DownloadResult(symbol="EURUSD", data_type="ticks")
        assert r.total_written == 0

    def test_str_contains_symbol(self):
        r = DownloadResult(symbol="EURUSD", data_type="ticks", total_written=1000)
        assert "EURUSD" in str(r)

    def test_str_contains_total_written(self):
        r = DownloadResult(symbol="EURUSD", data_type="ticks", total_written=5000)
        assert "5,000" in str(r)

    def test_str_contains_ok_on_success(self):
        r = DownloadResult(symbol="EURUSD", data_type="ticks")
        assert "OK" in str(r)

    def test_str_shows_error_count(self):
        r = DownloadResult(symbol="EURUSD", data_type="ticks", errors=["e1", "e2"])
        assert "2" in str(r)

    def test_default_floor_none(self):
        r = DownloadResult(symbol="EURUSD", data_type="ticks")
        assert r.floor is None

    def test_errors_list_mutable(self):
        r = DownloadResult(symbol="EURUSD", data_type="ticks")
        r.errors.append("something")
        assert len(r.errors) == 1


# ═════════════════════════════════════════════════════════════════════════════
# 3. download_bars() — the backward walk
# ═════════════════════════════════════════════════════════════════════════════

# The double's MaxBars of 111 lets a window span 100 H1 periods.
SPAN = 100 * HOUR
LAST_OPEN = int(dt(2024, 12, 31).timestamp()) - HOUR
BAR_WINDOWS = [
    (LAST_OPEN - SPAN, LAST_OPEN),
    (LAST_OPEN - 2 * SPAN - 1, LAST_OPEN - SPAN - 1),
    (LAST_OPEN - 3 * SPAN - 2, LAST_OPEN - 2 * SPAN - 2),
]


class TestDownloadBarsWalk:

    def test_walks_back_to_the_advertised_floor_and_records_it(self, downloader, catalog, package):
        floor = LAST_OPEN - 2 * SPAN - 50 * HOUR
        server = Server(*[Answer.ROWS] * 3, floors={Series.H1: floor})
        with serving(server):
            result = downloader.download_bars("EURUSD", dt(2024, 1, 1), dt(2024, 12, 31))

        assert server.asked == [("EURUSD", Series.H1, lo, hi) for lo, hi in BAR_WINDOWS]
        assert (result.chunks_processed, result.total_written) == (3, 6)
        assert catalog.write_data.call_count == 3
        assert (result.success, result.floor) == (True, datetime.fromtimestamp(floor, UTC))

    def test_an_empty_window_reads_the_floor_again_and_the_walk_stops_at_it(
        self, downloader, catalog, package
    ):
        floor = LAST_OPEN - SPAN
        server = Server(Answer.ROWS, Answer.EMPTY, floors={Series.H1: floor}, measured_after=1)
        with serving(server):
            result = downloader.download_bars("EURUSD", dt(2024, 1, 1), dt(2024, 12, 31))

        assert server.asked == [("EURUSD", Series.H1, lo, hi) for lo, hi in BAR_WINDOWS[:2]]
        # Before the walk, after the empty window, and once the walk ends.
        assert server.ranges_read == 3
        assert (result.chunks_empty, result.total_written) == (1, 2)
        assert result.floor == datetime.fromtimestamp(floor, UTC)

    def test_a_floor_measured_while_the_last_window_was_answered_is_recorded(
        self, downloader, catalog, package
    ):
        floor = int(dt(2024, 12, 28).timestamp())
        server = Server(Answer.ROWS, floors={Series.H1: floor}, measured_after=1)
        with serving(server):
            result = downloader.download_bars("EURUSD", dt(2024, 12, 27), dt(2024, 12, 31))

        assert len(server.asked) == 1
        assert result.floor == datetime.fromtimestamp(floor, UTC)

    def test_an_empty_window_after_the_floor_does_not_end_the_walk(
        self, downloader, catalog, package
    ):
        server = Server(
            Answer.ROWS, Answer.EMPTY, Answer.ROWS, floors={Series.H1: LAST_OPEN - 50 * SPAN}
        )
        with serving(server):
            result = downloader.download_bars("EURUSD", dt(2024, 12, 19), dt(2024, 12, 31))

        assert len(server.asked) == 3
        assert server.ranges_read == 3
        assert (result.chunks_empty, result.total_written, result.floor) == (1, 4, None)

    def test_writes_bars_stamped_at_their_close(self, downloader, catalog, package):
        server = Server(Answer.ROWS, floors={Series.H1: LAST_OPEN})
        with serving(server):
            downloader.download_bars("EURUSD", dt(2024, 1, 1), dt(2024, 12, 31))

        written = catalog.write_data.call_args_list[0][0][0]
        assert all(isinstance(bar, Bar) for bar in written)
        assert [bar.ts_event for bar in written] == [
            LAST_OPEN * 1_000_000_000,
            (LAST_OPEN + HOUR) * 1_000_000_000,
        ]

    def test_a_failed_window_is_an_error_and_the_walk_goes_on(self, downloader, catalog, package):
        server = Server(
            Answer.FAILED, Answer.ROWS, floors={Series.H1: LAST_OPEN - SPAN - 50 * HOUR}
        )
        with serving(server):
            result = downloader.download_bars("EURUSD", dt(2024, 1, 1), dt(2024, 12, 31))

        assert len(server.asked) == 2
        assert result.errors == [
            "Window 2024-12-26T19:00:00+00:00..2024-12-30T23:00:00+00:00 failed: "
            "(-4, 'Terminal: Not found')"
        ]
        assert catalog.write_data.call_count == 1

    def test_a_window_raising_is_an_error_and_the_walk_goes_on(self, downloader, package):
        server = Server(Answer.ROWS, floors={Series.H1: LAST_OPEN - SPAN - 50 * HOUR})
        failures = [RuntimeError("connection reset"), rate_rows(LAST_OPEN - SPAN - HOUR)]
        with serving(server), patch("mt5connector.client.history.bars", side_effect=failures):
            result = downloader.download_bars("EURUSD", dt(2024, 1, 1), dt(2024, 12, 31))

        assert (result.chunks_processed, result.total_written) == (2, 1)
        assert "connection reset" in result.errors[0]

    def test_failed_ranges_are_an_error_and_nothing_is_walked(self, downloader, package):
        with (
            patch("mt5connector.client.history.ranges", return_value=None),
            patch("mt5connector.client.history.bars") as bars,
        ):
            result = downloader.download_bars("EURUSD", dt(2024, 1, 1), dt(2024, 12, 31))

        bars.assert_not_called()
        assert result.errors == ["Ranges failed: (-4, 'Terminal: Not found')"]

    def test_the_default_timeframe_is_h1_and_an_explicit_one_is_used(self, downloader, package):
        floors = {Series.H1: int(dt(2024, 12, 29).timestamp()), Series.D1: 0}
        server = Server(Answer.ROWS, Answer.ROWS, floors=floors)
        with serving(server):
            downloader.download_bars("EURUSD", dt(2024, 12, 1), dt(2024, 12, 31))
            downloader.download_bars("EURUSD", dt(2024, 12, 1), dt(2024, 12, 31), timeframe=16408)

        assert [asked[1] for asked in server.asked] == [Series.H1, Series.D1]

    def test_a_month_timeframe_has_no_history(self, downloader, package):
        with pytest.raises(ValueError, match="49153"):
            downloader.download_bars("EURUSD", dt(2024, 1, 1), dt(2024, 12, 31), timeframe=49153)

    def test_a_timeframe_of_zero_is_refused_not_defaulted(self, downloader, package):
        server = Server(Answer.ROWS, floors={Series.H1: LAST_OPEN})
        with serving(server):
            with pytest.raises(ValueError, match="^timeframe 0 has no history series$"):
                downloader.download_bars("EURUSD", dt(2024, 1, 1), dt(2024, 12, 31), timeframe=0)
        assert server.asked == []

    @pytest.mark.parametrize(
        ("info", "price_type"),
        [({"chart_mode": "BID"}, "BID"), ({"chart_mode": "LAST"}, "LAST"), ({}, "LAST")],
        ids=["bid-chart", "last-chart", "no-chart-mode"],
    )
    def test_bars_are_typed_by_the_price_the_venue_charts_them_on(
        self, conn, catalog, package, info, price_type
    ):
        downloader = MT5DataDownloader(conn, make_provider(make_eurusd_instrument(info)), catalog)
        server = Server(Answer.ROWS, floors={Series.H1: LAST_OPEN})
        with serving(server):
            downloader.download_bars("EURUSD", dt(2024, 1, 1), dt(2024, 12, 31))

        written = catalog.write_data.call_args_list[0][0][0]
        assert {str(bar.bar_type) for bar in written} == {
            f"EURUSD.MT5-1-HOUR-{price_type}-EXTERNAL"
        }

    def test_data_type_is_bars(self, downloader, package):
        server = Server(Answer.ROWS, floors={Series.H1: LAST_OPEN})
        with serving(server):
            result = downloader.download_bars("EURUSD", dt(2024, 1, 1), dt(2024, 12, 31))

        assert result.data_type == "bars"


# ═════════════════════════════════════════════════════════════════════════════
# 4. download_ticks() — the backward walk over UTC days
# ═════════════════════════════════════════════════════════════════════════════


class TestDownloadTicksWalk:

    def test_walks_back_day_by_day_to_the_advertised_floor(self, downloader, catalog, package):
        floor = int(dt(2024, 1, 8, 12).timestamp())
        server = Server(*[Answer.ROWS] * 3, floors={Series.TICKS: floor})
        with serving(server):
            result = downloader.download_ticks("EURUSD", dt(2024, 1, 1), dt(2024, 1, 10, 12))

        end = int(dt(2024, 1, 10, 12).timestamp())
        midnight = int(dt(2024, 1, 10).timestamp())
        assert server.asked == [
            ("EURUSD", midnight, end),
            ("EURUSD", midnight - DAY, midnight - 1),
            ("EURUSD", midnight - 2 * DAY, midnight - DAY - 1),
        ]
        assert (result.total_written, catalog.write_data.call_count) == (3, 3)
        assert result.floor == datetime.fromtimestamp(floor, UTC)
        written = catalog.write_data.call_args_list[0][0][0]
        assert all(isinstance(tick, QuoteTick) for tick in written)

    def test_both_sizes_of_a_tick_are_the_instruments_largest_order(
        self, downloader, catalog, package
    ):
        server = Server(Answer.ROWS, floors={Series.TICKS: int(dt(2024, 1, 8).timestamp())})
        with serving(server):
            downloader.download_ticks("EURUSD", dt(2024, 1, 1), dt(2024, 1, 8))

        [tick] = catalog.write_data.call_args_list[0][0][0]
        assert (tick.bid_size, tick.ask_size) == (Quantity(1000, 2), Quantity(1000, 2))

    def test_walks_no_further_back_than_start(self, downloader, package):
        server = Server(Answer.ROWS, Answer.ROWS)
        with serving(server):
            result = downloader.download_ticks("EURUSD", dt(2024, 1, 9, 6), dt(2024, 1, 10, 12))

        assert server.asked[-1] == (
            "EURUSD",
            int(dt(2024, 1, 9, 6).timestamp()),
            int(dt(2024, 1, 10).timestamp()) - 1,
        )
        assert (result.chunks_processed, result.floor) == (2, None)

    def test_a_floor_measured_while_the_last_day_was_answered_is_recorded(
        self, downloader, catalog, package
    ):
        floor = int(dt(2024, 1, 9, 12).timestamp())
        server = Server(Answer.ROWS, Answer.ROWS, floors={Series.TICKS: floor}, measured_after=1)
        with serving(server):
            result = downloader.download_ticks("EURUSD", dt(2024, 1, 9, 6), dt(2024, 1, 10, 12))

        assert len(server.asked) == 2
        assert result.floor == datetime.fromtimestamp(floor, UTC)

    def test_a_day_answered_without_ticks_is_empty_and_writes_nothing(
        self, downloader, catalog, package
    ):
        server = Server(Answer.EMPTY)
        with serving(server):
            result = downloader.download_ticks("EURUSD", dt(2024, 1, 10), dt(2024, 1, 10, 12))

        assert (result.chunks_empty, result.total_written) == (1, 0)
        catalog.write_data.assert_not_called()

    def test_data_type_is_ticks(self, downloader, package):
        server = Server(Answer.ROWS, floors={Series.TICKS: int(dt(2024, 1, 8).timestamp())})
        with serving(server):
            result = downloader.download_ticks("EURUSD", dt(2024, 1, 1), dt(2024, 1, 8))

        assert result.data_type == "ticks"


# ═════════════════════════════════════════════════════════════════════════════
# 5. Instruments
# ═════════════════════════════════════════════════════════════════════════════


class TestInstruments:

    def test_a_symbol_not_found_is_an_error_and_nothing_is_asked(self, conn, catalog, package):
        provider = MagicMock()
        provider.get_instrument.return_value = None
        provider.load_symbol.side_effect = MT5SymbolNotFoundError("FAKESYM")
        downloader = MT5DataDownloader(conn, provider, catalog)

        with (
            patch("mt5connector.client.history.ticks") as ticks,
            patch("mt5connector.client.history.bars") as bars,
        ):
            ticks_result = downloader.download_ticks("FAKESYM", dt(2024, 1, 1), dt(2024, 1, 8))
            bars_result = downloader.download_bars("FAKESYM", dt(2024, 1, 1), dt(2024, 1, 8))

        assert "FAKESYM" in ticks_result.errors[0] and "FAKESYM" in bars_result.errors[0]
        assert (ticks_result.total_written, bars_result.total_written) == (0, 0)
        ticks.assert_not_called()
        bars.assert_not_called()

    def test_a_definition_the_venue_refuses_is_that_symbols_error(self, conn, catalog, package):
        provider = MagicMock()
        provider.get_instrument.return_value = None
        provider.load_symbol.side_effect = MT5InstrumentError("DE40: trade_calc_mode FUTURES")
        downloader = MT5DataDownloader(conn, provider, catalog)

        with patch("mt5connector.client.history.bars") as bars:
            result = downloader.download_bars("DE40", dt(2024, 1, 1), dt(2024, 1, 8))

        assert len(result.errors) == 1 and "DE40: trade_calc_mode FUTURES" in result.errors[0]
        bars.assert_not_called()

    def test_a_load_the_server_does_not_answer_is_raised_not_recorded(self, conn, catalog, package):
        provider = MagicMock()
        provider.get_instrument.return_value = None
        provider.load_symbol.side_effect = ServerUnreachable("symbol_select: refused")
        downloader = MT5DataDownloader(conn, provider, catalog)

        with pytest.raises(ServerUnreachable, match="symbol_select"):
            downloader.download_ticks("EURUSD", dt(2024, 1, 1), dt(2024, 1, 8))

    def test_raises_when_not_connected(self, catalog, provider):
        downloader = MT5DataDownloader(make_conn(connected=False), provider, catalog)

        with pytest.raises(MT5ConnectionError):
            downloader.download_ticks("EURUSD", dt(2024, 1, 1), dt(2024, 1, 8))

    def test_auto_loads_instrument_if_not_pre_loaded(self, conn, catalog, package):
        provider = MagicMock()
        provider.get_instrument.return_value = None
        provider.load_symbol.return_value = make_eurusd_instrument()
        downloader = MT5DataDownloader(conn, provider, catalog)
        server = Server(Answer.ROWS, floors={Series.TICKS: int(dt(2024, 1, 8).timestamp())})

        with serving(server):
            result = downloader.download_ticks("EURUSD", dt(2024, 1, 1), dt(2024, 1, 8))

        provider.load_symbol.assert_called_once_with("EURUSD")
        assert result.total_written == 1

    def test_symbol_whitespace_stripped_and_casing_preserved(self, downloader, package):
        server = Server(Answer.ROWS, floors={Series.TICKS: int(dt(2024, 1, 8).timestamp())})
        with serving(server):
            result = downloader.download_ticks("  EURUSDm  ", dt(2024, 1, 1), dt(2024, 1, 8))

        assert result.symbol == "EURUSDm"
        assert server.asked[0][0] == "EURUSDm"


# ═════════════════════════════════════════════════════════════════════════════
# 6. download_all()
# ═════════════════════════════════════════════════════════════════════════════


# Floors that end each walk from 2024-01-08 at its first window.
ONE_WINDOW_FLOORS = {
    Series.TICKS: int(dt(2024, 1, 8).timestamp()),
    Series.H1: int(dt(2024, 1, 7, 23).timestamp()),
    Series.D1: int(dt(2024, 1, 7).timestamp()),
}


class TestDownloadAll:

    def test_every_symbol_gets_its_ticks_then_each_timeframe(self, downloader, package):
        server = Server(*[Answer.ROWS] * 6, floors=ONE_WINDOW_FLOORS)
        with serving(server):
            results = downloader.download_all(
                ["EURUSD", "XAUUSD"], dt(2024, 1, 1), dt(2024, 1, 8), timeframes=[16385, 16408]
            )

        assert {symbol: [r.data_type for r in rs] for symbol, rs in results.items()} == {
            "EURUSD": ["ticks", "bars", "bars"],
            "XAUUSD": ["ticks", "bars", "bars"],
        }

    def test_default_timeframes_are_h1_and_d1(self, downloader, package):
        server = Server(*[Answer.ROWS] * 3, floors=ONE_WINDOW_FLOORS)
        with serving(server):
            downloader.download_all(["EURUSD"], dt(2024, 1, 1), dt(2024, 1, 8))

        assert [asked[1] for asked in server.asked[1:]] == [Series.H1, Series.D1]

    def test_no_timeframes_downloads_no_bars(self, downloader, package):
        server = Server(Answer.ROWS, floors=ONE_WINDOW_FLOORS)
        with serving(server):
            results = downloader.download_all(
                ["EURUSD"], dt(2024, 1, 1), dt(2024, 1, 8), timeframes=[]
            )

        assert [r.data_type for r in results["EURUSD"]] == ["ticks"]

    def test_skip_ticks_when_include_ticks_false(self, downloader, package):
        server = Server(Answer.ROWS, floors=ONE_WINDOW_FLOORS)
        with serving(server), patch("mt5connector.client.history.ticks") as ticks:
            downloader.download_all(
                ["EURUSD"], dt(2024, 1, 1), dt(2024, 1, 8), include_ticks=False, timeframes=[16385]
            )

        ticks.assert_not_called()

    def test_skip_bars_when_include_bars_false(self, downloader, package):
        server = Server(Answer.ROWS, floors=ONE_WINDOW_FLOORS)
        with serving(server), patch("mt5connector.client.history.bars") as bars:
            downloader.download_all(["EURUSD"], dt(2024, 1, 1), dt(2024, 1, 8), include_bars=False)

        bars.assert_not_called()
