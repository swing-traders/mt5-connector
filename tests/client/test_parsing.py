"""The terminal's ticks and bars as NautilusTrader data: quote ticks from a history row, venue bars
stamped at their close and typed by the price the venue charts them on, and the timeframe map."""

from decimal import Decimal

import numpy as np
import pytest
from nautilus_trader.model.data import Bar, BarType, QuoteTick
from nautilus_trader.model.enums import BarAggregation
from nautilus_trader.model.instruments import CurrencyPair
from nautilus_trader.model.objects import Price, Quantity
from venue_doubles import account_info, symbol_info

from mt5connector.client.connection import AccountSnapshot
from mt5connector.client.parsing import (
    _MT5_TIMEFRAME_MAP,
    parse_quote_tick,
    parse_symbol_info,
    venue_bar,
    venue_bar_type,
)
from mt5connector.wire import mirror

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────


def instrument(**fields):
    """The instrument the parser builds from a symbol definition under `fields`."""
    account = AccountSnapshot.from_mt5(account_info())
    return parse_symbol_info(symbol_info(**fields), account, Decimal(0), 0)


TICKS = np.dtype(list(mirror.TICKS.dtype))


def make_tick_row(bid=1.08500, ask=1.08502, time_msc=1700000000123):
    """A history tick row as the shim decodes it."""
    return np.array([(time_msc // 1000, bid, ask, 0.0, 0, time_msc, 6, 0.0)], dtype=TICKS)[0]


def make_rate(
    open_=1.085,
    high=1.090,
    low=1.080,
    close=1.088,
    tick_volume=1000,
    spread=2,
    real_volume=0,
    time_s=1700000000,
):
    """Build a numpy structured array row matching mt5.copy_rates_range() output."""
    dtype = np.dtype(
        [
            ("time", np.int64),
            ("open", np.float64),
            ("high", np.float64),
            ("low", np.float64),
            ("close", np.float64),
            ("tick_volume", np.int64),
            ("spread", np.int32),
            ("real_volume", np.int64),
        ]
    )
    arr = np.array(
        [(time_s, open_, high, low, close, tick_volume, spread, real_volume)], dtype=dtype
    )
    return arr[0]


# ═════════════════════════════════════════════════════════════════════════════
# 14. parse_quote_tick()
# ═════════════════════════════════════════════════════════════════════════════


class TestParseQuoteTick:

    @pytest.fixture
    def eurusd(self):
        return instrument(name="EURUSD")

    def test_returns_quote_tick(self, eurusd):
        result = parse_quote_tick(make_tick_row(bid=1.08500, ask=1.08502), eurusd)
        assert isinstance(result, QuoteTick)

    def test_instrument_id_matches(self, eurusd):
        result = parse_quote_tick(make_tick_row(), eurusd)
        assert result.instrument_id == eurusd.id

    def test_bid_and_ask_are_the_venues_at_the_instruments_precision(self, eurusd):
        result = parse_quote_tick(make_tick_row(bid=1.08500, ask=1.08502), eurusd)
        assert (result.bid_price, result.ask_price) == (
            Price.from_str("1.08500"),
            Price.from_str("1.08502"),
        )

    def test_ts_event_is_the_rows_millisecond_time(self, eurusd):
        result = parse_quote_tick(make_tick_row(time_msc=1700000000123), eurusd)
        assert result.ts_event == 1700000000123 * 1_000_000

    def test_both_sizes_are_the_instruments_largest_order(self):
        inst = instrument(name="EURUSD", volume_max=500.0, volume_step=0.01)
        result = parse_quote_tick(make_tick_row(), inst)
        assert (result.bid_size, result.ask_size) == (Quantity(500, 2), Quantity(500, 2))

    def test_gold_tick_precision(self):
        inst = instrument(
            name="XAUUSD",
            digits=2,
            point=0.01,
            trade_tick_size=0.01,
            currency_base="XAU",
            currency_profit="USD",
            trade_contract_size=100.0,
        )
        result = parse_quote_tick(make_tick_row(bid=1985.50, ask=1985.75), inst)
        assert result.bid_price == Price.from_str("1985.50")


# ═════════════════════════════════════════════════════════════════════════════
# 15. venue_bar_type() and venue_bar()
# ═════════════════════════════════════════════════════════════════════════════


class TestVenueBarType:

    def test_a_bid_charted_symbols_bars_are_bid(self):
        eurusd = instrument(name="EURUSD", chart_mode=mirror.SYMBOL_CHART_MODE_BID)
        assert venue_bar_type(eurusd, mirror.TIMEFRAME_H1) == BarType.from_str(
            "EURUSD.MT5-1-HOUR-BID-EXTERNAL"
        )

    def test_a_last_charted_symbols_bars_are_last(self):
        eurusd = instrument(name="EURUSD", chart_mode=mirror.SYMBOL_CHART_MODE_LAST)
        assert venue_bar_type(eurusd, mirror.TIMEFRAME_H1) == BarType.from_str(
            "EURUSD.MT5-1-HOUR-LAST-EXTERNAL"
        )

    @pytest.mark.parametrize("info", [{}, None])
    def test_a_definition_stating_no_chart_mode_is_last(self, info):
        eurusd = instrument(name="EURUSD")
        unstated = CurrencyPair.from_dict(CurrencyPair.to_dict(eurusd) | {"info": info})
        assert venue_bar_type(unstated, mirror.TIMEFRAME_M5) == BarType.from_str(
            "EURUSD.MT5-5-MINUTE-LAST-EXTERNAL"
        )

    def test_a_chart_mode_outside_the_venues_is_refused(self):
        eurusd = instrument(name="EURUSD")
        odd = CurrencyPair.from_dict(CurrencyPair.to_dict(eurusd) | {"info": {"chart_mode": "ASK"}})
        with pytest.raises(ValueError, match="ASK"):
            venue_bar_type(odd, mirror.TIMEFRAME_H1)

    def test_a_month_has_no_fixed_interval(self):
        with pytest.raises(ValueError, match="49153"):
            venue_bar_type(instrument(name="EURUSD"), mirror.TIMEFRAME_MN1)

    def test_an_unknown_timeframe_raises_naming_it(self):
        with pytest.raises(ValueError, match="99999"):
            venue_bar_type(instrument(name="EURUSD"), 99999)

    @pytest.mark.parametrize(
        ("timeframe", "step", "aggregation"),
        [
            (mirror.TIMEFRAME_M1, 1, BarAggregation.MINUTE),
            (mirror.TIMEFRAME_M2, 2, BarAggregation.MINUTE),
            (mirror.TIMEFRAME_M3, 3, BarAggregation.MINUTE),
            (mirror.TIMEFRAME_M4, 4, BarAggregation.MINUTE),
            (mirror.TIMEFRAME_M5, 5, BarAggregation.MINUTE),
            (mirror.TIMEFRAME_M6, 6, BarAggregation.MINUTE),
            (mirror.TIMEFRAME_M10, 10, BarAggregation.MINUTE),
            (mirror.TIMEFRAME_M12, 12, BarAggregation.MINUTE),
            (mirror.TIMEFRAME_M15, 15, BarAggregation.MINUTE),
            (mirror.TIMEFRAME_M20, 20, BarAggregation.MINUTE),
            (mirror.TIMEFRAME_M30, 30, BarAggregation.MINUTE),
            (mirror.TIMEFRAME_H1, 1, BarAggregation.HOUR),
            (mirror.TIMEFRAME_H2, 2, BarAggregation.HOUR),
            (mirror.TIMEFRAME_H3, 3, BarAggregation.HOUR),
            (mirror.TIMEFRAME_H4, 4, BarAggregation.HOUR),
            (mirror.TIMEFRAME_H6, 6, BarAggregation.HOUR),
            (mirror.TIMEFRAME_H8, 8, BarAggregation.HOUR),
            (mirror.TIMEFRAME_H12, 12, BarAggregation.HOUR),
            (mirror.TIMEFRAME_D1, 1, BarAggregation.DAY),
            (mirror.TIMEFRAME_W1, 1, BarAggregation.WEEK),
        ],
    )
    def test_each_timeframe_with_a_fixed_period_is_its_step_and_aggregation(
        self, timeframe, step, aggregation
    ):
        spec = venue_bar_type(instrument(name="EURUSD"), timeframe).spec
        assert (spec.step, spec.aggregation) == (step, aggregation)


class TestVenueBar:

    @pytest.fixture
    def eurusd(self):
        return instrument(name="EURUSD")

    def test_prices_and_tick_volume_are_the_venues(self, eurusd):
        bar_type = venue_bar_type(eurusd, mirror.TIMEFRAME_H1)
        result = venue_bar(make_rate(1.085, 1.090, 1.080, 1.088, 1500), bar_type, eurusd)
        assert isinstance(result, Bar)
        assert (result.open, result.high, result.low, result.close, result.volume) == (
            Price.from_str("1.08500"),
            Price.from_str("1.09000"),
            Price.from_str("1.08000"),
            Price.from_str("1.08800"),
            Quantity(1500, 0),
        )
        assert result.bar_type == bar_type

    def test_a_bar_is_stamped_at_its_close_in_event_and_init(self, eurusd):
        bar_type = venue_bar_type(eurusd, mirror.TIMEFRAME_M1)
        result = venue_bar(make_rate(time_s=1_752_570_000), bar_type, eurusd)
        assert (result.ts_event, result.ts_init) == (1_752_570_060 * 1_000_000_000,) * 2


# ═════════════════════════════════════════════════════════════════════════════
# 16. Timeframe mapping completeness
# ═════════════════════════════════════════════════════════════════════════════


class TestTimeframeMapping:

    def test_all_minute_timeframes_present(self):
        minute_tfs = [1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30]
        for tf in minute_tfs:
            assert tf in _MT5_TIMEFRAME_MAP, f"M{tf} timeframe missing"

    def test_all_hour_timeframes_present(self):
        hour_tfs = [16385, 16386, 16387, 16388, 16390, 16392, 16396]
        for tf in hour_tfs:
            assert tf in _MT5_TIMEFRAME_MAP, f"Hour TF {tf} missing"

    def test_d1_present(self):
        assert 16408 in _MT5_TIMEFRAME_MAP

    def test_w1_present(self):
        assert 32769 in _MT5_TIMEFRAME_MAP

    def test_mn1_present(self):
        assert 49153 in _MT5_TIMEFRAME_MAP

    def test_all_aggregations_are_valid(self):
        valid = {
            BarAggregation.MINUTE,
            BarAggregation.HOUR,
            BarAggregation.DAY,
            BarAggregation.WEEK,
            BarAggregation.MONTH,
        }
        for _, (step, agg) in _MT5_TIMEFRAME_MAP.items():
            assert agg in valid
            assert step >= 1
