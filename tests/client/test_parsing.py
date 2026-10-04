"""
tests/client/test_parsing.py

The terminal's ticks and bars as NautilusTrader data: quote ticks from a live tick or a history
row, bars stamped at their close, and the timeframe map. Symbol definitions are pinned in
tests/client/test_instrument_definitions.py.
"""

from decimal import Decimal
from unittest.mock import MagicMock

import numpy as np
import pytest
from nautilus_trader.model.data import Bar, QuoteTick
from nautilus_trader.model.enums import BarAggregation
from venue_doubles import account_info, symbol_info

from mt5connector.client.connection import AccountSnapshot
from mt5connector.client.parsing import (
    _MT5_TIMEFRAME_MAP,
    parse_bar,
    parse_quote_tick,
    parse_symbol_info,
)
from mt5connector.wire import mirror

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────


def instrument(**fields):
    """The instrument the parser builds from a symbol definition under `fields`."""
    account = AccountSnapshot.from_mt5(account_info())
    return parse_symbol_info(symbol_info(**fields), account, Decimal(0), 0)


def make_tick(bid=1.08500, ask=1.08502, last=1.08501, volume=1, time_s=1700000000, time_msc=None):
    tick = MagicMock()
    tick.bid = bid
    tick.ask = ask
    tick.last = last
    tick.volume = volume
    tick.time = time_s
    tick.time_msc = time_msc or (time_s * 1000)
    return tick


def make_tick_row(bid=1.08500, ask=1.08502, time_s=1700000000, time_msc=1700000000123):
    """Build a numpy structured array row matching mt5.copy_ticks_range() output."""
    dtype = np.dtype(
        [
            ("time", np.int64),
            ("bid", np.float64),
            ("ask", np.float64),
            ("last", np.float64),
            ("volume", np.uint64),
            ("time_msc", np.int64),
            ("flags", np.uint32),
            ("volume_real", np.float64),
        ]
    )
    return np.array([(time_s, bid, ask, 0.0, 0, time_msc, 6, 0.0)], dtype=dtype)[0]


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
        tick = make_tick(bid=1.08500, ask=1.08502)
        result = parse_quote_tick(tick, eurusd)
        assert isinstance(result, QuoteTick)

    def test_instrument_id_matches(self, eurusd):
        tick = make_tick()
        result = parse_quote_tick(tick, eurusd)
        assert result.instrument_id == eurusd.id

    def test_bid_price_correct(self, eurusd):
        tick = make_tick(bid=1.08500)
        result = parse_quote_tick(tick, eurusd)
        assert float(result.bid_price) == pytest.approx(1.08500)

    def test_ask_price_correct(self, eurusd):
        tick = make_tick(ask=1.08502)
        result = parse_quote_tick(tick, eurusd)
        assert float(result.ask_price) == pytest.approx(1.08502)

    def test_bid_price_precision(self, eurusd):
        tick = make_tick(bid=1.08500)
        result = parse_quote_tick(tick, eurusd)
        assert result.bid_price.precision == 5

    def test_ask_price_precision(self, eurusd):
        tick = make_tick(ask=1.08502)
        result = parse_quote_tick(tick, eurusd)
        assert result.ask_price.precision == 5

    def test_ts_event_in_nanoseconds(self):
        inst = instrument(name="EURUSD")
        tick = make_tick(time_s=1700000000)
        result = parse_quote_tick(tick, inst)
        expected_ns = 1700000000 * 1_000_000_000
        assert result.ts_event == expected_ns

    def test_structured_row_keeps_millisecond_time(self, eurusd):
        row = make_tick_row(time_s=1700000000, time_msc=1700000000123)
        result = parse_quote_tick(row, eurusd)
        assert result.ts_event == 1700000000123 * 1_000_000

    def test_bid_size_nominal(self, eurusd):
        """MT5 has no depth — bid_size should be a large nominal value."""
        tick = make_tick()
        result = parse_quote_tick(tick, eurusd)
        assert float(result.bid_size) == 1_000_000

    def test_ask_size_nominal(self, eurusd):
        tick = make_tick()
        result = parse_quote_tick(tick, eurusd)
        assert float(result.ask_size) == 1_000_000

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
        tick = make_tick(bid=1985.50, ask=1985.75)
        result = parse_quote_tick(tick, inst)
        assert result.bid_price.precision == 2
        assert float(result.bid_price) == pytest.approx(1985.50)


# ═════════════════════════════════════════════════════════════════════════════
# 15. parse_bar()
# ═════════════════════════════════════════════════════════════════════════════


class TestParseBar:

    @pytest.fixture
    def eurusd(self):
        return instrument(name="EURUSD")

    def test_returns_bar(self, eurusd):
        rate = make_rate()
        result = parse_bar(rate, eurusd, timeframe=16385)  # H1
        assert isinstance(result, Bar)

    def test_open_price(self, eurusd):
        rate = make_rate(open_=1.085)
        result = parse_bar(rate, eurusd, timeframe=16385)
        assert float(result.open) == pytest.approx(1.085)

    def test_high_price(self, eurusd):
        rate = make_rate(high=1.090)
        result = parse_bar(rate, eurusd, timeframe=16385)
        assert float(result.high) == pytest.approx(1.090)

    def test_low_price(self, eurusd):
        rate = make_rate(low=1.080)
        result = parse_bar(rate, eurusd, timeframe=16385)
        assert float(result.low) == pytest.approx(1.080)

    def test_close_price(self, eurusd):
        rate = make_rate(close=1.088)
        result = parse_bar(rate, eurusd, timeframe=16385)
        assert float(result.close) == pytest.approx(1.088)

    def test_volume(self, eurusd):
        rate = make_rate(tick_volume=1500)
        result = parse_bar(rate, eurusd, timeframe=16385)
        assert float(result.volume) == pytest.approx(1500)

    def test_ts_event_is_the_close_in_nanoseconds(self, eurusd):
        rate = make_rate(time_s=1700000000)
        result = parse_bar(rate, eurusd, timeframe=16385)
        assert result.ts_event == (1700000000 + 3600) * 1_000_000_000

    def test_a_minute_bar_is_stamped_at_its_close_in_event_and_init(self, eurusd):
        rate = make_rate(time_s=1_752_570_000)
        result = parse_bar(rate, eurusd, timeframe=1)
        assert result.ts_event == 1_752_570_060 * 1_000_000_000
        assert result.ts_init == result.ts_event

    def test_a_month_bar_has_no_close_to_stamp(self, eurusd):
        with pytest.raises(ValueError, match="49153"):
            parse_bar(make_rate(), eurusd, timeframe=49153)

    def test_bar_type_instrument_id(self, eurusd):
        rate = make_rate()
        result = parse_bar(rate, eurusd, timeframe=16385)
        assert result.bar_type.instrument_id == eurusd.id

    def test_m1_aggregation(self, eurusd):
        rate = make_rate()
        result = parse_bar(rate, eurusd, timeframe=1)
        assert result.bar_type.spec.aggregation == BarAggregation.MINUTE
        assert result.bar_type.spec.step == 1

    def test_h1_aggregation(self, eurusd):
        rate = make_rate()
        result = parse_bar(rate, eurusd, timeframe=16385)
        assert result.bar_type.spec.aggregation == BarAggregation.HOUR
        assert result.bar_type.spec.step == 1

    def test_h4_aggregation(self, eurusd):
        rate = make_rate()
        result = parse_bar(rate, eurusd, timeframe=16388)
        assert result.bar_type.spec.aggregation == BarAggregation.HOUR
        assert result.bar_type.spec.step == 4

    def test_d1_aggregation(self, eurusd):
        rate = make_rate()
        result = parse_bar(rate, eurusd, timeframe=16408)
        assert result.bar_type.spec.aggregation == BarAggregation.DAY
        assert result.bar_type.spec.step == 1

    def test_an_unknown_timeframe_raises_naming_it(self, eurusd):
        with pytest.raises(ValueError, match="99999"):
            parse_bar(make_rate(), eurusd, timeframe=99999)

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
        self, eurusd, timeframe, step, aggregation
    ):
        spec = parse_bar(make_rate(), eurusd, timeframe=timeframe).bar_type.spec
        assert (spec.step, spec.aggregation) == (step, aggregation)


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
