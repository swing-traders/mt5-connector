"""Converts the terminal's symbol definitions, ticks and bars into NautilusTrader domain objects. A
definition is typed by its calc mode and filled from the venue's own facts alone."""

from __future__ import annotations

import calendar
import time
from decimal import Decimal
from enum import StrEnum
from typing import TYPE_CHECKING

from nautilus_trader.model.data import Bar, BarSpecification, BarType, MarkPriceUpdate, QuoteTick
from nautilus_trader.model.enums import AssetClass, BarAggregation, PriceType
from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue
from nautilus_trader.model.instruments import Cfd, CurrencyPair
from nautilus_trader.model.objects import Price, Quantity

from mt5connector.client.currencies import base_currency, venue_currency
from mt5connector.client.errors import MT5InstrumentError
from mt5connector.wire import mirror
from mt5connector.wire.history_wire import Series, bar_series

if TYPE_CHECKING:
    from mt5connector.client.connection import AccountSnapshot

InstrumentAny = CurrencyPair | Cfd


# ─────────────────────────────────────────────────────────────────────────────
# THE VENUE'S CLOSED SETS
# ─────────────────────────────────────────────────────────────────────────────


class CalcMode(StrEnum):
    """How the venue calculates a symbol's profit and margin: `symbol_info().trade_calc_mode`."""

    FOREX = "FOREX"
    FOREX_NO_LEVERAGE = "FOREX_NO_LEVERAGE"
    FUTURES = "FUTURES"
    CFD = "CFD"
    CFDINDEX = "CFDINDEX"
    CFDLEVERAGE = "CFDLEVERAGE"
    EXCH_STOCKS = "EXCH_STOCKS"
    EXCH_FUTURES = "EXCH_FUTURES"
    EXCH_OPTIONS = "EXCH_OPTIONS"
    EXCH_OPTIONS_MARGIN = "EXCH_OPTIONS_MARGIN"
    EXCH_BONDS = "EXCH_BONDS"
    EXCH_STOCKS_MOEX = "EXCH_STOCKS_MOEX"
    EXCH_BONDS_MOEX = "EXCH_BONDS_MOEX"
    SERV_COLLATERAL = "SERV_COLLATERAL"


class TradeMode(StrEnum):
    """What the venue lets a run do with a symbol: `symbol_info().trade_mode`."""

    DISABLED = "DISABLED"
    LONGONLY = "LONGONLY"
    SHORTONLY = "SHORTONLY"
    CLOSEONLY = "CLOSEONLY"
    FULL = "FULL"


class ChartMode(StrEnum):
    """The one price a symbol's bars are built from: `symbol_info().chart_mode`."""

    BID = "BID"
    LAST = "LAST"


class BarVolume(StrEnum):
    """What a bar's volume counts."""

    TICK_COUNT = "tick_count"


_CALC_MODES = {
    mirror.SYMBOL_CALC_MODE_FOREX: CalcMode.FOREX,
    mirror.SYMBOL_CALC_MODE_FOREX_NO_LEVERAGE: CalcMode.FOREX_NO_LEVERAGE,
    mirror.SYMBOL_CALC_MODE_FUTURES: CalcMode.FUTURES,
    mirror.SYMBOL_CALC_MODE_CFD: CalcMode.CFD,
    mirror.SYMBOL_CALC_MODE_CFDINDEX: CalcMode.CFDINDEX,
    mirror.SYMBOL_CALC_MODE_CFDLEVERAGE: CalcMode.CFDLEVERAGE,
    mirror.SYMBOL_CALC_MODE_EXCH_STOCKS: CalcMode.EXCH_STOCKS,
    mirror.SYMBOL_CALC_MODE_EXCH_FUTURES: CalcMode.EXCH_FUTURES,
    mirror.SYMBOL_CALC_MODE_EXCH_OPTIONS: CalcMode.EXCH_OPTIONS,
    mirror.SYMBOL_CALC_MODE_EXCH_OPTIONS_MARGIN: CalcMode.EXCH_OPTIONS_MARGIN,
    mirror.SYMBOL_CALC_MODE_EXCH_BONDS: CalcMode.EXCH_BONDS,
    mirror.SYMBOL_CALC_MODE_EXCH_STOCKS_MOEX: CalcMode.EXCH_STOCKS_MOEX,
    mirror.SYMBOL_CALC_MODE_EXCH_BONDS_MOEX: CalcMode.EXCH_BONDS_MOEX,
    mirror.SYMBOL_CALC_MODE_SERV_COLLATERAL: CalcMode.SERV_COLLATERAL,
}
_TRADE_MODES = {
    mirror.SYMBOL_TRADE_MODE_DISABLED: TradeMode.DISABLED,
    mirror.SYMBOL_TRADE_MODE_LONGONLY: TradeMode.LONGONLY,
    mirror.SYMBOL_TRADE_MODE_SHORTONLY: TradeMode.SHORTONLY,
    mirror.SYMBOL_TRADE_MODE_CLOSEONLY: TradeMode.CLOSEONLY,
    mirror.SYMBOL_TRADE_MODE_FULL: TradeMode.FULL,
}
_CHART_MODES = {
    mirror.SYMBOL_CHART_MODE_BID: ChartMode.BID,
    mirror.SYMBOL_CHART_MODE_LAST: ChartMode.LAST,
}

FOREX_MODES = frozenset({CalcMode.FOREX, CalcMode.FOREX_NO_LEVERAGE})
CFD_MODES = frozenset({CalcMode.CFD, CalcMode.CFDINDEX, CalcMode.CFDLEVERAGE})

# The venue's bar day opens at broker midnight, 17:00 in New York, and its bar week on Sunday's.
_BAR_TZ = "America/New_York"
_BAR_DAY_OPEN = "17:00"
_BAR_WEEK_OPEN = calendar.Day.SUNDAY.name


def calc_mode(info) -> CalcMode:
    """The symbol's calc mode; raises MT5InstrumentError for a value the package lacks."""
    return _member(info.trade_calc_mode, _CALC_MODES, "trade_calc_mode")


def trade_mode(info) -> TradeMode:
    """The symbol's trade mode; raises MT5InstrumentError for a value the package lacks."""
    return _member(info.trade_mode, _TRADE_MODES, "trade_mode")


def chart_mode(info) -> ChartMode:
    """The symbol's chart mode; raises MT5InstrumentError for a value the package lacks."""
    return _member(info.chart_mode, _CHART_MODES, "chart_mode")


def _member(value: int, members: dict, field: str):
    if value not in members:
        raise MT5InstrumentError(f"{field} {value} is unknown")
    return members[value]


# ─────────────────────────────────────────────────────────────────────────────
# SYMBOL DEFINITIONS
# ─────────────────────────────────────────────────────────────────────────────


def parse_symbol_info(
    info, venue: Venue, account: AccountSnapshot, taker_fee: Decimal, ts: int
) -> InstrumentAny:
    """The instrument an mt5.symbol_info() definition describes at `venue`: a FOREX calc mode is a
    CurrencyPair and a CFD calc mode a Cfd; any other mode raises MT5InstrumentError naming it.
    Margins stay NT's defaults: the venue states them as money per lot, which no ratio expresses
    without a price."""
    kind = calc_mode(info)
    if kind in FOREX_MODES:
        return CurrencyPair(**_definition(info, kind, venue, account, taker_fee, ts))
    elif kind in CFD_MODES:
        return Cfd(
            asset_class=_asset_class(kind),
            **_definition(info, kind, venue, account, taker_fee, ts),
        )
    else:
        raise MT5InstrumentError(f"trade_calc_mode {kind} is not one this adapter trades")


def _definition(
    info, kind: CalcMode, venue: Venue, account: AccountSnapshot, taker_fee: Decimal, ts: int
) -> dict:
    if info.trade_tick_size <= 0:
        raise MT5InstrumentError(f"trade_tick_size {info.trade_tick_size} is not positive")
    size_precision = _decimals(info.volume_step)
    return {
        "instrument_id": InstrumentId(Symbol(info.name), venue),
        "raw_symbol": Symbol(info.name),
        "base_currency": base_currency(info.currency_base, account),
        "quote_currency": venue_currency(info.currency_profit, account),
        "price_precision": info.digits,
        "size_precision": size_precision,
        "price_increment": Price(info.trade_tick_size, info.digits),
        "size_increment": Quantity(info.volume_step, size_precision),
        "multiplier": Quantity(info.trade_contract_size, _decimals(info.trade_contract_size)),
        "min_quantity": Quantity(info.volume_min, size_precision),
        "max_quantity": Quantity(info.volume_max, size_precision),
        "maker_fee": Decimal(0),
        "taker_fee": taker_fee,
        "ts_event": ts,
        "ts_init": ts,
        "info": {
            "chart_mode": chart_mode(info).value,
            "filling_mode": info.filling_mode,
            "trade_calc_mode": kind.value,
            "trade_mode": trade_mode(info).value,
            "trade_stops_level": info.trade_stops_level,
            "trade_freeze_level": info.trade_freeze_level,
            "volume_limit": info.volume_limit,
            "currency_margin": info.currency_margin,
            "bar_tz": _BAR_TZ,
            "bar_day_open": _BAR_DAY_OPEN,
            "bar_week_open": _BAR_WEEK_OPEN,
            "bar_volume": BarVolume.TICK_COUNT.value,
        },
    }


def _asset_class(kind: CalcMode) -> AssetClass:
    if kind == CalcMode.CFDINDEX:
        return AssetClass.INDEX
    else:
        return AssetClass.ALTERNATIVE


def finite_decimal(value: float, field: str) -> Decimal:
    """A raw terminal number as an exact Decimal; raises MT5InstrumentError for NaN or infinity."""
    number = Decimal(str(value))
    if not number.is_finite():
        raise MT5InstrumentError(f"{field} {value} is not finite")
    return number


def _decimals(value: float) -> int:
    """The decimal places of a step or size as the terminal states it."""
    return max(0, -Decimal(str(value)).normalize().as_tuple().exponent)


# ─────────────────────────────────────────────────────────────────────────────
# TICKS AND BARS
# ─────────────────────────────────────────────────────────────────────────────


def parse_quote_tick(row, instrument: InstrumentAny) -> QuoteTick:
    """A history tick row as a QuoteTick stamped at its time_msc: bid and ask as the venue quotes
    them, and both sizes the instrument's largest order, since the venue publishes no depth."""
    return QuoteTick(
        instrument_id=instrument.id,
        bid_price=instrument.make_price(row["bid"]),
        ask_price=instrument.make_price(row["ask"]),
        bid_size=instrument.max_quantity,
        ask_size=instrument.max_quantity,
        ts_event=int(row["time_msc"]) * 1_000_000,
        ts_init=time.time_ns(),
    )


# Map MT5 timeframe integers to NautilusTrader BarAggregation + step
_MT5_TIMEFRAME_MAP: dict[int, tuple[int, BarAggregation]] = {
    1: (1, BarAggregation.MINUTE),  # M1
    2: (2, BarAggregation.MINUTE),  # M2
    3: (3, BarAggregation.MINUTE),  # M3
    4: (4, BarAggregation.MINUTE),  # M4
    5: (5, BarAggregation.MINUTE),  # M5
    6: (6, BarAggregation.MINUTE),  # M6
    10: (10, BarAggregation.MINUTE),  # M10
    12: (12, BarAggregation.MINUTE),  # M12
    15: (15, BarAggregation.MINUTE),  # M15
    20: (20, BarAggregation.MINUTE),  # M20
    30: (30, BarAggregation.MINUTE),  # M30
    16385: (1, BarAggregation.HOUR),  # H1
    16386: (2, BarAggregation.HOUR),  # H2
    16387: (3, BarAggregation.HOUR),  # H3
    16388: (4, BarAggregation.HOUR),  # H4
    16390: (6, BarAggregation.HOUR),  # H6
    16392: (8, BarAggregation.HOUR),  # H8
    16396: (12, BarAggregation.HOUR),  # H12
    16408: (1, BarAggregation.DAY),  # D1
    32769: (1, BarAggregation.WEEK),  # W1
    49153: (1, BarAggregation.MONTH),  # MN1
}
# The terminal's series of each (step, aggregation) it has a timeframe of; a month has no fixed
# period.
_VENUE_SERIES: dict[tuple[int, BarAggregation], Series] = {
    step_and_aggregation: bar_series(timeframe)
    for timeframe, step_and_aggregation in _MT5_TIMEFRAME_MAP.items()
    if step_and_aggregation[1] != BarAggregation.MONTH
}


def quote_tick_from_frame(frame: dict, instrument: InstrumentAny, ts_init: int) -> QuoteTick:
    """A pushed tick as a QuoteTick stamped at its time_msc: bid and ask as the venue quotes them,
    and both sizes the instrument's largest order, since the venue publishes no depth."""
    return QuoteTick(
        instrument_id=instrument.id,
        bid_price=instrument.make_price(frame["bid"]),
        ask_price=instrument.make_price(frame["ask"]),
        bid_size=instrument.max_quantity,
        ask_size=instrument.max_quantity,
        ts_event=frame["time_msc"] * 1_000_000,
        ts_init=ts_init,
    )


def mark_at_mid(quote: QuoteTick) -> MarkPriceUpdate:
    """The mark at a quote's mid, one decimal finer than the quote so every mid is exact."""
    mid = (quote.bid_price.as_decimal() + quote.ask_price.as_decimal()) / 2
    return MarkPriceUpdate(
        quote.instrument_id,
        Price(mid, quote.bid_price.precision + 1),
        quote.ts_event,
        quote.ts_init,
    )


def venue_bar(rates, bar_type: BarType, instrument: InstrumentAny) -> Bar:
    """A venue bar of `bar_type` from an MqlRates row stamped at its open, stamped at its close:
    the open plus the bar's interval, its prices and tick volume as the venue counts them."""
    ts_event = int(rates["time"]) * 1_000_000_000 + bar_type.spec.get_interval_ns()
    return Bar(
        bar_type=bar_type,
        open=instrument.make_price(rates["open"]),
        high=instrument.make_price(rates["high"]),
        low=instrument.make_price(rates["low"]),
        close=instrument.make_price(rates["close"]),
        volume=Quantity(int(rates["tick_volume"]), 0),
        ts_event=ts_event,
        ts_init=ts_event,
    )


def venue_series(bar_type: BarType) -> Series:
    """The terminal's series of a venue bar type; raises ValueError for a step and aggregation the
    terminal has no timeframe for."""
    key = (bar_type.spec.step, bar_type.spec.aggregation)
    if key not in _VENUE_SERIES:
        raise ValueError(f"{bar_type}: the terminal has no timeframe of its step")
    return _VENUE_SERIES[key]


def venue_bar_type(instrument: InstrumentAny, timeframe: int) -> BarType:
    """The venue bar type of an MT5 timeframe's bars of `instrument`, priced as its definition's
    chart mode states, LAST where it states none; raises ValueError for a timeframe the package does
    not define, for MN1, since a month has no fixed interval, and for a chart mode the venue
    lacks."""
    if timeframe not in _MT5_TIMEFRAME_MAP:
        raise ValueError(f"timeframe {timeframe} is unknown")
    step, aggregation = _MT5_TIMEFRAME_MAP[timeframe]
    if aggregation == BarAggregation.MONTH:
        raise ValueError(f"timeframe {timeframe}: a month has no fixed interval")
    charted = ChartMode((instrument.info or {}).get("chart_mode", ChartMode.LAST))
    if charted == ChartMode.BID:
        return BarType(instrument.id, BarSpecification(step, aggregation, PriceType.BID))
    else:
        return BarType(instrument.id, BarSpecification(step, aggregation, PriceType.LAST))
