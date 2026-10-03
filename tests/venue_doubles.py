"""The terminal's account, symbol and tick structs as the shim decodes them, every field the package
carries present, for tests that feed the client a venue."""

from mt5connect import mirror
from mt5connect.remote_mt5 import STRUCT_TYPES

_ACCOUNT_STRINGS = frozenset({"name", "server", "currency", "company"})
_SYMBOL_STRINGS = frozenset(
    {
        "basis",
        "category",
        "currency_base",
        "currency_profit",
        "currency_margin",
        "bank",
        "description",
        "exchange",
        "formula",
        "isin",
        "name",
        "page",
        "path",
    }
)


def account_info(**fields):
    """A hedging account in USD with two currency digits that may trade, under `fields`."""
    struct = STRUCT_TYPES[mirror.StructName.ACCOUNT_INFO]
    values = {name: "" if name in _ACCOUNT_STRINGS else 0 for name in struct._fields}
    values |= {
        "login": 12345678,
        "server": "Broker-Demo",
        "currency": "USD",
        "currency_digits": 2,
        "margin_mode": mirror.ACCOUNT_MARGIN_MODE_RETAIL_HEDGING,
        "trade_allowed": True,
        "leverage": 500,
        "balance": 10000.0,
        "equity": 10050.25,
        "margin": 100.0,
        "margin_free": 9950.25,
        "margin_level": 10050.25,
        "credit": 0.0,
        "profit": 50.25,
    }
    return struct(**(values | fields))


def symbol_info(**fields):
    """EURUSD as a FOREX pair a run may fully trade, under `fields`."""
    struct = STRUCT_TYPES[mirror.StructName.SYMBOL_INFO]
    values = {name: "" if name in _SYMBOL_STRINGS else 0 for name in struct._fields}
    values |= {
        "name": "EURUSD",
        "digits": 5,
        "point": 0.00001,
        "trade_tick_size": 0.00001,
        "volume_min": 0.01,
        "volume_max": 500.0,
        "volume_step": 0.01,
        "trade_contract_size": 100000.0,
        "currency_base": "EUR",
        "currency_profit": "USD",
        "currency_margin": "EUR",
        "trade_calc_mode": mirror.SYMBOL_CALC_MODE_FOREX,
        "trade_mode": mirror.SYMBOL_TRADE_MODE_FULL,
        "chart_mode": mirror.SYMBOL_CHART_MODE_BID,
        "filling_mode": 3,
        "margin_initial": 0.0,
        "margin_maintenance": 0.0,
    }
    return struct(**(values | fields))


def tick(bid: float, ask: float, time: int = 1_760_000_000):
    """A quote the terminal answers for a selected symbol; `time` 0 is the all-zero struct an
    unselected or closed symbol answers."""
    struct = STRUCT_TYPES[mirror.StructName.TICK]
    return struct(
        time=time,
        bid=bid,
        ask=ask,
        last=0.0,
        volume=0,
        time_msc=time * 1000,
        flags=6,
        volume_real=0.0,
    )
