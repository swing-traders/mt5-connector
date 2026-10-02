"""The inventory, and the shim generated from it, against the pinned MetaTrader5 wheel."""

import ast
import inspect

import pytest
from mt5_wheel import METH_KEYWORDS, METH_NOARGS, METH_VARARGS

import mt5connect.remote_mt5 as shim
from mt5connect import mirror

# The package's functions as the MQL5 reference indexes them.
DOCUMENTED_FUNCTIONS = {
    "initialize",
    "login",
    "shutdown",
    "version",
    "last_error",
    "account_info",
    "terminal_info",
    "symbols_total",
    "symbols_get",
    "symbol_info",
    "symbol_info_tick",
    "symbol_select",
    "market_book_add",
    "market_book_get",
    "market_book_release",
    "copy_rates_from",
    "copy_rates_from_pos",
    "copy_rates_range",
    "copy_ticks_from",
    "copy_ticks_range",
    "orders_total",
    "orders_get",
    "order_calc_margin",
    "order_calc_profit",
    "order_check",
    "order_send",
    "positions_total",
    "positions_get",
    "history_orders_total",
    "history_orders_get",
    "history_deals_total",
    "history_deals_get",
}

PINNED_CONSTANTS = {
    "SYMBOL_CHART_MODE_BID": 0,
    "DEAL_REASON_SL": 4,
    "DEAL_REASON_TP": 5,
    "DEAL_ENTRY_OUT": 1,
    "TRADE_RETCODE_CONNECTION": 10031,
    "TRADE_RETCODE_DONE_PARTIAL": 10010,
    "TRADE_RETCODE_INVALID_PRICE": 10015,
    "TRADE_RETCODE_INVALID_STOPS": 10016,
    "ACCOUNT_MARGIN_MODE_RETAIL_HEDGING": 2,
    "TIMEFRAME_M1": 1,
    "TIMEFRAME_H1": 16385,
    "TIMEFRAME_D1": 16408,
    "COPY_TICKS_INFO": 1,
    "COPY_TICKS_TRADE": 2,
    "COPY_TICKS_ALL": -1,
}

# MqlTradeRequest's fields in the MQL5 reference's order.
TRADE_REQUEST_FIELDS = (
    "action",
    "magic",
    "order",
    "symbol",
    "volume",
    "price",
    "stoplimit",
    "sl",
    "tp",
    "deviation",
    "type",
    "type_filling",
    "type_time",
    "expiration",
    "comment",
    "position",
    "position_by",
)

HELPERS = ("Buy", "Sell", "Close")

CALLING_FLAGS = {
    mirror.Calling.NO_ARGS: METH_NOARGS,
    mirror.Calling.POSITIONAL: METH_VARARGS,
    mirror.Calling.KEYWORDS: METH_VARARGS | METH_KEYWORDS,
}


def test_wheel_is_the_pinned_release(package_surface):
    assert package_surface.version == "5.0.6231"
    assert mirror.PACKAGE_VERSION == package_surface.version
    assert shim.__version__ == package_surface.version


def test_package_namespace_takes_the_compiled_core(package_surface):
    assert package_surface.star_imports_core


def test_the_documented_functions_are_the_packages(package_surface):
    assert len(DOCUMENTED_FUNCTIONS) == 32
    assert set(package_surface.functions) == DOCUMENTED_FUNCTIONS


def test_inventory_functions_are_the_packages(package_surface):
    assert {name.value for name in mirror.FUNCTIONS} == set(package_surface.functions)


@pytest.mark.parametrize("function", mirror.FUNCTIONS.values(), ids=str)
def test_calling_convention_matches_the_package(package_surface, function):
    assert package_surface.functions[function.name].flags == CALLING_FLAGS[function.calling]


@pytest.mark.parametrize("function", mirror.FUNCTIONS.values(), ids=str)
def test_shim_has_every_function(function):
    assert callable(getattr(shim, function.name))


def test_inventory_constants_are_the_packages_both_ways(package_surface):
    assert mirror.CONSTANTS == package_surface.constants


@pytest.mark.parametrize("name", sorted(PINNED_CONSTANTS))
def test_pinned_constant(package_surface, name):
    assert package_surface.constants[name] == PINNED_CONSTANTS[name]
    assert mirror.CONSTANTS[name] == PINNED_CONSTANTS[name]
    assert getattr(shim, name) == PINNED_CONSTANTS[name]


def test_shim_carries_every_constant(package_surface):
    assert {name: getattr(shim, name) for name in package_surface.constants} == (
        package_surface.constants
    )


def test_inventory_structs_are_the_packages(package_surface):
    assert {name.value for name in mirror.STRUCTS} == set(package_surface.structs)


@pytest.mark.parametrize("struct", mirror.STRUCTS.values(), ids=str)
def test_struct_field_order_matches_the_package(package_surface, struct):
    assert struct.fields == package_surface.structs[struct.name]
    assert shim.STRUCT_TYPES[struct.name]._fields == package_surface.structs[struct.name]


def test_pinned_struct_facts(package_surface):
    assert len(package_surface.structs["SymbolInfo"]) == 96
    assert package_surface.structs["TradeRequest"] == TRADE_REQUEST_FIELDS
    assert "price_stoplimit" in package_surface.structs["TradeOrder"]
    request = mirror.StructName.TRADE_REQUEST
    assert mirror.STRUCTS[mirror.StructName.ORDER_SEND_RESULT].nested == {"request": request}
    assert mirror.STRUCTS[mirror.StructName.ORDER_CHECK_RESULT].nested == {"request": request}


def test_the_packages_helpers_are_buy_sell_close(package_surface):
    assert set(package_surface.helpers) == set(HELPERS)


@pytest.mark.parametrize("name", HELPERS)
def test_shim_helper_has_the_package_signature(package_surface, name):
    assert inspect.signature(getattr(shim, name)) == _signature(package_surface.helpers[name])


def _signature(function: ast.FunctionDef) -> inspect.Signature:
    arguments = function.args
    parameters = []
    defaults = [None] * (len(arguments.args) - len(arguments.defaults)) + arguments.defaults
    for argument, default in zip(arguments.args, defaults, strict=True):
        parameters.append(_parameter(argument, inspect.Parameter.POSITIONAL_OR_KEYWORD, default))
    for argument, default in zip(arguments.kwonlyargs, arguments.kw_defaults, strict=True):
        parameters.append(_parameter(argument, inspect.Parameter.KEYWORD_ONLY, default))
    return inspect.Signature(parameters)


def _parameter(argument: ast.arg, kind, default: ast.expr | None) -> inspect.Parameter:
    if default is None:
        return inspect.Parameter(argument.arg, kind)
    return inspect.Parameter(argument.arg, kind, default=ast.literal_eval(default))
