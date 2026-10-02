"""The inventory against the installed MetaTrader5 package and a running terminal.

Runs only with MT5_LIVE_CONFORMANCE=1, on a Windows host whose terminal initialize() can reach."""

import importlib
import os
import sys
import time

import numpy as np
import pytest

from mt5connect import mirror

pytestmark = pytest.mark.skipif(
    os.environ.get("MT5_LIVE_CONFORMANCE") != "1", reason="MT5_LIVE_CONFORMANCE is not 1"
)


@pytest.fixture(scope="module")
def package():
    # tests/conftest.py stands a MagicMock in for MetaTrader5; the real package replaces it here.
    with pytest.MonkeyPatch.context() as patch:
        patch.delitem(sys.modules, "MetaTrader5", raising=False)
        yield importlib.import_module("MetaTrader5")


@pytest.fixture(scope="module")
def terminal(package):
    if not package.initialize():
        pytest.fail(f"initialize failed: {package.last_error()}")
    yield package
    package.shutdown()


@pytest.fixture(scope="module")
def symbol(terminal) -> str:
    symbols = terminal.symbols_get()
    if not symbols:
        pytest.fail(f"symbols_get answered {symbols!r}: {terminal.last_error()}")
    terminal.symbol_select(symbols[0].name, True)
    return symbols[0].name


def test_version_is_the_pinned_release(package):
    assert package.__version__ == mirror.PACKAGE_VERSION


@pytest.mark.parametrize("function", mirror.FUNCTIONS.values(), ids=str)
def test_function_exists(package, function):
    assert callable(getattr(package, function.name))


@pytest.mark.parametrize("name", ["Buy", "Sell", "Close"])
def test_helper_exists(package, name):
    assert callable(getattr(package, name))


def test_constants(package):
    assert {name: getattr(package, name) for name in mirror.CONSTANTS} == mirror.CONSTANTS


@pytest.mark.parametrize("struct", mirror.STRUCTS.values(), ids=str)
def test_struct_type_fields(package, struct):
    # The package's structs are PyStructSequence types, whose field names are __match_args__.
    assert getattr(package, struct.name).__match_args__ == struct.fields


def test_answered_struct_fields(terminal, symbol):
    answers = {
        mirror.StructName.TERMINAL_INFO: terminal.terminal_info(),
        mirror.StructName.ACCOUNT_INFO: terminal.account_info(),
        mirror.StructName.SYMBOL_INFO: terminal.symbol_info(symbol),
        mirror.StructName.TICK: terminal.symbol_info_tick(symbol),
    }
    for name, answer in answers.items():
        assert answer is not None, f"{name}: {terminal.last_error()}"
        assert tuple(answer._asdict()) == mirror.STRUCTS[name].fields


def test_rates_dtype(terminal, symbol):
    rates = terminal.copy_rates_from_pos(symbol, mirror.TIMEFRAME_M1, 0, 10)
    assert rates is not None, terminal.last_error()
    assert rates.dtype == np.dtype(list(mirror.RATES.dtype))


def test_ticks_dtype(terminal, symbol):
    ticks = terminal.copy_ticks_from(
        symbol, int(time.time()) - 7 * 86400, 10, mirror.COPY_TICKS_ALL
    )
    assert ticks is not None, terminal.last_error()
    assert ticks.dtype == np.dtype(list(mirror.TICKS.dtype))
