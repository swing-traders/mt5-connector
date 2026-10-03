import importlib
import sys

import pytest
from mt5_wheel import PackageSurface, fetch_wheel, read_surface


@pytest.fixture(scope="session")
def package_surface(request) -> PackageSurface:
    return read_surface(fetch_wheel(request.config.cache.mkdir("mt5-wheel")))


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
