"""
tests/conftest.py

Shared fixtures: a valid MT5Config, the shim mocked inside mt5connect.connection, and the shim the
execution client calls.
"""

from unittest.mock import MagicMock, patch

import pytest
from venue_doubles import account_info, send_result, tick

from mt5connect import mirror
from mt5connect.config import MT5Config

# ─────────────────────────────────────────────────────────────────────────────
# SHARED CONFIG FIXTURE
# ─────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def config():
    """A valid MT5Config with fast reconnect settings for testing."""
    return MT5Config(
        account=12345678,
        password="test_password",
        server="Exness-MT5Trial1",
        symbols=["EURUSD", "XAUUSD"],
        server_url="http://127.0.0.1:5000",
        reconnect_initial_delay_s=0.01,
        reconnect_max_delay_s=0.05,
        reconnect_max_attempts=3,
        timeout_s=5.0,
    )


# ─────────────────────────────────────────────────────────────────────────────
# MT5 MODULE MOCK FIXTURE
# ─────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def mock_mt5():
    """
    Patches the shim (mt5connect.remote_mt5) used inside mt5connect.connection.

    Provides realistic defaults for all common mt5.* calls so tests
    can focus on adapter logic rather than MT5 plumbing.

    Override in individual tests:
        mock_mt5.initialize.return_value = False
        mock_mt5.last_error.return_value = (5, "IPC timeout")
    """
    with patch("mt5connect.connection.mt5") as mock:
        mock.initialize.return_value = True
        mock.shutdown.return_value = None
        mock.login.return_value = True
        mock.last_error.return_value = (0, "No error")

        mock.account_info.return_value = account_info(
            login=12345678,
            server="Exness-MT5Trial1",
            balance=10000.00,
            equity=10050.25,
            margin=100.00,
            margin_free=9950.25,
            margin_level=10050.25,
            currency="USD",
            leverage=2000,
            profit=50.25,
            name="Test Trader",
            company="Exness Technologies Ltd",
        )

        terminal = MagicMock()
        terminal.name = "MetaTrader 5"
        terminal.path = "C:\\Program Files\\MetaTrader 5"
        terminal.data_path = "C:\\Users\\Trader\\AppData\\Roaming\\MetaQuotes\\Terminal"
        terminal.connected = True
        terminal.trade_allowed = True
        terminal.ping_last = 3
        terminal.retransmission = 0.0
        mock.terminal_info.return_value = terminal

        yield mock


@pytest.fixture
def exec_shim():
    """The shim the execution client calls, holding the package's signatures: no order, deal or
    position, a quote, and every trade request completed."""
    with patch("mt5connect.execution.mt5", autospec=True) as shim:
        shim.orders_get.return_value = ()
        shim.positions_get.return_value = ()
        shim.history_deals_get.return_value = ()
        shim.history_orders_get.return_value = ()
        shim.symbol_info_tick.return_value = tick(bid=1.085, ask=1.0852)
        shim.order_send.return_value = send_result()
        shim.last_error.return_value = mirror.SUCCESS
        yield shim
