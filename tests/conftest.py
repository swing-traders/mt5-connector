"""
tests/conftest.py

Shared fixtures: a valid MT5Config, and the shim mocked inside mt5connect.connection.
"""

from unittest.mock import MagicMock, patch

import pytest

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

        # Realistic Exness demo account
        account = MagicMock()
        account.login = 12345678
        account.server = "Exness-MT5Trial1"
        account.balance = 10000.00
        account.equity = 10050.25
        account.margin = 100.00
        account.margin_free = 9950.25
        account.margin_level = 10050.25
        account.currency = "USD"
        account.leverage = 2000
        account.profit = 50.25
        account.name = "Test Trader"
        account.company = "Exness Technologies Ltd"
        mock.account_info.return_value = account

        terminal = MagicMock()
        terminal.name = "MetaTrader 5"
        terminal.path = "C:\\Program Files\\MetaTrader 5"
        terminal.data_path = "C:\\Users\\Trader\\AppData\\Roaming\\MetaQuotes\\Terminal"
        terminal.connected = True
        terminal.ping_last = 3
        terminal.retransmission = 0.0
        mock.terminal_info.return_value = terminal

        yield mock
