"""Shared fixtures: a valid MT5Config, the shim mocked inside mt5connector.client.connection, and
the shim the execution client calls."""

from unittest.mock import MagicMock, patch

import pytest
from venue_doubles import account_info, send_result, tick

from mt5connector.client.config import MT5Config
from mt5connector.wire import mirror

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
    """The shim as mt5connector.client.connection calls it, answering a terminal that initializes,
    logs in and reports a USD account; a test overrides the answer it needs."""
    with patch("mt5connector.client.connection.mt5") as mock:
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
    with patch("mt5connector.client.execution.mt5", autospec=True) as shim:
        shim.orders_get.return_value = ()
        shim.positions_get.return_value = ()
        shim.history_deals_get.return_value = ()
        shim.history_orders_get.return_value = ()
        shim.symbol_info_tick.return_value = tick(bid=1.085, ask=1.0852)
        shim.order_send.return_value = send_result()
        shim.last_error.return_value = mirror.SUCCESS
        yield shim
