"""Shared fixtures: a valid MT5Config, the transport an MT5Connection builds, mocked, and a
transport for the execution client's connection."""

from unittest.mock import MagicMock, create_autospec, patch

import pytest
from venue_doubles import account_info, send_result, tick

from mt5connector.client.config import MT5Config
from mt5connector.client.remote_mt5 import RemoteMT5
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
    """The transport every MT5Connection built in the test holds, answering a terminal that
    initializes, logs in and reports a USD account; a test overrides the answer it needs."""
    mock = create_autospec(RemoteMT5, instance=True)
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

    with patch("mt5connector.client.connection.RemoteMT5", autospec=True) as transport:
        transport.return_value = mock
        yield mock


@pytest.fixture
def exec_shim():
    """A transport for the execution client's connection, holding the package's signatures: no
    order, deal or position, a quote, and every trade request completed."""
    shim = create_autospec(RemoteMT5, instance=True)
    shim.orders_get.return_value = ()
    shim.positions_get.return_value = ()
    shim.history_deals_get.return_value = ()
    shim.history_orders_get.return_value = ()
    shim.symbol_info_tick.return_value = tick(bid=1.085, ask=1.0852)
    shim.order_send.return_value = send_result()
    shim.last_error.return_value = mirror.SUCCESS
    return shim
