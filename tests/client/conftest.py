"""Shared fixtures: a valid MT5Config, the venue behind the execution client's hub session, and a
hub double on a free loopback port."""

from unittest.mock import MagicMock

import pytest
from hub_double import HubDouble
from venue_doubles import account_info, tick, trade_result
from websockets.asyncio.server import serve

from mt5connector.client.config import MT5Config

# ─────────────────────────────────────────────────────────────────────────────
# SHARED CONFIG FIXTURE
# ─────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def config():
    """A valid MT5Config with fast reconnect settings for testing."""
    return MT5Config(
        account=12345678,
        server="Exness-MT5Trial1",
        ws_url="ws://127.0.0.1:9000",
        reconnect_initial_delay_s=0.01,
        reconnect_max_delay_s=0.05,
        timeout_s=5.0,
    )


# ─────────────────────────────────────────────────────────────────────────────
# THE EXECUTION CLIENT'S VENUE
# ─────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def exec_venue():
    """The calls the execution client asks its hub session, each answered as the venue answers it:
    the account, no order, deal or position, a quote, and every trade request sent."""
    venue = MagicMock(
        spec=[
            "account_info",
            "orders_get",
            "positions_get",
            "history_orders_get",
            "history_deals_get",
            "symbol_info_tick",
            "order_send",
        ]
    )
    venue.account_info.return_value = account_info()
    venue.orders_get.return_value = ()
    venue.positions_get.return_value = ()
    venue.history_deals_get.return_value = ()
    venue.history_orders_get.return_value = ()
    venue.symbol_info_tick.return_value = tick(bid=1.085, ask=1.0852)
    venue.order_send.return_value = trade_result()
    return venue


@pytest.fixture
async def hub():
    double = HubDouble()
    server = await serve(double.handler, "127.0.0.1", 0)
    double.port = server.sockets[0].getsockname()[1]
    yield double
    server.close()
