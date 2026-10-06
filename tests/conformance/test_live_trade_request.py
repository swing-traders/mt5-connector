"""A trade request through the client shim against a running MT5 server and the package behind it.
Runs only when MT5_LIVE_SERVER_URL names the server; it checks a request and places nothing."""

import os
from http import HTTPStatus

import pytest
import requests

from mt5connector.client.execution import SymbolFilling
from mt5connector.client.remote_mt5 import RemoteMT5
from mt5connector.wire import mirror

SERVER_URL = os.environ.get("MT5_LIVE_SERVER_URL")

pytestmark = pytest.mark.skipif(SERVER_URL is None, reason="MT5_LIVE_SERVER_URL is not set")


@pytest.fixture(scope="module")
def remote():
    transport = RemoteMT5(SERVER_URL)
    yield transport
    transport.close_session()


@pytest.fixture(scope="module")
def symbol() -> str:
    """The chart symbol of the clock sample the server's health reports, which its EA publishes."""
    response = requests.get(f"{SERVER_URL}/health", timeout=10)
    assert response.status_code == HTTPStatus.OK, response.text
    return response.json()["result"]["symbol"]


def test_order_check_of_a_market_request_answers_a_retcode(remote, symbol):
    info = remote.symbol_info(symbol)
    tick = remote.symbol_info_tick(symbol)
    assert info is not None and tick is not None, remote.last_error()
    allowed = SymbolFilling(info.filling_mode)
    if SymbolFilling.IOC in allowed:
        filling = mirror.ORDER_FILLING_IOC
    elif SymbolFilling.FOK in allowed:
        filling = mirror.ORDER_FILLING_FOK
    else:
        filling = mirror.ORDER_FILLING_RETURN

    result = remote.order_check(
        {
            "action": mirror.TRADE_ACTION_DEAL,
            "symbol": symbol,
            "volume": 0.01,
            "type": mirror.ORDER_TYPE_BUY,
            "price": tick.ask,
            "type_filling": filling,
        }
    )

    assert result is not None, remote.last_error()
    assert isinstance(result.retcode, int)
    assert remote.last_error() == mirror.SUCCESS
