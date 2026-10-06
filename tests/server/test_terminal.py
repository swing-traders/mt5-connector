import pytest

from mt5connector.server.terminal import package_call
from mt5connector.server.wire import mirror


@pytest.mark.parametrize(
    "name", [mirror.FunctionName.ORDER_CHECK, mirror.FunctionName.ORDER_SEND], ids=str
)
def test_a_trade_request_is_the_calls_one_positional_argument_and_no_keyword(name):
    request = {"action": mirror.TRADE_ACTION_DEAL, "symbol": "EURUSD", "volume": 0.1}

    assert package_call(mirror.FUNCTIONS[name], {"request": request}) == ((request,), {})
