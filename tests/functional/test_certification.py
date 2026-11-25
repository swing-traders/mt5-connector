from types import SimpleNamespace

import pytest

from functional.certify import MAGIC, CertificationError, certify
from mt5connector.wire import calls, mirror


class Session:
    def __init__(self, *, close_failures=0, buy_failure=False, close_answer_lost=False):
        self.account = SimpleNamespace(
            login=1,
            server="Example-Demo",
            trade_mode=mirror.ACCOUNT_TRADE_MODE_DEMO,
            trade_allowed=True,
        )
        self.terminal = SimpleNamespace(connected=True, trade_allowed=True)
        self.position = None
        self.orders = []
        self.sent = []
        self.close_failures = close_failures
        self.buy_failure = buy_failure
        self.close_answer_lost = close_answer_lost

    async def deferring(self, call, **arguments):
        if call == calls.Call.ACCOUNT_INFO:
            result = self.account
        elif call == calls.Call.TERMINAL_INFO:
            result = self.terminal
        elif call == calls.Call.POSITIONS_GET:
            result = [] if self.position is None else [self.position]
        elif call == calls.Call.ORDERS_GET:
            result = self.orders.copy()
        elif call == calls.Call.SYMBOL_INFO:
            result = SimpleNamespace(name="TEST", volume_min=0.01, digits=2, filling_mode=3)
        elif call == calls.Call.SYMBOL_INFO_TICK:
            result = SimpleNamespace(bid=100.0, ask=100.1)
        elif call == calls.Call.ORDER_SEND:
            request = arguments["request"]
            self.sent.append(request.copy())
            if request["type"] == mirror.ORDER_TYPE_BUY:
                self.position = SimpleNamespace(
                    ticket=42,
                    symbol="TEST",
                    magic=MAGIC,
                    volume=0.01,
                    type=mirror.POSITION_TYPE_BUY,
                )
                if self.buy_failure:
                    raise TimeoutError("answer lost after placement")
            elif self.close_failures:
                self.close_failures -= 1
                raise TimeoutError("close failed")
            else:
                self.position = None
                if self.close_answer_lost:
                    self.close_answer_lost = False
                    raise TimeoutError("close answer lost after execution")
            result = SimpleNamespace(retcode=mirror.TRADE_RETCODE_PLACED)
        else:
            raise AssertionError(call)
        return SimpleNamespace(result=result)


async def test_the_smallest_buy_is_closed_and_the_account_is_flat():
    session = Session()
    await certify(session, login=1, server="Example-Demo", symbol="TEST")
    assert session.position is None
    assert session.orders == []
    assert [request["type"] for request in session.sent] == [
        mirror.ORDER_TYPE_BUY,
        mirror.ORDER_TYPE_SELL,
    ]
    assert [request["volume"] for request in session.sent] == [0.01, 0.01]
    assert session.sent[1]["position"] == 42


async def test_a_failed_close_is_retried_once():
    session = Session(close_failures=1)
    await certify(session, login=1, server="Example-Demo", symbol="TEST")
    assert session.position is None
    assert len(session.sent) == 3


async def test_two_failed_closes_fail_loudly_without_a_third_attempt():
    session = Session(close_failures=2)
    with pytest.raises(CertificationError, match="close failed after 2 attempts"):
        await certify(session, login=1, server="Example-Demo", symbol="TEST")
    assert session.position is not None
    assert len(session.sent) == 3


async def test_a_lost_buy_answer_still_closes_the_filled_position():
    session = Session(buy_failure=True)
    with pytest.raises(TimeoutError, match="answer lost"):
        await certify(session, login=1, server="Example-Demo", symbol="TEST")
    assert session.position is None
    assert len(session.sent) == 2


async def test_a_lost_close_answer_rechecks_flatness_without_closing_twice():
    session = Session(close_answer_lost=True)
    await certify(session, login=1, server="Example-Demo", symbol="TEST")
    assert session.position is None
    assert len(session.sent) == 2


@pytest.mark.parametrize("field", ["login", "server", "trade_mode", "trade_allowed"])
async def test_invalid_login_or_permissions_refuse_before_placing_an_order(field):
    session = Session()
    setattr(session.account, field, False if field == "trade_allowed" else "different")
    with pytest.raises(CertificationError):
        await certify(session, login=1, server="Example-Demo", symbol="TEST")
    assert session.sent == []


async def test_existing_exposure_is_left_untouched():
    session = Session()
    held = SimpleNamespace(ticket=43, magic=0)
    session.position = held
    with pytest.raises(CertificationError, match="not flat"):
        await certify(session, login=1, server="Example-Demo", symbol="TEST")
    assert session.position is held
    assert session.sent == []


async def test_a_terminal_that_disallows_trading_refuses_before_any_order():
    session = Session()
    session.terminal.trade_allowed = False
    with pytest.raises(CertificationError, match="does not allow trading"):
        await certify(session, login=1, server="Example-Demo", symbol="TEST")
    assert session.sent == []


async def test_a_resting_order_is_left_untouched():
    session = Session()
    resting = SimpleNamespace(ticket=44, magic=0)
    session.orders = [resting]
    with pytest.raises(CertificationError, match="not flat"):
        await certify(session, login=1, server="Example-Demo", symbol="TEST")
    assert session.orders == [resting]
    assert session.sent == []
