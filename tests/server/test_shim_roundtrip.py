"""The remote shim through waitress and the mirror routes to a package double, generated from the
inventory, plus the contract's pinned cases."""

import threading
import time
from datetime import UTC, datetime

import numpy as np
import pytest
import requests
from mirror_samples import (
    PACKAGE_TYPES,
    argument_samples,
    in_true_utc,
    result_sample,
    struct_sample,
)

from mt5connect import mirror
from mt5connect.errors import ServerUnreachable

FUNCTIONS = [
    function
    for function in mirror.FUNCTIONS.values()
    if function.name is not mirror.FunctionName.LAST_ERROR
]
FAILURE_ANSWERS = {mirror.Failure.NONE: None, mirror.Failure.FALSE: False}

RATES_FIELDS = ("time", "open", "high", "low", "close", "tick_volume", "spread", "real_volume")
TICKS_FIELDS = ("time", "bid", "ask", "last", "volume", "time_msc", "flags", "volume_real")
TRADE_REQUEST_FIELDS = (
    "action",
    "magic",
    "order",
    "symbol",
    "volume",
    "price",
    "stoplimit",
    "sl",
    "tp",
    "deviation",
    "type",
    "type_filling",
    "type_time",
    "expiration",
    "comment",
    "position",
    "position_by",
)


def _call_shim(remote, function, arguments):
    if function.calling is mirror.Calling.POSITIONAL:
        return getattr(remote, function.name)(*arguments.values())
    else:
        return getattr(remote, function.name)(**arguments)


@pytest.mark.parametrize("function", FUNCTIONS, ids=str)
def test_shim_round_trips_the_result_into_the_packages_types(remote, stub, function):
    answered = result_sample(function)
    getattr(stub, function.name).return_value = answered
    expected = in_true_utc(answered)

    answer = _call_shim(remote, function, argument_samples(function))

    if function.result is mirror.ResultKind.STRUCT:
        assert type(answer).__name__ == function.struct
        assert answer._fields == mirror.STRUCTS[function.struct].fields
        assert answer == expected
    elif function.result is mirror.ResultKind.STRUCTS:
        assert isinstance(answer, tuple)
        assert [type(item).__name__ for item in answer] == [function.struct] * len(expected)
        assert answer == expected
    elif function.result is mirror.ResultKind.ARRAY:
        assert answer.dtype == np.dtype(list(mirror.ARRAYS[function.array].dtype))
        assert answer.tolist() == expected.tolist()
    elif function.result is mirror.ResultKind.TUPLE:
        assert answer == expected
        assert isinstance(answer, tuple)
    else:
        assert answer == expected


@pytest.mark.parametrize(
    "function", [function for function in FUNCTIONS if function.failure in FAILURE_ANSWERS], ids=str
)
def test_shim_answers_the_failure_value_and_records_last_error(remote, stub, function):
    getattr(stub, function.name).return_value = FAILURE_ANSWERS[function.failure]
    stub.last_error.return_value = (-1, "Terminal: Call failed")

    answer = _call_shim(remote, function, argument_samples(function))

    assert answer is FAILURE_ANSWERS[function.failure]
    assert remote.last_error() == (-1, "Terminal: Call failed")


def test_positions_get_failure_and_emptiness_are_distinct(remote, stub, client):
    stub.positions_get.return_value = None
    stub.last_error.return_value = (-10005, "IPC timeout")
    assert client.post("/mt5/positions_get", json={}).json == {
        "ok": False,
        "error": {"code": -10005, "message": "IPC timeout"},
        "last_error": [-10005, "IPC timeout"],
    }
    assert remote.positions_get() is None
    assert remote.last_error() == (-10005, "IPC timeout")

    stub.positions_get.return_value = ()
    stub.last_error.return_value = (1, "Success")
    assert client.post("/mt5/positions_get", json={}).json == {
        "ok": True,
        "result": [],
        "last_error": [1, "Success"],
    }
    assert remote.positions_get() == ()


def test_a_call_the_server_is_not_ready_for_raises_server_unreachable(remote, stub, clock_status):
    clock_status.clear()

    with pytest.raises(
        ServerUnreachable,
        match="^positions_total: server not ready — the broker clock is not verified$",
    ):
        remote.positions_total()
    stub.positions_total.assert_not_called()


def test_last_error_follows_every_call_as_the_package_reports_it(remote, stub):
    stub.positions_get.return_value = None
    stub.positions_total.return_value = 3
    stub.last_error.side_effect = [(-10005, "IPC timeout"), (1, "Success")]

    assert remote.positions_get() is None
    assert remote.last_error() == (-10005, "IPC timeout")
    assert remote.positions_total() == 3
    assert remote.last_error() == (1, "Success")


def test_symbol_select_false_carries_the_packages_last_error(remote, stub):
    stub.symbol_select.return_value = False
    stub.last_error.return_value = (-4, "Terminal: Not found")

    assert remote.symbol_select("EURUSD", True) is False
    assert remote.last_error() == (-4, "Terminal: Not found")


def test_shutdown_keeps_the_servers_terminal_session(remote, stub):
    stub.positions_total.return_value = 3

    assert remote.shutdown() is None
    assert remote.positions_total() == 3
    stub.shutdown.assert_not_called()


@pytest.mark.parametrize(
    ("call", "function", "args", "kwargs"),
    [
        (
            lambda shim: shim.positions_get(symbol="EURUSD.a"),
            "positions_get",
            (),
            {"symbol": "EURUSD.a"},
        ),
        (lambda shim: shim.positions_get(group="*USD*"), "positions_get", (), {"group": "*USD*"}),
        (
            lambda shim: shim.history_orders_get(
                datetime(2026, 9, 1, tzinfo=UTC), datetime(2026, 9, 2, tzinfo=UTC), group="*USD*"
            ),
            "history_orders_get",
            # Under EDT the broker's clock runs three hours ahead of UTC.
            (datetime(2026, 9, 1, 3, tzinfo=UTC), datetime(2026, 9, 2, 3, tzinfo=UTC)),
            {"group": "*USD*"},
        ),
        (
            lambda shim: shim.history_deals_get(ticket=123456),
            "history_deals_get",
            (),
            {"ticket": 123456},
        ),
        (lambda shim: shim.symbols_get(group="*USD*"), "symbols_get", (), {"group": "*USD*"}),
        (
            lambda shim: shim.login(
                12345678, password="pw", server="example-server", timeout=90000
            ),
            "login",
            (12345678,),
            {"password": "pw", "server": "example-server", "timeout": 90000},
        ),
    ],
    ids=[
        "positions_get-symbol",
        "positions_get-group",
        "history_orders_get-range-group",
        "history_deals_get-ticket",
        "symbols_get-group",
        "login-timeout",
    ],
)
def test_parameters_reach_the_package(remote, stub, call, function, args, kwargs):
    getattr(stub, function).return_value = result_sample(mirror.FUNCTIONS[function])
    call(remote)
    getattr(stub, function).assert_called_once_with(*args, **kwargs)


def test_order_send_result_carries_the_trade_request(remote, stub):
    stub.order_send.return_value = struct_sample(mirror.StructName.ORDER_SEND_RESULT)

    result = remote.order_send({"action": 1, "symbol": "EURUSD", "volume": 0.1})

    assert type(result).__name__ == "OrderSendResult"
    assert type(result.request).__name__ == "TradeRequest"
    assert result.request._fields == TRADE_REQUEST_FIELDS
    assert result.request == in_true_utc(stub.order_send.return_value.request)


def test_copy_rates_range_answers_a_structured_array(remote, stub):
    stub.copy_rates_range.return_value = np.array(
        [(1_752_580_800, 1.1, 1.2, 1.0, 1.15, 120, 2, 0)],
        dtype=[
            (name, "<f8" if name in ("open", "high", "low", "close") else "<i8")
            for name in RATES_FIELDS
        ],
    )

    rates = remote.copy_rates_range("EURUSD", 16385, 1_752_570_000, 1_752_573_600)

    assert rates.dtype.names == RATES_FIELDS
    assert rates["time"][0] == 1_752_570_000
    assert rates["close"][0] == 1.15


def test_copy_ticks_range_answers_a_structured_array(remote, stub):
    stub.copy_ticks_range.return_value = np.array(
        [(1_752_580_800, 1.1, 1.1001, 0.0, 0, 1_752_580_800_123, 6, 0.0)],
        dtype=[(name, "<i8") for name in TICKS_FIELDS],
    )

    ticks = remote.copy_ticks_range("EURUSD", 1_752_570_000, 1_752_573_600, -1)

    assert ticks.dtype.names == TICKS_FIELDS
    assert ticks["time_msc"][0] == 1_752_570_000_123


def test_package_calls_never_overlap(served, stub):
    spans = []

    def slow_positions_total():
        entered = time.monotonic()
        time.sleep(0.3)
        spans.append((entered, time.monotonic()))
        return 0

    stub.positions_total.side_effect = slow_positions_total
    threads = [
        threading.Thread(target=requests.post, args=(f"{served}/mt5/positions_total",))
        for _ in range(2)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    first, second = sorted(spans)
    assert first[1] <= second[0]


def test_a_failure_reports_its_own_last_error(served, stub):
    errors = {"current": (1, "Success")}

    def failing_positions_get():
        errors["current"] = (-1, "first")
        time.sleep(0.3)
        return None

    def failing_orders_get():
        errors["current"] = (-2, "second")
        return None

    stub.positions_get.side_effect = failing_positions_get
    stub.orders_get.side_effect = failing_orders_get
    stub.last_error.side_effect = lambda: errors["current"]
    answers = {}

    def post(function):
        answers[function] = requests.post(f"{served}/mt5/{function}", json={}).json()

    first = threading.Thread(target=post, args=("positions_get",))
    first.start()
    time.sleep(0.1)
    post("orders_get")
    first.join()

    assert answers["positions_get"]["error"] == {"code": -1, "message": "first"}
    assert answers["orders_get"]["error"] == {"code": -2, "message": "second"}


def _tick(bid, ask):
    return struct_sample(mirror.StructName.TICK)._replace(bid=bid, ask=ask)


def _send_result(retcode):
    return struct_sample(mirror.StructName.ORDER_SEND_RESULT)._replace(retcode=retcode)


def _position(ticket, position_type, volume):
    return struct_sample(mirror.StructName.TRADE_POSITION)._replace(
        ticket=ticket, type=position_type, volume=volume
    )


def test_buy_at_a_price_sends_one_market_order(remote, stub):
    stub.order_send.return_value = _send_result(10009)

    result = remote.Buy("EURUSD", 0.1, 1.1, comment="entry", ticket=7)

    assert result.retcode == 10009
    stub.order_send.assert_called_once_with(
        {
            "action": 1,
            "symbol": "EURUSD",
            "volume": 0.1,
            "type": 0,
            "price": 1.1,
            "deviation": 10,
            "comment": "entry",
            "position": 7,
        }
    )
    stub.symbol_info_tick.assert_not_called()


def test_buy_at_market_resends_at_the_ask_after_a_requote(remote, stub):
    stub.symbol_info_tick.side_effect = [_tick(1.1, 1.2), _tick(1.3, 1.4)]
    stub.order_send.side_effect = [_send_result(10004), _send_result(10009)]

    result = remote.Buy("EURUSD", 0.1)

    assert result.retcode == 10009
    prices = [call.args[0]["price"] for call in stub.order_send.call_args_list]
    assert prices == [1.2, 1.4]


def test_sell_at_market_answers_none_when_the_send_fails(remote, stub):
    stub.symbol_info_tick.return_value = _tick(1.1, 1.2)
    stub.order_send.return_value = None

    assert remote.Sell("EURUSD", 0.1) is None
    assert stub.order_send.call_args.args[0]["type"] == 1
    assert stub.order_send.call_args.args[0]["price"] == 1.1


def test_close_closes_each_position_with_the_opposite_order(remote, stub):
    stub.positions_get.return_value = (_position(11, 0, 0.1), _position(12, 1, 0.2))
    stub.symbol_info_tick.return_value = _tick(1.1, 1.2)
    stub.order_send.return_value = _send_result(10009)

    assert remote.Close("EURUSD") is True

    stub.positions_get.assert_called_once_with(symbol="EURUSD")
    sent = [call.args[0] for call in stub.order_send.call_args_list]
    assert [(r["type"], r["price"], r["volume"], r["position"]) for r in sent] == [
        (1, 1.1, 0.1, 11),
        (0, 1.2, 0.2, 12),
    ]


def test_close_answers_partially_when_some_positions_close(remote, stub):
    stub.positions_get.return_value = (_position(11, 0, 0.1), _position(12, 0, 0.1))
    stub.symbol_info_tick.return_value = _tick(1.1, 1.2)
    stub.order_send.side_effect = [_send_result(10009), _send_result(10006)]

    assert remote.Close("EURUSD") == "Partially"


def test_close_by_ticket_answers_false_when_nothing_closes(remote, stub):
    stub.positions_get.return_value = (_position(11, 0, 0.1),)
    stub.symbol_info_tick.return_value = _tick(1.1, 1.2)
    stub.order_send.return_value = _send_result(10006)

    assert remote.Close("EURUSD", ticket=11) is False
    stub.positions_get.assert_called_once_with(ticket=11)


def test_close_answers_none_without_a_quote(remote, stub):
    stub.positions_get.return_value = (_position(11, 0, 0.1),)
    stub.symbol_info_tick.return_value = None
    stub.last_error.return_value = (-4, "Terminal: Not found")

    assert remote.Close("EURUSD") is None
    stub.order_send.assert_not_called()


def test_package_types_are_the_shims(remote):
    for name, package_type in PACKAGE_TYPES.items():
        assert remote.STRUCT_TYPES[name]._fields == package_type._fields
