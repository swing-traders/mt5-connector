"""Every mirror route, generated from the inventory, against a package double."""

import logging

import numpy as np
import pytest
from mirror_samples import (
    argument_samples,
    array_sample,
    expected_call,
    expected_json,
    in_true_utc,
    result_sample,
)

from mt5connect import mirror

FUNCTIONS = list(mirror.FUNCTIONS.values())
WITH_REQUIRED = [
    function for function in FUNCTIONS if any(param.required for param in function.params)
]
FAILING = [
    function
    for function in FUNCTIONS
    if function.failure in (mirror.Failure.NONE, mirror.Failure.FALSE)
]
LISTING = [function for function in FUNCTIONS if function.result is mirror.ResultKind.STRUCTS]
FAILURE_ANSWERS = {mirror.Failure.NONE: None, mirror.Failure.FALSE: False}


UNVERIFIED = {"ok": False, "error": {"code": -1, "message": "the broker clock is not verified"}}


def test_the_server_routes_the_mirror_health_commissions_and_the_server_time_relay(app):
    routes = {
        (rule.rule, method)
        for rule in app.url_map.iter_rules()
        for method in rule.methods - {"HEAD", "OPTIONS"}
    }
    assert routes == {(f"/mt5/{function.name}", "POST") for function in FUNCTIONS} | {
        ("/health", "GET"),
        ("/commissions/<symbol>", "GET"),
        ("/relay/server_time", "POST"),
    }


@pytest.mark.parametrize("function", FUNCTIONS, ids=str)
def test_every_mirror_route_is_unavailable_while_the_clock_is_not_verified(
    client, stub, clock_status, function
):
    clock_status.clear()

    response = client.post(f"/mt5/{function.name}", json=argument_samples(function))

    assert (response.status_code, response.json) == (503, UNVERIFIED)
    assert stub.mock_calls == []


def test_commissions_are_unavailable_while_the_clock_is_not_verified(client, clock_status):
    clock_status.clear()

    response = client.get("/commissions/EURUSD")

    assert (response.status_code, response.json) == (503, UNVERIFIED)


def test_answer_carries_the_last_error_read_after_the_call(client, stub):
    stub.positions_total.return_value = 3
    stub.last_error.return_value = (1, "Success")
    assert client.post("/mt5/positions_total").json == {
        "ok": True,
        "result": 3,
        "last_error": [1, "Success"],
    }


def test_failure_carries_its_error_as_the_last_error(client, stub):
    stub.positions_get.return_value = None
    stub.last_error.return_value = (-10005, "IPC timeout")
    assert client.post("/mt5/positions_get").json == {
        "ok": False,
        "error": {"code": -10005, "message": "IPC timeout"},
        "last_error": [-10005, "IPC timeout"],
    }


def test_shutdown_answers_without_ending_the_servers_session(client, stub, caplog):
    caplog.set_level(logging.INFO, logger="mt5server.app.app")
    response = client.post("/mt5/shutdown", environ_base={"REMOTE_ADDR": "10.0.0.7"})
    assert response.json == {"ok": True, "result": None, "last_error": [1, "Success"]}
    stub.shutdown.assert_not_called()
    assert "10.0.0.7" in caplog.text


@pytest.mark.parametrize("function", FUNCTIONS, ids=str)
def test_route_forwards_every_parameter_and_answers_the_result(client, stub, function):
    arguments = argument_samples(function)
    result = result_sample(function)
    getattr(stub, function.name).return_value = result

    response = client.post(f"/mt5/{function.name}", json=arguments)

    assert response.status_code == 200
    if function.server_session:
        getattr(stub, function.name).assert_not_called()
        assert response.json == {"ok": True, "result": None, "last_error": [1, "Success"]}
    elif function.name is mirror.FunctionName.LAST_ERROR:
        # The call itself, then the last_error() read that follows every call.
        assert stub.last_error.call_count == 2
        assert response.json == {
            "ok": True,
            "result": expected_json(result),
            "last_error": list(result),
        }
    else:
        args, kwargs = expected_call(function, arguments)
        getattr(stub, function.name).assert_called_once_with(*args, **kwargs)
        assert response.json == {
            "ok": True,
            "result": expected_json(in_true_utc(result)),
            "last_error": list(stub.last_error.return_value),
        }


@pytest.mark.parametrize(
    "function",
    [function for function in FUNCTIONS if function.result is mirror.ResultKind.STRUCT],
    ids=str,
)
def test_struct_result_keeps_the_packages_field_order(client, stub, function):
    getattr(stub, function.name).return_value = result_sample(function)
    response = client.post(f"/mt5/{function.name}", json=argument_samples(function))
    assert list(response.json["result"]) == list(mirror.STRUCTS[function.struct].fields)


@pytest.mark.parametrize("function", FAILING, ids=str)
def test_failure_answers_the_packages_last_error(client, stub, function):
    getattr(stub, function.name).return_value = FAILURE_ANSWERS[function.failure]
    stub.last_error.return_value = (-10005, "IPC timeout")

    response = client.post(f"/mt5/{function.name}", json=argument_samples(function))

    if function.name is mirror.FunctionName.TERMINAL_INFO:
        assert response.status_code == 503
    else:
        assert response.status_code == 200
    assert response.json == {
        "ok": False,
        "error": {"code": -10005, "message": "IPC timeout"},
        "last_error": [-10005, "IPC timeout"],
    }
    stub.last_error.assert_called_once_with()


def test_terminal_info_failure_answers_unavailable(client, stub):
    stub.terminal_info.return_value = None
    stub.last_error.return_value = (-10004, "No IPC connection")
    response = client.post("/mt5/terminal_info")
    assert response.status_code == 503
    assert response.json == {
        "ok": False,
        "error": {"code": -10004, "message": "No IPC connection"},
        "last_error": [-10004, "No IPC connection"],
    }


HISTORY_QUERIES = [mirror.FunctionName.HISTORY_ORDERS_GET, mirror.FunctionName.HISTORY_DEALS_GET]


@pytest.mark.parametrize("function_name", HISTORY_QUERIES)
@pytest.mark.parametrize(
    "arguments",
    [{}, {"group": "*USD*"}, {"date_from": 1704067200}, {"date_to": 1704153600, "group": "*"}],
    ids=["nothing", "group-only", "date_from-only", "date_to-and-group"],
)
def test_history_query_in_no_call_form_is_refused(client, stub, function_name, arguments):
    response = client.post(f"/mt5/{function_name}", json=arguments)
    assert response.status_code == 400
    message = "missing parameter: date_from and date_to, or ticket, or position"
    assert response.json == {
        "ok": False,
        "error": {"code": -2, "message": message},
        "last_error": [-2, message],
    }
    getattr(stub, function_name).assert_not_called()


@pytest.mark.parametrize("function_name", HISTORY_QUERIES)
@pytest.mark.parametrize(
    "arguments",
    [
        {"date_from": 1704067200, "date_to": 1704153600},
        {"date_from": 1704067200, "date_to": 1704153600, "group": "*USD*"},
        {"ticket": 5},
        {"position": 7},
    ],
    ids=["range", "range-group", "ticket", "position"],
)
def test_history_query_in_a_call_form_reaches_the_package(client, stub, function_name, arguments):
    getattr(stub, function_name).return_value = ()
    assert client.post(f"/mt5/{function_name}", json=arguments).status_code == 200
    getattr(stub, function_name).assert_called_once()


@pytest.mark.parametrize("function", LISTING, ids=str)
def test_empty_tuple_is_an_empty_result(client, stub, function):
    getattr(stub, function.name).return_value = ()
    response = client.post(f"/mt5/{function.name}", json=argument_samples(function))
    assert response.json == {"ok": True, "result": [], "last_error": [1, "Success"]}
    stub.last_error.assert_called_once_with()


def test_symbol_select_false_is_a_result(client, stub):
    stub.symbol_select.return_value = False
    stub.last_error.return_value = (-4, "Terminal: Not found")
    response = client.post("/mt5/symbol_select", json={"symbol": "EURUSD", "enable": True})
    assert response.json == {"ok": True, "result": False, "last_error": [-4, "Terminal: Not found"]}


def test_last_error_route_answers_the_servers_last_error(client, stub):
    stub.last_error.return_value = (-1, "Terminal: Call failed")
    assert client.post("/mt5/last_error").json == {
        "ok": True,
        "result": [-1, "Terminal: Call failed"],
        "last_error": [-1, "Terminal: Call failed"],
    }


def test_numpy_scalars_answer_as_plain_numbers(client, stub):
    stub.order_calc_margin.return_value = np.float64(12.5)
    stub.positions_total.return_value = np.int64(3)
    margin = client.post(
        "/mt5/order_calc_margin",
        json={"action": 0, "symbol": "EURUSD", "volume": 0.1, "price": 1.1},
    )
    total = client.post("/mt5/positions_total")
    assert margin.json == {"ok": True, "result": 12.5, "last_error": [1, "Success"]}
    assert total.json == {"ok": True, "result": 3, "last_error": [1, "Success"]}


@pytest.mark.parametrize("function", WITH_REQUIRED, ids=str)
def test_missing_required_parameter_is_refused(client, stub, function):
    required = next(param for param in function.params if param.required)
    arguments = argument_samples(function)
    del arguments[required.name]

    response = client.post(f"/mt5/{function.name}", json=arguments)

    assert response.status_code == 400
    message = f"missing parameter: {required.name}"
    assert response.json == {
        "ok": False,
        "error": {"code": -2, "message": message},
        "last_error": [-2, message],
    }
    getattr(stub, function.name).assert_not_called()


@pytest.mark.parametrize("function", FUNCTIONS, ids=str)
def test_unknown_parameter_is_refused(client, stub, function):
    arguments = argument_samples(function) | {"bogus": 1}

    response = client.post(f"/mt5/{function.name}", json=arguments)

    assert response.status_code == 400
    assert response.json == {
        "ok": False,
        "error": {"code": -2, "message": "unknown parameter: bogus"},
        "last_error": [-2, "unknown parameter: bogus"],
    }
    getattr(stub, function.name).assert_not_called()


def test_body_that_is_not_an_object_is_refused(client, stub):
    response = client.post("/mt5/symbols_get", data="[1, 2]", content_type="application/json")
    assert response.status_code == 400
    assert response.json["error"]["code"] == -2
    stub.symbols_get.assert_not_called()


def test_array_whose_fields_are_not_the_inventorys_is_a_server_error(client, stub):
    foreign = array_sample(mirror.TICKS)
    stub.copy_rates_range.return_value = foreign
    response = client.post(
        "/mt5/copy_rates_range",
        json={"symbol": "EURUSD", "timeframe": 16385, "date_from": 0, "date_to": 1},
    )
    assert response.status_code == 500


def test_absent_optional_parameters_are_not_passed(client, stub):
    stub.positions_get.return_value = ()
    client.post("/mt5/positions_get", json={"symbol": "EURUSD"})
    stub.positions_get.assert_called_once_with(symbol="EURUSD")
