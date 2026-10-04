"""Answers leave the server in true UTC and client times reach the package on the broker's clock,
which runs 10,800 s ahead of UTC under EDT and 7,200 s under EST."""

import calendar
import logging
import re
from datetime import UTC, datetime

import numpy as np
import pytest
from mirror_samples import PACKAGE_TYPES, array_sample, struct_sample

from mt5connector.server.wire import mirror

EPOCH_NAME = re.compile(r"^(time|.*_time|.*time_msc|time_.*|expiration.*|start_time|.*_msc)$")
# Fields named like epochs that hold none: ENUM_ORDER_TYPE_TIME values and SYMBOL_EXPIRATION_MODE
# flags.
NOT_EPOCHS = {
    (mirror.StructName.TRADE_ORDER, "type_time"),
    (mirror.StructName.TRADE_REQUEST, "type_time"),
    (mirror.StructName.SYMBOL_INFO, "expiration_mode"),
}
# The epoch fields and their units, as the MQL5 reference documents them.
EPOCHS = {
    mirror.StructName.TRADE_POSITION: {
        "time": mirror.EpochUnit.SECONDS,
        "time_msc": mirror.EpochUnit.MILLISECONDS,
        "time_update": mirror.EpochUnit.SECONDS,
        "time_update_msc": mirror.EpochUnit.MILLISECONDS,
    },
    mirror.StructName.TRADE_ORDER: {
        "time_setup": mirror.EpochUnit.SECONDS,
        "time_setup_msc": mirror.EpochUnit.MILLISECONDS,
        "time_done": mirror.EpochUnit.SECONDS,
        "time_done_msc": mirror.EpochUnit.MILLISECONDS,
        "time_expiration": mirror.EpochUnit.SECONDS,
    },
    mirror.StructName.TRADE_DEAL: {
        "time": mirror.EpochUnit.SECONDS,
        "time_msc": mirror.EpochUnit.MILLISECONDS,
    },
    mirror.StructName.TRADE_REQUEST: {"expiration": mirror.EpochUnit.SECONDS},
    mirror.StructName.ORDER_SEND_RESULT: {},
    mirror.StructName.ORDER_CHECK_RESULT: {},
    mirror.StructName.TICK: {
        "time": mirror.EpochUnit.SECONDS,
        "time_msc": mirror.EpochUnit.MILLISECONDS,
    },
    mirror.StructName.TERMINAL_INFO: {},
    mirror.StructName.SYMBOL_INFO: {
        "time": mirror.EpochUnit.SECONDS,
        "start_time": mirror.EpochUnit.SECONDS,
        "expiration_time": mirror.EpochUnit.SECONDS,
    },
    mirror.StructName.ACCOUNT_INFO: {},
    mirror.StructName.BOOK_INFO: {},
}
ARRAY_EPOCHS = {
    mirror.ArrayName.RATES: {"time": mirror.EpochUnit.SECONDS},
    mirror.ArrayName.TICKS: {
        "time": mirror.EpochUnit.SECONDS,
        "time_msc": mirror.EpochUnit.MILLISECONDS,
    },
}

BROKER = 1_752_580_800
UTC_EPOCH = 1_752_570_000
REFERENCE = {
    mirror.EpochUnit.SECONDS: (BROKER, UTC_EPOCH),
    mirror.EpochUnit.MILLISECONDS: (BROKER * 1000 + 123, UTC_EPOCH * 1000 + 123),
}


def test_every_epoch_named_field_is_annotated_or_a_known_non_epoch():
    for struct in mirror.STRUCTS.values():
        for field in struct.fields:
            if EPOCH_NAME.match(field):
                assert field in struct.epochs or (struct.name, field) in NOT_EPOCHS, field
    for array in mirror.ARRAYS.values():
        for field in array.names:
            if EPOCH_NAME.match(field):
                assert field in array.epochs, field


def test_the_inventory_annotates_the_documented_epochs():
    assert {name: struct.epochs for name, struct in mirror.STRUCTS.items()} == EPOCHS
    assert {name: array.epochs for name, array in mirror.ARRAYS.items()} == ARRAY_EPOCHS
    for struct_name, field in NOT_EPOCHS:
        assert field not in mirror.STRUCTS[struct_name].epochs


def _answering(name: mirror.StructName) -> tuple[mirror.Function, str | None]:
    """The first function answering the struct, with the field it is nested in when no function
    answers it directly."""
    for function in mirror.FUNCTIONS.values():
        if function.struct is name:
            return function, None
    for function in mirror.FUNCTIONS.values():
        if function.struct is not None:
            for field, nested in mirror.STRUCTS[function.struct].nested.items():
                if nested is name:
                    return function, field
    raise LookupError(f"no function answers {name}")


def _carriers():
    """(function, struct it answers, nested field, field, unit) for every annotated struct epoch."""
    cases = []
    for struct in mirror.STRUCTS.values():
        for field, unit in struct.epochs.items():
            function, nested = _answering(struct.name)
            cases.append((function, function.struct, nested, field, unit))
    return cases


def _case_id(value: object) -> str:
    if isinstance(value, mirror.Function):
        return value.name.value
    else:
        return str(value)


def _zeroed(name: mirror.StructName) -> tuple:
    """A struct sample with every epoch, its nested structs' included, zero."""
    struct = mirror.STRUCTS[name]
    sample = struct_sample(name)
    changes = {field: 0 for field in struct.epochs}
    for field, nested in struct.nested.items():
        changes[field] = _zeroed(nested)
    return sample._replace(**changes)


def _call(client, function):
    """Posts the function with arguments in its first call form; the stub ignores them."""
    arguments = {}
    for param in function.params:
        if param.kind is mirror.ParamKind.DATETIME:
            arguments[param.name] = UTC_EPOCH
        elif param.kind is mirror.ParamKind.REQUEST:
            arguments[param.name] = {"action": 1}
        elif param.required:
            arguments[param.name] = "EURUSD" if param.kind is mirror.ParamKind.STR else 1
    return client.post(f"/mt5/{function.name}", json=arguments)


@pytest.mark.parametrize(
    ("function", "struct", "nested", "field", "unit"),
    _carriers(),
    ids=_case_id,
)
def test_every_annotated_struct_epoch_answers_in_true_utc(
    client, stub, function, struct, nested, field, unit
):
    broker, utc = REFERENCE[unit]
    answer = _zeroed(struct)
    if nested is None:
        answer = answer._replace(**{field: broker})
    else:
        inner = getattr(answer, nested)._replace(**{field: broker})
        answer = answer._replace(**{nested: inner})
    if function.result is mirror.ResultKind.STRUCTS:
        getattr(stub, function.name).return_value = (answer,)
    else:
        getattr(stub, function.name).return_value = answer

    response = _call(client, function)

    assert response.status_code == 200
    if function.result is mirror.ResultKind.STRUCTS:
        result = response.json["result"][0]
    else:
        result = response.json["result"]
    if nested is None:
        epochs = {name: result[name] for name in mirror.STRUCTS[struct].epochs}
    else:
        owner = mirror.STRUCTS[mirror.STRUCTS[struct].nested[nested]]
        epochs = {name: result[nested][name] for name in owner.epochs}
    assert epochs == {name: utc if name == field else 0 for name in epochs}


@pytest.mark.parametrize(
    ("function", "field", "unit"),
    [
        (function, field, unit)
        for array in mirror.ARRAYS.values()
        for field, unit in array.epochs.items()
        for function in mirror.FUNCTIONS.values()
        if function.array is array.name
    ],
    ids=_case_id,
)
def test_every_annotated_array_epoch_answers_in_true_utc(client, stub, function, field, unit):
    broker, utc = REFERENCE[unit]
    array = mirror.ARRAYS[function.array]
    answer = array_sample(array)
    for name in array.epochs:
        answer[name] = 0
    answer[field] = broker
    getattr(stub, function.name).return_value = answer

    rows = _call(client, function).json["result"]

    assert [{name: row[name] for name in array.epochs} for row in rows] == [
        {name: utc if name == field else 0 for name in array.epochs}
    ] * len(answer)


def test_a_bar_stays_open_stamped(client, stub):
    stub.copy_rates_range.return_value = np.array(
        [(BROKER, 1.1, 1.2, 1.0, 1.15, 120, 2, 0)], dtype=list(mirror.RATES.dtype)
    )

    response = client.post(
        "/mt5/copy_rates_range",
        json={"symbol": "EURUSD", "timeframe": 16385, "date_from": 0, "date_to": 1},
    )

    assert response.json["result"] == [
        {
            "time": 1752570000,
            "open": 1.1,
            "high": 1.2,
            "low": 1.0,
            "close": 1.15,
            "tick_volume": 120,
            "spread": 2,
            "real_volume": 0,
        }
    ]


def test_a_tick_converts_its_seconds_and_milliseconds(client, stub):
    stub.symbol_info_tick.return_value = PACKAGE_TYPES[mirror.StructName.TICK](
        time=BROKER,
        bid=1.1,
        ask=1.2,
        last=0.0,
        volume=0,
        time_msc=BROKER * 1000 + 123,
        flags=6,
        volume_real=0.0,
    )

    result = client.post("/mt5/symbol_info_tick", json={"symbol": "EURUSD"}).json["result"]

    assert result == {
        "time": 1752570000,
        "bid": 1.1,
        "ask": 1.2,
        "last": 0.0,
        "volume": 0,
        "time_msc": 1752570000123,
        "flags": 6,
        "volume_real": 0.0,
    }


def test_an_order_without_expiration_keeps_zero(client, stub):
    orders = (
        _zeroed(mirror.StructName.TRADE_ORDER)._replace(time_setup=BROKER, time_expiration=0),
        _zeroed(mirror.StructName.TRADE_ORDER)._replace(time_setup=BROKER, time_expiration=BROKER),
    )
    stub.orders_get.return_value = orders

    result = client.post("/mt5/orders_get", json={}).json["result"]

    assert [(order["time_setup"], order["time_expiration"]) for order in result] == [
        (UTC_EPOCH, 0),
        (UTC_EPOCH, UTC_EPOCH),
    ]


def test_an_order_send_result_converts_its_requests_expiration(client, stub):
    answer = _zeroed(mirror.StructName.ORDER_SEND_RESULT)
    stub.order_send.return_value = answer._replace(
        request=answer.request._replace(expiration=BROKER)
    )

    result = client.post("/mt5/order_send", json={"request": {"action": 1}}).json["result"]

    assert result["request"]["expiration"] == UTC_EPOCH


def test_symbol_info_converts_its_quote_time(client, stub):
    stub.symbol_info.return_value = _zeroed(mirror.StructName.SYMBOL_INFO)._replace(time=BROKER)

    result = client.post("/mt5/symbol_info", json={"symbol": "EURUSD"}).json["result"]

    assert (result["time"], result["start_time"], result["expiration_time"]) == (UTC_EPOCH, 0, 0)


def test_the_repeated_hour_answers_its_first_occurrence_with_one_warning(client, stub, caplog):
    # 2025-11-02 08:30 on the broker's clock is New York's repeated 01:30; its first occurrence is
    # under EDT.
    broker = calendar.timegm((2025, 11, 2, 8, 30, 0))
    ticks = array_sample(mirror.TICKS, rows=3)
    ticks["time"] = [broker, broker + 1, broker + 2]
    ticks["time_msc"] = [broker * 1000, (broker + 1) * 1000, (broker + 2) * 1000]
    stub.copy_ticks_range.return_value = ticks
    caplog.set_level(logging.WARNING, logger="mt5connector.server.encoding")

    response = client.post(
        "/mt5/copy_ticks_range",
        json={"symbol": "EURUSD", "date_from": 0, "date_to": 1, "flags": -1},
    )

    assert [row["time"] for row in response.json["result"]] == [
        broker - 10_800,
        broker + 1 - 10_800,
        broker + 2 - 10_800,
    ]
    assert [record.getMessage() for record in caplog.records] == [
        f"copy_ticks_range: ticks.time {broker} is in the broker's repeated hour, read as its "
        "first occurrence"
    ]


def test_the_skipped_hour_is_a_server_error(client, stub):
    # 2026-03-08 09:30 on the broker's clock is New York's skipped 02:30.
    stub.symbol_info_tick.return_value = _zeroed(mirror.StructName.TICK)._replace(
        time=calendar.timegm((2026, 3, 8, 9, 30, 0))
    )
    response = client.post("/mt5/symbol_info_tick", json={"symbol": "EURUSD"})
    assert response.status_code == 500


def test_an_epoch_that_is_not_an_integer_is_a_server_error(client, stub):
    stub.symbol_info_tick.return_value = _zeroed(mirror.StructName.TICK)._replace(time=1.5)
    response = client.post("/mt5/symbol_info_tick", json={"symbol": "EURUSD"})
    assert response.status_code == 500


def test_a_time_window_reaches_the_package_on_the_brokers_clock(client, stub):
    stub.copy_ticks_from.return_value = array_sample(mirror.TICKS, rows=0)

    client.post(
        "/mt5/copy_ticks_from",
        json={"symbol": "EURUSD", "date_from": UTC_EPOCH, "count": 10, "flags": -1},
    )

    stub.copy_ticks_from.assert_called_once_with(
        "EURUSD", datetime.fromtimestamp(1_752_580_800, UTC), 10, -1
    )
    assert stub.copy_ticks_from.call_args.args[1].timestamp() == 1_752_580_800


def test_a_history_range_converts_each_end_in_its_own_era(client, stub):
    stub.history_orders_get.return_value = ()

    client.post("/mt5/history_orders_get", json={"date_from": 1_736_935_200, "date_to": UTC_EPOCH})

    stub.history_orders_get.assert_called_once_with(
        datetime.fromtimestamp(1_736_942_400, UTC), datetime.fromtimestamp(1_752_580_800, UTC)
    )


def test_a_history_ticket_query_carries_no_time(client, stub):
    stub.history_orders_get.return_value = ()
    client.post("/mt5/history_orders_get", json={"ticket": 5})
    stub.history_orders_get.assert_called_once_with(ticket=5)


@pytest.mark.parametrize(
    ("expiration", "sent"), [(UTC_EPOCH, 1_752_580_800), (0, 0)], ids=["set", "none"]
)
def test_a_requests_expiration_reaches_the_package_on_the_brokers_clock(
    client, stub, expiration, sent
):
    stub.order_send.return_value = _zeroed(mirror.StructName.ORDER_SEND_RESULT)
    request = {"action": 5, "symbol": "EURUSD", "volume": 0.1, "type_time": 2}

    client.post("/mt5/order_send", json={"request": request | {"expiration": expiration}})

    stub.order_send.assert_called_once_with(request | {"expiration": sent})


@pytest.mark.parametrize("value", ["2025-07-15T09:00:00Z", 1_752_570_000.0, True, None])
def test_a_time_window_that_is_not_an_integer_epoch_is_refused(client, stub, value):
    response = client.post(
        "/mt5/history_deals_get", json={"date_from": value, "date_to": UTC_EPOCH}
    )

    assert response.status_code == 400
    message = "not an integer epoch: date_from"
    assert response.json == {
        "ok": False,
        "error": {"code": -2, "message": message},
        "last_error": [-2, message],
    }
    stub.history_deals_get.assert_not_called()


@pytest.mark.parametrize("value", ["tomorrow", 1.5, False])
def test_a_request_expiration_that_is_not_an_integer_epoch_is_refused(client, stub, value):
    response = client.post("/mt5/order_check", json={"request": {"action": 5, "expiration": value}})

    assert response.status_code == 400
    assert response.json["error"] == {
        "code": -2,
        "message": "not an integer epoch: request.expiration",
    }
    stub.order_check.assert_not_called()
