"""The hub's call registry: the calls, who serves each and under which deadline, their arguments and
answers on the wire, and the results a session hands back."""

import numpy as np
import pytest

from mt5connector.wire import binary, calls, mirror
from mt5connector.wire.binary import CodecError, Writer
from mt5connector.wire.history_wire import BARS, FloorOrigin, Series
from mt5connector.wire.push_wire import ChartRole, TransactionType

Call = calls.Call
ArgType = calls.ArgType
_TICKS = np.dtype(list(mirror.TICKS.dtype))
_BARS = np.dtype(list(BARS.dtype))

SPAWNER_CALLS = {
    Call.ACCOUNT_INFO,
    Call.TERMINAL_INFO,
    Call.SYMBOLS_GET,
    Call.SYMBOLS_TOTAL,
    Call.POSITIONS_GET,
    Call.ORDERS_GET,
    Call.HISTORY_ORDERS_GET,
    Call.HISTORY_DEALS_GET,
}
HISTORY_CALLS = {
    Call.COPY_RATES_RANGE,
    Call.COPY_RATES_FROM,
    Call.COPY_TICKS_RANGE,
    Call.COPY_TICKS_FROM,
}
# The tick history, which the history EA reads in its own thread.
TICK_CALLS = {Call.COPY_TICKS_RANGE, Call.COPY_TICKS_FROM}
PUBLISHER_CALLS = {
    Call.SYMBOL_INFO,
    Call.SYMBOL_INFO_TICK,
    Call.ORDER_SEND,
    Call.OUTCOMES,
} | (HISTORY_CALLS - TICK_CALLS)
HUB_CALLS = {Call.RANGES, Call.COMMISSION_SCHEDULE, Call.TRADING_SESSIONS}


def zeroed(layout: binary.Layout, **values) -> dict:
    """A record of the layout with every member zero, as MQL5 initialises a struct, but `values`."""
    record = {}
    for member in layout.members:
        if member.declared is mirror.MqlType.STRING:
            record[member.name] = ""
        elif member.declared is mirror.MqlType.DOUBLE:
            record[member.name] = 0.0
        elif member.declared is mirror.MqlType.BOOL:
            record[member.name] = False
        elif member.declared is not None:
            record[member.name] = 0
    return record | values


def ticks(*milliseconds):
    rows = [(ms // 1000, 1.085, 1.08512, 0.0, 0, ms, 6, 0.0) for ms in milliseconds]
    return np.array(rows, dtype=_TICKS)


# ── The registry ──────────────────────────────────────────────────────────────


def test_the_registry_serves_the_nineteen_calls():
    assert {call.name.lower() for call in calls.CALLS} == {
        "account_info",
        "terminal_info",
        "symbols_get",
        "symbols_total",
        "symbol_info",
        "symbol_info_tick",
        "positions_get",
        "orders_get",
        "history_orders_get",
        "history_deals_get",
        "order_send",
        "copy_rates_range",
        "copy_rates_from",
        "copy_ticks_range",
        "copy_ticks_from",
        "ranges",
        "outcomes",
        "commission_schedule",
        "trading_sessions",
    }


def test_each_call_is_served_by_the_role_the_design_gives_it():
    served = {}
    for call, row in calls.CALLS.items():
        served.setdefault(row.role, set()).add(call)

    assert served == {
        ChartRole.SPAWNER: SPAWNER_CALLS,
        ChartRole.PUBLISHER: PUBLISHER_CALLS,
        ChartRole.HISTORY: TICK_CALLS,
        None: HUB_CALLS,
    }


def test_a_command_waits_under_the_command_deadline_a_copy_under_its_historys_the_rest_reads():
    waits = {call: row.wait for call, row in calls.CALLS.items()}

    assert {call for call, wait in waits.items() if wait is calls.Wait.COMMAND} == {Call.ORDER_SEND}
    assert {call for call, wait in waits.items() if wait is calls.Wait.HISTORY} == (
        HISTORY_CALLS - TICK_CALLS
    )
    assert {call for call, wait in waits.items() if wait is calls.Wait.TICKS} == TICK_CALLS
    assert all(
        wait is calls.Wait.READ
        for call, wait in waits.items()
        if call not in HISTORY_CALLS | {Call.ORDER_SEND}
    )


@pytest.mark.parametrize(
    ("call", "arguments"),
    [
        (
            Call.COPY_RATES_RANGE,
            [
                ("symbol", ArgType.STRING, True),
                ("timeframe", ArgType.INTEGER, True),
                ("date_from", ArgType.EPOCH, True),
                ("date_to", ArgType.EPOCH, True),
            ],
        ),
        (
            Call.COPY_TICKS_FROM,
            [
                ("symbol", ArgType.STRING, True),
                ("date_from", ArgType.EPOCH, True),
                ("count", ArgType.INTEGER, True),
                ("flags", ArgType.INTEGER, True),
            ],
        ),
        (
            Call.HISTORY_DEALS_GET,
            [
                ("date_from", ArgType.EPOCH, False),
                ("date_to", ArgType.EPOCH, False),
                ("group", ArgType.STRING, False),
                ("ticket", ArgType.INTEGER, False),
                ("position", ArgType.INTEGER, False),
            ],
        ),
        (Call.ORDER_SEND, [("request", ArgType.TRADE_REQUEST, True)]),
        (
            Call.OUTCOMES,
            [("symbol", ArgType.STRING, True), ("request_ids", ArgType.REQUEST_IDS, True)],
        ),
        (Call.RANGES, [("symbol", ArgType.STRING, True)]),
        (Call.ACCOUNT_INFO, []),
    ],
    ids=lambda value: value.name.lower() if isinstance(value, Call) else "",
)
def test_a_calls_arguments_travel_by_their_wire_types(call, arguments):
    assert [
        (argument.name, argument.type, argument.required)
        for argument in calls.CALLS[call].arguments
    ] == arguments


@pytest.mark.parametrize(
    ("call", "shape", "layout"),
    [
        (Call.ACCOUNT_INFO, calls.Shape.STRUCT, "AccountInfo"),
        (Call.TERMINAL_INFO, calls.Shape.STRUCT, "TerminalInfo"),
        (Call.SYMBOL_INFO, calls.Shape.STRUCT, "SymbolInfo"),
        (Call.SYMBOLS_GET, calls.Shape.STRUCTS, "SymbolListing"),
        (Call.POSITIONS_GET, calls.Shape.STRUCTS, "TradePosition"),
        (Call.HISTORY_ORDERS_GET, calls.Shape.STRUCTS, "TradeOrder"),
        (Call.HISTORY_DEALS_GET, calls.Shape.STRUCTS, "TradeDeal"),
        (Call.ORDER_SEND, calls.Shape.STRUCT, "MqlTradeResult"),
        (Call.OUTCOMES, calls.Shape.TRANSACTIONS, None),
        (Call.SYMBOL_INFO_TICK, calls.Shape.TICK, None),
        (Call.COPY_TICKS_RANGE, calls.Shape.TICKS, None),
        (Call.COPY_RATES_FROM, calls.Shape.RATES, None),
        (Call.SYMBOLS_TOTAL, calls.Shape.INTEGER, None),
        (Call.RANGES, calls.Shape.RANGES, None),
        (Call.COMMISSION_SCHEDULE, calls.Shape.SCHEDULE, None),
        (Call.TRADING_SESSIONS, calls.Shape.SESSIONS, None),
    ],
    ids=lambda value: value.name.lower() if isinstance(value, Call) else "",
)
def test_a_calls_answer_has_the_shape_of_its_result(call, shape, layout):
    row = calls.CALLS[call]

    assert (row.shape, None if row.layout is None else row.layout.name) == (shape, layout)


@pytest.mark.parametrize(
    ("function", "kind"),
    [
        (mirror.FunctionName.MARKET_BOOK_ADD, "bool"),
        (mirror.FunctionName.VERSION, "tuple"),
        (mirror.FunctionName.SHUTDOWN, "none"),
    ],
    ids=lambda value: value if isinstance(value, str) else "",
)
def test_a_package_function_of_a_result_kind_the_registry_serves_no_shape_for_is_refused(
    function, kind
):
    with pytest.raises(ValueError, match=f"{function}: .*{kind}"):
        calls._package(Call.ACCOUNT_INFO, function, ChartRole.SPAWNER, calls.Wait.READ)


def test_a_terminal_info_drops_the_member_no_mql5_property_backs_and_a_result_its_request():
    terminal = calls.CALLS[Call.TERMINAL_INFO].layout
    result = calls.CALLS[Call.ORDER_SEND].layout

    # Without tradeapi_disabled, which no MQL5 property backs, and community_balance, which the
    # terminal refuses without an MQL5 community login.
    assert terminal.fields == (
        "community_account",
        "community_connection",
        "connected",
        "dlls_allowed",
        "trade_allowed",
        "email_enabled",
        "ftp_enabled",
        "notifications_enabled",
        "mqid",
        "build",
        "maxbars",
        "codepage",
        "ping_last",
        "retransmission",
        "company",
        "name",
        "language",
        "path",
        "data_path",
        "commondata_path",
    )
    assert result.fields == (
        "retcode",
        "deal",
        "order",
        "volume",
        "price",
        "bid",
        "ask",
        "comment",
        "request_id",
        "retcode_external",
    )


@pytest.mark.parametrize(
    ("call", "arguments", "symbol"),
    [
        (Call.SYMBOL_INFO, {"symbol": "EURUSD.a"}, "EURUSD.a"),
        (
            Call.COPY_TICKS_FROM,
            {"symbol": "XAUUSD", "date_from": 1, "count": 1, "flags": 1},
            "XAUUSD",
        ),
        (Call.ORDER_SEND, {"request": calls.trade_request({"symbol": "US500#"})}, "US500#"),
        (Call.POSITIONS_GET, {"symbol": "EURUSD"}, None),
        (Call.RANGES, {"symbol": "GBPUSD"}, "GBPUSD"),
    ],
    ids=["read", "history", "command", "spawner", "hub"],
)
def test_a_request_is_routed_by_its_calls_symbol(call, arguments, symbol):
    assert calls.routed_symbol(calls.CALLS[call], arguments) == symbol


# ── Arguments ─────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("call", "arguments"),
    [
        (Call.ACCOUNT_INFO, {}),
        (Call.SYMBOLS_GET, {"group": "*USD*"}),
        (Call.HISTORY_DEALS_GET, {"date_from": 1_752_570_000, "date_to": 1_752_573_600}),
        (Call.HISTORY_ORDERS_GET, {"position": 9001}),
        (Call.POSITIONS_GET, {"ticket": 2**62}),
        (
            Call.COPY_RATES_RANGE,
            {"symbol": "EURUSD", "timeframe": 16385, "date_from": 0, "date_to": 1_752_570_000},
        ),
        (
            Call.ORDER_SEND,
            {"request": calls.trade_request({"symbol": "EURUSD", "volume": 0.1, "magic": 2**62})},
        ),
        (Call.OUTCOMES, {"symbol": "EURUSD", "request_ids": (1, 2, 4_294_967_295)}),
        (Call.COMMISSION_SCHEDULE, {"symbol": "ünïcødé"}),
    ],
    ids=lambda value: value.name.lower() if isinstance(value, Call) else "",
)
def test_a_calls_arguments_round_trip(call, arguments):
    row = calls.CALLS[call]
    count, body = calls.encode_arguments(row, arguments)

    assert calls.decode_arguments(row, binary.Request(1, call, count, body)) == arguments


@pytest.mark.parametrize(
    ("call", "arguments", "message"),
    [
        (Call.SYMBOL_INFO, {}, "symbol_info: missing argument: symbol"),
        (Call.SYMBOL_INFO, {"symbol": "X", "group": "*"}, "symbol_info: unknown argument: group"),
        (Call.HISTORY_DEALS_GET, {"date_from": 1}, "history_deals_get: the arguments name none"),
        (Call.SYMBOL_INFO, {"symbol": 7}, "symbol_info.symbol: 7 is not a string"),
        (Call.POSITIONS_GET, {"ticket": True}, "positions_get.ticket: True is not an integer"),
        (Call.ORDER_SEND, {"request": {"symbol": "X"}}, "order_send.request: not a whole"),
        (Call.OUTCOMES, {"symbol": "X", "request_ids": (-1,)}, "outcomes.request_ids: \\(-1,\\)"),
    ],
    ids=[
        "missing",
        "unknown",
        "no-form",
        "string",
        "integer",
        "partial-request",
        "negative-id",
    ],
)
def test_arguments_a_call_does_not_take_are_refused_before_they_are_sent(call, arguments, message):
    with pytest.raises(ValueError, match=message):
        calls.encode_arguments(calls.CALLS[call], arguments)


def test_arguments_out_of_their_calls_order_are_refused_on_arrival():
    row = calls.CALLS[Call.COPY_TICKS_FROM]
    writer = Writer()
    writer.u8(1)
    writer.i64(1)
    writer.u8(0)
    writer.string("EURUSD")

    with pytest.raises(CodecError, match="index 0 is out of order"):
        calls.decode_arguments(row, binary.Request(1, Call.COPY_TICKS_FROM, 2, writer.payload()))


def test_a_trade_request_is_whole_with_every_member_not_given_zero():
    request = calls.trade_request({"symbol": "EURUSD", "volume": 0.1, "action": 1})

    assert request == {
        "action": 1,
        "magic": 0,
        "order": 0,
        "symbol": "EURUSD",
        "volume": 0.1,
        "price": 0.0,
        "stoplimit": 0.0,
        "sl": 0.0,
        "tp": 0.0,
        "deviation": 0,
        "type": 0,
        "type_filling": 0,
        "type_time": 0,
        "expiration": 0,
        "comment": "",
        "position": 0,
        "position_by": 0,
    }
    with pytest.raises(ValueError, match="MqlTradeRequest: unknown field: lots"):
        calls.trade_request({"lots": 1})


# ── Results ───────────────────────────────────────────────────────────────────


def _answer(call, value, *, part=0, last=True):
    count, body = calls.write_result(calls.CALLS[call], value)
    return binary.Answer(1, part, last, 0, count, body)


def test_a_struct_result_round_trips_and_comes_back_as_its_record_type():
    row = calls.CALLS[Call.ACCOUNT_INFO]
    account = zeroed(
        row.layout,
        login=12345678,
        trade_allowed=True,
        balance=10_000.0,
        name="Test",
        currency="USD",
    )

    value = calls.read_hub_result(row, _answer(Call.ACCOUNT_INFO, account))
    record = calls.typed(row, calls.joined(row, [value]))

    assert value == account
    assert type(record).__name__ == "AccountInfo"
    assert (record.trade_allowed, record.trade_expert, record.currency) == (True, False, "USD")


def test_struct_rows_in_parts_join_in_order():
    row = calls.CALLS[Call.POSITIONS_GET]
    first = zeroed(row.layout)
    parts = [
        calls.read_hub_result(
            row, _answer(Call.POSITIONS_GET, [first | {"ticket": 1}], last=False)
        ),
        calls.read_hub_result(row, _answer(Call.POSITIONS_GET, [first | {"ticket": 2}], part=1)),
    ]

    records = calls.typed(row, calls.joined(row, parts))

    assert [record.ticket for record in records] == [1, 2]


def outcome(
    request_id: int, retcode=mirror.TRADE_RETCODE_INVALID_EXPIRATION
) -> binary.TradeTransaction:
    """A REQUEST transaction as a publisher's outcome log holds it."""
    return binary.TradeTransaction(
        zeroed(binary.TRADE_TRANSACTION, type=TransactionType.REQUEST),
        zeroed(
            binary.TRADE_REQUEST,
            action=mirror.TRADE_ACTION_PENDING,
            symbol="EURUSD",
            volume=0.01,
            expiration=60,
        ),
        zeroed(binary.TRADE_RESULT, retcode=retcode, request_id=request_id, comment="expiration"),
    )


def test_outcomes_travel_as_the_transactions_they_hold_and_join_across_parts():
    row = calls.CALLS[Call.OUTCOMES]
    parts = [
        calls.read_hub_result(row, _answer(Call.OUTCOMES, [outcome(1)], last=False)),
        calls.read_hub_result(row, _answer(Call.OUTCOMES, [outcome(2), outcome(3)], part=1)),
        calls.read_hub_result(row, _answer(Call.OUTCOMES, [], part=2)),
    ]

    held = calls.typed(row, calls.joined(row, parts))

    assert held == (outcome(1), outcome(2), outcome(3))


def test_an_eas_outcomes_are_read_as_the_three_structs_of_each_transaction_back_to_back():
    row = calls.CALLS[Call.OUTCOMES]
    writer = Writer()
    for request_id in (7, 9):
        binary.write_record(writer, binary.TRADE_TRANSACTION, outcome(request_id).transaction)
        binary.write_record(writer, binary.TRADE_REQUEST, outcome(request_id).request)
        binary.write_record(writer, binary.TRADE_RESULT, outcome(request_id).result)

    held, fact = calls.read_ea_result(row, binary.Answer(1, 0, True, 0, 2, writer.payload()))

    assert (held, fact) == ([outcome(7), outcome(9)], None)


def test_tick_rows_in_parts_join_into_one_array():
    row = calls.CALLS[Call.COPY_TICKS_RANGE]
    parts = [
        calls.read_hub_result(row, _answer(Call.COPY_TICKS_RANGE, ticks(1_000, 2_000))),
        calls.read_hub_result(row, _answer(Call.COPY_TICKS_RANGE, ticks(3_000))),
    ]

    joined = calls.joined(row, parts)

    assert joined.dtype == _TICKS
    assert joined["time_msc"].tolist() == [1_000, 2_000, 3_000]


def test_bars_reach_a_consumer_with_their_close():
    row = calls.CALLS[Call.COPY_RATES_RANGE]
    bars = np.array([(60, 1.0, 1.1, 0.9, 1.05, 3, 1, 0, 120)], dtype=_BARS)

    value = calls.read_hub_result(row, _answer(Call.COPY_RATES_RANGE, bars))

    assert value.dtype == _BARS
    assert value[["time", "time_close"]].tolist() == [(60, 120)]


@pytest.mark.parametrize(
    ("call", "value"),
    [
        (Call.SYMBOLS_TOTAL, 1_234),
        (
            Call.RANGES,
            calls.Ranges(
                100_000,
                {
                    Series.M1: calls.SeriesRange(1_700_000_000, 1_752_570_000, 2),
                    Series.TICKS: calls.SeriesRange(1_600_000_000, 1_752_570_001, 1),
                },
            ),
        ),
        (Call.RANGES, calls.Ranges(100_000_000, {})),
        (Call.COMMISSION_SCHEDULE, {"ret": 0, "last_error": 0, "rules": []}),
        (
            Call.TRADING_SESSIONS,
            {"quote": [[] for _ in range(7)], "trade": [[[0, 0]]] * 7, "last_error": 0},
        ),
    ],
    ids=["integer", "ranges", "no-floors", "schedule", "sessions"],
)
def test_a_single_record_result_round_trips(call, value):
    row = calls.CALLS[call]

    assert calls.joined(row, [calls.read_hub_result(row, _answer(call, value))]) == value


def test_a_single_record_result_in_two_parts_is_refused():
    row = calls.CALLS[Call.SYMBOLS_TOTAL]

    with pytest.raises(CodecError, match="symbols_total: a integer answer in 2 parts"):
        calls.joined(row, [1, 2])


def test_a_struct_result_of_two_records_is_refused():
    row = calls.CALLS[Call.SYMBOLS_TOTAL]
    count, body = calls.write_result(row, 1)

    with pytest.raises(CodecError, match="symbols_total: 2 records where a integer is one"):
        calls.read_hub_result(row, binary.Answer(1, 0, True, 0, 2, body + body))


# ── An EA's history answer ────────────────────────────────────────────────────


def _ea_part(rows, facts: bytes | None, *, last=True):
    body = rows.tobytes()
    if facts is not None:
        body += facts
    return binary.Answer(1, 0, last, 0, len(rows), body)


def _floor(value: int, origin: FloorOrigin) -> bytes:
    writer = Writer()
    writer.u8(1)
    writer.i64(value)
    writer.string(origin)
    return writer.payload()


def test_the_last_part_of_a_history_read_carries_its_floor_fact():
    row = calls.CALLS[Call.COPY_TICKS_RANGE]

    rows, fact = calls.read_ea_result(
        row, _ea_part(ticks(5_000), _floor(1_000_000, FloorOrigin.COARSE))
    )

    assert rows["time_msc"].tolist() == [5_000]
    assert fact == calls.FloorFact(1_000_000, FloorOrigin.COARSE)


def test_a_history_read_that_measured_no_floor_says_so():
    row = calls.CALLS[Call.COPY_TICKS_RANGE]

    assert calls.read_ea_result(row, _ea_part(ticks(), b"\x00"))[1] is None


def test_a_part_before_the_last_carries_no_floor_fact():
    row = calls.CALLS[Call.COPY_TICKS_RANGE]

    rows, fact = calls.read_ea_result(row, _ea_part(ticks(5_000), None, last=False))
    assert (len(rows), fact) == (1, None)
    with pytest.raises(CodecError, match="bytes past the end"):
        calls.read_ea_result(row, _ea_part(ticks(5_000), b"\x00", last=False))


def test_a_history_answer_with_two_floors_is_refused():
    row = calls.CALLS[Call.COPY_TICKS_RANGE]

    with pytest.raises(CodecError, match="2 floors on one answer"):
        calls.read_ea_result(row, _ea_part(ticks(), b"\x02"))


def test_an_eas_single_record_result_in_a_part_before_its_last_is_refused():
    row = calls.CALLS[Call.SYMBOLS_TOTAL]
    count, body = calls.write_result(row, 1)

    with pytest.raises(
        CodecError, match="symbols_total: a integer answer in a part before its last"
    ):
        calls.read_ea_result(row, binary.Answer(1, 0, False, 0, count, body))
