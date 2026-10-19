"""The EA's half of the hub protocol as the generator writes it in MQL5, against the inventory it
is written from."""

import re
import subprocess
import sys
from enum import StrEnum

import pytest

from mt5connector.server import mql5gen
from mt5connector.server.wire import binary, calls, mirror
from mt5connector.server.wire.history_wire import BAR_PERIOD_S, BAR_TIMEFRAME
from mt5connector.server.wire.push_wire import ChartRole

Call = calls.Call


class Failure(StrEnum):
    """How a property read tells its failure: the checked overload's answer, or, for a family with
    none, the error the read leaves."""

    ANSWERED = "answered"
    LEFT = "left"


# Each record an EA writes: its layout's name, and how MQL5 reads a member of it — a family's
# property functions with the arguments ahead of the property and how a failed read tells, or a
# struct variable's fields.
WRITERS = {
    "WriteAccountInfo": ("AccountInfo", "AccountInfo", "", Failure.LEFT),
    "WriteTerminalInfo": ("TerminalInfo", "TerminalInfo", "", Failure.LEFT),
    "WriteSymbolInfo": ("SymbolInfo", "SymbolInfo", "symbol, ", Failure.ANSWERED),
    "WriteSymbolListing": ("SymbolListing", "SymbolInfo", "symbol, ", Failure.ANSWERED),
    "WritePosition": ("TradePosition", "PositionGet", "", Failure.ANSWERED),
    "WriteOrder": ("TradeOrder", "OrderGet", "", Failure.ANSWERED),
    "WriteHistoryOrder": ("TradeOrder", "HistoryOrderGet", "ticket, ", Failure.ANSWERED),
    "WriteDeal": ("TradeDeal", "HistoryDealGet", "ticket, ", Failure.ANSWERED),
    "WriteTradeRequest": ("MqlTradeRequest", "request", None, None),
    "WriteTradeResult": ("MqlTradeResult", "result", None, None),
    "WriteTradeTransaction": ("MqlTradeTransaction", "trans", None, None),
}
_LAYOUTS = {
    layout.name: layout
    for layout in (
        *(row.layout for row in calls.CALLS.values() if row.layout is not None),
        binary.TRADE_REQUEST,
        binary.TRADE_TRANSACTION,
    )
}
_FAMILY = {
    mirror.Accessor.INTEGER: "Integer",
    mirror.Accessor.DOUBLE: "Double",
    mirror.Accessor.STRING: "String",
}
# The variable a property writer reads each accessor family's members into, and its MQL5 type.
_VARIABLES = {
    mirror.Accessor.INTEGER: ("integer", "long"),
    mirror.Accessor.DOUBLE: ("real", "double"),
    mirror.Accessor.STRING: ("text", "string"),
}
_FUNCTION = re.compile(r"^(\w+) (\w+)\(([^)]*)\)\n\{\n(.*?)^\}", re.MULTILINE | re.DOTALL)
_DECLARE = re.compile(r"(long|double|string) (\w+) = (?:0|0\.0|\"\");")
_WRITE = re.compile(r"w\.(I64|I64Bool|F64|String)\((?:\(long\))?(?:(\w+)\.(\w+)|(\w+))\);")
_ANSWERED = re.compile(r"if \((\w+?)(Integer|Double|String)\((\w+, )?(\w+), (\w+)\)\)")
_LEFT = re.compile(r"(\w+) = (\w+?)(Integer|Double|String)\((\w+, )?(\w+)\);")
_FAILED = re.compile(r'HubPropertyFailed\(failures, "(\w+)"\);')
_READ = re.compile(r"request\.(\w+) = (?:\((\w+)\))?r\.(I64|F64|String)\(\);")


@pytest.fixture(scope="module")
def text() -> str:
    return mql5gen.render()


def functions(text: str) -> dict[str, tuple[str, str, str]]:
    """Each generated function by its name: its return type, its parameters and its body."""
    return {
        name: (returns, parameters, body)
        for returns, name, parameters, body in _FUNCTION.findall(text)
    }


def enum(text: str, name: str) -> dict[str, int]:
    body = re.search(rf"^enum {name}\n\{{\n(.*?)\n\}};", text, re.MULTILINE | re.DOTALL).group(1)
    return {member: int(value) for member, value in re.findall(r"(\w+) = (\d+)", body)}


def defines(text: str) -> dict[str, str]:
    return dict(re.findall(r"^#define (\w+) (.+)$", text, re.MULTILINE))


def wire_type(member: mirror.Member) -> str:
    """The writer method a member travels by: a bool as 0 or 1 in an int64."""
    kind = mirror.accessor(member.declared)
    if kind is mirror.Accessor.STRING:
        return "String"
    elif kind is mirror.Accessor.DOUBLE:
        return "F64"
    elif member.declared is mirror.MqlType.BOOL:
        return "I64Bool"
    else:
        return "I64"


def written(body: str) -> tuple[dict[str, str], list[tuple]]:
    """A writer's declared variables by name with their types, and each write of its body: the key
    it writes first; for a property read — written when it succeeds, recorded as failed and passed
    over when it does not — the writer method, the family, accessor, leading arguments and property
    it reads, how a failed read tells, and the variable it reads into; or the struct variable and
    field it writes."""
    lines = [line.strip() for line in body.splitlines() if line.strip()]
    declared = {}
    writes = []
    at = 0
    while at < len(lines) and _DECLARE.fullmatch(lines[at]):
        kind, name = _DECLARE.fullmatch(lines[at]).groups()
        declared[name] = kind
        at += 1
    while at < len(lines) and lines[at] != "return failures.count == 0;":
        if lines[at] == "w.String(symbol);":
            writes.append(("key", "symbol"))
            at += 1
        elif lines[at] == "ResetLastError();":
            # The read's own error, whichever way the family tells it.
            answered = _ANSWERED.fullmatch(lines[at + 1])
            if answered is not None:
                family, accessor, leading, prop, variable = answered.groups()
                how = Failure.ANSWERED
                at += 2
            else:
                left = _LEFT.fullmatch(lines[at + 1])
                assert left is not None and lines[at + 2] == "if (GetLastError() == 0)", lines[at:]
                variable, family, accessor, leading, prop = left.groups()
                how = Failure.LEFT
                at += 3
            write = _WRITE.fullmatch(lines[at])
            failed = _FAILED.fullmatch(lines[at + 2])
            assert write is not None and write.group(4) == variable, lines[at]
            assert lines[at + 1] == "else", lines[at + 1]
            assert failed is not None and failed.group(1) == prop, lines[at + 2]
            writes.append((write.group(1), family, accessor, leading or "", prop, how, variable))
            at += 3
        else:
            write = _WRITE.fullmatch(lines[at])
            assert write is not None, lines[at]
            method, struct, field, _ = write.groups()
            writes.append((method, struct, field))
            at += 1
    return declared, writes


def expected_writes(
    layout: binary.Layout, family: str, leading: str | None, how: Failure | None
) -> list[tuple]:
    """The writes of a layout's record: its key first, the symbol it is read for, then every typed
    member in order, a property read into the variable of its accessor family."""
    writes = []
    if layout.key is not None:
        writes.append(("key", "symbol"))
    for member in layout.members:
        if member.declared is not None:
            kind = mirror.accessor(member.declared)
            if leading is None:
                writes.append((wire_type(member), family, member.property))
            else:
                variable, _ = _VARIABLES[kind]
                writes.append(
                    (
                        wire_type(member),
                        family,
                        _FAMILY[kind],
                        leading,
                        member.property,
                        how,
                        variable,
                    )
                )
    return writes


def test_every_struct_an_ea_sends_has_its_writer(text):
    served = {
        row.layout.name
        for row in calls.CALLS.values()
        if row.role is not None and row.layout is not None
    }
    # A transaction, streamed or answered as an outcome, is the three structs back to back.
    served |= {binary.TRADE_TRANSACTION.name, binary.TRADE_REQUEST.name, binary.TRADE_RESULT.name}

    generated = functions(text)

    assert set(WRITERS) <= set(generated)
    assert served <= {layout for layout, _, _, _ in WRITERS.values()}


@pytest.mark.parametrize("name", WRITERS)
def test_a_writer_reads_each_typed_member_once_in_order_by_its_property(text, name):
    layout_name, family, leading, how = WRITERS[name]
    _, _, body = functions(text)[name]

    _, writes = written(body)

    assert writes == expected_writes(_LAYOUTS[layout_name], family, leading, how)


@pytest.mark.parametrize(
    "name", [name for name, writer in WRITERS.items() if writer[3] is not None]
)
def test_a_property_writer_reads_past_a_failed_read_and_answers_whether_every_one_succeeded(
    text, name
):
    returns, parameters, body = functions(text)[name]

    declared, writes = written(body)

    assert returns == "bool"
    assert parameters.startswith("BinaryWriter &w")
    assert parameters.endswith("HubPropertyFailures &failures")
    assert body.rstrip().endswith("return failures.count == 0;")
    # Each variable declared is read into, as its family's type.
    assert declared == {
        variable: kind
        for variable, kind in _VARIABLES.values()
        if variable in {write[-1] for write in writes if len(write) == 7}
    }


@pytest.mark.parametrize("name", [name for name, writer in WRITERS.items() if writer[3] is None])
def test_a_struct_writer_reads_fields_that_cannot_fail(text, name):
    layout_name, variable, _, _ = WRITERS[name]
    returns, parameters, _ = functions(text)[name]

    assert returns == "void"
    assert parameters == f"BinaryWriter &w, const {layout_name} &{variable}"


def test_every_typed_member_of_a_struct_an_ea_sends_names_its_mql5_property():
    for layout in _LAYOUTS.values():
        for member in layout.members:
            assert (member.declared is None) == (member.property is None), member


def test_a_symbols_greeks_are_read_by_their_price_properties():
    symbol = {member.name: member.property for member in _LAYOUTS["SymbolInfo"].members}
    greeks = ("delta", "theta", "gamma", "vega", "rho", "omega")

    assert [symbol[f"price_greeks_{name}"] for name in greeks] == [
        f"SYMBOL_PRICE_{name.upper()}" for name in greeks
    ]


def test_the_trade_request_is_read_member_by_member_as_its_mql5_type(text):
    _, parameters, body = functions(text)["ReadTradeRequest"]
    expected = []
    for member in binary.TRADE_REQUEST.members:
        kind = mirror.accessor(member.declared)
        if kind is mirror.Accessor.STRING:
            expected.append((member.property, "", "String"))
        elif kind is mirror.Accessor.DOUBLE:
            expected.append((member.property, "", "F64"))
        else:
            expected.append((member.property, member.declared.value, "I64"))

    assert parameters == "BinaryReader &r, MqlTradeRequest &request"
    assert _READ.findall(body) == expected
    assert len(body.strip().splitlines()) == len(expected)


def test_the_protocols_codes_are_the_wire_packages(text):
    values = defines(text)

    assert enum(text, "ENUM_HUB_FRAME") == {
        f"HUB_FRAME_{kind.name}": kind.value for kind in binary.FrameKind
    }
    assert enum(text, "ENUM_HUB_ERROR") == {
        f"HUB_ERROR_{kind.name}": kind.value for kind in binary.EA_ERRORS
    }
    assert enum(text, "ENUM_HUB_CALL") == {f"HUB_CALL_{call.name}": call.value for call in Call}
    assert (values["HUB_VERSION"], values["HUB_HEADER_SIZE"]) == ("2", "20")
    assert (values["HUB_FLAG_OK"], values["HUB_FLAG_LAST"]) == ("1", "2")
    assert (values["HUB_TICK_SIZE"], values["HUB_RATES_SIZE"]) == ("60", "60")
    assert values["HUB_PART_BYTES"] == "1000000"
    assert values["HUB_ROLE_EA"] == '"ea"'


def test_the_history_constants_are_the_protocols():
    values = defines(mql5gen._constants())

    assert set(values) == {
        "HUB_VERSION",
        "HUB_HEADER_SIZE",
        "HUB_FLAG_OK",
        "HUB_FLAG_LAST",
        "HUB_TICK_SIZE",
        "HUB_RATES_SIZE",
        "HUB_PART_BYTES",
        "HUB_ROLE_EA",
        "HUB_HISTORY_BUSY_S",
        "HUB_RETRY_AFTER_MS",
        "HUB_SPAN_MARGIN",
        "HUB_TICK_SERIES",
        "HUB_TICK_TIMEFRAME",
        "HUB_PROPERTY_FAILURES_MAX",
    }
    assert values["HUB_HISTORY_BUSY_S"] == "900"
    assert values["HUB_RETRY_AFTER_MS"] == "5000"
    assert values["HUB_SPAN_MARGIN"] == "11"
    assert values["HUB_TICK_SERIES"] == '"ticks"'
    assert values["HUB_TICK_TIMEFRAME"] == "0"
    assert values["HUB_PROPERTY_FAILURES_MAX"] == "128"


def test_the_generated_enums_are_the_protocols_closed_sets(text):
    assert set(re.findall(r"^enum (\w+)$", text, re.MULTILINE)) == {
        "ENUM_HUB_FRAME",
        "ENUM_HUB_ERROR",
        "ENUM_HUB_CALL",
        "ENUM_CHART_ROLE",
        "ENUM_HUB_ARGUMENT",
    }


def test_each_bar_series_is_read_by_its_timeframe_with_its_name_and_period(text):
    _, parameters, body = functions(text)["HubSeries"]
    cases = re.findall(r'case (\d+):\s+name = "(\w+)";\s+period = (\d+);', body)

    assert parameters == "const int timeframe, string &name, int &period"
    assert {int(timeframe): (name, int(period)) for timeframe, name, period in cases} == {
        timeframe: (series.value, BAR_PERIOD_S[series])
        for series, timeframe in BAR_TIMEFRAME.items()
    }
    assert len(cases) == len(BAR_TIMEFRAME)


def test_the_series_table_lists_every_bar_series_timeframe_in_order(text):
    table = re.search(r"^const int HUB_SERIES\[\] = \{([^}]*)\};$", text, re.MULTILINE)

    assert [int(value) for value in table.group(1).split(",")] == list(BAR_TIMEFRAME.values())


def test_the_error_kinds_an_ea_defers_carry_their_retry_delay(text):
    _, parameters, body = functions(text)["HubDefers"]

    assert parameters == "const ENUM_HUB_ERROR kind"
    assert set(re.findall(r"HUB_ERROR_(\w+)", body)) == {
        kind.name for kind in binary.EA_ERRORS & binary.DEFERRING
    }


def test_the_chart_roles_are_numbered_as_the_templates_set_them_and_named_as_the_hub_reads_them(
    text,
):
    named = dict(re.findall(r'case (ROLE_\w+):\s+name = "(\w+)";', text))
    read = dict(re.findall(r'name == "(\w+)"\)\s+role = (\w+);', text))

    # image/install-ea.sh writes these values into the three roles' templates.
    assert enum(text, "ENUM_CHART_ROLE") == {
        "ROLE_SPAWNER": 0,
        "ROLE_PUBLISHER": 1,
        "ROLE_HISTORY": 2,
    }
    assert named == {f"ROLE_{role.name}": role.value for role in ChartRole}
    assert read == {role.value: f"ROLE_{role.name}" for role in ChartRole}


def test_each_role_serves_the_calls_the_registry_gives_it(text):
    _, _, body = functions(text)["HubServes"]
    segments = re.split(r"case (ROLE_\w+):", body)[1:]
    serves = {
        role: set(re.findall(r"HUB_CALL_(\w+)", segment))
        for role, segment in zip(segments[::2], segments[1::2], strict=True)
    }

    assert serves == {
        f"ROLE_{role.name}": {row.call.name for row in calls.CALLS.values() if row.role is role}
        for role in ChartRole
    }
    assert serves["ROLE_HISTORY"] == {"COPY_RATES_RANGE", "COPY_TICKS_RANGE", "RANGES"}
    assert serves["ROLE_PUBLISHER"] == {"SYMBOL_INFO", "SYMBOL_INFO_TICK", "ORDER_SEND", "OUTCOMES"}


def test_each_served_calls_arguments_are_read_by_their_registry_index_and_type(text):
    values = defines(text)
    _, _, body = functions(text)["HubArgumentType"]
    taken = re.findall(
        r"call == HUB_CALL_(\w+) && index == (HUB_ARG_\w+)\)\s+type = HUB_ARGUMENT_(\w+);", body
    )
    served = [row for row in calls.CALLS.values() if row.role is not None]
    expected = [
        (row.call.name, f"HUB_ARG_{row.call.name}_{argument.name.upper()}", argument.type.name)
        for row in served
        for argument in row.arguments
    ]
    indexes = {
        f"HUB_ARG_{row.call.name}_{argument.name.upper()}": str(index)
        for row in served
        for index, argument in enumerate(row.arguments)
    }

    assert taken == expected
    assert {name: values[name] for name in indexes} == indexes
    assert int(values["HUB_ARGUMENTS_MAX"]) == max(len(row.arguments) for row in served)
    assert enum(text, "ENUM_HUB_ARGUMENT") == {
        f"HUB_ARGUMENT_{kind.name}": index for index, kind in enumerate(calls.ArgType)
    }


def test_the_generated_text_is_the_same_on_every_run(tmp_path):
    first, second = tmp_path / "first.mqh", tmp_path / "second.mqh"
    for path in (first, second):
        subprocess.run([sys.executable, "-m", "mt5connector.server.mql5gen", str(path)], check=True)

    assert first.read_bytes() == second.read_bytes()
    assert first.read_text(encoding="utf-8") == mql5gen.render()
