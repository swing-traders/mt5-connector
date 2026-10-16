"""The EA's half of the hub protocol as the generator writes it in MQL5, against the inventory it
is written from."""

import re
import subprocess
import sys

import pytest

from mt5connector.server import mql5gen
from mt5connector.server.wire import binary, calls, mirror
from mt5connector.server.wire.push_wire import ChartRole

Call = calls.Call

# Each record an EA writes: its layout's name, and how MQL5 reads a member of it — a family's
# property functions with the arguments ahead of the property, or a struct variable's fields.
WRITERS = {
    "WriteAccountInfo": ("AccountInfo", "AccountInfo", ""),
    "WriteTerminalInfo": ("TerminalInfo", "TerminalInfo", ""),
    "WriteSymbolInfo": ("SymbolInfo", "SymbolInfo", "symbol, "),
    "WritePosition": ("TradePosition", "PositionGet", ""),
    "WriteOrder": ("TradeOrder", "OrderGet", ""),
    "WriteHistoryOrder": ("TradeOrder", "HistoryOrderGet", "ticket, "),
    "WriteDeal": ("TradeDeal", "HistoryDealGet", "ticket, "),
    "WriteTradeRequest": ("MqlTradeRequest", "request", None),
    "WriteTradeResult": ("MqlTradeResult", "result", None),
    "WriteTradeTransaction": ("MqlTradeTransaction", "trans", None),
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
_FUNCTION = re.compile(r"^(\w+) (\w+)\(([^)]*)\)\n\{\n(.*?)^\}", re.MULTILINE | re.DOTALL)
_WRITE = re.compile(
    r"w\.(I64|I64Bool|F64|String)\((?:\(long\))?"
    r"(?:(\w+?)(Integer|Double|String)\((\w+, )?(\w+)\)|(\w+)\.(\w+)|(\w+))\);"
)
_READ = re.compile(r"request\.(\w+) = (?:\((\w+)\))?r\.(I64|F64|String)\(\);")


@pytest.fixture(scope="module")
def text() -> str:
    return mql5gen.render()


def functions(text: str) -> dict[str, tuple[str, str]]:
    """Each generated function by its name: its parameters and its body."""
    return {name: (parameters, body) for _, name, parameters, body in _FUNCTION.findall(text)}


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


def written(body: str) -> list[tuple]:
    """Each write of a writer's body: the writer method, and the family, accessor, leading arguments
    and property it reads, or the struct variable and field, or the parameter it writes whole."""
    writes = []
    for line in body.splitlines():
        if line.strip():
            match = _WRITE.fullmatch(line.strip())
            assert match is not None, line
            method, family, accessor, leading, prop, variable, field, whole = match.groups()
            if family is not None:
                writes.append((method, family, accessor, leading or "", prop))
            elif variable is not None:
                writes.append((method, variable, field))
            else:
                writes.append((method, whole))
    return writes


def expected_writes(layout: binary.Layout, family: str, leading: str | None) -> list[tuple]:
    """The writes of a layout's record: its key first, the symbol it is read for, then every typed
    member in order."""
    writes = []
    if layout.key is not None:
        writes.append(("String", "symbol"))
    for member in layout.members:
        if member.declared is not None:
            kind = mirror.accessor(member.declared)
            if leading is None:
                writes.append((wire_type(member), family, member.property))
            else:
                writes.append((wire_type(member), family, _FAMILY[kind], leading, member.property))
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
    assert served <= {layout for layout, _, _ in WRITERS.values()}


@pytest.mark.parametrize("name", WRITERS)
def test_a_writer_reads_each_typed_member_once_in_order_by_its_property(text, name):
    layout_name, family, leading = WRITERS[name]
    _, body = functions(text)[name]

    assert written(body) == expected_writes(_LAYOUTS[layout_name], family, leading)


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
    parameters, body = functions(text)["ReadTradeRequest"]
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


def test_the_chart_roles_are_numbered_as_the_templates_set_them_and_named_as_the_hub_reads_them(
    text,
):
    named = dict(re.findall(r'case (ROLE_\w+):\s+name = "(\w+)";', text))
    read = dict(re.findall(r'name == "(\w+)"\)\s+role = (\w+);', text))

    # image/install-ea.sh writes these values into the spawner's and the publisher's templates.
    assert enum(text, "ENUM_CHART_ROLE") == {"ROLE_SPAWNER": 0, "ROLE_PUBLISHER": 1}
    assert named == {f"ROLE_{role.name}": role.value for role in ChartRole}
    assert read == {role.value: f"ROLE_{role.name}" for role in ChartRole}


def test_each_role_serves_the_calls_the_registry_gives_it(text):
    _, body = functions(text)["HubServes"]
    segments = re.split(r"case (ROLE_\w+):", body)[1:]
    serves = {
        role: set(re.findall(r"HUB_CALL_(\w+)", segment))
        for role, segment in zip(segments[::2], segments[1::2], strict=True)
    }

    assert serves == {
        f"ROLE_{role.name}": {row.call.name for row in calls.CALLS.values() if row.role is role}
        for role in ChartRole
    }


def test_each_served_calls_arguments_are_read_by_their_registry_index_and_type(text):
    values = defines(text)
    _, body = functions(text)["HubArgumentType"]
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
