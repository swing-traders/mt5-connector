"""Writes `hub_wire.mqh`, the EA's half of the hub protocol in MQL5, from the wire inventory."""

import sys
from dataclasses import dataclass
from pathlib import Path

from mt5connector.server.wire import binary, calls, mirror
from mt5connector.server.wire.push_wire import ChartRole, Role

_FAMILY = {
    mirror.Accessor.INTEGER: "Integer",
    mirror.Accessor.DOUBLE: "Double",
    mirror.Accessor.STRING: "String",
}
# The parameter that names the record a SymbolInfo writer reads, written first as its key.
_SYMBOL = "symbol"


@dataclass(frozen=True)
class _Properties:
    """Members read by one MQL5 family's property functions — `<family>Integer`, `<family>Double`
    and `<family>String` — each taking `leading` ahead of the property."""

    family: str
    leading: str = ""

    def read(self, member: mirror.Member) -> str:
        function = f"{self.family}{_FAMILY[mirror.accessor(member.declared)]}"
        return f"{function}({self.leading}{member.property})"


@dataclass(frozen=True)
class _Fields:
    """Members read off a variable of the MQL5 struct, each by its field name."""

    variable: str

    def read(self, member: mirror.Member) -> str:
        if mirror.accessor(member.declared) is mirror.Accessor.INTEGER:
            return f"(long){self.variable}.{member.property}"
        else:
            return f"{self.variable}.{member.property}"


@dataclass(frozen=True)
class _Writer:
    """A generated function writing one record of a layout."""

    name: str
    layout: binary.Layout
    parameters: str
    source: _Properties | _Fields
    # Which record it reads, where its name does not say.
    note: str | None


_WRITERS = (
    _Writer(
        "WriteAccountInfo",
        calls.CALLS[calls.Call.ACCOUNT_INFO].layout,
        "",
        _Properties("AccountInfo"),
        None,
    ),
    _Writer(
        "WriteTerminalInfo",
        calls.CALLS[calls.Call.TERMINAL_INFO].layout,
        "",
        _Properties("TerminalInfo"),
        None,
    ),
    _Writer(
        "WriteSymbolInfo",
        calls.CALLS[calls.Call.SYMBOL_INFO].layout,
        f"const string {_SYMBOL}",
        _Properties("SymbolInfo", f"{_SYMBOL}, "),
        "A symbol's SymbolInfo, its name the key ahead of its members.",
    ),
    _Writer(
        "WritePosition",
        calls.CALLS[calls.Call.POSITIONS_GET].layout,
        "",
        _Properties("PositionGet"),
        "The open position last selected.",
    ),
    _Writer(
        "WriteOrder",
        calls.CALLS[calls.Call.ORDERS_GET].layout,
        "",
        _Properties("OrderGet"),
        "The pending order last selected.",
    ),
    _Writer(
        "WriteHistoryOrder",
        calls.CALLS[calls.Call.HISTORY_ORDERS_GET].layout,
        "const ulong ticket",
        _Properties("HistoryOrderGet", "ticket, "),
        "An order of the history selected, by its ticket.",
    ),
    _Writer(
        "WriteDeal",
        calls.CALLS[calls.Call.HISTORY_DEALS_GET].layout,
        "const ulong ticket",
        _Properties("HistoryDealGet", "ticket, "),
        "A deal of the history selected, by its ticket.",
    ),
    _Writer(
        "WriteTradeRequest",
        binary.TRADE_REQUEST,
        "const MqlTradeRequest &request",
        _Fields("request"),
        None,
    ),
    _Writer(
        "WriteTradeResult",
        binary.TRADE_RESULT,
        "const MqlTradeResult &result",
        _Fields("result"),
        None,
    ),
    _Writer(
        "WriteTradeTransaction",
        binary.TRADE_TRANSACTION,
        "const MqlTradeTransaction &trans",
        _Fields("trans"),
        None,
    ),
)

_HEADER = (
    "// hub_wire.mqh: the EA's half of the hub protocol, written from the wire inventory by\n"
    "// mt5connector.server.mql5gen."
)


def render() -> str:
    """The text of hub_wire.mqh; raises ValueError for a struct an EA sends that no writer writes,
    or a typed member without its MQL5 property."""
    _check()
    sections = [
        _HEADER,
        _constants(),
        _enum(
            "ENUM_HUB_FRAME", [(f"HUB_FRAME_{kind.name}", kind.value) for kind in binary.FrameKind]
        ),
        "// The error kinds an EA answers; the hub answers the others itself.\n"
        + _enum(
            "ENUM_HUB_ERROR",
            [(f"HUB_ERROR_{kind.name}", kind.value) for kind in sorted(binary.EA_ERRORS)],
        ),
        _enum("ENUM_HUB_CALL", [(f"HUB_CALL_{call.name}", call.value) for call in calls.Call]),
        "// The role an EA's chart template sets.\n"
        + _enum(
            "ENUM_CHART_ROLE",
            [(f"ROLE_{role.name}", index) for index, role in enumerate(ChartRole)],
        ),
        "// How a request's argument travels.\n"
        + _enum(
            "ENUM_HUB_ARGUMENT",
            [(f"HUB_ARGUMENT_{kind.name}", index) for index, kind in enumerate(calls.ArgType)],
        ),
        _argument_indexes(),
        '#include "hub_codec.mqh"',
        _chart_role_name(),
        _chart_role_named(),
        _serves(),
        _argument_type(),
        *(_writer(writer) for writer in _WRITERS),
        _request_reader(),
    ]
    return "\n\n".join(sections) + "\n"


def _check() -> None:
    written = {writer.layout.name for writer in _WRITERS}
    sent = {
        row.layout.name
        for row in calls.CALLS.values()
        if row.role is not None and row.layout is not None
    }
    missing = sorted(sent - written)
    if missing:
        raise ValueError(f"no writer of {', '.join(missing)}")
    for writer in _WRITERS:
        for member in writer.layout.members:
            if member.declared is not None and member.property is None:
                raise ValueError(f"{writer.layout.name}.{member.name}: no MQL5 property")


def _constants() -> str:
    values = [
        ("HUB_VERSION", str(binary.VERSION)),
        ("HUB_HEADER_SIZE", str(binary.HEADER.size)),
        ("HUB_FLAG_OK", str(binary.Flag.OK.value)),
        ("HUB_FLAG_LAST", str(binary.Flag.LAST.value)),
        ("HUB_TICK_SIZE", str(binary.TICK.size)),
        ("HUB_RATES_SIZE", str(binary.RATES.size)),
        ("HUB_PART_BYTES", str(calls.PART_BYTES)),
        ("HUB_ROLE_EA", f'"{Role.EA.value}"'),
    ]
    return "\n".join(f"#define {name} {value}" for name, value in values)


def _enum(name: str, members: list[tuple[str, int]]) -> str:
    lines = ",\n".join(f"    {member} = {value}" for member, value in members)
    return f"enum {name}\n{{\n{lines}\n}};"


def _ea_rows() -> list[calls.Row]:
    return [row for row in calls.CALLS.values() if row.role is not None]


def _argument(row: calls.Row, argument: calls.Argument) -> str:
    return f"HUB_ARG_{row.call.name}_{argument.name.upper()}"


def _argument_indexes() -> str:
    lines = ["// Each argument of a call an EA serves, by its index in the request."]
    for row in _ea_rows():
        for index, argument in enumerate(row.arguments):
            lines.append(f"#define {_argument(row, argument)} {index}")
    most = max(len(row.arguments) for row in _ea_rows())
    lines.append(f"#define HUB_ARGUMENTS_MAX {most}")
    return "\n".join(lines)


def _chart_role_name() -> str:
    cases = "\n".join(
        f'    case ROLE_{role.name}:\n        name = "{role.value}";\n        break;'
        for role in ChartRole
    )
    return (
        "// A chart role's name on the wire.\n"
        "string ChartRoleName(const ENUM_CHART_ROLE role)\n{\n"
        '    string name = "";\n'
        f"    switch (role)\n    {{\n{cases}\n    }}\n"
        "    return name;\n}"
    )


def _chart_role_named() -> str:
    branches = "\n    else ".join(
        f'if (name == "{role.value}")\n        role = ROLE_{role.name};' for role in ChartRole
    )
    return (
        "// The chart role a name on the wire gives; false for a name no role has.\n"
        "bool ChartRoleNamed(const string name, ENUM_CHART_ROLE &role)\n{\n"
        "    bool known = true;\n"
        f"    {branches}\n"
        "    else\n        known = false;\n"
        "    return known;\n}"
    )


def _serves() -> str:
    cases = []
    for role in ChartRole:
        served = [f"call == HUB_CALL_{row.call.name}" for row in _ea_rows() if row.role is role]
        condition = "\n                 || ".join(served)
        cases.append(f"    case ROLE_{role.name}:\n        serves = {condition};\n        break;")
    return (
        "// Whether an EA of the role serves the call.\n"
        "bool HubServes(const ENUM_CHART_ROLE role, const ENUM_HUB_CALL call)\n{\n"
        "    bool serves = false;\n"
        "    switch (role)\n    {\n" + "\n".join(cases) + "\n    }\n"
        "    return serves;\n}"
    )


def _argument_type() -> str:
    branches = []
    for row in _ea_rows():
        for argument in row.arguments:
            branches.append(
                f"if (call == HUB_CALL_{row.call.name} && index == {_argument(row, argument)})\n"
                f"        type = HUB_ARGUMENT_{argument.type.name};"
            )
    chain = "\n    else ".join(branches)
    return (
        "// How the call's argument at the index travels; false for an index the call does not"
        " take.\n"
        "bool HubArgumentType(const ENUM_HUB_CALL call, const int index, ENUM_HUB_ARGUMENT &type)\n"
        "{\n"
        "    bool taken = true;\n"
        f"    {chain}\n"
        "    else\n        taken = false;\n"
        "    return taken;\n}"
    )


def _write(member: mirror.Member, value: str) -> str:
    """The line writing one member's value as its accessor family travels: a bool as 0 or 1 in an
    int64."""
    kind = mirror.accessor(member.declared)
    if kind is mirror.Accessor.STRING:
        method = "String"
    elif kind is mirror.Accessor.DOUBLE:
        method = "F64"
    elif member.declared is mirror.MqlType.BOOL:
        method = "I64Bool"
    else:
        method = "I64"
    return f"    w.{method}({value});"


def _writer(writer: _Writer) -> str:
    parameters = "BinaryWriter &w"
    if writer.parameters:
        parameters += f", {writer.parameters}"
    lines = []
    if writer.layout.key is not None:
        lines.append(f"    w.String({_SYMBOL});")
    for member in writer.layout.members:
        if member.declared is not None:
            lines.append(_write(member, writer.source.read(member)))
    body = "\n".join(lines)
    function = f"void {writer.name}({parameters})\n{{\n{body}\n}}"
    if writer.note is None:
        return function
    else:
        return f"// {writer.note}\n{function}"


def _request_reader() -> str:
    lines = []
    for member in binary.TRADE_REQUEST.members:
        kind = mirror.accessor(member.declared)
        if kind is mirror.Accessor.STRING:
            value = "r.String()"
        elif kind is mirror.Accessor.DOUBLE:
            value = "r.F64()"
        else:
            value = f"({member.declared.value})r.I64()"
        lines.append(f"    request.{member.property} = {value};")
    body = "\n".join(lines)
    return f"void ReadTradeRequest(BinaryReader &r, MqlTradeRequest &request)\n{{\n{body}\n}}"


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: python -m mt5connector.server.mql5gen <path>")
    Path(sys.argv[1]).write_text(render(), encoding="utf-8")


if __name__ == "__main__":
    main()
