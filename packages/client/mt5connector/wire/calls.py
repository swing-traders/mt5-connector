"""The hub's call registry: every call a consumer asks, its arguments with their wire types, the
shape of its answer, the role of the EA that serves it — none for a call the hub answers itself —
and the deadline it waits under; and the codecs of a call's arguments and of its answer's
results."""

from collections import namedtuple
from dataclasses import dataclass
from enum import IntEnum, StrEnum

import numpy as np

from . import binary, mirror
from .binary import CodecError, Reader, Writer
from .history_wire import BARS, FloorOrigin, Series
from .push_wire import ChartRole

# The deadlines a request waits under, which bind the hub and the session alike: a read's answer, a
# command's answer that it was sent, a bar history read's without a busy, and the margin past a
# busy's bound.
READ_DEADLINE_S = 5.0
COMMAND_DEADLINE_S = 1.0
HISTORY_DEADLINE_S = 45.0
BUSY_MARGIN_S = 10.0
# A tick read's deadline from the moment it is sent, and the bound of the busy the history EA renews
# while the read is in flight: the EA repeats the terminal's call until its answer settles, one call
# can wait past the history deadline, and a read queued behind another's call hears no busy until
# that call returns.
TICK_BUSY_S = 900
# How long a deferred request waits before it is asked again, by default.
RETRY_AFTER_S = 5.0
# The most result bytes an EA's answer part carries, which keeps its frame within the hub's 1 MiB
# message limit.
PART_BYTES = 1_000_000

_TICKS = np.dtype(list(mirror.TICKS.dtype))
_RATES = np.dtype(list(mirror.RATES.dtype))
_BARS = np.dtype(list(BARS.dtype))
_U32_MAX = 0xFFFFFFFF


class Call(IntEnum):
    """A call, by the code its request carries."""

    ACCOUNT_INFO = 1
    TERMINAL_INFO = 2
    SYMBOLS_GET = 3
    SYMBOLS_TOTAL = 4
    SYMBOL_INFO = 6
    SYMBOL_INFO_TICK = 7
    POSITIONS_GET = 8
    ORDERS_GET = 9
    HISTORY_ORDERS_GET = 10
    HISTORY_DEALS_GET = 11
    ORDER_SEND = 12
    COPY_RATES_RANGE = 13
    COPY_RATES_FROM = 14
    COPY_TICKS_RANGE = 15
    COPY_TICKS_FROM = 16
    RANGES = 17
    OUTCOMES = 18
    COMMISSION_SCHEDULE = 19
    TRADING_SESSIONS = 20


class ArgType(StrEnum):
    """How an argument travels."""

    INTEGER = "integer"
    STRING = "string"
    # An int64 of epoch seconds: true UTC from a consumer, the broker's clock to the EA.
    EPOCH = "epoch"
    # MqlTradeRequest's field sequence.
    TRADE_REQUEST = "trade_request"
    # A count, then each MQL5 request id as a uint32.
    REQUEST_IDS = "request_ids"


class Shape(StrEnum):
    """What an answer's records are."""

    # One record of the call's layout, or a record per row.
    STRUCT = "struct"
    STRUCTS = "structs"
    # One MqlTick, or MqlTicks, as StructToCharArray packs them.
    TICK = "tick"
    TICKS = "ticks"
    # MqlRates from the EA; to a consumer each bar with its close.
    RATES = "rates"
    INTEGER = "integer"
    RANGES = "ranges"
    SCHEDULE = "schedule"
    SESSIONS = "sessions"
    # Records of the three structs one OnTradeTransaction call receives, back to back.
    TRANSACTIONS = "transactions"


# The shapes of a result of rows, which alone travels in parts.
_ROWS = frozenset({Shape.STRUCTS, Shape.TICKS, Shape.RATES, Shape.TRANSACTIONS})
# The shapes of a history read's result, whose last part carries the floor fact.
_HISTORY = frozenset({Shape.TICKS, Shape.RATES})


class Wait(StrEnum):
    """Which deadline a call waits under."""

    READ = "read"
    COMMAND = "command"
    HISTORY = "history"
    TICKS = "ticks"


@dataclass(frozen=True)
class Argument:
    name: str
    type: ArgType
    required: bool


@dataclass(frozen=True)
class Row:
    """A call, its arguments and their forms, its answer's shape and layout, the role of the EA that
    serves it, and the deadline it waits under."""

    call: Call
    arguments: tuple[Argument, ...]
    shape: Shape
    role: ChartRole | None
    wait: Wait
    layout: binary.Layout | None = None
    # The argument sets of which a call carries at least one in full.
    forms: tuple[tuple[str, ...], ...] = ()


@dataclass(frozen=True)
class SeriesRange:
    """A series' floor in true UTC, when the hub learned it, and its generation."""

    floor: int
    measured_at: int
    generation: int


@dataclass(frozen=True)
class Ranges:
    """The terminal's MaxBars and every floor the hub holds for a symbol."""

    maxbars: int
    series: dict[Series, SeriesRange]


@dataclass(frozen=True)
class FloorFact:
    """A floor an EA measured on a history read, on the broker's clock — seconds for bars,
    milliseconds for ticks — and what set it."""

    floor: int
    origin: FloorOrigin


_ARG_TYPES = {
    mirror.ParamKind.INT: ArgType.INTEGER,
    mirror.ParamKind.STR: ArgType.STRING,
    mirror.ParamKind.DATETIME: ArgType.EPOCH,
    mirror.ParamKind.REQUEST: ArgType.TRADE_REQUEST,
}
_SYMBOL = Argument("symbol", ArgType.STRING, required=True)


def _package(call: Call, name: mirror.FunctionName, role: ChartRole, wait: Wait) -> Row:
    """The row of a call that is one package function, its arguments and answer as the inventory
    declares them."""
    function = mirror.FUNCTIONS[name]
    arguments = tuple(
        Argument(param.name, _ARG_TYPES[param.kind], param.required) for param in function.params
    )
    struct_layout = None
    if name is mirror.FunctionName.ORDER_SEND:
        shape = Shape.STRUCT
        struct_layout = binary.TRADE_RESULT
    elif name is mirror.FunctionName.SYMBOLS_GET:
        shape = Shape.STRUCTS
        struct_layout = binary.SYMBOL_LISTING
    elif function.result is mirror.ResultKind.STRUCT and function.struct is mirror.StructName.TICK:
        shape = Shape.TICK
    elif function.result is mirror.ResultKind.STRUCT:
        shape = Shape.STRUCT
        struct_layout = binary.layout(mirror.STRUCTS[function.struct])
    elif function.result is mirror.ResultKind.STRUCTS:
        shape = Shape.STRUCTS
        struct_layout = binary.layout(mirror.STRUCTS[function.struct])
    elif function.array is mirror.ArrayName.TICKS:
        shape = Shape.TICKS
    elif function.array is mirror.ArrayName.RATES:
        shape = Shape.RATES
    elif function.result is mirror.ResultKind.SCALAR:
        shape = Shape.INTEGER
    else:
        raise ValueError(f"{name}: the call registry has no shape for a {function.result} result")
    return Row(call, arguments, shape, role, wait, struct_layout, function.forms)


CALLS: dict[Call, Row] = {
    row.call: row
    for row in (
        _package(Call.ACCOUNT_INFO, mirror.FunctionName.ACCOUNT_INFO, ChartRole.SPAWNER, Wait.READ),
        _package(
            Call.TERMINAL_INFO, mirror.FunctionName.TERMINAL_INFO, ChartRole.SPAWNER, Wait.READ
        ),
        _package(Call.SYMBOLS_GET, mirror.FunctionName.SYMBOLS_GET, ChartRole.SPAWNER, Wait.READ),
        _package(
            Call.SYMBOLS_TOTAL, mirror.FunctionName.SYMBOLS_TOTAL, ChartRole.SPAWNER, Wait.READ
        ),
        _package(Call.SYMBOL_INFO, mirror.FunctionName.SYMBOL_INFO, ChartRole.PUBLISHER, Wait.READ),
        _package(
            Call.SYMBOL_INFO_TICK,
            mirror.FunctionName.SYMBOL_INFO_TICK,
            ChartRole.PUBLISHER,
            Wait.READ,
        ),
        _package(
            Call.POSITIONS_GET, mirror.FunctionName.POSITIONS_GET, ChartRole.SPAWNER, Wait.READ
        ),
        _package(Call.ORDERS_GET, mirror.FunctionName.ORDERS_GET, ChartRole.SPAWNER, Wait.READ),
        _package(
            Call.HISTORY_ORDERS_GET,
            mirror.FunctionName.HISTORY_ORDERS_GET,
            ChartRole.SPAWNER,
            Wait.READ,
        ),
        _package(
            Call.HISTORY_DEALS_GET,
            mirror.FunctionName.HISTORY_DEALS_GET,
            ChartRole.SPAWNER,
            Wait.READ,
        ),
        _package(
            Call.ORDER_SEND, mirror.FunctionName.ORDER_SEND, ChartRole.PUBLISHER, Wait.COMMAND
        ),
        _package(
            Call.COPY_RATES_RANGE,
            mirror.FunctionName.COPY_RATES_RANGE,
            ChartRole.PUBLISHER,
            Wait.HISTORY,
        ),
        _package(
            Call.COPY_RATES_FROM,
            mirror.FunctionName.COPY_RATES_FROM,
            ChartRole.PUBLISHER,
            Wait.HISTORY,
        ),
        _package(
            Call.COPY_TICKS_RANGE,
            mirror.FunctionName.COPY_TICKS_RANGE,
            ChartRole.HISTORY,
            Wait.TICKS,
        ),
        _package(
            Call.COPY_TICKS_FROM,
            mirror.FunctionName.COPY_TICKS_FROM,
            ChartRole.HISTORY,
            Wait.TICKS,
        ),
        # The hub answers it from the floors it holds and the MaxBars of a terminal_info it asks.
        Row(Call.RANGES, (_SYMBOL,), Shape.RANGES, None, Wait.READ),
        # A publisher answers each request id its outcome log holds with its REQUEST transaction.
        Row(
            Call.OUTCOMES,
            (_SYMBOL, Argument("request_ids", ArgType.REQUEST_IDS, required=True)),
            Shape.TRANSACTIONS,
            ChartRole.PUBLISHER,
            Wait.READ,
        ),
        Row(Call.COMMISSION_SCHEDULE, (_SYMBOL,), Shape.SCHEDULE, None, Wait.READ),
        Row(Call.TRADING_SESSIONS, (_SYMBOL,), Shape.SESSIONS, None, Wait.READ),
    )
}

# The records a session hands back for a struct, by its layout's name.
STRUCT_TYPES: dict[str, type] = {
    row.layout.name: namedtuple(row.layout.name, row.layout.fields)
    for row in CALLS.values()
    if row.layout is not None
}
Tick = namedtuple("Tick", mirror.TICKS.names)


def routed_symbol(row: Row, arguments: dict[str, object]) -> str | None:
    """The symbol whose EA serves a request, or whose relays answer it; None for the spawner's."""
    if row.role is ChartRole.SPAWNER:
        return None
    elif row.call is Call.ORDER_SEND:
        return arguments["request"]["symbol"]
    else:
        return arguments["symbol"]


def trade_request(fields: dict[str, object]) -> dict[str, object]:
    """A whole MqlTradeRequest from the fields given, every other member zero as MQL5 initialises
    it; raises ValueError naming a field it has no member for."""
    unknown = sorted(set(fields) - set(binary.TRADE_REQUEST.fields))
    if unknown:
        raise ValueError(f"MqlTradeRequest: unknown field: {', '.join(unknown)}")
    request = {}
    for member in binary.TRADE_REQUEST.members:
        kind = mirror.accessor(member.declared)
        if member.name in fields:
            request[member.name] = fields[member.name]
        elif kind is mirror.Accessor.STRING:
            request[member.name] = ""
        elif kind is mirror.Accessor.DOUBLE:
            request[member.name] = 0.0
        else:
            request[member.name] = 0
    return request


# ── Arguments ─────────────────────────────────────────────────────────────────


def encode_arguments(row: Row, arguments: dict[str, object]) -> tuple[int, bytes]:
    """The count and the bytes of a request's arguments, each by its index in the row; raises
    ValueError for an argument the row does not take, one of the wrong type, or a missing one."""
    known = {argument.name for argument in row.arguments}
    unknown = sorted(set(arguments) - known)
    if unknown:
        raise ValueError(f"{row.call.name.lower()}: unknown argument: {', '.join(unknown)}")
    refusal = _arguments_refusal(row, set(arguments))
    if refusal is not None:
        raise ValueError(refusal)
    writer = Writer()
    count = 0
    for index, argument in enumerate(row.arguments):
        if argument.name in arguments:
            writer.u8(index)
            _write_argument(writer, row, argument, arguments[argument.name])
            count += 1
    return count, writer.payload()


def decode_arguments(row: Row, request: binary.Request) -> dict[str, object]:
    """The arguments a request carries; raises CodecError for any out of the row's layout, a missing
    one, or none of its forms."""
    reader = Reader(request.arguments)
    arguments: dict[str, object] = {}
    last = -1
    for _ in range(request.count):
        at = reader.offset
        index = reader.u8("argument index")
        if not last < index < len(row.arguments):
            raise CodecError(f"argument offset {at}: index {index} is out of order or range")
        last = index
        argument = row.arguments[index]
        arguments[argument.name] = _read_argument(reader, argument)
    reader.done()
    refusal = _arguments_refusal(row, set(arguments))
    if refusal is not None:
        raise CodecError(refusal)
    return arguments


def _arguments_refusal(row: Row, given: set[str]) -> str | None:
    missing = [
        argument.name
        for argument in row.arguments
        if argument.required and argument.name not in given
    ]
    if missing:
        return f"{row.call.name.lower()}: missing argument: {', '.join(missing)}"
    elif row.forms and not any(set(form) <= given for form in row.forms):
        return f"{row.call.name.lower()}: the arguments name none of its forms"
    else:
        return None


def _write_argument(writer: Writer, row: Row, argument: Argument, value: object) -> None:
    where = f"{row.call.name.lower()}.{argument.name}"
    if argument.type is ArgType.INTEGER or argument.type is ArgType.EPOCH:
        if not _is_integer(value):
            raise ValueError(f"{where}: {value!r} is not an integer")
        writer.i64(value)
    elif argument.type is ArgType.STRING:
        if not isinstance(value, str):
            raise ValueError(f"{where}: {value!r} is not a string")
        writer.string(value)
    elif argument.type is ArgType.TRADE_REQUEST:
        if not (isinstance(value, dict) and set(value) == set(binary.TRADE_REQUEST.fields)):
            raise ValueError(f"{where}: not a whole MqlTradeRequest")
        binary.write_record(writer, binary.TRADE_REQUEST, value)
    else:
        if not all(_is_integer(item) and 0 <= item <= _U32_MAX for item in value):
            raise ValueError(f"{where}: {value!r} are not uint32 request ids")
        writer.u32(len(value))
        for item in value:
            writer.u32(item)


def _read_argument(reader: Reader, argument: Argument) -> object:
    if argument.type is ArgType.INTEGER or argument.type is ArgType.EPOCH:
        return reader.i64(argument.name)
    elif argument.type is ArgType.STRING:
        return reader.string(argument.name)
    elif argument.type is ArgType.TRADE_REQUEST:
        return binary.read_record(reader, binary.TRADE_REQUEST)
    else:
        return tuple(reader.u32(argument.name) for _ in range(reader.u32(argument.name)))


# ── Results ───────────────────────────────────────────────────────────────────


def read_ea_result(row: Row, answer: binary.Answer) -> tuple[object, FloorFact | None]:
    """The results of an EA's answer part, on the broker's clock, and the floor fact the last part
    of a history read carries; raises CodecError for a part out of the row's layout, a part before
    the last of a result that is one record among them."""
    if not answer.last and row.shape not in _ROWS:
        raise CodecError(f"{row.call.name.lower()}: a {row.shape} answer in a part before its last")
    reader = Reader(answer.result)
    value = _read_value(row, reader, answer.count, binary.Sender.EA)
    fact = None
    if row.shape in _HISTORY and answer.last:
        at = reader.offset
        facts = reader.u8("floors")
        if facts == 1:
            fact = FloorFact(reader.i64("floor"), reader.member(FloorOrigin, "origin"))
        elif facts != 0:
            raise CodecError(f"result offset {at}: {facts} floors on one answer")
    reader.done()
    return value, fact


def read_hub_result(row: Row, answer: binary.Answer) -> object:
    """The results of a hub's answer part, in true UTC; raises CodecError for a part out of the
    row's layout."""
    reader = Reader(answer.result)
    value = _read_value(row, reader, answer.count, binary.Sender.HUB)
    reader.done()
    return value


def write_result(row: Row, value: object) -> tuple[int, bytes]:
    """The count and the bytes of a result as the hub sends it to a consumer."""
    writer = Writer()
    if row.shape is Shape.STRUCT:
        count = 1
        binary.write_record(writer, row.layout, value)
    elif row.shape is Shape.STRUCTS:
        count = len(value)
        for record in value:
            binary.write_record(writer, row.layout, record)
    elif row.shape is Shape.TICK:
        count = 1
        binary.write_pod(writer, binary.TICK, mirror.TICKS, value)
    elif row.shape is Shape.TICKS:
        count = len(value)
        writer.raw(np.ascontiguousarray(value, dtype=_TICKS).tobytes())
    elif row.shape is Shape.RATES:
        count = len(value)
        writer.raw(np.ascontiguousarray(value, dtype=_BARS).tobytes())
    elif row.shape is Shape.INTEGER:
        count = 1
        writer.i64(value)
    elif row.shape is Shape.RANGES:
        count = len(value.series)
        writer.i64(value.maxbars)
        for series, entry in value.series.items():
            writer.string(series)
            writer.i64(entry.floor)
            writer.i64(entry.measured_at)
            writer.i64(entry.generation)
    elif row.shape is Shape.SCHEDULE:
        count = 1
        binary.write_schedule(writer, value)
    elif row.shape is Shape.TRANSACTIONS:
        count = len(value)
        for transaction in value:
            binary.write_transaction(writer, transaction)
    else:
        count = 1
        binary.write_sessions(writer, value)
    return count, writer.payload()


def joined(row: Row, parts: list[object]) -> object:
    """A result from the values of its answer's parts, in order: the rows of every part, or the one
    part of a single-record shape."""
    if row.shape is Shape.STRUCTS or row.shape is Shape.TRANSACTIONS:
        return tuple(record for part in parts for record in part)
    elif row.shape is Shape.TICKS or row.shape is Shape.RATES:
        return np.concatenate(parts)
    elif len(parts) == 1:
        return parts[0]
    else:
        raise CodecError(f"{row.call.name.lower()}: a {row.shape} answer in {len(parts)} parts")


def typed(row: Row, value: object) -> object:
    """A result as a session hands it back: a struct as its record type, rows as numpy arrays,
    transactions as the frames a live one arrives as."""
    if row.shape is Shape.STRUCT:
        return STRUCT_TYPES[row.layout.name](**value)
    elif row.shape is Shape.STRUCTS:
        return tuple(STRUCT_TYPES[row.layout.name](**record) for record in value)
    elif row.shape is Shape.TICK:
        return Tick(**value)
    else:
        return value


def _read_value(row: Row, reader: Reader, count: int, sender: binary.Sender) -> object:
    if row.shape is Shape.STRUCT:
        _one(row, count)
        return binary.read_record(reader, row.layout)
    elif row.shape is Shape.STRUCTS:
        return [binary.read_record(reader, row.layout) for _ in range(count)]
    elif row.shape is Shape.TICK:
        _one(row, count)
        return binary.read_pod(reader, binary.TICK, mirror.TICKS)
    elif row.shape is Shape.TICKS:
        return _rows(reader, count, _TICKS)
    elif row.shape is Shape.RATES and sender is binary.Sender.EA:
        return _rows(reader, count, _RATES)
    elif row.shape is Shape.RATES:
        return _rows(reader, count, _BARS)
    elif row.shape is Shape.INTEGER:
        _one(row, count)
        return reader.i64("result")
    elif row.shape is Shape.RANGES:
        maxbars = reader.i64("maxbars")
        series = {}
        for _ in range(count):
            name = reader.member(Series, "series")
            series[name] = SeriesRange(
                reader.i64("floor"), reader.i64("measured_at"), reader.i64("generation")
            )
        return Ranges(maxbars, series)
    elif row.shape is Shape.SCHEDULE:
        _one(row, count)
        return binary.read_schedule(reader)
    elif row.shape is Shape.TRANSACTIONS:
        return [binary.read_transaction(reader) for _ in range(count)]
    else:
        _one(row, count)
        return binary.read_sessions(reader)


def _rows(reader: Reader, count: int, dtype: np.dtype) -> np.ndarray:
    return np.frombuffer(reader.raw(count * dtype.itemsize, "rows"), dtype=dtype)


def _one(row: Row, count: int) -> None:
    if count != 1:
        raise CodecError(f"{row.call.name.lower()}: {count} records where a {row.shape} is one")


def _is_integer(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)
