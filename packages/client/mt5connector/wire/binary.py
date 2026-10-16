"""The hub protocol's binary frames, encoded as the hub and its consumers send them and decoded as
they receive them: a fixed little-endian header, then a payload laid out as the MQL5 data is —
MqlTick and MqlRates as StructToCharArray packs them, every other struct as its field sequence, each
member an int64, a float64 or a length-prefixed UTF-8 string by the MQL5 accessor that reads it."""

import struct
from dataclasses import dataclass
from enum import IntEnum, IntFlag, StrEnum

from . import mirror
from .history_wire import BARS, Series
from .push_wire import (
    TRANSACTION_EPOCHS,
    TRANSACTION_MEMBERS,
    ChartRole,
    CommissionChargeMode,
    CommissionDirectionMode,
    CommissionEntryMode,
    CommissionMode,
    CommissionProfitMode,
    CommissionRangeMode,
    CommissionVolumeType,
    Role,
    Stream,
    Subscription,
)

# The header's one accepted version; neither it nor any header byte can open a JSON text.
VERSION = 2
# version, kind, flags, correlation id, count, payload length
HEADER = struct.Struct("<BBHQII")

_POD_CODES = {"<i8": "q", "<u8": "Q", "<i4": "i", "<u4": "I", "<f8": "d"}
_U8 = struct.Struct("<B")
_U16 = struct.Struct("<H")
_U32 = struct.Struct("<I")
_I32 = struct.Struct("<i")
_I64 = struct.Struct("<q")
_F64 = struct.Struct("<d")
_ACCESSOR_CODES = {mirror.Accessor.INTEGER: _I64, mirror.Accessor.DOUBLE: _F64}
_DAYS = 7


def _pod(array: mirror.Array) -> struct.Struct:
    return struct.Struct("<" + "".join(_POD_CODES[dtype] for _, dtype in array.dtype))


TICK = _pod(mirror.TICKS)
RATES = _pod(mirror.RATES)
# A bar as the hub sends it to a consumer: its MqlRates, then its close.
BAR = _pod(BARS)
# StructToCharArray packs both with no padding: 60 bytes each on the terminal's build.
if TICK.size != 60 or RATES.size != 60:
    raise ImportError(f"MqlTick packs to {TICK.size} bytes and MqlRates to {RATES.size}, not 60")


class FrameKind(IntEnum):
    """A frame's kind, the header's second byte."""

    HELLO = 1
    SUBSCRIBE = 2
    UNSUBSCRIBE = 3
    ACK = 4
    ERROR = 5
    WANTED = 6
    OPEN_CHART = 7
    CLOSE_CHART = 8
    CHART_OPENED = 9
    CHART_FAILED = 10
    CHART_KEPT = 11
    DUPLICATE = 12
    TICK = 13
    BAR = 14
    TRADE_TRANSACTION = 15
    SERVER_TIME = 16
    COMMISSIONS = 17
    SESSIONS = 18
    REQUEST = 19
    ANSWER = 20
    BUSY = 21


class ErrorKind(IntEnum):
    """Why a request was answered without its result."""

    SYNCING = 1
    NOT_READY = 2
    NO_PUBLISHER = 3
    INVALID_WINDOW = 4
    OVERFLOW = 5
    EA_GONE = 6
    TIMEOUT = 7
    EA_ERROR = 8


# The kinds that defer a request: each carries the delay before asking again.
DEFERRING = frozenset({ErrorKind.SYNCING, ErrorKind.NOT_READY, ErrorKind.NO_PUBLISHER})
# The kinds an EA answers; the hub originates the others.
EA_ERRORS = frozenset({ErrorKind.SYNCING, ErrorKind.INVALID_WINDOW, ErrorKind.EA_ERROR})


class Flag(IntFlag):
    """An answer's header flags: its result rather than an error, and its last part."""

    OK = 1
    LAST = 2


class Sender(StrEnum):
    """Who sent a frame being decoded, which decides the kinds it may be and a bar's layout."""

    EA = "ea"
    ADAPTER = "adapter"
    HUB = "hub"


class CodecError(ValueError):
    """Raised for a frame out of its kind's layout, naming the offset where it breaks, with the kind
    and the correlation id its header carried; None for both where the header itself breaks."""

    def __init__(
        self, message: str, kind: "FrameKind | None" = None, correlation: int | None = None
    ) -> None:
        super().__init__(message)
        self.kind = kind
        self.correlation = correlation


class UnknownKind(CodecError):
    """Raised for a well-formed header of a kind this codec does not know."""


@dataclass(frozen=True)
class Layout:
    """A struct's field sequence in a frame: its key string, when it has one, then each typed member
    by its accessor."""

    name: str
    members: tuple[mirror.Member, ...]
    epochs: dict[str, mirror.EpochUnit]
    key: str | None = None

    @property
    def fields(self) -> tuple[str, ...]:
        """The fields a record of this layout carries, in the struct's order."""
        return tuple(
            member.name
            for member in self.members
            if member.declared is not None or member.name == self.key
        )


def layout(
    struct_: mirror.Struct, *, name: str | None = None, without: frozenset[str] = frozenset()
) -> Layout:
    """The frame layout of an inventory struct, under `name` and without the fields `without`
    names."""
    return Layout(
        name or struct_.name.value,
        tuple(member for member in struct_.members if member.name not in without),
        struct_.epochs,
        struct_.key,
    )


_SEND_RESULT = mirror.STRUCTS[mirror.StructName.ORDER_SEND_RESULT]
TRADE_REQUEST = layout(mirror.STRUCTS[mirror.StructName.TRADE_REQUEST], name="MqlTradeRequest")
# MqlTradeResult: the package's OrderSendResult without the request the package appends to it.
TRADE_RESULT = layout(_SEND_RESULT, name="MqlTradeResult", without=frozenset(_SEND_RESULT.nested))
TRADE_TRANSACTION = Layout("MqlTradeTransaction", TRANSACTION_MEMBERS, TRANSACTION_EPOCHS)

# ── Frames ────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class AdapterHello:
    """A consumer's first frame."""


@dataclass(frozen=True)
class EaHello:
    """An EA's first frame: its chart's role and symbol."""

    chart_role: ChartRole
    symbol: str


@dataclass(frozen=True)
class Subscribe:
    op_id: int
    subscription: Subscription


@dataclass(frozen=True)
class Unsubscribe:
    op_id: int
    subscription: Subscription


@dataclass(frozen=True)
class Ack:
    """The hub's acknowledgement of an op, queued ahead of every frame the op routes."""

    op_id: int


@dataclass(frozen=True)
class Refusal:
    """The hub's answer to a frame it cannot handle; the connection closes after it."""

    message: str


@dataclass(frozen=True)
class Wanted:
    """The streams of its symbol some consumer subscribes to, told to the symbol's publisher."""

    symbol: str
    ticks: bool
    # MQL5's ENUM_TIMEFRAMES values.
    timeframes: tuple[int, ...]


@dataclass(frozen=True)
class OpenChart:
    chart_role: ChartRole
    symbol: str


@dataclass(frozen=True)
class CloseChart:
    chart_role: ChartRole
    symbol: str


@dataclass(frozen=True)
class ChartOpened:
    chart_role: ChartRole
    symbol: str


@dataclass(frozen=True)
class ChartFailed:
    """The spawner's answer that no chart opened, with the failing call's MQL5 error."""

    chart_role: ChartRole
    symbol: str
    error: int


@dataclass(frozen=True)
class ChartKept:
    """The spawner's answer that a chart stays open for the account's holdings on its symbol."""

    chart_role: ChartRole
    symbol: str
    positions: int
    orders: int


@dataclass(frozen=True)
class Duplicate:
    """The hub's answer to an EA whose role and symbol another EA holds; it closes after it."""

    chart_role: ChartRole
    symbol: str


@dataclass(frozen=True)
class Ticks:
    """A symbol's ticks, each its MqlTick's fields."""

    symbol: str
    ticks: tuple[dict[str, object], ...]


@dataclass(frozen=True)
class RatesBar:
    """A closed bar as its EA publishes it: its MqlRates on the broker's clock."""

    symbol: str
    timeframe: int
    rates: dict[str, object]


@dataclass(frozen=True)
class Bar:
    """A closed bar as a consumer receives it: its MqlRates and its close in true UTC."""

    symbol: str
    series: Series
    bar: dict[str, object]


@dataclass(frozen=True)
class TradeTransaction:
    """The three structs one OnTradeTransaction call receives."""

    transaction: dict[str, object]
    request: dict[str, object]
    result: dict[str, object]


@dataclass(frozen=True)
class ServerTime:
    """The trade server's time as an EA reads it, on the broker's clock."""

    symbol: str
    trade_server: int
    current: int
    gmt: int
    connected: bool


@dataclass(frozen=True)
class Commissions:
    """A symbol's SymbolInfoCommissions answer: `ret`, `last_error` and its `rules`."""

    symbol: str
    schedule: dict[str, object]


@dataclass(frozen=True)
class Sessions:
    """A symbol's quote and trade sessions per weekday, and the error their reads left."""

    symbol: str
    sessions: dict[str, object]


@dataclass(frozen=True)
class Request:
    """A call asked under a correlation id; its arguments are the registry's to decode."""

    request_id: int
    call: int
    count: int
    arguments: bytes


@dataclass(frozen=True)
class AnswerError:
    """Why an answer carries no result; `retry_after_ms` is set for a deferring kind alone."""

    kind: ErrorKind
    message: str
    retry_after_ms: int | None = None


@dataclass(frozen=True)
class Answer:
    """One part of a request's answer: its result's records, the registry's to decode, or an error,
    which is always the last part."""

    request_id: int
    part: int
    last: bool
    last_error: int
    count: int = 0
    result: bytes = b""
    error: AnswerError | None = None


@dataclass(frozen=True)
class Busy:
    """An EA's word that it is about to wait on a request, for at most `bound_s`."""

    request_id: int
    bound_s: int


Frame = (
    AdapterHello
    | EaHello
    | Subscribe
    | Unsubscribe
    | Ack
    | Refusal
    | Wanted
    | OpenChart
    | CloseChart
    | ChartOpened
    | ChartFailed
    | ChartKept
    | Duplicate
    | Ticks
    | RatesBar
    | Bar
    | TradeTransaction
    | ServerTime
    | Commissions
    | Sessions
    | Request
    | Answer
    | Busy
)

# The kinds the hub and its consumers decode, by who sends them: an EA alone reads the frames the
# hub sends it.
_SENT_BY: dict[Sender, frozenset[FrameKind]] = {
    Sender.EA: frozenset(
        {
            FrameKind.HELLO,
            FrameKind.CHART_OPENED,
            FrameKind.CHART_FAILED,
            FrameKind.CHART_KEPT,
            FrameKind.TICK,
            FrameKind.BAR,
            FrameKind.TRADE_TRANSACTION,
            FrameKind.SERVER_TIME,
            FrameKind.COMMISSIONS,
            FrameKind.SESSIONS,
            FrameKind.ANSWER,
            FrameKind.BUSY,
        }
    ),
    Sender.ADAPTER: frozenset(
        {FrameKind.HELLO, FrameKind.SUBSCRIBE, FrameKind.UNSUBSCRIBE, FrameKind.REQUEST}
    ),
    Sender.HUB: frozenset(
        {
            FrameKind.ACK,
            FrameKind.ERROR,
            FrameKind.TICK,
            FrameKind.BAR,
            FrameKind.TRADE_TRANSACTION,
            FrameKind.ANSWER,
            FrameKind.BUSY,
        }
    ),
}

# ── Primitives ────────────────────────────────────────────────────────────────


class Reader:
    """Reads a payload's fields in order, raising CodecError naming the frame offset where one does
    not fit."""

    def __init__(self, data: bytes, offset: int = 0, end: int | None = None) -> None:
        self._data = data
        self.offset = offset
        self._end = len(data) if end is None else end

    def unpack(self, layout_: struct.Struct, what: str) -> tuple:
        if self.offset + layout_.size > self._end:
            raise CodecError(f"offset {self.offset}: {what} runs past the payload")
        values = layout_.unpack_from(self._data, self.offset)
        self.offset += layout_.size
        return values

    def u8(self, what: str) -> int:
        return self.unpack(_U8, what)[0]

    def u16(self, what: str) -> int:
        return self.unpack(_U16, what)[0]

    def u32(self, what: str) -> int:
        return self.unpack(_U32, what)[0]

    def i32(self, what: str) -> int:
        return self.unpack(_I32, what)[0]

    def i64(self, what: str) -> int:
        return self.unpack(_I64, what)[0]

    def f64(self, what: str) -> float:
        return self.unpack(_F64, what)[0]

    def boolean(self, what: str) -> bool:
        at = self.offset
        value = self.u8(what)
        if value not in (0, 1):
            raise CodecError(f"offset {at}: {what} {value} is not 0 or 1")
        return value == 1

    def string(self, what: str) -> str:
        length = self.u16(what)
        at = self.offset
        if at + length > self._end:
            raise CodecError(f"offset {at}: {what} runs past the payload")
        raw = self._data[at : at + length]
        self.offset += length
        try:
            return bytes(raw).decode("utf-8")
        except UnicodeDecodeError:
            raise CodecError(f"offset {at}: {what} is not UTF-8") from None

    def member(self, enum: type, what: str) -> object:
        """A member of a StrEnum, read by its value."""
        at = self.offset
        value = self.string(what)
        try:
            return enum(value)
        except ValueError:
            raise CodecError(f"offset {at}: {what} {value!r} is not a {enum.__name__}") from None

    def raw(self, length: int, what: str) -> bytes:
        if self.offset + length > self._end:
            raise CodecError(f"offset {self.offset}: {what} runs past the payload")
        raw = bytes(self._data[self.offset : self.offset + length])
        self.offset += length
        return raw

    def rest(self) -> bytes:
        return self.raw(self._end - self.offset, "the rest")

    def done(self) -> None:
        if self.offset != self._end:
            raise CodecError(f"offset {self.offset}: {self._end - self.offset} bytes past the end")


class Writer:
    """Packs a payload's fields in order."""

    def __init__(self) -> None:
        self._parts: list[bytes] = []

    def pack(self, layout_: struct.Struct, *values: object) -> None:
        self._parts.append(layout_.pack(*values))

    def u8(self, value: int) -> None:
        self.pack(_U8, value)

    def u16(self, value: int) -> None:
        self.pack(_U16, value)

    def u32(self, value: int) -> None:
        self.pack(_U32, value)

    def i32(self, value: int) -> None:
        self.pack(_I32, value)

    def i64(self, value: int) -> None:
        self.pack(_I64, value)

    def f64(self, value: float) -> None:
        self.pack(_F64, value)

    def boolean(self, value: bool) -> None:
        self.pack(_U8, int(value))

    def string(self, value: str) -> None:
        raw = value.encode("utf-8")
        if len(raw) > 0xFFFF:
            raise CodecError(f"a string of {len(raw)} bytes does not fit its length")
        self._parts.append(_U16.pack(len(raw)))
        self._parts.append(raw)

    def raw(self, value: bytes) -> None:
        self._parts.append(value)

    def payload(self) -> bytes:
        return b"".join(self._parts)


def write_record(writer: Writer, layout_: Layout, values: dict[str, object]) -> None:
    """Packs one record of a layout: its key string, then each typed member by its accessor."""
    if layout_.key is not None:
        writer.string(values[layout_.key])
    for member in layout_.members:
        if member.declared is not None:
            value = values[member.name]
            kind = mirror.accessor(member.declared)
            if kind is mirror.Accessor.STRING:
                writer.string(value)
            elif member.declared is mirror.MqlType.BOOL:
                writer.i64(int(value))
            else:
                writer.pack(_ACCESSOR_CODES[kind], value)


def read_record(reader: Reader, layout_: Layout) -> dict[str, object]:
    """Reads one record of a layout, each value typed as MQL5 declares it: a bool as a bool."""
    values: dict[str, object] = {}
    if layout_.key is not None:
        values[layout_.key] = reader.string(f"{layout_.name}.{layout_.key}")
    for member in layout_.members:
        if member.declared is not None:
            what = f"{layout_.name}.{member.name}"
            kind = mirror.accessor(member.declared)
            if kind is mirror.Accessor.STRING:
                values[member.name] = reader.string(what)
            elif kind is mirror.Accessor.DOUBLE:
                values[member.name] = reader.f64(what)
            elif member.declared is mirror.MqlType.BOOL:
                at = reader.offset
                value = reader.i64(what)
                if value not in (0, 1):
                    raise CodecError(f"offset {at}: {what} {value} is not 0 or 1")
                values[member.name] = value == 1
            else:
                values[member.name] = reader.i64(what)
    return {name: values[name] for name in layout_.fields}


def write_pod(writer: Writer, layout_: struct.Struct, array: mirror.Array, row: dict) -> None:
    writer.pack(layout_, *(row[name] for name in array.names))


def read_pod(reader: Reader, layout_: struct.Struct, array: mirror.Array) -> dict[str, object]:
    return dict(zip(array.names, reader.unpack(layout_, array.name), strict=True))


def write_transaction(writer: Writer, transaction: TradeTransaction) -> None:
    """Packs the three structs one OnTradeTransaction call receives, back to back."""
    write_record(writer, TRADE_TRANSACTION, transaction.transaction)
    write_record(writer, TRADE_REQUEST, transaction.request)
    write_record(writer, TRADE_RESULT, transaction.result)


def read_transaction(reader: Reader) -> TradeTransaction:
    return TradeTransaction(
        read_record(reader, TRADE_TRANSACTION),
        read_record(reader, TRADE_REQUEST),
        read_record(reader, TRADE_RESULT),
    )


# ── The commission and sessions bodies ───────────────────────────────────────

_RULE_NAMES = (
    ("mode_range", CommissionRangeMode),
    ("mode_charge", CommissionChargeMode),
    ("mode_entry", CommissionEntryMode),
    ("mode_direction", CommissionDirectionMode),
    ("mode_profit", CommissionProfitMode),
)
_TIER_NAMES = (("mode", CommissionMode), ("volume_type", CommissionVolumeType))
_TIER_NUMBERS = ("value", "min_value", "max_value", "range_from", "range_to")


def write_schedule(writer: Writer, schedule: dict) -> None:
    """Packs a commission schedule: `ret`, `last_error`, then each rule with its enum members by
    their EnumToString names and its tiers."""
    writer.i32(schedule["ret"])
    writer.i32(schedule["last_error"])
    writer.u16(len(schedule["rules"]))
    for rule in schedule["rules"]:
        writer.string(rule["currency"])
        for name, _ in _RULE_NAMES:
            writer.string(rule[name])
        writer.u16(len(rule["tiers"]))
        for tier in rule["tiers"]:
            for name, _ in _TIER_NAMES:
                writer.string(tier[name])
            for name in _TIER_NUMBERS:
                writer.f64(tier[name])
            writer.string(tier["currency"])


def read_schedule(reader: Reader) -> dict[str, object]:
    """Reads a commission schedule, each name as the member it names; a name no member of its enum
    holds is a CodecError."""
    ret = reader.i32("ret")
    last_error = reader.i32("last_error")
    rules = []
    for index in range(reader.u16("rules")):
        where = f"rules[{index}]"
        rule: dict[str, object] = {"currency": reader.string(f"{where}.currency")}
        for name, enum in _RULE_NAMES:
            rule[name] = reader.member(enum, f"{where}.{name}")
        tiers = []
        for tier_index in range(reader.u16(f"{where}.tiers")):
            at = f"{where}.tiers[{tier_index}]"
            tier: dict[str, object] = {}
            for name, enum in _TIER_NAMES:
                tier[name] = reader.member(enum, f"{at}.{name}")
            for name in _TIER_NUMBERS:
                tier[name] = reader.f64(f"{at}.{name}")
            tier["currency"] = reader.string(f"{at}.currency")
            tiers.append(tier)
        rule["tiers"] = tiers
        rules.append(rule)
    return {"ret": ret, "last_error": last_error, "rules": rules}


def write_sessions(writer: Writer, sessions: dict) -> None:
    """Packs quote and trade sessions, seven days each, Sunday first, then `last_error`."""
    for week in ("quote", "trade"):
        if len(sessions[week]) != _DAYS:
            raise CodecError(f"{week} holds {len(sessions[week])} days, not {_DAYS}")
        for day in sessions[week]:
            writer.u16(len(day))
            for start, end in day:
                writer.i32(start)
                writer.i32(end)
    writer.i32(sessions["last_error"])


def read_sessions(reader: Reader) -> dict[str, object]:
    weeks: dict[str, object] = {}
    for week in ("quote", "trade"):
        days = []
        for day_index in range(_DAYS):
            where = f"{week}[{day_index}]"
            days.append(
                [
                    [reader.i32(f"{where} from"), reader.i32(f"{where} to")]
                    for _ in range(reader.u16(where))
                ]
            )
        weeks[week] = days
    return weeks | {"last_error": reader.i32("last_error")}


# ── The codec ─────────────────────────────────────────────────────────────────


def encode(frame: Frame) -> bytes:
    """The bytes of a frame."""
    writer = Writer()
    flags = 0
    correlation = 0
    count = 0
    if isinstance(frame, AdapterHello):
        kind = FrameKind.HELLO
        writer.string(Role.ADAPTER)
    elif isinstance(frame, Subscribe):
        kind = FrameKind.SUBSCRIBE
        correlation = frame.op_id
        _write_subscription(writer, frame.subscription)
    elif isinstance(frame, Unsubscribe):
        kind = FrameKind.UNSUBSCRIBE
        correlation = frame.op_id
        _write_subscription(writer, frame.subscription)
    elif isinstance(frame, Ack):
        kind = FrameKind.ACK
        correlation = frame.op_id
    elif isinstance(frame, Refusal):
        kind = FrameKind.ERROR
        writer.string(frame.message)
    elif isinstance(frame, Wanted):
        kind = FrameKind.WANTED
        count = len(frame.timeframes)
        writer.string(frame.symbol)
        writer.boolean(frame.ticks)
        for timeframe in frame.timeframes:
            writer.i32(timeframe)
    elif isinstance(frame, OpenChart):
        kind = FrameKind.OPEN_CHART
        _write_chart(writer, frame.chart_role, frame.symbol)
    elif isinstance(frame, CloseChart):
        kind = FrameKind.CLOSE_CHART
        _write_chart(writer, frame.chart_role, frame.symbol)
    elif isinstance(frame, Duplicate):
        kind = FrameKind.DUPLICATE
        _write_chart(writer, frame.chart_role, frame.symbol)
    elif isinstance(frame, Ticks):
        kind = FrameKind.TICK
        count = len(frame.ticks)
        writer.string(frame.symbol)
        for tick in frame.ticks:
            write_pod(writer, TICK, mirror.TICKS, tick)
    elif isinstance(frame, Bar):
        kind = FrameKind.BAR
        writer.string(frame.symbol)
        writer.string(frame.series)
        write_pod(writer, BAR, BARS, frame.bar)
    elif isinstance(frame, TradeTransaction):
        kind = FrameKind.TRADE_TRANSACTION
        write_transaction(writer, frame)
    elif isinstance(frame, Request):
        kind = FrameKind.REQUEST
        correlation = frame.request_id
        count = frame.count
        writer.u16(frame.call)
        writer.raw(frame.arguments)
    elif isinstance(frame, Answer):
        kind = FrameKind.ANSWER
        correlation = frame.request_id
        writer.u32(frame.part)
        writer.i32(frame.last_error)
        if frame.last:
            flags |= Flag.LAST
        if frame.error is None:
            flags |= Flag.OK
            count = frame.count
            writer.raw(frame.result)
        else:
            _write_error(writer, frame.error)
    elif isinstance(frame, Busy):
        kind = FrameKind.BUSY
        correlation = frame.request_id
        writer.u32(frame.bound_s)
    else:
        raise TypeError(f"not a frame: {frame!r}")
    payload = writer.payload()
    return HEADER.pack(VERSION, kind, flags, correlation, count, len(payload)) + payload


def decode(data: bytes, sender: Sender) -> Frame:
    """The frame `sender` sent as `data`. Raises UnknownKind for a well-formed header of a kind no
    member names, and CodecError, naming the offset, for anything else out of its layout, a kind no
    frame from `sender` takes among it."""
    if len(data) < HEADER.size:
        raise CodecError(f"offset 0: {len(data)} bytes hold no header")
    version, code, flags, correlation, count, length = HEADER.unpack_from(data)
    if version != VERSION:
        raise CodecError(f"offset 0: version {version} is not {VERSION}")
    try:
        kind = FrameKind(code)
    except ValueError:
        raise UnknownKind(f"offset 1: unknown kind {code}") from None
    try:
        if HEADER.size + length != len(data):
            raise CodecError(f"offset 16: a payload of {length} bytes in a frame of {len(data)}")
        elif kind not in _SENT_BY[sender]:
            raise CodecError(f"offset 1: a {kind.name.lower()} frame from the {sender}")
        elif kind is not FrameKind.ANSWER and flags != 0:
            raise CodecError(f"offset 2: flags {flags:#x} on a {kind.name.lower()} frame")
        reader = Reader(data, HEADER.size)
        frame = _payload(reader, kind, flags, correlation, count, sender)
        reader.done()
    except CodecError as failure:
        raise CodecError(str(failure), kind, correlation) from None
    return frame


def decode_hello(data: bytes) -> AdapterHello | EaHello:
    """A connection's first frame, its hello; raises CodecError for any other frame."""
    if len(data) < HEADER.size:
        raise CodecError(f"offset 0: {len(data)} bytes hold no header")
    elif data[0] != VERSION:
        raise CodecError(f"offset 0: version {data[0]} is not {VERSION}")
    code = data[1]
    if code != FrameKind.HELLO:
        raise CodecError(f"offset 1: a frame of kind {code} before the connection's hello")
    reader = Reader(data, HEADER.size)
    role = reader.member(Role, "role")
    if role is Role.EA:
        return decode(data, Sender.EA)
    else:
        return decode(data, Sender.ADAPTER)


def _payload(
    reader: Reader, kind: FrameKind, flags: int, correlation: int, count: int, sender: Sender
) -> Frame:
    if kind not in _COUNTED and count != 0:
        raise CodecError(f"offset 12: a count of {count} on a {kind.name.lower()} frame")
    if kind is FrameKind.HELLO:
        role = reader.member(Role, "role")
        if role is Role.EA and sender is Sender.EA:
            frame = EaHello(reader.member(ChartRole, "chart_role"), _symbol(reader))
        elif role is Role.ADAPTER and sender is Sender.ADAPTER:
            frame = AdapterHello()
        else:
            raise CodecError(f"offset {HEADER.size}: an {role} hello from the {sender}")
    elif kind is FrameKind.SUBSCRIBE:
        frame = Subscribe(correlation, _read_subscription(reader))
    elif kind is FrameKind.UNSUBSCRIBE:
        frame = Unsubscribe(correlation, _read_subscription(reader))
    elif kind is FrameKind.ACK:
        frame = Ack(correlation)
    elif kind is FrameKind.ERROR:
        frame = Refusal(reader.string("message"))
    elif kind is FrameKind.CHART_OPENED:
        frame = ChartOpened(*_read_chart(reader))
    elif kind is FrameKind.CHART_FAILED:
        frame = ChartFailed(*_read_chart(reader), reader.i32("error"))
    elif kind is FrameKind.CHART_KEPT:
        frame = ChartKept(*_read_chart(reader), reader.i32("positions"), reader.i32("orders"))
    elif kind is FrameKind.TICK:
        symbol = _symbol(reader)
        frame = Ticks(symbol, tuple(read_pod(reader, TICK, mirror.TICKS) for _ in range(count)))
    elif kind is FrameKind.BAR and sender is Sender.EA:
        symbol = _symbol(reader)
        timeframe = reader.i32("timeframe")
        frame = RatesBar(symbol, timeframe, read_pod(reader, RATES, mirror.RATES))
    elif kind is FrameKind.BAR:
        symbol = _symbol(reader)
        series = reader.member(Series, "series")
        if series is Series.TICKS:
            raise CodecError(f"offset {reader.offset}: a bar of the {series} series")
        frame = Bar(symbol, series, read_pod(reader, BAR, BARS))
    elif kind is FrameKind.TRADE_TRANSACTION:
        frame = read_transaction(reader)
    elif kind is FrameKind.SERVER_TIME:
        frame = ServerTime(
            _symbol(reader),
            reader.i64("trade_server"),
            reader.i64("current"),
            reader.i64("gmt"),
            reader.boolean("connected"),
        )
    elif kind is FrameKind.COMMISSIONS:
        frame = Commissions(_symbol(reader), read_schedule(reader))
    elif kind is FrameKind.SESSIONS:
        frame = Sessions(_symbol(reader), read_sessions(reader))
    elif kind is FrameKind.REQUEST:
        frame = Request(correlation, reader.u16("call"), count, reader.rest())
    elif kind is FrameKind.ANSWER:
        frame = _answer(reader, flags, correlation, count, sender)
    else:
        # BUSY, the one kind of any sender's left.
        frame = Busy(correlation, reader.u32("bound_s"))
    return frame


def _answer(reader: Reader, flags: int, correlation: int, count: int, sender: Sender) -> Answer:
    if flags & ~int(Flag.OK | Flag.LAST):
        raise CodecError(f"offset 2: unknown answer flags {flags:#x}")
    part = reader.u32("part")
    last_error = reader.i32("last_error")
    last = bool(flags & Flag.LAST)
    if flags & Flag.OK:
        return Answer(correlation, part, last, last_error, count, reader.rest())
    elif not last:
        raise CodecError("offset 2: an error answer that is not the last part")
    elif count != 0:
        raise CodecError(f"offset 12: a count of {count} on an error answer")
    else:
        at = reader.offset
        code = reader.u8("error kind")
        try:
            error_kind = ErrorKind(code)
        except ValueError:
            raise CodecError(f"offset {at}: unknown error kind {code}") from None
        if sender is Sender.EA and error_kind not in EA_ERRORS:
            raise CodecError(f"offset {at}: an EA answering {error_kind.name.lower()}")
        message = reader.string("message")
        if error_kind in DEFERRING:
            error = AnswerError(error_kind, message, reader.u32("retry_after_ms"))
        else:
            error = AnswerError(error_kind, message)
        return Answer(correlation, part, last, last_error, error=error)


def _write_error(writer: Writer, error: AnswerError) -> None:
    writer.u8(error.kind)
    writer.string(error.message)
    if error.kind in DEFERRING:
        writer.u32(error.retry_after_ms)
    elif error.retry_after_ms is not None:
        raise CodecError(f"a retry delay on a {error.kind.name.lower()} answer")


def _write_subscription(writer: Writer, subscription: Subscription) -> None:
    writer.string(subscription.stream)
    if subscription.stream is Stream.TICKS:
        writer.string(subscription.symbol)
    elif subscription.stream is Stream.BARS:
        writer.string(subscription.symbol)
        writer.string(subscription.timeframe)


def _read_subscription(reader: Reader) -> Subscription:
    at = reader.offset
    stream = reader.member(Stream, "stream")
    symbol = None
    timeframe = None
    if stream is Stream.TICKS:
        symbol = reader.string("symbol")
    elif stream is Stream.BARS:
        symbol = reader.string("symbol")
        timeframe = reader.member(Series, "timeframe")
    try:
        return Subscription(stream, symbol, timeframe)
    except ValueError as failure:
        raise CodecError(f"offset {at}: {failure}") from None


def _symbol(reader: Reader) -> str:
    at = reader.offset
    symbol = reader.string("symbol")
    if symbol == "":
        raise CodecError(f"offset {at}: an empty symbol")
    return symbol


def _write_chart(writer: Writer, chart_role: ChartRole, symbol: str) -> None:
    writer.string(chart_role)
    writer.string(symbol)


def _read_chart(reader: Reader) -> tuple[ChartRole, str]:
    return reader.member(ChartRole, "chart_role"), _symbol(reader)


# The kinds whose header counts their payload's records.
_COUNTED = frozenset({FrameKind.TICK, FrameKind.REQUEST, FrameKind.ANSWER})
