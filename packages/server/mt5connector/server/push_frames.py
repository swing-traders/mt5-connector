"""The frames an EA publishes — ticks, closed bars and trade transactions — held to their MQL5
structs and passed on to consumers with every epoch in true UTC."""

from dataclasses import dataclass

from mt5connector.server.wire import mirror
from mt5connector.server.wire.broker_clock import BrokerClock
from mt5connector.server.wire.history_wire import BAR_TIMEFRAME, Series
from mt5connector.server.wire.push_wire import (
    RESULT_FIELDS,
    TRANSACTION_EPOCHS,
    TRANSACTION_FIELDS,
    FrameType,
    Stream,
    Subscription,
    TransactionType,
)

_TICK = mirror.STRUCTS[mirror.StructName.TICK]
_REQUEST = mirror.STRUCTS[mirror.StructName.TRADE_REQUEST]
_ENVELOPE = ("v", "type")
_SERIES = {timeframe: series for series, timeframe in BAR_TIMEFRAME.items()}


class FrameError(ValueError):
    """Raised for a published frame out of the shape its kind declares."""


@dataclass(frozen=True)
class Published:
    """A frame an EA published, as its consumers receive it."""

    subscription: Subscription
    # The symbol the frame is of: a tick's or a bar's, a request transaction's request's, any other
    # transaction's own; empty for a transaction that names none.
    symbol: str
    frame: dict[str, object]
    # The field and broker epoch, in seconds, of its first epoch in the broker's repeated hour.
    ambiguous: tuple[str, int] | None


def published(frame: dict[str, object], clock: BrokerClock) -> Published:
    """The tick, bar or trade-transaction frame an EA published, as its consumers receive it: its
    struct fields verbatim, every epoch in true UTC, and a bar's timeframe by its series name.
    Raises FrameError naming what breaks the shape, and for a frame of any other kind."""
    kind = frame.get("type")
    conversion = _Conversion(clock)
    if kind == FrameType.TICK:
        hold(frame, (*_ENVELOPE, "symbol", *_TICK.fields), kind)
        subscription = _subscription(kind, Stream.TICKS, frame["symbol"])
        symbol = frame["symbol"]
        passed = frame | conversion.epochs(_TICK.epochs, frame, kind)
    elif kind == FrameType.BAR:
        hold(frame, (*_ENVELOPE, "symbol", "timeframe", *mirror.RATES.names), kind)
        if frame["timeframe"] not in _SERIES:
            raise FrameError(f"{kind}: timeframe {frame['timeframe']!r} has no series")
        series = _SERIES[frame["timeframe"]]
        subscription = _subscription(kind, Stream.BARS, frame["symbol"], series)
        symbol = frame["symbol"]
        passed = frame | {"timeframe": series.value}
        passed |= conversion.epochs(mirror.RATES.epochs, frame, kind)
    elif kind == FrameType.TRADE_TRANSACTION:
        hold(frame, (*_ENVELOPE, "transaction", "request", "result"), kind)
        hold(frame["transaction"], TRANSACTION_FIELDS, f"{kind}: transaction")
        hold(frame["request"], _REQUEST.fields, f"{kind}: request")
        hold(frame["result"], RESULT_FIELDS, f"{kind}: result")
        subscription = Subscription(Stream.TRADE_TRANSACTIONS)
        transaction = frame["transaction"]
        request = frame["request"]
        # MQL5 fills only the type of a request transaction; its request carries the rest.
        if transaction["type"] == TransactionType.REQUEST:
            symbol = request["symbol"]
        else:
            symbol = transaction["symbol"]
        if not isinstance(symbol, str):
            raise FrameError(f"{kind}: not a symbol: {symbol!r}")
        passed = frame | {
            "transaction": transaction
            | conversion.epochs(TRANSACTION_EPOCHS, transaction, f"{kind}.transaction"),
            "request": request | conversion.epochs(_REQUEST.epochs, request, f"{kind}.request"),
        }
    else:
        raise FrameError(f"{kind} is not a published frame")
    return Published(subscription, symbol, passed, conversion.ambiguous)


class _Conversion:
    """One frame's epochs converted to true UTC; remembers the first it read in the broker's
    repeated hour."""

    def __init__(self, clock: BrokerClock) -> None:
        self._clock = clock
        self.ambiguous: tuple[str, int] | None = None

    def epochs(
        self, epochs: dict[str, mirror.EpochUnit], struct: dict[str, object], where: str
    ) -> dict[str, int]:
        converted = {}
        for name, unit in epochs.items():
            value = struct[name]
            if not (isinstance(value, int) and not isinstance(value, bool)):
                raise FrameError(f"{where}: {name} {value!r} is not an integer epoch")
            elif unit is mirror.EpochUnit.SECONDS:
                converted[name] = self._clock.to_utc(value)
                seconds = value
            else:
                converted[name] = self._clock.to_utc_msc(value)
                seconds = value // 1000
            if self.ambiguous is None and value != 0 and self._clock.is_ambiguous(seconds):
                self.ambiguous = (f"{where}.{name}", seconds)
        return converted


def hold(struct: object, fields: tuple[str, ...], where: str) -> None:
    """Raises FrameError, naming `where`, unless `struct` is an object of exactly these fields."""
    if not isinstance(struct, dict):
        raise FrameError(f"{where}: not an object: {struct!r}")
    missing = [name for name in fields if name not in struct]
    unknown = sorted(name for name in struct if name not in fields)
    if missing:
        raise FrameError(f"{where}: missing field: {', '.join(missing)}")
    elif unknown:
        raise FrameError(f"{where}: unknown field: {', '.join(unknown)}")


def _subscription(
    kind: str, stream: Stream, symbol: object, timeframe: Series | None = None
) -> Subscription:
    if not (isinstance(symbol, str) and symbol):
        raise FrameError(f"{kind}: not a symbol: {symbol!r}")
    return Subscription(stream, symbol, timeframe)
