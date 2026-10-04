"""The push protocol's vocabulary, shared by the hub, its consumers and the server: its frames,
roles, streams and chart route, and the MQL5 trade-transaction names its frames carry."""

from dataclasses import dataclass
from enum import IntEnum, StrEnum

from . import mirror
from .history_wire import Series

PROTOCOL_VERSION = 1

# The hub pings every peer at this interval, and drops one that leaves this many pings in a row
# without a pong.
PING_INTERVAL_S = 10
MISSED_PONGS = 3

# The hub's loopback route the server posts a symbol it serves a read of to.
CHARTS_PATH = "/charts"


class FrameType(StrEnum):
    """The `type` of a frame."""

    HELLO = "hello"
    SUBSCRIBE = "subscribe"
    UNSUBSCRIBE = "unsubscribe"
    ACK = "ack"
    ERROR = "error"
    WANTED = "wanted"
    OPEN_CHART = "open_chart"
    CLOSE_CHART = "close_chart"
    CHART_OPENED = "chart_opened"
    CHART_FAILED = "chart_failed"
    CHART_KEPT = "chart_kept"
    DUPLICATE = "duplicate"
    TICK = "tick"
    BAR = "bar"
    TRADE_TRANSACTION = "trade_transaction"
    SERVER_TIME = "server_time"
    COMMISSIONS = "commissions"


class Role(StrEnum):
    """What a connection is to the hub, as its hello declares: an EA publishes, an adapter
    consumes."""

    EA = "ea"
    ADAPTER = "adapter"


class ChartState(StrEnum):
    """The hub's answer to a post naming a symbol: an EA publishes it, its chart is requested, or
    the spawner failed to open its chart."""

    PUBLISHED = "published"
    REQUESTED = "requested"
    FAILED = "failed"


class Stream(StrEnum):
    """What a consumer subscribes to."""

    TICKS = "ticks"
    BARS = "bars"
    TRADE_TRANSACTIONS = "trade_transactions"


class TransactionType(IntEnum):
    """MQL5's ENUM_TRADE_TRANSACTION_TYPE. The reference names its members without their values;
    these are the values the terminal's compiler assigns them."""

    ORDER_ADD = 0
    ORDER_UPDATE = 1
    ORDER_DELETE = 2
    HISTORY_ADD = 3
    HISTORY_UPDATE = 4
    HISTORY_DELETE = 5
    DEAL_ADD = 6
    DEAL_UPDATE = 7
    DEAL_DELETE = 8
    POSITION = 9
    REQUEST = 10


# MqlTradeTransaction's fields, and those holding an epoch on the broker's clock.
TRANSACTION_FIELDS = (
    "deal",
    "order",
    "symbol",
    "type",
    "order_type",
    "order_state",
    "deal_type",
    "time_type",
    "time_expiration",
    "price",
    "price_trigger",
    "price_sl",
    "price_tp",
    "volume",
    "position",
    "position_by",
)
TRANSACTION_EPOCHS = {"time_expiration": mirror.EpochUnit.SECONDS}

_SEND_RESULT = mirror.STRUCTS[mirror.StructName.ORDER_SEND_RESULT]
# MqlTradeResult: the package's OrderSendResult without the request the package appends to it.
RESULT_FIELDS = tuple(name for name in _SEND_RESULT.fields if name not in _SEND_RESULT.nested)


@dataclass(frozen=True)
class Subscription:
    """One stream a consumer subscribes to: ticks of a symbol, bars of a symbol's timeframe, or the
    account's trade transactions."""

    stream: Stream
    symbol: str | None = None
    timeframe: Series | None = None

    def __post_init__(self) -> None:
        symbol = isinstance(self.symbol, str) and self.symbol != ""
        timeframe = isinstance(self.timeframe, Series) and self.timeframe != Series.TICKS
        if self.stream == Stream.TICKS:
            named = symbol and self.timeframe is None
        elif self.stream == Stream.BARS:
            named = symbol and timeframe
        else:
            named = self.symbol is None and self.timeframe is None
        if not (isinstance(self.stream, Stream) and named):
            raise ValueError(f"not a subscription: {self}")

    def op(self, frame_type: FrameType, op_id: int) -> dict[str, object]:
        """The subscribe or unsubscribe frame of this subscription under the op id `op_id`."""
        frame: dict[str, object] = {
            "v": PROTOCOL_VERSION,
            "type": frame_type.value,
            "id": op_id,
            "stream": self.stream.value,
        }
        if self.symbol is not None:
            frame["symbol"] = self.symbol
        if self.timeframe is not None:
            frame["timeframe"] = self.timeframe.value
        return frame


def subscription_of(op: dict[str, object]) -> Subscription:
    """The subscription a subscribe or unsubscribe frame names; raises ValueError for one that names
    none."""
    unknown = sorted(set(op) - {"v", "type", "id", "stream", "symbol", "timeframe"})
    if unknown:
        raise ValueError(f"unknown field: {', '.join(unknown)}")
    symbol = op.get("symbol")
    if symbol is not None and not isinstance(symbol, str):
        raise ValueError(f"not a symbol: {symbol!r}")
    timeframe = op.get("timeframe")
    if timeframe is None:
        return Subscription(Stream(op.get("stream")), symbol)
    else:
        return Subscription(Stream(op.get("stream")), symbol, Series(timeframe))
