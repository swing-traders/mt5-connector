"""The hub protocol's vocabulary beside its frames, shared by the hub and its consumers: its roles,
streams and keepalive, the MQL5 names and structs its frames carry, and the chart events a publisher
exchanges with its history indicator."""

from dataclasses import dataclass
from enum import IntEnum, StrEnum

from . import mirror
from .history_wire import Series

# The hub pings every peer at this interval, one ping outstanding at a time; a ping that goes this
# many intervals without its pong fails the connection, dropped within the close timeout. Both wait
# on the hub's writes to the peer draining, so a peer that stops reading delays them.
PING_INTERVAL_S = 10
PING_TIMEOUT_INTERVALS = 3
CLOSE_TIMEOUT_S = 10

# The hub's liveness route.
HEALTH_PATH = "/health"
# The indicator a publisher attaches to its own chart to sync the bars it reads.
HISTORY_INDICATOR = "hub_history"


class Role(StrEnum):
    """What a connection is to the hub, as its hello declares: an EA publishes, an adapter
    consumes."""

    EA = "ea"
    ADAPTER = "adapter"


class ChartRole(StrEnum):
    """What an EA serves, as its chart's template sets it: the spawner the account and the charts, a
    publisher its symbol but its tick history, which a history EA serves."""

    SPAWNER = "spawner"
    PUBLISHER = "publisher"
    HISTORY = "history"


class HistoryEvent(IntEnum):
    """A chart event between a publisher and its history indicator, by the id EventChartCustom sends
    it under."""

    ASK_RATES = 1
    ASK_RATES_FROM = 2
    RATES_READY = 3
    FORGET = 4


class ChartState(StrEnum):
    """A chart at the hub: an EA serves it, it is requested, or the spawner failed to open it."""

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


# The enums of SymbolInfoCommissions' MqlCommission and MqlCommissionTier, as the MQL5 reference's
# SymbolInfoCommissions page lists them: each member by the name EnumToString gives it, since the
# reference names the members without their values.


class CommissionMode(StrEnum):
    """How a commission tier states its value: ENUM_SYMBOL_COMMISSION_MODE."""

    DISABLED = "SYMBOL_COMMISSION_DISABLED"
    MONEY_DEPOSIT = "SYMBOL_COMMISSION_MONEY_DEPOSIT"
    MONEY_SYMBOL_BASE = "SYMBOL_COMMISSION_MONEY_SYMBOL_BASE"
    MONEY_SYMBOL_PROFIT = "SYMBOL_COMMISSION_MONEY_SYMBOL_PROFIT"
    MONEY_SYMBOL_MARGIN = "SYMBOL_COMMISSION_MONEY_SYMBOL_MARGIN"
    PIPS = "SYMBOL_COMMISSION_PIPS"
    PERCENT = "SYMBOL_COMMISSION_PERCENT"
    MONEY_SPECIFIED = "SYMBOL_COMMISSION_MONEY_SPECIFIED"
    PERCENT_PROFIT = "SYMBOL_COMMISSION_PERCENT_PROFIT"


class CommissionVolumeType(StrEnum):
    """How a commission tier's value applies to a deal: ENUM_SYMBOL_COMMISSION_VOLUME_TYPE."""

    TRADE = "SYMBOL_COMMISSION_VOLUME_TYPE_TRADE"
    VOLUME = "SYMBOL_COMMISSION_VOLUME_TYPE_VOLUME"
    TURNOVER = "SYMBOL_COMMISSION_VOLUME_TYPE_TURNOVER"


class CommissionRangeMode(StrEnum):
    """What selects a commission rule's tier: ENUM_SYMBOL_COMMISSION_RANGE_MODE."""

    VOLUME = "SYMBOL_COMMISSION_RANGE_VOLUME"
    TURNOVER_MONEY = "SYMBOL_COMMISSION_RANGE_TURNOVER_MONEY"
    TURNOVER_VOLUME = "SYMBOL_COMMISSION_RANGE_TURNOVER_VOLUME"
    VALUE = "SYMBOL_COMMISSION_RANGE_VALUE"
    PROFIT = "SYMBOL_COMMISSION_RANGE_PROFIT"


class CommissionChargeMode(StrEnum):
    """When a commission rule charges: ENUM_SYMBOL_COMMISSION_CHARGE_MODE."""

    DAILY = "SYMBOL_COMMISSION_CHARGE_DAILY"
    MONTHLY = "SYMBOL_COMMISSION_CHARGE_MONTHLY"
    INSTANT = "SYMBOL_COMMISSION_CHARGE_INSTANT"


class CommissionEntryMode(StrEnum):
    """The legs a commission rule charges: ENUM_SYMBOL_COMMISSION_ENTRY_MODE."""

    INOUT = "SYMBOL_COMMISSION_ENTRY_INOUT"
    IN = "SYMBOL_COMMISSION_ENTRY_IN"
    OUT = "SYMBOL_COMMISSION_ENTRY_OUT"


class CommissionDirectionMode(StrEnum):
    """The deal directions a commission rule charges: ENUM_SYMBOL_COMMISSION_DIRECTION_MODE."""

    BOTH = "SYMBOL_COMMISSION_DIRECTION_BOTH"
    BUY = "SYMBOL_COMMISSION_DIRECTION_BUY"
    SELL = "SYMBOL_COMMISSION_DIRECTION_SELL"


class CommissionProfitMode(StrEnum):
    """The deal results a commission rule charges: ENUM_SYMBOL_COMMISSION_PROFIT_MODE."""

    ALL = "SYMBOL_COMMISSION_PROFIT_ALL"
    PROFIT = "SYMBOL_COMMISSION_PROFIT_PROFIT"
    LOSS = "SYMBOL_COMMISSION_PROFIT_LOSS"


# MqlTradeTransaction's members with their MQL5 types and the MQL5 field each is read by, from the
# reference's MqlTradeTransaction page, and those holding an epoch on the broker's clock.
TRANSACTION_MEMBERS = (
    mirror.Member("deal", mirror.MqlType.ULONG, "deal"),
    mirror.Member("order", mirror.MqlType.ULONG, "order"),
    mirror.Member("symbol", mirror.MqlType.STRING, "symbol"),
    mirror.Member("type", mirror.MqlType.ENUM_TRADE_TRANSACTION_TYPE, "type"),
    mirror.Member("order_type", mirror.MqlType.ENUM_ORDER_TYPE, "order_type"),
    mirror.Member("order_state", mirror.MqlType.ENUM_ORDER_STATE, "order_state"),
    mirror.Member("deal_type", mirror.MqlType.ENUM_DEAL_TYPE, "deal_type"),
    mirror.Member("time_type", mirror.MqlType.ENUM_ORDER_TYPE_TIME, "time_type"),
    mirror.Member("time_expiration", mirror.MqlType.DATETIME, "time_expiration"),
    mirror.Member("price", mirror.MqlType.DOUBLE, "price"),
    mirror.Member("price_trigger", mirror.MqlType.DOUBLE, "price_trigger"),
    mirror.Member("price_sl", mirror.MqlType.DOUBLE, "price_sl"),
    mirror.Member("price_tp", mirror.MqlType.DOUBLE, "price_tp"),
    mirror.Member("volume", mirror.MqlType.DOUBLE, "volume"),
    mirror.Member("position", mirror.MqlType.ULONG, "position"),
    mirror.Member("position_by", mirror.MqlType.ULONG, "position_by"),
)
TRANSACTION_EPOCHS = {"time_expiration": mirror.EpochUnit.SECONDS}


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
