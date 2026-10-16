"""The MetaTrader5 package's call surface at one release: its functions, the structs and arrays they
return with their epoch fields, and its constants."""

from dataclasses import dataclass, field
from enum import StrEnum

PACKAGE_VERSION = "5.0.6231"


class FunctionName(StrEnum):
    INITIALIZE = "initialize"
    SHUTDOWN = "shutdown"
    LOGIN = "login"
    VERSION = "version"
    TERMINAL_INFO = "terminal_info"
    ACCOUNT_INFO = "account_info"
    COPY_TICKS_FROM = "copy_ticks_from"
    COPY_TICKS_RANGE = "copy_ticks_range"
    COPY_RATES_FROM = "copy_rates_from"
    COPY_RATES_FROM_POS = "copy_rates_from_pos"
    COPY_RATES_RANGE = "copy_rates_range"
    POSITIONS_TOTAL = "positions_total"
    POSITIONS_GET = "positions_get"
    ORDERS_TOTAL = "orders_total"
    ORDERS_GET = "orders_get"
    HISTORY_ORDERS_TOTAL = "history_orders_total"
    HISTORY_ORDERS_GET = "history_orders_get"
    HISTORY_DEALS_TOTAL = "history_deals_total"
    HISTORY_DEALS_GET = "history_deals_get"
    ORDER_CHECK = "order_check"
    ORDER_SEND = "order_send"
    ORDER_CALC_MARGIN = "order_calc_margin"
    ORDER_CALC_PROFIT = "order_calc_profit"
    SYMBOL_INFO = "symbol_info"
    SYMBOL_INFO_TICK = "symbol_info_tick"
    SYMBOL_SELECT = "symbol_select"
    SYMBOLS_TOTAL = "symbols_total"
    SYMBOLS_GET = "symbols_get"
    MARKET_BOOK_ADD = "market_book_add"
    MARKET_BOOK_RELEASE = "market_book_release"
    MARKET_BOOK_GET = "market_book_get"
    LAST_ERROR = "last_error"


class StructName(StrEnum):
    TRADE_POSITION = "TradePosition"
    TRADE_ORDER = "TradeOrder"
    TRADE_DEAL = "TradeDeal"
    TRADE_REQUEST = "TradeRequest"
    ORDER_SEND_RESULT = "OrderSendResult"
    ORDER_CHECK_RESULT = "OrderCheckResult"
    TICK = "Tick"
    TERMINAL_INFO = "TerminalInfo"
    SYMBOL_INFO = "SymbolInfo"
    ACCOUNT_INFO = "AccountInfo"
    BOOK_INFO = "BookInfo"


class ArrayName(StrEnum):
    RATES = "rates"
    TICKS = "ticks"


class Calling(StrEnum):
    """How the package's C function takes its arguments."""

    NO_ARGS = "no_args"
    POSITIONAL = "positional"
    KEYWORDS = "keywords"


class ParamKind(StrEnum):
    INT = "int"
    STR = "str"
    FLOAT = "float"
    BOOL = "bool"
    DATETIME = "datetime"
    TIMEOUT_MS = "timeout_ms"
    REQUEST = "request"


class ResultKind(StrEnum):
    SCALAR = "scalar"
    BOOL = "bool"
    TUPLE = "tuple"
    STRUCT = "struct"
    STRUCTS = "structs"
    ARRAY = "array"
    NONE = "none"


class EpochUnit(StrEnum):
    SECONDS = "seconds"
    MILLISECONDS = "milliseconds"


class Failure(StrEnum):
    """What the package answers when a function fails."""

    NONE = "none"
    FALSE = "false"
    FALSE_IS_RESULT = "false_is_result"
    NEVER = "never"


@dataclass(frozen=True)
class Param:
    """One parameter; `named` marks the ones the package documents as keyword arguments."""

    name: str
    kind: ParamKind
    required: bool = False
    named: bool = False


@dataclass(frozen=True)
class Function:
    """One package function, as the server routes it and the remote shim calls it."""

    name: FunctionName
    calling: Calling
    params: tuple[Param, ...]
    result: ResultKind
    failure: Failure
    struct: StructName | None = None
    array: ArrayName | None = None
    # The documented call forms, of which a call carries at least one in full.
    forms: tuple[tuple[str, ...], ...] = ()
    # A call on the terminal session the server owns, which the server answers without the package.
    server_session: bool = False


class MqlType(StrEnum):
    """A struct member's type as MQL5 declares it."""

    BOOL = "bool"
    INT = "int"
    UINT = "uint"
    LONG = "long"
    ULONG = "ulong"
    DATETIME = "datetime"
    DOUBLE = "double"
    STRING = "string"
    ENUM_ACCOUNT_MARGIN_MODE = "ENUM_ACCOUNT_MARGIN_MODE"
    ENUM_ACCOUNT_STOPOUT_MODE = "ENUM_ACCOUNT_STOPOUT_MODE"
    ENUM_ACCOUNT_TRADE_MODE = "ENUM_ACCOUNT_TRADE_MODE"
    ENUM_DAY_OF_WEEK = "ENUM_DAY_OF_WEEK"
    ENUM_DEAL_ENTRY = "ENUM_DEAL_ENTRY"
    ENUM_DEAL_REASON = "ENUM_DEAL_REASON"
    ENUM_DEAL_TYPE = "ENUM_DEAL_TYPE"
    ENUM_ORDER_REASON = "ENUM_ORDER_REASON"
    ENUM_ORDER_STATE = "ENUM_ORDER_STATE"
    ENUM_ORDER_TYPE = "ENUM_ORDER_TYPE"
    ENUM_ORDER_TYPE_FILLING = "ENUM_ORDER_TYPE_FILLING"
    ENUM_ORDER_TYPE_TIME = "ENUM_ORDER_TYPE_TIME"
    ENUM_POSITION_REASON = "ENUM_POSITION_REASON"
    ENUM_POSITION_TYPE = "ENUM_POSITION_TYPE"
    ENUM_SYMBOL_CALC_MODE = "ENUM_SYMBOL_CALC_MODE"
    ENUM_SYMBOL_CHART_MODE = "ENUM_SYMBOL_CHART_MODE"
    ENUM_SYMBOL_OPTION_MODE = "ENUM_SYMBOL_OPTION_MODE"
    ENUM_SYMBOL_OPTION_RIGHT = "ENUM_SYMBOL_OPTION_RIGHT"
    ENUM_SYMBOL_ORDER_GTC_MODE = "ENUM_SYMBOL_ORDER_GTC_MODE"
    ENUM_SYMBOL_SWAP_MODE = "ENUM_SYMBOL_SWAP_MODE"
    ENUM_SYMBOL_TRADE_EXECUTION = "ENUM_SYMBOL_TRADE_EXECUTION"
    ENUM_SYMBOL_TRADE_MODE = "ENUM_SYMBOL_TRADE_MODE"
    ENUM_TRADE_REQUEST_ACTIONS = "ENUM_TRADE_REQUEST_ACTIONS"
    ENUM_TRADE_TRANSACTION_TYPE = "ENUM_TRADE_TRANSACTION_TYPE"


class Accessor(StrEnum):
    """The family of MQL5 functions that reads a member — *InfoInteger, *InfoDouble, *InfoString —
    which fixes how it travels in a hub frame: an int64, a float64, or a length-prefixed UTF-8
    string."""

    INTEGER = "integer"
    DOUBLE = "double"
    STRING = "string"


def accessor(declared: MqlType) -> Accessor:
    """The accessor family that reads a member of the declared type: every integer, bool, enum and
    datetime as an integer."""
    if declared is MqlType.DOUBLE:
        return Accessor.DOUBLE
    elif declared is MqlType.STRING:
        return Accessor.STRING
    else:
        return Accessor.INTEGER


@dataclass(frozen=True)
class Member:
    """A struct's field, its MQL5 type, and the MQL5 property an EA reads it by — an `*Info*` or
    `*Get*` property identifier, or the MQL5 struct's own field name; type and property are None for
    a field no MQL5 property backs, which travels in no hub frame's field sequence."""

    name: str
    declared: MqlType | None
    property: str | None = None


@dataclass(frozen=True)
class Struct:
    """A package struct; each member's type is the one the MQL5 reference declares for it."""

    name: StructName
    members: tuple[Member, ...]
    # The fields that hold another struct.
    nested: dict[str, StructName] = field(default_factory=dict)
    # The fields that hold an epoch on the broker's clock.
    epochs: dict[str, EpochUnit] = field(default_factory=dict)
    # The field naming the record: a hub frame carries it as a key string ahead of the typed
    # members.
    key: str | None = None

    @property
    def fields(self) -> tuple[str, ...]:
        return tuple(member.name for member in self.members)


@dataclass(frozen=True)
class Array:
    """A numpy structured array the package returns, as (field, numpy type) pairs in order."""

    name: ArrayName
    dtype: tuple[tuple[str, str], ...]
    # The fields that hold an epoch on the broker's clock.
    epochs: dict[str, EpochUnit] = field(default_factory=dict)

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(name for name, _ in self.dtype)


def is_failure(function: Function, value: object) -> bool:
    """Whether a value the package returned is its failure answer for this function."""
    if function.failure is Failure.NONE:
        return value is None
    elif function.failure is Failure.FALSE:
        return value is False
    else:
        return False


def failure_value(function: Function) -> bool | None:
    """The value the package answers when this function fails."""
    if function.failure in (Failure.FALSE, Failure.FALSE_IS_RESULT):
        return False
    else:
        return None


_SYMBOL = Param("symbol", ParamKind.STR, required=True)
_DATE_FROM = Param("date_from", ParamKind.DATETIME, required=True)
_DATE_TO = Param("date_to", ParamKind.DATETIME, required=True)
_HISTORY_QUERY = (
    Param("date_from", ParamKind.DATETIME),
    Param("date_to", ParamKind.DATETIME),
    Param("group", ParamKind.STR, named=True),
    Param("ticket", ParamKind.INT, named=True),
    Param("position", ParamKind.INT, named=True),
)
_HISTORY_FORMS = (("date_from", "date_to"), ("ticket",), ("position",))
_OPEN_QUERY = (
    Param("symbol", ParamKind.STR, named=True),
    Param("group", ParamKind.STR, named=True),
    Param("ticket", ParamKind.INT, named=True),
)
_REQUEST = Param("request", ParamKind.REQUEST, required=True)

FUNCTIONS: dict[FunctionName, Function] = {
    function.name: function
    for function in (
        Function(
            FunctionName.INITIALIZE,
            Calling.KEYWORDS,
            (
                Param("path", ParamKind.STR),
                Param("login", ParamKind.INT, named=True),
                Param("password", ParamKind.STR, named=True),
                Param("server", ParamKind.STR, named=True),
                Param("timeout", ParamKind.TIMEOUT_MS, named=True),
                Param("portable", ParamKind.BOOL, named=True),
            ),
            ResultKind.BOOL,
            Failure.FALSE,
        ),
        Function(
            FunctionName.SHUTDOWN,
            Calling.NO_ARGS,
            (),
            ResultKind.NONE,
            Failure.NEVER,
            server_session=True,
        ),
        Function(
            FunctionName.LOGIN,
            Calling.KEYWORDS,
            (
                Param("login", ParamKind.INT, required=True),
                Param("password", ParamKind.STR, named=True),
                Param("server", ParamKind.STR, named=True),
                Param("timeout", ParamKind.TIMEOUT_MS, named=True),
            ),
            ResultKind.BOOL,
            Failure.FALSE,
        ),
        Function(FunctionName.VERSION, Calling.NO_ARGS, (), ResultKind.TUPLE, Failure.NONE),
        Function(
            FunctionName.TERMINAL_INFO,
            Calling.NO_ARGS,
            (),
            ResultKind.STRUCT,
            Failure.NONE,
            struct=StructName.TERMINAL_INFO,
        ),
        Function(
            FunctionName.ACCOUNT_INFO,
            Calling.NO_ARGS,
            (),
            ResultKind.STRUCT,
            Failure.NONE,
            struct=StructName.ACCOUNT_INFO,
        ),
        Function(
            FunctionName.COPY_TICKS_FROM,
            Calling.POSITIONAL,
            (
                _SYMBOL,
                _DATE_FROM,
                Param("count", ParamKind.INT, required=True),
                Param("flags", ParamKind.INT, required=True),
            ),
            ResultKind.ARRAY,
            Failure.NONE,
            array=ArrayName.TICKS,
        ),
        Function(
            FunctionName.COPY_TICKS_RANGE,
            Calling.POSITIONAL,
            (_SYMBOL, _DATE_FROM, _DATE_TO, Param("flags", ParamKind.INT, required=True)),
            ResultKind.ARRAY,
            Failure.NONE,
            array=ArrayName.TICKS,
        ),
        Function(
            FunctionName.COPY_RATES_FROM,
            Calling.POSITIONAL,
            (
                _SYMBOL,
                Param("timeframe", ParamKind.INT, required=True),
                _DATE_FROM,
                Param("count", ParamKind.INT, required=True),
            ),
            ResultKind.ARRAY,
            Failure.NONE,
            array=ArrayName.RATES,
        ),
        Function(
            FunctionName.COPY_RATES_FROM_POS,
            Calling.POSITIONAL,
            (
                _SYMBOL,
                Param("timeframe", ParamKind.INT, required=True),
                Param("start_pos", ParamKind.INT, required=True),
                Param("count", ParamKind.INT, required=True),
            ),
            ResultKind.ARRAY,
            Failure.NONE,
            array=ArrayName.RATES,
        ),
        Function(
            FunctionName.COPY_RATES_RANGE,
            Calling.POSITIONAL,
            (_SYMBOL, Param("timeframe", ParamKind.INT, required=True), _DATE_FROM, _DATE_TO),
            ResultKind.ARRAY,
            Failure.NONE,
            array=ArrayName.RATES,
        ),
        Function(
            FunctionName.POSITIONS_TOTAL, Calling.NO_ARGS, (), ResultKind.SCALAR, Failure.NONE
        ),
        Function(
            FunctionName.POSITIONS_GET,
            Calling.KEYWORDS,
            _OPEN_QUERY,
            ResultKind.STRUCTS,
            Failure.NONE,
            struct=StructName.TRADE_POSITION,
        ),
        Function(FunctionName.ORDERS_TOTAL, Calling.NO_ARGS, (), ResultKind.SCALAR, Failure.NONE),
        Function(
            FunctionName.ORDERS_GET,
            Calling.KEYWORDS,
            _OPEN_QUERY,
            ResultKind.STRUCTS,
            Failure.NONE,
            struct=StructName.TRADE_ORDER,
        ),
        Function(
            FunctionName.HISTORY_ORDERS_TOTAL,
            Calling.POSITIONAL,
            (_DATE_FROM, _DATE_TO),
            ResultKind.SCALAR,
            Failure.NONE,
        ),
        Function(
            FunctionName.HISTORY_ORDERS_GET,
            Calling.KEYWORDS,
            _HISTORY_QUERY,
            ResultKind.STRUCTS,
            Failure.NONE,
            struct=StructName.TRADE_ORDER,
            forms=_HISTORY_FORMS,
        ),
        Function(
            FunctionName.HISTORY_DEALS_TOTAL,
            Calling.POSITIONAL,
            (_DATE_FROM, _DATE_TO),
            ResultKind.SCALAR,
            Failure.NONE,
        ),
        Function(
            FunctionName.HISTORY_DEALS_GET,
            Calling.KEYWORDS,
            _HISTORY_QUERY,
            ResultKind.STRUCTS,
            Failure.NONE,
            struct=StructName.TRADE_DEAL,
            forms=_HISTORY_FORMS,
        ),
        Function(
            FunctionName.ORDER_CHECK,
            Calling.KEYWORDS,
            (_REQUEST,),
            ResultKind.STRUCT,
            Failure.NONE,
            struct=StructName.ORDER_CHECK_RESULT,
        ),
        Function(
            FunctionName.ORDER_SEND,
            Calling.KEYWORDS,
            (_REQUEST,),
            ResultKind.STRUCT,
            Failure.NONE,
            struct=StructName.ORDER_SEND_RESULT,
        ),
        Function(
            FunctionName.ORDER_CALC_MARGIN,
            Calling.POSITIONAL,
            (
                Param("action", ParamKind.INT, required=True),
                _SYMBOL,
                Param("volume", ParamKind.FLOAT, required=True),
                Param("price", ParamKind.FLOAT, required=True),
            ),
            ResultKind.SCALAR,
            Failure.NONE,
        ),
        Function(
            FunctionName.ORDER_CALC_PROFIT,
            Calling.POSITIONAL,
            (
                Param("action", ParamKind.INT, required=True),
                _SYMBOL,
                Param("volume", ParamKind.FLOAT, required=True),
                Param("price_open", ParamKind.FLOAT, required=True),
                Param("price_close", ParamKind.FLOAT, required=True),
            ),
            ResultKind.SCALAR,
            Failure.NONE,
        ),
        Function(
            FunctionName.SYMBOL_INFO,
            Calling.POSITIONAL,
            (_SYMBOL,),
            ResultKind.STRUCT,
            Failure.NONE,
            struct=StructName.SYMBOL_INFO,
        ),
        Function(
            FunctionName.SYMBOL_INFO_TICK,
            Calling.POSITIONAL,
            (_SYMBOL,),
            ResultKind.STRUCT,
            Failure.NONE,
            struct=StructName.TICK,
        ),
        Function(
            FunctionName.SYMBOL_SELECT,
            Calling.POSITIONAL,
            (_SYMBOL, Param("enable", ParamKind.BOOL)),
            ResultKind.BOOL,
            Failure.FALSE_IS_RESULT,
        ),
        Function(FunctionName.SYMBOLS_TOTAL, Calling.NO_ARGS, (), ResultKind.SCALAR, Failure.NONE),
        Function(
            FunctionName.SYMBOLS_GET,
            Calling.KEYWORDS,
            (Param("group", ParamKind.STR, named=True),),
            ResultKind.STRUCTS,
            Failure.NONE,
            struct=StructName.SYMBOL_INFO,
        ),
        Function(
            FunctionName.MARKET_BOOK_ADD,
            Calling.POSITIONAL,
            (_SYMBOL,),
            ResultKind.BOOL,
            Failure.FALSE,
        ),
        Function(
            FunctionName.MARKET_BOOK_RELEASE,
            Calling.POSITIONAL,
            (_SYMBOL,),
            ResultKind.BOOL,
            Failure.FALSE,
        ),
        Function(
            FunctionName.MARKET_BOOK_GET,
            Calling.POSITIONAL,
            (_SYMBOL,),
            ResultKind.STRUCTS,
            Failure.NONE,
            struct=StructName.BOOK_INFO,
        ),
        Function(FunctionName.LAST_ERROR, Calling.NO_ARGS, (), ResultKind.TUPLE, Failure.NEVER),
    )
}

STRUCTS: dict[StructName, Struct] = {
    struct.name: struct
    for struct in (
        # https://www.mql5.com/en/docs/constants/tradingconstants/positionproperties
        Struct(
            StructName.TRADE_POSITION,
            (
                Member("ticket", MqlType.LONG, "POSITION_TICKET"),
                Member("time", MqlType.DATETIME, "POSITION_TIME"),
                Member("time_msc", MqlType.LONG, "POSITION_TIME_MSC"),
                Member("time_update", MqlType.DATETIME, "POSITION_TIME_UPDATE"),
                Member("time_update_msc", MqlType.LONG, "POSITION_TIME_UPDATE_MSC"),
                Member("type", MqlType.ENUM_POSITION_TYPE, "POSITION_TYPE"),
                Member("magic", MqlType.LONG, "POSITION_MAGIC"),
                Member("identifier", MqlType.LONG, "POSITION_IDENTIFIER"),
                Member("reason", MqlType.ENUM_POSITION_REASON, "POSITION_REASON"),
                Member("volume", MqlType.DOUBLE, "POSITION_VOLUME"),
                Member("price_open", MqlType.DOUBLE, "POSITION_PRICE_OPEN"),
                Member("sl", MqlType.DOUBLE, "POSITION_SL"),
                Member("tp", MqlType.DOUBLE, "POSITION_TP"),
                Member("price_current", MqlType.DOUBLE, "POSITION_PRICE_CURRENT"),
                Member("swap", MqlType.DOUBLE, "POSITION_SWAP"),
                Member("profit", MqlType.DOUBLE, "POSITION_PROFIT"),
                Member("symbol", MqlType.STRING, "POSITION_SYMBOL"),
                Member("comment", MqlType.STRING, "POSITION_COMMENT"),
                Member("external_id", MqlType.STRING, "POSITION_EXTERNAL_ID"),
            ),
            epochs={
                "time": EpochUnit.SECONDS,
                "time_msc": EpochUnit.MILLISECONDS,
                "time_update": EpochUnit.SECONDS,
                "time_update_msc": EpochUnit.MILLISECONDS,
            },
        ),
        # https://www.mql5.com/en/docs/constants/tradingconstants/orderproperties
        Struct(
            StructName.TRADE_ORDER,
            (
                Member("ticket", MqlType.LONG, "ORDER_TICKET"),
                Member("time_setup", MqlType.DATETIME, "ORDER_TIME_SETUP"),
                Member("time_setup_msc", MqlType.LONG, "ORDER_TIME_SETUP_MSC"),
                Member("time_done", MqlType.DATETIME, "ORDER_TIME_DONE"),
                Member("time_done_msc", MqlType.LONG, "ORDER_TIME_DONE_MSC"),
                Member("time_expiration", MqlType.DATETIME, "ORDER_TIME_EXPIRATION"),
                Member("type", MqlType.ENUM_ORDER_TYPE, "ORDER_TYPE"),
                Member("type_time", MqlType.ENUM_ORDER_TYPE_TIME, "ORDER_TYPE_TIME"),
                Member("type_filling", MqlType.ENUM_ORDER_TYPE_FILLING, "ORDER_TYPE_FILLING"),
                Member("state", MqlType.ENUM_ORDER_STATE, "ORDER_STATE"),
                Member("magic", MqlType.LONG, "ORDER_MAGIC"),
                Member("position_id", MqlType.LONG, "ORDER_POSITION_ID"),
                Member("position_by_id", MqlType.LONG, "ORDER_POSITION_BY_ID"),
                Member("reason", MqlType.ENUM_ORDER_REASON, "ORDER_REASON"),
                Member("volume_initial", MqlType.DOUBLE, "ORDER_VOLUME_INITIAL"),
                Member("volume_current", MqlType.DOUBLE, "ORDER_VOLUME_CURRENT"),
                Member("price_open", MqlType.DOUBLE, "ORDER_PRICE_OPEN"),
                Member("sl", MqlType.DOUBLE, "ORDER_SL"),
                Member("tp", MqlType.DOUBLE, "ORDER_TP"),
                Member("price_current", MqlType.DOUBLE, "ORDER_PRICE_CURRENT"),
                Member("price_stoplimit", MqlType.DOUBLE, "ORDER_PRICE_STOPLIMIT"),
                Member("symbol", MqlType.STRING, "ORDER_SYMBOL"),
                Member("comment", MqlType.STRING, "ORDER_COMMENT"),
                Member("external_id", MqlType.STRING, "ORDER_EXTERNAL_ID"),
            ),
            epochs={
                "time_setup": EpochUnit.SECONDS,
                "time_setup_msc": EpochUnit.MILLISECONDS,
                "time_done": EpochUnit.SECONDS,
                "time_done_msc": EpochUnit.MILLISECONDS,
                "time_expiration": EpochUnit.SECONDS,
            },
        ),
        # https://www.mql5.com/en/docs/constants/tradingconstants/dealproperties
        Struct(
            StructName.TRADE_DEAL,
            (
                Member("ticket", MqlType.LONG, "DEAL_TICKET"),
                Member("order", MqlType.LONG, "DEAL_ORDER"),
                Member("time", MqlType.DATETIME, "DEAL_TIME"),
                Member("time_msc", MqlType.LONG, "DEAL_TIME_MSC"),
                Member("type", MqlType.ENUM_DEAL_TYPE, "DEAL_TYPE"),
                Member("entry", MqlType.ENUM_DEAL_ENTRY, "DEAL_ENTRY"),
                Member("magic", MqlType.LONG, "DEAL_MAGIC"),
                Member("position_id", MqlType.LONG, "DEAL_POSITION_ID"),
                Member("reason", MqlType.ENUM_DEAL_REASON, "DEAL_REASON"),
                Member("volume", MqlType.DOUBLE, "DEAL_VOLUME"),
                Member("price", MqlType.DOUBLE, "DEAL_PRICE"),
                Member("commission", MqlType.DOUBLE, "DEAL_COMMISSION"),
                Member("swap", MqlType.DOUBLE, "DEAL_SWAP"),
                Member("profit", MqlType.DOUBLE, "DEAL_PROFIT"),
                Member("fee", MqlType.DOUBLE, "DEAL_FEE"),
                Member("symbol", MqlType.STRING, "DEAL_SYMBOL"),
                Member("comment", MqlType.STRING, "DEAL_COMMENT"),
                Member("external_id", MqlType.STRING, "DEAL_EXTERNAL_ID"),
            ),
            epochs={
                "time": EpochUnit.SECONDS,
                "time_msc": EpochUnit.MILLISECONDS,
            },
        ),
        # https://www.mql5.com/en/docs/constants/structures/mqltraderequest
        Struct(
            StructName.TRADE_REQUEST,
            (
                Member("action", MqlType.ENUM_TRADE_REQUEST_ACTIONS, "action"),
                Member("magic", MqlType.ULONG, "magic"),
                Member("order", MqlType.ULONG, "order"),
                Member("symbol", MqlType.STRING, "symbol"),
                Member("volume", MqlType.DOUBLE, "volume"),
                Member("price", MqlType.DOUBLE, "price"),
                Member("stoplimit", MqlType.DOUBLE, "stoplimit"),
                Member("sl", MqlType.DOUBLE, "sl"),
                Member("tp", MqlType.DOUBLE, "tp"),
                Member("deviation", MqlType.ULONG, "deviation"),
                Member("type", MqlType.ENUM_ORDER_TYPE, "type"),
                Member("type_filling", MqlType.ENUM_ORDER_TYPE_FILLING, "type_filling"),
                Member("type_time", MqlType.ENUM_ORDER_TYPE_TIME, "type_time"),
                Member("expiration", MqlType.DATETIME, "expiration"),
                Member("comment", MqlType.STRING, "comment"),
                Member("position", MqlType.ULONG, "position"),
                Member("position_by", MqlType.ULONG, "position_by"),
            ),
            epochs={
                "expiration": EpochUnit.SECONDS,
            },
        ),
        # https://www.mql5.com/en/docs/constants/structures/mqltraderesult
        Struct(
            StructName.ORDER_SEND_RESULT,
            (
                Member("retcode", MqlType.UINT, "retcode"),
                Member("deal", MqlType.ULONG, "deal"),
                Member("order", MqlType.ULONG, "order"),
                Member("volume", MqlType.DOUBLE, "volume"),
                Member("price", MqlType.DOUBLE, "price"),
                Member("bid", MqlType.DOUBLE, "bid"),
                Member("ask", MqlType.DOUBLE, "ask"),
                Member("comment", MqlType.STRING, "comment"),
                Member("request_id", MqlType.UINT, "request_id"),
                Member("retcode_external", MqlType.INT, "retcode_external"),
                Member("request", None),
            ),
            nested={"request": StructName.TRADE_REQUEST},
        ),
        Struct(
            StructName.ORDER_CHECK_RESULT,
            (
                Member("retcode", None),
                Member("balance", None),
                Member("equity", None),
                Member("profit", None),
                Member("margin", None),
                Member("margin_free", None),
                Member("margin_level", None),
                Member("comment", None),
                Member("request", None),
            ),
            nested={"request": StructName.TRADE_REQUEST},
        ),
        Struct(
            StructName.TICK,
            (
                Member("time", None),
                Member("bid", None),
                Member("ask", None),
                Member("last", None),
                Member("volume", None),
                Member("time_msc", None),
                Member("flags", None),
                Member("volume_real", None),
            ),
            epochs={
                "time": EpochUnit.SECONDS,
                "time_msc": EpochUnit.MILLISECONDS,
            },
        ),
        # https://www.mql5.com/en/docs/constants/environment_state/terminalstatus
        Struct(
            StructName.TERMINAL_INFO,
            (
                Member("community_account", MqlType.BOOL, "TERMINAL_COMMUNITY_ACCOUNT"),
                Member("community_connection", MqlType.BOOL, "TERMINAL_COMMUNITY_CONNECTION"),
                Member("connected", MqlType.BOOL, "TERMINAL_CONNECTED"),
                Member("dlls_allowed", MqlType.BOOL, "TERMINAL_DLLS_ALLOWED"),
                Member("trade_allowed", MqlType.BOOL, "TERMINAL_TRADE_ALLOWED"),
                Member("tradeapi_disabled", None),
                Member("email_enabled", MqlType.BOOL, "TERMINAL_EMAIL_ENABLED"),
                Member("ftp_enabled", MqlType.BOOL, "TERMINAL_FTP_ENABLED"),
                Member("notifications_enabled", MqlType.BOOL, "TERMINAL_NOTIFICATIONS_ENABLED"),
                Member("mqid", MqlType.BOOL, "TERMINAL_MQID"),
                Member("build", MqlType.INT, "TERMINAL_BUILD"),
                Member("maxbars", MqlType.INT, "TERMINAL_MAXBARS"),
                Member("codepage", MqlType.INT, "TERMINAL_CODEPAGE"),
                Member("ping_last", MqlType.INT, "TERMINAL_PING_LAST"),
                Member("community_balance", MqlType.DOUBLE, "TERMINAL_COMMUNITY_BALANCE"),
                Member("retransmission", MqlType.DOUBLE, "TERMINAL_RETRANSMISSION"),
                Member("company", MqlType.STRING, "TERMINAL_COMPANY"),
                Member("name", MqlType.STRING, "TERMINAL_NAME"),
                Member("language", MqlType.STRING, "TERMINAL_LANGUAGE"),
                Member("path", MqlType.STRING, "TERMINAL_PATH"),
                Member("data_path", MqlType.STRING, "TERMINAL_DATA_PATH"),
                Member("commondata_path", MqlType.STRING, "TERMINAL_COMMONDATA_PATH"),
            ),
        ),
        # https://www.mql5.com/en/docs/constants/environment_state/marketinfoconstants
        Struct(
            StructName.SYMBOL_INFO,
            (
                Member("custom", MqlType.BOOL, "SYMBOL_CUSTOM"),
                Member("chart_mode", MqlType.ENUM_SYMBOL_CHART_MODE, "SYMBOL_CHART_MODE"),
                Member("select", MqlType.BOOL, "SYMBOL_SELECT"),
                Member("visible", MqlType.BOOL, "SYMBOL_VISIBLE"),
                Member("session_deals", MqlType.LONG, "SYMBOL_SESSION_DEALS"),
                Member("session_buy_orders", MqlType.LONG, "SYMBOL_SESSION_BUY_ORDERS"),
                Member("session_sell_orders", MqlType.LONG, "SYMBOL_SESSION_SELL_ORDERS"),
                Member("volume", MqlType.LONG, "SYMBOL_VOLUME"),
                Member("volumehigh", MqlType.LONG, "SYMBOL_VOLUMEHIGH"),
                Member("volumelow", MqlType.LONG, "SYMBOL_VOLUMELOW"),
                Member("time", MqlType.DATETIME, "SYMBOL_TIME"),
                Member("digits", MqlType.INT, "SYMBOL_DIGITS"),
                Member("spread", MqlType.INT, "SYMBOL_SPREAD"),
                Member("spread_float", MqlType.BOOL, "SYMBOL_SPREAD_FLOAT"),
                Member("ticks_bookdepth", MqlType.INT, "SYMBOL_TICKS_BOOKDEPTH"),
                Member("trade_calc_mode", MqlType.ENUM_SYMBOL_CALC_MODE, "SYMBOL_TRADE_CALC_MODE"),
                Member("trade_mode", MqlType.ENUM_SYMBOL_TRADE_MODE, "SYMBOL_TRADE_MODE"),
                Member("start_time", MqlType.DATETIME, "SYMBOL_START_TIME"),
                Member("expiration_time", MqlType.DATETIME, "SYMBOL_EXPIRATION_TIME"),
                Member("trade_stops_level", MqlType.INT, "SYMBOL_TRADE_STOPS_LEVEL"),
                Member("trade_freeze_level", MqlType.INT, "SYMBOL_TRADE_FREEZE_LEVEL"),
                Member(
                    "trade_exemode", MqlType.ENUM_SYMBOL_TRADE_EXECUTION, "SYMBOL_TRADE_EXEMODE"
                ),
                Member("swap_mode", MqlType.ENUM_SYMBOL_SWAP_MODE, "SYMBOL_SWAP_MODE"),
                Member("swap_rollover3days", MqlType.ENUM_DAY_OF_WEEK, "SYMBOL_SWAP_ROLLOVER3DAYS"),
                Member("margin_hedged_use_leg", MqlType.BOOL, "SYMBOL_MARGIN_HEDGED_USE_LEG"),
                Member("expiration_mode", MqlType.INT, "SYMBOL_EXPIRATION_MODE"),
                Member("filling_mode", MqlType.INT, "SYMBOL_FILLING_MODE"),
                Member("order_mode", MqlType.INT, "SYMBOL_ORDER_MODE"),
                Member(
                    "order_gtc_mode", MqlType.ENUM_SYMBOL_ORDER_GTC_MODE, "SYMBOL_ORDER_GTC_MODE"
                ),
                Member("option_mode", MqlType.ENUM_SYMBOL_OPTION_MODE, "SYMBOL_OPTION_MODE"),
                Member("option_right", MqlType.ENUM_SYMBOL_OPTION_RIGHT, "SYMBOL_OPTION_RIGHT"),
                Member("bid", MqlType.DOUBLE, "SYMBOL_BID"),
                Member("bidhigh", MqlType.DOUBLE, "SYMBOL_BIDHIGH"),
                Member("bidlow", MqlType.DOUBLE, "SYMBOL_BIDLOW"),
                Member("ask", MqlType.DOUBLE, "SYMBOL_ASK"),
                Member("askhigh", MqlType.DOUBLE, "SYMBOL_ASKHIGH"),
                Member("asklow", MqlType.DOUBLE, "SYMBOL_ASKLOW"),
                Member("last", MqlType.DOUBLE, "SYMBOL_LAST"),
                Member("lasthigh", MqlType.DOUBLE, "SYMBOL_LASTHIGH"),
                Member("lastlow", MqlType.DOUBLE, "SYMBOL_LASTLOW"),
                Member("volume_real", MqlType.DOUBLE, "SYMBOL_VOLUME_REAL"),
                Member("volumehigh_real", MqlType.DOUBLE, "SYMBOL_VOLUMEHIGH_REAL"),
                Member("volumelow_real", MqlType.DOUBLE, "SYMBOL_VOLUMELOW_REAL"),
                Member("option_strike", MqlType.DOUBLE, "SYMBOL_OPTION_STRIKE"),
                Member("point", MqlType.DOUBLE, "SYMBOL_POINT"),
                Member("trade_tick_value", MqlType.DOUBLE, "SYMBOL_TRADE_TICK_VALUE"),
                Member("trade_tick_value_profit", MqlType.DOUBLE, "SYMBOL_TRADE_TICK_VALUE_PROFIT"),
                Member("trade_tick_value_loss", MqlType.DOUBLE, "SYMBOL_TRADE_TICK_VALUE_LOSS"),
                Member("trade_tick_size", MqlType.DOUBLE, "SYMBOL_TRADE_TICK_SIZE"),
                Member("trade_contract_size", MqlType.DOUBLE, "SYMBOL_TRADE_CONTRACT_SIZE"),
                Member("trade_accrued_interest", MqlType.DOUBLE, "SYMBOL_TRADE_ACCRUED_INTEREST"),
                Member("trade_face_value", MqlType.DOUBLE, "SYMBOL_TRADE_FACE_VALUE"),
                Member("trade_liquidity_rate", MqlType.DOUBLE, "SYMBOL_TRADE_LIQUIDITY_RATE"),
                Member("volume_min", MqlType.DOUBLE, "SYMBOL_VOLUME_MIN"),
                Member("volume_max", MqlType.DOUBLE, "SYMBOL_VOLUME_MAX"),
                Member("volume_step", MqlType.DOUBLE, "SYMBOL_VOLUME_STEP"),
                Member("volume_limit", MqlType.DOUBLE, "SYMBOL_VOLUME_LIMIT"),
                Member("swap_long", MqlType.DOUBLE, "SYMBOL_SWAP_LONG"),
                Member("swap_short", MqlType.DOUBLE, "SYMBOL_SWAP_SHORT"),
                Member("margin_initial", MqlType.DOUBLE, "SYMBOL_MARGIN_INITIAL"),
                Member("margin_maintenance", MqlType.DOUBLE, "SYMBOL_MARGIN_MAINTENANCE"),
                Member("session_volume", MqlType.DOUBLE, "SYMBOL_SESSION_VOLUME"),
                Member("session_turnover", MqlType.DOUBLE, "SYMBOL_SESSION_TURNOVER"),
                Member("session_interest", MqlType.DOUBLE, "SYMBOL_SESSION_INTEREST"),
                Member(
                    "session_buy_orders_volume", MqlType.DOUBLE, "SYMBOL_SESSION_BUY_ORDERS_VOLUME"
                ),
                Member(
                    "session_sell_orders_volume",
                    MqlType.DOUBLE,
                    "SYMBOL_SESSION_SELL_ORDERS_VOLUME",
                ),
                Member("session_open", MqlType.DOUBLE, "SYMBOL_SESSION_OPEN"),
                Member("session_close", MqlType.DOUBLE, "SYMBOL_SESSION_CLOSE"),
                Member("session_aw", MqlType.DOUBLE, "SYMBOL_SESSION_AW"),
                Member(
                    "session_price_settlement", MqlType.DOUBLE, "SYMBOL_SESSION_PRICE_SETTLEMENT"
                ),
                Member("session_price_limit_min", MqlType.DOUBLE, "SYMBOL_SESSION_PRICE_LIMIT_MIN"),
                Member("session_price_limit_max", MqlType.DOUBLE, "SYMBOL_SESSION_PRICE_LIMIT_MAX"),
                Member("margin_hedged", MqlType.DOUBLE, "SYMBOL_MARGIN_HEDGED"),
                Member("price_change", MqlType.DOUBLE, "SYMBOL_PRICE_CHANGE"),
                Member("price_volatility", MqlType.DOUBLE, "SYMBOL_PRICE_VOLATILITY"),
                Member("price_theoretical", MqlType.DOUBLE, "SYMBOL_PRICE_THEORETICAL"),
                Member("price_greeks_delta", MqlType.DOUBLE, "SYMBOL_PRICE_DELTA"),
                Member("price_greeks_theta", MqlType.DOUBLE, "SYMBOL_PRICE_THETA"),
                Member("price_greeks_gamma", MqlType.DOUBLE, "SYMBOL_PRICE_GAMMA"),
                Member("price_greeks_vega", MqlType.DOUBLE, "SYMBOL_PRICE_VEGA"),
                Member("price_greeks_rho", MqlType.DOUBLE, "SYMBOL_PRICE_RHO"),
                Member("price_greeks_omega", MqlType.DOUBLE, "SYMBOL_PRICE_OMEGA"),
                Member("price_sensitivity", MqlType.DOUBLE, "SYMBOL_PRICE_SENSITIVITY"),
                Member("basis", MqlType.STRING, "SYMBOL_BASIS"),
                Member("category", MqlType.STRING, "SYMBOL_CATEGORY"),
                Member("currency_base", MqlType.STRING, "SYMBOL_CURRENCY_BASE"),
                Member("currency_profit", MqlType.STRING, "SYMBOL_CURRENCY_PROFIT"),
                Member("currency_margin", MqlType.STRING, "SYMBOL_CURRENCY_MARGIN"),
                Member("bank", MqlType.STRING, "SYMBOL_BANK"),
                Member("description", MqlType.STRING, "SYMBOL_DESCRIPTION"),
                Member("exchange", MqlType.STRING, "SYMBOL_EXCHANGE"),
                Member("formula", MqlType.STRING, "SYMBOL_FORMULA"),
                Member("isin", MqlType.STRING, "SYMBOL_ISIN"),
                Member("name", None),
                Member("page", MqlType.STRING, "SYMBOL_PAGE"),
                Member("path", MqlType.STRING, "SYMBOL_PATH"),
            ),
            epochs={
                "time": EpochUnit.SECONDS,
                "start_time": EpochUnit.SECONDS,
                "expiration_time": EpochUnit.SECONDS,
            },
            key="name",
        ),
        # https://www.mql5.com/en/docs/constants/environment_state/accountinformation
        Struct(
            StructName.ACCOUNT_INFO,
            (
                Member("login", MqlType.LONG, "ACCOUNT_LOGIN"),
                Member("trade_mode", MqlType.ENUM_ACCOUNT_TRADE_MODE, "ACCOUNT_TRADE_MODE"),
                Member("leverage", MqlType.LONG, "ACCOUNT_LEVERAGE"),
                Member("limit_orders", MqlType.INT, "ACCOUNT_LIMIT_ORDERS"),
                Member(
                    "margin_so_mode", MqlType.ENUM_ACCOUNT_STOPOUT_MODE, "ACCOUNT_MARGIN_SO_MODE"
                ),
                Member("trade_allowed", MqlType.BOOL, "ACCOUNT_TRADE_ALLOWED"),
                Member("trade_expert", MqlType.BOOL, "ACCOUNT_TRADE_EXPERT"),
                Member("margin_mode", MqlType.ENUM_ACCOUNT_MARGIN_MODE, "ACCOUNT_MARGIN_MODE"),
                Member("currency_digits", MqlType.INT, "ACCOUNT_CURRENCY_DIGITS"),
                Member("fifo_close", MqlType.BOOL, "ACCOUNT_FIFO_CLOSE"),
                Member("balance", MqlType.DOUBLE, "ACCOUNT_BALANCE"),
                Member("credit", MqlType.DOUBLE, "ACCOUNT_CREDIT"),
                Member("profit", MqlType.DOUBLE, "ACCOUNT_PROFIT"),
                Member("equity", MqlType.DOUBLE, "ACCOUNT_EQUITY"),
                Member("margin", MqlType.DOUBLE, "ACCOUNT_MARGIN"),
                Member("margin_free", MqlType.DOUBLE, "ACCOUNT_MARGIN_FREE"),
                Member("margin_level", MqlType.DOUBLE, "ACCOUNT_MARGIN_LEVEL"),
                Member("margin_so_call", MqlType.DOUBLE, "ACCOUNT_MARGIN_SO_CALL"),
                Member("margin_so_so", MqlType.DOUBLE, "ACCOUNT_MARGIN_SO_SO"),
                Member("margin_initial", MqlType.DOUBLE, "ACCOUNT_MARGIN_INITIAL"),
                Member("margin_maintenance", MqlType.DOUBLE, "ACCOUNT_MARGIN_MAINTENANCE"),
                Member("assets", MqlType.DOUBLE, "ACCOUNT_ASSETS"),
                Member("liabilities", MqlType.DOUBLE, "ACCOUNT_LIABILITIES"),
                Member("commission_blocked", MqlType.DOUBLE, "ACCOUNT_COMMISSION_BLOCKED"),
                Member("name", MqlType.STRING, "ACCOUNT_NAME"),
                Member("server", MqlType.STRING, "ACCOUNT_SERVER"),
                Member("currency", MqlType.STRING, "ACCOUNT_CURRENCY"),
                Member("company", MqlType.STRING, "ACCOUNT_COMPANY"),
            ),
        ),
        Struct(
            StructName.BOOK_INFO,
            (
                Member("type", None),
                Member("price", None),
                Member("volume", None),
                Member("volume_dbl", None),
            ),
        ),
    )
}

RATES = Array(
    ArrayName.RATES,
    (
        ("time", "<i8"),
        ("open", "<f8"),
        ("high", "<f8"),
        ("low", "<f8"),
        ("close", "<f8"),
        ("tick_volume", "<u8"),
        ("spread", "<i4"),
        ("real_volume", "<u8"),
    ),
    epochs={"time": EpochUnit.SECONDS},
)
TICKS = Array(
    ArrayName.TICKS,
    (
        ("time", "<i8"),
        ("bid", "<f8"),
        ("ask", "<f8"),
        ("last", "<f8"),
        ("volume", "<u8"),
        ("time_msc", "<i8"),
        ("flags", "<u4"),
        ("volume_real", "<f8"),
    ),
    epochs={"time": EpochUnit.SECONDS, "time_msc": EpochUnit.MILLISECONDS},
)
ARRAYS: dict[ArrayName, Array] = {array.name: array for array in (RATES, TICKS)}

# ENUM_TIMEFRAMES
TIMEFRAME_M1 = 1
TIMEFRAME_M2 = 2
TIMEFRAME_M3 = 3
TIMEFRAME_M4 = 4
TIMEFRAME_M5 = 5
TIMEFRAME_M6 = 6
TIMEFRAME_M10 = 10
TIMEFRAME_M12 = 12
TIMEFRAME_M15 = 15
TIMEFRAME_M20 = 20
TIMEFRAME_M30 = 30
TIMEFRAME_H1 = 16385
TIMEFRAME_H2 = 16386
TIMEFRAME_H4 = 16388
TIMEFRAME_H3 = 16387
TIMEFRAME_H6 = 16390
TIMEFRAME_H8 = 16392
TIMEFRAME_H12 = 16396
TIMEFRAME_D1 = 16408
TIMEFRAME_W1 = 32769
TIMEFRAME_MN1 = 49153

# COPY_TICKS
COPY_TICKS_ALL = -1
COPY_TICKS_INFO = 1
COPY_TICKS_TRADE = 2

# TICK_FLAG
TICK_FLAG_BID = 2
TICK_FLAG_ASK = 4
TICK_FLAG_LAST = 8
TICK_FLAG_VOLUME = 16
TICK_FLAG_BUY = 32
TICK_FLAG_SELL = 64

# ENUM_POSITION_TYPE
POSITION_TYPE_BUY = 0
POSITION_TYPE_SELL = 1

# ENUM_POSITION_REASON
POSITION_REASON_CLIENT = 0
POSITION_REASON_MOBILE = 1
POSITION_REASON_WEB = 2
POSITION_REASON_EXPERT = 3

# ENUM_ORDER_TYPE
ORDER_TYPE_BUY = 0
ORDER_TYPE_SELL = 1
ORDER_TYPE_BUY_LIMIT = 2
ORDER_TYPE_SELL_LIMIT = 3
ORDER_TYPE_BUY_STOP = 4
ORDER_TYPE_SELL_STOP = 5
ORDER_TYPE_BUY_STOP_LIMIT = 6
ORDER_TYPE_SELL_STOP_LIMIT = 7
ORDER_TYPE_CLOSE_BY = 8

# ENUM_ORDER_STATE
ORDER_STATE_STARTED = 0
ORDER_STATE_PLACED = 1
ORDER_STATE_CANCELED = 2
ORDER_STATE_PARTIAL = 3
ORDER_STATE_FILLED = 4
ORDER_STATE_REJECTED = 5
ORDER_STATE_EXPIRED = 6
ORDER_STATE_REQUEST_ADD = 7
ORDER_STATE_REQUEST_MODIFY = 8
ORDER_STATE_REQUEST_CANCEL = 9

# ENUM_ORDER_TYPE_FILLING
ORDER_FILLING_FOK = 0
ORDER_FILLING_IOC = 1
ORDER_FILLING_RETURN = 2
ORDER_FILLING_BOC = 3

# ENUM_ORDER_TYPE_TIME
ORDER_TIME_GTC = 0
ORDER_TIME_DAY = 1
ORDER_TIME_SPECIFIED = 2
ORDER_TIME_SPECIFIED_DAY = 3

# ENUM_ORDER_REASON
ORDER_REASON_CLIENT = 0
ORDER_REASON_MOBILE = 1
ORDER_REASON_WEB = 2
ORDER_REASON_EXPERT = 3
ORDER_REASON_SL = 4
ORDER_REASON_TP = 5
ORDER_REASON_SO = 6

# ENUM_DEAL_TYPE
DEAL_TYPE_BUY = 0
DEAL_TYPE_SELL = 1
DEAL_TYPE_BALANCE = 2
DEAL_TYPE_CREDIT = 3
DEAL_TYPE_CHARGE = 4
DEAL_TYPE_CORRECTION = 5
DEAL_TYPE_BONUS = 6
DEAL_TYPE_COMMISSION = 7
DEAL_TYPE_COMMISSION_DAILY = 8
DEAL_TYPE_COMMISSION_MONTHLY = 9
DEAL_TYPE_COMMISSION_AGENT_DAILY = 10
DEAL_TYPE_COMMISSION_AGENT_MONTHLY = 11
DEAL_TYPE_INTEREST = 12
DEAL_TYPE_BUY_CANCELED = 13
DEAL_TYPE_SELL_CANCELED = 14
DEAL_DIVIDEND = 15
DEAL_DIVIDEND_FRANKED = 16
DEAL_TAX = 17

# ENUM_DEAL_ENTRY
DEAL_ENTRY_IN = 0
DEAL_ENTRY_OUT = 1
DEAL_ENTRY_INOUT = 2
DEAL_ENTRY_OUT_BY = 3

# ENUM_DEAL_REASON
DEAL_REASON_CLIENT = 0
DEAL_REASON_MOBILE = 1
DEAL_REASON_WEB = 2
DEAL_REASON_EXPERT = 3
DEAL_REASON_SL = 4
DEAL_REASON_TP = 5
DEAL_REASON_SO = 6
DEAL_REASON_ROLLOVER = 7
DEAL_REASON_VMARGIN = 8
DEAL_REASON_SPLIT = 9

# ENUM_TRADE_REQUEST_ACTIONS
TRADE_ACTION_DEAL = 1
TRADE_ACTION_PENDING = 5
TRADE_ACTION_SLTP = 6
TRADE_ACTION_MODIFY = 7
TRADE_ACTION_REMOVE = 8
TRADE_ACTION_CLOSE_BY = 10

# ENUM_SYMBOL_CHART_MODE
SYMBOL_CHART_MODE_BID = 0
SYMBOL_CHART_MODE_LAST = 1

# ENUM_SYMBOL_CALC_MODE
SYMBOL_CALC_MODE_FOREX = 0
SYMBOL_CALC_MODE_FUTURES = 1
SYMBOL_CALC_MODE_CFD = 2
SYMBOL_CALC_MODE_CFDINDEX = 3
SYMBOL_CALC_MODE_CFDLEVERAGE = 4
SYMBOL_CALC_MODE_FOREX_NO_LEVERAGE = 5
SYMBOL_CALC_MODE_EXCH_STOCKS = 32
SYMBOL_CALC_MODE_EXCH_FUTURES = 33
SYMBOL_CALC_MODE_EXCH_OPTIONS = 34
SYMBOL_CALC_MODE_EXCH_OPTIONS_MARGIN = 36
SYMBOL_CALC_MODE_EXCH_BONDS = 37
SYMBOL_CALC_MODE_EXCH_STOCKS_MOEX = 38
SYMBOL_CALC_MODE_EXCH_BONDS_MOEX = 39
SYMBOL_CALC_MODE_SERV_COLLATERAL = 64

# ENUM_SYMBOL_TRADE_MODE
SYMBOL_TRADE_MODE_DISABLED = 0
SYMBOL_TRADE_MODE_LONGONLY = 1
SYMBOL_TRADE_MODE_SHORTONLY = 2
SYMBOL_TRADE_MODE_CLOSEONLY = 3
SYMBOL_TRADE_MODE_FULL = 4

# ENUM_SYMBOL_TRADE_EXECUTION
SYMBOL_TRADE_EXECUTION_REQUEST = 0
SYMBOL_TRADE_EXECUTION_INSTANT = 1
SYMBOL_TRADE_EXECUTION_MARKET = 2
SYMBOL_TRADE_EXECUTION_EXCHANGE = 3

# ENUM_SYMBOL_SWAP_MODE
SYMBOL_SWAP_MODE_DISABLED = 0
SYMBOL_SWAP_MODE_POINTS = 1
SYMBOL_SWAP_MODE_CURRENCY_SYMBOL = 2
SYMBOL_SWAP_MODE_CURRENCY_MARGIN = 3
SYMBOL_SWAP_MODE_CURRENCY_DEPOSIT = 4
SYMBOL_SWAP_MODE_INTEREST_CURRENT = 5
SYMBOL_SWAP_MODE_INTEREST_OPEN = 6
SYMBOL_SWAP_MODE_REOPEN_CURRENT = 7
SYMBOL_SWAP_MODE_REOPEN_BID = 8

# ENUM_DAY_OF_WEEK
DAY_OF_WEEK_SUNDAY = 0
DAY_OF_WEEK_MONDAY = 1
DAY_OF_WEEK_TUESDAY = 2
DAY_OF_WEEK_WEDNESDAY = 3
DAY_OF_WEEK_THURSDAY = 4
DAY_OF_WEEK_FRIDAY = 5
DAY_OF_WEEK_SATURDAY = 6

# ENUM_SYMBOL_ORDER_GTC_MODE
SYMBOL_ORDERS_GTC = 0
SYMBOL_ORDERS_DAILY = 1
SYMBOL_ORDERS_DAILY_NO_STOPS = 2

# ENUM_SYMBOL_OPTION_RIGHT
SYMBOL_OPTION_RIGHT_CALL = 0
SYMBOL_OPTION_RIGHT_PUT = 1

# ENUM_SYMBOL_OPTION_MODE
SYMBOL_OPTION_MODE_EUROPEAN = 0
SYMBOL_OPTION_MODE_AMERICAN = 1

# ENUM_ACCOUNT_TRADE_MODE
ACCOUNT_TRADE_MODE_DEMO = 0
ACCOUNT_TRADE_MODE_CONTEST = 1
ACCOUNT_TRADE_MODE_REAL = 2

# ENUM_ACCOUNT_STOPOUT_MODE
ACCOUNT_STOPOUT_MODE_PERCENT = 0
ACCOUNT_STOPOUT_MODE_MONEY = 1

# ENUM_ACCOUNT_MARGIN_MODE
ACCOUNT_MARGIN_MODE_RETAIL_NETTING = 0
ACCOUNT_MARGIN_MODE_EXCHANGE = 1
ACCOUNT_MARGIN_MODE_RETAIL_HEDGING = 2

# ENUM_BOOK_TYPE
BOOK_TYPE_SELL = 1
BOOK_TYPE_BUY = 2
BOOK_TYPE_SELL_MARKET = 3
BOOK_TYPE_BUY_MARKET = 4

# trade server return codes
TRADE_RETCODE_REQUOTE = 10004
TRADE_RETCODE_REJECT = 10006
TRADE_RETCODE_CANCEL = 10007
TRADE_RETCODE_PLACED = 10008
TRADE_RETCODE_DONE = 10009
TRADE_RETCODE_DONE_PARTIAL = 10010
TRADE_RETCODE_ERROR = 10011
TRADE_RETCODE_TIMEOUT = 10012
TRADE_RETCODE_INVALID = 10013
TRADE_RETCODE_INVALID_VOLUME = 10014
TRADE_RETCODE_INVALID_PRICE = 10015
TRADE_RETCODE_INVALID_STOPS = 10016
TRADE_RETCODE_TRADE_DISABLED = 10017
TRADE_RETCODE_MARKET_CLOSED = 10018
TRADE_RETCODE_NO_MONEY = 10019
TRADE_RETCODE_PRICE_CHANGED = 10020
TRADE_RETCODE_PRICE_OFF = 10021
TRADE_RETCODE_INVALID_EXPIRATION = 10022
TRADE_RETCODE_ORDER_CHANGED = 10023
TRADE_RETCODE_TOO_MANY_REQUESTS = 10024
TRADE_RETCODE_NO_CHANGES = 10025
TRADE_RETCODE_SERVER_DISABLES_AT = 10026
TRADE_RETCODE_CLIENT_DISABLES_AT = 10027
TRADE_RETCODE_LOCKED = 10028
TRADE_RETCODE_FROZEN = 10029
TRADE_RETCODE_INVALID_FILL = 10030
TRADE_RETCODE_CONNECTION = 10031
TRADE_RETCODE_ONLY_REAL = 10032
TRADE_RETCODE_LIMIT_ORDERS = 10033
TRADE_RETCODE_LIMIT_VOLUME = 10034
TRADE_RETCODE_INVALID_ORDER = 10035
TRADE_RETCODE_POSITION_CLOSED = 10036
TRADE_RETCODE_INVALID_CLOSE_VOLUME = 10038
TRADE_RETCODE_CLOSE_ORDER_EXIST = 10039
TRADE_RETCODE_LIMIT_POSITIONS = 10040
TRADE_RETCODE_REJECT_CANCEL = 10041
TRADE_RETCODE_LONG_ONLY = 10042
TRADE_RETCODE_SHORT_ONLY = 10043
TRADE_RETCODE_CLOSE_ONLY = 10044
TRADE_RETCODE_FIFO_CLOSE = 10045

# last_error() codes
RES_S_OK = 1
RES_E_FAIL = -1
RES_E_INVALID_PARAMS = -2
RES_E_NO_MEMORY = -3
RES_E_NOT_FOUND = -4
RES_E_INVALID_VERSION = -5
RES_E_AUTH_FAILED = -6
RES_E_UNSUPPORTED = -7
RES_E_AUTO_TRADING_DISABLED = -8
RES_E_INTERNAL_FAIL = -10000
RES_E_INTERNAL_FAIL_SEND = -10001
RES_E_INTERNAL_FAIL_RECEIVE = -10002
RES_E_INTERNAL_FAIL_INIT = -10003
RES_E_INTERNAL_FAIL_CONNECT = -10004
RES_E_INTERNAL_FAIL_TIMEOUT = -10005


CONSTANT_FAMILIES = (
    "TIMEFRAME_",
    "COPY_TICKS_",
    "TICK_FLAG_",
    "POSITION_",
    "ORDER_",
    "DEAL_",
    "TRADE_ACTION_",
    "SYMBOL_",
    "DAY_OF_WEEK_",
    "ACCOUNT_",
    "BOOK_TYPE_",
    "TRADE_RETCODE_",
    "RES_",
)
CONSTANTS: dict[str, int] = {
    name: value for name, value in globals().items() if name.startswith(CONSTANT_FAMILIES)
}

# What last_error() reports after a call that succeeded.
SUCCESS: tuple[int, str] = (RES_S_OK, "Success")
