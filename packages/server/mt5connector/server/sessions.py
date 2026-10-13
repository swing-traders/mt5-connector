"""Trading sessions the terminal's EAs relay from SymbolInfoSessionQuote and SymbolInfoSessionTrade,
kept per symbol, and the shape of the frame that relays them.

State: the outcome of the latest relay for each symbol, its sessions or the refusal of its frame. An
absent symbol has never been relayed, and a refusal stands until a later relay is accepted. Nothing
is persisted."""

from dataclasses import dataclass

from mt5connector.server.commissions import RelayRefusal
from mt5connector.server.wire.push_wire import PROTOCOL_VERSION, FrameType

_FRAME_FIELDS = ("v", "type", "symbol", "quote", "trade", "last_error")
_WEEKS = ("quote", "trade")
# ENUM_DAY_OF_WEEK, SUNDAY to SATURDAY.
_DAYS = 7

# A session's [from, to], seconds from 00:00 of the broker day.
Session = tuple[int, int]
Week = tuple[tuple[Session, ...], ...]


@dataclass(frozen=True)
class TradingSessions:
    """A symbol's quote and trade sessions per weekday, Sunday first, as the terminal states them,
    and the error its reads left."""

    quote: Week
    trade: Week
    last_error: int


class SessionStore:
    """The outcome of the latest sessions relay per symbol."""

    def __init__(self) -> None:
        self._relays: dict[str, TradingSessions | RelayRefusal] = {}

    def write(self, symbol: str, relay: TradingSessions | RelayRefusal) -> None:
        self._relays[symbol] = relay

    def read(self, symbol: str) -> TradingSessions | RelayRefusal | None:
        return self._relays.get(symbol)


def sessions_refusal(frame: object, symbol: str) -> str | None:
    """Why a body relayed for `symbol` is not its sessions frame, or None when it is."""
    if not isinstance(frame, dict):
        return "request body is not a JSON object"
    missing = [name for name in _FRAME_FIELDS if name not in frame]
    unknown = sorted(name for name in frame if name not in _FRAME_FIELDS)
    if missing:
        return f"missing field: {', '.join(missing)}"
    elif unknown:
        return f"unknown field: {', '.join(unknown)}"
    elif not (_is_integer(frame["v"]) and frame["v"] == PROTOCOL_VERSION):
        return f"unsupported frame version: {frame['v']!r}"
    elif frame["type"] != FrameType.SESSIONS:
        return f"not a {FrameType.SESSIONS} frame: {frame['type']!r}"
    elif not (isinstance(frame["symbol"], str) and frame["symbol"]):
        return f"not a symbol: {frame['symbol']!r}"
    elif frame["symbol"] != symbol:
        return f"symbol {frame['symbol']!r} is not the relay's {symbol!r}"
    elif not _is_integer(frame["last_error"]):
        return f"last_error is not an integer: {frame['last_error']!r}"
    else:
        return _first(_week_refusal(frame[name], name) for name in _WEEKS)


def trading_sessions(frame: dict) -> TradingSessions:
    """The sessions a sessions frame carries; the frame is one sessions_refusal accepts."""
    return TradingSessions(
        quote=_week(frame["quote"]),
        trade=_week(frame["trade"]),
        last_error=frame["last_error"],
    )


def _week(days: list) -> Week:
    return tuple(tuple((session[0], session[1]) for session in day) for day in days)


def _week_refusal(week: object, where: str) -> str | None:
    if not isinstance(week, list):
        return f"{where} is not a list: {week!r}"
    elif len(week) != _DAYS:
        return f"{where} holds {len(week)} days, not {_DAYS}"
    else:
        return _first(_day_refusal(day, f"{where}[{index}]") for index, day in enumerate(week))


def _day_refusal(day: object, where: str) -> str | None:
    if not isinstance(day, list):
        return f"{where} is not a list: {day!r}"
    else:
        return _first(
            _session_refusal(session, f"{where}[{index}]") for index, session in enumerate(day)
        )


def _session_refusal(session: object, where: str) -> str | None:
    if not (isinstance(session, list) and len(session) == 2 and all(map(_is_integer, session))):
        return f"{where} is not two integers: {session!r}"
    elif session[0] < 0 or session[1] < 0:
        return f"{where} holds a negative second: {session!r}"
    # A session that runs to midnight ends at 0.
    elif session[1] != 0 and session[0] >= session[1]:
        return f"{where} does not end after it starts: {session!r}"
    else:
        return None


def _first(refusals) -> str | None:
    """The first refusal that is not None, or None."""
    return next((refusal for refusal in refusals if refusal is not None), None)


def _is_integer(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)
