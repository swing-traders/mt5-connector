"""Trading sessions the terminal's EAs relay from SymbolInfoSessionQuote and SymbolInfoSessionTrade,
kept per symbol.

State: the outcome of the latest relay for each symbol, its sessions or the refusal of its frame. An
absent symbol has never been relayed, and a refusal stands until a later relay is accepted. Nothing
is persisted."""

from dataclasses import dataclass

from mt5connector.server.commissions import RelayRefusal
from mt5connector.server.wire import binary

_WEEKS = ("quote", "trade")

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


def sessions_refusal(frame: binary.Sessions, symbol: str) -> str | None:
    """Why a frame the EA serving `symbol` relays is not its trading sessions, or None when it is;
    the decoder has read seven days of integer seconds into each week."""
    if frame.symbol != symbol:
        return f"symbol {frame.symbol!r} is not the relay's {symbol!r}"
    else:
        return _first(_week_refusal(frame.sessions[name], name) for name in _WEEKS)


def trading_sessions(sessions: dict) -> TradingSessions:
    """The sessions a decoded sessions frame carries."""
    return TradingSessions(
        quote=_week(sessions["quote"]),
        trade=_week(sessions["trade"]),
        last_error=sessions["last_error"],
    )


def _week(days: list) -> Week:
    return tuple(tuple((session[0], session[1]) for session in day) for day in days)


def _week_refusal(week: list, where: str) -> str | None:
    return _first(
        _session_refusal(session, f"{where}[{day_index}][{index}]")
        for day_index, day in enumerate(week)
        for index, session in enumerate(day)
    )


def _session_refusal(session: list[int], where: str) -> str | None:
    if session[0] < 0 or session[1] < 0:
        return f"{where} holds a negative second: {session!r}"
    # A session that runs to midnight ends at 0.
    elif session[1] != 0 and session[0] >= session[1]:
        return f"{where} does not end after it starts: {session!r}"
    else:
        return None


def _first(refusals) -> str | None:
    """The first refusal that is not None, or None."""
    return next((refusal for refusal in refusals if refusal is not None), None)
