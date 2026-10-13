"""A symbol's trading sessions as the server relays them from the terminal: per weekday, Sunday
first, each quote and trade session's [from, to] in seconds from 00:00 of the broker day."""

from dataclasses import dataclass

from mt5connector.client.errors import MT5InstrumentError

# ENUM_DAY_OF_WEEK, SUNDAY to SATURDAY.
_DAYS = 7


@dataclass(frozen=True)
class TradingSessions:
    """A symbol's quote and trade sessions per weekday, Sunday first, exactly as relayed."""

    quote: list[list[list[int]]]
    trade: list[list[list[int]]]


def parse_sessions(relayed: dict) -> TradingSessions:
    """The sessions a relayed answer states; raises MT5InstrumentError for a week out of shape."""
    return TradingSessions(
        quote=_week(relayed.get("quote"), "quote sessions"),
        trade=_week(relayed.get("trade"), "trade sessions"),
    )


def _week(week: object, field: str) -> list[list[list[int]]]:
    if not (isinstance(week, list) and len(week) == _DAYS):
        raise MT5InstrumentError(f"{field} {week!r} are not {_DAYS} days")
    for day in week:
        if not (isinstance(day, list) and all(map(_is_session, day))):
            raise MT5InstrumentError(f"{field} {day!r} are not [from, to] pairs")
    return week


def _is_session(session: object) -> bool:
    return (
        isinstance(session, list)
        and len(session) == 2
        and all(isinstance(second, int) and not isinstance(second, bool) for second in session)
    )
