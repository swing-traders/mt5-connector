"""A symbol's trading sessions as the server relays them from the terminal: per weekday, Sunday
first, each quote and trade session's [from, to] in seconds from 00:00 of the broker day."""

from dataclasses import dataclass


@dataclass(frozen=True)
class TradingSessions:
    """A symbol's quote and trade sessions per weekday, Sunday first, exactly as relayed."""

    quote: list[list[list[int]]]
    trade: list[list[list[int]]]


def parse_sessions(relayed: dict) -> TradingSessions:
    """The sessions a relayed answer states."""
    return TradingSessions(quote=relayed["quote"], trade=relayed["trade"])
