"""The messages the execution client publishes on the node's message bus beside NT's own events: the
figures of each deal it reads."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal

from nautilus_trader.model.identifiers import Venue

from mt5connector.client.parsing import finite_decimal


@dataclass(frozen=True)
class DealFigures:
    """One deal of the venue's history as the client read it: its identities, its entry and
    reason codes, its volume and the money it booked in the deposit currency, its time in epoch
    milliseconds, and `raw`, every wire field of the deal by name, rendered as its exact decimal
    text."""

    ticket: int
    order: int
    position_id: int
    entry: int
    reason: int
    volume: Decimal
    profit: Decimal
    commission: Decimal
    fee: Decimal
    swap: Decimal
    time_msc: int
    raw: Mapping[str, str]


@dataclass(frozen=True)
class DealsRecovery:
    """A consumer's ask that the execution client read every deal of `position`, a position of
    the raw `symbol`, from the history and publish their figures on the deals topic."""

    position: int
    symbol: str


def recovery_topic(venue: Venue) -> str:
    """The message-bus topic the execution client of the account at `venue` takes its deals
    recoveries on."""
    return f"mt5.recover.{venue}"


def deals_topic(venue: Venue) -> str:
    """The message-bus topic the execution client of the account at `venue` publishes its deals'
    figures on."""
    return f"mt5.deals.{venue}"


def deal_figures(deal) -> DealFigures:
    """The figures of a deal record as the hub answers it; raises MT5InstrumentError for a figure
    that is not finite."""
    return DealFigures(
        ticket=deal.ticket,
        order=deal.order,
        position_id=deal.position_id,
        entry=deal.entry,
        reason=deal.reason,
        volume=finite_decimal(deal.volume, "volume"),
        profit=finite_decimal(deal.profit, "profit"),
        commission=finite_decimal(deal.commission, "commission"),
        fee=finite_decimal(deal.fee, "fee"),
        swap=finite_decimal(deal.swap, "swap"),
        time_msc=deal.time_msc,
        raw={name: _text(value, name) for name, value in deal._asdict().items()},
    )


def _text(value: object, name: str) -> str:
    """A wire field's exact text: a number as its decimal, a string as it came."""
    if isinstance(value, float):
        return str(finite_decimal(value, name))
    else:
        return str(value)
