"""A symbol's commission rule as the server relays it from the terminal, typed, and the taker fee
per fill it implies."""

from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum

from mt5connect.errors import MT5InstrumentError
from mt5connect.parsing import finite_decimal


class CommissionMode(StrEnum):
    """How a commission tier states its value: MQL5's ENUM_SYMBOL_COMMISSION_MODE."""

    MONEY_DEPOSIT = "MONEY_DEPOSIT"
    MONEY_SPECIFIED = "MONEY_SPECIFIED"


class CommissionEntry(StrEnum):
    """The legs a commission rule charges: MQL5's ENUM_SYMBOL_COMMISSION_ENTRY_MODE."""

    INOUT = "INOUT"
    IN = "IN"


# The MQL5 reference names these members without their values; these are the values the terminal
# assigns them.
_MODES = {0: CommissionMode.MONEY_DEPOSIT, 6: CommissionMode.MONEY_SPECIFIED}
_ENTRIES = {0: CommissionEntry.INOUT, 1: CommissionEntry.IN}


@dataclass(frozen=True)
class CommissionRule:
    """A symbol's commission rule, as its first tier states it."""

    entry: CommissionEntry
    mode: CommissionMode
    value: Decimal
    currency: str


def parse_schedule(schedule: dict | None) -> CommissionRule | None:
    """The first rule of a relayed schedule with its first tier; None when nothing was relayed or
    the schedule holds no rule. Raises MT5InstrumentError for a failed read or a mode with no known
    value."""
    if schedule is None:
        return None
    elif schedule["ret"] < 0:
        raise MT5InstrumentError(f"the commission schedule read failed: {schedule['last_error']}")
    elif not schedule["rules"]:
        return None
    else:
        rule = schedule["rules"][0]
        tier = rule["tiers"][0]
        if tier["mode"] not in _MODES:
            raise MT5InstrumentError(f"commission mode {tier['mode']} has no known value")
        elif rule["mode_entry"] not in _ENTRIES:
            raise MT5InstrumentError(
                f"commission entry mode {rule['mode_entry']} has no known value"
            )
        else:
            return CommissionRule(
                entry=_ENTRIES[rule["mode_entry"]],
                mode=_MODES[tier["mode"]],
                value=finite_decimal(tier["value"], "commission value"),
                currency=tier["currency"],
            )


def taker_fee(
    rule: CommissionRule, rate: Decimal, contract_size: Decimal, price: Decimal
) -> Decimal:
    """The fee per fill as a fraction of notional: the rule's value in the quote currency (`rate`
    quote units per unit of the rule's currency) per lot of `contract_size` at `price`, or per unit
    at `price`."""
    value_in_quote = rule.value * rate
    if rule.mode == CommissionMode.MONEY_DEPOSIT:
        fee = value_in_quote / (contract_size * price)
    else:
        fee = value_in_quote / price
    # NT charges a fee on every fill, so a rule charged on entry alone spreads over both legs.
    if rule.entry == CommissionEntry.IN:
        return fee / 2
    else:
        return fee
