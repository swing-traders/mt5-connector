"""A symbol's commission rule as the server relays it from the terminal, typed, and the taker fee
per fill it implies."""

from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal

from mt5connector.client.errors import MT5InstrumentError
from mt5connector.client.parsing import finite_decimal
from mt5connector.wire.push_wire import CommissionEntryMode, CommissionMode

# The modes that state money in the rule's currency, which the fee converts into the quote currency.
MONEY_MODES = frozenset({CommissionMode.MONEY_DEPOSIT, CommissionMode.MONEY_SPECIFIED})


@dataclass(frozen=True)
class CommissionRule:
    """A symbol's commission rule, as its first tier states it."""

    entry: CommissionEntryMode
    mode: CommissionMode
    value: Decimal
    currency: str


def parse_schedule(schedule: dict) -> CommissionRule | None:
    """The first rule of a relayed schedule with its first tier; None when it holds no rule. Raises
    MT5InstrumentError for a failed read or a mode MQL5 does not name."""
    if schedule["ret"] < 0:
        raise MT5InstrumentError(f"the commission schedule read failed: {schedule['last_error']}")
    elif not schedule["rules"]:
        return None
    else:
        rule = schedule["rules"][0]
        tier = rule["tiers"][0]
        return CommissionRule(
            entry=_member(CommissionEntryMode, rule["mode_entry"], "commission entry mode"),
            mode=_member(CommissionMode, tier["mode"], "commission mode"),
            value=finite_decimal(tier["value"], "commission value"),
            currency=tier["currency"],
        )


def taker_fee(
    rule: CommissionRule,
    *,
    price: Decimal,
    contract_size: Decimal,
    point: Decimal,
    rate: Callable[[str], Decimal],
) -> Decimal:
    """The fee per fill as a fraction of notional at `price`: money per lot of `contract_size` or
    per unit, converted at `rate` — quote units per unit of the rule's currency, read for a money
    rule alone — a percentage, or points of `point` in price. Raises MT5InstrumentError for a mode
    or entry no fee derives from."""
    if rule.mode == CommissionMode.MONEY_DEPOSIT:
        fee = rule.value * rate(rule.currency) / (contract_size * price)
    elif rule.mode == CommissionMode.MONEY_SPECIFIED:
        fee = rule.value * rate(rule.currency) / price
    elif rule.mode == CommissionMode.PERCENT:
        fee = rule.value / 100
    elif rule.mode == CommissionMode.PIPS:
        fee = rule.value * point / price
    else:
        raise MT5InstrumentError(f"commission mode {rule.mode} derives no fee")
    # NT charges a fee on every fill, so a rule charged on entry alone spreads over both legs.
    if rule.entry == CommissionEntryMode.INOUT:
        return fee
    elif rule.entry == CommissionEntryMode.IN:
        return fee / 2
    else:
        raise MT5InstrumentError(f"commission entry mode {rule.entry} derives no fee")


def _member(members, name: str, field: str):
    """The member of `members` the venue's name is; raises MT5InstrumentError for a name MQL5 does
    not give one."""
    try:
        return members(name)
    except ValueError:
        raise MT5InstrumentError(f"{field} {name!r} is unknown") from None
