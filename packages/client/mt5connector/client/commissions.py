"""A symbol's commission rule as the server relays it from the terminal, typed, and the fee per fill
it implies."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from decimal import Decimal

from mt5connector.client.errors import MT5InstrumentError
from mt5connector.client.parsing import finite_decimal
from mt5connector.wire.push_wire import (
    CommissionChargeMode,
    CommissionDirectionMode,
    CommissionEntryMode,
    CommissionMode,
    CommissionProfitMode,
    CommissionVolumeType,
)

# The modes whose tier states money per lot in its currency, converted into the quote currency.
CONVERTED_MODES = frozenset(
    {
        CommissionMode.MONEY_DEPOSIT,
        CommissionMode.MONEY_SYMBOL_PROFIT,
        CommissionMode.MONEY_SYMBOL_MARGIN,
        CommissionMode.MONEY_SPECIFIED,
    }
)


@dataclass(frozen=True)
class CommissionRule:
    """A symbol's one commission rule, as its one tier states it."""

    entry: CommissionEntryMode
    mode: CommissionMode
    volume_type: CommissionVolumeType
    value: Decimal
    currency: str


def parse_schedule(schedule: dict) -> CommissionRule | None:
    """The one rule of a relayed schedule with its one tier; None when it holds no rule. Raises
    MT5InstrumentError for a failed read, and naming the field for a schedule charged otherwise than
    instantly, on every deal, by one tier without bounds."""
    if schedule["ret"] < 0:
        raise MT5InstrumentError(f"the commission schedule read failed: {schedule['last_error']}")
    elif not schedule["rules"]:
        return None
    elif len(schedule["rules"]) > 1:
        raise MT5InstrumentError(f"the commission schedule holds {len(schedule['rules'])} rules")
    rule = schedule["rules"][0]
    if not rule["tiers"]:
        raise MT5InstrumentError("the commission rule has no tier")
    elif len(rule["tiers"]) > 1:
        raise MT5InstrumentError(f"the commission rule holds {len(rule['tiers'])} tiers")
    elif rule["mode_charge"] != CommissionChargeMode.INSTANT:
        raise MT5InstrumentError(f"the commission rule's mode_charge is {rule['mode_charge']}")
    elif rule["mode_direction"] != CommissionDirectionMode.BOTH:
        raise MT5InstrumentError(
            f"the commission rule's mode_direction is {rule['mode_direction']}"
        )
    elif rule["mode_profit"] != CommissionProfitMode.ALL:
        raise MT5InstrumentError(f"the commission rule's mode_profit is {rule['mode_profit']}")
    tier = rule["tiers"][0]
    for bound in ("min_value", "max_value"):
        if finite_decimal(tier[bound], f"commission {bound}") != 0:
            raise MT5InstrumentError(f"the commission rule's {bound} is {tier[bound]}")
    return CommissionRule(
        entry=rule["mode_entry"],
        mode=tier["mode"],
        volume_type=tier["volume_type"],
        value=finite_decimal(tier["value"], "commission value"),
        currency=tier["currency"],
    )


async def taker_fee(
    rule: CommissionRule,
    *,
    price: Decimal,
    contract_size: Decimal,
    point: Decimal,
    rate: Callable[[str], Awaitable[Decimal]],
) -> Decimal:
    """The fee per fill as a fraction of notional at `price`: money per lot of `contract_size`, in
    the tier's currency converted at `rate` — quote units per unit of it, awaited for a converted
    mode alone — or in the base currency; a percentage of the turnover; or points of `point` in
    price. Raises MT5InstrumentError naming the mode or volume_type of a tier no fraction expresses.
    """
    if rule.mode == CommissionMode.DISABLED:
        fee = Decimal(0)
    elif rule.mode in CONVERTED_MODES and rule.volume_type == CommissionVolumeType.VOLUME:
        fee = rule.value * await rate(rule.currency) / (contract_size * price)
    elif (
        rule.mode == CommissionMode.MONEY_SYMBOL_BASE
        and rule.volume_type == CommissionVolumeType.VOLUME
    ):
        fee = rule.value / contract_size
    elif rule.mode == CommissionMode.PERCENT and rule.volume_type == CommissionVolumeType.TURNOVER:
        fee = rule.value / 100
    elif rule.mode == CommissionMode.PIPS and rule.volume_type == CommissionVolumeType.VOLUME:
        fee = rule.value * point / price
    elif rule.mode == CommissionMode.PERCENT_PROFIT:
        raise MT5InstrumentError(f"the commission rule's mode is {rule.mode}")
    else:
        raise MT5InstrumentError(
            f"the commission rule's volume_type is {rule.volume_type} under mode {rule.mode}"
        )
    # NT charges a fee on every fill, so a rule charged on one leg alone spreads over both.
    if rule.entry == CommissionEntryMode.INOUT:
        return fee
    else:
        return fee / 2
