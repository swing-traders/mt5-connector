"""Commission schedules the terminal's EAs relay from SymbolInfoCommissions, kept per symbol.

State: the outcome of the latest relay for each symbol, its schedule or the refusal of its frame. An
absent symbol has never been relayed, a successful empty schedule confirms no rule, and a refusal
stands until a later relay is accepted. Nothing is persisted."""

from dataclasses import dataclass

from mt5connector.server.wire import binary
from mt5connector.server.wire.push_wire import (
    CommissionChargeMode,
    CommissionDirectionMode,
    CommissionEntryMode,
    CommissionMode,
    CommissionProfitMode,
    CommissionRangeMode,
    CommissionVolumeType,
)


@dataclass(frozen=True)
class CommissionTier:
    mode: CommissionMode
    volume_type: CommissionVolumeType
    value: float
    min_value: float
    max_value: float
    range_from: float
    range_to: float
    currency: str


@dataclass(frozen=True)
class CommissionRule:
    currency: str
    mode_range: CommissionRangeMode
    mode_charge: CommissionChargeMode
    mode_entry: CommissionEntryMode
    mode_direction: CommissionDirectionMode
    mode_profit: CommissionProfitMode
    tiers: tuple[CommissionTier, ...]


@dataclass(frozen=True)
class CommissionSchedule:
    """One SymbolInfoCommissions answer: its return value, the error it left, and its rules."""

    ret: int
    last_error: int
    rules: tuple[CommissionRule, ...]


@dataclass(frozen=True)
class RelayRefusal:
    """A relayed frame the hub refused, and why."""

    reason: str


class CommissionStore:
    """The outcome of the latest commission relay per symbol."""

    def __init__(self) -> None:
        self._relays: dict[str, CommissionSchedule | RelayRefusal] = {}

    def write(self, symbol: str, relay: CommissionSchedule | RelayRefusal) -> None:
        self._relays[symbol] = relay

    def read(self, symbol: str) -> CommissionSchedule | RelayRefusal | None:
        return self._relays.get(symbol)


def commissions_refusal(frame: binary.Commissions, symbol: str) -> str | None:
    """Why a frame the EA serving `symbol` relays is not its commission schedule, or None when it
    is; the decoder has refused every name no member of its enum holds."""
    if frame.symbol != symbol:
        return f"symbol {frame.symbol!r} is not the relay's {symbol!r}"
    else:
        return None


def commission_schedule(schedule: dict) -> CommissionSchedule:
    """The schedule a decoded commissions frame carries."""
    rules = []
    for rule in schedule["rules"]:
        tiers = tuple(CommissionTier(**tier) for tier in rule["tiers"])
        rules.append(CommissionRule(**(rule | {"tiers": tiers})))
    return CommissionSchedule(
        ret=schedule["ret"], last_error=schedule["last_error"], rules=tuple(rules)
    )
