"""Commission schedules the terminal's EA relays from SymbolInfoCommissions, kept per symbol.

State: the latest schedule written for each symbol; nothing is persisted."""

from dataclasses import dataclass

# The mode fields carry MQL5's ENUM_SYMBOL_COMMISSION_* values as the EA relays them: the reference
# names those enumerations' members but not their integer values.


@dataclass(frozen=True)
class CommissionTier:
    mode: int
    volume_type: int
    value: float
    min_value: float
    max_value: float
    range_from: float
    range_to: float
    currency: str


@dataclass(frozen=True)
class CommissionRule:
    currency: str
    mode_range: int
    mode_charge: int
    mode_entry: int
    mode_direction: int
    mode_profit: int
    tiers: tuple[CommissionTier, ...]


@dataclass(frozen=True)
class CommissionSchedule:
    """One SymbolInfoCommissions answer: its return value, the error it left, and its rules."""

    ret: int
    last_error: int
    rules: tuple[CommissionRule, ...]


class CommissionStore:
    """The latest relayed commission schedule per symbol."""

    def __init__(self) -> None:
        self._schedules: dict[str, CommissionSchedule] = {}

    def write(self, symbol: str, schedule: CommissionSchedule) -> None:
        self._schedules[symbol] = schedule

    def read(self, symbol: str) -> CommissionSchedule | None:
        return self._schedules.get(symbol)
