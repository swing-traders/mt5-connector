"""Commission schedules the terminal's EAs relay from SymbolInfoCommissions, kept per symbol, and
the shape of the frame that relays one.

State: the outcome of the latest relay for each symbol, its schedule or the refusal of its frame. An
absent symbol has never been relayed, a successful empty schedule confirms no rule, and a refusal
stands until a later relay is accepted. Nothing is persisted."""

from dataclasses import dataclass
from enum import StrEnum

from mt5connector.server.wire.push_wire import (
    PROTOCOL_VERSION,
    CommissionChargeMode,
    CommissionDirectionMode,
    CommissionEntryMode,
    CommissionMode,
    CommissionProfitMode,
    CommissionRangeMode,
    CommissionVolumeType,
    FrameType,
)

_FRAME_FIELDS = ("v", "type", "symbol", "ret", "last_error", "rules")
_RULE_FIELDS = (
    "currency",
    "mode_range",
    "mode_charge",
    "mode_entry",
    "mode_direction",
    "mode_profit",
    "tiers",
)
# The fields carrying an enum member by its EnumToString name, and the enum of each.
_RULE_NAMES = {
    "mode_range": CommissionRangeMode,
    "mode_charge": CommissionChargeMode,
    "mode_entry": CommissionEntryMode,
    "mode_direction": CommissionDirectionMode,
    "mode_profit": CommissionProfitMode,
}
_TIER_FIELDS = (
    "mode",
    "volume_type",
    "value",
    "min_value",
    "max_value",
    "range_from",
    "range_to",
    "currency",
)
_TIER_NAMES = {"mode": CommissionMode, "volume_type": CommissionVolumeType}
_TIER_NUMBERS = ("value", "min_value", "max_value", "range_from", "range_to")


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
    """A relayed commissions frame the server refused, and why."""

    reason: str


class CommissionStore:
    """The outcome of the latest commission relay per symbol."""

    def __init__(self) -> None:
        self._relays: dict[str, CommissionSchedule | RelayRefusal] = {}

    def write(self, symbol: str, relay: CommissionSchedule | RelayRefusal) -> None:
        self._relays[symbol] = relay

    def read(self, symbol: str) -> CommissionSchedule | RelayRefusal | None:
        return self._relays.get(symbol)


def commissions_refusal(frame: object, symbol: str) -> str | None:
    """Why a body relayed for `symbol` is not its commissions frame, or None when it is."""
    if not isinstance(frame, dict):
        return "request body is not a JSON object"
    refusal = _fields_refusal(frame, _FRAME_FIELDS, "")
    if refusal is not None:
        return refusal
    elif not (_is_integer(frame["v"]) and frame["v"] == PROTOCOL_VERSION):
        return f"unsupported frame version: {frame['v']!r}"
    elif frame["type"] != FrameType.COMMISSIONS:
        return f"not a {FrameType.COMMISSIONS} frame: {frame['type']!r}"
    elif not (isinstance(frame["symbol"], str) and frame["symbol"]):
        return f"not a symbol: {frame['symbol']!r}"
    elif frame["symbol"] != symbol:
        return f"symbol {frame['symbol']!r} is not the relay's {symbol!r}"
    elif not _is_integer(frame["ret"]):
        return f"ret is not an integer: {frame['ret']!r}"
    elif not _is_integer(frame["last_error"]):
        return f"last_error is not an integer: {frame['last_error']!r}"
    elif not isinstance(frame["rules"], list):
        return f"rules is not a list: {frame['rules']!r}"
    else:
        return _first(
            _rule_refusal(rule, f"rules[{index}]") for index, rule in enumerate(frame["rules"])
        )


def commission_schedule(frame: dict) -> CommissionSchedule:
    """The schedule a commissions frame carries, each name as the member it names; the frame is one
    commissions_refusal accepts."""
    rules = []
    for rule in frame["rules"]:
        tiers = []
        for tier in rule["tiers"]:
            tiers.append(CommissionTier(**(tier | _members(tier, _TIER_NAMES))))
        named = _members(rule, _RULE_NAMES)
        rules.append(CommissionRule(**(rule | named | {"tiers": tuple(tiers)})))
    return CommissionSchedule(ret=frame["ret"], last_error=frame["last_error"], rules=tuple(rules))


def _rule_refusal(rule: object, where: str) -> str | None:
    if not isinstance(rule, dict):
        return f"{where} is not an object: {rule!r}"
    refusal = _fields_refusal(rule, _RULE_FIELDS, f"{where}: ")
    if refusal is not None:
        return refusal
    elif not isinstance(rule["currency"], str):
        return f"{where}.currency is not a string: {rule['currency']!r}"
    elif not isinstance(rule["tiers"], list):
        return f"{where}.tiers is not a list: {rule['tiers']!r}"
    else:
        names = (_name_refusal(rule, name, members, where) for name, members in _RULE_NAMES.items())
        tiers = (
            _tier_refusal(tier, f"{where}.tiers[{index}]")
            for index, tier in enumerate(rule["tiers"])
        )
        return _first((*names, *tiers))


def _tier_refusal(tier: object, where: str) -> str | None:
    if not isinstance(tier, dict):
        return f"{where} is not an object: {tier!r}"
    refusal = _fields_refusal(tier, _TIER_FIELDS, f"{where}: ")
    if refusal is not None:
        return refusal
    elif not isinstance(tier["currency"], str):
        return f"{where}.currency is not a string: {tier['currency']!r}"
    else:
        names = (_name_refusal(tier, name, members, where) for name, members in _TIER_NAMES.items())
        numbers = (
            f"{where}.{name} is not a number: {tier[name]!r}"
            for name in _TIER_NUMBERS
            if not _is_number(tier[name])
        )
        return _first((*names, *numbers))


def _fields_refusal(value: dict, fields: tuple[str, ...], where: str) -> str | None:
    missing = [name for name in fields if name not in value]
    unknown = sorted(name for name in value if name not in fields)
    if missing:
        return f"{where}missing field: {', '.join(missing)}"
    elif unknown:
        return f"{where}unknown field: {', '.join(unknown)}"
    else:
        return None


def _name_refusal(value: dict, name: str, members: type[StrEnum], where: str) -> str | None:
    if isinstance(value[name], str) and value[name] in {member.value for member in members}:
        return None
    else:
        return f"{where}.{name} is unknown: {value[name]!r}"


def _members(value: dict, names: dict[str, type[StrEnum]]) -> dict[str, StrEnum]:
    """The member each name field of a rule or tier names."""
    return {name: members(value[name]) for name, members in names.items()}


def _first(refusals) -> str | None:
    """The first refusal that is not None, or None."""
    return next((refusal for refusal in refusals if refusal is not None), None)


def _is_integer(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)
