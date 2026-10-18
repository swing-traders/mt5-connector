"""The hub's settings, read once from the environment at start."""

from collections.abc import Mapping
from dataclasses import dataclass, fields
from typing import NamedTuple
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


class _Variable(NamedTuple):
    """A setting's environment variable and the value it takes unset."""

    name: str
    default: str


# Each HubSettings field's variable.
_VARIABLES = {
    "hub_port": _Variable("MT5_HUB_PORT", "9000"),
    "broker_tz": _Variable("MT5_BROKER_TZ", "America/New_York"),
    "broker_offset_hours": _Variable("MT5_BROKER_OFFSET_HOURS", "7"),
    "chart_idle_seconds": _Variable("MT5_CHART_IDLE_SECONDS", "900"),
    "retry_after_s": _Variable("MT5_HISTORY_RETRY_SECONDS", "5"),
    "clock_sample_max_age_seconds": _Variable("MT5_CLOCK_SAMPLE_MAX_AGE_SECONDS", "30"),
    "clock_bootstrap_seconds": _Variable("MT5_CLOCK_BOOTSTRAP_SECONDS", "120"),
    "clock_check_seconds": _Variable("MT5_CLOCK_CHECK_SECONDS", "300"),
    "consumer_queue_bound": _Variable("MT5_HUB_CONSUMER_QUEUE_BOUND", "10000"),
    "ea_queue_bound": _Variable("MT5_HUB_EA_QUEUE_BOUND", "10000"),
    "pending_bound": _Variable("MT5_HUB_PENDING_BOUND", "1000"),
}
_POSITIVE = (
    "chart_idle_seconds",
    "retry_after_s",
    "clock_sample_max_age_seconds",
    "clock_bootstrap_seconds",
    "clock_check_seconds",
    "consumer_queue_bound",
    "ea_queue_bound",
    "pending_bound",
)


class SettingsError(Exception):
    """Raised when a setting is malformed or out of range."""


@dataclass(frozen=True)
class HubSettings:
    hub_port: int
    broker_tz: ZoneInfo
    broker_offset_hours: int
    chart_idle_seconds: int
    retry_after_s: float
    clock_sample_max_age_seconds: float
    clock_bootstrap_seconds: float
    clock_check_seconds: float
    consumer_queue_bound: int
    ea_queue_bound: int
    pending_bound: int

    def __post_init__(self) -> None:
        """Refuses a setting out of range, naming its environment variable and its value."""
        if not 1 <= self.hub_port <= 65535:
            variable = _VARIABLES["hub_port"].name
            raise SettingsError(f"{variable}={self.hub_port} is not a TCP port")
        elif not isinstance(self.broker_tz, ZoneInfo):
            variable = _VARIABLES["broker_tz"].name
            raise SettingsError(f"{variable}={self.broker_tz!r} is not a known time zone")
        for name in _POSITIVE:
            value = getattr(self, name)
            if not value > 0:
                raise SettingsError(f"{_VARIABLES[name].name}={value} is not positive")


def read_hub_settings(environ: Mapping[str, str]) -> HubSettings:
    """The hub's settings the environment carries; raises SettingsError naming a bad variable and
    its value."""
    values = {}
    for setting in fields(HubSettings):
        variable = _VARIABLES[setting.name]
        text = environ.get(variable.name, variable.default)
        if setting.name == "broker_tz":
            values[setting.name] = _zone(variable.name, text)
        else:
            values[setting.name] = _integer(variable.name, text)
    return HubSettings(**values)


def _integer(name: str, value: str) -> int:
    try:
        return int(value)
    except ValueError:
        raise SettingsError(f"{name}={value!r} is not an integer") from None


def _zone(name: str, value: str) -> ZoneInfo:
    try:
        return ZoneInfo(value)
    except (ZoneInfoNotFoundError, ValueError):
        raise SettingsError(f"{name}={value!r} is not a known time zone") from None
