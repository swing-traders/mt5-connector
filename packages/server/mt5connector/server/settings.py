"""The hub's settings, read once from the environment at start."""

import math
from collections.abc import Mapping
from dataclasses import dataclass
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

_HUB_PORT = "9000"
_CHART_IDLE_SECONDS = "900"
_BROKER_TZ = "America/New_York"
_BROKER_OFFSET_HOURS = "7"
_HISTORY_RETRY_SECONDS = "5"
_CLOCK_SAMPLE_MAX_AGE_SECONDS = "30"
_CLOCK_BOOTSTRAP_SECONDS = "120"
_CLOCK_CHECK_SECONDS = "300"
_CONSUMER_QUEUE_BOUND = "10000"
_EA_QUEUE_BOUND = "10000"
_PENDING_BOUND = "1000"


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
        durations = {
            "retry_after_s": self.retry_after_s,
            "clock_sample_max_age_seconds": self.clock_sample_max_age_seconds,
            "clock_bootstrap_seconds": self.clock_bootstrap_seconds,
            "clock_check_seconds": self.clock_check_seconds,
        }
        bounds = {
            "consumer_queue_bound": self.consumer_queue_bound,
            "ea_queue_bound": self.ea_queue_bound,
            "pending_bound": self.pending_bound,
        }
        not_durations = [name for name, value in durations.items() if not 0 < value < math.inf]
        not_bounds = [name for name, value in bounds.items() if value < 1]
        if not 1 <= self.hub_port <= 65535:
            raise SettingsError(f"hub_port {self.hub_port} is not a TCP port")
        elif self.chart_idle_seconds < 1:
            raise SettingsError(f"chart_idle_seconds {self.chart_idle_seconds} is not positive")
        elif not_durations:
            raise SettingsError(f"{not_durations[0]} is not finite and positive")
        elif not_bounds:
            raise SettingsError(f"{not_bounds[0]} is not positive")


def read_hub_settings(environ: Mapping[str, str]) -> HubSettings:
    """The hub's settings the environment carries; raises SettingsError naming the first bad
    setting."""
    return HubSettings(
        hub_port=_integer("MT5_HUB_PORT", environ.get("MT5_HUB_PORT", _HUB_PORT)),
        broker_tz=_zone("MT5_BROKER_TZ", environ.get("MT5_BROKER_TZ", _BROKER_TZ)),
        broker_offset_hours=_integer(
            "MT5_BROKER_OFFSET_HOURS", environ.get("MT5_BROKER_OFFSET_HOURS", _BROKER_OFFSET_HOURS)
        ),
        chart_idle_seconds=_integer(
            "MT5_CHART_IDLE_SECONDS", environ.get("MT5_CHART_IDLE_SECONDS", _CHART_IDLE_SECONDS)
        ),
        retry_after_s=_integer(
            "MT5_HISTORY_RETRY_SECONDS",
            environ.get("MT5_HISTORY_RETRY_SECONDS", _HISTORY_RETRY_SECONDS),
        ),
        clock_sample_max_age_seconds=_integer(
            "MT5_CLOCK_SAMPLE_MAX_AGE_SECONDS",
            environ.get("MT5_CLOCK_SAMPLE_MAX_AGE_SECONDS", _CLOCK_SAMPLE_MAX_AGE_SECONDS),
        ),
        clock_bootstrap_seconds=_integer(
            "MT5_CLOCK_BOOTSTRAP_SECONDS",
            environ.get("MT5_CLOCK_BOOTSTRAP_SECONDS", _CLOCK_BOOTSTRAP_SECONDS),
        ),
        clock_check_seconds=_integer(
            "MT5_CLOCK_CHECK_SECONDS", environ.get("MT5_CLOCK_CHECK_SECONDS", _CLOCK_CHECK_SECONDS)
        ),
        consumer_queue_bound=_integer(
            "MT5_HUB_CONSUMER_QUEUE_BOUND",
            environ.get("MT5_HUB_CONSUMER_QUEUE_BOUND", _CONSUMER_QUEUE_BOUND),
        ),
        ea_queue_bound=_integer(
            "MT5_HUB_EA_QUEUE_BOUND", environ.get("MT5_HUB_EA_QUEUE_BOUND", _EA_QUEUE_BOUND)
        ),
        pending_bound=_integer(
            "MT5_HUB_PENDING_BOUND", environ.get("MT5_HUB_PENDING_BOUND", _PENDING_BOUND)
        ),
    )


def _integer(name: str, value: str) -> int:
    try:
        return int(value)
    except ValueError:
        raise SettingsError(f"{name} is not an integer") from None


def _zone(name: str, value: str) -> ZoneInfo:
    try:
        return ZoneInfo(value)
    except (ZoneInfoNotFoundError, ValueError):
        raise SettingsError(f"{name} {value!r} is not a known time zone") from None
