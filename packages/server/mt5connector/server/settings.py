"""The HTTP server's and the hub's settings, each read once from the environment at start."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

_API_PORT = "5000"
_HUB_PORT = "9000"
_CHART_IDLE_SECONDS = "900"
_BROKER_TZ = "America/New_York"
_BROKER_OFFSET_HOURS = "7"


class SettingsError(Exception):
    """Raised when a setting is missing, malformed or out of range."""


@dataclass(frozen=True)
class Settings:
    terminal_path: str
    login: int = field(repr=False)
    password: str = field(repr=False)
    server: str = field(repr=False)
    spawner_symbol: str
    login_timeout_ms: int
    api_host: str
    api_port: int
    api_threads: int
    broker_tz: ZoneInfo
    broker_offset_hours: int
    clock_check_seconds: int
    clock_sample_max_age_seconds: int
    clock_bootstrap_seconds: int
    history_retry_seconds: int
    floor_ttl_seconds: int
    hub_port: int
    chart_idle_seconds: int

    def __post_init__(self) -> None:
        if self.login_timeout_ms <= 0:
            raise SettingsError(f"login_timeout_ms {self.login_timeout_ms} is not positive")
        elif not 1 <= self.api_port <= 65535:
            raise SettingsError(f"api_port {self.api_port} is not a TCP port")
        elif self.api_threads < 2:
            # One worker is kept free for /health and the relay.
            raise SettingsError(f"api_threads {self.api_threads} is below 2")
        elif self.clock_check_seconds < 1:
            raise SettingsError(f"clock_check_seconds {self.clock_check_seconds} is not positive")
        elif self.clock_sample_max_age_seconds < 1:
            raise SettingsError(
                f"clock_sample_max_age_seconds {self.clock_sample_max_age_seconds} is not positive"
            )
        elif self.clock_bootstrap_seconds < 1:
            raise SettingsError(
                f"clock_bootstrap_seconds {self.clock_bootstrap_seconds} is not positive"
            )
        elif self.history_retry_seconds < 1:
            raise SettingsError(
                f"history_retry_seconds {self.history_retry_seconds} is not positive"
            )
        elif self.floor_ttl_seconds < 1:
            raise SettingsError(f"floor_ttl_seconds {self.floor_ttl_seconds} is not positive")
        elif not 1 <= self.hub_port <= 65535:
            raise SettingsError(f"hub_port {self.hub_port} is not a TCP port")
        elif self.chart_idle_seconds < 1:
            raise SettingsError(f"chart_idle_seconds {self.chart_idle_seconds} is not positive")


def read_settings(environ: Mapping[str, str]) -> Settings:
    """The settings the environment carries; raises SettingsError naming the first bad setting."""
    return Settings(
        terminal_path=_required(environ, "MT5_TERMINAL_PATH"),
        login=_integer("MT5_LOGIN", _required(environ, "MT5_LOGIN")),
        password=_required(environ, "MT5_PASSWORD"),
        server=_required(environ, "MT5_SERVER"),
        spawner_symbol=_required(environ, "MT5_SPAWNER_SYMBOL"),
        login_timeout_ms=_integer(
            "MT5_LOGIN_TIMEOUT_MS", environ.get("MT5_LOGIN_TIMEOUT_MS", "60000")
        ),
        api_host=environ.get("MT5_API_HOST", "0.0.0.0"),
        api_port=_integer("MT5_API_PORT", environ.get("MT5_API_PORT", _API_PORT)),
        api_threads=_integer("MT5_API_THREADS", environ.get("MT5_API_THREADS", "5")),
        broker_tz=_zone("MT5_BROKER_TZ", environ.get("MT5_BROKER_TZ", _BROKER_TZ)),
        broker_offset_hours=_integer(
            "MT5_BROKER_OFFSET_HOURS", environ.get("MT5_BROKER_OFFSET_HOURS", _BROKER_OFFSET_HOURS)
        ),
        clock_check_seconds=_integer(
            "MT5_CLOCK_CHECK_SECONDS", environ.get("MT5_CLOCK_CHECK_SECONDS", "300")
        ),
        clock_sample_max_age_seconds=_integer(
            "MT5_CLOCK_SAMPLE_MAX_AGE_SECONDS",
            environ.get("MT5_CLOCK_SAMPLE_MAX_AGE_SECONDS", "30"),
        ),
        clock_bootstrap_seconds=_integer(
            "MT5_CLOCK_BOOTSTRAP_SECONDS", environ.get("MT5_CLOCK_BOOTSTRAP_SECONDS", "120")
        ),
        history_retry_seconds=_integer(
            "MT5_HISTORY_RETRY_SECONDS", environ.get("MT5_HISTORY_RETRY_SECONDS", "5")
        ),
        floor_ttl_seconds=_integer(
            "MT5_FLOOR_TTL_SECONDS", environ.get("MT5_FLOOR_TTL_SECONDS", "900")
        ),
        hub_port=_integer("MT5_HUB_PORT", environ.get("MT5_HUB_PORT", _HUB_PORT)),
        chart_idle_seconds=_integer(
            "MT5_CHART_IDLE_SECONDS", environ.get("MT5_CHART_IDLE_SECONDS", _CHART_IDLE_SECONDS)
        ),
    )


@dataclass(frozen=True)
class HubSettings:
    hub_port: int
    api_port: int
    broker_tz: ZoneInfo
    broker_offset_hours: int
    chart_idle_seconds: int

    def __post_init__(self) -> None:
        if not 1 <= self.hub_port <= 65535:
            raise SettingsError(f"hub_port {self.hub_port} is not a TCP port")
        elif not 1 <= self.api_port <= 65535:
            raise SettingsError(f"api_port {self.api_port} is not a TCP port")
        elif self.chart_idle_seconds < 1:
            raise SettingsError(f"chart_idle_seconds {self.chart_idle_seconds} is not positive")


def read_hub_settings(environ: Mapping[str, str]) -> HubSettings:
    """The hub's settings the environment carries; raises SettingsError naming the first bad
    setting."""
    return HubSettings(
        hub_port=_integer("MT5_HUB_PORT", environ.get("MT5_HUB_PORT", _HUB_PORT)),
        api_port=_integer("MT5_API_PORT", environ.get("MT5_API_PORT", _API_PORT)),
        broker_tz=_zone("MT5_BROKER_TZ", environ.get("MT5_BROKER_TZ", _BROKER_TZ)),
        broker_offset_hours=_integer(
            "MT5_BROKER_OFFSET_HOURS", environ.get("MT5_BROKER_OFFSET_HOURS", _BROKER_OFFSET_HOURS)
        ),
        chart_idle_seconds=_integer(
            "MT5_CHART_IDLE_SECONDS", environ.get("MT5_CHART_IDLE_SECONDS", _CHART_IDLE_SECONDS)
        ),
    )


def _required(environ: Mapping[str, str], name: str) -> str:
    value = environ.get(name, "")
    if not value:
        raise SettingsError(f"{name} is not set")
    return value


def _integer(name: str, value: str) -> int:
    # The message never carries the value: MT5_LOGIN is an account number.
    try:
        return int(value)
    except ValueError:
        raise SettingsError(f"{name} is not an integer") from None


def _zone(name: str, value: str) -> ZoneInfo:
    try:
        return ZoneInfo(value)
    except (ZoneInfoNotFoundError, ValueError):
        raise SettingsError(f"{name} {value!r} is not a known time zone") from None
