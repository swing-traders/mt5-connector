"""`MT5Config`: what the adapter runs with, checked when it is built."""

import math
from dataclasses import dataclass
from urllib.parse import urlparse, urlunparse

from mt5connector.client.constants import (
    DEFAULT_EXEC_POLL_INTERVAL_MS,
    RECONNECT_INITIAL_DELAY_S,
    RECONNECT_MAX_ATTEMPTS,
    RECONNECT_MAX_DELAY_S,
)
from mt5connector.client.errors import MT5ConfigError


@dataclass
class MT5Config:
    """What the adapter runs with; README.md's configuration reference documents each field."""

    # ── Required ──────────────────────────────────────────────────────────────
    account: int
    password: str
    server: str
    symbols: list[str]

    # ── Optional / defaults ───────────────────────────────────────────────────
    exec_poll_interval_ms: int = DEFAULT_EXEC_POLL_INTERVAL_MS
    deviation_points: int = 20
    account_refresh_seconds: int = 10
    history_lookback_mins: int = 60
    reconnect_initial_delay_s: float = RECONNECT_INITIAL_DELAY_S
    reconnect_max_delay_s: float = RECONNECT_MAX_DELAY_S
    reconnect_max_attempts: int = RECONNECT_MAX_ATTEMPTS
    timeout_s: float = 10.0

    # Required: defaulted only so that a config without it is refused as an MT5ConfigError.
    server_url: str | None = None
    ws_url: str | None = None

    def __post_init__(self) -> None:
        if not self.account or self.account <= 0:
            raise ValueError("MT5Config.account must be a positive integer.")
        if not self.password:
            raise ValueError("MT5Config.password cannot be empty.")
        if not self.server:
            raise ValueError("MT5Config.server cannot be empty.")
        if not self.symbols:
            raise ValueError("MT5Config.symbols cannot be empty.")

        # Broker casing is the symbol (EURUSDm): only surrounding whitespace goes.
        self.symbols = [s.strip() for s in self.symbols]
        if "" in self.symbols:
            raise ValueError("MT5Config.symbols holds a blank symbol")

        if self.exec_poll_interval_ms < 50:
            raise ValueError("exec_poll_interval_ms must be at least 50ms.")
        if self.deviation_points < 0:
            raise ValueError("deviation_points must not be negative.")
        if self.account_refresh_seconds < 1:
            raise ValueError("account_refresh_seconds must be at least 1.")
        if self.history_lookback_mins <= 0:
            raise ValueError("history_lookback_mins must be positive.")
        if not (math.isfinite(self.timeout_s) and self.timeout_s > 0):
            raise ValueError(f"MT5Config.timeout_s {self.timeout_s} is not finite and positive")
        if self.reconnect_max_attempts < 1:
            raise ValueError(
                f"MT5Config.reconnect_max_attempts {self.reconnect_max_attempts} is below 1"
            )
        if not (
            math.isfinite(self.reconnect_initial_delay_s) and self.reconnect_initial_delay_s >= 0
        ):
            raise ValueError(
                f"MT5Config.reconnect_initial_delay_s {self.reconnect_initial_delay_s} is not "
                "finite and non-negative"
            )
        if not (math.isfinite(self.reconnect_max_delay_s) and self.reconnect_max_delay_s >= 0):
            raise ValueError(
                f"MT5Config.reconnect_max_delay_s {self.reconnect_max_delay_s} is not finite and "
                "non-negative"
            )

        if not self.server_url:
            raise MT5ConfigError("MT5Config.server_url cannot be empty.")
        url = urlparse(self.server_url)
        if not (url.scheme and url.hostname):
            raise MT5ConfigError("MT5Config.server_url has no scheme or no host")
        if self.ws_url is None:
            self.ws_url = derive_ws_url(self.server_url)

    @property
    def exec_poll_interval_s(self) -> float:
        return self.exec_poll_interval_ms / 1000.0


def derive_ws_url(server_url: str) -> str:
    """Derive the WebSocket hub URL from the REST server URL (port 9000)."""
    p = urlparse(server_url)
    if p.scheme == "ws" and p.port == 9000:
        return server_url
    return urlunparse(("ws", f"{p.hostname}:9000", p.path, "", "", ""))
