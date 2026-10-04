"""`MT5Config`: what the adapter runs with, checked when it is built."""

import math
from dataclasses import dataclass, field
from enum import StrEnum
from urllib.parse import ParseResult, urlparse, urlunparse

from mt5connector.client.constants import (
    DEFAULT_EXEC_POLL_INTERVAL_MS,
    RECONNECT_INITIAL_DELAY_S,
    RECONNECT_MAX_ATTEMPTS,
    RECONNECT_MAX_DELAY_S,
)
from mt5connector.client.errors import MT5ConfigError


class HttpScheme(StrEnum):
    """The schemes the server's URL may name."""

    HTTP = "http"
    HTTPS = "https"


class WebSocketScheme(StrEnum):
    """The schemes the push hub's URL may name."""

    WS = "ws"
    WSS = "wss"


@dataclass
class MT5Config:
    """What the adapter runs with; README.md's configuration reference documents each field."""

    # ── Required ──────────────────────────────────────────────────────────────
    # Credentials: the broker server's name identifies the account as the login does.
    account: int = field(repr=False)
    password: str = field(repr=False)
    server: str = field(repr=False)

    # ── Optional / defaults ───────────────────────────────────────────────────
    # A node's instrument provider loads these; a consumer loading symbols by name needs none.
    symbols: list[str] = field(default_factory=list)
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
        server = _endpoint("server_url", self.server_url)
        if server.scheme not in set(HttpScheme) or not server.hostname:
            raise MT5ConfigError("MT5Config.server_url is no http or https URL with a host")
        if self.ws_url is None:
            self.ws_url = derive_ws_url(self.server_url)
        else:
            ws = _endpoint("ws_url", self.ws_url)
            if ws.scheme not in set(WebSocketScheme) or not ws.hostname:
                raise MT5ConfigError("MT5Config.ws_url is no ws or wss URL with a host")

    @property
    def exec_poll_interval_s(self) -> float:
        return self.exec_poll_interval_ms / 1000.0

    @property
    def ws_display_url(self) -> str:
        """The hub's URL as it may be shown: its scheme, host, port and path, without the userinfo
        or query that can carry credentials."""
        ws = urlparse(self.ws_url)
        return urlunparse((ws.scheme, ws.netloc.rpartition("@")[2], ws.path, "", "", ""))


def _endpoint(name: str, url: str) -> ParseResult:
    """The URL of the field `name`, parsed; raises MT5ConfigError naming the field for a malformed
    URL or a port outside 1-65535, without the parser's error, which can carry the URL's
    credentials."""
    try:
        parsed = urlparse(url)
        port = parsed.port
    except ValueError:
        raise MT5ConfigError(f"MT5Config.{name} is malformed") from None
    if port is not None and not 1 <= port <= 65535:
        raise MT5ConfigError(f"MT5Config.{name} port {port} is not in 1-65535")
    return parsed


def derive_ws_url(server_url: str) -> str:
    """Derive the WebSocket hub URL from the REST server URL (port 9000)."""
    p = urlparse(server_url)
    # The netloc's host keeps an IPv6 address's brackets, which `hostname` strips.
    host = p.netloc.rpartition("@")[2]
    if host.startswith("["):
        host = host[: host.index("]") + 1]
    else:
        host = host.partition(":")[0]
    return urlunparse((WebSocketScheme.WS, f"{host}:9000", p.path, "", "", ""))
