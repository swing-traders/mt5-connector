from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum, StrEnum, auto
from typing import TYPE_CHECKING

from mt5connect import mirror
from mt5connect import remote_mt5 as mt5
from mt5connect.errors import MT5ConnectionError, MT5LoginError

if TYPE_CHECKING:
    from mt5connect.config import MT5Config

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# CONNECTION STATE
# ─────────────────────────────────────────────────────────────────────────────


class ConnectionState(Enum):
    """Tracks exactly where in the lifecycle the connection is."""

    DISCONNECTED = auto()  # nothing attempted yet
    INITIALIZING = auto()  # server initialize in progress
    INITIALIZED = auto()  # server initialized, not logged in
    LOGGING_IN = auto()  # mt5.login() in progress
    CONNECTED = auto()  # fully ready to use
    RECONNECTING = auto()  # lost connection, retrying
    SHUTTING_DOWN = auto()  # mt5.shutdown() called
    FAILED = auto()  # gave up after max attempts


# ─────────────────────────────────────────────────────────────────────────────
# ACCOUNT SNAPSHOT
# ─────────────────────────────────────────────────────────────────────────────


class MarginMode(StrEnum):
    """How the account books positions, as `account_info().margin_mode` declares it."""

    RETAIL_NETTING = "RETAIL_NETTING"
    EXCHANGE = "EXCHANGE"
    RETAIL_HEDGING = "RETAIL_HEDGING"


_MARGIN_MODES = {
    mirror.ACCOUNT_MARGIN_MODE_RETAIL_NETTING: MarginMode.RETAIL_NETTING,
    mirror.ACCOUNT_MARGIN_MODE_EXCHANGE: MarginMode.EXCHANGE,
    mirror.ACCOUNT_MARGIN_MODE_RETAIL_HEDGING: MarginMode.RETAIL_HEDGING,
}


@dataclass
class AccountSnapshot:
    """The account as `account_info()` reports it, its money in `Decimal`."""

    login: int
    server: str
    balance: Decimal
    equity: Decimal
    margin: Decimal
    margin_free: Decimal
    margin_level: Decimal
    credit: Decimal
    profit: Decimal
    currency: str
    currency_digits: int
    leverage: Decimal
    margin_mode: MarginMode
    trade_allowed: bool
    name: str
    company: str

    @classmethod
    def from_mt5(cls, info) -> AccountSnapshot:
        """Build from the raw mt5.account_info() namedtuple; raises MT5ConnectionError for a margin
        mode the package does not define."""
        if info.margin_mode not in _MARGIN_MODES:
            raise MT5ConnectionError(f"account_info: margin mode {info.margin_mode} is unknown")
        return cls(
            login=info.login,
            server=info.server,
            balance=_finite(info.balance, "balance"),
            equity=_finite(info.equity, "equity"),
            margin=_finite(info.margin, "margin"),
            margin_free=_finite(info.margin_free, "margin_free"),
            margin_level=_finite(info.margin_level, "margin_level"),
            credit=_finite(info.credit, "credit"),
            profit=_finite(info.profit, "profit"),
            currency=info.currency,
            currency_digits=info.currency_digits,
            leverage=_finite(info.leverage, "leverage"),
            margin_mode=_MARGIN_MODES[info.margin_mode],
            trade_allowed=info.trade_allowed,
            name=info.name,
            company=info.company,
        )

    def __str__(self) -> str:
        return (
            f"Account | {self.server} | "
            f"Balance: {self.balance:.2f} {self.currency} | "
            f"Equity: {self.equity:.2f} | "
            f"Free Margin: {self.margin_free:.2f} | "
            f"Leverage: 1:{self.leverage}"
        )


def _finite(value: float, field: str) -> Decimal:
    number = Decimal(str(value))
    if not number.is_finite():
        raise MT5ConnectionError(f"account_info: {field} {value} is not finite")
    return number


# ─────────────────────────────────────────────────────────────────────────────
# MT5 CONNECTION
# ─────────────────────────────────────────────────────────────────────────────


class MT5Connection:
    """
    Owns the connection lifecycle to the MT5 terminal behind the server.

    Usage
    -----
        conn = MT5Connection(config)
        conn.connect()                   # initialize + login
        conn.ensure_connected()          # call before every mt5.* API call
        info = conn.get_account_info()
        conn.disconnect()                # clean shutdown

    Context manager
    ---------------
        with MT5Connection(config) as conn:
            info = conn.get_account_info()

    All core methods are synchronous because the shim's calls to the server
    are synchronous. reconnect_async() is provided for use inside asyncio
    polling loops.
    """

    def __init__(self, config: MT5Config) -> None:
        self._config = config
        self._state = ConnectionState.DISCONNECTED
        self._attempt = 0
        self._last_error: tuple[int, str] | None = None
        self._connected_at: float | None = None

    # ── Public properties ─────────────────────────────────────────────────────

    @property
    def state(self) -> ConnectionState:
        return self._state

    @property
    def is_connected(self) -> bool:
        return self._state == ConnectionState.CONNECTED

    # ── Core lifecycle ────────────────────────────────────────────────────────

    def connect(self) -> None:
        """
        Full connection: bind the shim to the configured server, initialize, then login to broker.
        Raises MT5ConnectionError or MT5LoginError on failure.
        """
        mt5.configure(self._config.server_url, self._config.ws_url)
        self._initialize()
        self._login()
        self._attempt = 0  # reset backoff counter on clean connect

    def disconnect(self) -> None:
        """
        Cleanly shut down. Safe to call even if not connected.
        """
        if self._state in (ConnectionState.DISCONNECTED, ConnectionState.SHUTTING_DOWN):
            return
        logger.info("MT5Connection: shutting down")
        self._state = ConnectionState.SHUTTING_DOWN
        mt5.shutdown()
        self._state = ConnectionState.DISCONNECTED
        self._connected_at = None
        logger.info("MT5Connection: disconnected")

    def ensure_connected(self) -> None:
        """
        Call before EVERY mt5.* API call in data.py, execution.py, providers.py.

        CONNECTED  → returns immediately (fast path).
        FAILED     → raises MT5ConnectionError (gave up reconnecting).
        anything else → raises MT5ConnectionError (not ready yet).
        """
        if self._state == ConnectionState.CONNECTED:
            return

        if self._state == ConnectionState.FAILED:
            raise MT5ConnectionError(
                f"MT5 connection permanently failed after "
                f"{self._config.reconnect_max_attempts} attempts. "
                "Restart the trading node to try again."
            )

        raise MT5ConnectionError(
            f"MT5 not connected (state={self._state.name}). "
            "Call connect() first or wait for reconnect to complete."
        )

    # ── Reconnect ─────────────────────────────────────────────────────────────

    def reconnect(self) -> bool:
        """
        Synchronous reconnect with exponential backoff.
        Returns True on success, False when max attempts exceeded.
        Use reconnect_async() inside asyncio polling loops.
        """
        self._state = ConnectionState.RECONNECTING
        delay = self._config.reconnect_initial_delay_s

        while self._attempt < self._config.reconnect_max_attempts:
            self._attempt += 1
            logger.warning(
                f"MT5 reconnect attempt {self._attempt}/"
                f"{self._config.reconnect_max_attempts} "
                f"(waiting {delay:.1f}s)"
            )
            time.sleep(delay)
            delay = min(delay * 2.0, self._config.reconnect_max_delay_s)

            try:
                mt5.shutdown()  # clean slate before retry
                self._initialize()
                self._login()
                logger.info(f"MT5 reconnected on attempt {self._attempt}")
                self._attempt = 0
                return True
            except (MT5ConnectionError, MT5LoginError) as exc:
                logger.warning(f"Reconnect attempt {self._attempt} failed: {exc}")

        self._state = ConnectionState.FAILED
        logger.error(f"MT5 gave up after {self._config.reconnect_max_attempts} attempts")
        return False

    async def reconnect_async(self) -> bool:
        """
        Async reconnect with exponential backoff.
        Use this inside asyncio polling loops in data.py and execution.py.

        Example:
            except MT5ConnectionError:
                ok = await self._conn.reconnect_async()
                if not ok:
                    raise
        """
        self._state = ConnectionState.RECONNECTING
        delay = self._config.reconnect_initial_delay_s

        while self._attempt < self._config.reconnect_max_attempts:
            self._attempt += 1
            logger.warning(
                f"MT5 async reconnect attempt {self._attempt}/"
                f"{self._config.reconnect_max_attempts} "
                f"(waiting {delay:.1f}s)"
            )
            await asyncio.sleep(delay)
            delay = min(delay * 2.0, self._config.reconnect_max_delay_s)

            try:
                mt5.shutdown()
                self._initialize()
                self._login()
                logger.info(f"MT5 async reconnected on attempt {self._attempt}")
                self._attempt = 0
                return True
            except (MT5ConnectionError, MT5LoginError) as exc:
                logger.warning(f"Async reconnect attempt {self._attempt} failed: {exc}")

        self._state = ConnectionState.FAILED
        logger.error(f"MT5 async gave up after {self._config.reconnect_max_attempts} attempts")
        return False

    # ── Data accessors ────────────────────────────────────────────────────────

    def get_account_info(self) -> AccountSnapshot:
        """
        Return a snapshot of the current account state.
        Raises MT5ConnectionError if not connected or if call fails.
        """
        self.ensure_connected()
        info = mt5.account_info()
        if info is None:
            code, msg = mt5.last_error()
            raise MT5ConnectionError(f"mt5.account_info() returned None — error {code}: {msg}")
        return AccountSnapshot.from_mt5(info)

    def get_terminal_info(self) -> dict:
        """
        Return the server's MT5 terminal as `terminal_info()` reports it, its trading permission and
        connection included. Raises MT5ConnectionError if not connected or if the call fails.
        """
        self.ensure_connected()
        info = mt5.terminal_info()
        if info is None:
            code, msg = mt5.last_error()
            raise MT5ConnectionError(f"mt5.terminal_info() returned None — error {code}: {msg}")
        return {
            "name": info.name,
            "path": info.path,
            "data_path": info.data_path,
            "connected": info.connected,
            "trade_allowed": info.trade_allowed,
            "ping_last": info.ping_last,
            "retransmission": info.retransmission,
        }

    def last_error(self) -> tuple[int, str]:
        """
        Return the last MT5 error (code, message).
        Safe to call at any time — does not require connection.
        """
        code, msg = mt5.last_error()
        self._last_error = (code, msg)
        return code, msg

    def uptime_seconds(self) -> float | None:
        """How long this connection has been alive. None if not connected."""
        if self._connected_at is None:
            return None
        return time.time() - self._connected_at

    # ── Private ───────────────────────────────────────────────────────────────

    def _initialize(self) -> None:
        """Ask the server to initialize its MT5 terminal."""
        logger.debug("MT5Connection: calling mt5.initialize()")
        self._state = ConnectionState.INITIALIZING

        ok = mt5.initialize()
        if not ok:
            code, msg = mt5.last_error()
            self._state = ConnectionState.DISCONNECTED
            raise MT5ConnectionError(
                f"mt5.initialize() failed on the server's MT5 terminal — error {code}: {msg}"
            )

        self._state = ConnectionState.INITIALIZED
        logger.debug("MT5Connection: server initialized")

    def _login(self) -> None:
        """Authenticate with the broker. Must be called after _initialize()."""
        logger.debug(f"MT5Connection: logging in — server={self._config.server}")
        self._state = ConnectionState.LOGGING_IN

        ok = mt5.login(
            login=self._config.account,
            password=self._config.password,
            server=self._config.server,
            timeout=int(self._config.timeout_s * 1000),  # mt5 wants milliseconds
        )

        if not ok:
            code, msg = mt5.last_error()
            self._state = ConnectionState.INITIALIZED
            raise MT5LoginError(
                f"mt5.login() failed on {self._config.server} — error {code}: {msg}"
            )

        self._state = ConnectionState.CONNECTED
        self._connected_at = time.time()

        info = mt5.account_info()
        if info:
            logger.info(f"MT5Connection: connected — {AccountSnapshot.from_mt5(info)}")
        else:
            logger.info(f"MT5Connection: connected to {self._config.server}")

    # ── Context manager ───────────────────────────────────────────────────────

    def __enter__(self) -> MT5Connection:
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self.disconnect()
        return False  # never suppress exceptions

    def __repr__(self) -> str:
        return f"MT5Connection(server={self._config.server!r}, state={self._state.name})"
