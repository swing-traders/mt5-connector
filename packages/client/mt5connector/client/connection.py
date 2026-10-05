"""The adapter's connection to the MT5 terminal behind the server, and the account as the terminal
reports it.

State: the connection's own transport to its server, where the connection is in its lifecycle, and
the reconnect attempts made since it last connected."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum, StrEnum, auto
from typing import TYPE_CHECKING

from mt5connector.client.errors import MT5ConnectionError, MT5LoginError, ServerBusy
from mt5connector.client.remote_mt5 import RemoteMT5
from mt5connector.wire import mirror

if TYPE_CHECKING:
    from mt5connector.client.config import MT5Config

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


_CONNECTED_OR_CONNECTING = frozenset(
    {
        ConnectionState.INITIALIZING,
        ConnectionState.LOGGING_IN,
        ConnectionState.CONNECTED,
        ConnectionState.RECONNECTING,
    }
)


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

    # Credentials: the broker server's name identifies the account as the login does.
    login: int = field(repr=False)
    server: str = field(repr=False)
    balance: Decimal
    equity: Decimal
    margin: Decimal
    # ACCOUNT_MARGIN_MAINTENANCE: the minimum equity reserved for the open positions (MQL5's
    # ENUM_ACCOUNT_INFO_DOUBLE).
    margin_maintenance: Decimal
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
            margin_maintenance=_finite(info.margin_maintenance, "margin_maintenance"),
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
            f"Account | Balance: {self.balance:.2f} {self.currency} | "
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
    """The connection to the MT5 terminal behind one server, shared by the clients of one account:
    its lifecycle, and the transport every call to that server travels on."""

    def __init__(self, config: MT5Config) -> None:
        self._config = config
        self.mt5 = RemoteMT5(config.server_url)
        self._state = ConnectionState.DISCONNECTED
        self._attempt = 0
        self._reconnect_lock = asyncio.Lock()

    # ── Core lifecycle ────────────────────────────────────────────────────────

    def connect(self) -> None:
        """Initializes the terminal and logs in to the broker. Raises MT5ConnectionError naming the
        state on a connection already connected or connecting, and MT5ConnectionError or
        MT5LoginError for a step that fails."""
        if self._state in _CONNECTED_OR_CONNECTING:
            raise MT5ConnectionError(
                f"MT5 already connected or connecting (state={self._state.name})"
            )
        self._initialize()
        self._login()
        self._log_connected()
        self._attempt = 0

    def disconnect(self) -> None:
        """Shuts the terminal session down and closes the transport's HTTP session, even when the
        shutdown raises; raises MT5ConnectionError naming the state on a connection not
        connected."""
        if self._state in (ConnectionState.DISCONNECTED, ConnectionState.SHUTTING_DOWN):
            raise MT5ConnectionError(f"MT5 not connected (state={self._state.name})")
        logger.info("MT5Connection: shutting down")
        self._state = ConnectionState.SHUTTING_DOWN
        try:
            self.mt5.shutdown()
        finally:
            self.mt5.close_session()
        self._state = ConnectionState.DISCONNECTED
        logger.info("MT5Connection: disconnected")

    def ensure_connected(self) -> None:
        """Returns at once while connected; otherwise raises MT5ConnectionError naming the state, a
        FAILED connection by the reconnect attempts it gave up after."""
        if self._state == ConnectionState.CONNECTED:
            return

        if self._state == ConnectionState.FAILED:
            raise MT5ConnectionError(
                f"MT5 connection gave up after {self._config.reconnect_max_attempts} reconnect "
                "attempts"
            )

        raise MT5ConnectionError(f"MT5 not connected (state={self._state.name})")

    # ── Reconnect ─────────────────────────────────────────────────────────────

    async def reconnect_async(self) -> bool:
        """Reconnects with exponential backoff, waiting out a busy server at no attempt's cost; True
        once connected. The clients sharing the connection reconnect it one at a time, so a caller
        that finds it already reconnected, or given up on, takes that outcome rather than running
        the sequence again."""
        async with self._reconnect_lock:
            if self._state == ConnectionState.CONNECTED:
                return True
            elif self._state == ConnectionState.FAILED:
                return False
            else:
                return await self._reconnect_with_backoff()

    async def _reconnect_with_backoff(self) -> bool:
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
                await self._served(self.mt5.shutdown)
                await self._served(self._initialize)
                await self._served(self._login)
                await self._served(self._log_connected)
                logger.info(f"MT5 async reconnected on attempt {self._attempt}")
                self._attempt = 0
                return True
            except (MT5ConnectionError, MT5LoginError) as exc:
                logger.warning(f"Async reconnect attempt {self._attempt} failed: {exc}")
                # Between attempts the connection is reconnecting, whatever the failed step left.
                self._state = ConnectionState.RECONNECTING

        self._state = ConnectionState.FAILED
        logger.error(f"MT5 async gave up after {self._config.reconnect_max_attempts} attempts")
        return False

    async def _served(self, call: Callable[[], object]) -> None:
        """Makes a call, asking it again after the delay each busy answer gives: a busy server is
        up, so its refusal is no failed step. Short of the login the connection is reconnecting
        through the wait; once logged in it stays connected."""
        while True:
            try:
                call()
            except ServerBusy as exc:
                if self._state != ConnectionState.CONNECTED:
                    self._state = ConnectionState.RECONNECTING
                logger.warning(f"MT5 async reconnect: {exc}, asking again in {exc.retry_after_s}s")
                await asyncio.sleep(exc.retry_after_s)
            else:
                return

    # ── Data accessors ────────────────────────────────────────────────────────

    def get_account_info(self) -> AccountSnapshot:
        """The account as the terminal reports it now; raises MT5ConnectionError when not connected
        or when the read fails."""
        self.ensure_connected()
        info = self.mt5.account_info()
        if info is None:
            code, msg = self.mt5.last_error()
            raise MT5ConnectionError(f"mt5.account_info() returned None — error {code}: {msg}")
        return AccountSnapshot.from_mt5(info)

    def get_terminal_info(self) -> dict:
        """The server's MT5 terminal as `terminal_info()` reports it, its trading permission and
        connection included; raises MT5ConnectionError when not connected or when the read fails."""
        self.ensure_connected()
        info = self.mt5.terminal_info()
        if info is None:
            code, msg = self.mt5.last_error()
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

    # ── Private ───────────────────────────────────────────────────────────────

    def _initialize(self) -> None:
        """Asks the server to initialize its MT5 terminal; a failure leaves the connection
        DISCONNECTED."""
        logger.debug("MT5Connection: calling mt5.initialize()")
        self._state = ConnectionState.INITIALIZING

        try:
            ok = self.mt5.initialize()
        except MT5ConnectionError:
            self._state = ConnectionState.DISCONNECTED
            raise
        if not ok:
            code, msg = self.mt5.last_error()
            self._state = ConnectionState.DISCONNECTED
            raise MT5ConnectionError(
                f"mt5.initialize() failed on the server's MT5 terminal — error {code}: {msg}"
            )

        self._state = ConnectionState.INITIALIZED
        logger.debug("MT5Connection: server initialized")

    def _login(self) -> None:
        """Logs in to the broker on an initialized terminal; a failure leaves the connection
        INITIALIZED."""
        logger.debug("MT5Connection: logging in")
        self._state = ConnectionState.LOGGING_IN

        try:
            ok = self.mt5.login(
                login=self._config.account,
                password=self._config.password,
                server=self._config.server,
                timeout=int(self._config.timeout_s * 1000),  # mt5 wants milliseconds
            )
        except MT5ConnectionError:
            self._state = ConnectionState.INITIALIZED
            raise

        if not ok:
            code, msg = self.mt5.last_error()
            self._state = ConnectionState.INITIALIZED
            raise MT5LoginError(f"mt5.login() failed — error {code}: {msg}")

        self._state = ConnectionState.CONNECTED

    def _log_connected(self) -> None:
        info = self.mt5.account_info()
        if info:
            logger.info(f"MT5Connection: connected — {AccountSnapshot.from_mt5(info)}")
        else:
            logger.info("MT5Connection: connected")

    # ── Context manager ───────────────────────────────────────────────────────

    def __enter__(self) -> MT5Connection:
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self.disconnect()
        return False  # never suppress exceptions

    def __repr__(self) -> str:
        return f"MT5Connection(state={self._state.name})"
