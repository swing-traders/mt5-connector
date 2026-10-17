"""The adapter's connection to the MT5 terminal behind the hub, and the account as the terminal
reports it.

State: where the connection is in its lifecycle; its transport to the server; and while connected,
its hub session and the event loop the session runs on, on the connection's own thread. A connection
not connected runs no thread. A lost hub leaves the connection connected: NT's client reconnects its
session."""

from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Coroutine
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum, StrEnum, auto
from typing import TYPE_CHECKING, Any, TypeVar

from nautilus_trader.core.nautilus_pyo3 import WebSocketClientError

from mt5connector.client.errors import Deferred, MT5ConnectionError, NotReady, Syncing
from mt5connector.client.hub import HubSession
from mt5connector.client.remote_mt5 import RemoteMT5
from mt5connector.wire import mirror
from mt5connector.wire.binary import ErrorKind

if TYPE_CHECKING:
    from mt5connector.client.config import MT5Config

logger = logging.getLogger(__name__)

_T = TypeVar("_T")


# ─────────────────────────────────────────────────────────────────────────────
# CONNECTION STATE
# ─────────────────────────────────────────────────────────────────────────────


class ConnectionState(Enum):
    """Tracks exactly where in the lifecycle the connection is."""

    DISCONNECTED = auto()  # nothing attempted yet, or disconnected
    CONNECTING = auto()  # the hub session connecting and the login being confirmed
    CONNECTED = auto()  # fully ready to use
    SHUTTING_DOWN = auto()  # the hub session disconnecting


_CONNECTED_OR_CONNECTING = frozenset({ConnectionState.CONNECTING, ConnectionState.CONNECTED})


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
    """The connection to the MT5 terminal behind one hub, shared by the clients of one account: its
    lifecycle, the hub session its reads ride, and the transport every server call travels on."""

    def __init__(self, config: MT5Config) -> None:
        self._config = config
        self.mt5 = RemoteMT5(config.server_url)
        self._state = ConnectionState.DISCONNECTED
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._session: HubSession | None = None

    # ── Core lifecycle ────────────────────────────────────────────────────────

    def connect(self) -> None:
        """Connects the hub session on the connection's own thread and confirms the terminal's login
        by reading its account. Raises MT5ConnectionError naming the state on a connection already
        connected or connecting, for a hub it cannot reach and for one not ready within the config's
        timeout, and the hub's error for an account read it fails."""
        if self._state in _CONNECTED_OR_CONNECTING:
            raise MT5ConnectionError(
                f"MT5 already connected or connecting (state={self._state.name})"
            )
        self._state = ConnectionState.CONNECTING
        try:
            self._open()
        except BaseException:
            self._state = ConnectionState.DISCONNECTED
            raise
        self._state = ConnectionState.CONNECTED

    def disconnect(self) -> None:
        """Disconnects the hub session, stops the connection's thread and closes the transport's
        HTTP session, the last two even when the first raises; raises MT5ConnectionError naming the
        state on a connection not connected."""
        if self._state != ConnectionState.CONNECTED:
            raise MT5ConnectionError(f"MT5 not connected (state={self._state.name})")
        logger.info("MT5Connection: shutting down")
        self._state = ConnectionState.SHUTTING_DOWN
        try:
            asyncio.run_coroutine_threadsafe(self._session.disconnect(), self._loop).result()
        finally:
            _stop(self._loop, self._thread)
            self._loop = None
            self._thread = None
            self._session = None
            self.mt5.close_session()
            self._state = ConnectionState.DISCONNECTED
        logger.info("MT5Connection: disconnected")

    def ensure_connected(self) -> None:
        """Returns at once while connected; otherwise raises MT5ConnectionError naming the state."""
        if self._state != ConnectionState.CONNECTED:
            raise MT5ConnectionError(f"MT5 not connected (state={self._state.name})")

    # ── The hub session ───────────────────────────────────────────────────────

    @property
    def session(self) -> HubSession:
        """The hub session, for a coroutine `run` is handed; raises RuntimeError when not
        connected."""
        if self._state != ConnectionState.CONNECTED:
            raise RuntimeError(f"MT5 not connected (state={self._state.name})")
        return self._session

    def run(self, coroutine: Coroutine[Any, Any, _T]) -> _T:
        """Runs a coroutine on the session's loop and blocks for its result, raising what it raised;
        raises RuntimeError when not connected."""
        if self._state != ConnectionState.CONNECTED:
            coroutine.close()
            raise RuntimeError(f"MT5 not connected (state={self._state.name})")
        return asyncio.run_coroutine_threadsafe(coroutine, self._loop).result()

    # ── Data accessors ────────────────────────────────────────────────────────

    def get_account_info(self) -> AccountSnapshot:
        """The account as the terminal reports it now; raises MT5ConnectionError when not connected,
        and the hub's error when the read fails."""
        self.ensure_connected()
        return AccountSnapshot.from_mt5(self.run(self._session.account_info()).result)

    def get_terminal_info(self) -> dict:
        """The hub's MT5 terminal as `terminal_info()` reports it, its trading permission and
        connection included; raises MT5ConnectionError when not connected, and the hub's error when
        the read fails."""
        self.ensure_connected()
        info = self.run(self._session.terminal_info()).result
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

    def _open(self) -> None:
        """Starts a loop on a thread of the connection's, connects a hub session on it and confirms
        the login; a failure stops the thread."""
        loop = asyncio.new_event_loop()
        thread = threading.Thread(target=loop.run_forever, name="MT5Connection", daemon=True)
        thread.start()
        session = HubSession(self._config, loop, _unsubscribed, None, _SessionLog())
        try:
            snapshot = asyncio.run_coroutine_threadsafe(self._logged_in(session), loop).result()
        except BaseException:
            _stop(loop, thread)
            raise
        self._loop = loop
        self._thread = thread
        self._session = session
        logger.info(f"MT5Connection: connected — {snapshot}")

    async def _logged_in(self, session: HubSession) -> AccountSnapshot:
        """The terminal's account, read once the session connects; the session is disconnected
        again when the read fails or the hub stays not ready past the config's timeout."""
        try:
            await session.connect()
        except WebSocketClientError as exc:
            raise MT5ConnectionError(f"hub at {self._config.ws_display_url}: {exc}") from None
        try:
            return AccountSnapshot.from_mt5(await self._account(session))
        except BaseException:
            await session.disconnect()
            raise

    async def _account(self, session: HubSession) -> object:
        """The account, asked again after each deferring answer's delay; raises MT5ConnectionError
        naming the last deferral when no other answer comes within the config's timeout."""
        deferred: Deferred | None = None

        async def asked() -> object:
            nonlocal deferred
            while True:
                try:
                    return (await session.account_info()).result
                except Deferred as exc:
                    deferred = exc
                    await asyncio.sleep(exc.retry_after_s)

        hub = f"hub at {self._config.ws_display_url}"
        try:
            return await asyncio.wait_for(asked(), self._config.timeout_s)
        except TimeoutError:
            if deferred is None:
                raise MT5ConnectionError(
                    f"{hub}: account_info unanswered within {self._config.timeout_s}s"
                ) from None
            else:
                raise MT5ConnectionError(
                    f"{hub}: not ready within {self._config.timeout_s}s, the last answer "
                    f"{_deferral(deferred).name.lower()}: {deferred}"
                ) from None

    # ── Context manager ───────────────────────────────────────────────────────

    def __enter__(self) -> MT5Connection:
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self.disconnect()
        return False  # never suppress exceptions

    def __repr__(self) -> str:
        return f"MT5Connection(state={self._state.name})"


def _deferral(deferred: Deferred) -> ErrorKind:
    """The error kind of a deferring answer."""
    if isinstance(deferred, NotReady):
        return ErrorKind.NOT_READY
    elif isinstance(deferred, Syncing):
        return ErrorKind.SYNCING
    else:
        return ErrorKind.NO_PUBLISHER


def _stop(loop: asyncio.AbstractEventLoop, thread: threading.Thread) -> None:
    loop.call_soon_threadsafe(loop.stop)
    thread.join()
    loop.close()


def _unsubscribed(frame: object) -> None:
    raise ValueError(f"a {type(frame).__name__} frame on the connection's session")


class _SessionLog:
    """The connection's hub session's log, in NT's Logger's shape, on the module's logger."""

    def debug(self, message: str) -> None:
        logger.debug(message)

    def info(self, message: str) -> None:
        logger.info(message)

    def warning(self, message: str) -> None:
        logger.warning(message)

    def error(self, message: str) -> None:
        logger.error(message)

    def exception(self, message: str, ex: BaseException) -> None:
        logger.error(message, exc_info=ex)
