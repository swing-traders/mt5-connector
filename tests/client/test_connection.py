"""MT5Connection against a patched shim: its states, connect, disconnect and reconnect, the account
and terminal reads, the account snapshot, and the credentials none of them shows. A test observes
the connection's state through what ensure_connected answers."""

import asyncio
import logging
from dataclasses import replace
from unittest.mock import patch

import pytest
from nautilus_trader.common.component import LiveClock
from venue_doubles import account_info

from mt5connector.client import connection
from mt5connector.client.config import MT5Config
from mt5connector.client.connection import AccountSnapshot, ConnectionState, MT5Connection
from mt5connector.client.errors import (
    MT5ConnectionError,
    MT5LoginError,
    ServerBusy,
    ServerUnreachable,
)
from mt5connector.client.factories import _connection_registry, _get_or_create_connection


def assert_not_connected(conn, state: str) -> None:
    """The connection refuses to be used, naming `state`."""
    with pytest.raises(MT5ConnectionError, match=rf"^MT5 not connected \(state={state}\)$"):
        conn.ensure_connected()


# ═════════════════════════════════════════════════════════════════════════════
# 1. ConnectionState enum
# ═════════════════════════════════════════════════════════════════════════════


class TestConnectionState:

    def test_all_states_exist(self):
        states = {s.name for s in ConnectionState}
        assert states == {
            "DISCONNECTED",
            "INITIALIZING",
            "INITIALIZED",
            "LOGGING_IN",
            "CONNECTED",
            "RECONNECTING",
            "SHUTTING_DOWN",
            "FAILED",
        }

    def test_states_are_unique(self):
        values = [s.value for s in ConnectionState]
        assert len(values) == len(set(values))


# ═════════════════════════════════════════════════════════════════════════════
# 2. Initial state
# ═════════════════════════════════════════════════════════════════════════════


class TestInitialState:

    def test_starts_disconnected(self, config, mock_mt5):
        conn = MT5Connection(config)
        assert_not_connected(conn, "DISCONNECTED")

    def test_repr_shows_disconnected(self, config, mock_mt5):
        conn = MT5Connection(config)
        assert "DISCONNECTED" in repr(conn)


# ═════════════════════════════════════════════════════════════════════════════
# 3. Successful connect / disconnect
# ═════════════════════════════════════════════════════════════════════════════


class TestSuccessfulConnect:

    def test_connect_binds_the_shim_to_the_configured_server_before_initializing(
        self, config, mock_mt5
    ):
        conn = MT5Connection(config)
        conn.connect()
        mock_mt5.configure.assert_called_once_with("http://127.0.0.1:5000", "ws://127.0.0.1:9000")
        assert [name for name, _, _ in mock_mt5.mock_calls][:2] == ["configure", "initialize"]

    def test_connect_calls_initialize_and_login(self, config, mock_mt5):
        conn = MT5Connection(config)
        conn.connect()
        mock_mt5.initialize.assert_called_once()
        mock_mt5.login.assert_called_once_with(
            login=12345678,
            password="test_password",
            server="Exness-MT5Trial1",
            timeout=5000,
        )

    def test_connected_after_connect(self, config, mock_mt5):
        conn = MT5Connection(config)
        conn.connect()
        conn.ensure_connected()

    def test_disconnect_calls_mt5_shutdown(self, config, mock_mt5):
        conn = MT5Connection(config)
        conn.connect()
        conn.disconnect()
        mock_mt5.shutdown.assert_called_once()

    def test_state_is_disconnected_after_disconnect(self, config, mock_mt5):
        conn = MT5Connection(config)
        conn.connect()
        conn.disconnect()
        assert_not_connected(conn, "DISCONNECTED")

    def test_repr_shows_connected(self, config, mock_mt5):
        conn = MT5Connection(config)
        conn.connect()
        assert "CONNECTED" in repr(conn)


# ═════════════════════════════════════════════════════════════════════════════
# 4. initialize() failure
# ═════════════════════════════════════════════════════════════════════════════


class TestInitializeFailure:

    def test_raises_connection_error_when_initialize_fails(self, config, mock_mt5):
        mock_mt5.initialize.return_value = False
        mock_mt5.last_error.return_value = (5, "IPC timeout")
        conn = MT5Connection(config)
        with pytest.raises(MT5ConnectionError) as exc_info:
            conn.connect()
        assert "mt5.initialize() failed" in str(exc_info.value)
        assert "5" in str(exc_info.value)
        assert "IPC timeout" in str(exc_info.value)

    def test_state_is_disconnected_after_initialize_failure(self, config, mock_mt5):
        mock_mt5.initialize.return_value = False
        mock_mt5.last_error.return_value = (5, "IPC timeout")
        conn = MT5Connection(config)
        with pytest.raises(MT5ConnectionError):
            conn.connect()
        assert_not_connected(conn, "DISCONNECTED")

    def test_helpful_message_mentions_terminal(self, config, mock_mt5):
        mock_mt5.initialize.return_value = False
        mock_mt5.last_error.return_value = (5, "IPC timeout")
        conn = MT5Connection(config)
        with pytest.raises(MT5ConnectionError) as exc_info:
            conn.connect()
        assert "MT5 terminal" in str(exc_info.value)


# ═════════════════════════════════════════════════════════════════════════════
# 5. login() failure
# ═════════════════════════════════════════════════════════════════════════════


class TestLoginFailure:

    def test_raises_login_error_when_login_fails(self, config, mock_mt5):
        mock_mt5.login.return_value = False
        mock_mt5.last_error.return_value = (65537, "Invalid account")
        conn = MT5Connection(config)
        with pytest.raises(MT5LoginError) as exc_info:
            conn.connect()
        assert "mt5.login() failed" in str(exc_info.value)
        assert "12345678" not in str(exc_info.value)

    def test_state_stays_initialized_after_login_failure(self, config, mock_mt5):
        """Terminal IPC is up, only login failed — state must be INITIALIZED not DISCONNECTED."""
        mock_mt5.login.return_value = False
        mock_mt5.last_error.return_value = (65537, "Invalid account")
        conn = MT5Connection(config)
        with pytest.raises(MT5LoginError):
            conn.connect()
        assert_not_connected(conn, "INITIALIZED")

    def test_login_error_names_the_terminals_error(self, config, mock_mt5):
        mock_mt5.login.return_value = False
        mock_mt5.last_error.return_value = (65537, "Invalid account")
        conn = MT5Connection(config)
        with pytest.raises(MT5LoginError) as exc_info:
            conn.connect()
        assert "65537" in str(exc_info.value)
        assert "Invalid account" in str(exc_info.value)

    def test_login_not_called_if_initialize_failed(self, config, mock_mt5):
        mock_mt5.initialize.return_value = False
        mock_mt5.last_error.return_value = (5, "IPC timeout")
        conn = MT5Connection(config)
        with pytest.raises(MT5ConnectionError):
            conn.connect()
        mock_mt5.login.assert_not_called()


# ═════════════════════════════════════════════════════════════════════════════
# 6. ensure_connected() — all states
# ═════════════════════════════════════════════════════════════════════════════


class TestEnsureConnected:

    def test_passes_when_connected(self, config, mock_mt5):
        conn = MT5Connection(config)
        conn.connect()
        conn.ensure_connected()  # must not raise

    def test_raises_when_disconnected(self, config, mock_mt5):
        conn = MT5Connection(config)
        with pytest.raises(MT5ConnectionError) as exc_info:
            conn.ensure_connected()
        assert str(exc_info.value) == "MT5 not connected (state=DISCONNECTED)"

    def test_raises_when_failed(self, config, mock_mt5):
        conn = MT5Connection(config)
        conn._state = ConnectionState.FAILED
        with pytest.raises(MT5ConnectionError) as exc_info:
            conn.ensure_connected()
        assert str(exc_info.value) == (
            f"MT5 connection gave up after {config.reconnect_max_attempts} reconnect attempts"
        )

    def test_raises_when_initializing(self, config, mock_mt5):
        conn = MT5Connection(config)
        conn._state = ConnectionState.INITIALIZING
        with pytest.raises(MT5ConnectionError):
            conn.ensure_connected()

    def test_raises_when_reconnecting(self, config, mock_mt5):
        conn = MT5Connection(config)
        conn._state = ConnectionState.RECONNECTING
        with pytest.raises(MT5ConnectionError):
            conn.ensure_connected()

    def test_is_fast_when_connected(self, config, mock_mt5):
        """ensure_connected() must be O(1) — no mt5 calls in the fast path."""
        conn = MT5Connection(config)
        conn.connect()
        initial_call_count = mock_mt5.terminal_info.call_count
        for _ in range(1000):
            conn.ensure_connected()
        # No additional mt5 calls from ensure_connected itself
        assert mock_mt5.terminal_info.call_count == initial_call_count


# ═════════════════════════════════════════════════════════════════════════════
# 7. Reconnect — success path
# ═════════════════════════════════════════════════════════════════════════════


class TestReconnectAsync:

    @pytest.mark.asyncio
    async def test_async_reconnect_succeeds(self, config, mock_mt5):
        conn = MT5Connection(config)
        result = await conn.reconnect_async()
        assert result is True

    @pytest.mark.asyncio
    async def test_async_reconnect_connects(self, config, mock_mt5):
        conn = MT5Connection(config)
        await conn.reconnect_async()
        conn.ensure_connected()
        mock_mt5.shutdown.assert_called()
        mock_mt5.initialize.assert_called_once()
        mock_mt5.login.assert_called_once()

    @pytest.mark.asyncio
    async def test_async_reconnect_resets_attempt_counter(self, config, mock_mt5):
        conn = MT5Connection(config)
        conn._attempt = 1
        await conn.reconnect_async()
        assert conn._attempt == 0

    @pytest.mark.asyncio
    async def test_async_reconnect_succeeds_after_initial_failures(self, config, mock_mt5):
        call_count = {"n": 0}

        def flaky_init():
            call_count["n"] += 1
            return call_count["n"] >= 2

        mock_mt5.initialize.side_effect = flaky_init

        conn = MT5Connection(config)
        result = await conn.reconnect_async()
        assert result is True


# ═════════════════════════════════════════════════════════════════════════════
# 8. Reconnect — failure / max attempts
# ═════════════════════════════════════════════════════════════════════════════


class TestReconnectAsyncFailure:

    @pytest.mark.asyncio
    async def test_async_reconnect_returns_false_on_exhaustion(self, config, mock_mt5):
        mock_mt5.initialize.return_value = False
        mock_mt5.last_error.return_value = (5, "IPC timeout")
        conn = MT5Connection(config)
        result = await conn.reconnect_async()
        assert result is False

    @pytest.mark.asyncio
    async def test_a_connection_given_up_on_names_its_attempts(self, config, mock_mt5):
        mock_mt5.initialize.return_value = False
        mock_mt5.last_error.return_value = (5, "IPC timeout")
        conn = MT5Connection(config)
        await conn.reconnect_async()
        assert mock_mt5.initialize.call_count == config.reconnect_max_attempts
        with pytest.raises(MT5ConnectionError) as exc_info:
            conn.ensure_connected()
        assert str(exc_info.value) == (
            f"MT5 connection gave up after {config.reconnect_max_attempts} reconnect attempts"
        )


class TestReconnectThroughBusy:
    """A busy server is up: a reconnect waits out its delay and asks the same call again."""

    @pytest.mark.parametrize(
        ("busy_call", "answer"),
        [("shutdown", None), ("initialize", True), ("login", True)],
    )
    async def test_a_busy_answer_is_asked_again_after_its_delay_at_no_attempts_cost(
        self, config, mock_mt5, busy_call, answer
    ):
        getattr(mock_mt5, busy_call).side_effect = [
            ServerBusy(f"{busy_call}: server busy", 7),
            answer,
        ]
        conn = MT5Connection(replace(config, reconnect_max_attempts=1))
        delays = []
        states = []

        async def wait(delay):
            delays.append(delay)
            with pytest.raises(MT5ConnectionError) as refused:
                conn.ensure_connected()
            states.append(str(refused.value))

        with patch.object(connection.asyncio, "sleep", wait):
            assert await conn.reconnect_async() is True

        steps = ["shutdown", "initialize", "login"]
        asked = [name for name, _, _ in mock_mt5.mock_calls if name in steps]
        expected = steps[: steps.index(busy_call) + 1] + steps[steps.index(busy_call) :]
        assert asked == expected
        assert delays == [config.reconnect_initial_delay_s, 7]
        assert all("RECONNECTING" in state for state in states)
        conn.ensure_connected()

    async def test_a_busy_account_read_after_the_login_leaves_the_connection_connected(
        self, config, mock_mt5
    ):
        account = mock_mt5.account_info.return_value
        mock_mt5.account_info.side_effect = [ServerBusy("account_info: server busy", 7), account]
        conn = MT5Connection(replace(config, reconnect_max_attempts=1))
        delays = []

        async def wait(delay):
            delays.append(delay)

        with patch.object(connection.asyncio, "sleep", wait):
            assert await conn.reconnect_async() is True

        conn.ensure_connected()
        steps = ["shutdown", "initialize", "login", "account_info"]
        asked = [name for name, _, _ in mock_mt5.mock_calls if name in steps]
        assert asked == steps + ["account_info"]
        assert delays == [config.reconnect_initial_delay_s, 7]


class TestReconnectAsyncSerialised:
    """Clients sharing one connection reconnect it once between them."""

    async def test_a_concurrent_second_caller_awaits_the_first_reconnect(self, config, mock_mt5):
        conn = MT5Connection(config)
        first, second = await asyncio.gather(conn.reconnect_async(), conn.reconnect_async())
        assert (first, second) == (True, True)
        assert mock_mt5.initialize.call_count == 1
        assert mock_mt5.login.call_count == 1
        conn.ensure_connected()

    async def test_a_concurrent_second_caller_shares_the_first_ones_failure(self, config, mock_mt5):
        mock_mt5.initialize.return_value = False
        mock_mt5.last_error.return_value = (5, "IPC timeout")
        conn = MT5Connection(config)
        first, second = await asyncio.gather(conn.reconnect_async(), conn.reconnect_async())
        assert (first, second) == (False, False)
        assert mock_mt5.initialize.call_count == config.reconnect_max_attempts
        with pytest.raises(MT5ConnectionError, match="gave up"):
            conn.ensure_connected()


# ═════════════════════════════════════════════════════════════════════════════
# 9. get_account_info()
# ═════════════════════════════════════════════════════════════════════════════


class TestGetAccountInfo:

    def test_returns_account_snapshot(self, config, mock_mt5):
        conn = MT5Connection(config)
        conn.connect()
        info = conn.get_account_info()
        assert isinstance(info, AccountSnapshot)

    def test_account_fields_correct(self, config, mock_mt5):
        conn = MT5Connection(config)
        conn.connect()
        info = conn.get_account_info()
        assert info.login == 12345678
        assert info.server == "Exness-MT5Trial1"
        assert info.balance == 10000.00
        assert info.currency == "USD"
        assert info.leverage == 2000

    def test_raises_if_not_connected(self, config, mock_mt5):
        conn = MT5Connection(config)
        with pytest.raises(MT5ConnectionError):
            conn.get_account_info()

    def test_raises_when_account_info_returns_none(self, config, mock_mt5):
        mock_mt5.account_info.return_value = None
        mock_mt5.last_error.return_value = (6, "No connection")
        conn = MT5Connection(config)
        conn.connect()
        with pytest.raises(MT5ConnectionError) as exc_info:
            conn.get_account_info()
        assert "None" in str(exc_info.value)

    def test_account_snapshot_str_contains_balance(self, config, mock_mt5):
        conn = MT5Connection(config)
        conn.connect()
        info = conn.get_account_info()
        s = str(info)
        assert "10000.00" in s
        assert "USD" in s
        assert "2000" in s


# ═════════════════════════════════════════════════════════════════════════════
# 10. get_terminal_info()
# ═════════════════════════════════════════════════════════════════════════════


class TestGetTerminalInfo:

    def test_returns_dict(self, config, mock_mt5):
        conn = MT5Connection(config)
        conn.connect()
        info = conn.get_terminal_info()
        assert isinstance(info, dict)

    def test_contains_expected_keys(self, config, mock_mt5):
        conn = MT5Connection(config)
        conn.connect()
        info = conn.get_terminal_info()
        assert "name" in info
        assert "connected" in info
        assert "ping_last" in info

    def test_raises_when_terminal_info_returns_none(self, config, mock_mt5):
        mock_mt5.terminal_info.return_value = None
        mock_mt5.last_error.return_value = (6, "No connection")
        conn = MT5Connection(config)
        conn.connect()
        with pytest.raises(MT5ConnectionError, match="None"):
            conn.get_terminal_info()

    def test_raises_when_not_connected(self, config, mock_mt5):
        conn = MT5Connection(config)
        with pytest.raises(MT5ConnectionError):
            conn.get_terminal_info()


# ═════════════════════════════════════════════════════════════════════════════
# 11. Context manager
# ═════════════════════════════════════════════════════════════════════════════


class TestContextManager:

    def test_connects_on_enter(self, config, mock_mt5):
        with MT5Connection(config) as conn:
            conn.ensure_connected()

    def test_disconnects_on_exit(self, config, mock_mt5):
        with MT5Connection(config) as conn:
            pass
        assert_not_connected(conn, "DISCONNECTED")
        mock_mt5.shutdown.assert_called_once()

    def test_disconnects_on_exception(self, config, mock_mt5):
        conn = None
        try:
            with MT5Connection(config) as c:
                conn = c
                raise ValueError("strategy error")
        except ValueError:
            pass
        assert_not_connected(conn, "DISCONNECTED")

    def test_does_not_suppress_exceptions(self, config, mock_mt5):
        with pytest.raises(ValueError):
            with MT5Connection(config):
                raise ValueError("should propagate")


# ═════════════════════════════════════════════════════════════════════════════
# 12. __repr__
# ═════════════════════════════════════════════════════════════════════════════


class TestRepr:

    def test_repr_omits_the_login(self, config, mock_mt5):
        conn = MT5Connection(config)
        assert "12345678" not in repr(conn)

    def test_repr_omits_the_server(self, config, mock_mt5):
        conn = MT5Connection(config)
        assert "Exness-MT5Trial1" not in repr(conn)

    def test_repr_contains_state(self, config, mock_mt5):
        conn = MT5Connection(config)
        assert "DISCONNECTED" in repr(conn)
        conn.connect()
        assert "CONNECTED" in repr(conn)


# ═════════════════════════════════════════════════════════════════════════════
# 13. Backoff delay calculation
# ═════════════════════════════════════════════════════════════════════════════


class TestBackoffDelays:

    async def test_delay_doubles_each_attempt_up_to_the_max(self, config, mock_mt5):
        mock_mt5.initialize.return_value = False
        mock_mt5.last_error.return_value = (5, "IPC timeout")
        delays_seen = []

        async def backoff(delay):
            delays_seen.append(delay)

        with patch.object(connection.asyncio, "sleep", backoff):
            await MT5Connection(config).reconnect_async()

        assert delays_seen == [0.01, 0.02, 0.04]
        assert all(delay <= config.reconnect_max_delay_s for delay in delays_seen)


# ═════════════════════════════════════════════════════════════════════════════
# 14. Lifecycle transitions
# ═════════════════════════════════════════════════════════════════════════════


class TestLifecycleTransitions:

    def test_a_disconnect_on_a_connection_never_connected_is_refused_naming_its_state(
        self, config, mock_mt5
    ):
        conn = MT5Connection(config)
        with pytest.raises(MT5ConnectionError, match="DISCONNECTED"):
            conn.disconnect()
        mock_mt5.shutdown.assert_not_called()

    def test_a_second_disconnect_is_refused_and_shuts_down_once(self, config, mock_mt5):
        conn = MT5Connection(config)
        conn.connect()
        conn.disconnect()
        with pytest.raises(MT5ConnectionError, match="DISCONNECTED"):
            conn.disconnect()
        mock_mt5.shutdown.assert_called_once()

    def test_a_connect_on_a_connected_connection_is_refused_naming_its_state(
        self, config, mock_mt5
    ):
        conn = MT5Connection(config)
        conn.connect()
        with pytest.raises(MT5ConnectionError, match="CONNECTED"):
            conn.connect()
        mock_mt5.configure.assert_called_once()
        mock_mt5.initialize.assert_called_once()
        mock_mt5.login.assert_called_once()

    async def test_a_connect_while_reconnecting_is_refused_naming_its_state(self, config, mock_mt5):
        mock_mt5.initialize.return_value = False
        mock_mt5.last_error.return_value = (5, "IPC timeout")
        conn = MT5Connection(config)
        refusals = []

        async def backoff(delay):
            with pytest.raises(MT5ConnectionError) as refused:
                conn.connect()
            refusals.append(str(refused.value))

        with patch.object(connection.asyncio, "sleep", backoff):
            assert await conn.reconnect_async() is False

        assert len(refusals) == config.reconnect_max_attempts
        assert all("RECONNECTING" in refusal for refusal in refusals)
        assert mock_mt5.initialize.call_count == config.reconnect_max_attempts

    def test_a_connect_the_server_did_not_answer_can_be_made_again(self, config, mock_mt5):
        mock_mt5.initialize.side_effect = [ServerUnreachable("initialize: refused"), True]
        conn = MT5Connection(config)
        with pytest.raises(ServerUnreachable):
            conn.connect()
        conn.connect()
        conn.ensure_connected()

    def test_a_login_the_server_did_not_answer_can_be_made_again(self, config, mock_mt5):
        mock_mt5.login.side_effect = [ServerUnreachable("login: refused"), True]
        conn = MT5Connection(config)
        with pytest.raises(ServerUnreachable):
            conn.connect()
        conn.connect()
        conn.ensure_connected()


# ═════════════════════════════════════════════════════════════════════════════
# 15. State integrity after a failed connect
# ═════════════════════════════════════════════════════════════════════════════


class TestStateIntegrity:

    def test_login_failure_leaves_state_initialized_not_disconnected(self, config, mock_mt5):
        """A failed login leaves the terminal initialized: INITIALIZED, never DISCONNECTED."""
        mock_mt5.login.return_value = False
        mock_mt5.last_error.return_value = (65537, "Invalid account")
        conn = MT5Connection(config)
        with pytest.raises(MT5LoginError):
            conn.connect()
        assert_not_connected(conn, "INITIALIZED")

    def test_initialize_failure_leaves_state_disconnected(self, config, mock_mt5):
        mock_mt5.initialize.return_value = False
        mock_mt5.last_error.return_value = (5, "IPC timeout")
        conn = MT5Connection(config)
        with pytest.raises(MT5ConnectionError):
            conn.connect()
        assert_not_connected(conn, "DISCONNECTED")


# ═════════════════════════════════════════════════════════════════════════════
# 16. AccountSnapshot
# ═════════════════════════════════════════════════════════════════════════════


class TestAccountSnapshot:

    def test_from_mt5_builds_correctly(self, config, mock_mt5):
        conn = MT5Connection(config)
        conn.connect()
        snap = conn.get_account_info()
        assert snap.login == 12345678
        assert snap.equity == 10050.25
        assert snap.leverage == 2000
        assert snap.company == "Exness Technologies Ltd"

    def test_str_carries_the_account_but_not_its_login(self, config, mock_mt5):
        conn = MT5Connection(config)
        conn.connect()
        snap = conn.get_account_info()
        s = str(snap)
        assert "12345678" not in s
        assert "10000.00" in s
        assert "USD" in s
        assert "2000" in s


# ═════════════════════════════════════════════════════════════════════════════
# 17. Credentials
# ═════════════════════════════════════════════════════════════════════════════

SERVER = "Zq7Broker-Live42"
LOGIN = 918273645
PASSWORD = "pw-Zq7-secret"


def secret_config() -> MT5Config:
    return MT5Config(
        account=LOGIN,
        password=PASSWORD,
        server=SERVER,
        symbols=["EURUSD"],
        server_url="http://127.0.0.1:5000",
        reconnect_initial_delay_s=0,
        reconnect_max_attempts=1,
    )


def assert_names_no_credential(text: str) -> None:
    for credential in (SERVER, str(LOGIN), PASSWORD):
        assert credential not in text


class TestCredentials:
    """The broker server, the login and the password reach no log, repr, string or exception."""

    @pytest.fixture
    def secret_mt5(self, mock_mt5):
        mock_mt5.account_info.return_value = account_info(login=LOGIN, server=SERVER)
        return mock_mt5

    def test_the_config_shows_none(self):
        config = secret_config()
        assert_names_no_credential(repr(config))
        assert_names_no_credential(str(config))

    def test_the_account_snapshot_shows_none(self, secret_mt5):
        conn = MT5Connection(secret_config())
        conn.connect()
        snapshot = conn.get_account_info()
        assert (snapshot.login, snapshot.server) == (LOGIN, SERVER)
        assert_names_no_credential(repr(snapshot))
        assert_names_no_credential(str(snapshot))

    def test_the_connection_shows_none(self, secret_mt5):
        conn = MT5Connection(secret_config())
        assert_names_no_credential(repr(conn) + str(conn))
        conn.connect()
        assert_names_no_credential(repr(conn) + str(conn))

    def test_a_failed_login_names_none(self, secret_mt5):
        secret_mt5.login.return_value = False
        secret_mt5.last_error.return_value = (-6, "Terminal: Authorization failed")
        with pytest.raises(MT5LoginError) as failed:
            MT5Connection(secret_config()).connect()
        assert_names_no_credential(str(failed.value))
        assert_names_no_credential(repr(failed.value))

    async def test_the_logs_name_none(self, secret_mt5, caplog):
        caplog.set_level(logging.DEBUG, logger="mt5connector")
        secret_mt5.login.side_effect = [True, False, True]
        secret_mt5.last_error.return_value = (-6, "Terminal: Authorization failed")
        try:
            conn, _ = _get_or_create_connection(
                replace(secret_config(), reconnect_max_attempts=2), LiveClock()
            )
            conn.disconnect()
            assert await conn.reconnect_async() is True
            conn.disconnect()
        finally:
            _connection_registry.clear()
        assert caplog.records
        assert_names_no_credential("\n".join(record.getMessage() for record in caplog.records))
