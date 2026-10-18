"""MT5Connection against a hub double: its states, connect and disconnect on its own thread, the
synchronous bridge onto its hub session, the account and terminal reads, the account snapshot, and
the credentials none of them shows. A test observes the connection's state through what
ensure_connected answers, and calls each blocking method off the loop the hub double runs on."""

import asyncio
import logging
import threading
import time
from dataclasses import replace

import numpy as np
import pytest
from hub_double import zeroed
from venue_doubles import account_info
from websockets.asyncio.server import serve

from mt5connector.client import history
from mt5connector.client.config import MT5Config
from mt5connector.client.connection import AccountSnapshot, ConnectionState, MT5Connection
from mt5connector.client.errors import EaError, InvalidWindow, MT5ConnectionError
from mt5connector.client.factories import _connection_registry, _ensure_connection
from mt5connector.wire import binary, calls
from mt5connector.wire.binary import ErrorKind
from mt5connector.wire.history_wire import BARS, Series

Call = calls.Call
NINE = 1_752_570_000
NOT_READY = binary.AnswerError(ErrorKind.NOT_READY, "the broker clock is not verified", 50)
EA_ERROR = binary.AnswerError(ErrorKind.EA_ERROR, "the spawner's read failed")
TERMINAL = zeroed(
    calls.CALLS[Call.TERMINAL_INFO].layout,
    name="MetaTrader 5",
    path="C:\\Program Files\\MetaTrader 5",
    data_path="C:\\Program Files\\MetaTrader 5",
    connected=True,
    trade_allowed=True,
    ping_last=3,
    retransmission=0.0,
)


def assert_not_connected(conn, state: str) -> None:
    """The connection refuses to be used, naming `state`."""
    with pytest.raises(MT5ConnectionError, match=rf"^MT5 not connected \(state={state}\)$"):
        conn.ensure_connected()


def threads() -> set[threading.Thread]:
    return {thread for thread in threading.enumerate() if thread.name == "MT5Connection"}


def off_loop(call, *args):
    """A blocking call of the connection's, run off the loop the hub double serves on."""
    return asyncio.to_thread(call, *args)


@pytest.fixture
def ready(hub, config):
    """A config on the hub double, which answers the account and the terminal."""
    hub.answers[Call.ACCOUNT_INFO] = [account_info()._asdict()]
    hub.answers[Call.TERMINAL_INFO] = [TERMINAL]
    return replace(config, ws_url=f"ws://127.0.0.1:{hub.port}")


@pytest.fixture
async def unreachable(config):
    """A config on a loopback port nothing listens on."""
    server = await serve(lambda ws: None, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    server.close()
    await server.wait_closed()
    return replace(config, ws_url=f"ws://127.0.0.1:{port}")


@pytest.fixture
async def connections():
    """The connections a test builds; those it leaves connected are disconnected after it."""
    built = []
    yield built
    for conn in built:
        if conn._state is ConnectionState.CONNECTED:
            await off_loop(conn.disconnect)


@pytest.fixture
def connect(ready, connections):
    """A connected connection on the ready hub double."""

    async def connected(config=None) -> MT5Connection:
        conn = MT5Connection(config or ready)
        connections.append(conn)
        await off_loop(conn.connect)
        return conn

    return connected


# ═════════════════════════════════════════════════════════════════════════════
# 1. ConnectionState enum
# ═════════════════════════════════════════════════════════════════════════════


class TestConnectionState:

    def test_all_states_exist(self):
        states = {s.name for s in ConnectionState}
        assert states == {"DISCONNECTED", "CONNECTING", "CONNECTED", "SHUTTING_DOWN"}

    def test_states_are_unique(self):
        values = [s.value for s in ConnectionState]
        assert len(values) == len(set(values))


# ═════════════════════════════════════════════════════════════════════════════
# 2. Initial state
# ═════════════════════════════════════════════════════════════════════════════


class TestInitialState:

    def test_starts_disconnected(self, config):
        conn = MT5Connection(config)
        assert_not_connected(conn, "DISCONNECTED")

    def test_repr_shows_disconnected(self, config):
        conn = MT5Connection(config)
        assert "DISCONNECTED" in repr(conn)


# ═════════════════════════════════════════════════════════════════════════════
# 3. Successful connect / disconnect
# ═════════════════════════════════════════════════════════════════════════════


class TestSuccessfulConnect:

    async def test_connect_confirms_the_login_by_one_account_read_through_the_hub(
        self, hub, connect
    ):
        conn = await connect()

        conn.ensure_connected()
        (request,) = hub.asked(Call.ACCOUNT_INFO)
        assert hub.received[0][0] == binary.AdapterHello()
        assert len(hub.connections) == 1

    async def test_connect_logs_the_account(self, connect, caplog):
        caplog.set_level(logging.INFO, logger="mt5connector")
        await connect()
        assert "MT5Connection: connected — Account | Balance: 10000.00 USD" in caplog.text

    async def test_a_config_of_the_credentials_and_the_hubs_url_alone_connects(
        self, hub, ready, connect
    ):
        conn = await connect(
            MT5Config(
                account=12345678,
                server="Broker-Demo",
                ws_url=ready.ws_url,
            )
        )
        conn.ensure_connected()

    async def test_a_connection_runs_its_session_on_a_thread_of_its_own(self, connect):
        before = threads()
        await connect()
        assert len(threads() - before) == 1

    async def test_disconnect_closes_the_session_and_stops_the_thread(self, hub, connect):
        before = threads()
        conn = await connect()
        await off_loop(conn.disconnect)

        assert threads() - before == set()
        await hub.connections[0].wait_closed()

    async def test_state_is_disconnected_after_disconnect(self, connect):
        conn = await connect()
        await off_loop(conn.disconnect)
        assert_not_connected(conn, "DISCONNECTED")

    async def test_repr_shows_connected(self, connect):
        conn = await connect()
        assert "CONNECTED" in repr(conn)


# ═════════════════════════════════════════════════════════════════════════════
# 4. A hub that is not ready, or not there
# ═════════════════════════════════════════════════════════════════════════════


class TestReadiness:

    async def test_a_hub_that_defers_the_account_within_the_timeout_connects(
        self, hub, ready, connect
    ):
        hub.answers[Call.ACCOUNT_INFO].insert(0, NOT_READY)
        conn = await connect()
        conn.ensure_connected()
        assert len(hub.asked(Call.ACCOUNT_INFO)) == 2

    async def test_a_hub_not_ready_within_the_timeout_fails_the_connect_naming_its_deferral(
        self, hub, ready
    ):
        hub.answers[Call.ACCOUNT_INFO] = [NOT_READY]
        conn = MT5Connection(replace(ready, timeout_s=0.3))
        before = threads()

        started = time.monotonic()
        with pytest.raises(MT5ConnectionError, match="not_ready") as refused:
            await off_loop(conn.connect)
        waited = time.monotonic() - started

        assert waited < 1
        assert "the broker clock is not verified" in str(refused.value)
        assert threads() - before == set()
        assert_not_connected(conn, "DISCONNECTED")
        assert "DISCONNECTED" in repr(conn)

    async def test_a_connect_the_hub_was_not_ready_for_can_be_made_again(self, hub, ready, connect):
        hub.answers[Call.ACCOUNT_INFO] = [NOT_READY]
        conn = MT5Connection(replace(ready, timeout_s=0.2))
        with pytest.raises(MT5ConnectionError):
            await off_loop(conn.connect)

        hub.answers[Call.ACCOUNT_INFO] = [account_info()._asdict()]
        await off_loop(conn.connect)
        conn.ensure_connected()
        await off_loop(conn.disconnect)

    async def test_an_unreachable_hub_fails_the_connect_naming_it(self, unreachable):
        conn = MT5Connection(unreachable)
        before = threads()

        with pytest.raises(MT5ConnectionError, match=f"^hub at {unreachable.ws_url}: "):
            await off_loop(conn.connect)

        assert threads() - before == set()
        assert_not_connected(conn, "DISCONNECTED")

    async def test_an_account_read_the_ea_fails_fails_the_connect_with_its_error(self, hub, ready):
        hub.answers[Call.ACCOUNT_INFO] = [EA_ERROR]
        conn = MT5Connection(ready)
        before = threads()

        with pytest.raises(EaError, match="^account_info: the spawner's read failed$"):
            await off_loop(conn.connect)

        assert threads() - before == set()
        assert_not_connected(conn, "DISCONNECTED")

    async def test_an_account_of_a_margin_mode_unknown_fails_the_connect_and_closes_the_session(
        self, hub, ready
    ):
        hub.answers[Call.ACCOUNT_INFO] = [account_info(margin_mode=99)._asdict()]
        conn = MT5Connection(ready)
        before = threads()

        with pytest.raises(MT5ConnectionError, match="^account_info: margin mode 99 is unknown$"):
            await off_loop(conn.connect)

        await asyncio.wait_for(hub.connections[0].wait_closed(), 2)
        assert threads() - before == set()
        assert_not_connected(conn, "DISCONNECTED")


# ═════════════════════════════════════════════════════════════════════════════
# 5. ensure_connected() — all states
# ═════════════════════════════════════════════════════════════════════════════


class TestEnsureConnected:

    async def test_passes_when_connected(self, connect):
        conn = await connect()
        conn.ensure_connected()

    def test_raises_when_disconnected(self, config):
        conn = MT5Connection(config)
        with pytest.raises(MT5ConnectionError) as exc_info:
            conn.ensure_connected()
        assert str(exc_info.value) == "MT5 not connected (state=DISCONNECTED)"

    def test_raises_while_connecting(self, config):
        conn = MT5Connection(config)
        conn._state = ConnectionState.CONNECTING
        assert_not_connected(conn, "CONNECTING")

    async def test_asks_the_hub_nothing(self, hub, connect):
        conn = await connect()
        for _ in range(1000):
            conn.ensure_connected()
        assert len(hub.requests()) == 1


# ═════════════════════════════════════════════════════════════════════════════
# 6. The bridge onto the session
# ═════════════════════════════════════════════════════════════════════════════


class TestRun:

    async def test_run_serves_a_history_read_on_the_session(self, hub, connect):
        bars = np.zeros(3, dtype=np.dtype(list(BARS.dtype)))
        bars["time"] = [NINE, NINE + 60, NINE + 120]
        bars["time_close"] = bars["time"] + 60
        hub.answers[Call.COPY_RATES_RANGE] = [bars]
        conn = await connect()

        read = await off_loop(
            conn.run, history.bars(conn.session, "X", Series.M1, NINE, NINE + 179)
        )

        assert read.dtype == np.dtype(list(BARS.dtype))
        assert read.tolist() == bars.tolist()

    async def test_run_raises_what_the_coroutine_raised(self, hub, connect):
        hub.answers[Call.COPY_RATES_RANGE] = [
            binary.AnswerError(ErrorKind.INVALID_WINDOW, "starts after the terminal's time")
        ]
        conn = await connect()

        with pytest.raises(InvalidWindow):
            await off_loop(conn.run, history.bars(conn.session, "X", Series.M1, NINE, NINE))

    def test_run_and_the_session_refuse_a_connection_not_connected(self, config):
        conn = MT5Connection(config)

        async def nothing():
            return None

        with pytest.raises(RuntimeError, match=r"^MT5 not connected \(state=DISCONNECTED\)$"):
            conn.run(nothing())
        with pytest.raises(RuntimeError, match=r"^MT5 not connected \(state=DISCONNECTED\)$"):
            conn.session  # noqa: B018


# ═════════════════════════════════════════════════════════════════════════════
# 7. get_account_info()
# ═════════════════════════════════════════════════════════════════════════════


class TestGetAccountInfo:

    async def test_returns_account_snapshot_of_the_hubs_account(self, hub, connect):
        conn = await connect()
        info = await off_loop(conn.get_account_info)
        assert isinstance(info, AccountSnapshot)
        assert len(hub.asked(Call.ACCOUNT_INFO)) == 2

    async def test_account_fields_correct(self, connect):
        conn = await connect()
        info = await off_loop(conn.get_account_info)
        assert info.login == 12345678
        assert info.server == "Broker-Demo"
        assert info.balance == 10000.00
        assert info.currency == "USD"
        assert info.leverage == 500

    def test_raises_if_not_connected(self, config):
        conn = MT5Connection(config)
        with pytest.raises(MT5ConnectionError):
            conn.get_account_info()

    async def test_raises_the_hubs_error_when_the_read_fails(self, hub, connect):
        conn = await connect()
        hub.answers[Call.ACCOUNT_INFO] = [EA_ERROR]
        with pytest.raises(EaError):
            await off_loop(conn.get_account_info)

    async def test_account_snapshot_str_contains_balance(self, connect):
        conn = await connect()
        s = str(await off_loop(conn.get_account_info))
        assert "10000.00" in s
        assert "USD" in s
        assert "500" in s


# ═════════════════════════════════════════════════════════════════════════════
# 8. get_terminal_info()
# ═════════════════════════════════════════════════════════════════════════════


class TestGetTerminalInfo:

    async def test_returns_the_hubs_terminal(self, connect):
        conn = await connect()
        info = await off_loop(conn.get_terminal_info)
        assert info == {
            "name": "MetaTrader 5",
            "path": "C:\\Program Files\\MetaTrader 5",
            "data_path": "C:\\Program Files\\MetaTrader 5",
            "connected": True,
            "trade_allowed": True,
            "ping_last": 3,
            "retransmission": 0.0,
        }

    async def test_raises_the_hubs_error_when_the_read_fails(self, hub, connect):
        hub.answers[Call.TERMINAL_INFO] = [EA_ERROR]
        conn = await connect()
        with pytest.raises(EaError):
            await off_loop(conn.get_terminal_info)

    def test_raises_when_not_connected(self, config):
        conn = MT5Connection(config)
        with pytest.raises(MT5ConnectionError):
            conn.get_terminal_info()


# ═════════════════════════════════════════════════════════════════════════════
# 9. Context manager
# ═════════════════════════════════════════════════════════════════════════════


class TestContextManager:

    async def test_connects_on_enter_and_disconnects_on_exit(self, ready):
        def used() -> MT5Connection:
            with MT5Connection(ready) as conn:
                conn.ensure_connected()
            return conn

        conn = await off_loop(used)
        assert_not_connected(conn, "DISCONNECTED")

    async def test_disconnects_on_exception_and_does_not_suppress_it(self, ready):
        held = []

        def failing() -> None:
            with MT5Connection(ready) as conn:
                held.append(conn)
                raise ValueError("strategy error")

        with pytest.raises(ValueError, match="strategy error"):
            await off_loop(failing)
        assert_not_connected(held[0], "DISCONNECTED")


# ═════════════════════════════════════════════════════════════════════════════
# 10. __repr__
# ═════════════════════════════════════════════════════════════════════════════


class TestRepr:

    def test_repr_omits_the_login(self, config):
        conn = MT5Connection(config)
        assert "12345678" not in repr(conn)

    def test_repr_omits_the_server(self, config):
        conn = MT5Connection(config)
        assert "Exness-MT5Trial1" not in repr(conn)

    async def test_repr_contains_state(self, ready, connect):
        conn = MT5Connection(ready)
        assert "DISCONNECTED" in repr(conn)
        conn = await connect()
        assert "CONNECTED" in repr(conn)


# ═════════════════════════════════════════════════════════════════════════════
# 11. Lifecycle transitions
# ═════════════════════════════════════════════════════════════════════════════


class TestLifecycleTransitions:

    def test_a_disconnect_on_a_connection_never_connected_is_refused_naming_its_state(self, config):
        conn = MT5Connection(config)
        with pytest.raises(MT5ConnectionError, match="DISCONNECTED"):
            conn.disconnect()

    async def test_a_second_disconnect_is_refused(self, connect):
        conn = await connect()
        await off_loop(conn.disconnect)
        with pytest.raises(MT5ConnectionError, match="DISCONNECTED"):
            conn.disconnect()

    async def test_a_connect_on_a_connected_connection_is_refused_naming_its_state(
        self, hub, connect
    ):
        before = threads()
        conn = await connect()
        with pytest.raises(MT5ConnectionError, match="CONNECTED"):
            conn.connect()
        assert len(hub.connections) == 1
        assert len(threads() - before) == 1

    async def test_a_connect_the_hub_did_not_answer_can_be_made_again(
        self, hub, ready, unreachable, connect
    ):
        conn = MT5Connection(unreachable)
        with pytest.raises(MT5ConnectionError):
            await off_loop(conn.connect)
        conn._config = ready
        await off_loop(conn.connect)
        conn.ensure_connected()
        await off_loop(conn.disconnect)


# ═════════════════════════════════════════════════════════════════════════════
# 12. AccountSnapshot
# ═════════════════════════════════════════════════════════════════════════════


class TestAccountSnapshot:

    def test_from_mt5_builds_correctly(self):
        snap = AccountSnapshot.from_mt5(account_info(company="Broker Ltd"))
        assert snap.login == 12345678
        assert snap.equity == 10050.25
        assert snap.leverage == 500
        assert snap.company == "Broker Ltd"

    def test_from_the_hubs_struct_builds_correctly(self):
        struct = calls.STRUCT_TYPES["AccountInfo"](**account_info()._asdict())
        assert AccountSnapshot.from_mt5(struct) == AccountSnapshot.from_mt5(account_info())

    def test_str_carries_the_account_but_not_its_login(self):
        s = str(AccountSnapshot.from_mt5(account_info()))
        assert "12345678" not in s
        assert "10000.00" in s
        assert "USD" in s
        assert "500" in s


# ═════════════════════════════════════════════════════════════════════════════
# 13. Credentials
# ═════════════════════════════════════════════════════════════════════════════

SERVER = "Zq7Broker-Live42"
LOGIN = 918273645


def secret_config(ws_url: str) -> MT5Config:
    return MT5Config(
        account=LOGIN,
        server=SERVER,
        ws_url=ws_url,
        reconnect_initial_delay_s=0.01,
        timeout_s=0.2,
    )


def assert_names_no_credential(text: str) -> None:
    for credential in (SERVER, str(LOGIN)):
        assert credential not in text


class TestCredentials:
    """The broker server and the login reach no log, repr, string or exception."""

    @pytest.fixture
    def secret(self, hub, ready):
        hub.answers[Call.ACCOUNT_INFO] = [account_info(login=LOGIN, server=SERVER)._asdict()]
        return secret_config(ready.ws_url)

    def test_the_config_shows_none(self):
        config = secret_config("ws://127.0.0.1:9000")
        assert_names_no_credential(repr(config))
        assert_names_no_credential(str(config))

    async def test_the_account_snapshot_shows_none(self, secret, connect):
        conn = await connect(secret)
        snapshot = await off_loop(conn.get_account_info)
        assert (snapshot.login, snapshot.server) == (LOGIN, SERVER)
        assert_names_no_credential(repr(snapshot))
        assert_names_no_credential(str(snapshot))

    async def test_the_connection_shows_none(self, secret, connect):
        assert_names_no_credential(repr(MT5Connection(secret)) + str(MT5Connection(secret)))
        conn = await connect(secret)
        assert_names_no_credential(repr(conn) + str(conn))

    async def test_a_failed_connect_names_none(self, hub, secret):
        hub.answers[Call.ACCOUNT_INFO] = [NOT_READY]
        with pytest.raises(MT5ConnectionError) as failed:
            await off_loop(MT5Connection(secret).connect)
        assert_names_no_credential(str(failed.value))
        assert_names_no_credential(repr(failed.value))

    async def test_the_logs_name_none(self, hub, secret, caplog):
        caplog.set_level(logging.DEBUG, logger="mt5connector")
        try:
            conn = await off_loop(_ensure_connection, secret)
            await off_loop(conn.disconnect)
            hub.answers[Call.ACCOUNT_INFO].insert(0, NOT_READY)
            await off_loop(conn.connect)
            await off_loop(conn.disconnect)
        finally:
            _connection_registry.clear()
        assert caplog.records
        assert_names_no_credential("\n".join(record.getMessage() for record in caplog.records))
