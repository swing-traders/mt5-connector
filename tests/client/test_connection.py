"""MT5Connection against a hub double: its lifecycle on its caller's loop, its account reads, a
disconnect under reads in flight or deferred, and a connection left idle on a loop not running."""

import asyncio
import logging
import time
from dataclasses import replace

import numpy as np
import pytest
from hub_double import eventually, zeroed
from venue_doubles import account_info
from websockets.asyncio.server import serve

from mt5connector.client import history
from mt5connector.client.config import MT5Config
from mt5connector.client.connection import AccountSnapshot, ConnectionState, MT5Connection
from mt5connector.client.errors import EaError, InvalidWindow, MT5ConnectionError, ResponseLost
from mt5connector.client.factories import _connection_registry, _ensure_connection
from mt5connector.client.hub import HubSession
from mt5connector.wire import binary, calls
from mt5connector.wire.binary import ErrorKind
from mt5connector.wire.history_wire import BARS, Series

Call = calls.Call
NINE = 1_752_570_000
NOT_READY = binary.AnswerError(ErrorKind.NOT_READY, "the broker clock is not verified", 50)
NOT_READY_200MS = binary.AnswerError(ErrorKind.NOT_READY, "the broker clock is not verified", 200)
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


def others() -> set[asyncio.Task]:
    """The tasks on the running loop but the current one."""
    return asyncio.all_tasks() - {asyncio.current_task()}


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
            await conn.disconnect()


@pytest.fixture
def connect(ready, connections):
    """A connected connection on the ready hub double."""

    async def connected(config=None) -> MT5Connection:
        conn = MT5Connection(config or ready)
        connections.append(conn)
        await conn.connect()
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

    async def test_disconnect_closes_the_session(self, hub, connect):
        conn = await connect()
        await conn.disconnect()

        await hub.connections[0].wait_closed()

    async def test_state_is_disconnected_after_disconnect(self, connect):
        conn = await connect()
        await conn.disconnect()
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

        started = time.monotonic()
        with pytest.raises(MT5ConnectionError) as refused:
            await conn.connect()
        waited = time.monotonic() - started

        assert waited < 1
        assert str(refused.value) == (
            f"hub at {ready.ws_display_url}: not ready within 0.3s, the last answer not_ready: "
            "account_info: the broker clock is not verified"
        )
        await asyncio.wait_for(hub.connections[0].wait_closed(), 2)
        assert_not_connected(conn, "DISCONNECTED")
        assert "DISCONNECTED" in repr(conn)

    async def test_a_connect_the_hub_was_not_ready_for_can_be_made_again(self, hub, ready, connect):
        hub.answers[Call.ACCOUNT_INFO] = [NOT_READY]
        conn = MT5Connection(replace(ready, timeout_s=0.2))
        with pytest.raises(MT5ConnectionError):
            await conn.connect()
        hub.answers[Call.ACCOUNT_INFO] = [account_info()._asdict()]
        await conn.connect()
        conn.ensure_connected()
        await conn.disconnect()

    async def test_an_unreachable_hub_fails_the_connect_naming_it(self, unreachable):
        conn = MT5Connection(unreachable)

        with pytest.raises(MT5ConnectionError, match=f"^hub at {unreachable.ws_display_url}: "):
            await conn.connect()

        assert_not_connected(conn, "DISCONNECTED")

    async def test_an_account_read_the_ea_fails_fails_the_connect_with_its_error(self, hub, ready):
        hub.answers[Call.ACCOUNT_INFO] = [EA_ERROR]
        conn = MT5Connection(ready)

        with pytest.raises(EaError, match="^account_info: the spawner's read failed$"):
            await conn.connect()

        await asyncio.wait_for(hub.connections[0].wait_closed(), 2)
        assert_not_connected(conn, "DISCONNECTED")

    async def test_an_account_of_a_margin_mode_unknown_fails_the_connect_and_closes_the_session(
        self, hub, ready
    ):
        hub.answers[Call.ACCOUNT_INFO] = [account_info(margin_mode=99)._asdict()]
        conn = MT5Connection(ready)

        with pytest.raises(MT5ConnectionError, match="^account_info: margin mode 99 is unknown$"):
            await conn.connect()

        await asyncio.wait_for(hub.connections[0].wait_closed(), 2)
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
# 6. Reads on the session
# ═════════════════════════════════════════════════════════════════════════════


class TestSession:

    async def test_a_history_read_rides_the_session(self, hub, connect):
        bars = np.zeros(3, dtype=np.dtype(list(BARS.dtype)))
        bars["time"] = [NINE, NINE + 60, NINE + 120]
        bars["time_close"] = bars["time"] + 60
        hub.answers[Call.COPY_RATES_RANGE] = [bars]
        conn = await connect()

        read = await history.bars(conn.session, "X", Series.M1, NINE, NINE + 179)

        assert read.dtype == np.dtype(list(BARS.dtype))
        assert read.tolist() == bars.tolist()

    async def test_a_read_raises_what_the_hub_answered(self, hub, connect):
        hub.answers[Call.COPY_RATES_RANGE] = [
            binary.AnswerError(ErrorKind.INVALID_WINDOW, "starts after the terminal's time")
        ]
        conn = await connect()

        with pytest.raises(InvalidWindow):
            await history.bars(conn.session, "X", Series.M1, NINE, NINE)

    def test_the_session_of_a_connection_not_connected_is_refused(self, config):
        conn = MT5Connection(config)

        with pytest.raises(RuntimeError, match=r"^MT5 not connected \(state=DISCONNECTED\)$"):
            conn.session  # noqa: B018


# ═════════════════════════════════════════════════════════════════════════════
# 6b. A disconnect under a read
# ═════════════════════════════════════════════════════════════════════════════


class TestDisconnectUnderARead:

    async def test_a_read_whose_request_is_in_flight_fails_as_lost(self, hub, connect):
        conn = await connect()
        read = asyncio.create_task(history.ranges(conn.session, "X"))
        await eventually(lambda: len(hub.asked(Call.RANGES)) == 1)

        await conn.disconnect()

        with pytest.raises(ResponseLost):
            await asyncio.wait_for(read, 1)

    async def test_a_read_sleeping_in_a_deferral_fails_as_not_connected_and_leaves_no_task(
        self, hub, connect, caplog
    ):
        caplog.set_level(logging.DEBUG, logger="mt5connector")
        hub.answers[Call.RANGES] = [NOT_READY_200MS]
        conn = await connect()
        read = asyncio.create_task(history.ranges(conn.session, "X"))
        await eventually(lambda: "asking again" in caplog.text)

        await conn.disconnect()

        with pytest.raises(RuntimeError, match=r"^hub session: not connected$"):
            await asyncio.wait_for(read, 0.5)
        await eventually(lambda: others() == set(), timeout=1)


# ═════════════════════════════════════════════════════════════════════════════
# 7. account_info()
# ═════════════════════════════════════════════════════════════════════════════


class TestAccountInfo:

    async def test_returns_account_snapshot_of_the_hubs_account(self, hub, connect):
        conn = await connect()
        info = await conn.account_info()
        assert isinstance(info, AccountSnapshot)
        assert len(hub.asked(Call.ACCOUNT_INFO)) == 2

    async def test_account_fields_correct(self, connect):
        conn = await connect()
        info = await conn.account_info()
        assert info.login == 12345678
        assert info.server == "Broker-Demo"
        assert info.balance == 10000.00
        assert info.currency == "USD"
        assert info.leverage == 500

    async def test_raises_if_not_connected(self, config):
        conn = MT5Connection(config)
        with pytest.raises(MT5ConnectionError):
            await conn.account_info()

    async def test_raises_the_hubs_error_when_the_read_fails(self, hub, connect):
        conn = await connect()
        hub.answers[Call.ACCOUNT_INFO] = [EA_ERROR]
        with pytest.raises(EaError):
            await conn.account_info()

    async def test_account_snapshot_str_contains_balance(self, connect):
        conn = await connect()
        s = str(await conn.account_info())
        assert "10000.00" in s
        assert "USD" in s
        assert "500" in s


# ═════════════════════════════════════════════════════════════════════════════
# 8. terminal_info()
# ═════════════════════════════════════════════════════════════════════════════


class TestTerminalInfo:

    async def test_returns_the_hubs_terminal(self, connect):
        conn = await connect()
        info = await conn.terminal_info()
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
            await conn.terminal_info()

    async def test_raises_when_not_connected(self, config):
        conn = MT5Connection(config)
        with pytest.raises(MT5ConnectionError):
            await conn.terminal_info()


# ═════════════════════════════════════════════════════════════════════════════
# 9. Context manager
# ═════════════════════════════════════════════════════════════════════════════


class TestContextManager:

    async def test_connects_on_enter_and_disconnects_on_exit(self, ready):
        async with MT5Connection(ready) as conn:
            conn.ensure_connected()
        assert_not_connected(conn, "DISCONNECTED")

    async def test_disconnects_on_exception_and_does_not_suppress_it(self, ready):
        held = []

        with pytest.raises(ValueError, match="strategy error"):
            async with MT5Connection(ready) as conn:
                held.append(conn)
                raise ValueError("strategy error")
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

    async def test_a_disconnect_on_a_connection_never_connected_is_refused_naming_its_state(
        self, config
    ):
        conn = MT5Connection(config)
        with pytest.raises(RuntimeError, match=r"^MT5 not connected \(state=DISCONNECTED\)$"):
            await conn.disconnect()

    async def test_a_second_disconnect_is_refused(self, connect):
        conn = await connect()
        await conn.disconnect()
        with pytest.raises(RuntimeError, match=r"^MT5 not connected \(state=DISCONNECTED\)$"):
            await conn.disconnect()

    async def test_a_connect_on_a_connected_connection_is_refused_naming_its_state(
        self, hub, connect
    ):
        conn = await connect()
        with pytest.raises(RuntimeError, match=r"^MT5 not disconnected \(state=CONNECTED\)$"):
            await conn.connect()
        assert len(hub.connections) == 1
        conn.ensure_connected()

    async def test_a_connect_while_a_disconnect_closes_the_session_is_refused_naming_its_state(
        self, hub, connect
    ):
        conn = await connect()
        disconnecting = asyncio.create_task(conn.disconnect())
        await asyncio.sleep(0)
        assert conn._state is ConnectionState.SHUTTING_DOWN

        with pytest.raises(RuntimeError, match=r"^MT5 not disconnected \(state=SHUTTING_DOWN\)$"):
            await conn.connect()

        await disconnecting
        assert_not_connected(conn, "DISCONNECTED")
        assert len(hub.connections) == 1

    async def test_a_connect_cancelled_while_its_session_says_hello_closes_the_session(
        self, hub, ready, monkeypatch
    ):
        saying_hello = asyncio.Event()

        async def held(session) -> None:
            saying_hello.set()
            await asyncio.Event().wait()

        monkeypatch.setattr(HubSession, "_send_wanted", held)
        conn = MT5Connection(ready)
        connecting = asyncio.create_task(conn.connect())
        await saying_hello.wait()

        connecting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await connecting

        await asyncio.wait_for(hub.connections[0].wait_closed(), 2)
        assert_not_connected(conn, "DISCONNECTED")

    async def test_a_connect_the_hub_did_not_answer_can_be_made_again(
        self, hub, ready, unreachable, connect
    ):
        conn = MT5Connection(unreachable)
        with pytest.raises(MT5ConnectionError):
            await conn.connect()
        conn._config = ready
        await conn.connect()
        conn.ensure_connected()
        await conn.disconnect()


# ═════════════════════════════════════════════════════════════════════════════
# 12. A connection on a loop not running
# ═════════════════════════════════════════════════════════════════════════════


def test_a_connection_left_idle_on_a_loop_not_running_keeps_its_hub_and_reads_once_it_runs(
    pinging_hub, config
):
    pinging_hub.answers[Call.ACCOUNT_INFO] = [account_info()._asdict()]
    conn = MT5Connection(replace(config, ws_url=f"ws://127.0.0.1:{pinging_hub.port}"))
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(conn.connect())
        time.sleep(1.0)

        account = loop.run_until_complete(conn.account_info())

        assert account.currency == "USD"
        assert len(pinging_hub.connections) == 1
        assert pinging_hub.connections[0].latency > 0
        loop.run_until_complete(conn.disconnect())
    finally:
        loop.close()


# ═════════════════════════════════════════════════════════════════════════════
# 13. AccountSnapshot
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
# 14. Credentials
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
        snapshot = await conn.account_info()
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
            await MT5Connection(secret).connect()
        assert_names_no_credential(str(failed.value))
        assert_names_no_credential(repr(failed.value))

    def test_the_logs_name_none(self, pinging_hub, caplog):
        pinging_hub.answers[Call.ACCOUNT_INFO] = [
            account_info(login=LOGIN, server=SERVER)._asdict()
        ]
        caplog.set_level(logging.DEBUG, logger="mt5connector")
        loop = asyncio.new_event_loop()
        try:
            conn = _ensure_connection(secret_config(f"ws://127.0.0.1:{pinging_hub.port}"), loop)
            loop.run_until_complete(conn.disconnect())
            pinging_hub.answers[Call.ACCOUNT_INFO].insert(0, NOT_READY)
            loop.run_until_complete(conn.connect())
            loop.run_until_complete(conn.disconnect())
        finally:
            _connection_registry.clear()
            loop.close()
        assert caplog.records
        assert_names_no_credential("\n".join(record.getMessage() for record in caplog.records))
