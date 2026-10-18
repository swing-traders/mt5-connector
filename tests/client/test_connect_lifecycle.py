"""The clients' connect and disconnect through NT's public lifecycle, over a real connection,
provider and hub session against a hub double."""

import asyncio
from dataclasses import replace
from unittest.mock import MagicMock

import pytest
from hub_double import eventually, zeroed
from nautilus_trader.common.component import LiveClock, MessageBus
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.model.identifiers import InstrumentId, TraderId
from nautilus_trader.test_kit.stubs.component import TestComponentStubs
from venue_doubles import account_info, listing, relayed_sessions, symbol_info, tick

from mt5connector.client.config import MT5Config
from mt5connector.client.connection import AccountSnapshot, MT5Connection
from mt5connector.client.constants import MT5_VENUE
from mt5connector.client.data import MT5DataClient
from mt5connector.client.execution import MT5LiveExecutionClient
from mt5connector.client.providers import MT5InstrumentProvider
from mt5connector.wire import binary, calls
from mt5connector.wire.binary import ErrorKind

Call = calls.Call
EURUSD_ONLY = InstrumentProviderConfig(load_ids=frozenset({InstrumentId.from_str("EURUSD.MT5")}))
NOT_READY = binary.AnswerError(ErrorKind.NOT_READY, "the broker clock is not verified", 20)


@pytest.fixture
def hub_config(hub, config):
    return replace(config, ws_url=f"ws://127.0.0.1:{hub.port}")


@pytest.fixture
async def conn(hub, hub_config):
    """A connection to a hub that defers every symbol definition, answering the account, the
    terminal and every other read a load of EURUSD asks."""
    hub.answers = {
        Call.ACCOUNT_INFO: [account_info()._asdict()],
        Call.TERMINAL_INFO: [
            zeroed(calls.CALLS[Call.TERMINAL_INFO].layout, trade_allowed=True, connected=True)
        ],
        Call.SYMBOL_INFO: [NOT_READY],
        Call.SYMBOLS_GET: [[listing(symbol_info())._asdict()]],
        Call.SYMBOL_INFO_TICK: [tick(bid=1.085, ask=1.0852)._asdict()],
        Call.COMMISSION_SCHEDULE: [{"ret": 0, "last_error": 0, "rules": []}],
        Call.TRADING_SESSIONS: [relayed_sessions()],
    }
    connection = MT5Connection(hub_config)
    await connection.connect()
    yield connection
    await connection.disconnect()


def provider_of(conn: MT5Connection) -> MT5InstrumentProvider:
    return MT5InstrumentProvider(conn, venue=MT5_VENUE, clock=LiveClock(), config=EURUSD_ONLY)


def data_client(conn: MT5Connection, config: MT5Config) -> MT5DataClient:
    return MT5DataClient(
        loop=asyncio.get_running_loop(),
        connection=conn,
        msgbus=TestComponentStubs.msgbus(),
        cache=TestComponentStubs.cache(),
        clock=LiveClock(),
        instrument_provider=provider_of(conn),
        config=config,
    )


def exec_client(conn: MT5Connection, config: MT5Config) -> MT5LiveExecutionClient:
    clock = LiveClock()
    return MT5LiveExecutionClient(
        loop=asyncio.get_running_loop(),
        connection=conn,
        account=AccountSnapshot.from_mt5(account_info()),
        msgbus=MessageBus(trader_id=TraderId("TESTER-001"), clock=clock),
        cache=TestComponentStubs.cache(),
        clock=clock,
        instrument_provider=provider_of(conn),
        config=config,
    )


async def shut_down_while_loading(client, hub) -> tuple[asyncio.Task, int]:
    """Connects the client, disconnects it once the hub has deferred its definition a few times,
    then has the hub answer the definition; returns NT's disconnect task and how many definition
    requests reached the hub after that task ended."""
    client.connect()
    await eventually(lambda: len(hub.asked(Call.SYMBOL_INFO)) >= 3)
    running = asyncio.all_tasks()
    client.disconnect()
    [disconnecting] = asyncio.all_tasks() - running
    await asyncio.wait([disconnecting])
    asked = len(hub.asked(Call.SYMBOL_INFO))
    await asyncio.sleep(0.2)
    asked_after = len(hub.asked(Call.SYMBOL_INFO)) - asked
    hub.answers[Call.SYMBOL_INFO] = [symbol_info()._asdict()]
    await asyncio.sleep(0.2)
    return disconnecting, asked_after


@pytest.mark.parametrize("client_of", [data_client, exec_client], ids=["data", "exec"])
async def test_a_disconnect_while_the_connect_loads_ends_the_connect(
    hub, hub_config, conn, client_of
):
    client = client_of(conn, hub_config)

    disconnecting, asked_after = await shut_down_while_loading(client, hub)

    assert disconnecting.exception() is None
    assert asked_after <= 1
    assert [task for task in client._tasks if not task.done()] == []
    assert not client.is_connected
    assert len(hub.connections) == 1


async def test_a_disconnect_on_a_data_client_never_connected_is_refused(config):
    client = data_client(MagicMock(spec=MT5Connection), config)
    with pytest.raises(RuntimeError, match="not connected"):
        await client._disconnect()
