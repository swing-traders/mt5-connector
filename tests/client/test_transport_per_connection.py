"""Two connections in one process, each on its own hub double: every read a connection makes reaches
its own hub, whichever connected last."""

from datetime import UTC, datetime
from decimal import Decimal

import numpy as np
import pytest
from hub_double import HubDouble
from nautilus_trader.common.component import TestClock
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from venue_doubles import account_info, listing, relayed_sessions, symbol_info, tick
from websockets.asyncio.server import serve

from mt5connector.client.config import MT5Config
from mt5connector.client.connection import ConnectionState, MT5Connection
from mt5connector.client.downloader import MT5DataDownloader
from mt5connector.client.errors import EaError
from mt5connector.client.providers import MT5InstrumentProvider
from mt5connector.wire import binary, calls, mirror
from mt5connector.wire.binary import ErrorKind
from mt5connector.wire.history_wire import BARS

Call = calls.Call
NOW_NS = 1_760_000_000_000_000_000


# EURUSD's schedule as its publisher relays it: 3.5 USD a lot, charged on both legs.
SCHEDULE = {
    "ret": 1,
    "last_error": 0,
    "rules": [
        {
            "currency": "USD",
            "mode_range": "SYMBOL_COMMISSION_RANGE_VOLUME",
            "mode_charge": "SYMBOL_COMMISSION_CHARGE_INSTANT",
            "mode_entry": "SYMBOL_COMMISSION_ENTRY_INOUT",
            "mode_direction": "SYMBOL_COMMISSION_DIRECTION_BOTH",
            "mode_profit": "SYMBOL_COMMISSION_PROFIT_ALL",
            "tiers": [
                {
                    "mode": "SYMBOL_COMMISSION_MONEY_DEPOSIT",
                    "volume_type": "SYMBOL_COMMISSION_VOLUME_TYPE_VOLUME",
                    "value": 3.5,
                    "min_value": 0.0,
                    "max_value": 0.0,
                    "range_from": 0.0,
                    "range_to": 1000000.0,
                    "currency": "USD",
                }
            ],
        }
    ],
}


class Terminal:
    """A hub on its own loopback port serving one logged-in account, EURUSD charging SCHEDULE, and
    no history."""

    def __init__(self, hub: HubDouble, login: int, server: str) -> None:
        self.hub = hub
        self.login = login
        self.server = server
        self.forgotten = 0
        hub.answers = {
            Call.ACCOUNT_INFO: [account_info(login=login, server=server)._asdict()],
            Call.SYMBOLS_GET: [[listing(symbol_info())._asdict()]],
            Call.SYMBOL_INFO: [symbol_info()._asdict()],
            Call.SYMBOL_INFO_TICK: [tick(bid=1.085, ask=1.0852)._asdict()],
            Call.COMMISSION_SCHEDULE: [SCHEDULE],
            Call.TRADING_SESSIONS: [relayed_sessions()],
            Call.RANGES: [calls.Ranges(100_000, {})],
            Call.COPY_RATES_RANGE: [np.zeros(0, dtype=np.dtype(list(BARS.dtype)))],
            Call.COPY_TICKS_RANGE: [np.zeros(0, dtype=np.dtype(list(mirror.TICKS.dtype)))],
        }

    def calls(self) -> list[Call]:
        """The calls the hub was asked since it was last forgotten, over every connection, in
        order."""
        asked = [
            Call(frame.call)
            for frames in self.hub.received
            for frame in frames
            if isinstance(frame, binary.Request)
        ]
        return asked[self.forgotten :]


@pytest.fixture
async def terminals():
    """Starts a Terminal on a free loopback port for each (login, server) asked; stops them all."""
    hubs = []

    async def start(login: int, server: str) -> Terminal:
        hub = HubDouble()
        hub_server = await serve(hub.handler, "127.0.0.1", 0)
        hub.port = hub_server.sockets[0].getsockname()[1]
        hubs.append(hub_server)
        return Terminal(hub, login, server)

    yield start
    for hub_server in hubs:
        hub_server.close()


@pytest.fixture
async def alpha(terminals):
    return await terminals(1001, "MT5_ALPHA")


@pytest.fixture
async def beta(terminals):
    return await terminals(1002, "MT5_BETA")


@pytest.fixture
async def connected():
    """Connects a connection to each Terminal asked, in turn; disconnects those still connected."""
    connections = []

    async def connect(terminal: Terminal) -> MT5Connection:
        connection = MT5Connection(
            MT5Config(
                account=terminal.login,
                server=terminal.server,
                ws_url=f"ws://127.0.0.1:{terminal.hub.port}",
                venue=Venue(terminal.server),
            )
        )
        await connection.connect()
        connections.append(connection)
        return connection

    yield connect
    for connection in connections:
        if connection._state is ConnectionState.CONNECTED:
            await connection.disconnect()


def provider_of(connection: MT5Connection, venue: str) -> MT5InstrumentProvider:
    clock = TestClock()
    clock.set_time(NOW_NS)
    return MT5InstrumentProvider(
        connection, venue=Venue(venue), clock=clock, config=InstrumentProviderConfig()
    )


def forget(*terminals: Terminal) -> None:
    for terminal in terminals:
        terminal.forgotten += len(terminal.calls())


async def test_an_account_read_after_another_connection_connects_reaches_its_own_hub(
    alpha, beta, connected
):
    first = await connected(alpha)
    await connected(beta)
    forget(alpha, beta)

    account = await first.account_info()

    assert account.login == 1001
    assert alpha.calls() == [Call.ACCOUNT_INFO]
    assert beta.calls() == []


async def test_a_failed_read_reports_its_own_hubs_error(alpha, beta, connected):
    first = await connected(alpha)
    await connected(beta)
    alpha.hub.answers[Call.ACCOUNT_INFO] = [
        binary.AnswerError(ErrorKind.EA_ERROR, "the spawner of alpha failed it")
    ]

    with pytest.raises(EaError, match="^account_info: the spawner of alpha failed it$"):
        await first.account_info()


async def test_an_instrument_load_after_another_connection_connects_reaches_its_own_hub(
    alpha, beta, connected
):
    first = await connected(alpha)
    await connected(beta)
    forget(alpha, beta)

    instrument = await provider_of(first, "MT5_ALPHA").load_symbol("EURUSD")

    assert instrument.id.value == "EURUSD.MT5_ALPHA"
    assert set(alpha.calls()) == {
        Call.ACCOUNT_INFO,
        Call.SYMBOL_INFO,
        Call.SYMBOLS_GET,
        Call.COMMISSION_SCHEDULE,
        Call.TRADING_SESSIONS,
        Call.SYMBOL_INFO_TICK,
    }
    assert beta.calls() == []


async def test_a_loaded_instrument_carries_the_fee_and_sessions_its_hub_relays(alpha, connected):
    first = await connected(alpha)

    instrument = await provider_of(first, "MT5_ALPHA").load_symbol("EURUSD")

    value, contract_size, mid = Decimal("3.5"), Decimal(100000), Decimal("1.0851")
    assert abs(instrument.taker_fee - value / (contract_size * mid)) < Decimal("1e-18")
    assert instrument.info["quote_sessions"] == relayed_sessions()["quote"]
    assert instrument.info["trade_sessions"] == relayed_sessions()["trade"]


async def test_history_reads_after_another_connection_connects_reach_their_own_hub(
    alpha, beta, connected, tmp_path
):
    first = await connected(alpha)
    await connected(beta)
    forget(alpha, beta)
    downloader = MT5DataDownloader(
        first, provider_of(first, "MT5_ALPHA"), ParquetDataCatalog(str(tmp_path))
    )

    results = await downloader.download_all(
        ["EURUSD"],
        datetime(2025, 7, 14, tzinfo=UTC),
        datetime(2025, 7, 15, tzinfo=UTC),
    )

    assert all(result.success for result in results["EURUSD"])
    assert {Call.RANGES, Call.COPY_RATES_RANGE, Call.COPY_TICKS_RANGE} <= set(alpha.calls())
    assert beta.calls() == []


async def test_a_history_failure_is_recorded_with_its_own_hubs_error(
    alpha, beta, connected, tmp_path
):
    first = await connected(alpha)
    await connected(beta)
    alpha.hub.answers[Call.RANGES] = [binary.AnswerError(ErrorKind.EA_ERROR, "Terminal: Not found")]
    downloader = MT5DataDownloader(
        first, provider_of(first, "MT5_ALPHA"), ParquetDataCatalog(str(tmp_path))
    )

    result = await downloader.download_bars(
        "EURUSD",
        datetime(2025, 7, 14, tzinfo=UTC),
        datetime(2025, 7, 15, tzinfo=UTC),
    )

    assert result.errors == ["Ranges failed: ranges: Terminal: Not found"]


async def test_disconnecting_another_connection_leaves_this_ones_session_open_on_its_own(
    alpha, beta, connected
):
    first = await connected(alpha)
    second = await connected(beta)
    await second.disconnect()
    forget(alpha, beta)

    account = await first.account_info()

    assert account.login == 1001
    assert len(alpha.hub.connections) == 1
    assert alpha.hub.requests(0)[-1].call == Call.ACCOUNT_INFO
    assert beta.calls() == []
