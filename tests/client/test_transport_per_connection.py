"""Two connections in one process, each on its own server and hub: every call a connection makes — a
package call, a commission read on the server, an account or history read on the hub — reaches its
own, whichever connected last. Each blocking call runs off the loop the hub doubles serve on."""

import asyncio
import json
import threading
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

import numpy as np
import pytest
from hub_double import HubDouble
from nautilus_trader.common.component import TestClock
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from venue_doubles import account_info, relayed_sessions, symbol_info, tick
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


def _answer(result, last_error=(1, "Success")) -> dict:
    return {"ok": True, "result": result, "last_error": list(last_error)}


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_POST(self):
        self.rfile.read(int(self.headers.get("Content-Length", "0")))
        self._answer()

    def do_GET(self):
        self._answer()

    def _answer(self):
        path = urlparse(self.path).path
        self.server.terminal.received.append((path, self.client_address))
        if path in self.server.terminal.answers:
            status = 200
            payload = json.dumps(self.server.terminal.answers[path]).encode()
        else:
            status = 404
            payload = b"{}"
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *args):
        pass


class Terminal:
    """A server on its own loopback port serving EURUSD without a commission, and a hub on its own
    serving one logged-in account with no history; the requests the server received in order."""

    def __init__(self, url: str, hub: HubDouble, login: int, server: str) -> None:
        self.url = url
        self.hub = hub
        self.login = login
        self.server = server
        self.received = []
        self.forgotten = 0
        self.answers = {
            "/mt5/symbol_select": _answer(True),
            "/mt5/symbol_info": _answer(symbol_info()._asdict()),
            "/mt5/symbols_get": _answer([symbol_info()._asdict()]),
            "/mt5/symbol_info_tick": _answer(tick(bid=1.085, ask=1.0852)._asdict()),
            "/commissions/EURUSD": {"ok": True, "result": {"ret": 0, "last_error": 0, "rules": []}},
            "/sessions/EURUSD": {"ok": True, "result": relayed_sessions()},
        }
        hub.answers = {
            Call.ACCOUNT_INFO: [account_info(login=login, server=server)._asdict()],
            Call.RANGES: [calls.Ranges(100_000, {})],
            Call.COPY_RATES_RANGE: [np.zeros(0, dtype=np.dtype(list(BARS.dtype)))],
            Call.COPY_TICKS_RANGE: [np.zeros(0, dtype=np.dtype(list(mirror.TICKS.dtype)))],
        }

    def paths(self) -> list[str]:
        return [path for path, _ in self.received]

    def clients(self) -> list:
        return [client for _, client in self.received]

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
    """Starts a Terminal on free loopback ports for each (login, server) asked; stops them all."""
    servers = []
    hubs = []

    async def start(login: int, server: str) -> Terminal:
        hub = HubDouble()
        hub_server = await serve(hub.handler, "127.0.0.1", 0)
        hub.port = hub_server.sockets[0].getsockname()[1]
        hubs.append(hub_server)
        http = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        http.daemon_threads = True
        http.terminal = Terminal(f"http://127.0.0.1:{http.server_address[1]}", hub, login, server)
        threading.Thread(target=http.serve_forever, daemon=True).start()
        servers.append(http)
        return http.terminal

    yield start
    for http in servers:
        http.shutdown()
        http.server_close()
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
                password="p",
                server=terminal.server,
                server_url=terminal.url,
                ws_url=f"ws://127.0.0.1:{terminal.hub.port}",
                venue=Venue(terminal.server),
            )
        )
        await asyncio.to_thread(connection.connect)
        connections.append(connection)
        return connection

    yield connect
    for connection in connections:
        if connection._state is ConnectionState.CONNECTED:
            await asyncio.to_thread(connection.disconnect)


def provider_of(connection: MT5Connection, venue: str) -> MT5InstrumentProvider:
    clock = TestClock()
    clock.set_time(NOW_NS)
    return MT5InstrumentProvider(
        connection, venue=Venue(venue), clock=clock, config=InstrumentProviderConfig()
    )


def forget(*terminals: Terminal) -> None:
    for terminal in terminals:
        terminal.received.clear()
        terminal.forgotten += len(terminal.calls())


async def test_an_account_read_after_another_connection_connects_reaches_its_own_hub(
    alpha, beta, connected
):
    first = await connected(alpha)
    await connected(beta)
    forget(alpha, beta)

    account = await asyncio.to_thread(first.get_account_info)

    assert account.login == 1001
    assert alpha.calls() == [Call.ACCOUNT_INFO]
    assert (alpha.paths(), beta.paths(), beta.calls()) == ([], [], [])


async def test_a_failed_read_reports_its_own_hubs_error(alpha, beta, connected):
    first = await connected(alpha)
    await connected(beta)
    alpha.hub.answers[Call.ACCOUNT_INFO] = [
        binary.AnswerError(ErrorKind.EA_ERROR, "the spawner of alpha failed it")
    ]

    with pytest.raises(EaError, match="^account_info: the spawner of alpha failed it$"):
        await asyncio.to_thread(first.get_account_info)


async def test_a_commission_read_after_another_connection_connects_reaches_its_own_server(
    alpha, beta, connected
):
    first = await connected(alpha)
    await connected(beta)
    forget(alpha, beta)

    instrument = await asyncio.to_thread(provider_of(first, "MT5_ALPHA").load_symbol, "EURUSD")

    assert instrument.id.value == "EURUSD.MT5_ALPHA"
    assert "/commissions/EURUSD" in alpha.paths()
    assert (beta.paths(), beta.calls()) == ([], [])


async def test_history_reads_after_another_connection_connects_reach_their_own_hub(
    alpha, beta, connected, tmp_path
):
    first = await connected(alpha)
    await connected(beta)
    forget(alpha, beta)
    downloader = MT5DataDownloader(
        first, provider_of(first, "MT5_ALPHA"), ParquetDataCatalog(str(tmp_path))
    )

    results = await asyncio.to_thread(
        downloader.download_all,
        ["EURUSD"],
        datetime(2025, 7, 14, tzinfo=UTC),
        datetime(2025, 7, 15, tzinfo=UTC),
    )

    assert all(result.success for result in results["EURUSD"])
    assert {Call.RANGES, Call.COPY_RATES_RANGE, Call.COPY_TICKS_RANGE} <= set(alpha.calls())
    assert (beta.paths(), beta.calls()) == ([], [])


async def test_a_history_failure_is_recorded_with_its_own_hubs_error(
    alpha, beta, connected, tmp_path
):
    first = await connected(alpha)
    await connected(beta)
    alpha.hub.answers[Call.RANGES] = [binary.AnswerError(ErrorKind.EA_ERROR, "Terminal: Not found")]
    downloader = MT5DataDownloader(
        first, provider_of(first, "MT5_ALPHA"), ParquetDataCatalog(str(tmp_path))
    )

    result = await asyncio.to_thread(
        downloader.download_bars,
        "EURUSD",
        datetime(2025, 7, 14, tzinfo=UTC),
        datetime(2025, 7, 15, tzinfo=UTC),
    )

    assert result.errors == ["Ranges failed: ranges: Terminal: Not found"]


async def test_disconnecting_another_connection_leaves_this_ones_sessions_open_on_its_own(
    alpha, beta, connected
):
    first = await connected(alpha)
    second = await connected(beta)
    await asyncio.to_thread(first.mt5.symbol_info, "EURUSD")
    kept_alive = alpha.clients()[-1]
    await asyncio.to_thread(second.disconnect)
    forget(alpha, beta)

    account = await asyncio.to_thread(first.get_account_info)
    await asyncio.to_thread(first.mt5.symbol_info, "EURUSD")

    assert account.login == 1001
    assert len(alpha.hub.connections) == 1
    assert alpha.hub.requests(0)[-1].call == Call.ACCOUNT_INFO
    assert alpha.received == [("/mt5/symbol_info", kept_alive)]
    assert (beta.paths(), beta.calls()) == ([], [])
