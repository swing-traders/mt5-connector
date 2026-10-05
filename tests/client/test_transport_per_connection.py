"""Two connections in one process, each on its own server: every call a connection makes — a package
call, a history read, a commission read — reaches its own server, whichever connected last."""

import json
import threading
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

import pytest
from nautilus_trader.common.component import TestClock
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from venue_doubles import account_info, symbol_info, tick

from mt5connector.client.config import MT5Config
from mt5connector.client.connection import MT5Connection
from mt5connector.client.downloader import MT5DataDownloader
from mt5connector.client.errors import MT5ConnectionError
from mt5connector.client.providers import MT5InstrumentProvider

NOW_NS = 1_760_000_000_000_000_000


def _answer(result, last_error=(1, "Success")) -> dict:
    return {"ok": True, "result": result, "last_error": list(last_error)}


def _failure(code: int, message: str) -> dict:
    return {"ok": False, "error": {"code": code, "message": message}, "last_error": [code, message]}


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
    """A server on its own loopback port, serving one logged-in account and EURUSD without a
    commission, and the requests it received in order."""

    def __init__(self, url: str, login: int, server: str) -> None:
        self.url = url
        self.login = login
        self.server = server
        self.received = []
        self.answers = {
            "/mt5/initialize": _answer(True),
            "/mt5/login": _answer(True),
            "/mt5/shutdown": _answer(None),
            "/mt5/account_info": _answer(account_info(login=login, server=server)._asdict()),
            "/mt5/symbol_select": _answer(True),
            "/mt5/symbol_info": _answer(symbol_info()._asdict()),
            "/mt5/symbols_get": _answer([symbol_info()._asdict()]),
            "/mt5/symbol_info_tick": _answer(tick(bid=1.085, ask=1.0852)._asdict()),
            "/commissions/EURUSD": {"ok": True, "result": {"ret": 0, "last_error": 0, "rules": []}},
            "/history/ranges": _answer({"maxbars": 100_000, "ranges": {}}),
            "/history/bars": _answer([]),
            "/history/ticks": _answer([]),
        }

    def paths(self) -> list[str]:
        return [path for path, _ in self.received]

    def clients(self) -> list:
        return [client for _, client in self.received]


@pytest.fixture
def terminals():
    """Starts a Terminal on a free loopback port for each (login, server) asked; stops them all."""
    servers = []

    def start(login: int, server: str) -> Terminal:
        http = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        http.daemon_threads = True
        http.terminal = Terminal(f"http://127.0.0.1:{http.server_address[1]}", login, server)
        threading.Thread(target=http.serve_forever, daemon=True).start()
        servers.append(http)
        return http.terminal

    yield start
    for http in servers:
        http.shutdown()
        http.server_close()


@pytest.fixture
def alpha(terminals):
    return terminals(1001, "MT5_ALPHA")


@pytest.fixture
def beta(terminals):
    return terminals(1002, "MT5_BETA")


@pytest.fixture
def connected():
    """Connects a connection to each Terminal asked, in turn; disconnects those still connected."""
    connections = []

    def connect(terminal: Terminal) -> MT5Connection:
        connection = MT5Connection(
            MT5Config(
                account=terminal.login,
                password="p",
                server=terminal.server,
                server_url=terminal.url,
                venue=Venue(terminal.server),
            )
        )
        connection.connect()
        connections.append(connection)
        return connection

    yield connect
    for connection in connections:
        try:
            connection.disconnect()
        except MT5ConnectionError:
            pass


def provider_of(connection: MT5Connection, venue: str) -> MT5InstrumentProvider:
    clock = TestClock()
    clock.set_time(NOW_NS)
    return MT5InstrumentProvider(
        connection, venue=Venue(venue), clock=clock, config=InstrumentProviderConfig()
    )


def forget(*terminals: Terminal) -> None:
    for terminal in terminals:
        terminal.received.clear()


def test_a_call_after_another_connection_connects_reaches_its_own_server(alpha, beta, connected):
    first = connected(alpha)
    connected(beta)
    forget(alpha, beta)

    account = first.get_account_info()

    assert account.login == 1001
    assert alpha.paths() == ["/mt5/account_info"]
    assert beta.paths() == []


def test_a_failed_call_reports_its_own_servers_error(alpha, beta, connected):
    first = connected(alpha)
    connected(beta)
    alpha.answers["/mt5/account_info"] = _failure(-10004, "No IPC connection")

    with pytest.raises(MT5ConnectionError, match="error -10004: No IPC connection"):
        first.get_account_info()


def test_a_commission_read_after_another_connection_connects_reaches_its_own_server(
    alpha, beta, connected
):
    first = connected(alpha)
    connected(beta)
    forget(alpha, beta)

    instrument = provider_of(first, "MT5_ALPHA").load_symbol("EURUSD")

    assert instrument.id.value == "EURUSD.MT5_ALPHA"
    assert "/commissions/EURUSD" in alpha.paths()
    assert beta.paths() == []


def test_history_reads_after_another_connection_connects_reach_their_own_server(
    alpha, beta, connected, tmp_path
):
    first = connected(alpha)
    connected(beta)
    forget(alpha, beta)
    downloader = MT5DataDownloader(
        first, provider_of(first, "MT5_ALPHA"), ParquetDataCatalog(str(tmp_path))
    )

    results = downloader.download_all(
        ["EURUSD"], datetime(2025, 7, 14, tzinfo=UTC), datetime(2025, 7, 15, tzinfo=UTC)
    )

    assert all(result.success for result in results["EURUSD"])
    assert {"/history/ranges", "/history/bars", "/history/ticks"} <= set(alpha.paths())
    assert beta.paths() == []


def test_a_history_failure_is_recorded_with_its_own_servers_error(alpha, beta, connected, tmp_path):
    first = connected(alpha)
    connected(beta)
    alpha.answers["/history/ranges"] = _failure(-4, "Terminal: Not found")
    downloader = MT5DataDownloader(
        first, provider_of(first, "MT5_ALPHA"), ParquetDataCatalog(str(tmp_path))
    )

    result = downloader.download_bars(
        "EURUSD", datetime(2025, 7, 14, tzinfo=UTC), datetime(2025, 7, 15, tzinfo=UTC)
    )

    assert result.errors == ["Ranges failed: (-4, 'Terminal: Not found')"]


def test_disconnecting_another_connection_leaves_this_ones_session_open_on_its_own_server(
    alpha, beta, connected
):
    first = connected(alpha)
    second = connected(beta)
    first.get_account_info()
    kept_alive = alpha.clients()[-1]
    second.disconnect()
    forget(alpha, beta)

    account = first.get_account_info()

    assert account.login == 1001
    assert alpha.received == [("/mt5/account_info", kept_alive)]
    assert beta.paths() == []
