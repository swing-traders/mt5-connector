import threading
from unittest.mock import MagicMock

import pytest
import waitress
from mirror_samples import CLOCK

import mt5connect.remote_mt5 as shim
from mt5server.app.app import create_app
from mt5server.app.clock_check import ClockStatus, ClockVerification
from mt5server.app.commissions import CommissionStore
from mt5server.app.history import FloorStore, History
from mt5server.app.server_time import ServerTimeSink
from mt5server.app.terminal import Terminal


@pytest.fixture
def stub():
    """A MetaTrader5 package double: every function a MagicMock, last_error() answering success."""
    package = MagicMock()
    package.last_error.return_value = (1, "Success")
    return package


@pytest.fixture
def commissions():
    return CommissionStore()


@pytest.fixture
def terminal(stub):
    return Terminal(stub)


@pytest.fixture
def server_times():
    return ServerTimeSink()


@pytest.fixture
def clock_status():
    """The broker clock verified on EURUSD's trade server at 2025-07-15T09:00:00Z, 30 s behind the
    server's clock, under EDT."""
    status = ClockStatus()
    status.set(
        ClockVerification(
            symbol="EURUSD",
            trade_server=1_752_570_000,
            current=1_752_569_998,
            gmt=1_752_570_000,
            skew_s=-30,
            offset_s=10_800,
        )
    )
    return status


@pytest.fixture
def floors():
    return FloorStore()


@pytest.fixture
def history(terminal, floors):
    """The history protocol under the settings' defaults."""
    return History(terminal, CLOCK, floors, retry_s=5, floor_ttl_s=900)


@pytest.fixture
def workers():
    """The server's worker threads: one kept free, so two terminal-bound calls at once."""
    return 3


@pytest.fixture
def app(terminal, commissions, server_times, clock_status, history, workers):
    """The app with the broker clock verified, refusing a call past its cap with Retry-After 5."""
    return create_app(
        terminal,
        commissions,
        CLOCK,
        server_times,
        clock_status,
        history,
        workers=workers,
        retry_s=5,
    )


@pytest.fixture
def client(app):
    return app.test_client()


@pytest.fixture
def served(app, workers):
    """The app served by waitress on a free loopback port; yields its base URL."""
    server = waitress.create_server(app, host="127.0.0.1", port=0, threads=workers)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.effective_port}"
    server.close()
    server.task_dispatcher.shutdown()


@pytest.fixture
def remote(served, monkeypatch):
    """The remote shim configured against the served app."""
    monkeypatch.setattr(shim, "_session", None)
    monkeypatch.setattr(shim, "_server_url", None)
    monkeypatch.setattr(shim, "_ws_url", None)
    monkeypatch.setattr(shim, "_last_error", shim._last_error)
    shim.configure(served)
    yield shim
    shim._session.close()
