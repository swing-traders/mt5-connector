import threading
from unittest.mock import MagicMock

import pytest
import waitress
from mirror_samples import CLOCK

import mt5connect.remote_mt5 as shim
from mt5server.app.app import create_app
from mt5server.app.commissions import CommissionStore
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
def app(terminal, commissions):
    """The app with the broker clock verified."""
    ready = threading.Event()
    ready.set()
    return create_app(terminal, commissions, CLOCK, ready)


@pytest.fixture
def client(app):
    return app.test_client()


@pytest.fixture
def served(app):
    """The app served by waitress on a free loopback port; yields its base URL."""
    server = waitress.create_server(app, host="127.0.0.1", port=0, threads=4)
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
