import threading
from unittest.mock import MagicMock

import pytest
import waitress
from chart_posts import ChartPosts, publishers_on
from mirror_samples import CLOCK
from saturation import Held

import mt5connector.client.remote_mt5 as shim
from mt5connector.server.app import create_app
from mt5connector.server.clock_check import ClockStatus, ClockVerification
from mt5connector.server.commissions import CommissionStore
from mt5connector.server.encoding import RepeatedHours
from mt5connector.server.history import FloorStore, History
from mt5connector.server.server_time import ServerTimeSink
from mt5connector.server.terminal import Terminal


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
    return ServerTimeSink(max_age_s=30)


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
def repeated_hours():
    """The repeated broker hours warned of, shared by the history protocol and the app."""
    return RepeatedHours()


@pytest.fixture
def history(terminal, repeated_hours, floors):
    """The history protocol under the settings' defaults."""
    return History(terminal, CLOCK, repeated_hours, floors, retry_s=5, floor_ttl_s=900)


@pytest.fixture
def chart_posts():
    """The hub's chart route, answering that an EA publishes every symbol posted."""
    return ChartPosts()


@pytest.fixture
def publishers(chart_posts):
    return publishers_on(chart_posts)


@pytest.fixture
def workers():
    """The server's worker threads: one kept free, so two slots."""
    return 3


@pytest.fixture
def app(
    terminal,
    commissions,
    repeated_hours,
    server_times,
    clock_status,
    history,
    publishers,
    workers,
):
    """The app with the broker clock verified, refusing a call past its cap with Retry-After 5."""
    return create_app(
        terminal,
        commissions,
        CLOCK,
        repeated_hours,
        server_times,
        clock_status,
        history,
        publishers,
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


@pytest.fixture
def held(served, stub):
    """positions_total calls held in the package until released, the first in it and the rest
    waiting on the terminal behind it."""
    held = Held(served)

    def blocked():
        held.released.wait(5)
        return 0

    stub.positions_total.side_effect = blocked
    yield held
    held.release()
