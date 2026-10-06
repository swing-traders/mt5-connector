"""The server-time relay route, the sink it writes, and the clock check that sink wakes."""

import os
import threading
import time
from types import SimpleNamespace

import pytest
from chart_posts import ChartPosts, publishers_on
from mirror_samples import CLOCK

from mt5connector.server.app import create_app
from mt5connector.server.clock_check import ClockCheck
from mt5connector.server.server_time import ServerTimeSample, ServerTimeSink

FRAME = {
    "v": 1,
    "type": "server_time",
    "symbol": "EURUSD",
    "trade_server": 1_752_580_800,
    "current": 1_752_580_798,
    "gmt": 1_752_570_000,
    "connected": 1,
}
SAMPLE = ServerTimeSample(
    symbol="EURUSD",
    trade_server=1_752_580_800,
    current=1_752_580_798,
    gmt=1_752_570_000,
    connected=True,
)


def kept(server_times: ServerTimeSink) -> ServerTimeSample | None:
    received = server_times.wait_newer(None, 0)
    if received is None:
        return None
    else:
        return received.sample


def test_a_frame_relayed_over_loopback_is_kept(client, server_times):
    response = client.post("/relay/server_time", json=FRAME)

    assert (response.status_code, response.json) == (200, {"ok": True, "result": None})
    assert kept(server_times) == SAMPLE


def test_a_frame_from_another_host_is_refused(client, server_times):
    response = client.post(
        "/relay/server_time", json=FRAME, environ_base={"REMOTE_ADDR": "10.0.0.5"}
    )

    assert response.status_code == 403
    assert response.json == {
        "ok": False,
        "error": {"code": -1, "message": "10.0.0.5 is not loopback"},
    }
    assert kept(server_times) is None


def test_a_frame_missing_its_trade_server_time_is_refused(client, server_times):
    frame = {name: value for name, value in FRAME.items() if name != "trade_server"}

    response = client.post("/relay/server_time", json=frame)

    assert response.status_code == 400
    assert response.json == {
        "ok": False,
        "error": {"code": -2, "message": "missing field: trade_server"},
    }
    assert kept(server_times) is None


@pytest.mark.parametrize(
    ("body", "message"),
    [
        ([1], "request body is not a JSON object"),
        (FRAME | {"tick": 1}, "unknown field: tick"),
        (FRAME | {"v": 2}, "unsupported frame version: 2"),
        (FRAME | {"v": True}, "unsupported frame version: True"),
        (FRAME | {"type": "hello"}, "not a server_time frame: 'hello'"),
        (FRAME | {"symbol": ""}, "not a symbol: ''"),
        (FRAME | {"trade_server": "1752580800"}, "not an integer epoch: trade_server"),
        (FRAME | {"current": 1.5, "gmt": None}, "not an integer epoch: current, gmt"),
        (FRAME | {"connected": 2}, "connected is not 0 or 1: 2"),
        (FRAME | {"connected": True}, "connected is not 0 or 1: True"),
    ],
    ids=[
        "array",
        "unknown-field",
        "version-2",
        "version-bool",
        "hello",
        "empty-symbol",
        "string-epoch",
        "float-and-null-epochs",
        "connected-2",
        "connected-bool",
    ],
)
def test_a_body_that_is_not_a_server_time_frame_is_refused(client, server_times, body, message):
    response = client.post("/relay/server_time", json=body)

    assert response.status_code == 400
    assert response.json == {"ok": False, "error": {"code": -2, "message": message}}
    assert kept(server_times) is None


def test_a_disconnected_frame_is_kept_as_disconnected(client, server_times):
    client.post("/relay/server_time", json=FRAME | {"connected": 0})

    assert kept(server_times).connected is False


def test_the_relay_answers_while_the_clock_is_not_verified(client, server_times, clock_status):
    clock_status.clear()

    response = client.post("/relay/server_time", json=FRAME)

    assert (response.status_code, response.json) == (200, {"ok": True, "result": None})
    assert kept(server_times) == SAMPLE
    assert client.get("/health").status_code == 503


def test_a_wait_answers_at_once_with_a_sample_other_than_the_one_seen(server_times):
    server_times.write(SAMPLE)

    assert server_times.wait_newer(None, 5).sample == SAMPLE


def test_a_wait_ends_at_its_timeout_with_the_sample_already_seen(server_times):
    server_times.write(SAMPLE)
    seen = server_times.wait_newer(None, 0)
    started = time.monotonic()

    assert server_times.wait_newer(seen, 0.05) is seen
    assert time.monotonic() - started >= 0.05


def test_a_wait_wakes_on_a_write_from_another_thread(server_times):
    writer = threading.Timer(0.05, server_times.write, args=(SAMPLE,))
    writer.start()

    received = server_times.wait_newer(None, 5)

    writer.join()
    assert received.sample == SAMPLE


class _SinkEndingTheCheck(ServerTimeSink):
    """A sink whose first wait past a sample from a connected run of the maximum age ends the clock
    check's run: the check has verified that sample by then."""

    def __init__(self) -> None:
        super().__init__(max_age_s=30)

    def wait_newer(self, than, timeout):
        if than is not None and than.arrived - than.connected_since >= 30:
            raise SystemExit
        return super().wait_newer(than, timeout)


def test_a_frame_relayed_through_the_route_verifies_the_clock(
    terminal, stub, commissions, repeated_hours, history, monkeypatch
):
    stub.terminal_info.return_value = SimpleNamespace(connected=True)
    stub.symbols_total.return_value = 1
    stub.symbol_info.return_value = SimpleNamespace(name="XAUUSD")
    exits = []
    monkeypatch.setattr(os, "_exit", exits.append)
    monkeypatch.setattr(time, "time", lambda: 1_752_570_030.0)
    monotonic = [0.0]
    monkeypatch.setattr(time, "monotonic", lambda: monotonic[0])
    server_times = _SinkEndingTheCheck()
    check = ClockCheck(
        server_times,
        terminal,
        CLOCK,
        spawner_symbol="XAUUSD",
        max_age_s=30,
        check_s=300,
        bootstrap_s=60,
    )
    client = create_app(
        terminal,
        commissions,
        CLOCK,
        repeated_hours,
        server_times,
        check.status,
        history,
        publishers_on(ChartPosts()),
        workers=3,
        retry_s=5,
    ).test_client()

    def run_until_ended():
        with pytest.raises(SystemExit):
            check.run()

    thread = threading.Thread(target=run_until_ended)
    thread.start()

    unverified = client.get("/health").status_code
    client.post("/relay/server_time", json=FRAME)
    connected_for_0_s = client.get("/health").status_code
    for at in (15.0, 30.0):
        monotonic[0] = at
        client.post("/relay/server_time", json=FRAME)
    thread.join(timeout=5)

    assert not thread.is_alive()
    assert exits == []
    assert (unverified, connected_for_0_s) == (503, 503)
    assert client.get("/health").status_code == 200
