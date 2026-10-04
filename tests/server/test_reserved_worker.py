"""The slot cap under waitress: the worker it keeps free for /health and the relays, the calls past
it refused at once, and the counters /health reports."""

import logging
import threading
import time

import pytest
import requests
from chart_posts import ChartPosts
from saturation import COUNTERS, counters

from mt5connector.client.errors import MT5ConnectionError, ServerBusy, ServerUnreachable
from mt5connector.server.wire.push_wire import ChartState
from mt5connector.wire.history_wire import ServerCode

UNVERIFIED = {"ok": False, "error": {"code": -1, "message": "the broker clock is not verified"}}
FRAME = {
    "v": 1,
    "type": "server_time",
    "symbol": "EURUSD",
    "trade_server": 1_752_580_800,
    "current": 1_752_580_798,
    "gmt": 1_752_570_000,
    "connected": 1,
}
WINDOW = {"start": 1_752_570_000, "end": 1_752_573_600}


def busy(route: str) -> dict[str, object]:
    message = f"{route}: the server is busy"
    code = int(ServerCode.BUSY)
    return {"ok": False, "error": {"code": code, "message": message}, "last_error": [code, message]}


def timed(request) -> tuple[requests.Response, float]:
    started = time.monotonic()
    response = request()
    return response, time.monotonic() - started


def test_a_call_past_the_cap_is_refused_at_once_while_health_and_the_relay_answer(
    served, stub, held
):
    stub.orders_total.return_value = 3
    held.take(2, "/mt5/positions_total")

    refused, refused_s = timed(lambda: requests.post(f"{served}/mt5/orders_total", timeout=1))
    health, health_s = timed(lambda: requests.get(f"{served}/health", timeout=1))
    relayed, relayed_s = timed(
        lambda: requests.post(f"{served}/relay/server_time", json=FRAME, timeout=1)
    )
    commissions = requests.get(f"{served}/commissions/EURUSD", timeout=1)

    assert (refused.status_code, refused.headers["Retry-After"]) == (503, "5")
    assert refused.json() == busy("/mt5/orders_total")
    assert health.status_code == 200
    assert {name: health.json()["result"][name] for name in COUNTERS} == {
        "in_flight": 2,
        "peak_in_flight": 2,
        "refusals": 1,
        "workers": 3,
    }
    assert relayed.status_code == 200
    assert (commissions.status_code, commissions.headers["Retry-After"]) == (503, "5")
    assert commissions.json() == busy("/commissions/EURUSD")
    assert max(refused_s, health_s, relayed_s) < 1
    stub.orders_total.assert_not_called()

    held.release()
    answered = requests.post(f"{served}/mt5/orders_total", timeout=1)

    assert [answer.status_code for answer in held.answers] == [200, 200]
    assert answered.json() == {"ok": True, "result": 3, "last_error": [1, "Success"]}
    assert counters(served) == {"in_flight": 0, "peak_in_flight": 2, "refusals": 2, "workers": 3}


class HungHub(ChartPosts):
    """The hub's chart route, holding every post until released."""

    def __init__(self) -> None:
        super().__init__()
        self.entered = threading.Event()
        self.released = threading.Event()

    def __call__(self, symbol: str) -> ChartState:
        self.posted.append(symbol)
        self.entered.set()
        self.released.wait(10)
        return self.state


@pytest.mark.parametrize(("workers", "chart_posts"), [(2, HungHub())])
def test_a_hub_that_hangs_holds_the_one_slot_and_never_the_free_worker(served, chart_posts):
    first = []
    reading = threading.Thread(
        target=lambda: first.append(requests.get(f"{served}/commissions/USDJPY", timeout=15))
    )
    reading.start()
    try:
        assert chart_posts.entered.wait(5)
        second, second_s = timed(lambda: requests.get(f"{served}/commissions/GBPUSD", timeout=3))
        health, health_s = timed(lambda: requests.get(f"{served}/health", timeout=1))
    finally:
        chart_posts.released.set()
        reading.join(15)

    assert (second.status_code, second.headers["Retry-After"]) == (503, "5")
    assert second.json() == busy("/commissions/GBPUSD")
    assert health.status_code == 200
    assert {name: health.json()["result"][name] for name in COUNTERS} == {
        "in_flight": 1,
        "peak_in_flight": 1,
        "refusals": 1,
        "workers": 2,
    }
    assert max(second_s, health_s) < 1
    assert chart_posts.posted == ["USDJPY"]
    assert first[0].json()["error"]["code"] == ServerCode.SYNCING


def test_each_refusal_logs_one_warning_naming_its_route_and_the_calls_in_flight(
    served, stub, held, caplog
):
    caplog.set_level(logging.WARNING, logger="mt5connector.server.app")
    held.take(2, "/mt5/positions_total")

    requests.post(f"{served}/mt5/orders_total", timeout=1)
    requests.get(f"{served}/history/ranges", params={"symbol": "EURUSD"}, timeout=1)

    assert [
        (record.levelno, record.getMessage())
        for record in caplog.records
        if record.name == "mt5connector.server.app"
    ] == [
        (logging.WARNING, "/mt5/orders_total refused: 2 calls in flight"),
        (logging.WARNING, "/history/ranges refused: 2 calls in flight"),
    ]


def test_a_call_within_the_cap_waits_on_the_terminal_and_is_served(served, stub, held):
    stub.orders_total.return_value = 3
    held.take(1, "/mt5/positions_total")

    held.take(1, "/mt5/orders_total")
    while_held = counters(served)
    held.release()

    assert while_held == {"in_flight": 2, "peak_in_flight": 2, "refusals": 0, "workers": 3}
    assert sorted((answer.json() for answer in held.answers), key=lambda body: body["result"]) == [
        {"ok": True, "result": 0, "last_error": [1, "Success"]},
        {"ok": True, "result": 3, "last_error": [1, "Success"]},
    ]
    assert counters(served) == {"in_flight": 0, "peak_in_flight": 2, "refusals": 0, "workers": 3}


def test_every_history_route_past_the_cap_is_refused_at_once(served, stub, held):
    held.take(2, "/mt5/positions_total")

    answers = {
        "/history/bars": requests.post(
            f"{served}/history/bars",
            json={"symbol": "EURUSD", "timeframe": "H1"} | WINDOW,
            timeout=1,
        ),
        "/history/ticks": requests.post(
            f"{served}/history/ticks", json={"symbol": "EURUSD"} | WINDOW, timeout=1
        ),
        "/history/ranges": requests.get(
            f"{served}/history/ranges", params={"symbol": "EURUSD"}, timeout=1
        ),
    }

    assert {
        route: (answer.status_code, answer.headers["Retry-After"], answer.json())
        for route, answer in answers.items()
    } == {route: (503, "5", busy(route)) for route in answers}
    assert counters(served)["refusals"] == 3
    assert {name for name, _, _ in stub.mock_calls} <= {"positions_total"}


def test_the_clock_gate_answers_before_the_cap_and_takes_no_slot(served, stub, clock_status, held):
    held.take(2, "/mt5/positions_total")
    verification = clock_status.read()

    clock_status.clear()
    answers = [
        requests.post(f"{served}/mt5/orders_total", timeout=1),
        requests.get(f"{served}/history/ranges", params={"symbol": "EURUSD"}, timeout=1),
    ]
    clock_status.set(verification)

    assert [(answer.status_code, answer.json()) for answer in answers] == [(503, UNVERIFIED)] * 2
    assert counters(served) == {"in_flight": 2, "peak_in_flight": 2, "refusals": 0, "workers": 3}


def test_a_function_the_server_is_busy_for_raises_server_busy(remote, stub, held):
    held.take(2, "/mt5/positions_total")

    with pytest.raises(ServerBusy, match="^orders_total: server busy$"):
        remote.orders_total()
    assert remote.last_error() == (ServerCode.BUSY, "/mt5/orders_total: the server is busy")
    assert remote.last_error()[0] is ServerCode.BUSY
    assert issubclass(ServerBusy, MT5ConnectionError)
    assert not issubclass(ServerBusy, ServerUnreachable)
    stub.orders_total.assert_not_called()


def test_a_function_the_gate_refuses_while_busy_raises_server_unreachable(
    remote, stub, clock_status, held
):
    held.take(2, "/mt5/positions_total")
    clock_status.clear()

    with pytest.raises(
        ServerUnreachable,
        match="^orders_total: server not ready — the broker clock is not verified$",
    ):
        remote.orders_total()
