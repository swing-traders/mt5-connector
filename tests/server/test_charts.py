"""The server's publisher check on reads of a symbol: its posts to the hub, their debounce, and its
place among the gates."""

import json
import time
import urllib.request
from types import SimpleNamespace

import pytest
import requests
from chart_posts import FRESH_S, TOUCH_S
from saturation import counters

from mt5connector.server.charts import ChartFailed, HubError, hub_charts
from mt5connector.server.commissions import CommissionSchedule
from mt5connector.server.wire.history_wire import ServerCode
from mt5connector.server.wire.push_wire import ChartState

WINDOW = {"start": 1_752_570_000, "end": 1_752_573_600}
UNVERIFIED = {"ok": False, "error": {"code": -1, "message": "the broker clock is not verified"}}
NO_RULE = CommissionSchedule(ret=0, last_error=0, rules=())
NOT_LISTED = "SymbolSelect failed with error 4301"


def sample(symbol: str) -> dict[str, object]:
    return {
        "v": 1,
        "type": "server_time",
        "symbol": symbol,
        "trade_server": 1_752_580_800,
        "current": 1_752_580_798,
        "gmt": 1_752_570_000,
        "connected": 1,
    }


def commissions_frame(symbol: str) -> dict[str, object]:
    return {
        "v": 1,
        "type": "commissions",
        "symbol": symbol,
        "ret": 0,
        "last_error": 0,
        "rules": [],
    }


def chart_requested(symbol: str) -> str:
    return f"no publisher for {symbol}; chart requested"


@pytest.fixture
def clock(monkeypatch):
    """The monotonic clock, at a value a test sets."""
    now = [1_000.0]
    monkeypatch.setattr(time, "monotonic", lambda: now[0])
    return now


# ── A symbol no EA publishes ─────────────────────────────────────────────────


def test_a_symbol_whose_chart_the_hub_requested_is_posted_again_once_per_retry_after(
    client, chart_posts, clock
):
    chart_posts.state = ChartState.REQUESTED

    first = client.get("/commissions/USDJPY")
    clock[0] = 1_001.0
    early = client.get("/commissions/USDJPY")
    posted_early = list(chart_posts.posted)
    clock[0] = 1_005.0
    again = client.get("/commissions/USDJPY")
    client.post("/relay/server_time", json=sample("USDJPY"))
    client.post("/relay/commissions/USDJPY", json=commissions_frame("USDJPY"))
    relayed = client.get("/commissions/USDJPY")

    deferred = {"ok": False, "error": {"code": -20001, "message": chart_requested("USDJPY")}}
    for response in (first, early, again):
        assert (response.status_code, response.headers["Retry-After"], response.json) == (
            503,
            "5",
            deferred,
        )
    assert (relayed.status_code, relayed.json["result"]) == (
        200,
        {"ret": 0, "last_error": 0, "rules": []},
    )
    assert posted_early == ["USDJPY"]
    assert chart_posts.posted == ["USDJPY", "USDJPY"]


def test_the_first_post_after_the_spawner_failed_a_requested_chart_fails_the_read(
    client, chart_posts, clock
):
    chart_posts.state = ChartState.REQUESTED

    requested = client.get("/commissions/XYZ")
    chart_posts.failure = ChartFailed(NOT_LISTED)
    clock[0] = 1_005.0
    failed = client.get("/commissions/XYZ")

    message = f"the chart of XYZ failed to open: {NOT_LISTED}"
    assert (requested.status_code, requested.json["error"]["message"]) == (
        503,
        chart_requested("XYZ"),
    )
    assert (failed.status_code, failed.json) == (
        400,
        {"ok": False, "error": {"code": -20004, "message": message}},
    )
    assert chart_posts.posted == ["XYZ", "XYZ"]


def test_bar_and_tick_reads_of_a_symbol_no_ea_publishes_are_deferred_without_the_terminal(
    client, chart_posts, stub
):
    chart_posts.state = ChartState.REQUESTED

    bars = client.post("/history/bars", json={"symbol": "USDJPY", "timeframe": "H1"} | WINDOW)
    ticks = client.post("/history/ticks", json={"symbol": "GBPUSD"} | WINDOW)

    for response, symbol in ((bars, "USDJPY"), (ticks, "GBPUSD")):
        message = chart_requested(symbol)
        assert (response.status_code, response.headers["Retry-After"], response.json) == (
            503,
            "5",
            {
                "ok": False,
                "error": {"code": -20001, "message": message},
                "last_error": [-20001, message],
            },
        )
    assert chart_posts.posted == ["USDJPY", "GBPUSD"]
    assert stub.mock_calls == []
    health = client.get("/health").json["result"]
    assert (health["in_flight"], health["peak_in_flight"], health["refusals"]) == (0, 1, 0)


def test_the_floors_and_the_mirror_post_nothing_to_the_hub(client, chart_posts, stub):
    stub.terminal_info.return_value = None
    stub.symbol_info_tick.return_value = None

    ranges = client.get("/history/ranges", query_string={"symbol": "USDJPY"})
    tick = client.post("/mt5/symbol_info_tick", json={"symbol": "USDJPY"})

    assert (ranges.status_code, tick.status_code) == (200, 200)
    assert chart_posts.posted == []


def test_a_symbol_the_hub_says_an_ea_publishes_is_read_at_once_and_posted_once_per_half_idle_period(
    client, chart_posts, commissions, clock
):
    commissions.write("GBPUSD", NO_RULE)

    first = client.get("/commissions/GBPUSD")
    clock[0] = 1_005.0
    second = client.get("/commissions/GBPUSD")
    posted_second = list(chart_posts.posted)
    clock[0] = 1_000.0 + TOUCH_S
    touched = client.get("/commissions/GBPUSD")

    assert (first.status_code, second.status_code, touched.status_code) == (200, 200, 200)
    assert posted_second == ["GBPUSD"]
    assert chart_posts.posted == ["GBPUSD", "GBPUSD"]


def test_a_post_the_hub_does_not_answer_fails_the_read_and_the_next_read_posts_again(
    client, chart_posts
):
    failure = "chart post to http://127.0.0.1:9000/charts/USDJPY failed: connection refused"
    chart_posts.failure = HubError(failure)

    first = client.get("/commissions/USDJPY")
    second = client.get("/commissions/USDJPY")

    for response in (first, second):
        assert (response.status_code, response.json) == (
            503,
            {"ok": False, "error": {"code": -1, "message": failure}},
        )
        assert "Retry-After" not in response.headers
    assert chart_posts.posted == ["USDJPY", "USDJPY"]


def test_a_read_of_a_symbol_whose_chart_failed_to_open_fails_naming_it_and_asks_the_hub_again(
    client, chart_posts, stub
):
    chart_posts.failure = ChartFailed(NOT_LISTED)

    commission = client.get("/commissions/XYZ")
    bars = client.post("/history/bars", json={"symbol": "XYZ", "timeframe": "H1"} | WINDOW)
    ticks = client.post("/history/ticks", json={"symbol": "XYZ"} | WINDOW)
    chart_posts.failure = None
    chart_posts.state = ChartState.REQUESTED
    cleared = client.get("/commissions/XYZ")

    message = f"the chart of XYZ failed to open: {NOT_LISTED}"
    error = {"code": -20004, "message": message}
    assert (commission.status_code, commission.json) == (400, {"ok": False, "error": error})
    for response in (bars, ticks):
        assert (response.status_code, response.json) == (
            400,
            {"ok": False, "error": error, "last_error": [-20004, message]},
        )
    for response in (commission, bars, ticks):
        assert "Retry-After" not in response.headers
    assert (cleared.status_code, cleared.json["error"]["message"]) == (503, chart_requested("XYZ"))
    assert chart_posts.posted == ["XYZ", "XYZ", "XYZ", "XYZ"]
    assert stub.mock_calls == []


class _Answer:
    """The hub's HTTP answer to a chart post, as urllib hands it over."""

    def __init__(self, body: dict[str, object]) -> None:
        self._raw = json.dumps(body).encode()

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def read(self) -> bytes:
        return self._raw


def _hub_answering(monkeypatch, body: dict[str, object]):
    """The server's post to a hub that answers every chart post with `body`."""
    opener = SimpleNamespace(open=lambda request, timeout: _Answer(body))
    monkeypatch.setattr(urllib.request, "build_opener", lambda *handlers: opener)
    return hub_charts("http://127.0.0.1:9000")


def test_the_hubs_failed_answer_raises_chart_failed_with_the_spawners_reason(monkeypatch):
    post = _hub_answering(monkeypatch, {"ok": True, "result": "failed", "reason": NOT_LISTED})

    with pytest.raises(ChartFailed) as failed:
        post("XYZ")

    assert str(failed.value) == NOT_LISTED


def test_a_failed_answer_without_a_reason_is_not_a_chart_answer(monkeypatch):
    post = _hub_answering(monkeypatch, {"ok": True, "result": "failed"})

    with pytest.raises(HubError, match="not a chart failure's reason"):
        post("XYZ")


# ── A symbol an EA publishes ─────────────────────────────────────────────────


def test_a_read_of_a_symbol_seen_publishing_calls_the_hub_not_at_all(
    client, chart_posts, commissions
):
    client.post("/relay/server_time", json=sample("EURUSD"))
    commissions.write("EURUSD", NO_RULE)

    commission = client.get("/commissions/EURUSD")
    # MN1 is refused after the publisher check.
    bars = client.post("/history/bars", json={"symbol": "EURUSD", "timeframe": "MN1"} | WINDOW)

    assert (commission.status_code, bars.status_code) == (200, 400)
    assert chart_posts.posted == []


def test_a_published_symbol_in_use_is_touched_at_the_hub_once_per_half_idle_period(
    client, chart_posts, commissions, clock
):
    commissions.write("EURUSD", NO_RULE)
    touched = []
    # The EA relays a sample every few seconds; the symbol is read at each.
    for at in range(1_000, 1_910, 10):
        clock[0] = float(at)
        client.post("/relay/server_time", json=sample("EURUSD"))
        assert client.get("/commissions/EURUSD").status_code == 200
        if len(chart_posts.posted) > len(touched):
            touched.append(at)

    assert touched == [1_450, 1_900]
    assert chart_posts.posted == ["EURUSD", "EURUSD"]


def test_a_read_after_the_symbols_ea_went_quiet_requests_its_chart_again(
    client, chart_posts, commissions, clock
):
    commissions.write("EURUSD", NO_RULE)
    client.post("/relay/server_time", json=sample("EURUSD"))
    clock[0] = 1_000.0 + FRESH_S - 1
    still_fresh = client.get("/commissions/EURUSD")
    chart_posts.state = ChartState.REQUESTED

    clock[0] = 2_000.0
    closed = client.get("/commissions/EURUSD")

    assert still_fresh.status_code == 200
    assert (closed.status_code, closed.json["error"]["message"]) == (
        503,
        chart_requested("EURUSD"),
    )
    assert chart_posts.posted == ["EURUSD"]


def test_samples_from_two_eas_keep_the_latest_and_either_disconnected_one_restarts_the_run(
    client, server_times, clock
):
    client.post("/relay/server_time", json=sample("EURUSD"))
    run = server_times.latest().connected_since
    clock[0] = 1_005.0
    client.post("/relay/server_time", json=sample("GBPUSD"))
    latest = server_times.latest()
    clock[0] = 1_010.0
    client.post("/relay/server_time", json=sample("EURUSD") | {"connected": 0})
    broken = server_times.latest()
    clock[0] = 1_015.0
    client.post("/relay/server_time", json=sample("GBPUSD"))

    assert (latest.sample.symbol, latest.connected_since) == ("GBPUSD", run)
    assert (broken.sample.symbol, broken.connected_since) == ("EURUSD", None)
    assert server_times.latest().connected_since == 1_015.0


# ── The order of the checks ──────────────────────────────────────────────────


def test_the_publisher_check_answers_after_the_clock_gate_and_inside_a_slot_of_the_cap(
    served, held, chart_posts, clock_status
):
    chart_posts.state = ChartState.REQUESTED
    requests.post(f"{served}/relay/server_time", json=sample("EURUSD"), timeout=1)
    held.take(2, "/mt5/positions_total")

    unpublished = requests.post(
        f"{served}/history/bars", json={"symbol": "USDJPY", "timeframe": "H1"} | WINDOW, timeout=1
    )
    unpublished_commissions = requests.get(f"{served}/commissions/USDJPY", timeout=1)
    published = requests.post(
        f"{served}/history/bars", json={"symbol": "EURUSD", "timeframe": "H1"} | WINDOW, timeout=1
    )
    verification = clock_status.read()
    clock_status.clear()
    gated = requests.get(f"{served}/commissions/GBPUSD", timeout=1)
    clock_status.set(verification)

    for response in (unpublished, unpublished_commissions, published):
        assert (response.status_code, response.json()["error"]["code"]) == (503, ServerCode.BUSY)
    assert (gated.status_code, gated.json()) == (503, UNVERIFIED)
    assert chart_posts.posted == []
    assert counters(served)["refusals"] == 3
