"""The hub's chart route over HTTP, and a read that has the spawner open a chart, end to end."""

import asyncio
import json
import socket
import time
from types import SimpleNamespace

import pytest
import requests
import websockets
from chart_posts import FRESH_S, RETRY_S, TOUCH_S
from mirror_samples import BROKER_EPOCH, CLOCK

import mt5connector.client.history as history_client
from mt5connector.client.errors import MT5InstrumentError
from mt5connector.server import charts
from mt5connector.server.charts import HubError, Publishers, hub_charts
from mt5connector.server.wire.push_wire import ChartState
from mt5connector.server.ws_server import Hub, post_to, serve_hub
from mt5connector.wire.history_wire import Series, ServerCode

NOT_LISTED = "SymbolSelect failed with error 4301"
FAILED = f"the chart of XYZ failed to open: {NOT_LISTED}"


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


def chart_failed(symbol: str) -> dict[str, object]:
    return {"v": 1, "type": "chart_failed", "symbol": symbol, "reason": NOT_LISTED}


def chart_opened(symbol: str) -> dict[str, object]:
    return {"v": 1, "type": "chart_opened", "symbol": symbol}


def commissions_frame(symbol: str) -> dict[str, object]:
    return {
        "v": 1,
        "type": "commissions",
        "symbol": symbol,
        "ret": 0,
        "last_error": 0,
        "rules": [],
    }


@pytest.fixture
def hub_port():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


@pytest.fixture
def hub_posts():
    return []


@pytest.fixture
def server_clock(monkeypatch):
    """The monotonic clock of the server's chart posts, at a value a test sets."""
    now = [1_000.0]
    monkeypatch.setattr(charts, "time", SimpleNamespace(monotonic=lambda: now[0]))
    return now


@pytest.fixture
def publishers(hub_port, hub_posts):
    """The server's view of the publishers, posting to a hub on `hub_port` and recording each
    symbol it posts."""
    post = hub_charts(f"http://127.0.0.1:{hub_port}")

    def recorded(symbol):
        hub_posts.append(symbol)
        return post(symbol)

    return Publishers(recorded, fresh_s=FRESH_S, touch_s=TOUCH_S, retry_s=RETRY_S)


async def _ea(port, symbol, spawner=False):
    ws = await websockets.connect(f"ws://127.0.0.1:{port}")
    hello = {"v": 1, "type": "hello", "role": "ea", "symbol": symbol, "spawner": spawner}
    await ws.send(json.dumps(hello))
    assert json.loads(await asyncio.wait_for(ws.recv(), 1))["type"] == "wanted"
    return ws


async def _nothing(ws):
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(ws.recv(), timeout=0.3)


async def _received(ws):
    return json.loads(await asyncio.wait_for(ws.recv(), 1))


async def _ticks_of(port, *symbols):
    """An adapter subscribed to the ticks of each symbol, its acks read."""
    ws = await websockets.connect(f"ws://127.0.0.1:{port}")
    await ws.send(json.dumps({"v": 1, "type": "hello", "role": "adapter"}))
    for op_id, symbol in enumerate(symbols, start=1):
        subscribe = {"v": 1, "type": "subscribe", "id": op_id, "stream": "ticks", "symbol": symbol}
        await ws.send(json.dumps(subscribe))
        assert await _received(ws) == {"v": 1, "type": "ack", "id": op_id}
    return ws


async def _answered(spawner, adapter, answer):
    """Sends the spawner's chart answer, then a tick of its own symbol, which reaches the adapter
    only once the hub has handled the answer before it."""
    tick = {
        "v": 1,
        "type": "tick",
        "symbol": "EURUSD",
        "time": BROKER_EPOCH,
        "bid": 1.085,
        "ask": 1.08512,
        "last": 0.0,
        "volume": 0,
        "time_msc": BROKER_EPOCH * 1000,
        "flags": 6,
        "volume_real": 0.0,
    }
    await spawner.send(json.dumps(answer))
    await spawner.send(json.dumps(tick))
    assert (await _received(adapter))["symbol"] == "EURUSD"


def forbidden_sleep(seconds):
    raise AssertionError(f"slept {seconds} s")


async def test_a_post_requests_the_chart_once_and_answers_published_once_the_ea_says_hello(
    hub_port,
):
    server = await serve_hub(
        Hub(CLOCK, lambda path, frame: None, idle_s=900), "127.0.0.1", hub_port
    )
    spawner = await _ea(hub_port, "EURUSD", spawner=True)
    post = hub_charts(f"http://127.0.0.1:{hub_port}")
    loop = asyncio.get_running_loop()

    first = await loop.run_in_executor(None, post, "US500#")
    opened = json.loads(await asyncio.wait_for(spawner.recv(), 1))
    second = await loop.run_in_executor(None, post, "US500#")
    await _nothing(spawner)
    us500 = await _ea(hub_port, "US500#")
    third = await loop.run_in_executor(None, post, "US500#")

    assert (first, second, third) == (
        ChartState.REQUESTED,
        ChartState.REQUESTED,
        ChartState.PUBLISHED,
    )
    assert opened == {"v": 1, "type": "open_chart", "symbol": "US500#"}
    for ws in (spawner, us500):
        await ws.close()
    server.close()


async def test_a_request_other_than_a_post_naming_a_symbol_is_refused(hub_port):
    server = await serve_hub(
        Hub(CLOCK, lambda path, frame: None, idle_s=900), "127.0.0.1", hub_port
    )
    base = f"http://127.0.0.1:{hub_port}"
    loop = asyncio.get_running_loop()

    def answers():
        return [
            requests.get(f"{base}/charts/USDJPY", timeout=1),
            requests.post(f"{base}/charts/", timeout=1),
            requests.post(f"{base}/charts/a/b", timeout=1),
        ]

    refused = await loop.run_in_executor(None, answers)

    assert [(answer.status_code, answer.json()) for answer in refused] == [
        (405, {"ok": False, "error": "GET is not POST"}),
        (404, {"ok": False, "error": "/charts/ names no symbol"}),
        (404, {"ok": False, "error": "/charts/a/b names no symbol"}),
    ]
    with pytest.raises(HubError, match="HTTP Error 404"):
        await loop.run_in_executor(None, hub_charts(f"{base}/charts/x"), "USDJPY")
    server.close()


async def test_a_post_from_another_host_is_refused():
    hub = Hub(CLOCK, lambda path, frame: None, idle_s=900)
    connection = SimpleNamespace(
        remote_address=("10.0.0.5", 50_000),
        respond=lambda status, text: SimpleNamespace(
            status_code=status, body=text, headers={"Content-Type": "text/plain"}
        ),
    )
    request = SimpleNamespace(path="/charts/USDJPY", method="POST")

    response = await hub.process_request(connection, request)

    assert (response.status_code, json.loads(response.body)) == (
        403,
        {"ok": False, "error": "10.0.0.5 is not loopback"},
    )


def test_a_hub_that_does_not_answer_raises_hub_error(hub_port):
    with pytest.raises(HubError, match=f"chart post to http://127.0.0.1:{hub_port}/charts/USDJPY"):
        hub_charts(f"http://127.0.0.1:{hub_port}")("USDJPY")


async def test_a_read_has_the_spawner_open_the_chart_and_turns_200_once_its_ea_relays(
    served, hub_port, hub_posts, server_clock
):
    server = await serve_hub(Hub(CLOCK, post_to(served), idle_s=900), "127.0.0.1", hub_port)
    spawner = await _ea(hub_port, "EURUSD", spawner=True)
    loop = asyncio.get_running_loop()

    def read():
        return requests.get(f"{served}/commissions/USDJPY", timeout=5)

    first = await loop.run_in_executor(None, read)
    opened = json.loads(await asyncio.wait_for(spawner.recv(), 1))
    server_clock[0] = 1_001.0
    early = await loop.run_in_executor(None, read)
    posted_early = list(hub_posts)
    server_clock[0] = 1_005.0
    again = await loop.run_in_executor(None, read)
    await _nothing(spawner)
    usdjpy = await _ea(hub_port, "USDJPY")
    await usdjpy.send(json.dumps(sample("USDJPY")))
    await usdjpy.send(json.dumps(commissions_frame("USDJPY")))
    for _ in range(50):
        relayed = await loop.run_in_executor(None, read)
        if relayed.status_code == 200:
            break
        await asyncio.sleep(0.05)

    for deferred in (first, early, again):
        assert (deferred.status_code, deferred.json()["error"]["message"]) == (
            503,
            "no publisher for USDJPY; chart requested",
        )
    assert opened == {"v": 1, "type": "open_chart", "symbol": "USDJPY"}
    assert (relayed.status_code, relayed.json()["result"]["rules"]) == (200, [])
    assert posted_early == ["USDJPY"]
    assert hub_posts == ["USDJPY", "USDJPY"]
    for ws in (spawner, usdjpy):
        await ws.close()
    server.close()


async def test_a_reader_deferred_on_a_requested_chart_fails_on_its_retry_once_the_chart_failed(
    served, remote, hub_port, hub_posts, server_clock, monkeypatch
):
    server = await serve_hub(Hub(CLOCK, post_to(served), idle_s=900), "127.0.0.1", hub_port)
    spawner = await _ea(hub_port, "EURUSD", spawner=True)
    adapter = await _ticks_of(hub_port, "EURUSD")
    await _received(spawner)
    loop = asyncio.get_running_loop()

    requested = await loop.run_in_executor(
        None, lambda: requests.get(f"{served}/commissions/XYZ", timeout=5)
    )
    opened = await _received(spawner)
    await _answered(spawner, adapter, chart_failed("XYZ"))
    server_clock[0] = 1_001.0
    slept = []

    def sleep(seconds):
        slept.append(seconds)
        server_clock[0] += seconds

    monkeypatch.setattr(time, "sleep", sleep)

    def read():
        with pytest.raises(MT5InstrumentError) as refused:
            remote.commission_schedule("XYZ")
        return str(refused.value)

    refused = await loop.run_in_executor(None, read)

    assert (requested.status_code, requested.json()["error"]["message"]) == (
        503,
        "no publisher for XYZ; chart requested",
    )
    assert opened == {"v": 1, "type": "open_chart", "symbol": "XYZ"}
    assert (refused, slept) == (f"commissions: {FAILED}", [5])
    assert hub_posts == ["XYZ", "XYZ"]
    await _nothing(spawner)
    for ws in (spawner, adapter):
        await ws.close()
    server.close()


async def test_the_route_answers_failed_with_the_spawners_reason_until_the_chart_opens(hub_port):
    server = await serve_hub(
        Hub(CLOCK, lambda path, frame: None, idle_s=900), "127.0.0.1", hub_port
    )
    spawner = await _ea(hub_port, "EURUSD", spawner=True)
    adapter = await _ticks_of(hub_port, "EURUSD")
    await _received(spawner)
    loop = asyncio.get_running_loop()

    def post():
        return requests.post(f"http://127.0.0.1:{hub_port}/charts/XYZ", timeout=1).json()

    requested = await loop.run_in_executor(None, post)
    opened = await _received(spawner)
    await _answered(spawner, adapter, chart_failed("XYZ"))
    failed = await loop.run_in_executor(None, post)
    await _answered(spawner, adapter, chart_opened("XYZ"))
    cleared = await loop.run_in_executor(None, post)

    assert opened == {"v": 1, "type": "open_chart", "symbol": "XYZ"}
    assert [requested, failed, cleared] == [
        {"ok": True, "result": "requested"},
        {"ok": True, "result": "failed", "reason": NOT_LISTED},
        {"ok": True, "result": "requested"},
    ]
    await _nothing(spawner)
    for ws in (spawner, adapter):
        await ws.close()
    server.close()


async def test_a_read_of_a_symbol_whose_chart_failed_to_open_fails_at_once_naming_the_reason(
    served, remote, hub_port, hub_posts, monkeypatch
):
    server = await serve_hub(Hub(CLOCK, post_to(served), idle_s=900), "127.0.0.1", hub_port)
    spawner = await _ea(hub_port, "EURUSD", spawner=True)
    # The subscription requests XYZ's chart.
    adapter = await _ticks_of(hub_port, "XYZ", "EURUSD")
    assert await _received(spawner) == {"v": 1, "type": "open_chart", "symbol": "XYZ"}
    await _received(spawner)
    await _answered(spawner, adapter, chart_failed("XYZ"))
    monkeypatch.setattr(time, "sleep", forbidden_sleep)
    loop = asyncio.get_running_loop()

    def reads():
        with pytest.raises(MT5InstrumentError) as refused:
            remote.commission_schedule("XYZ")
        bars = history_client.bars("XYZ", Series.H1, 1_752_570_000, 1_752_573_600)
        return str(refused.value), bars, remote.last_error()

    refused, bars, last_error = await loop.run_in_executor(None, reads)
    await _answered(spawner, adapter, chart_opened("XYZ"))
    deferred = await loop.run_in_executor(
        None, lambda: requests.get(f"{served}/commissions/XYZ", timeout=5)
    )

    assert refused == f"commissions: {FAILED}"
    assert (bars, last_error) == (None, (ServerCode.CHART_FAILED, FAILED))
    assert (deferred.status_code, deferred.json()["error"]["message"]) == (
        503,
        "no publisher for XYZ; chart requested",
    )
    assert hub_posts == ["XYZ", "XYZ", "XYZ"]
    for ws in (spawner, adapter):
        await ws.close()
    server.close()
