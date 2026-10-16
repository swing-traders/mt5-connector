"""The hub's chart route over HTTP, and a read of the HTTP server that has the spawner open a chart,
end to end."""

import asyncio
import json
import socket
import time
from types import SimpleNamespace

import pytest
import requests
from chart_posts import FRESH_S, RETRY_S, TOUCH_S
from ea_wire import FakeEa, zeroed
from hub_doubles import Consumer, settings
from mirror_samples import CLOCK

import mt5connector.client.history as history_client
from mt5connector.client.errors import MT5InstrumentError
from mt5connector.server import charts
from mt5connector.server.charts import HubError, Publishers, hub_charts
from mt5connector.server.wire import binary
from mt5connector.server.wire.push_wire import ChartRole, ChartState, Stream, Subscription
from mt5connector.server.ws_server import Hub, post_to, serve_hub
from mt5connector.wire.history_wire import Series, ServerCode

NOT_LISTED = "MQL5 error 4301"
FAILED = f"the chart of XYZ failed to open: {NOT_LISTED}"
PUBLISHER = ChartRole.PUBLISHER


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
    """The server's view of the publishers, posting to a hub on `hub_port` and recording each symbol
    it posts."""
    post = hub_charts(f"http://127.0.0.1:{hub_port}")

    def recorded(symbol):
        hub_posts.append(symbol)
        return post(symbol)

    return Publishers(recorded, fresh_s=FRESH_S, touch_s=TOUCH_S, retry_s=RETRY_S)


async def _serve(port, relay=None):
    hub = Hub(CLOCK, relay or (lambda path, body: None), settings())
    return await serve_hub(hub, "127.0.0.1", port)


async def _nothing(ea: FakeEa):
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(ea.ws.recv(), timeout=0.3)


async def _transactions(port) -> Consumer:
    consumer = await Consumer.connect(port)
    await consumer.subscribed(1, Subscription(Stream.TRADE_TRANSACTIONS))
    return consumer


async def _answered(spawner: FakeEa, consumer: Consumer, answer) -> None:
    """Sends the spawner's chart answer, then a transaction naming no symbol, which reaches the
    consumer only once the hub has handled the answer before it."""
    unmarked = binary.TradeTransaction(
        zeroed(binary.TRADE_TRANSACTION), zeroed(binary.TRADE_REQUEST), zeroed(binary.TRADE_RESULT)
    )
    await spawner.send(answer)
    await spawner.send(unmarked)
    assert await consumer.received() == unmarked


def forbidden_sleep(seconds):
    raise AssertionError(f"slept {seconds} s")


async def test_a_post_requests_the_chart_once_and_answers_published_once_its_publisher_says_hello(
    hub_port,
):
    server = await _serve(hub_port)
    spawner = await FakeEa.connect(hub_port, ChartRole.SPAWNER, "EURUSD")
    post = hub_charts(f"http://127.0.0.1:{hub_port}")
    loop = asyncio.get_running_loop()

    first = await loop.run_in_executor(None, post, "US500#")
    opened = await spawner.received()
    second = await loop.run_in_executor(None, post, "US500#")
    await _nothing(spawner)
    us500 = await FakeEa.connect(hub_port, PUBLISHER, "US500#")
    await us500.received()
    third = await loop.run_in_executor(None, post, "US500#")

    assert (first, second, third) == (
        ChartState.REQUESTED,
        ChartState.REQUESTED,
        ChartState.PUBLISHED,
    )
    assert opened == binary.OpenChart(PUBLISHER, "US500#")
    for ea in (spawner, us500):
        await ea.close()
    server.close()


async def test_a_request_other_than_a_post_naming_a_symbol_is_refused(hub_port):
    server = await _serve(hub_port)
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
    hub = Hub(CLOCK, lambda path, body: None, settings())
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


async def test_a_read_has_the_spawner_open_the_chart_and_turns_200_once_its_publisher_relays(
    served, hub_port, hub_posts, server_clock
):
    server = await _serve(hub_port, post_to(served))
    spawner = await FakeEa.connect(hub_port, ChartRole.SPAWNER, "EURUSD")
    loop = asyncio.get_running_loop()

    def read():
        return requests.get(f"{served}/commissions/USDJPY", timeout=5)

    first = await loop.run_in_executor(None, read)
    opened = await spawner.received()
    server_clock[0] = 1_001.0
    early = await loop.run_in_executor(None, read)
    posted_early = list(hub_posts)
    server_clock[0] = 1_005.0
    again = await loop.run_in_executor(None, read)
    await _nothing(spawner)
    usdjpy = await FakeEa.connect(hub_port, PUBLISHER, "USDJPY")
    await usdjpy.received()
    await usdjpy.send(binary.Commissions("USDJPY", {"ret": 0, "last_error": 0, "rules": []}))
    # The server's next post, a retry interval on, finds the publisher.
    server_clock[0] = 1_010.0
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
    assert opened == binary.OpenChart(PUBLISHER, "USDJPY")
    assert (relayed.status_code, relayed.json()["result"]["rules"]) == (200, [])
    assert posted_early == ["USDJPY"]
    assert hub_posts == ["USDJPY", "USDJPY", "USDJPY"]
    for ea in (spawner, usdjpy):
        await ea.close()
    server.close()


async def test_a_reader_deferred_on_a_requested_chart_fails_on_its_retry_once_the_chart_failed(
    served, remote, hub_port, hub_posts, server_clock, monkeypatch
):
    server = await _serve(hub_port, post_to(served))
    spawner = await FakeEa.connect(hub_port, ChartRole.SPAWNER, "EURUSD")
    consumer = await _transactions(hub_port)
    loop = asyncio.get_running_loop()

    requested = await loop.run_in_executor(
        None, lambda: requests.get(f"{served}/commissions/XYZ", timeout=5)
    )
    opened = await spawner.received()
    await _answered(spawner, consumer, binary.ChartFailed(PUBLISHER, "XYZ", 4301))
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
    assert opened == binary.OpenChart(PUBLISHER, "XYZ")
    assert (refused, slept) == (f"commissions: {FAILED}", [5])
    assert hub_posts == ["XYZ", "XYZ"]
    await _nothing(spawner)
    await spawner.close()
    await consumer.close()
    server.close()


async def test_the_route_answers_failed_with_the_spawners_reason_until_the_chart_opens(hub_port):
    server = await _serve(hub_port)
    spawner = await FakeEa.connect(hub_port, ChartRole.SPAWNER, "EURUSD")
    consumer = await _transactions(hub_port)
    loop = asyncio.get_running_loop()

    def post():
        return requests.post(f"http://127.0.0.1:{hub_port}/charts/XYZ", timeout=1).json()

    requested = await loop.run_in_executor(None, post)
    opened = await spawner.received()
    await _answered(spawner, consumer, binary.ChartFailed(PUBLISHER, "XYZ", 4301))
    failed = await loop.run_in_executor(None, post)
    await _answered(spawner, consumer, binary.ChartOpened(PUBLISHER, "XYZ"))
    cleared = await loop.run_in_executor(None, post)

    assert opened == binary.OpenChart(PUBLISHER, "XYZ")
    assert [requested, failed, cleared] == [
        {"ok": True, "result": "requested"},
        {"ok": True, "result": "failed", "reason": NOT_LISTED},
        {"ok": True, "result": "requested"},
    ]
    await _nothing(spawner)
    await spawner.close()
    await consumer.close()
    server.close()


async def test_a_read_of_a_symbol_whose_chart_failed_to_open_fails_at_once_naming_the_reason(
    served, remote, connection, hub_port, hub_posts, monkeypatch
):
    server = await _serve(hub_port, post_to(served))
    spawner = await FakeEa.connect(hub_port, ChartRole.SPAWNER, "EURUSD")
    consumer = await _transactions(hub_port)
    # The subscription requests XYZ's chart.
    await consumer.subscribed(2, Subscription(Stream.TICKS, "XYZ"))
    assert await spawner.received() == binary.OpenChart(PUBLISHER, "XYZ")
    await _answered(spawner, consumer, binary.ChartFailed(PUBLISHER, "XYZ", 4301))
    monkeypatch.setattr(time, "sleep", forbidden_sleep)
    loop = asyncio.get_running_loop()

    def reads():
        with pytest.raises(MT5InstrumentError) as refused:
            remote.commission_schedule("XYZ")
        bars = history_client.bars(connection, "XYZ", Series.H1, 1_752_570_000, 1_752_573_600)
        return str(refused.value), bars, remote.last_error()

    refused, bars, last_error = await loop.run_in_executor(None, reads)
    await _answered(spawner, consumer, binary.ChartOpened(PUBLISHER, "XYZ"))
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
    await spawner.close()
    await consumer.close()
    server.close()
