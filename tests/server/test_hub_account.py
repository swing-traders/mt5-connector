"""The account EA's lifecycle and its independence from symbol reads and chart control."""

import asyncio
import time
from types import SimpleNamespace

import pytest
from ea_wire import FakeEa, answer, arguments_of, zeroed
from hub_doubles import Consumer, eventually, health, health_of, run_hub, verified_by

from mt5connector.server import ws_server
from mt5connector.server.wire import binary, calls
from mt5connector.server.wire.binary import ErrorKind
from mt5connector.server.wire.push_wire import ChartRole, Stream, Subscription


@pytest.fixture
async def account_hub():
    hub, server, port = await run_hub()
    spawner = await FakeEa.connect(port, ChartRole.SPAWNER, "EURUSD")
    consumer = await Consumer.connect(port)
    peers = [spawner, consumer]
    try:
        opened = await spawner.received()
        assert opened == binary.OpenChart(ChartRole.ACCOUNT, "EURUSD")
        async with verified_by(spawner):
            yield hub, port, spawner, consumer, peers
    finally:
        await asyncio.gather(*(peer.close() for peer in peers))
        server.close()
        await server.wait_closed()


async def connect_account(port, peers):
    ea = await FakeEa.connect(port, ChartRole.ACCOUNT, "EURUSD")
    peers.append(ea)
    return ea


async def connect_publisher(port, peers):
    ea = await FakeEa.connect(port, ChartRole.PUBLISHER, "GBPUSD")
    peers.append(ea)
    assert await ea.received() == binary.Wanted("GBPUSD", False, ())
    return ea


async def test_account_reads_defer_until_its_hello_and_health_names_the_missing_account(
    account_hub,
):
    hub, port, spawner, consumer, peers = account_hub
    status, body = await health_of(port)
    assert status == 503
    assert body["error"] == "no account EA is connected"
    assert body["result"]["account"] is False
    await consumer.ask(1, calls.Call.ACCOUNT_INFO)
    refused = await consumer.received()
    assert refused.error.kind is ErrorKind.NO_PUBLISHER
    assert refused.error.retry_after_ms == 5_000

    ea = await connect_account(port, peers)
    await consumer.ask(2, calls.Call.ACCOUNT_INFO)
    request = await ea.request()
    assert arguments_of(request) == (calls.Call.ACCOUNT_INFO, {})
    await ea.send(
        answer(
            request.request_id, calls.Call.ACCOUNT_INFO, zeroed(calls.CALLS[request.call].layout)
        )
    )
    received = await consumer.received()
    assert received.request_id == 2
    assert received.error is None
    assert received.count == 1
    status, body = await health_of(port)
    assert status == 200
    assert body["result"]["account"] is True


@pytest.mark.parametrize(
    ("call", "arguments", "on_symbol"),
    [
        (calls.Call.POSITIONS_GET, {"symbol": "GBPUSD"}, True),
        (calls.Call.ORDERS_GET, {"symbol": "GBPUSD"}, True),
        (calls.Call.SYMBOL_MARGIN_RATE, {"symbol": "GBPUSD"}, True),
        (calls.Call.POSITIONS_GET, {}, False),
        (calls.Call.POSITIONS_GET, {"ticket": 71}, False),
        (calls.Call.ORDERS_GET, {"ticket": 72}, False),
        (calls.Call.HISTORY_DEALS_GET, {"ticket": 73}, False),
        (calls.Call.HISTORY_ORDERS_GET, {"ticket": 74}, False),
    ],
)
async def test_each_read_reaches_its_symbol_or_account_ea(account_hub, call, arguments, on_symbol):
    hub, port, spawner, consumer, peers = account_hub
    account = await connect_account(port, peers)
    publisher = await connect_publisher(port, peers)
    await consumer.ask(1, call, **arguments)
    if on_symbol:
        request = await publisher.request(timeout=0.2)
    else:
        request = await account.request(timeout=0.2)
    assert arguments_of(request) == (call, arguments)


async def test_a_busy_account_read_delays_neither_a_symbol_read_nor_a_chart_open(account_hub):
    hub, port, spawner, consumer, peers = account_hub
    account = await connect_account(port, peers)
    publisher = await connect_publisher(port, peers)
    other = await Consumer.connect(port)
    peers.append(other)
    await consumer.ask(1, calls.Call.HISTORY_DEALS_GET, ticket=73)
    held = await account.request()
    await account.send(binary.Busy(held.request_id, 10))

    await other.ask(2, calls.Call.POSITIONS_GET, symbol="GBPUSD")
    request = await publisher.request(timeout=0.2)
    await publisher.send(answer(request.request_id, calls.Call.POSITIONS_GET, []))
    received = await other.received(timeout=0.2)
    assert (received.request_id, received.error, received.count) == (2, None, 0)
    await other.subscribed(3, Subscription(Stream.TICKS, "AUDUSD"))
    assert await spawner.received(timeout=0.2) == binary.OpenChart(ChartRole.PUBLISHER, "AUDUSD")
    status, body = await health_of(port)
    assert status == 200
    assert body["result"]["pending"] == [{"role": "account", "symbol": "EURUSD", "requests": 1}]


async def test_the_account_chart_is_kept_past_the_idle_period(account_hub, monkeypatch):
    hub, port, spawner, consumer, peers = account_hub
    account = await connect_account(port, peers)
    await consumer.ask(1, calls.Call.ACCOUNT_INFO)
    request = await account.request()
    await account.send(
        answer(
            request.request_id, calls.Call.ACCOUNT_INFO, zeroed(calls.CALLS[request.call].layout)
        )
    )
    assert (await consumer.received()).error is None
    await eventually(lambda: health(port)[1]["result"]["in_flight"] == 0)
    future = time.monotonic() + 1_000
    monkeypatch.setattr(
        ws_server, "time", SimpleNamespace(monotonic=lambda: future, time=time.time)
    )
    hub.close_idle_charts()
    with pytest.raises(TimeoutError):
        await spawner.received(timeout=0.05)
    assert (await health_of(port))[1]["result"]["account"] is True


async def test_account_disconnect_fails_its_read_and_requests_the_chart_again(account_hub):
    hub, port, spawner, consumer, peers = account_hub
    account = await connect_account(port, peers)
    await consumer.ask(1, calls.Call.ACCOUNT_INFO)
    await account.request()
    await account.close()
    refused = await consumer.received()
    assert refused.request_id == 1
    assert refused.error.kind is ErrorKind.EA_GONE
    assert await spawner.received() == binary.OpenChart(ChartRole.ACCOUNT, "EURUSD")
    assert (await health_of(port))[1]["result"]["account"] is False


async def test_account_chart_failure_is_reported_and_its_hello_clears_it(account_hub):
    hub, port, spawner, consumer, peers = account_hub
    await spawner.send(binary.ChartFailed(ChartRole.ACCOUNT, "EURUSD", 4301))
    await consumer.ask(1, calls.Call.ACCOUNT_INFO)
    refused = await consumer.received()
    assert refused.error.kind is ErrorKind.NO_PUBLISHER
    assert refused.error.retry_after_ms == 5000
    assert refused.last_error == 4301
    account = await connect_account(port, peers)
    await consumer.ask(2, calls.Call.ACCOUNT_INFO)
    assert arguments_of(await account.request()) == (calls.Call.ACCOUNT_INFO, {})
