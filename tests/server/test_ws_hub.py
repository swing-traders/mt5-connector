"""The hub's push protocol against a real websockets server and connection doubles."""

import asyncio
import json
import logging
import socket
from types import SimpleNamespace

import pytest
import websockets
from mirror_samples import BROKER_EPOCH, CLOCK, UTC_EPOCH

from mt5connector.server import ws_server
from mt5connector.server.server_time import ServerTimeSample
from mt5connector.server.wire import mirror
from mt5connector.server.wire.push_wire import ChartState
from mt5connector.server.ws_server import (
    COMMISSIONS_RELAY_PATH,
    SERVER_TIME_RELAY_PATH,
    Hub,
    post_to,
    serve_hub,
)

IDLE_S = 900
DEAL_ADD = 6
REQUEST = 10
NOT_LISTED = "SymbolSelect failed with error 4301"
HELD = "open positions: 1, pending orders: 0"


def server_time(symbol="EURUSD"):
    return {
        "v": 1,
        "type": "server_time",
        "symbol": symbol,
        "trade_server": 1752580800,
        "current": 1752580798,
        "gmt": 1752570000,
        "connected": 1,
    }


def commissions(symbol="EURUSD"):
    return {
        "v": 1,
        "type": "commissions",
        "symbol": symbol,
        "ret": 0,
        "last_error": 0,
        "rules": [],
    }


def tick(symbol="EURUSD", time_msc=BROKER_EPOCH * 1000):
    return {
        "v": 1,
        "type": "tick",
        "symbol": symbol,
        "time": time_msc // 1000,
        "bid": 1.085,
        "ask": 1.08512,
        "last": 0.0,
        "volume": 0,
        "time_msc": time_msc,
        "flags": 6,
        "volume_real": 0.0,
    }


def utc_tick(symbol="EURUSD"):
    return tick(symbol) | {"time": UTC_EPOCH, "time_msc": UTC_EPOCH * 1000}


def bar(timeframe=mirror.TIMEFRAME_M5):
    return {
        "v": 1,
        "type": "bar",
        "symbol": "EURUSD",
        "timeframe": timeframe,
        "time": BROKER_EPOCH,
        "open": 1.085,
        "high": 1.0852,
        "low": 1.0849,
        "close": 1.0851,
        "tick_volume": 42,
        "spread": 12,
        "real_volume": 0,
    }


def transaction(symbol="EURUSD", kind=DEAL_ADD, request_symbol=""):
    request = {name: 0 for name in mirror.STRUCTS[mirror.StructName.TRADE_REQUEST].fields}
    result = {
        name: 0
        for name in mirror.STRUCTS[mirror.StructName.ORDER_SEND_RESULT].fields
        if name != "request"
    }
    return {
        "v": 1,
        "type": "trade_transaction",
        "transaction": {
            "deal": 7001,
            "order": 9001,
            "symbol": symbol,
            "type": kind,
            "order_type": 0,
            "order_state": 0,
            "deal_type": 0,
            "time_type": 0,
            "time_expiration": 0,
            "price": 1.085,
            "price_trigger": 0.0,
            "price_sl": 0.0,
            "price_tp": 0.0,
            "volume": 0.01,
            "position": 9001,
            "position_by": 0,
        },
        "request": request | {"symbol": request_symbol, "comment": ""},
        "result": result | {"comment": ""},
    }


def request_transaction(request_symbol):
    """A TRADE_TRANSACTION_REQUEST: MQL5 fills only its type, its request carries the symbol."""
    return transaction(symbol="", kind=REQUEST, request_symbol=request_symbol)


def ea_hello(symbol="EURUSD", spawner=False):
    return {"v": 1, "type": "hello", "role": "ea", "symbol": symbol, "spawner": spawner}


def adapter_hello():
    return {"v": 1, "type": "hello", "role": "adapter"}


def subscribe(op_id, stream, **named):
    return {"v": 1, "type": "subscribe", "id": op_id, "stream": stream} | named


def unsubscribe(op_id, stream, **named):
    return {"v": 1, "type": "unsubscribe", "id": op_id, "stream": stream} | named


def wanted(symbol="EURUSD", ticks=False, timeframes=()):
    return {
        "v": 1,
        "type": "wanted",
        "symbol": symbol,
        "ticks": ticks,
        "timeframes": list(timeframes),
    }


def addressed(kind, symbol):
    return {"v": 1, "type": kind, "symbol": symbol}


def chart_opened(symbol):
    return addressed("chart_opened", symbol)


def chart_failed(symbol, reason=NOT_LISTED):
    return addressed("chart_failed", symbol) | {"reason": reason}


def chart_kept(symbol, reason=HELD):
    return addressed("chart_kept", symbol) | {"reason": reason}


async def _run_hub():
    """The hub served on a free loopback port, recording each frame it relays and its path."""
    relayed = []
    hub = Hub(CLOCK, lambda path, frame: relayed.append((path, frame)), idle_s=IDLE_S)
    server = await serve_hub(hub, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    return hub, server, port, relayed


async def _connect(port, hello):
    ws = await websockets.connect(f"ws://127.0.0.1:{port}")
    await ws.send(json.dumps(hello))
    return ws


async def _ea(port, symbol="EURUSD", spawner=False):
    """An EA connected for the symbol, its first wanted frame read."""
    ws = await _connect(port, ea_hello(symbol, spawner))
    assert (await _received(ws))["type"] == "wanted"
    return ws


async def _adapter(port):
    return await _connect(port, adapter_hello())


async def _received(ws, timeout=1.0):
    return json.loads(await asyncio.wait_for(ws.recv(), timeout=timeout))


async def _nothing(ws):
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(ws.recv(), timeout=0.3)


async def _close(server, *connections):
    for ws in connections:
        await ws.close()
    server.close()


def _warnings(caplog):
    return [record.getMessage() for record in caplog.records if record.levelno == logging.WARNING]


# ── Versions and acknowledgements ───────────────────────────────────────────


async def test_a_frame_of_another_version_is_answered_with_an_error_and_the_connection_closed():
    hub, server, port, _ = await _run_hub()
    ws = await websockets.connect(f"ws://127.0.0.1:{port}")

    await ws.send(json.dumps(adapter_hello() | {"v": 2}))

    assert await _received(ws) == {
        "v": 1,
        "type": "error",
        "message": "unsupported frame version: 2",
    }
    await asyncio.wait_for(ws.wait_closed(), timeout=1)
    assert ws.close_code == 1002
    server.close()


async def test_a_subscribe_is_acknowledged_with_its_op_id():
    hub, server, port, _ = await _run_hub()
    adapter = await _adapter(port)

    await adapter.send(json.dumps(subscribe(7, "ticks", symbol="EURUSD")))

    assert await _received(adapter) == {"v": 1, "type": "ack", "id": 7}
    await _close(server, adapter)


async def test_an_unsubscribe_is_acknowledged_and_ends_the_stream():
    hub, server, port, _ = await _run_hub()
    ea = await _ea(port)
    adapter = await _adapter(port)
    await adapter.send(json.dumps(subscribe(1, "ticks", symbol="EURUSD")))
    await _received(adapter)

    await adapter.send(json.dumps(unsubscribe(2, "ticks", symbol="EURUSD")))
    assert await _received(adapter) == {"v": 1, "type": "ack", "id": 2}
    await ea.send(json.dumps(tick()))

    await _nothing(adapter)
    await _close(server, adapter, ea)


# ── Fan-out ──────────────────────────────────────────────────────────────────


async def test_an_eas_tick_reaches_the_consumers_subscribed_to_its_symbol_only():
    hub, server, port, _ = await _run_hub()
    ea = await _ea(port)
    eurusd = await _adapter(port)
    xauusd = await _adapter(port)
    await eurusd.send(json.dumps(subscribe(1, "ticks", symbol="EURUSD")))
    await xauusd.send(json.dumps(subscribe(1, "ticks", symbol="XAUUSD")))
    await _received(eurusd)
    await _received(xauusd)

    await ea.send(json.dumps(tick("EURUSD")))

    assert await _received(eurusd) == utc_tick("EURUSD")
    await _nothing(xauusd)
    await _close(server, eurusd, xauusd, ea)


async def test_ticks_the_ea_reads_on_a_tick_and_on_its_timer_are_each_passed_on_in_order():
    hub, server, port, _ = await _run_hub()
    ea = await _ea(port)
    adapter = await _adapter(port)
    await adapter.send(json.dumps(subscribe(1, "ticks", symbol="EURUSD")))
    await _received(adapter)
    # The tick event's read, then the timer's cursor read of one more tick in the same millisecond.
    on_tick = tick() | {"bid": 1.08501}
    on_timer = tick() | {"bid": 1.08502}

    await ea.send(json.dumps(on_tick))
    await ea.send(json.dumps(on_timer))

    assert [await _received(adapter), await _received(adapter)] == [
        utc_tick() | {"bid": 1.08501},
        utc_tick() | {"bid": 1.08502},
    ]
    await _close(server, adapter, ea)


async def test_a_bar_reaches_the_consumers_of_its_series_by_the_series_name_in_true_utc():
    hub, server, port, _ = await _run_hub()
    ea = await _ea(port)
    m5 = await _adapter(port)
    m1 = await _adapter(port)
    await m5.send(json.dumps(subscribe(1, "bars", symbol="EURUSD", timeframe="M5")))
    await m1.send(json.dumps(subscribe(1, "bars", symbol="EURUSD", timeframe="M1")))
    await _received(m5)
    await _received(m1)

    await ea.send(json.dumps(bar(mirror.TIMEFRAME_M5)))

    assert await _received(m5) == bar() | {"timeframe": "M5", "time": UTC_EPOCH}
    await _nothing(m1)
    await _close(server, m5, m1, ea)


async def test_an_eas_trade_transactions_reach_their_subscribers_in_arrival_order():
    hub, server, port, _ = await _run_hub()
    ea = await _ea(port)
    adapter = await _adapter(port)
    await adapter.send(json.dumps(subscribe(1, "trade_transactions")))
    await _received(adapter)
    first = transaction()
    second = request_transaction("EURUSD")

    await ea.send(json.dumps(first))
    await ea.send(json.dumps(second))

    assert [await _received(adapter), await _received(adapter)] == [first, second]
    await _close(server, adapter, ea)


# ── One EA per symbol ────────────────────────────────────────────────────────


async def test_each_ea_is_told_the_wanted_frame_of_its_own_symbol_only():
    hub, server, port, _ = await _run_hub()
    eurusd = await _connect(port, ea_hello("EURUSD"))
    gbpusd = await _connect(port, ea_hello("GBPUSD"))
    told = [await _received(eurusd), await _received(gbpusd)]
    adapter = await _adapter(port)

    await adapter.send(json.dumps(subscribe(1, "ticks", symbol="EURUSD")))
    eurusd_ticks = await _received(eurusd)
    await adapter.send(json.dumps(subscribe(2, "bars", symbol="GBPUSD", timeframe="H1")))
    await adapter.send(json.dumps(subscribe(3, "bars", symbol="GBPUSD", timeframe="M5")))
    gbpusd_bars = [await _received(gbpusd), await _received(gbpusd)]

    assert told == [wanted("EURUSD"), wanted("GBPUSD")]
    assert eurusd_ticks == wanted("EURUSD", ticks=True)
    assert gbpusd_bars == [
        wanted("GBPUSD", timeframes=[mirror.TIMEFRAME_H1]),
        wanted("GBPUSD", timeframes=[mirror.TIMEFRAME_M5, mirror.TIMEFRAME_H1]),
    ]
    await _nothing(eurusd)
    await _close(server, adapter, eurusd, gbpusd)


async def test_an_ea_is_told_its_wanted_frame_again_when_it_reconnects():
    hub, server, port, _ = await _run_hub()
    ea = await _ea(port)
    adapter = await _adapter(port)
    await adapter.send(json.dumps(subscribe(1, "ticks", symbol="EURUSD")))
    await _received(ea)
    await adapter.send(json.dumps(subscribe(2, "trade_transactions")))
    await ea.close()

    reconnected = await _connect(port, ea_hello("EURUSD"))

    assert await _received(reconnected) == wanted("EURUSD", ticks=True)
    await adapter.close()
    assert await _received(reconnected) == wanted("EURUSD")
    await _close(server, reconnected)


async def test_a_subscription_another_consumer_shares_keeps_the_wanted_frame():
    hub, server, port, _ = await _run_hub()
    ea = await _ea(port)
    first = await _adapter(port)
    second = await _adapter(port)
    await first.send(json.dumps(subscribe(1, "ticks", symbol="EURUSD")))
    assert await _received(ea) == wanted("EURUSD", ticks=True)

    await second.send(json.dumps(subscribe(1, "ticks", symbol="EURUSD")))
    await _received(second)
    await first.close()

    await _nothing(ea)
    await _close(server, second, ea)


async def test_a_tick_an_ea_sends_for_another_symbol_is_dropped_with_one_warning(caplog):
    caplog.set_level(logging.WARNING, logger="mt5connector.server.ws_server")
    hub, server, port, _ = await _run_hub()
    eurusd = await _ea(port, "EURUSD")
    gbpusd = await _ea(port, "GBPUSD")
    adapter = await _adapter(port)
    await adapter.send(json.dumps(subscribe(1, "ticks", symbol="EURUSD")))
    await _received(adapter)

    await gbpusd.send(json.dumps(tick("EURUSD")))
    await eurusd.send(json.dumps(tick("EURUSD")))

    assert await _received(adapter) == utc_tick("EURUSD")
    await _nothing(adapter)
    assert _warnings(caplog) == ["dropping a tick frame for 'EURUSD' from the EA for GBPUSD"]
    await _close(server, adapter, eurusd, gbpusd)


async def test_each_transaction_passes_once_through_the_ea_of_its_symbol_and_none_crosses(caplog):
    caplog.set_level(logging.WARNING, logger="mt5connector.server.ws_server")
    hub, server, port, _ = await _run_hub()
    eurusd = await _ea(port, "EURUSD")
    gbpusd = await _ea(port, "GBPUSD")
    adapter = await _adapter(port)
    await adapter.send(json.dumps(subscribe(1, "trade_transactions")))
    await _received(adapter)

    await eurusd.send(json.dumps(transaction("EURUSD")))
    await gbpusd.send(json.dumps(transaction("GBPUSD")))
    await gbpusd.send(json.dumps(transaction("EURUSD")))
    await gbpusd.send(json.dumps(request_transaction("EURUSD")))

    received = [await _received(adapter), await _received(adapter)]
    assert sorted(frame["transaction"]["symbol"] for frame in received) == ["EURUSD", "GBPUSD"]
    await _nothing(adapter)
    assert _warnings(caplog) == [
        "dropping a trade_transaction frame for 'EURUSD' from the EA for GBPUSD",
        "dropping a trade_transaction frame for 'EURUSD' from the EA for GBPUSD",
    ]
    await _close(server, adapter, eurusd, gbpusd)


async def test_a_transaction_naming_no_symbol_passes_through_the_spawner_alone(caplog):
    caplog.set_level(logging.WARNING, logger="mt5connector.server.ws_server")
    hub, server, port, _ = await _run_hub()
    spawner = await _ea(port, "EURUSD", spawner=True)
    gbpusd = await _ea(port, "GBPUSD")
    adapter = await _adapter(port)
    await adapter.send(json.dumps(subscribe(1, "trade_transactions")))
    await _received(adapter)
    unmarked = request_transaction("")
    # A balance operation's deal names no symbol either.
    balance = transaction(symbol="")

    await gbpusd.send(json.dumps(unmarked))
    await spawner.send(json.dumps(unmarked))
    await spawner.send(json.dumps(balance))

    assert [await _received(adapter), await _received(adapter)] == [unmarked, balance]
    await _nothing(adapter)
    assert _warnings(caplog) == ["dropping a trade_transaction frame for '' from the EA for GBPUSD"]
    await _close(server, adapter, spawner, gbpusd)


async def test_a_second_ea_for_a_published_symbol_is_told_it_is_a_duplicate_and_closed(caplog):
    caplog.set_level(logging.WARNING, logger="mt5connector.server.ws_server")
    hub, server, port, _ = await _run_hub()
    first = await _ea(port, "EURUSD")
    adapter = await _adapter(port)
    await adapter.send(json.dumps(subscribe(1, "ticks", symbol="EURUSD")))
    await _received(first)
    await _received(adapter)

    second = await _connect(port, ea_hello("EURUSD"))

    assert await _received(second) == addressed("duplicate", "EURUSD")
    await asyncio.wait_for(second.wait_closed(), timeout=1)
    await first.send(json.dumps(tick("EURUSD")))
    assert await _received(adapter) == utc_tick("EURUSD")
    (warning,) = _warnings(caplog)
    assert warning.startswith("refusing a second EA for EURUSD from ")
    await _close(server, adapter, first)


async def test_a_refused_duplicates_frames_are_dropped_silently(caplog):
    caplog.set_level(logging.WARNING, logger="mt5connector.server.ws_server")
    hub = Hub(CLOCK, lambda path, frame: None, idle_s=IDLE_S)
    first = Peer()
    second = Peer()
    adapter = Peer()
    tasks = [asyncio.create_task(hub.handler(peer)) for peer in (first, adapter)]
    first.put(ea_hello("EURUSD"))
    adapter.put(adapter_hello())
    adapter.put(subscribe(1, "ticks", symbol="EURUSD"))
    await _settle()

    second.put(ea_hello("EURUSD"))
    second.put(tick("EURUSD"))
    await asyncio.wait_for(hub.handler(second), timeout=1)

    assert second.sent == [addressed("duplicate", "EURUSD")]
    assert adapter.sent == [{"v": 1, "type": "ack", "id": 1}]
    assert len(_warnings(caplog)) == 1
    for peer in (first, adapter):
        await peer.close()
    await asyncio.wait_for(asyncio.gather(*tasks), timeout=1)


async def test_a_second_spawner_for_the_spawners_symbol_is_a_duplicate_and_the_first_stays():
    hub, server, port, _ = await _run_hub()
    first = await _ea(port, "EURUSD", spawner=True)

    second = await _connect(port, ea_hello("EURUSD", spawner=True))

    assert await _received(second) == addressed("duplicate", "EURUSD")
    await asyncio.wait_for(second.wait_closed(), timeout=1)
    assert await hub.use_chart("USDJPY") == ChartState.REQUESTED
    assert await _received(first) == addressed("open_chart", "USDJPY")
    await _close(server, first)


# ── The spawner and its charts ───────────────────────────────────────────────


async def test_a_subscription_to_a_symbol_no_ea_publishes_has_the_spawner_alone_open_its_chart():
    hub, server, port, _ = await _run_hub()
    spawner = await _ea(port, "EURUSD", spawner=True)
    gbpusd = await _ea(port, "GBPUSD")
    adapter = await _adapter(port)

    await adapter.send(json.dumps(subscribe(1, "ticks", symbol="USDJPY")))

    assert await _received(adapter) == {"v": 1, "type": "ack", "id": 1}
    assert await _received(spawner) == addressed("open_chart", "USDJPY")
    await _nothing(gbpusd)
    usdjpy = await _connect(port, ea_hello("USDJPY"))
    assert await _received(usdjpy) == wanted("USDJPY", ticks=True)
    await usdjpy.send(json.dumps(tick("USDJPY")))
    assert (await _received(adapter))["symbol"] == "USDJPY"
    await _close(server, adapter, spawner, gbpusd, usdjpy)


async def test_a_chart_requested_while_no_spawner_is_connected_is_sent_on_its_next_hello():
    hub, server, port, _ = await _run_hub()
    adapter = await _adapter(port)
    await adapter.send(json.dumps(subscribe(1, "ticks", symbol="USDJPY")))
    await _received(adapter)

    spawner = await _connect(port, ea_hello("EURUSD", spawner=True))

    assert await _received(spawner) == wanted("EURUSD")
    assert await _received(spawner) == addressed("open_chart", "USDJPY")
    await _close(server, adapter, spawner)


async def test_a_chart_is_requested_once_per_spawner_connection_until_its_ea_says_hello():
    hub, server, port, _ = await _run_hub()
    spawner = await _ea(port, "EURUSD", spawner=True)

    states = [await hub.use_chart("USDJPY"), await hub.use_chart("USDJPY")]
    opened = await _received(spawner)
    await _nothing(spawner)
    usdjpy = await _ea(port, "USDJPY")
    published = await hub.use_chart("USDJPY")

    assert states == [ChartState.REQUESTED, ChartState.REQUESTED]
    assert opened == addressed("open_chart", "USDJPY")
    assert published == ChartState.PUBLISHED
    await _close(server, spawner, usdjpy)


async def test_the_latest_spawner_to_say_hello_is_the_spawner(caplog):
    caplog.set_level(logging.WARNING, logger="mt5connector.server.ws_server")
    hub, server, port, _ = await _run_hub()
    first = await _ea(port, "EURUSD", spawner=True)
    second = await _ea(port, "GBPUSD", spawner=True)
    adapter = await _adapter(port)
    await adapter.send(json.dumps(subscribe(1, "trade_transactions")))
    await _received(adapter)
    unmarked = request_transaction("")

    await hub.use_chart("USDJPY")
    await first.send(json.dumps(unmarked))
    await second.send(json.dumps(unmarked))

    assert await _received(second) == addressed("open_chart", "USDJPY")
    await _nothing(first)
    assert await _received(adapter) == unmarked
    await _nothing(adapter)
    assert _warnings(caplog) == [
        "the EA for GBPUSD replaces the EA for EURUSD as the spawner",
        "dropping a trade_transaction frame for '' from the EA for EURUSD",
    ]
    await _close(server, adapter, first, second)


async def test_a_symbol_in_use_whose_ea_goes_away_has_its_chart_requested_again():
    hub, server, port, _ = await _run_hub()
    spawner = await _ea(port, "EURUSD", spawner=True)
    usdjpy = await _ea(port, "USDJPY")
    adapter = await _adapter(port)
    await adapter.send(json.dumps(subscribe(1, "ticks", symbol="USDJPY")))
    await _received(usdjpy)

    await usdjpy.close()

    assert await _received(spawner) == addressed("open_chart", "USDJPY")
    await _close(server, adapter, spawner)


async def _spawning(hub, *eas):
    """Each EA double served by the hub, saying hello for its symbol, the first as the spawner."""
    tasks = []
    for index, (ea, symbol) in enumerate(eas):
        tasks.append(asyncio.create_task(hub.handler(ea)))
        ea.put(ea_hello(symbol, spawner=index == 0))
    await _settle()
    return tasks


async def _closed(tasks, *eas):
    for ea in eas:
        await ea.close()
    await asyncio.wait_for(asyncio.gather(*tasks), timeout=1)


async def test_a_chart_the_spawner_failed_to_open_is_answered_failed_until_it_opens(caplog):
    caplog.set_level(logging.WARNING, logger="mt5connector.server.ws_server")
    hub = Hub(CLOCK, lambda path, frame: None, idle_s=IDLE_S)
    spawner = Peer()
    tasks = await _spawning(hub, (spawner, "EURUSD"))

    requested = await hub.use_chart("XYZ")
    spawner.put(chart_failed("XYZ"))
    await _settle()
    failed = [await hub.use_chart("XYZ"), await hub.use_chart("XYZ")]
    spawner.put(chart_opened("XYZ"))
    await _settle()
    opened = await hub.use_chart("XYZ")

    assert (requested, failed, opened) == ("requested", ["failed", "failed"], "requested")
    assert spawner.sent == [wanted("EURUSD"), addressed("open_chart", "XYZ")]
    assert _warnings(caplog) == [f"the spawner cannot open a chart of XYZ: {NOT_LISTED}"]
    await _closed(tasks, spawner)


async def test_a_failed_chart_clears_on_its_eas_hello_and_a_use_after_the_ea_left_requests_it():
    hub = Hub(CLOCK, lambda path, frame: None, idle_s=IDLE_S)
    spawner = Peer()
    xyz = Peer()
    tasks = await _spawning(hub, (spawner, "EURUSD"))
    await hub.use_chart("XYZ")
    spawner.put(chart_failed("XYZ"))
    await _settle()
    failed = await hub.use_chart("XYZ")

    tasks.append(asyncio.create_task(hub.handler(xyz)))
    xyz.put(ea_hello("XYZ"))
    await _settle()
    published = await hub.use_chart("XYZ")
    await xyz.close()
    await _settle()
    again = await hub.use_chart("XYZ")

    assert (failed, published, again) == ("failed", "published", "requested")
    open_xyz = addressed("open_chart", "XYZ")
    assert spawner.sent == [wanted("EURUSD"), open_xyz, open_xyz]
    await _closed(tasks, spawner)


async def test_a_chart_answer_from_an_ea_other_than_the_spawner_is_dropped_with_a_warning(caplog):
    caplog.set_level(logging.WARNING, logger="mt5connector.server.ws_server")
    hub = Hub(CLOCK, lambda path, frame: None, idle_s=IDLE_S)
    spawner = Peer()
    gbpusd = Peer()
    tasks = await _spawning(hub, (spawner, "EURUSD"), (gbpusd, "GBPUSD"))
    await hub.use_chart("XYZ")

    gbpusd.put(chart_failed("XYZ"))
    await _settle()

    assert await hub.use_chart("XYZ") == "requested"
    assert _warnings(caplog) == ["dropping a chart_failed frame for 'XYZ' from the EA for GBPUSD"]
    await _closed(tasks, spawner, gbpusd)


# ── Idle charts ──────────────────────────────────────────────────────────────


@pytest.fixture
def clock(monkeypatch):
    """The hub's monotonic clock, at a value a test sets."""
    now = [1_000.0]
    monkeypatch.setattr(ws_server, "time", SimpleNamespace(monotonic=lambda: now[0]))
    return now


async def test_the_spawner_is_told_to_close_the_chart_of_a_symbol_out_of_use_for_the_idle_period(
    clock,
):
    hub, server, port, _ = await _run_hub()
    spawner = await _ea(port, "EURUSD", spawner=True)
    usdjpy = await _ea(port, "USDJPY")
    xauusd = await _ea(port, "XAUUSD")
    adapter = await _adapter(port)
    await adapter.send(json.dumps(subscribe(1, "ticks", symbol="XAUUSD")))
    await _received(xauusd)

    clock[0] = 1_000.0 + IDLE_S - 1
    await hub.close_idle_charts()
    await _nothing(spawner)
    clock[0] = 1_000.0 + IDLE_S
    await hub.close_idle_charts()
    await hub.close_idle_charts()

    assert await _received(spawner) == addressed("close_chart", "USDJPY")
    await _nothing(spawner)
    await _nothing(usdjpy)
    await _nothing(xauusd)
    await _close(server, adapter, spawner, usdjpy, xauusd)


async def test_a_touch_moves_a_symbols_idle_deadline(clock):
    hub, server, port, _ = await _run_hub()
    spawner = await _ea(port, "EURUSD", spawner=True)
    usdjpy = await _ea(port, "USDJPY")

    clock[0] = 1_800.0
    touched = await hub.use_chart("USDJPY")
    clock[0] = 1_000.0 + IDLE_S + 1
    await hub.close_idle_charts()
    await _nothing(spawner)
    clock[0] = 1_800.0 + IDLE_S
    await hub.close_idle_charts()

    assert touched == ChartState.PUBLISHED
    assert await _received(spawner) == addressed("close_chart", "USDJPY")
    await _close(server, spawner, usdjpy)


async def test_a_use_after_the_idle_close_requests_the_chart_again(clock):
    hub, server, port, _ = await _run_hub()
    spawner = await _ea(port, "EURUSD", spawner=True)
    usdjpy = await _ea(port, "USDJPY")
    clock[0] = 1_000.0 + IDLE_S
    await hub.close_idle_charts()
    assert await _received(spawner) == addressed("close_chart", "USDJPY")

    # The spawner closes the chart, which unloads its EA.
    await usdjpy.close()
    await _nothing(spawner)
    state = await hub.use_chart("USDJPY")

    assert state == ChartState.REQUESTED
    assert await _received(spawner) == addressed("open_chart", "USDJPY")
    await _close(server, spawner)


async def test_a_chart_the_spawner_keeps_open_is_closed_only_after_a_further_idle_period(clock):
    hub = Hub(CLOCK, lambda path, frame: None, idle_s=IDLE_S)
    spawner = Peer()
    usdjpy = Peer()
    tasks = await _spawning(hub, (spawner, "EURUSD"), (usdjpy, "USDJPY"))
    clock[0] = 1_000.0 + IDLE_S
    await hub.close_idle_charts()

    spawner.put(chart_kept("USDJPY"))
    await _settle()
    clock[0] = 1_000.0 + 2 * IDLE_S - 1
    await hub.close_idle_charts()
    kept = list(spawner.sent)
    clock[0] = 1_000.0 + 2 * IDLE_S
    await hub.close_idle_charts()

    close = addressed("close_chart", "USDJPY")
    assert kept == [wanted("EURUSD"), close]
    assert spawner.sent == [wanted("EURUSD"), close, close]
    assert usdjpy.sent == [wanted("USDJPY")]
    await _closed(tasks, spawner, usdjpy)


# ── Bounds and keepalive ─────────────────────────────────────────────────────


class Peer:
    """A connection double: what is put on `incoming` reaches the hub as frames, and while `stuck`
    no send of the hub's ever completes."""

    def __init__(self, stuck=False):
        self.incoming = asyncio.Queue()
        self.sent = []
        self.closed = None
        self.stuck = stuck
        self.remote_address = ("127.0.0.1", 0)

    def put(self, frame):
        self.incoming.put_nowait(json.dumps(frame))

    def __aiter__(self):
        return self

    async def __anext__(self):
        raw = await self.incoming.get()
        if raw is None:
            raise StopAsyncIteration
        return raw

    async def send(self, raw):
        if self.stuck:
            await asyncio.Event().wait()
        self.sent.append(json.loads(raw))

    async def close(self, code=1000, reason=""):
        self.closed = (code, reason)
        self.incoming.put_nowait(None)


async def _settle():
    for _ in range(20):
        await asyncio.sleep(0)


async def test_a_consumer_with_10_001_queued_frames_is_closed_with_4008_and_the_rest_serve_on():
    hub = Hub(CLOCK, lambda path, frame: None, idle_s=IDLE_S)
    stuck = Peer(stuck=True)
    reading = Peer()
    ea = Peer()
    tasks = [asyncio.create_task(hub.handler(peer)) for peer in (stuck, reading, ea)]
    for peer in (stuck, reading):
        peer.put(adapter_hello())
        peer.put(subscribe(1, "ticks", symbol="EURUSD"))
    ea.put(ea_hello("EURUSD"))
    await _settle()

    for _ in range(10_000):
        ea.put(tick())
    await _settle()
    behind_by_10_000 = stuck.closed
    ea.put(tick())
    await _settle()

    assert behind_by_10_000 is None
    assert stuck.closed[0] == 4008
    ea.put(tick())
    await _settle()
    assert len(reading.sent) == 1 + 10_002
    assert reading.closed is None
    for peer in (reading, ea):
        await peer.close()
    await asyncio.wait_for(asyncio.gather(*tasks), timeout=1)


async def test_the_hub_pings_every_10_s_and_drops_a_peer_after_3_missed_pongs():
    hub, server, port, _ = await _run_hub()
    adapter = await _adapter(port)
    await asyncio.sleep(0.05)

    (connection,) = server.connections
    assert (connection.ping_interval, connection.ping_timeout) == (10, 30)
    await _close(server, adapter)


# ── Roles, kinds and malformed frames ────────────────────────────────────────


@pytest.mark.parametrize(
    ("hello", "frame", "message"),
    [
        (adapter_hello(), tick(), "a tick frame from an adapter"),
        (ea_hello(), subscribe(1, "ticks", symbol="EURUSD"), "a subscribe frame from an ea"),
        (adapter_hello(), subscribe(True, "ticks", symbol="EURUSD"), "not an op id: True"),
        (
            adapter_hello(),
            subscribe(1, "quotes", symbol="EURUSD"),
            "'quotes' is not a valid Stream",
        ),
        (ea_hello(), tick() | {"time": "1"}, "tick: time '1' is not an integer epoch"),
        (None, ea_hello() | {"symbol": ""}, "hello: not a symbol: ''"),
        (None, ea_hello() | {"spawner": 1}, "hello: spawner is not a boolean: 1"),
        (
            None,
            {"v": 1, "type": "hello", "role": "ea", "spawner": False},
            "hello: missing field: symbol",
        ),
        (None, adapter_hello() | {"symbol": "EURUSD"}, "hello: unknown field: symbol"),
        (
            ea_hello(),
            addressed("chart_failed", "XYZ"),
            "chart_failed: missing field: reason",
        ),
        (
            ea_hello(),
            chart_opened("XYZ") | {"reason": NOT_LISTED},
            "chart_opened: unknown field: reason",
        ),
        (ea_hello(), chart_kept(""), "chart_kept: not a symbol: ''"),
        (ea_hello(), chart_failed("XYZ", 4301), "chart_failed: reason is not a string: 4301"),
    ],
    ids=[
        "tick-from-adapter",
        "op-from-ea",
        "bool-op-id",
        "unknown-stream",
        "string-epoch",
        "empty-symbol",
        "integer-spawner",
        "ea-without-symbol",
        "adapter-with-symbol",
        "chart-failure-without-reason",
        "chart-opened-with-reason",
        "chart-kept-without-symbol",
        "integer-chart-failure-reason",
    ],
)
async def test_a_frame_the_hub_cannot_handle_is_answered_with_an_error_and_closed(
    hello, frame, message, caplog
):
    caplog.set_level(logging.WARNING, logger="mt5connector.server.ws_server")
    hub, server, port, _ = await _run_hub()
    ws = await websockets.connect(f"ws://127.0.0.1:{port}")
    if hello is not None:
        await ws.send(json.dumps(hello))
    if hello is not None and hello["role"] == "ea":
        await _received(ws)

    await ws.send(json.dumps(frame))

    assert await _received(ws) == {"v": 1, "type": "error", "message": message}
    await asyncio.wait_for(ws.wait_closed(), timeout=1)
    assert ws.close_code == 1002
    warnings = _warnings(caplog)
    assert len(warnings) == 1
    assert warnings[0].startswith("closing connection ")
    assert warnings[0].endswith(f" on a frame the hub cannot handle: {message}")
    server.close()


async def test_a_malformed_frame_closes_its_connection_and_the_hub_serves_on():
    hub, server, port, _ = await _run_hub()
    broken = await _ea(port, "GBPUSD")
    ea = await _ea(port, "EURUSD")
    adapter = await _adapter(port)
    await adapter.send(json.dumps(subscribe(1, "ticks", symbol="EURUSD")))
    await _received(adapter)

    await broken.send("not json")
    await asyncio.wait_for(broken.wait_closed(), timeout=1)
    await ea.send(json.dumps(tick()))

    assert await _received(adapter) == utc_tick()
    await _close(server, adapter, ea)


async def test_a_frame_of_an_unknown_kind_is_logged_and_skipped(caplog):
    caplog.set_level(logging.INFO, logger="mt5connector.server.ws_server")
    hub, server, port, _ = await _run_hub()
    adapter = await _adapter(port)

    await adapter.send(json.dumps({"v": 1, "type": "quote"}))
    await adapter.send(json.dumps(subscribe(1, "ticks", symbol="EURUSD")))

    assert await _received(adapter) == {"v": 1, "type": "ack", "id": 1}
    assert "unknown kind 'quote'" in [record.getMessage() for record in caplog.records]
    await _close(server, adapter)


# ── Relays ───────────────────────────────────────────────────────────────────


async def test_an_eas_server_time_and_commissions_are_relayed_under_its_symbol_never_fanned_out():
    hub, server, port, relayed = await _run_hub()
    ea = await _ea(port, "EURUSD.a")
    adapter = await _adapter(port)
    await adapter.send(json.dumps(subscribe(1, "ticks", symbol="EURUSD.a")))
    await _received(adapter)

    await ea.send(json.dumps(server_time("EURUSD.a")))
    await ea.send(json.dumps(commissions("EURUSD.a")))

    await _nothing(adapter)
    assert relayed == [
        (SERVER_TIME_RELAY_PATH, server_time("EURUSD.a")),
        (f"{COMMISSIONS_RELAY_PATH}/EURUSD.a", commissions("EURUSD.a")),
    ]
    await _close(server, adapter, ea)


async def test_a_commissions_frame_naming_another_symbol_is_relayed_under_the_eas():
    hub, server, port, relayed = await _run_hub()
    ea = await _ea(port, "US500#")

    await ea.send(json.dumps(commissions("EURUSD")))
    await _settle_relays(relayed, 1)

    assert relayed == [(f"{COMMISSIONS_RELAY_PATH}/US500%23", commissions("EURUSD"))]
    await _close(server, ea)


async def test_a_server_time_sample_naming_another_symbol_is_dropped_with_one_warning(caplog):
    caplog.set_level(logging.WARNING, logger="mt5connector.server.ws_server")
    hub, server, port, relayed = await _run_hub()
    ea = await _ea(port, "GBPUSD")

    await ea.send(json.dumps(server_time("EURUSD")))
    await ea.send(json.dumps(server_time("GBPUSD")))
    await _settle_relays(relayed, 1)

    assert relayed == [(SERVER_TIME_RELAY_PATH, server_time("GBPUSD"))]
    assert _warnings(caplog) == ["dropping a server_time frame for 'EURUSD' from the EA for GBPUSD"]
    await _close(server, ea)


async def _settle_relays(relayed, count):
    for _ in range(50):
        if len(relayed) >= count:
            return
        await asyncio.sleep(0.01)


def test_the_relay_posts_a_frame_to_the_servers_route_over_loopback(served, server_times):
    post_to(served)(SERVER_TIME_RELAY_PATH, server_time())

    assert server_times.wait_newer(None, 0).sample == ServerTimeSample(
        symbol="EURUSD",
        trade_server=1752580800,
        current=1752580798,
        gmt=1752570000,
        connected=True,
    )


def test_a_refused_post_is_logged_and_not_raised(served, server_times, caplog):
    caplog.set_level(logging.WARNING, logger="mt5connector.server.ws_server")

    post_to(served)(SERVER_TIME_RELAY_PATH, server_time() | {"v": 2})

    assert server_times.wait_newer(None, 0) is None
    assert [record.getMessage() for record in caplog.records] == [
        f"relay to {served}/relay/server_time failed: HTTP Error 400: BAD REQUEST"
    ]


def test_an_unreachable_server_is_logged_and_not_raised(caplog):
    caplog.set_level(logging.WARNING, logger="mt5connector.server.ws_server")
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    base = f"http://127.0.0.1:{port}"

    post_to(base)(SERVER_TIME_RELAY_PATH, server_time())

    messages = [record.getMessage() for record in caplog.records]
    assert len(messages) == 1
    assert messages[0].startswith(f"relay to {base}/relay/server_time failed: ")
