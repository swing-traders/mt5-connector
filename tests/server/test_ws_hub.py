import asyncio
import json
import logging
import socket

import pytest
import websockets
from websockets.asyncio.server import serve

from mt5connector.server.server_time import ServerTimeSample
from mt5connector.server.ws_server import TickHub, post_to

SERVER_TIME = {
    "v": 1,
    "type": "server_time",
    "symbol": "EURUSD",
    "trade_server": 1752580800,
    "current": 1752580798,
    "gmt": 1752570000,
    "connected": 1,
}
TICK = {"symbol": "EURUSD", "bid": "1.08", "time_msec": "1", "flags": "2"}


async def _run_hub():
    """A hub on a free loopback port whose relay records each frame it is handed."""
    relayed = []
    hub = TickHub(relayed.append)
    server = await serve(hub.handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    return hub, server, port, relayed


async def _connect(port, role):
    ws = await websockets.connect(f"ws://127.0.0.1:{port}")
    await ws.send(json.dumps({"type": "hello", "role": role}))
    return ws


@pytest.mark.asyncio
async def test_tick_relayed_to_adapter():
    hub, server, port, _ = await _run_hub()
    ea = await _connect(port, "ea")
    adapter = await _connect(port, "adapter")
    await asyncio.sleep(0.05)

    # new-format tick: no "type" key, all-string values
    await ea.send(
        json.dumps(
            {
                "symbol": "EURUSD",
                "time": "2024.06.01 10:00:00",
                "ask": "1.0801",
                "bid": "1.08",
                "volume": "0",
                "last": "0.0",
                "time_msec": "1712345678123",
                "flags": "2",
            }
        )
    )
    got = json.loads(await asyncio.wait_for(adapter.recv(), timeout=1))
    assert got["symbol"] == "EURUSD" and got["bid"] == "1.08"

    await adapter.close()
    await ea.close()
    server.close()


@pytest.mark.asyncio
async def test_multiple_ea_connections_all_relay():
    hub, server, port, _ = await _run_hub()
    ea1 = await _connect(port, "ea")
    ea2 = await _connect(port, "ea")
    adapter = await _connect(port, "adapter")
    await asyncio.sleep(0.05)

    await ea1.send(json.dumps({"symbol": "EURUSD", "bid": "1.08", "time_msec": "1", "flags": "2"}))
    m1 = json.loads(await asyncio.wait_for(adapter.recv(), timeout=1))
    assert m1["symbol"] == "EURUSD"

    await ea2.send(
        json.dumps({"symbol": "XAUUSD", "ask": "2340.5", "time_msec": "2", "flags": "4"})
    )
    m2 = json.loads(await asyncio.wait_for(adapter.recv(), timeout=1))
    assert m2["symbol"] == "XAUUSD"

    assert len(hub._eas) == 2

    await adapter.close()
    await ea1.close()
    await ea2.close()
    server.close()


@pytest.mark.asyncio
async def test_subscribe_not_forwarded_to_ea():
    hub, server, port, _ = await _run_hub()
    ea = await _connect(port, "ea")
    adapter = await _connect(port, "adapter")
    await asyncio.sleep(0.05)

    await adapter.send(json.dumps({"type": "subscribe", "symbols": ["EURUSD"]}))
    await asyncio.sleep(0.05)
    assert hub._symbols == {"EURUSD"}

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(ea.recv(), timeout=0.3)

    await adapter.close()
    await ea.close()
    server.close()


@pytest.mark.asyncio
async def test_ea_disconnect_releases_slot():
    hub, server, port, _ = await _run_hub()
    ea = await _connect(port, "ea")
    await asyncio.sleep(0.05)
    assert len(hub._eas) == 1

    await ea.close()
    await asyncio.sleep(0.05)
    assert len(hub._eas) == 0

    server.close()


@pytest.mark.asyncio
async def test_server_time_from_an_ea_is_relayed_once_and_not_broadcast():
    hub, server, port, relayed = await _run_hub()
    ea = await _connect(port, "ea")
    adapter = await _connect(port, "adapter")
    await asyncio.sleep(0.05)

    await ea.send(json.dumps(SERVER_TIME))

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(adapter.recv(), timeout=0.3)
    assert relayed == [SERVER_TIME]

    await ea.send(json.dumps(TICK))
    got = json.loads(await asyncio.wait_for(adapter.recv(), timeout=1))
    assert got == TICK

    await adapter.close()
    await ea.close()
    server.close()


@pytest.mark.asyncio
async def test_server_time_from_a_connection_that_is_not_an_ea_is_not_relayed():
    hub, server, port, relayed = await _run_hub()
    adapter = await _connect(port, "adapter")
    await asyncio.sleep(0.05)

    await adapter.send(json.dumps(SERVER_TIME))
    await asyncio.sleep(0.3)

    assert relayed == []

    await adapter.close()
    server.close()


@pytest.mark.asyncio
async def test_a_malformed_frame_closes_its_connection_and_the_hub_serves_on(caplog):
    caplog.set_level(logging.WARNING, logger="mt5connector.server.ws_server")
    hub, server, port, _ = await _run_hub()
    broken = await _connect(port, "ea")
    ea = await _connect(port, "ea")
    adapter = await _connect(port, "adapter")
    await asyncio.sleep(0.05)

    await broken.send("not json")
    await asyncio.wait_for(broken.wait_closed(), timeout=1)
    await ea.send(json.dumps(TICK))
    got = json.loads(await asyncio.wait_for(adapter.recv(), timeout=1))

    assert got == TICK
    assert len(hub._eas) == 1
    warnings = [record.getMessage() for record in caplog.records]
    assert len(warnings) == 1
    assert warnings[0].startswith("closing connection ")
    assert warnings[0].endswith(" on a frame the hub cannot handle")

    await adapter.close()
    await ea.close()
    server.close()


def test_the_relay_posts_a_frame_to_the_server_over_loopback(served, server_times):
    post_to(f"{served}/relay/server_time")(SERVER_TIME)

    assert server_times.wait_newer(None, 0).sample == ServerTimeSample(
        symbol="EURUSD",
        trade_server=1752580800,
        current=1752580798,
        gmt=1752570000,
        connected=True,
    )


def test_a_refused_post_is_logged_and_not_raised(served, server_times, caplog):
    caplog.set_level(logging.WARNING, logger="mt5connector.server.ws_server")

    post_to(f"{served}/relay/server_time")(SERVER_TIME | {"v": 2})

    assert server_times.wait_newer(None, 0) is None
    assert [record.getMessage() for record in caplog.records] == [
        f"server_time relay to {served}/relay/server_time failed: HTTP Error 400: BAD REQUEST"
    ]


def test_an_unreachable_server_is_logged_and_not_raised(caplog):
    caplog.set_level(logging.WARNING, logger="mt5connector.server.ws_server")
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    url = f"http://127.0.0.1:{port}/relay/server_time"

    post_to(url)(SERVER_TIME)

    messages = [record.getMessage() for record in caplog.records]
    assert len(messages) == 1
    assert messages[0].startswith(f"server_time relay to {url} failed: ")
