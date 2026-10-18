"""The hub's streams, charts and relays: each publisher's frames fanned out to their subscribers in
true UTC, and the frames the hub refuses."""

import asyncio
import calendar
import logging
import time
from types import SimpleNamespace

import pytest
import websockets
from ea_wire import FakeEa, encode_ea, frame, zeroed
from hub_doubles import Consumer, Peer, run_hub, sample, settings, settle
from mirror_samples import BROKER_EPOCH, CLOCK, UTC_EPOCH

from mt5connector.server import ws_server
from mt5connector.server.wire import binary, mirror
from mt5connector.server.wire.binary import FrameKind
from mt5connector.server.wire.history_wire import Series
from mt5connector.server.wire.push_wire import (
    ChartRole,
    ChartState,
    Stream,
    Subscription,
    TransactionType,
)
from mt5connector.server.ws_server import Hub

IDLE_S = 900
CLOSE_BOUND_S = 30
PUBLISHER = ChartRole.PUBLISHER
SPAWNER = ChartRole.SPAWNER


def ticks_of(symbol):
    return Subscription(Stream.TICKS, symbol)


def tick(epoch=BROKER_EPOCH, msc=0, bid=1.085):
    return {
        "time": epoch,
        "bid": bid,
        "ask": bid + 0.00012,
        "last": 0.0,
        "volume": 0,
        "time_msc": epoch * 1000 + msc,
        "flags": 6,
        "volume_real": 0.0,
    }


def rates(epoch=BROKER_EPOCH, **fields):
    return {
        "time": epoch,
        "open": 1.085,
        "high": 1.0852,
        "low": 1.0849,
        "close": 1.0851,
        "tick_volume": 42,
        "spread": 12,
        "real_volume": 0,
    } | fields


def transaction(symbol="EURUSD", kind=TransactionType.DEAL_ADD, request_symbol="", expiration=0):
    return binary.TradeTransaction(
        zeroed(binary.TRADE_TRANSACTION, deal=7001, order=9001, symbol=symbol, type=int(kind))
        | {"price": 1.085, "volume": 0.01, "position": 9001, "time_expiration": expiration},
        zeroed(binary.TRADE_REQUEST, symbol=request_symbol, expiration=expiration),
        zeroed(binary.TRADE_RESULT),
    )


async def publisher(port, symbol="EURUSD"):
    ea = await FakeEa.connect(port, PUBLISHER, symbol)
    assert await ea.received() == binary.Wanted(symbol, False, ())
    return ea


async def nothing(ws, timeout=0.3):
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(ws.recv(), timeout)


def warnings(caplog):
    return [record.getMessage() for record in caplog.records if record.levelno == logging.WARNING]


async def close_all(server, *peers):
    for peer in peers:
        await peer.close()
    server.close()


# ── Fan-out ───────────────────────────────────────────────────────────────────


async def test_a_publishers_ticks_reach_the_consumers_of_its_symbol_alone_in_true_utc():
    hub, server, port = await run_hub()
    eurusd = await publisher(port)
    first = await Consumer.connect(port)
    other = await Consumer.connect(port)
    await first.subscribed(1, ticks_of("EURUSD"))
    await other.subscribed(1, ticks_of("XAUUSD"))

    await eurusd.send(binary.Ticks("EURUSD", (tick(msc=1), tick(msc=2))))

    assert await first.received() == binary.Ticks(
        "EURUSD", (tick(UTC_EPOCH, msc=1), tick(UTC_EPOCH, msc=2))
    )
    await nothing(other.ws)
    await close_all(server, first, other, eurusd)


async def test_ticks_in_two_frames_are_passed_on_in_the_order_published():
    hub, server, port = await run_hub()
    eurusd = await publisher(port)
    consumer = await Consumer.connect(port)
    await consumer.subscribed(1, ticks_of("EURUSD"))

    await eurusd.send(binary.Ticks("EURUSD", (tick(bid=1.08501),)))
    await eurusd.send(binary.Ticks("EURUSD", (tick(bid=1.08502),)))

    received = [await consumer.received(), await consumer.received()]
    assert [frame_.ticks[0]["bid"] for frame_ in received] == [1.08501, 1.08502]
    await close_all(server, consumer, eurusd)


async def test_a_bar_reaches_the_consumers_of_its_series_by_its_name_with_its_close_in_true_utc():
    hub, server, port = await run_hub()
    eurusd = await publisher(port)
    m5 = await Consumer.connect(port)
    m1 = await Consumer.connect(port)
    await m5.subscribed(1, Subscription(Stream.BARS, "EURUSD", Series.M5))
    await m1.subscribed(1, Subscription(Stream.BARS, "EURUSD", Series.M1))

    await eurusd.send(binary.RatesBar("EURUSD", mirror.TIMEFRAME_M5, rates()))

    assert await m5.received() == binary.Bar(
        "EURUSD", Series.M5, rates(UTC_EPOCH) | {"time_close": UTC_EPOCH + 300}
    )
    await nothing(m1.ws)
    await close_all(server, m5, m1, eurusd)


async def test_trade_transactions_reach_their_subscribers_in_order_their_expiration_in_true_utc():
    hub, server, port = await run_hub()
    eurusd = await publisher(port)
    consumer = await Consumer.connect(port)
    await consumer.subscribed(1, Subscription(Stream.TRADE_TRANSACTIONS))

    await eurusd.send(transaction(expiration=BROKER_EPOCH))
    await eurusd.send(transaction("", TransactionType.REQUEST, request_symbol="EURUSD"))

    first, second = await consumer.received(), await consumer.received()
    assert first.transaction["time_expiration"] == UTC_EPOCH
    assert first.request["expiration"] == UTC_EPOCH
    assert second == transaction("", TransactionType.REQUEST, request_symbol="EURUSD")
    await close_all(server, consumer, eurusd)


async def test_a_frame_a_publisher_sends_for_another_symbol_is_dropped_with_a_warning(caplog):
    caplog.set_level(logging.WARNING, logger="mt5connector.server.ws_server")
    hub, server, port = await run_hub()
    eurusd = await publisher(port)
    gbpusd = await publisher(port, "GBPUSD")
    consumer = await Consumer.connect(port)
    await consumer.subscribed(1, ticks_of("EURUSD"))
    await consumer.subscribed(2, Subscription(Stream.TRADE_TRANSACTIONS))

    await gbpusd.send(binary.Ticks("EURUSD", (tick(),)))
    await gbpusd.send(transaction("EURUSD"))
    await gbpusd.send(transaction("", TransactionType.REQUEST, request_symbol="EURUSD"))
    await gbpusd.send(transaction(""))
    await eurusd.send(binary.Ticks("EURUSD", (tick(),)))

    assert isinstance(await consumer.received(), binary.Ticks)
    await nothing(consumer.ws)
    dropped = warnings(caplog)
    assert len(dropped) == 4
    assert all(warning.endswith("from the publisher EA for GBPUSD") for warning in dropped)
    await close_all(server, consumer, eurusd, gbpusd)


async def test_a_transaction_naming_no_symbol_passes_through_the_spawner_alone():
    hub, server, port = await run_hub()
    spawner = await FakeEa.connect(port, SPAWNER, "EURUSD")
    consumer = await Consumer.connect(port)
    await consumer.subscribed(1, Subscription(Stream.TRADE_TRANSACTIONS))
    # A balance operation's deal names no symbol either.
    unmarked = transaction("", TransactionType.REQUEST)
    balance = transaction("")

    await spawner.send(unmarked)
    await spawner.send(balance)

    assert [await consumer.received(), await consumer.received()] == [unmarked, balance]
    await close_all(server, consumer, spawner)


# ── Wanted frames ─────────────────────────────────────────────────────────────


async def test_each_publisher_is_told_the_wanted_frame_of_its_own_symbol_alone():
    hub, server, port = await run_hub()
    eurusd = await publisher(port)
    gbpusd = await publisher(port, "GBPUSD")
    consumer = await Consumer.connect(port)

    await consumer.subscribed(1, ticks_of("EURUSD"))
    eurusd_told = await eurusd.received()
    await consumer.subscribed(2, Subscription(Stream.BARS, "GBPUSD", Series.H1))
    await consumer.subscribed(3, Subscription(Stream.BARS, "GBPUSD", Series.M5))
    gbpusd_told = [await gbpusd.received(), await gbpusd.received()]

    assert eurusd_told == binary.Wanted("EURUSD", True, ())
    assert gbpusd_told == [
        binary.Wanted("GBPUSD", False, (mirror.TIMEFRAME_H1,)),
        binary.Wanted("GBPUSD", False, (mirror.TIMEFRAME_M5, mirror.TIMEFRAME_H1)),
    ]
    await nothing(eurusd.ws)
    await close_all(server, consumer, eurusd, gbpusd)


async def test_a_publisher_is_told_its_wanted_frame_on_its_hello_and_again_as_consumers_leave():
    hub, server, port = await run_hub()
    eurusd = await publisher(port)
    consumer = await Consumer.connect(port)
    await consumer.subscribed(1, ticks_of("EURUSD"))
    await eurusd.received()
    await eurusd.close()

    again = await FakeEa.connect(port, PUBLISHER, "EURUSD")
    told = await again.received()
    await consumer.close()

    assert told == binary.Wanted("EURUSD", True, ())
    assert await again.received() == binary.Wanted("EURUSD", False, ())
    await close_all(server, again)


async def test_a_subscription_another_consumer_shares_keeps_the_wanted_frame():
    hub, server, port = await run_hub()
    eurusd = await publisher(port)
    first = await Consumer.connect(port)
    second = await Consumer.connect(port)
    await first.subscribed(1, ticks_of("EURUSD"))
    assert await eurusd.received() == binary.Wanted("EURUSD", True, ())

    await second.subscribed(1, ticks_of("EURUSD"))
    await first.close()

    await nothing(eurusd.ws)
    await close_all(server, second, eurusd)


async def test_an_unsubscribe_is_acknowledged_and_ends_the_stream():
    hub, server, port = await run_hub()
    eurusd = await publisher(port)
    consumer = await Consumer.connect(port)
    await consumer.subscribed(1, ticks_of("EURUSD"))

    await consumer.send(binary.Unsubscribe(2, ticks_of("EURUSD")))
    acked = await consumer.received()
    await eurusd.send(binary.Ticks("EURUSD", (tick(),)))

    assert acked == binary.Ack(2)
    await nothing(consumer.ws)
    await close_all(server, consumer, eurusd)


# ── One EA per role and symbol ────────────────────────────────────────────────


async def test_a_second_ea_for_a_served_role_and_symbol_is_told_it_is_a_duplicate_and_closed(
    caplog,
):
    caplog.set_level(logging.WARNING, logger="mt5connector.server.ws_server")
    hub, server, port = await run_hub()
    first = await publisher(port)
    # The spawner on a chart of the same symbol serves another role.
    spawner = await FakeEa.connect(port, SPAWNER, "EURUSD")
    consumer = await Consumer.connect(port)
    await consumer.subscribed(1, ticks_of("EURUSD"))
    await first.received()

    second = await FakeEa.connect(port, PUBLISHER, "EURUSD")

    assert await second.received() == binary.Duplicate(PUBLISHER, "EURUSD")
    await asyncio.wait_for(second.ws.wait_closed(), 1)
    await first.send(binary.Ticks("EURUSD", (tick(),)))
    assert isinstance(await consumer.received(), binary.Ticks)
    (warning,) = warnings(caplog)
    assert warning.startswith("refusing a second publisher EA for EURUSD from ")
    await close_all(server, consumer, first, spawner)


async def test_a_refused_duplicates_frames_are_dropped_silently(caplog):
    caplog.set_level(logging.WARNING, logger="mt5connector.server.ws_server")
    hub = Hub(CLOCK, settings())
    first = Peer()
    second = Peer()
    consumer = Peer()
    tasks = [asyncio.create_task(hub.handler(peer)) for peer in (first, consumer)]
    first.put_ea(binary.EaHello(PUBLISHER, "EURUSD"))
    consumer.put_frame(binary.AdapterHello())
    consumer.put_frame(binary.Subscribe(1, ticks_of("EURUSD")))
    await settle()

    second.put_ea(binary.EaHello(PUBLISHER, "EURUSD"))
    second.put_ea(binary.Ticks("EURUSD", (tick(),)))
    handled = asyncio.create_task(hub.handler(second))
    await settle()

    assert second.sent == [binary.encode(binary.Duplicate(PUBLISHER, "EURUSD"))]
    assert second.closed == (1000, "duplicate")
    assert consumer.sent == [binary.encode(binary.Ack(1))]
    assert len(warnings(caplog)) == 1
    for peer in (first, consumer):
        await peer.close()
    await asyncio.wait_for(asyncio.gather(*tasks, handled), timeout=1)


async def test_a_second_spawner_is_a_duplicate_and_the_first_stays_the_spawner():
    hub, server, port = await run_hub()
    first = await FakeEa.connect(port, SPAWNER, "EURUSD")

    second = await FakeEa.connect(port, SPAWNER, "GBPUSD")

    assert await second.received() == binary.Duplicate(SPAWNER, "GBPUSD")
    await asyncio.wait_for(second.ws.wait_closed(), 1)
    assert hub.use_chart((PUBLISHER, "USDJPY")) == ChartState.REQUESTED
    assert await first.received() == binary.OpenChart(PUBLISHER, "USDJPY")
    await close_all(server, first)


# ── The spawner's charts ──────────────────────────────────────────────────────


async def test_a_subscription_to_a_symbol_no_publisher_serves_has_the_spawner_open_its_chart():
    hub, server, port = await run_hub()
    spawner = await FakeEa.connect(port, SPAWNER, "EURUSD")
    gbpusd = await publisher(port, "GBPUSD")
    consumer = await Consumer.connect(port)

    await consumer.subscribed(1, ticks_of("USDJPY"))

    assert await spawner.received() == binary.OpenChart(PUBLISHER, "USDJPY")
    await nothing(gbpusd.ws)
    usdjpy = await FakeEa.connect(port, PUBLISHER, "USDJPY")
    assert await usdjpy.received() == binary.Wanted("USDJPY", True, ())
    await usdjpy.send(binary.Ticks("USDJPY", (tick(),)))
    assert (await consumer.received()).symbol == "USDJPY"
    await close_all(server, consumer, spawner, gbpusd, usdjpy)


async def test_a_chart_requested_while_no_spawner_is_connected_is_sent_on_its_hello():
    hub, server, port = await run_hub()
    consumer = await Consumer.connect(port)
    await consumer.subscribed(1, ticks_of("USDJPY"))

    spawner = await FakeEa.connect(port, SPAWNER, "EURUSD")

    assert await spawner.received() == binary.OpenChart(PUBLISHER, "USDJPY")
    await close_all(server, consumer, spawner)


async def test_a_chart_is_requested_once_per_spawner_connection_until_its_ea_says_hello():
    hub, server, port = await run_hub()
    spawner = await FakeEa.connect(port, SPAWNER, "EURUSD")
    await asyncio.sleep(0.05)

    states = [hub.use_chart((PUBLISHER, "USDJPY")), hub.use_chart((PUBLISHER, "USDJPY"))]
    opened = await spawner.received()
    await nothing(spawner.ws)
    usdjpy = await publisher(port, "USDJPY")
    served = hub.use_chart((PUBLISHER, "USDJPY"))

    assert states == [ChartState.REQUESTED, ChartState.REQUESTED]
    assert opened == binary.OpenChart(PUBLISHER, "USDJPY")
    assert served == ChartState.PUBLISHED
    await close_all(server, spawner, usdjpy)


async def test_a_chart_in_use_whose_ea_leaves_is_requested_again():
    hub, server, port = await run_hub()
    spawner = await FakeEa.connect(port, SPAWNER, "EURUSD")
    usdjpy = await publisher(port, "USDJPY")
    consumer = await Consumer.connect(port)
    await consumer.subscribed(1, ticks_of("USDJPY"))
    await usdjpy.received()

    await usdjpy.close()

    assert await spawner.received() == binary.OpenChart(PUBLISHER, "USDJPY")
    await close_all(server, consumer, spawner)


async def _spawning(hub, *eas):
    """Each EA double served by the hub, saying hello for its role and symbol."""
    tasks = []
    for peer, chart_role, symbol in eas:
        tasks.append(asyncio.create_task(hub.handler(peer)))
        peer.put_ea(binary.EaHello(chart_role, symbol))
    await settle()
    return tasks


async def _closed(tasks, *peers):
    for peer in peers:
        await peer.close()
    await asyncio.wait_for(asyncio.gather(*tasks), timeout=1)


def sent_to_ea(peer: Peer) -> list:
    from ea_wire import decode_to_ea

    return [decode_to_ea(raw) for raw in peer.sent]


async def test_a_chart_the_spawner_failed_to_open_is_answered_failed_until_it_opens(caplog):
    caplog.set_level(logging.WARNING, logger="mt5connector.server.ws_server")
    hub = Hub(CLOCK, settings())
    spawner = Peer()
    tasks = await _spawning(hub, (spawner, SPAWNER, "EURUSD"))

    requested = hub.use_chart((PUBLISHER, "XYZ"))
    spawner.put_ea(binary.ChartFailed(PUBLISHER, "XYZ", 4301))
    await settle()
    failed = [hub.use_chart((PUBLISHER, "XYZ")), hub.use_chart((PUBLISHER, "XYZ"))]
    spawner.put_ea(binary.ChartOpened(PUBLISHER, "XYZ"))
    await settle()
    opened = hub.use_chart((PUBLISHER, "XYZ"))

    assert (requested, failed, opened) == (
        ChartState.REQUESTED,
        [ChartState.FAILED, ChartState.FAILED],
        ChartState.REQUESTED,
    )
    assert sent_to_ea(spawner) == [binary.OpenChart(PUBLISHER, "XYZ")]
    (warning,) = warnings(caplog)
    assert "4301" in warning
    await _closed(tasks, spawner)


async def test_a_failed_chart_clears_on_its_eas_hello_and_a_use_after_the_ea_left_requests_it():
    hub = Hub(CLOCK, settings())
    spawner = Peer()
    xyz = Peer()
    tasks = await _spawning(hub, (spawner, SPAWNER, "EURUSD"))
    hub.use_chart((PUBLISHER, "XYZ"))
    spawner.put_ea(binary.ChartFailed(PUBLISHER, "XYZ", 4301))
    await settle()
    failed = hub.use_chart((PUBLISHER, "XYZ"))

    tasks.append(asyncio.create_task(hub.handler(xyz)))
    xyz.put_ea(binary.EaHello(PUBLISHER, "XYZ"))
    await settle()
    served = hub.use_chart((PUBLISHER, "XYZ"))
    await xyz.close()
    await settle()
    again = hub.use_chart((PUBLISHER, "XYZ"))

    assert (failed, served, again) == (
        ChartState.FAILED,
        ChartState.PUBLISHED,
        ChartState.REQUESTED,
    )
    assert sent_to_ea(spawner) == [binary.OpenChart(PUBLISHER, "XYZ")] * 2
    await _closed(tasks, spawner)


async def test_a_chart_answer_from_a_publisher_is_refused_and_closes_it():
    hub, server, port = await run_hub()
    gbpusd = await publisher(port, "GBPUSD")

    await gbpusd.send(binary.ChartFailed(PUBLISHER, "XYZ", 4301))

    assert await gbpusd.received() == binary.Refusal("a ChartFailed frame from a publisher EA")
    await asyncio.wait_for(gbpusd.ws.wait_closed(), 1)
    server.close()


# ── Idle charts ───────────────────────────────────────────────────────────────


@pytest.fixture
def clock(monkeypatch):
    """The hub's monotonic clock, at a value a test sets."""
    now = [1_000.0]
    monkeypatch.setattr(
        ws_server, "time", SimpleNamespace(monotonic=lambda: now[0], time=time.time)
    )
    return now


async def test_the_spawner_is_told_to_close_each_chart_out_of_use_for_the_idle_period(clock):
    hub = Hub(CLOCK, settings())
    spawner, usdjpy, gbpusd, xauusd, consumer = Peer(), Peer(), Peer(), Peer(), Peer()
    tasks = await _spawning(
        hub,
        (spawner, SPAWNER, "EURUSD"),
        (usdjpy, PUBLISHER, "USDJPY"),
        (gbpusd, PUBLISHER, "GBPUSD"),
        (xauusd, PUBLISHER, "XAUUSD"),
    )
    tasks.append(asyncio.create_task(hub.handler(consumer)))
    consumer.put_frame(binary.AdapterHello())
    consumer.put_frame(binary.Subscribe(1, ticks_of("XAUUSD")))
    await settle()

    clock[0] = 1_000.0 + IDLE_S - 1
    hub.close_idle_charts()
    early = list(spawner.sent)
    clock[0] = 1_000.0 + IDLE_S
    hub.close_idle_charts()
    hub.close_idle_charts()
    await settle()

    assert early == []
    assert sorted(sent_to_ea(spawner), key=str) == sorted(
        [binary.CloseChart(PUBLISHER, "GBPUSD"), binary.CloseChart(PUBLISHER, "USDJPY")], key=str
    )
    await _closed(tasks, spawner, usdjpy, gbpusd, xauusd, consumer)


async def test_a_touch_moves_a_charts_idle_deadline(clock):
    hub = Hub(CLOCK, settings())
    spawner, usdjpy = Peer(), Peer()
    tasks = await _spawning(hub, (spawner, SPAWNER, "EURUSD"), (usdjpy, PUBLISHER, "USDJPY"))

    clock[0] = 1_800.0
    touched = hub.use_chart((PUBLISHER, "USDJPY"))
    clock[0] = 1_000.0 + IDLE_S + 1
    hub.close_idle_charts()
    early = list(spawner.sent)
    clock[0] = 1_800.0 + IDLE_S
    hub.close_idle_charts()
    await settle()

    assert (touched, early) == (ChartState.PUBLISHED, [])
    assert sent_to_ea(spawner) == [binary.CloseChart(PUBLISHER, "USDJPY")]
    await _closed(tasks, spawner, usdjpy)


async def test_a_chart_the_spawner_keeps_open_is_closed_only_after_a_further_idle_period(clock):
    hub = Hub(CLOCK, settings())
    spawner, usdjpy = Peer(), Peer()
    tasks = await _spawning(hub, (spawner, SPAWNER, "EURUSD"), (usdjpy, PUBLISHER, "USDJPY"))
    clock[0] = 1_000.0 + IDLE_S
    hub.close_idle_charts()

    spawner.put_ea(binary.ChartKept(PUBLISHER, "USDJPY", 1, 0))
    await settle()
    clock[0] = 1_000.0 + 2 * IDLE_S - 1
    hub.close_idle_charts()
    await settle()
    kept = sent_to_ea(spawner)
    clock[0] = 1_000.0 + 2 * IDLE_S
    hub.close_idle_charts()
    await settle()

    close = binary.CloseChart(PUBLISHER, "USDJPY")
    assert kept == [close]
    assert sent_to_ea(spawner) == [close, close]
    assert sent_to_ea(usdjpy) == [binary.Wanted("USDJPY", False, ())]
    await _closed(tasks, spawner, usdjpy)


async def test_after_a_chart_kept_the_next_close_is_sent_a_full_idle_period_later(clock, caplog):
    caplog.set_level(logging.WARNING, logger="mt5connector.server.ws_server")
    hub = Hub(CLOCK, settings(chart_idle_seconds=10))
    spawner, usdjpy = Peer(), Peer()
    tasks = await _spawning(hub, (spawner, SPAWNER, "EURUSD"), (usdjpy, PUBLISHER, "USDJPY"))
    clock[0] = 1_010.0
    hub.close_idle_charts()
    spawner.put_ea(binary.ChartKept(PUBLISHER, "USDJPY", 1, 0))
    await settle()

    clock[0] = 1_019.0
    hub.close_idle_charts()
    await settle()
    kept = sent_to_ea(spawner)
    clock[0] = 1_020.0
    hub.close_idle_charts()
    await settle()

    close = binary.CloseChart(PUBLISHER, "USDJPY")
    assert kept == [close]
    assert sent_to_ea(spawner) == [close, close]
    assert warnings(caplog) == []
    await _closed(tasks, spawner, usdjpy)


async def test_a_publisher_saying_hello_while_its_close_is_pending_is_not_revived(clock, caplog):
    caplog.set_level(logging.WARNING, logger="mt5connector.server.ws_server")
    hub = Hub(CLOCK, settings())
    spawner, usdjpy, again = Peer(), Peer(), Peer()
    tasks = await _spawning(hub, (spawner, SPAWNER, "EURUSD"), (usdjpy, PUBLISHER, "USDJPY"))
    clock[0] = 1_000.0 + IDLE_S
    hub.close_idle_charts()
    await settle()

    await usdjpy.close()
    await settle()
    tasks.append(asyncio.create_task(hub.handler(again)))
    again.put_ea(binary.EaHello(PUBLISHER, "USDJPY"))
    await settle()
    await again.close()
    await settle()
    clock[0] = 1_000.0 + IDLE_S + CLOSE_BOUND_S
    hub.close_idle_charts()
    await settle()

    assert sent_to_ea(spawner) == [binary.CloseChart(PUBLISHER, "USDJPY")]
    assert sent_to_ea(again) == [binary.Wanted("USDJPY", False, ())]
    assert warnings(caplog) == []
    await _closed(tasks, spawner)


async def test_a_hello_once_its_chart_has_closed_starts_its_idle_period(clock):
    hub = Hub(CLOCK, settings())
    spawner, usdjpy, reopened = Peer(), Peer(), Peer()
    tasks = await _spawning(hub, (spawner, SPAWNER, "EURUSD"), (usdjpy, PUBLISHER, "USDJPY"))
    clock[0] = 1_000.0 + IDLE_S
    hub.close_idle_charts()
    await usdjpy.close()
    await settle()
    clock[0] = 1_000.0 + IDLE_S + CLOSE_BOUND_S
    hub.close_idle_charts()

    hello = clock[0] + 1
    clock[0] = hello
    tasks.append(asyncio.create_task(hub.handler(reopened)))
    reopened.put_ea(binary.EaHello(PUBLISHER, "USDJPY"))
    await settle()
    clock[0] = hello + IDLE_S - 1
    hub.close_idle_charts()
    await settle()
    early = sent_to_ea(spawner)
    clock[0] = hello + IDLE_S
    hub.close_idle_charts()
    await settle()

    close = binary.CloseChart(PUBLISHER, "USDJPY")
    assert early == [close]
    assert sent_to_ea(spawner) == [close, close]
    await _closed(tasks, spawner, reopened)


async def test_a_close_neither_ended_nor_kept_within_its_bound_is_sent_again_once_per_bound(
    clock, caplog
):
    caplog.set_level(logging.WARNING, logger="mt5connector.server.ws_server")
    hub = Hub(CLOCK, settings())
    spawner, usdjpy = Peer(), Peer()
    tasks = await _spawning(hub, (spawner, SPAWNER, "EURUSD"), (usdjpy, PUBLISHER, "USDJPY"))
    sent = 1_000.0 + IDLE_S
    clock[0] = sent
    hub.close_idle_charts()

    clock[0] = sent + CLOSE_BOUND_S - 1
    hub.close_idle_charts()
    await settle()
    within = sent_to_ea(spawner)
    clock[0] = sent + CLOSE_BOUND_S
    hub.close_idle_charts()
    clock[0] = sent + 2 * CLOSE_BOUND_S - 1
    hub.close_idle_charts()
    await settle()
    once = sent_to_ea(spawner)
    clock[0] = sent + 2 * CLOSE_BOUND_S
    hub.close_idle_charts()
    await settle()

    close = binary.CloseChart(PUBLISHER, "USDJPY")
    assert within == [close]
    assert once == [close, close]
    assert sent_to_ea(spawner) == [close, close, close]
    resent = warnings(caplog)
    assert len(resent) == 2
    assert all("publisher" in warning and "USDJPY" in warning for warning in resent)
    await _closed(tasks, spawner, usdjpy)


async def test_a_pending_close_of_a_chart_in_use_again_ends_at_its_bound_unsent(clock, caplog):
    caplog.set_level(logging.WARNING, logger="mt5connector.server.ws_server")
    hub = Hub(CLOCK, settings())
    spawner, usdjpy = Peer(), Peer()
    tasks = await _spawning(hub, (spawner, SPAWNER, "EURUSD"), (usdjpy, PUBLISHER, "USDJPY"))
    sent = 1_000.0 + IDLE_S
    clock[0] = sent
    hub.close_idle_charts()

    clock[0] = sent + 10
    used = hub.use_chart((PUBLISHER, "USDJPY"))
    clock[0] = sent + CLOSE_BOUND_S
    hub.close_idle_charts()
    clock[0] = sent + 10 + IDLE_S - 1
    hub.close_idle_charts()
    await settle()
    in_use = sent_to_ea(spawner)
    clock[0] = sent + 10 + IDLE_S
    hub.close_idle_charts()
    await settle()

    close = binary.CloseChart(PUBLISHER, "USDJPY")
    assert used == ChartState.PUBLISHED
    assert in_use == [close]
    assert sent_to_ea(spawner) == [close, close]
    assert warnings(caplog) == []
    await _closed(tasks, spawner, usdjpy)


async def test_a_close_that_overflows_the_spawners_queue_leaves_no_close_pending(clock):
    hub = Hub(CLOCK, settings(ea_queue_bound=1))
    stuck, gbpusd, usdjpy, xauusd = Peer(stuck=True), Peer(), Peer(), Peer()
    tasks = await _spawning(
        hub,
        (stuck, SPAWNER, "EURUSD"),
        (gbpusd, PUBLISHER, "GBPUSD"),
        (usdjpy, PUBLISHER, "USDJPY"),
        (xauusd, PUBLISHER, "XAUUSD"),
    )
    clock[0] = 1_000.0 + IDLE_S
    hub.close_idle_charts()
    await settle()

    hello = 1_000.0 + IDLE_S + CLOSE_BOUND_S
    clock[0] = hello
    await xauusd.close()
    await settle()
    again, spawner = Peer(), Peer()
    tasks += await _spawning(hub, (again, PUBLISHER, "XAUUSD"), (spawner, SPAWNER, "EURUSD"))
    clock[0] = hello + 1
    hub.use_chart((PUBLISHER, "GBPUSD"))
    hub.use_chart((PUBLISHER, "USDJPY"))
    clock[0] = hello + IDLE_S - 1
    hub.close_idle_charts()
    await settle()
    early = sent_to_ea(spawner)
    clock[0] = hello + IDLE_S
    hub.close_idle_charts()
    await settle()

    assert stuck.closed == (ws_server.OVERFLOW, "queue overflow")
    assert early == []
    assert sent_to_ea(spawner) == [binary.CloseChart(PUBLISHER, "XAUUSD")]
    await _closed(tasks, spawner, gbpusd, usdjpy, again)


# ── The broker's repeated and skipped hours ───────────────────────────────────

# Broker 08:30 on 2025-11-02 and on 2024-11-03 is New York's repeated 01:30.
REPEATED = calendar.timegm((2025, 11, 2, 8, 30, 0))
REPEATED_A_YEAR_BEFORE = calendar.timegm((2024, 11, 3, 8, 30, 0))


def repeated_hour_warning(epoch):
    return f"tick: time {epoch} is in the broker's repeated hour, read as its first occurrence"


async def test_two_repeated_broker_hours_alternating_are_each_warned_of_once(caplog):
    caplog.set_level(logging.WARNING)
    hub = Hub(CLOCK, settings())
    ea, consumer = Peer(), Peer()
    tasks = [asyncio.create_task(hub.handler(peer)) for peer in (ea, consumer)]
    ea.put_ea(binary.EaHello(PUBLISHER, "EURUSD"))
    consumer.put_frame(binary.AdapterHello())
    consumer.put_frame(binary.Subscribe(1, ticks_of("EURUSD")))
    await settle()

    for epoch in (REPEATED, REPEATED_A_YEAR_BEFORE, REPEATED + 60, REPEATED_A_YEAR_BEFORE + 60):
        ea.put_ea(binary.Ticks("EURUSD", (tick(epoch),)))
    await settle()

    assert [raw[1] for raw in consumer.sent] == [FrameKind.ACK] + [FrameKind.TICK] * 4
    assert warnings(caplog) == [
        repeated_hour_warning(REPEATED),
        repeated_hour_warning(REPEATED_A_YEAR_BEFORE),
    ]
    await _closed(tasks, ea, consumer)


# On a symbol that trades weekends the H1 bar stamped broker 09:00 on 2026-03-08, in the hour the
# broker's clock skips, and the one stamped 10:00 both open at 07:00Z and close at 08:00Z.
SKIPPED = calendar.timegm((2026, 3, 8, 9, 0, 0))
AFTER_SKIPPED = calendar.timegm((2026, 3, 8, 10, 0, 0))
NEXT_HOUR = calendar.timegm((2026, 3, 8, 11, 0, 0))
SPRING_OPEN = calendar.timegm((2026, 3, 8, 7, 0, 0))
SPRING_BARS = {
    SKIPPED: {"open": 1.11, "high": 1.13, "low": 1.10, "close": 1.12, "tick_volume": 110},
    AFTER_SKIPPED: {"open": 1.12, "high": 1.16, "low": 1.11, "close": 1.15, "tick_volume": 490},
    NEXT_HOUR: {"open": 1.15, "high": 1.17, "low": 1.14, "close": 1.16, "tick_volume": 600},
}


def pushed_spring_bar(stamp, opened):
    bar = rates(opened, **SPRING_BARS[stamp]) | {"time_close": opened + 3_600}
    return binary.Bar("EURUSD", Series.H1, bar)


async def _pushed_h1(*stamps):
    """The frames a consumer of EURUSD's H1 bars receives as its publisher sends bars stamped at
    `stamps`."""
    hub = Hub(CLOCK, settings())
    ea, consumer = Peer(), Peer()
    tasks = [asyncio.create_task(hub.handler(peer)) for peer in (ea, consumer)]
    ea.put_ea(binary.EaHello(PUBLISHER, "EURUSD"))
    consumer.put_frame(binary.AdapterHello())
    consumer.put_frame(binary.Subscribe(1, Subscription(Stream.BARS, "EURUSD", Series.H1)))
    await settle()

    for stamp in stamps:
        ea.put_ea(
            binary.RatesBar("EURUSD", mirror.TIMEFRAME_H1, rates(stamp, **SPRING_BARS[stamp]))
        )
    await settle()

    await _closed(tasks, ea, consumer)
    return [binary.decode(raw, binary.Sender.HUB) for raw in consumer.sent[1:]]


async def test_of_two_bars_opening_in_one_true_utc_period_the_first_is_pushed_with_a_warning(
    caplog,
):
    caplog.set_level(logging.WARNING)

    pushed = await _pushed_h1(SKIPPED, AFTER_SKIPPED, NEXT_HOUR)

    assert pushed == [
        pushed_spring_bar(SKIPPED, SPRING_OPEN),
        pushed_spring_bar(NEXT_HOUR, SPRING_OPEN + 3_600),
    ]
    assert warnings(caplog) == [
        f"bar: time {SKIPPED} is in the hour the broker's clock skips, read at the offset before "
        "the change",
        f"bar: time {AFTER_SKIPPED} opens in the true-UTC period of the bar before it and is "
        "dropped",
    ]


async def test_a_bar_after_the_skipped_hour_no_bar_was_stamped_in_is_pushed(caplog):
    caplog.set_level(logging.WARNING)

    pushed = await _pushed_h1(AFTER_SKIPPED, NEXT_HOUR)

    assert pushed == [
        pushed_spring_bar(AFTER_SKIPPED, SPRING_OPEN),
        pushed_spring_bar(NEXT_HOUR, SPRING_OPEN + 3_600),
    ]
    assert warnings(caplog) == []


# ── Bounds ────────────────────────────────────────────────────────────────────


async def test_a_consumer_whose_queue_overflows_is_closed_with_4008_and_the_rest_serve_on():
    hub = Hub(CLOCK, settings(consumer_queue_bound=3))
    stuck, reading, ea = Peer(stuck=True), Peer(), Peer()
    tasks = [asyncio.create_task(hub.handler(peer)) for peer in (stuck, reading, ea)]
    for peer in (stuck, reading):
        peer.put_frame(binary.AdapterHello())
        peer.put_frame(binary.Subscribe(1, ticks_of("EURUSD")))
    ea.put_ea(binary.EaHello(PUBLISHER, "EURUSD"))
    await settle()

    # The stuck consumer's ack is in the send that never completes; three ticks fill its queue.
    for _ in range(3):
        ea.put_ea(binary.Ticks("EURUSD", (tick(),)))
    await settle()
    full = stuck.closed
    ea.put_ea(binary.Ticks("EURUSD", (tick(),)))
    await settle()

    assert full is None
    assert stuck.closed == (ws_server.OVERFLOW, "queue overflow")
    ea.put_ea(binary.Ticks("EURUSD", (tick(),)))
    await settle()
    assert len(reading.sent) == 1 + 5
    assert reading.closed is None
    for peer in (reading, ea):
        await peer.close()
    await asyncio.wait_for(asyncio.gather(*tasks), timeout=1)


# ── Refusals ──────────────────────────────────────────────────────────────────


async def _refused(port, hello: bytes | None, sent: bytes | str) -> binary.Refusal:
    ws = await websockets.connect(f"ws://127.0.0.1:{port}")
    if hello is not None:
        await ws.send(hello)
        await asyncio.sleep(0.05)
        while True:
            try:
                await asyncio.wait_for(ws.recv(), 0.05)
            except TimeoutError:
                break
    await ws.send(sent)
    refusal = binary.decode(await asyncio.wait_for(ws.recv(), 1), binary.Sender.HUB)
    await asyncio.wait_for(ws.wait_closed(), 1)
    assert ws.close_code == 1002
    return refusal


EA_HELLO = encode_ea(binary.EaHello(PUBLISHER, "EURUSD"))
ADAPTER_HELLO = binary.encode(binary.AdapterHello())


@pytest.mark.parametrize(
    ("hello", "sent"),
    [
        (None, '{"v": 1, "type": "hello", "role": "adapter"}'),
        (None, frame(FrameKind.HELLO, b"", version=3)),
        (None, binary.encode(binary.Subscribe(1, ticks_of("EURUSD")))),
        (ADAPTER_HELLO, ADAPTER_HELLO),
        (EA_HELLO, EA_HELLO),
        (EA_HELLO, encode_ea(sample("EURUSD"))),
        (EA_HELLO, binary.encode(binary.Subscribe(1, ticks_of("EURUSD")))),
        (ADAPTER_HELLO, encode_ea(binary.Ticks("EURUSD", (tick(),)))),
        (EA_HELLO, encode_ea(binary.RatesBar("EURUSD", mirror.TIMEFRAME_MN1, rates()))),
        (ADAPTER_HELLO, frame(FrameKind.ACK, b"", flags=1)),
    ],
    ids=[
        "text-frame",
        "version-3",
        "op-before-hello",
        "second-adapter-hello",
        "second-ea-hello",
        "server-time-from-a-publisher",
        "op-from-an-ea",
        "tick-from-a-consumer",
        "bar-of-no-series",
        "ack-from-a-consumer",
    ],
)
async def test_a_frame_the_hub_cannot_handle_is_refused_and_closes_its_connection(
    hello, sent, caplog
):
    caplog.set_level(logging.WARNING, logger="mt5connector.server.ws_server")
    hub, server, port = await run_hub()

    refusal = await _refused(port, hello, sent)

    assert isinstance(refusal, binary.Refusal) and refusal.message
    (warning,) = warnings(caplog)
    assert warning.endswith(f" on a frame the hub cannot handle: {refusal.message}")
    server.close()


async def test_a_frame_of_an_unknown_kind_is_logged_and_skipped(caplog):
    caplog.set_level(logging.INFO, logger="mt5connector.server.ws_server")
    hub, server, port = await run_hub()
    consumer = await Consumer.connect(port)

    await consumer.ws.send(frame(99, b""))
    await consumer.subscribed(1, ticks_of("EURUSD"))

    assert any("unknown kind 99" in record.getMessage() for record in caplog.records)
    await close_all(server, consumer)


async def test_a_malformed_frame_closes_its_connection_and_the_hub_serves_on():
    hub, server, port = await run_hub()
    broken = await publisher(port, "GBPUSD")
    eurusd = await publisher(port)
    consumer = await Consumer.connect(port)
    await consumer.subscribed(1, ticks_of("EURUSD"))

    await broken.ws.send(b"\x02\x0d")
    await asyncio.wait_for(broken.ws.wait_closed(), 1)
    await eurusd.send(binary.Ticks("EURUSD", (tick(),)))

    assert isinstance(await consumer.received(), binary.Ticks)
    await close_all(server, consumer, eurusd)


# ── Relays ────────────────────────────────────────────────────────────────────


async def test_the_spawners_server_time_is_never_fanned_out_and_one_for_another_symbol_dropped(
    caplog,
):
    caplog.set_level(logging.WARNING, logger="mt5connector.server.ws_server")
    hub, server, port = await run_hub()
    spawner = await FakeEa.connect(port, SPAWNER, "EURUSD.a")
    consumer = await Consumer.connect(port)
    await consumer.subscribed(1, ticks_of("EURUSD.a"))
    measured = binary.ServerTime("EURUSD.a", 1_752_580_800, 1_752_580_798, 1_752_570_000, True)

    await spawner.send(measured)
    await spawner.send(binary.ServerTime("GBPUSD", 1, 1, 1, True))
    await nothing(consumer.ws)

    assert warnings(caplog) == [
        "dropping a server-time sample for 'GBPUSD' from the spawner for EURUSD.a"
    ]
    await close_all(server, consumer, spawner)
