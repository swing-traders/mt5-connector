"""The hub's requests: each routed to the EA of its call's role and symbol, or answered by the hub,
under its deadline and behind the clock gate, with every epoch converted at the hub."""

import asyncio
import calendar
import logging
import time
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

import hub_doubles
import pytest
from ea_wire import FakeEa, answer, arguments_of, decode_to_ea, zeroed
from hub_doubles import (
    MAX_AGE_S,
    Consumer,
    Peer,
    eventually,
    health_of,
    run_hub,
    sample,
    settings,
    settle,
    verified,
)
from mirror_samples import BROKER_EPOCH, CLOCK, UTC_EPOCH

from mt5connector.client.config import MT5Config
from mt5connector.client.hub import HubSession
from mt5connector.server import hub_gate, ws_server
from mt5connector.server.wire import binary, calls, mirror
from mt5connector.server.wire.binary import ErrorKind
from mt5connector.server.wire.history_wire import TICK_TIMEFRAME, Series
from mt5connector.server.wire.push_wire import ChartRole, TransactionType

Call = calls.Call
# Mid-July 2025 the broker's clock runs three hours ahead of true UTC.
SUMMER_UTC = calendar.timegm((2025, 7, 15, 10, 0, 0))
SUMMER_BROKER = calendar.timegm((2025, 7, 15, 13, 0, 0))
HOUR = 3_600
# The arguments beside the symbol a history read of each call carries here.
HISTORY_ARGUMENTS = {
    Call.COPY_RATES_RANGE: {
        "timeframe": mirror.TIMEFRAME_M1,
        "date_from": UTC_EPOCH,
        "date_to": UTC_EPOCH + 60,
    },
    Call.COPY_TICKS_RANGE: {"date_from": UTC_EPOCH, "date_to": UTC_EPOCH + 60, "flags": 1},
    Call.RANGES: {},
}


def tick(epoch=BROKER_EPOCH, msc=123, bid=1.085):
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


def rates(epoch):
    return {
        "time": epoch,
        "open": 1.085,
        "high": 1.0852,
        "low": 1.0849,
        "close": 1.0851,
        "tick_volume": 42,
        "spread": 12,
        "real_volume": 0,
    }


async def publisher(port, symbol="EURUSD"):
    """A publisher of the symbol, its first wanted frame read."""
    ea = await FakeEa.connect(port, ChartRole.PUBLISHER, symbol)
    assert isinstance(await ea.received(), binary.Wanted)
    return ea


async def nothing(ws, timeout=0.3):
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(ws.recv(), timeout)


def warnings(caplog):
    return [record.getMessage() for record in caplog.records if record.levelno == logging.WARNING]


def deferred(kind: ErrorKind, retry_after_ms=5_000):
    return (kind, retry_after_ms)


def error_of(reply: binary.Answer):
    assert reply.last and reply.count == 0
    return (reply.error.kind, reply.error.retry_after_ms)


# ── The gate and /health ──────────────────────────────────────────────────────


async def test_before_a_verifying_sample_every_request_is_answered_not_ready_and_health_is_503():
    hub, server, port = await run_hub()
    consumer = await Consumer.connect(port)

    await consumer.ask(1, Call.ACCOUNT_INFO)
    await consumer.ask(2, Call.COMMISSION_SCHEDULE, symbol="EURUSD")
    first, second = await consumer.received(), await consumer.received()
    status, body = await health_of(port)

    assert [(first.request_id, error_of(first)), (second.request_id, error_of(second))] == [
        (1, deferred(ErrorKind.NOT_READY)),
        (2, deferred(ErrorKind.NOT_READY)),
    ]
    assert status == 503
    assert body["ok"] is False
    assert body["result"] | {"sample_age_s": None} == {
        "spawner": False,
        "clock": None,
        "sample_age_s": None,
        "bootstrap_missed": False,
        "in_flight": 0,
        "pending": [],
    }
    await consumer.close()
    server.close()


async def test_after_a_verifying_sample_a_request_reaches_its_ea_and_health_is_200_with_the_load():
    hub, server, port = await run_hub()
    async with verified(port) as spawner:
        consumer = await Consumer.connect(port)

        await consumer.ask(7, Call.ACCOUNT_INFO)
        request = await spawner.request()
        status, body = await health_of(port)

        assert arguments_of(request) == (Call.ACCOUNT_INFO, {})
        assert (status, body["ok"]) == (200, True)
        result = body["result"]
        assert (result["spawner"], result["in_flight"], result["bootstrap_missed"]) == (
            True,
            1,
            False,
        )
        assert result["pending"] == [{"role": "spawner", "symbol": "EURUSD", "requests": 1}]
        assert 0 <= result["sample_age_s"] < MAX_AGE_S
        assert result["clock"]["symbol"] == "EURUSD"
        assert abs(result["clock"]["skew_s"]) <= 1
        assert result["clock"]["offset_s"] == CLOCK.offset_at(int(time.time())).total_seconds()
        await consumer.close()
        await spawner.close()
    server.close()


async def test_a_gate_that_shuts_when_the_samples_stop_answers_not_ready_again():
    hub, server, port = await run_hub()
    async with verified(port) as spawner:
        consumer = await Consumer.connect(port)
    await asyncio.sleep(MAX_AGE_S * 1.5)

    await consumer.ask(1, Call.ACCOUNT_INFO)

    assert error_of(await consumer.received()) == deferred(ErrorKind.NOT_READY)
    assert (await health_of(port))[0] == 503
    await spawner.close()
    server.close()


async def test_a_missed_bootstrap_window_leaves_the_hub_serving_with_its_gate_shut(caplog):
    caplog.set_level(logging.CRITICAL, logger="mt5connector.server.hub_gate")
    hub, server, port = await run_hub(clock_bootstrap_seconds=0.05)
    hub.start()
    await asyncio.sleep(0.1)
    consumer = await Consumer.connect(port)

    await consumer.ask(1, Call.ACCOUNT_INFO)
    refused = await consumer.received()
    status, body = await health_of(port)

    assert error_of(refused) == deferred(ErrorKind.NOT_READY)
    assert (status, body["result"]["bootstrap_missed"]) == (503, True)
    assert [record.getMessage() for record in caplog.records] == [
        "broker clock: no fresh server-time sample in 0.05 s"
    ]
    await consumer.close()
    server.close()


async def test_health_answers_get_alone():
    hub, server, port = await run_hub()
    loop = asyncio.get_running_loop()

    def post():
        import urllib.error
        import urllib.request

        request = urllib.request.Request(f"http://127.0.0.1:{port}/health", data=b"", method="POST")
        with pytest.raises(urllib.error.HTTPError) as refused:
            urllib.request.urlopen(request, timeout=1)
        return refused.value.code

    assert await loop.run_in_executor(None, post) == 405
    server.close()


# ── Routing ───────────────────────────────────────────────────────────────────


async def test_a_request_no_ea_of_its_role_serves_is_deferred_at_once_and_its_chart_asked_once():
    hub, server, port = await run_hub()
    loop = asyncio.get_running_loop()
    async with verified(port) as spawner:
        consumer = await Consumer.connect(port)

        started = loop.time()
        await consumer.ask(1, Call.SYMBOL_INFO, symbol="XAUUSD")
        first = await consumer.received()
        answered_in = loop.time() - started
        await consumer.ask(2, Call.SYMBOL_INFO_TICK, symbol="XAUUSD")
        second = await consumer.received()
        opened = await spawner.received()

        assert answered_in < 0.010
        assert [(first.request_id, error_of(first)), (second.request_id, error_of(second))] == [
            (1, deferred(ErrorKind.NO_PUBLISHER)),
            (2, deferred(ErrorKind.NO_PUBLISHER)),
        ]
        assert opened == binary.OpenChart(ChartRole.PUBLISHER, "XAUUSD")
        await nothing(spawner.ws)
        await consumer.close()
        await spawner.close()
    server.close()


@pytest.mark.parametrize("call", list(HISTORY_ARGUMENTS), ids=lambda call: call.name.lower())
async def test_a_history_read_with_no_history_ea_asks_the_spawner_for_its_history_chart(call):
    hub, server, port = await run_hub()
    async with verified(port) as spawner:
        consumer = await Consumer.connect(port)

        await consumer.ask(1, call, symbol="EURUSD", **HISTORY_ARGUMENTS[call])
        refused = await consumer.received()

        assert error_of(refused) == deferred(ErrorKind.NO_PUBLISHER)
        assert await spawner.received() == binary.OpenChart(ChartRole.HISTORY, "EURUSD")
        await consumer.close()
        await spawner.close()
    server.close()


async def test_a_request_on_a_chart_that_failed_to_open_carries_the_spawners_error():
    hub, server, port = await run_hub()
    async with verified(port) as spawner:
        consumer = await Consumer.connect(port)
        await consumer.ask(1, Call.SYMBOL_INFO, symbol="NOSUCH")
        await consumer.received()
        assert await spawner.received() == binary.OpenChart(ChartRole.PUBLISHER, "NOSUCH")

        await spawner.send(binary.ChartFailed(ChartRole.PUBLISHER, "NOSUCH", 4301))
        await eventually(lambda: (ChartRole.PUBLISHER, "NOSUCH") in hub._failed)
        await consumer.ask(2, Call.SYMBOL_INFO, symbol="NOSUCH")
        await consumer.ask(3, Call.COMMISSION_SCHEDULE, symbol="NOSUCH")
        refused = [await consumer.received(), await consumer.received()]

        assert [
            (reply.request_id, error_of(reply), reply.last_error, reply.error.message)
            for reply in refused
        ] == [
            (
                2,
                (ErrorKind.EA_ERROR, None),
                4301,
                "the publisher chart of NOSUCH failed to open: MQL5 error 4301",
            ),
            (
                3,
                (ErrorKind.EA_ERROR, None),
                4301,
                "the publisher chart of NOSUCH failed to open: MQL5 error 4301",
            ),
        ]
        await consumer.close()
        await spawner.close()
    server.close()


async def test_an_account_read_with_no_spawner_is_deferred_and_asks_for_no_chart():
    hub, server, port = await run_hub()
    async with verified(port) as spawner:
        consumer = await Consumer.connect(port)
        await spawner.close()
        await eventually(lambda: hub._spawner is None)

        await consumer.ask(1, Call.POSITIONS_GET)
        refused = await consumer.received()

        assert error_of(refused) == deferred(ErrorKind.NO_PUBLISHER)
        await consumer.close()
    server.close()


async def test_two_requests_an_ea_answers_in_reverse_order_resolve_each_with_its_own_answer():
    hub, server, port = await run_hub()
    async with verified(port) as spawner:
        eurusd = await publisher(port)
        session = HubSession(
            MT5Config(
                account=12345678,
                server="Broker-Demo",
                ws_url=f"ws://127.0.0.1:{port}",
            ),
            asyncio.get_running_loop(),
            lambda frame: None,
            None,
            MagicMock(),
        )
        await session.connect()

        tick_read = asyncio.create_task(session.symbol_info_tick("EURUSD"))
        outcomes_read = asyncio.create_task(session.outcomes("EURUSD", (7,)))
        asked = {}
        for _ in range(2):
            request = await eurusd.request()
            asked[arguments_of(request)[0]] = request
        await eurusd.send(answer(asked[Call.OUTCOMES].request_id, Call.OUTCOMES, []))
        await eurusd.send(
            answer(asked[Call.SYMBOL_INFO_TICK].request_id, Call.SYMBOL_INFO_TICK, tick())
        )

        outcomes = await asyncio.wait_for(outcomes_read, 1)
        ticked = await asyncio.wait_for(tick_read, 1)
        assert (outcomes.result, outcomes.last_error) == ((), 0)
        assert ticked.result._asdict() == tick(UTC_EPOCH)
        await session.disconnect()
        await eurusd.close()
        await spawner.close()
    server.close()


async def test_an_ea_leaving_answers_each_request_pending_on_it_ea_gone_at_once():
    hub, server, port = await run_hub()
    loop = asyncio.get_running_loop()
    async with verified(port) as spawner:
        eurusd = await publisher(port)
        consumer = await Consumer.connect(port)
        await consumer.ask(1, Call.SYMBOL_INFO_TICK, symbol="EURUSD")
        await consumer.ask(2, Call.SYMBOL_INFO_TICK, symbol="EURUSD")
        await eurusd.request()
        await eurusd.request()

        started = loop.time()
        await eurusd.close()
        gone = [await consumer.received(timeout=0.1), await consumer.received(timeout=0.1)]

        assert loop.time() - started < 0.1
        assert sorted((reply.request_id, error_of(reply)) for reply in gone) == [
            (1, (ErrorKind.EA_GONE, None)),
            (2, (ErrorKind.EA_GONE, None)),
        ]
        await consumer.close()
        await spawner.close()
    server.close()


async def test_a_read_its_ea_never_answers_is_answered_timeout_at_the_read_deadline(
    caplog, monkeypatch
):
    caplog.set_level(logging.WARNING, logger="mt5connector.server.ws_server")
    monkeypatch.setattr(calls, "READ_DEADLINE_S", 0.2)
    hub, server, port = await run_hub()
    loop = asyncio.get_running_loop()
    async with verified(port) as spawner:
        eurusd = await publisher(port)
        consumer = await Consumer.connect(port)

        await consumer.ask(5, Call.SYMBOL_INFO_TICK, symbol="EURUSD")
        request = await eurusd.request()
        started = loop.time()
        timed = await consumer.received()
        waited = loop.time() - started
        await eurusd.send(answer(request.request_id, Call.SYMBOL_INFO_TICK, tick()))

        assert 0.19 <= waited < 0.3
        assert (timed.request_id, error_of(timed)) == (5, (ErrorKind.TIMEOUT, None))
        await nothing(consumer.ws)
        assert [
            record.levelno for record in caplog.records if "deadline" in record.getMessage()
        ] == [logging.ERROR]
        assert any(f"request {request.request_id}" in warning for warning in warnings(caplog))
        await consumer.close()
        await eurusd.close()
        await spawner.close()
    server.close()


@pytest.mark.parametrize("call", list(HISTORY_ARGUMENTS), ids=lambda call: call.name.lower())
async def test_a_history_read_its_ea_sends_no_busy_for_is_held_at_the_hub_for_the_history_bound(
    call,
):
    hub, server, port = await run_hub()
    async with verified(port) as spawner:
        eurusd = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
        consumer = await Consumer.connect(port)

        asked = time.monotonic()
        await consumer.ask(5, call, symbol="EURUSD", **HISTORY_ARGUMENTS[call])
        await eurusd.request()
        (pending,) = hub._requests.all()

        assert 900 <= pending.deadline - asked < 901
        await consumer.close()
        await eurusd.close()
        await spawner.close()
    server.close()


async def test_a_busy_holds_a_read_past_its_deadline_and_its_answer_within_the_bound_is_delivered(
    monkeypatch,
):
    monkeypatch.setattr(calls, "READ_DEADLINE_S", 0.2)
    hub, server, port = await run_hub()
    async with verified(port) as spawner:
        eurusd = await publisher(port)
        consumer = await Consumer.connect(port)

        await consumer.ask(3, Call.SYMBOL_INFO_TICK, symbol="EURUSD")
        request = await eurusd.request()
        await eurusd.send(binary.Busy(request.request_id, 1))
        told = await consumer.received()
        await asyncio.sleep(0.5)
        await eurusd.send(answer(request.request_id, Call.SYMBOL_INFO_TICK, tick()))
        answered = await consumer.received()

        assert told == binary.Busy(3, 1)
        assert (answered.request_id, answered.error, answered.last) == (3, None, True)
        await consumer.close()
        await eurusd.close()
        await spawner.close()
    server.close()


async def test_an_ea_whose_pongs_stop_is_dropped_and_its_requests_answered_ea_gone(monkeypatch):
    monkeypatch.setattr(ws_server, "PING_INTERVAL_S", 0.02)
    hub, peers, tasks, eurusd = await _asked_a_deaf_publisher(settings())
    loop = asyncio.get_running_loop()
    started = loop.time()

    while eurusd.closed is None:
        await asyncio.sleep(0.005)

    # Three intervals without a pong, and the interval before the ping that went unanswered.
    assert loop.time() - started < 0.2
    assert _answers(peers[-1]) == [(9, ErrorKind.EA_GONE)]
    await _end(peers, tasks)


async def test_a_busy_holds_off_the_keepalive_until_its_bound_and_margin_pass(monkeypatch):
    monkeypatch.setattr(ws_server, "PING_INTERVAL_S", 0.02)
    monkeypatch.setattr(calls, "BUSY_MARGIN_S", 0.2)
    hub, peers, tasks, eurusd = await _asked_a_deaf_publisher(settings())
    loop = asyncio.get_running_loop()
    eurusd.put_ea(binary.Busy(eurusd.asked, 1))
    started = loop.time()

    await asyncio.sleep(0.5)
    kept = eurusd.closed is None
    while eurusd.closed is None:
        await asyncio.sleep(0.005)

    assert kept
    assert 1.2 <= loop.time() - started < 1.5
    assert _answers(peers[-1]) == [(9, ErrorKind.TIMEOUT)]
    await _end(peers, tasks)


async def test_a_history_reads_busy_holds_its_request_and_its_eas_keepalive_for_its_whole_bound(
    monkeypatch,
):
    monkeypatch.setattr(ws_server, "PING_INTERVAL_S", 0.02)
    timeouts = []
    wait = asyncio.wait

    async def timed_wait(aws, *, timeout=None, return_when=asyncio.ALL_COMPLETED):
        timeouts.append(timeout)
        return await wait(aws, timeout=timeout, return_when=return_when)

    monkeypatch.setattr(asyncio, "wait", timed_wait)
    hub, peers, tasks, eurusd = await _asked_a_deaf_publisher(settings())
    told = time.monotonic()
    eurusd.put_ea(binary.Busy(eurusd.asked, calls.HISTORY_BUSY_S))

    await asyncio.sleep(0.5)
    (pending,) = hub._requests.all()
    bound = calls.HISTORY_BUSY_S + calls.BUSY_MARGIN_S

    assert eurusd.closed is None
    assert bound <= pending.deadline - told < bound + 1
    # The keepalive waits for the deaf EA's pong until the busy's bound and margin pass.
    assert bound - 1 < max(timeout for timeout in timeouts if timeout is not None) <= bound
    await _end(peers, tasks)


async def _asked_a_deaf_publisher(hub_settings):
    """A hub with a verified clock and a publisher asked one read, after which it answers no ping;
    its peers end with the consumer's."""
    hub = ws_server.Hub(CLOCK, hub_settings)
    spawner = Peer()
    eurusd = Peer()
    consumer = Peer()
    peers = [spawner, eurusd, consumer]
    tasks = [asyncio.create_task(hub.handler(peer)) for peer in peers]
    spawner.put_ea(binary.EaHello(ChartRole.SPAWNER, "EURUSD"))
    eurusd.put_ea(binary.EaHello(ChartRole.PUBLISHER, "EURUSD"))
    consumer.put_frame(binary.AdapterHello())
    for _ in range(8):
        spawner.put_ea(sample("EURUSD"))
        await asyncio.sleep(MAX_AGE_S / 4)
    count, body = calls.encode_arguments(calls.CALLS[Call.SYMBOL_INFO_TICK], {"symbol": "EURUSD"})
    consumer.put_frame(binary.Request(9, Call.SYMBOL_INFO_TICK, count, body))
    await settle()
    (asked,) = [decode_to_ea(raw) for raw in eurusd.sent if raw[1] == binary.FrameKind.REQUEST]
    eurusd.asked = asked.request_id
    eurusd.deaf = True
    return hub, peers, tasks, eurusd


def _answers(consumer: Peer) -> list:
    replies = [binary.decode(raw, binary.Sender.HUB) for raw in consumer.sent]
    return [
        (reply.request_id, reply.error.kind)
        for reply in replies
        if isinstance(reply, binary.Answer)
    ]


async def _end(peers, tasks) -> None:
    for peer in peers:
        await peer.close()
    await asyncio.wait_for(asyncio.gather(*tasks), timeout=2)


async def test_the_consumers_request_past_its_pending_bound_is_answered_overflow_at_once():
    hub, server, port = await run_hub(pending_bound=2)
    async with verified(port) as spawner:
        consumer = await Consumer.connect(port)

        for request_id in (1, 2, 3):
            await consumer.ask(request_id, Call.ACCOUNT_INFO)
        refused = await consumer.received()

        assert (refused.request_id, error_of(refused)) == (3, (ErrorKind.OVERFLOW, None))
        await consumer.close()
        await spawner.close()
    server.close()


async def test_a_request_its_eas_queue_has_no_room_for_is_answered_overflow():
    hub = ws_server.Hub(CLOCK, settings(ea_queue_bound=1))
    spawner = Peer(stuck=True)
    consumer = Peer()
    tasks = [asyncio.create_task(hub.handler(peer)) for peer in (spawner, consumer)]
    spawner.put_ea(binary.EaHello(ChartRole.SPAWNER, "EURUSD"))
    consumer.put_frame(binary.AdapterHello())
    for _ in range(8):
        spawner.put_ea(sample("EURUSD"))
        await asyncio.sleep(MAX_AGE_S / 4)

    # The first is taken into the send that never completes, the second fills the queue of one.
    consumer.put_frame(binary.Request(1, Call.ACCOUNT_INFO, 0, b""))
    await settle()
    consumer.put_frame(binary.Request(2, Call.ACCOUNT_INFO, 0, b""))
    consumer.put_frame(binary.Request(3, Call.ACCOUNT_INFO, 0, b""))
    await settle()

    assert _answers(consumer) == [(3, ErrorKind.OVERFLOW)]
    await consumer.close()
    await asyncio.wait_for(tasks[1], timeout=1)
    tasks[0].cancel()


# ── Answers ───────────────────────────────────────────────────────────────────


async def test_a_bar_window_reaches_the_ea_on_the_brokers_clock_and_its_rows_return_in_true_utc():
    hub, server, port = await run_hub()
    async with verified(port) as spawner:
        eurusd = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
        consumer = await Consumer.connect(port)

        await consumer.ask(
            1,
            Call.COPY_RATES_RANGE,
            symbol="EURUSD",
            timeframe=mirror.TIMEFRAME_M1,
            date_from=SUMMER_UTC,
            date_to=SUMMER_UTC + HOUR,
        )
        request = await eurusd.request()
        await eurusd.send(
            answer(request.request_id, Call.COPY_RATES_RANGE, [rates(SUMMER_BROKER + 1_800)])
        )
        reply = await consumer.received()
        bars = calls.read_hub_result(calls.CALLS[Call.COPY_RATES_RANGE], reply)

        assert arguments_of(request) == (
            Call.COPY_RATES_RANGE,
            {
                "symbol": "EURUSD",
                "timeframe": mirror.TIMEFRAME_M1,
                "date_from": SUMMER_BROKER,
                "date_to": SUMMER_BROKER + HOUR,
            },
        )
        assert (reply.request_id, reply.part, reply.last) == (1, 0, True)
        assert bars["time"].tolist() == [SUMMER_UTC + 1_800]
        assert bars["time_close"].tolist() == [SUMMER_UTC + 1_800 + 60]
        await consumer.close()
        await eurusd.close()
        await spawner.close()
    server.close()


async def test_an_answer_in_parts_reaches_the_consumer_part_by_part_in_true_utc():
    hub, server, port = await run_hub()
    async with verified(port) as spawner:
        eurusd = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
        consumer = await Consumer.connect(port)
        await consumer.ask(
            4,
            Call.COPY_TICKS_RANGE,
            symbol="EURUSD",
            date_from=UTC_EPOCH,
            date_to=UTC_EPOCH + 60,
            flags=1,
        )
        request = await eurusd.request()

        await eurusd.send(
            answer(request.request_id, Call.COPY_TICKS_RANGE, [tick(msc=1)], last=False)
        )
        await eurusd.send(
            answer(request.request_id, Call.COPY_TICKS_RANGE, [tick(msc=2), tick(msc=3)], part=1)
        )
        first, second = await consumer.received(), await consumer.received()
        row = calls.CALLS[Call.COPY_TICKS_RANGE]

        assert [(part.part, part.last, part.count) for part in (first, second)] == [
            (0, False, 1),
            (1, True, 2),
        ]
        times = [calls.read_hub_result(row, part)["time_msc"].tolist() for part in (first, second)]
        assert times == [[UTC_EPOCH * 1000 + 1], [UTC_EPOCH * 1000 + 2, UTC_EPOCH * 1000 + 3]]
        await consumer.close()
        await eurusd.close()
        await spawner.close()
    server.close()


async def test_an_eas_error_reaches_the_consumer_with_the_mql5_error_it_left():
    hub, server, port = await run_hub()
    async with verified(port) as spawner:
        eurusd = await publisher(port)
        consumer = await Consumer.connect(port)

        await consumer.ask(2, Call.SYMBOL_INFO, symbol="EURUSD")
        request = await eurusd.request()
        failure = binary.AnswerError(ErrorKind.EA_ERROR, "")
        await eurusd.send(binary.Answer(request.request_id, 0, True, 4301, error=failure))
        reply = await consumer.received()

        assert (reply.request_id, reply.last_error, error_of(reply)) == (
            2,
            4301,
            (ErrorKind.EA_ERROR, None),
        )
        await consumer.close()
        await eurusd.close()
        await spawner.close()
    server.close()


async def test_an_ea_answering_a_kind_the_hub_alone_answers_is_refused_and_its_requests_gone():
    hub, server, port = await run_hub()
    async with verified(port) as spawner:
        eurusd = await publisher(port)
        consumer = await Consumer.connect(port)

        await consumer.ask(2, Call.SYMBOL_INFO, symbol="EURUSD")
        request = await eurusd.request()
        failure = binary.AnswerError(ErrorKind.NOT_READY, "", 5_000)
        await eurusd.send(binary.Answer(request.request_id, 0, True, 0, error=failure))
        reply = await consumer.received()
        refusal = await eurusd.received()

        assert (reply.request_id, error_of(reply)) == (2, (ErrorKind.EA_GONE, None))
        assert isinstance(refusal, binary.Refusal)
        await asyncio.wait_for(eurusd.ws.wait_closed(), 1)
        assert eurusd.ws.close_code == 1002
        await consumer.close()
        await spawner.close()
    server.close()


async def test_a_tick_read_reaches_the_history_ea_on_its_broker_window_with_no_fact_after_it():
    hub, server, port = await run_hub()
    async with verified(port) as spawner:
        eurusd = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
        consumer = await Consumer.connect(port)

        await consumer.ask(
            1,
            Call.COPY_TICKS_RANGE,
            symbol="EURUSD",
            date_from=UTC_EPOCH,
            date_to=UTC_EPOCH + 60,
            flags=1,
        )
        request = await eurusd.request()
        await eurusd.send(answer(request.request_id, Call.COPY_TICKS_RANGE, [tick()]))
        reply = await consumer.received()
        ticks = calls.read_hub_result(calls.CALLS[Call.COPY_TICKS_RANGE], reply)

        assert arguments_of(request) == (
            Call.COPY_TICKS_RANGE,
            {
                "symbol": "EURUSD",
                "date_from": BROKER_EPOCH,
                "date_to": BROKER_EPOCH + 60,
                "flags": 1,
            },
        )
        assert (reply.request_id, reply.error, reply.last) == (1, None, True)
        assert ticks["time_msc"].tolist() == [UTC_EPOCH * 1000 + 123]
        await consumer.close()
        await eurusd.close()
        await spawner.close()
    server.close()


async def ranges_read(consumer, ea, request_id: int, floors: list[tuple[int, int]]):
    """A ranges read the history EA answers with a MaxBars of 100,000 and the floors, each a
    timeframe and its broker second: the request the EA received, and the ranges the consumer
    received."""
    await consumer.ask(request_id, Call.RANGES, symbol="EURUSD")
    request = await ea.request()
    await ea.send(answer(request.request_id, Call.RANGES, (100_000, floors)))
    reply = await consumer.received()
    return request, calls.read_hub_result(calls.CALLS[Call.RANGES], reply)


def floors_of(ranges: calls.Ranges) -> dict[Series, tuple[int, int]]:
    return {series: (entry.floor, entry.generation) for series, entry in ranges.series.items()}


async def test_a_ranges_answer_is_stored_and_reaches_the_consumer_in_true_utc_by_generation():
    hub, server, port = await run_hub()
    async with verified(port) as spawner:
        eurusd = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
        consumer = await Consumer.connect(port)
        h1 = (mirror.TIMEFRAME_H1, BROKER_EPOCH)
        ticks = (TICK_TIMEFRAME, BROKER_EPOCH + 30)
        before = int(time.time())

        request, first = await ranges_read(
            consumer, eurusd, 1, [h1, (mirror.TIMEFRAME_M1, BROKER_EPOCH + HOUR), ticks]
        )
        _, again = await ranges_read(
            consumer, eurusd, 2, [h1, (mirror.TIMEFRAME_M1, BROKER_EPOCH + HOUR), ticks]
        )
        _, moved = await ranges_read(
            consumer, eurusd, 3, [h1, (mirror.TIMEFRAME_M1, BROKER_EPOCH + 2 * HOUR), ticks]
        )

        assert arguments_of(request) == (Call.RANGES, {"symbol": "EURUSD"})
        assert first.maxbars == 100_000
        assert list(first.series) == [Series.M1, Series.H1, Series.TICKS]
        assert floors_of(first) == {
            Series.M1: (UTC_EPOCH + HOUR, 1),
            Series.H1: (UTC_EPOCH, 1),
            Series.TICKS: (UTC_EPOCH + 30, 1),
        }
        assert all(
            before - 1 <= entry.measured_at <= int(time.time()) for entry in first.series.values()
        )
        assert floors_of(again) == floors_of(first)
        assert floors_of(moved) == {
            Series.M1: (UTC_EPOCH + 2 * HOUR, 2),
            Series.H1: (UTC_EPOCH, 1),
            Series.TICKS: (UTC_EPOCH + 30, 1),
        }
        await consumer.close()
        await eurusd.close()
        await spawner.close()
    server.close()


async def test_a_ranges_answer_omitting_a_series_the_last_one_carried_no_longer_advertises_it():
    hub, server, port = await run_hub()
    async with verified(port) as spawner:
        eurusd = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
        consumer = await Consumer.connect(port)
        h1 = (mirror.TIMEFRAME_H1, BROKER_EPOCH)
        ticks = (TICK_TIMEFRAME, BROKER_EPOCH + 30)

        await ranges_read(consumer, eurusd, 1, [h1, (mirror.TIMEFRAME_M1, BROKER_EPOCH), ticks])
        _, emptied = await ranges_read(consumer, eurusd, 2, [h1, ticks])

        assert floors_of(emptied) == {
            Series.H1: (UTC_EPOCH, 1),
            Series.TICKS: (UTC_EPOCH + 30, 1),
        }
        await consumer.close()
        await eurusd.close()
        await spawner.close()
    server.close()


async def test_a_series_back_in_a_ranges_answer_after_one_omitted_it_steps_its_generation():
    hub, server, port = await run_hub()
    async with verified(port) as spawner:
        eurusd = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
        consumer = await Consumer.connect(port)
        h1 = (mirror.TIMEFRAME_H1, BROKER_EPOCH)
        m1 = (mirror.TIMEFRAME_M1, BROKER_EPOCH)

        await ranges_read(consumer, eurusd, 1, [h1, m1])
        await ranges_read(consumer, eurusd, 2, [h1])
        _, back = await ranges_read(consumer, eurusd, 3, [h1, m1])

        assert floors_of(back) == {Series.M1: (UTC_EPOCH, 2), Series.H1: (UTC_EPOCH, 1)}
        await consumer.close()
        await eurusd.close()
        await spawner.close()
    server.close()


@pytest.mark.parametrize("call", [Call.COPY_RATES_RANGE, Call.COPY_TICKS_RANGE])
async def test_a_window_starting_past_the_clocks_range_after_it_ends_is_refused_invalid(call):
    hub, server, port = await run_hub()
    async with verified(port) as spawner:
        consumer = await Consumer.connect(port)
        window = {"symbol": "EURUSD", "date_from": 10**12, "date_to": UTC_EPOCH}
        if call is Call.COPY_RATES_RANGE:
            await consumer.ask(1, call, timeframe=mirror.TIMEFRAME_M1, **window)
        else:
            await consumer.ask(1, call, flags=1, **window)
        reply = await consumer.received()

        assert (reply.request_id, error_of(reply)) == (1, (ErrorKind.INVALID_WINDOW, None))
        assert reply.error.message == "the window starts after it ends"
        await consumer.close()
        await spawner.close()
    server.close()


SCHEDULE = {
    "ret": 0,
    "last_error": 0,
    "rules": [
        {
            "currency": "USD",
            "mode_range": "SYMBOL_COMMISSION_RANGE_VOLUME",
            "mode_charge": "SYMBOL_COMMISSION_CHARGE_INSTANT",
            "mode_entry": "SYMBOL_COMMISSION_ENTRY_INOUT",
            "mode_direction": "SYMBOL_COMMISSION_DIRECTION_BOTH",
            "mode_profit": "SYMBOL_COMMISSION_PROFIT_ALL",
            "tiers": [
                {
                    "mode": "SYMBOL_COMMISSION_MONEY_DEPOSIT",
                    "volume_type": "SYMBOL_COMMISSION_VOLUME_TYPE_VOLUME",
                    "value": 3.5,
                    "min_value": 0.0,
                    "max_value": 0.0,
                    "range_from": 0.0,
                    "range_to": 0.0,
                    "currency": "USD",
                }
            ],
        }
    ],
}
WEEK = [[[64_800, 0]]] + [[[0, 61_140], [64_800, 0]]] * 4 + [[[0, 61_020]], []]
SESSIONS = {"quote": WEEK, "trade": WEEK, "last_error": 0}


async def test_a_commission_schedule_is_answered_from_the_publishers_latest_relay():
    hub, server, port = await run_hub()
    async with verified(port) as spawner:
        consumer = await Consumer.connect(port)
        row = calls.CALLS[Call.COMMISSION_SCHEDULE]

        await consumer.ask(1, Call.COMMISSION_SCHEDULE, symbol="EURUSD")
        unpublished = await consumer.received()
        opened = await spawner.received()
        eurusd = await publisher(port)
        await consumer.ask(2, Call.COMMISSION_SCHEDULE, symbol="EURUSD")
        unrelayed = await consumer.received()
        await eurusd.send(binary.Commissions("EURUSD", SCHEDULE))
        await settle()
        await consumer.ask(3, Call.COMMISSION_SCHEDULE, symbol="EURUSD")
        schedule = await consumer.received()
        await eurusd.send(binary.Commissions("GBPUSD", SCHEDULE))
        await settle()
        await consumer.ask(4, Call.COMMISSION_SCHEDULE, symbol="EURUSD")
        refused = await consumer.received()

        assert error_of(unpublished) == deferred(ErrorKind.NO_PUBLISHER)
        assert opened == binary.OpenChart(ChartRole.PUBLISHER, "EURUSD")
        assert error_of(unrelayed) == deferred(ErrorKind.SYNCING)
        assert (schedule.error, calls.read_hub_result(row, schedule)) == (None, SCHEDULE)
        assert error_of(refused) == (ErrorKind.EA_ERROR, None)
        assert refused.error.message == (
            "the commission_schedule relay for EURUSD was refused: "
            "symbol 'GBPUSD' is not the relay's 'EURUSD'"
        )
        await consumer.close()
        await eurusd.close()
        await spawner.close()
    server.close()


async def test_trading_sessions_are_answered_from_the_publishers_latest_relay():
    hub, server, port = await run_hub()
    async with verified(port) as spawner:
        eurusd = await publisher(port)
        consumer = await Consumer.connect(port)

        await eurusd.send(binary.Sessions("EURUSD", SESSIONS))
        await settle()
        await consumer.ask(1, Call.TRADING_SESSIONS, symbol="EURUSD")
        reply = await consumer.received()

        assert calls.read_hub_result(calls.CALLS[Call.TRADING_SESSIONS], reply) == SESSIONS
        await consumer.close()
        await eurusd.close()
        await spawner.close()
    server.close()


def _with_day(week, index, day):
    return week[:index] + [day] + week[index + 1 :]


@pytest.mark.parametrize(
    ("relayed", "reason"),
    [
        (
            binary.Sessions("GBPUSD", SESSIONS),
            "symbol 'GBPUSD' is not the relay's 'EURUSD'",
        ),
        (
            binary.Sessions(
                "EURUSD", SESSIONS | {"quote": _with_day(WEEK, 1, [[0, 61_140], [-1, 0]])}
            ),
            "quote[1][1] holds a negative second: [-1, 0]",
        ),
        (
            binary.Sessions(
                "EURUSD", SESSIONS | {"trade": _with_day(WEEK, 2, [[61_140, 0], [64_800, 3_600]])}
            ),
            "trade[2][1] does not end after it starts: [64800, 3600]",
        ),
        (
            binary.Sessions("EURUSD", SESSIONS | {"trade": _with_day(WEEK, 2, [[3_600, 3_600]])}),
            "trade[2][0] does not end after it starts: [3600, 3600]",
        ),
    ],
    ids=["another-symbol", "negative-from", "from-after-to", "from-at-to"],
)
async def test_refused_trading_sessions_answer_the_read_with_the_refusal_until_a_frame_is_accepted(
    relayed, reason
):
    hub, server, port = await run_hub()
    async with verified(port) as spawner:
        eurusd = await publisher(port)
        consumer = await Consumer.connect(port)

        await eurusd.send(relayed)
        await settle()
        await consumer.ask(1, Call.TRADING_SESSIONS, symbol="EURUSD")
        refused = await consumer.received()
        await eurusd.send(binary.Sessions("EURUSD", SESSIONS))
        await settle()
        await consumer.ask(2, Call.TRADING_SESSIONS, symbol="EURUSD")
        accepted = await consumer.received()

        assert error_of(refused) == (ErrorKind.EA_ERROR, None)
        assert refused.error.message == (
            f"the trading_sessions relay for EURUSD was refused: {reason}"
        )
        assert calls.read_hub_result(calls.CALLS[Call.TRADING_SESSIONS], accepted) == SESSIONS
        await consumer.close()
        await eurusd.close()
        await spawner.close()
    server.close()


async def test_a_session_ending_at_0_is_kept_whatever_its_start():
    quote = _with_day(WEEK, 0, [[0, 0], [86_399, 0]])
    hub, server, port = await run_hub()
    async with verified(port) as spawner:
        eurusd = await publisher(port)
        consumer = await Consumer.connect(port)

        await eurusd.send(binary.Sessions("EURUSD", SESSIONS | {"quote": quote}))
        await settle()
        await consumer.ask(1, Call.TRADING_SESSIONS, symbol="EURUSD")
        reply = await consumer.received()

        assert calls.read_hub_result(calls.CALLS[Call.TRADING_SESSIONS], reply)["quote"] == quote
        await consumer.close()
        await eurusd.close()
        await spawner.close()
    server.close()


async def test_an_order_reaches_its_symbols_publisher_with_its_expiration_on_the_brokers_clock():
    hub, server, port = await run_hub()
    async with verified(port) as spawner:
        eurusd = await publisher(port)
        consumer = await Consumer.connect(port)
        order = calls.trade_request(
            {"action": 5, "symbol": "EURUSD", "volume": 0.1, "price": 1.08, "type": 2}
            | {"type_time": 2, "expiration": UTC_EPOCH, "magic": 77, "comment": "nt-1"}
        )

        await consumer.ask(6, Call.ORDER_SEND, request=order)
        request = await eurusd.request()
        sent = zeroed(binary.TRADE_RESULT, retcode=mirror.TRADE_RETCODE_PLACED, request_id=11)
        await eurusd.send(answer(request.request_id, Call.ORDER_SEND, sent))
        reply = await consumer.received()

        assert arguments_of(request) == (
            Call.ORDER_SEND,
            {"request": order | {"expiration": BROKER_EPOCH}},
        )
        assert calls.read_hub_result(calls.CALLS[Call.ORDER_SEND], reply) == sent
        await consumer.close()
        await eurusd.close()
        await spawner.close()
    server.close()


async def test_outcomes_reach_the_consumer_as_the_transactions_held_with_epochs_in_true_utc():
    hub, server, port = await run_hub()
    async with verified(port) as spawner:
        eurusd = await publisher(port)
        consumer = await Consumer.connect(port)
        held = binary.TradeTransaction(
            zeroed(
                binary.TRADE_TRANSACTION,
                type=TransactionType.REQUEST,
                time_type=mirror.ORDER_TIME_SPECIFIED,
                time_expiration=BROKER_EPOCH,
            ),
            zeroed(
                binary.TRADE_REQUEST,
                symbol="EURUSD",
                type_time=mirror.ORDER_TIME_SPECIFIED,
                expiration=BROKER_EPOCH,
            ),
            zeroed(
                binary.TRADE_RESULT,
                retcode=mirror.TRADE_RETCODE_INVALID_EXPIRATION,
                request_id=11,
                comment="Invalid expiration",
            ),
        )

        await consumer.ask(6, Call.OUTCOMES, symbol="EURUSD", request_ids=(11, 12))
        request = await eurusd.request()
        await eurusd.send(answer(request.request_id, Call.OUTCOMES, [held]))
        reply = await consumer.received()

        assert arguments_of(request) == (
            Call.OUTCOMES,
            {"symbol": "EURUSD", "request_ids": (11, 12)},
        )
        assert calls.read_hub_result(calls.CALLS[Call.OUTCOMES], reply) == [
            binary.TradeTransaction(
                held.transaction | {"time_expiration": UTC_EPOCH},
                held.request | {"expiration": UTC_EPOCH},
                held.result,
            )
        ]
        await consumer.close()
        await eurusd.close()
        await spawner.close()
    server.close()


@pytest.mark.parametrize(
    ("request_frame", "message"),
    [
        (binary.Request(1, 99, 0, b""), "request 1: unknown call 99"),
        (binary.Request(1, Call.SYMBOL_INFO, 0, b""), "symbol_info: missing argument: symbol"),
    ],
    ids=["unknown-call", "missing-argument"],
)
async def test_a_request_the_hub_cannot_read_is_refused_and_closes_the_consumer(
    request_frame, message
):
    hub, server, port = await run_hub()
    consumer = await Consumer.connect(port)

    await consumer.send(request_frame)

    assert await consumer.received() == binary.Refusal(message)
    await asyncio.wait_for(consumer.ws.wait_closed(), 1)
    assert consumer.ws.close_code == 1002
    server.close()


async def test_a_bar_read_of_a_timeframe_no_series_serves_is_refused():
    hub, server, port = await run_hub()
    consumer = await Consumer.connect(port)

    await consumer.ask(
        1,
        Call.COPY_RATES_RANGE,
        symbol="EURUSD",
        timeframe=mirror.TIMEFRAME_MN1,
        date_from=UTC_EPOCH,
        date_to=UTC_EPOCH + 3_600,
    )

    assert await consumer.received() == binary.Refusal(
        f"timeframe {mirror.TIMEFRAME_MN1} has no history series"
    )
    server.close()


async def test_an_answer_to_a_request_whose_consumer_left_is_dropped_without_a_warning(caplog):
    caplog.set_level(logging.WARNING, logger="mt5connector.server.ws_server")
    hub, server, port = await run_hub()
    async with verified(port) as spawner:
        eurusd = await publisher(port)
        consumer = await Consumer.connect(port)
        await consumer.ask(1, Call.SYMBOL_INFO_TICK, symbol="EURUSD")
        request = await eurusd.request()
        await consumer.close()
        await eventually(lambda: not hub._consumers)

        await eurusd.send(answer(request.request_id, Call.SYMBOL_INFO_TICK, tick()))
        await settle()

        assert warnings(caplog) == []
        await eurusd.close()
        await spawner.close()
    server.close()


async def test_a_last_part_the_hub_cannot_convert_still_ends_its_request_with_ea_gone():
    hub, server, port = await run_hub()
    async with verified(port) as spawner:
        eurusd = await publisher(port)
        consumer = await Consumer.connect(port)
        await consumer.ask(8, Call.SYMBOL_INFO_TICK, symbol="EURUSD")
        request = await eurusd.request()

        # A well-formed tick whose time no calendar holds fails its conversion to true UTC.
        await eurusd.send(answer(request.request_id, Call.SYMBOL_INFO_TICK, tick(2**50)))
        reply = await consumer.received()

        assert (reply.request_id, error_of(reply)) == (8, (ErrorKind.EA_GONE, None))
        await consumer.close()
        await spawner.close()
    server.close()


async def test_while_the_gate_is_shut_a_request_at_the_pending_bound_is_answered_not_ready():
    hub, server, port = await run_hub(pending_bound=1)
    async with verified(port) as spawner:
        consumer = await Consumer.connect(port)
        await consumer.ask(1, Call.ACCOUNT_INFO)
        await spawner.request()
    await asyncio.sleep(MAX_AGE_S * 1.5)

    await consumer.ask(2, Call.ACCOUNT_INFO)

    assert error_of(await consumer.received()) == deferred(ErrorKind.NOT_READY)
    await consumer.close()
    await spawner.close()
    server.close()


# New York's 01:00–02:00 on 2025-11-02 happens twice; the broker's clock shows 08:00–09:00 twice.
AUTUMN = calendar.timegm((2025, 11, 2, 0, 0, 0))
# The broker's clock skips 09:00–10:00 on 2026-03-08, though the terminal stamps records in it.
SPRING = calendar.timegm((2026, 3, 8, 0, 0, 0))


async def _window_read(port, call, date_from, date_to, rows):
    """The arguments a range read of true-UTC [date_from, date_to] reaches the EA of its call's role
    with, and the rows its answer carrying `rows` reaches the consumer with."""
    async with verified(port) as spawner:
        eurusd = await FakeEa.connect(port, calls.CALLS[call].role, "EURUSD")
        consumer = await Consumer.connect(port)
        if call is Call.COPY_RATES_RANGE:
            await consumer.ask(
                1,
                call,
                symbol="EURUSD",
                timeframe=mirror.TIMEFRAME_M1,
                date_from=date_from,
                date_to=date_to,
            )
        else:
            await consumer.ask(
                1, call, symbol="EURUSD", date_from=date_from, date_to=date_to, flags=1
            )
        request = await eurusd.request()
        await eurusd.send(answer(request.request_id, call, rows))
        reply = await consumer.received()
        await consumer.close()
        await eurusd.close()
        await spawner.close()
    return arguments_of(request)[1], calls.read_hub_result(calls.CALLS[call], reply)


async def test_a_window_about_the_repeated_hour_is_read_across_every_broker_stamp_reading_into_it():
    hub, server, port = await run_hub()
    # 05:45–06:15Z holds broker 08:45–08:59:59 at the hour's first pass and 08:00–08:15 at its
    # second.
    start = AUTUMN + 5 * HOUR + 45 * 60
    broker_hour = AUTUMN + 8 * HOUR
    stamps = [broker_hour - 10 * 60, broker_hour + 10 * 60, broker_hour + 50 * 60]
    stamps.append(broker_hour + HOUR + 10 * 60)

    arguments, bars = await _window_read(
        port, Call.COPY_RATES_RANGE, start, start + 30 * 60, [rates(stamp) for stamp in stamps]
    )

    assert arguments["date_from"] <= broker_hour
    assert arguments["date_to"] >= broker_hour + HOUR - 1
    # Broker 08:10 and 08:50 read into the window, each at its first occurrence's true UTC.
    assert bars["time"].tolist() == [AUTUMN + 5 * HOUR + 10 * 60, AUTUMN + 5 * HOUR + 50 * 60]
    server.close()


async def test_a_window_about_the_skipped_hour_holds_the_records_stamped_in_it():
    hub, server, port = await run_hub()
    # 07:00–07:10Z holds broker 09:00–09:10, the skipped hour read at the offset before the change,
    # and broker 10:00–10:10.
    start = SPRING + 7 * HOUR
    stamps = [SPRING + 9 * HOUR + 5 * 60, SPRING + 9 * HOUR + 30 * 60, SPRING + 10 * HOUR + 5 * 60]

    arguments, ticks = await _window_read(
        port, Call.COPY_TICKS_RANGE, start, start + 10 * 60, [tick(stamp) for stamp in stamps]
    )

    assert arguments["date_from"] <= SPRING + 9 * HOUR
    assert arguments["date_to"] >= SPRING + 10 * HOUR + 10 * 60
    assert ticks["time"].tolist() == [start + 5 * 60, start + 5 * 60]
    server.close()


@pytest.fixture
def terminal_at(monkeypatch):
    """Sets the true-UTC instant the hub's clock and the spawner's samples read."""

    def at(utc_epoch: int) -> None:
        wall = SimpleNamespace(time=lambda: float(utc_epoch), monotonic=time.monotonic)
        monkeypatch.setattr(hub_gate, "time", wall)
        monkeypatch.setattr(hub_doubles, "time", wall)

    return at


def _iso(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, UTC).isoformat(timespec="seconds")


# On 2026-03-08 at 07:30Z the terminal reads broker 10:30; a window about the change is read from
# the broker bounds of the offset before it, so its broker start is not after the terminal's time.
@pytest.mark.parametrize("call", [Call.COPY_RATES_RANGE, Call.COPY_TICKS_RANGE])
@pytest.mark.parametrize(
    ("terminal", "date_from", "date_to", "message"),
    [
        (
            SPRING + 7 * HOUR + 30 * 60,
            SPRING + 8 * HOUR,
            SPRING + 8 * HOUR + 60,
            f"the window starts at {_iso(SPRING + 8 * HOUR)}, after the terminal's time "
            f"{_iso(SPRING + 7 * HOUR + 30 * 60)}",
        ),
        (
            SPRING + 9 * HOUR + 30 * 60,
            SPRING + 8 * HOUR + 60,
            SPRING + 8 * HOUR,
            "the window starts after it ends",
        ),
    ],
    ids=["future", "reversed"],
)
async def test_a_window_invalid_in_true_utc_about_a_clock_change_is_refused_by_the_hub(
    terminal_at, call, terminal, date_from, date_to, message
):
    terminal_at(terminal)
    hub, server, port = await run_hub()
    async with verified(port) as spawner:
        ea = await FakeEa.connect(port, calls.CALLS[call].role, "EURUSD")
        consumer = await Consumer.connect(port)
        if call is Call.COPY_RATES_RANGE:
            await consumer.ask(
                1,
                call,
                symbol="EURUSD",
                timeframe=mirror.TIMEFRAME_M1,
                date_from=date_from,
                date_to=date_to,
            )
        else:
            await consumer.ask(
                1, call, symbol="EURUSD", date_from=date_from, date_to=date_to, flags=1
            )
        reply = await consumer.received()

        assert (reply.request_id, error_of(reply)) == (1, (ErrorKind.INVALID_WINDOW, None))
        assert reply.error.message == message
        with pytest.raises(asyncio.TimeoutError):
            await ea.request(timeout=0.3)
        await consumer.close()
        await ea.close()
        await spawner.close()
    server.close()


async def test_a_window_before_the_terminals_time_on_a_constant_offset_day_reaches_its_ea(
    terminal_at,
):
    # On 2026-03-10 the broker's clock runs three hours ahead of true UTC all day.
    day = SPRING + 2 * 86_400
    terminal_at(day + 7 * HOUR + 30 * 60)
    hub, server, port = await run_hub()

    arguments, _ = await _window_read(
        port, Call.COPY_TICKS_RANGE, day + 7 * HOUR, day + 7 * HOUR + 60, []
    )

    assert (arguments["date_from"], arguments["date_to"]) == (
        day + 10 * HOUR,
        day + 10 * HOUR + 60,
    )
    server.close()


@pytest.mark.parametrize("call", [Call.COPY_RATES_RANGE, Call.COPY_TICKS_RANGE])
async def test_a_range_reads_zero_bound_reaches_the_ea_as_zero(call):
    hub, server, port = await run_hub()

    arguments, _ = await _window_read(port, call, 0, 86_400, [])

    # 1970-01-02 00:00Z read on the broker's EST-based clock of that winter is broker 02:00.
    assert arguments["date_from"] == 0
    assert arguments["date_to"] >= 86_400 + 2 * HOUR
    server.close()
