"""Role scheduling and capacity on an explicitly owned throwaway terminal."""

import asyncio
import time
from contextlib import asynccontextmanager
from http import HTTPStatus

import pytest
from certification import ProbeCommand, container_log_time, health, until
from test_hub_certification import live as live
from test_hub_certification import subscribe, symbols_for, unsubscribe
from test_hub_certification import throwaway as throwaway
from test_hub_live import (
    DAY_S,
    HUB_URL,
    SYMBOL,
    Log,
    Stamped,
    ask,
    config,
    refused_order,
    verdict_of,
)

from mt5connector.client.errors import EaError, NotServed
from mt5connector.client.hub import HubSession
from mt5connector.server.ws_server import _CLOSE_BOUND_S, _FAILED_HOLD_RETRIES, _IDLE_SWEEP_S
from mt5connector.wire import binary, calls, mirror
from mt5connector.wire.push_wire import Stream, Subscription

pytestmark = pytest.mark.skipif(SYMBOL is None, reason="MT5_LIVE_HUB_SYMBOL is not set")

POLL_S = 0.016
STARVATION_S = 5 * 60
CHART_SETUP_S = 1200


class ObservedSession(HubSession):
    """A real session with a test-side record of every received wire frame."""

    def __init__(self):
        self.received = Stamped()
        super().__init__(
            config(HUB_URL), asyncio.get_running_loop(), self.received.on_frame, None, Log()
        )

    def _on_message(self, raw):
        frame = binary.decode(raw, binary.Sender.HUB)
        if isinstance(frame, binary.Answer | binary.Busy):
            self.received.on_frame(frame)
        super()._on_message(raw)


@pytest.fixture
async def observed():
    session = ObservedSession()
    await session.connect()
    try:
        yield session
    finally:
        await session.disconnect()


@asynccontextmanager
async def loaded_charts(session, control, pairs, extra_publisher=False):
    account = (await ask(session, calls.Call.ACCOUNT_INFO)).result
    assert not account.trade_allowed, "chart-ceiling scheduling requires the read-only account"
    symbols = await symbols_for(session, pairs + 11, control)
    held = symbols[:pairs]
    publishers = held + (symbols[pairs : pairs + 1] if extra_publisher else [])
    tasks = {}

    async def history(symbol):
        while True:
            await session.deferring(calls.Call.RANGES, symbol=symbol, source=calls.Source.TERMINAL)
            await asyncio.sleep(control.idle_seconds / 3)

    try:
        await subscribe(session, publishers)
        tasks = {symbol: asyncio.create_task(history(symbol)) for symbol in held}

        async def charts():
            for task in tasks.values():
                if task.done():
                    task.result()
            return await health(HUB_URL)

        expected = 2 + 2 * pairs + int(extra_publisher)
        await until(
            charts,
            lambda result: result[0] is HTTPStatus.OK
            and result[1]["result"]["chart_count"] == expected,
            CHART_SETUP_S,
            f"the owned terminal's {expected} listed charts",
            interval=1,
        )
        yield held, symbols[pairs + int(extra_publisher) :], tasks
    finally:
        for task in tasks.values():
            task.cancel()
        await asyncio.gather(*tasks.values(), return_exceptions=True)
        await unsubscribe(session, publishers)


async def test_99_charts_and_ten_fresh_subscriptions_keep_the_spawner_alive(live, throwaway):
    session, _ = live
    async with loaded_charts(session, throwaway, 48, extra_publisher=True) as (
        old,
        candidates,
        holders,
    ):
        since = time.time()
        for symbol in old[:15]:
            holders[symbol].cancel()
        await asyncio.gather(*(holders[symbol] for symbol in old[:15]), return_exceptions=True)
        await unsubscribe(session, old[:15])
        fresh = candidates[:10]
        try:
            await asyncio.gather(
                *(session.subscribe(Subscription(Stream.TICKS, symbol)) for symbol in fresh)
            )
            deadline = time.monotonic() + STARVATION_S
            while time.monotonic() < deadline:
                status, body = await health(HUB_URL)
                assert status is HTTPStatus.OK and body["result"]["spawner"], body
                await asyncio.sleep(1)
            log = await throwaway.logs(since)
            assert "pong is overdue" not in log, log
        finally:
            await unsubscribe(session, fresh)


async def test_capacity_is_reported_without_losing_the_spawner(live, throwaway):
    session, _ = live
    async with loaded_charts(session, throwaway, 49) as (old, candidates, holders):
        fresh = candidates[0]
        since = time.time()
        retry_s = float(
            await throwaway.run(
                "exec",
                throwaway.container,
                "python3",
                "-c",
                "import os; from mt5connector.server.settings import read_hub_settings; "
                "print(read_hub_settings(os.environ).retry_after_s)",
            )
        )
        try:
            status, body = await health(HUB_URL)
            assert status is HTTPStatus.OK and body["result"]["chart_limit"], body
            await session.subscribe(Subscription(Stream.TICKS, fresh))
            log = await until(
                lambda: throwaway.logs(since),
                lambda text: f"cannot open '{fresh}': ChartOpen failed:" in text
                and "the terminal's chart limit is 100" in text
                and f"cannot open the publisher chart of {fresh}" in text,
                POLL_S + calls.READ_DEADLINE_S,
                "the terminal's capacity refusal and the hub's warning",
            )
            assert "spawner" in log
            failed_at = next(
                container_log_time(line)
                for line in log.splitlines()
                if f"cannot open the publisher chart of {fresh}" in line
            )
            hold_s = _FAILED_HOLD_RETRIES * retry_s
            assert time.time() < failed_at + hold_s, "the fixture missed the failure hold"
            with pytest.raises(EaError) as refused:
                await session.request(calls.Call.SYMBOL_INFO_TICK, symbol=fresh)
            assert refused.value.last_error != 0
            with pytest.raises(NotServed):
                await session.order_send(
                    {
                        "action": mirror.TRADE_ACTION_PENDING,
                        "symbol": fresh,
                        "volume": 0.01,
                        "type": mirror.ORDER_TYPE_BUY_LIMIT,
                        "price": 0.00001,
                    }
                )
            released = old[-1]
            holders[released].cancel()
            await asyncio.gather(holders[released], return_exceptions=True)
            await unsubscribe(session, [released])
            # Retained close bookkeeping + idle grace + the next sweep.
            close_s = _CLOSE_BOUND_S + throwaway.idle_seconds + _IDLE_SWEEP_S
            await until(
                lambda: health(HUB_URL),
                lambda result: result[1]["result"]["chart_count"] < 100,
                close_s,
                "a released chart's capacity",
            )
            await asyncio.sleep(max(0, failed_at + hold_s - time.time()))
            freed_at = time.time()
            recovery = asyncio.create_task(
                session.deferring(calls.Call.SYMBOL_INFO_TICK, symbol=fresh)
            )
            try:
                await until(
                    lambda: throwaway.logs(freed_at),
                    lambda text: f"the spawner opened the publisher chart of {fresh}" in text
                    or f"publisher EA for {fresh} connected:" in text,
                    hold_s,
                    "the held publisher's open after capacity is released",
                )
                await asyncio.wait_for(recovery, retry_s + calls.READ_DEADLINE_S)
            finally:
                recovery.cancel()
                await asyncio.gather(recovery, return_exceptions=True)
            status, body = await health(HUB_URL)
            assert status is HTTPStatus.OK and body["result"]["spawner"], body
            assert "pong is overdue" not in await throwaway.logs(since)
        finally:
            await unsubscribe(session, [fresh])


async def test_account_queue_streams_its_first_part_and_keeps_a_publisher_responsive(
    live, throwaway
):
    session, _ = live
    await subscribe(session, [SYMBOL])
    sessions = [ObservedSession() for _ in range(10)]
    await asyncio.gather(*(peer.connect() for peer in sessions))
    since = time.time()
    now = int(time.time())
    began = time.monotonic()
    tasks = [
        asyncio.create_task(peer.request(call, **arguments))
        for peer in sessions
        for call, arguments in (
            (
                calls.Call.HISTORY_DEALS_GET,
                {"date_from": now - 90 * DAY_S, "date_to": now},
            ),
            (calls.Call.SYMBOLS_GET, {}),
        )
    ]
    try:
        while not all(task.done() for task in tasks):
            await session.request(calls.Call.SYMBOL_INFO_TICK, symbol=SYMBOL)
        answers = await asyncio.gather(*tasks)
        assert len(answers) == 20
        first = min(
            at
            for peer in sessions
            for at, frame in zip(peer.received.handed, peer.received.frames, strict=True)
            if isinstance(frame, binary.Answer) and frame.error is None and frame.part == 0
        )
        assert first - began <= 0.1
        log = await throwaway.logs(since)
        assert "pong is overdue" not in log, log
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.gather(*(peer.disconnect() for peer in sessions))
        await unsubscribe(session, [SYMBOL])


@pytest.mark.parametrize("cold_sibling", [False, True], ids=["alone", "cold-sibling"])
@pytest.mark.parametrize("reads_ahead", [0, 50], ids=["no-reads-ahead", "fifty-reads"])
async def test_a_publishers_fifty_reads_do_not_defer_the_venue_verdict(
    live, observed, throwaway, cold_sibling, reads_ahead
):
    session, _ = live
    if not (await ask(observed, calls.Call.ACCOUNT_INFO)).result.trade_allowed:
        pytest.skip("venue-verdict scheduling requires the demo half")
    await subscribe(observed, [SYMBOL])
    info = (await ask(observed, calls.Call.SYMBOL_INFO, symbol=SYMBOL)).result
    cold = None
    sibling = None
    reads = []
    try:
        if cold_sibling:
            sibling = (await symbols_for(session, 1, throwaway))[0]
            cold = asyncio.create_task(observed.subscribe(Subscription(Stream.TICKS, sibling)))
        reads = [
            asyncio.create_task(observed.request(calls.Call.SYMBOL_INFO_TICK, symbol=SYMBOL))
            for _ in range(reads_ahead)
        ]
        await asyncio.sleep(0)
        began = time.monotonic()
        sent = await observed.order_send(refused_order(info, info.point))
        matches = verdict_of(sent.result.request_id)
        await observed.received.first(matches, POLL_S + 0.05)
        assert observed.received.when(matches) - began <= POLL_S + 0.05
        await asyncio.gather(*reads)
    finally:
        if cold is not None:
            await asyncio.gather(cold, return_exceptions=True)
        for task in reads:
            task.cancel()
        await asyncio.gather(*reads, return_exceptions=True)
        await unsubscribe(observed, [SYMBOL, *([] if sibling is None else [sibling])])


async def test_a_slow_pass_names_its_longest_step(live, throwaway):
    session, _ = live
    symbol = (await symbols_for(session, 1, throwaway))[0]
    since = time.time()
    history = asyncio.create_task(
        session.deferring(calls.Call.RANGES, symbol=symbol, source=calls.Source.TERMINAL)
    )
    try:
        await session.subscribe(Subscription(Stream.TICKS, symbol))
        log = await until(
            lambda: throwaway.logs(since),
            lambda text: "ticks: slow tick:" in text and "PASS_" in text,
            2 * calls.HISTORY_BUSY_S,
            "a cold chart/history pass's slow-step report",
            interval=1,
        )
        assert " ms (" in log
    finally:
        history.cancel()
        await asyncio.gather(history, return_exceptions=True)
        await unsubscribe(session, [symbol])


async def test_a_five_thousand_tick_burst_keeps_reads_ahead_of_the_remaining_pushes(
    observed, throwaway
):
    symbol = f"CB{time.monotonic_ns():x}"
    await throwaway.command(ProbeCommand.CREATE_TICKS, symbol)
    try:
        await subscribe(observed, [symbol])
        await throwaway.command(ProbeCommand.BURST_TICKS, symbol)
        await observed.received.first(
            lambda frame: isinstance(frame, binary.Ticks) and frame.symbol == symbol,
            calls.READ_DEADLINE_S,
        )
        began = time.monotonic()
        await observed.request(calls.Call.SYMBOL_INFO_TICK, symbol=symbol)
        assert time.monotonic() - began <= POLL_S + 0.05

        async def burst():
            return [
                frame
                for frame in observed.received.frames
                if isinstance(frame, binary.Ticks) and frame.symbol == symbol
            ]

        frames = await until(
            burst,
            lambda received: sum(len(frame.ticks) for frame in received) >= 5000,
            calls.READ_DEADLINE_S,
            "all ticks of the custom-symbol burst",
        )
        assert len(frames) >= 5
        assert all(len(frame.ticks) <= 1000 for frame in frames)
    finally:
        await unsubscribe(observed, [symbol])
