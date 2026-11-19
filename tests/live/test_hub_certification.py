"""Certification on demo hubs and explicitly selected throwaway servers; never a unit-tier run."""

import asyncio
import os
import re
import time
from http import HTTPStatus
from urllib.error import URLError

import pytest
from certification import (
    ProbeCommand,
    ProbeEvent,
    RelayOperation,
    Throwaway,
    health,
    listed_charts,
    observed_pending,
    selected_symbols,
    until,
)
from test_hub_live import (
    ANSWERED_S,
    DAY_S,
    HUB_URL,
    MAGIC,
    SYMBOL,
    Owner,
    Stamped,
    ask,
    assert_flat,
    connected,
    verdict_of,
)

from mt5connector.client.errors import NoPublisher, NotSent, NotServed, ResponseLost
from mt5connector.client.execution import SymbolFilling
from mt5connector.wire import binary, calls, mirror
from mt5connector.wire.push_wire import ChartRole, Stream, Subscription

pytestmark = pytest.mark.skipif(SYMBOL is None, reason="MT5_LIVE_HUB_SYMBOL is not set")
Call = calls.Call


@pytest.fixture
async def live():
    owner = Stamped()
    session = await connected(owner)
    try:
        yield session, owner
    finally:
        await session.disconnect()


@pytest.fixture
async def throwaway():
    if not os.environ.get("MT5_CERT_CONTAINER"):
        pytest.skip("throwaway certification controls are not selected")
    control = Throwaway()
    await control.observations()
    try:
        yield control
    finally:
        await control.restore_network()


async def symbols_for(session, count, control=None):
    listing = (await ask(session, Call.SYMBOLS_GET)).result
    excluded = {SYMBOL}
    if control is not None:
        rows = await control.observations()
        excluded |= set(selected_symbols(rows)) | set(listed_charts(rows).values())
    symbols = [
        row.name
        for row in listing
        if row.name not in excluded and row.trade_mode == mirror.SYMBOL_TRADE_MODE_FULL
    ]
    assert len(symbols) >= count, f"certification needs {count} unused fully tradable symbols"
    return symbols[:count]


async def subscribe(session, symbols):
    await asyncio.gather(
        *(session.subscribe(Subscription(Stream.TICKS, symbol)) for symbol in symbols)
    )
    await asyncio.gather(
        *(
            asyncio.wait_for(ask(session, Call.SYMBOL_INFO_TICK, symbol=symbol), 5)
            for symbol in symbols
        )
    )


async def unsubscribe(session, symbols):
    await asyncio.gather(
        *(session.unsubscribe(Subscription(Stream.TICKS, symbol)) for symbol in symbols)
    )


async def chart_opened(session, symbol, seconds):
    await asyncio.wait_for(ask(session, Call.SYMBOL_INFO_TICK, symbol=symbol), seconds)


def closed_lines(log, symbols):
    return [
        line
        for line in log.splitlines()
        if any(f"closed the publisher chart of '{symbol}'" in line for symbol in symbols)
    ]


async def no_hidden_removal(control, since, before):
    rows = await control.observations()
    hidden = {
        row.symbol
        for row in rows
        if row.event in (ProbeEvent.WATCH_SELECTED, ProbeEvent.WATCH_HELD)
        and row.selected
        and not row.visible
    }
    log = await control.logs(since)
    removed = set(re.findall(r"took '([^']+)' out of Market Watch", log))
    assert not hidden & removed
    assert not any(row.event is ProbeEvent.WATCH_GONE and not row.visible for row in rows[before:])


async def test_account_load_keeps_health_and_two_publishers_responsive(live):
    observer, _ = live
    await ask(observer, Call.SYMBOL_INFO_TICK, symbol=SYMBOL)
    candidates = await symbols_for(observer, 5)
    fresh = None
    owners = [Owner() for _ in range(10)]
    sessions = await asyncio.gather(*(connected(owner) for owner in owners))
    now = int(time.time())
    started = asyncio.Event()
    finished = asyncio.Event()
    statuses = []

    async def account_reads(session):
        started.set()
        return await asyncio.gather(
            ask(session, Call.HISTORY_DEALS_GET, date_from=now - 30 * DAY_S, date_to=now),
            ask(session, Call.SYMBOLS_GET),
        )

    async def sample_health():
        while not finished.is_set():
            status, body = await health(HUB_URL)
            statuses.append(status)
            assert status is HTTPStatus.OK and body["result"]["account"] is True, body
            try:
                await asyncio.wait_for(finished.wait(), 1)
            except TimeoutError:
                pass

    sampling = asyncio.create_task(sample_health())
    reads = asyncio.gather(*(account_reads(session) for session in sessions))
    try:
        await started.wait()
        await asyncio.wait_for(
            observer.request(Call.SYMBOL_INFO_TICK, symbol=SYMBOL), calls.READ_DEADLINE_S
        )
        beginning = time.monotonic()
        for symbol in candidates:
            try:
                await observer.request(Call.SYMBOL_INFO_TICK, symbol=symbol)
            except NoPublisher:
                fresh = symbol
                break
        assert fresh is not None, "the load case needs a genuinely fresh publisher"
        await observer.subscribe(Subscription(Stream.TICKS, fresh))
        await chart_opened(observer, fresh, 5)
        assert time.monotonic() - beginning <= 5
        answers = await reads
        assert len(answers) == 10 and all(len(pair) == 2 for pair in answers)
    finally:
        finished.set()
        await sampling
        if not reads.done():
            reads.cancel()
        await asyncio.gather(reads, return_exceptions=True)
        if fresh is not None:
            await observer.unsubscribe(Subscription(Stream.TICKS, fresh))
        await asyncio.gather(*(session.disconnect() for session in sessions))
    assert statuses and all(status is HTTPStatus.OK for status in statuses)


async def test_symbol_account_calls_have_publisher_pending_labels(live):
    session, _ = live
    await ask(session, Call.SYMBOL_INFO_TICK, symbol=SYMBOL)
    answers, roles = await observed_pending(
        HUB_URL,
        [
            session.request(call, symbol=SYMBOL)
            for _ in range(16)
            for call in (Call.POSITIONS_GET, Call.SYMBOL_MARGIN_RATE)
        ],
    )
    assert len(answers) == 32
    assert ChartRole.PUBLISHER in roles
    assert ChartRole.SPAWNER not in roles and ChartRole.ACCOUNT not in roles


async def test_a_closed_publishers_command_is_refused_in_one_round_trip(live):
    session, _ = live
    candidates = await symbols_for(session, 5)
    for symbol in candidates:
        request = {
            "action": mirror.TRADE_ACTION_PENDING,
            "symbol": symbol,
            "volume": 0.01,
            "type": mirror.ORDER_TYPE_BUY_LIMIT,
            "price": 0.00001,
            "type_time": mirror.ORDER_TIME_SPECIFIED,
            "expiration": int(time.time()) - DAY_S,
            "type_filling": mirror.ORDER_FILLING_RETURN,
            "magic": MAGIC,
        }
        started = time.monotonic()
        try:
            await session.order_send(request)
        except NotServed:
            assert time.monotonic() - started < calls.READ_DEADLINE_S
            await chart_opened(session, symbol, 5)
            answered = await session.order_send(request)
            assert answered.result.retcode > 0
            await assert_flat(session)
            return
    pytest.fail("no candidate had a closed publisher")


@pytest.mark.parametrize("stray", [False, True], ids=["fresh", "same-symbol-stray"])
async def test_fifteen_closes_do_not_delay_a_fresh_open(live, throwaway, stray):
    session, _ = live
    symbols = await symbols_for(session, 16, throwaway)
    old, fresh = symbols[:15], symbols[15]
    await subscribe(session, old)
    before = len(await throwaway.observations())
    since = time.time()
    await throwaway.relay(RelayOperation.PAUSE)
    try:
        await unsubscribe(session, old)
        await until(
            throwaway.relay,
            lambda state: len(
                [
                    event
                    for event in state["queued"]
                    if binary.FrameKind[event["kind"]] is binary.FrameKind.CLOSE_CHART
                    and event["symbol"] in old
                ]
            )
            >= 15,
            throwaway.idle_seconds + 5,
            "fifteen queued publisher closes",
        )
        if stray:
            stray_chart = await throwaway.command(ProbeCommand.OPEN_STRAY, fresh)
        began = time.monotonic()
        await session.subscribe(Subscription(Stream.TICKS, fresh))
        await until(
            throwaway.relay,
            lambda state: any(
                binary.FrameKind[event["kind"]] is binary.FrameKind.OPEN_CHART
                and event["symbol"] == fresh
                for event in state["queued"]
            ),
            1,
            "the new open queued after the closes",
        )
        if stray:
            assert stray_chart.chart in listed_charts(await throwaway.observations())
        await throwaway.relay(RelayOperation.RELEASE)
        await chart_opened(session, fresh, 2)
        assert time.monotonic() - began <= 2
        log = await until(
            lambda: throwaway.logs(since),
            lambda log: len(closed_lines(log, old)) == 15,
            throwaway.idle_seconds + 15,
            "all fifteen publisher closes",
        )
        lines = log.splitlines()
        opened = next(
            index
            for index, line in enumerate(lines)
            if f"opened the publisher chart of '{fresh}'" in line
        )
        second_close = [
            index
            for index, line in enumerate(lines)
            if any(f"closed the publisher chart of '{symbol}'" in line for symbol in old)
        ][1]
        assert opened < second_close
        if stray:
            observations = (await throwaway.observations())[before:]
            created = next(
                row
                for row in observations
                if row.event is ProbeEvent.CHART_ADDED and row.symbol == fresh
            )
            assert any(
                row.event is ProbeEvent.CHART_GONE and row.chart == created.chart
                for row in observations
            )
        await no_hidden_removal(throwaway, since, before)
    finally:
        await throwaway.relay(RelayOperation.RELEASE)
        await unsubscribe(session, [*old, fresh])


async def test_a_resyncs_own_teardown_does_not_hold_another_symbols_open(live, throwaway):
    session, _ = live
    fresh = (await symbols_for(session, 1, throwaway))[0]
    since = time.time()
    await throwaway.relay(RelayOperation.PAUSE)
    try:
        await until(
            throwaway.relay,
            lambda state: any(
                binary.FrameKind[event["kind"]] is binary.FrameKind.CLOSE_CHART
                and ChartRole[event["role"]] is ChartRole.HISTORY
                and event["symbol"] == throwaway.resync_symbol
                for event in state["queued"]
            ),
            ANSWERED_S,
            "the prepared symbol's re-sync close",
        )
        log = await throwaway.logs(since)
        assert any(
            "re-sync" in line and throwaway.resync_symbol in line for line in log.splitlines()
        ), "the queued history close must belong to a re-sync"
        await throwaway.relay(RelayOperation.RELEASE)
        charts = listed_charts(await throwaway.observations())
        assert throwaway.resync_symbol in charts.values(), "the own close is no longer listed"
        began = time.monotonic()
        await session.subscribe(Subscription(Stream.TICKS, fresh))
        await chart_opened(session, fresh, 2)
        assert time.monotonic() - began <= 2
    finally:
        await throwaway.relay(RelayOperation.RELEASE)
        await unsubscribe(session, [fresh])


async def test_market_watch_follows_chart_disappearance_within_100ms(live, throwaway):
    session, _ = live
    symbol = (await symbols_for(session, 1, throwaway))[0]
    await subscribe(session, [symbol])
    since = time.time()
    before = len(await throwaway.observations())
    await unsubscribe(session, [symbol])
    rows = await until(
        throwaway.observations,
        lambda rows: any(
            row.event is ProbeEvent.WATCH_GONE and row.symbol == symbol for row in rows[before:]
        ),
        throwaway.idle_seconds + 10,
        "the unused symbol's Market Watch removal",
    )
    gone = next(
        row for row in rows[before:] if row.event is ProbeEvent.CHART_GONE and row.symbol == symbol
    )
    removed = next(
        row for row in rows[before:] if row.event is ProbeEvent.WATCH_GONE and row.symbol == symbol
    )
    assert 0 <= removed.microseconds - gone.microseconds <= 100_000
    log = await throwaway.logs(since)
    assert f"took '{symbol}' out of Market Watch" in log
    await no_hidden_removal(throwaway, since, before)


async def test_hidden_conversion_symbols_stay_selected_through_a_cursor_round(live, throwaway):
    session, _ = live
    before = await throwaway.observations()
    hidden = {symbol for symbol, row in selected_symbols(before).items() if not row.visible}
    assert hidden, "certification needs the terminal's hidden conversion symbols"
    since = time.time()
    await ask(session, Call.SYMBOLS_GET)
    await asyncio.sleep(3)
    after = await throwaway.observations()
    assert {symbol for symbol, row in selected_symbols(after).items() if not row.visible} == hidden
    await no_hidden_removal(throwaway, since, len(before))


async def test_a_held_symbol_is_reported_once_and_kept(live, throwaway):
    session, owner = live
    account = (await ask(session, Call.ACCOUNT_INFO)).result
    if not account.trade_allowed:
        pytest.skip("the held-position certification case requires the demo half")
    symbol = (await symbols_for(session, 1, throwaway))[0]
    info = (await ask(session, Call.SYMBOL_INFO, symbol=symbol)).result
    tick = (await ask(session, Call.SYMBOL_INFO_TICK, symbol=symbol)).result
    await session.subscribe(Subscription(Stream.TRADE_TRANSACTIONS))
    filling = (
        mirror.ORDER_FILLING_FOK
        if SymbolFilling(info.filling_mode) & SymbolFilling.FOK
        else mirror.ORDER_FILLING_IOC
    )
    order = {
        "action": mirror.TRADE_ACTION_DEAL,
        "symbol": symbol,
        "type": mirror.ORDER_TYPE_BUY,
        "volume": info.volume_min,
        "price": tick.ask,
        "type_filling": filling,
        "magic": MAGIC,
    }
    await throwaway.relay(RelayOperation.PAUSE)
    try:
        await throwaway.command(ProbeCommand.CLOSE_SYMBOL, symbol)

        async def publisher_absent():
            try:
                await session.request(Call.SYMBOL_INFO_TICK, symbol=symbol)
            except NoPublisher:
                return True
            return False

        await until(publisher_absent, bool, 5, "the closed publisher")
        request_id = session._next_request_id
        began = time.monotonic()
        with pytest.raises(NotServed):
            await session.order_send(order)
        assert session._next_request_id == request_id + 1
        assert time.monotonic() - began < calls.READ_DEADLINE_S
    finally:
        await throwaway.relay(RelayOperation.RELEASE)
    await chart_opened(session, symbol, 5)
    since = time.time()
    try:
        sent = await session.order_send(order)
        verdict = await owner.first(verdict_of(sent.result.request_id), 30)
        assert verdict.result["retcode"] in (
            mirror.TRADE_RETCODE_DONE,
            mirror.TRADE_RETCODE_DONE_PARTIAL,
        )
        await throwaway.command(ProbeCommand.CLOSE_SYMBOL, symbol)
        await until(
            lambda: throwaway.logs(since),
            lambda log: f"'{symbol}' stays in Market Watch:" in log,
            throwaway.idle_seconds + 10,
            "the held-symbol report",
        )
        await asyncio.sleep(3)
        log = await throwaway.logs(since)
        assert log.count(f"'{symbol}' stays in Market Watch:") == 1
        assert f"took '{symbol}' out of Market Watch" not in log
        assert symbol in selected_symbols(await throwaway.observations())
    finally:
        positions = (await ask(session, Call.POSITIONS_GET, symbol=symbol)).result
        for position in positions:
            if position.magic == MAGIC:
                quote = (await ask(session, Call.SYMBOL_INFO_TICK, symbol=symbol)).result
                closed = await session.order_send(
                    {
                        "action": mirror.TRADE_ACTION_DEAL,
                        "symbol": symbol,
                        "position": position.ticket,
                        "type": mirror.ORDER_TYPE_SELL,
                        "volume": position.volume,
                        "price": quote.bid,
                        "type_filling": filling,
                        "magic": MAGIC,
                    }
                )
                await owner.first(verdict_of(closed.result.request_id), 30)

        async def remaining():
            return [
                position
                for position in (await ask(session, Call.POSITIONS_GET, symbol=symbol)).result
                if position.magic == MAGIC
            ]

        await until(remaining, lambda rows: not rows, 30, "the certification position's close")


async def test_reconnect_readd_is_removed_in_one_cursor_round(live, throwaway):
    session, _ = live
    symbol = (await symbols_for(session, 1, throwaway))[0]
    seeded = await throwaway.command(ProbeCommand.SELECT_SYMBOL, symbol)
    await until(
        throwaway.observations,
        lambda rows: any(
            row.event is ProbeEvent.WATCH_GONE
            and row.symbol == symbol
            and row.microseconds >= seeded.microseconds
            for row in rows
        ),
        3,
        "the prepared symbol's initial deselection",
    )
    before = len(await throwaway.observations())
    since = time.time()
    await throwaway.cut_network()
    try:
        await asyncio.sleep(20)
    finally:
        await throwaway.restore_network()
    await until(
        lambda: throwaway.logs(since),
        lambda log: "Broker synchronized" in log,
        240,
        "the terminal's broker synchronization",
    )
    readded = await until(
        throwaway.observations,
        lambda rows: any(
            row.event is ProbeEvent.WATCH_SELECTED and row.symbol == symbol and row.visible
            for row in rows[before:]
        ),
        5,
        "the broker's actual visible re-add",
    )
    selected = next(
        row
        for row in readded[before:]
        if row.event is ProbeEvent.WATCH_SELECTED and row.symbol == symbol and row.visible
    )
    rows = await until(
        throwaway.observations,
        lambda rows: any(
            row.event is ProbeEvent.WATCH_GONE
            and row.symbol == symbol
            and row.microseconds >= selected.microseconds
            for row in rows
        ),
        3,
        "the visibly re-added symbol's removal",
    )
    gone = next(
        row
        for row in rows
        if row.event is ProbeEvent.WATCH_GONE
        and row.symbol == symbol
        and row.microseconds >= selected.microseconds
    )
    assert gone.microseconds - selected.microseconds <= 3_000_000
    await no_hidden_removal(throwaway, since, before)


async def test_read_only_ceiling_45_pairs_15_idle_and_10_fresh(live, throwaway):
    session, _ = live
    if os.environ.get("MT5_CERT_CEILING") != "1":
        pytest.skip("the once-per-certification ceiling run is not selected")
    account = (await ask(session, Call.ACCOUNT_INFO)).result
    assert account.trade_allowed is False, "the ceiling run requires the read-only account"
    names = await symbols_for(session, 55, throwaway)
    old, fresh = names[:45], names[45:]
    idle, retained = old[:15], old[15:]
    running = True
    holding = {}

    async def hold_history(symbol):
        while running:
            await ask(session, Call.RANGES, symbol=symbol, source=calls.Source.TERMINAL)
            await asyncio.sleep(throwaway.idle_seconds / 3)

    since = time.time()
    try:
        await subscribe(session, old)
        holding = {symbol: asyncio.create_task(hold_history(symbol)) for symbol in old}

        def all_charts(rows):
            for task in holding.values():
                if task.done():
                    task.result()
            return all(list(listed_charts(rows).values()).count(symbol) >= 2 for symbol in old)

        await until(
            throwaway.observations,
            all_charts,
            1200,
            "45 publisher and 45 history charts",
        )
        await throwaway.relay(RelayOperation.PAUSE)
        for symbol in idle:
            holding[symbol].cancel()
        await asyncio.gather(*(holding[symbol] for symbol in idle), return_exceptions=True)
        await unsubscribe(session, idle)
        await until(
            throwaway.relay,
            lambda state: len(
                [
                    event
                    for event in state["queued"]
                    if binary.FrameKind[event["kind"]] is binary.FrameKind.CLOSE_CHART
                    and event["symbol"] in idle
                ]
            )
            >= 15,
            throwaway.idle_seconds + 10,
            "the ceiling run's queued closes",
        )
        await asyncio.gather(
            *(session.subscribe(Subscription(Stream.TICKS, symbol)) for symbol in fresh)
        )
        await until(
            throwaway.relay,
            lambda state: {
                event["symbol"]
                for event in state["queued"]
                if binary.FrameKind[event["kind"]] is binary.FrameKind.OPEN_CHART
            }
            >= set(fresh),
            2,
            "the ceiling run's ten queued opens",
        )
        await throwaway.relay(RelayOperation.RELEASE)
        await asyncio.gather(*(chart_opened(session, symbol, 15) for symbol in fresh))
        log = await throwaway.logs(since)
        assert all(f"opened the publisher chart of '{symbol}'" in log for symbol in fresh)
        relayed = (await throwaway.relay())["events"]
        for symbol in fresh:
            assert any(
                binary.FrameKind[event["kind"]] is binary.FrameKind.CHART_OPENED
                and event["symbol"] == symbol
                for event in relayed
            )
            assert not any(
                binary.FrameKind[event["kind"]] is binary.FrameKind.CHART_FAILED
                and event["symbol"] == symbol
                for event in relayed
            )
        assert len(retained) == 30
    finally:
        await throwaway.relay(RelayOperation.RELEASE)
        running = False
        for task in holding.values():
            task.cancel()
        await asyncio.gather(*holding.values(), return_exceptions=True)
        await unsubscribe(session, [*old, *fresh])


async def test_restart_loses_an_inflight_read_refuses_outage_and_replays_subscriptions(
    live, throwaway
):
    session, owner = live
    await session.subscribe(Subscription(Stream.TICKS, SYMBOL))
    await chart_opened(session, SYMBOL, 5)
    await ask(session, Call.RANGES, symbol=SYMBOL, source=calls.Source.TERMINAL)
    now = int(time.time())
    read = asyncio.create_task(
        session.request(
            Call.COPY_TICKS_RANGE,
            symbol=SYMBOL,
            date_from=now - 30 * DAY_S,
            date_to=now,
            flags=mirror.COPY_TICKS_INFO,
            source=calls.Source.TERMINAL,
        )
    )
    try:
        await until(
            lambda: health(HUB_URL),
            lambda result: any(
                ChartRole[row["role"].upper()] is ChartRole.HISTORY and row["symbol"] == SYMBOL
                for row in result[1]["result"]["pending"]
            ),
            5,
            "the history read in flight",
        )
        await throwaway.run("kill", "--signal", "KILL", throwaway.container)
        with pytest.raises(ResponseLost):
            await asyncio.wait_for(read, 5)
        began = time.monotonic()
        with pytest.raises(NotSent):
            await asyncio.wait_for(session.request(Call.ACCOUNT_INFO), 0.05)
        assert time.monotonic() - began < 0.05
    finally:
        await throwaway.start()
        if not read.done():
            read.cancel()
            await asyncio.gather(read, return_exceptions=True)

    async def restarted_health():
        try:
            return await health(HUB_URL)
        except URLError:
            return None, None

    await until(
        restarted_health,
        lambda result: result[0] is HTTPStatus.OK,
        360,
        "the restarted hub's verified clock",
        interval=1,
    )
    owner.frames.clear()
    owner.handed.clear()
    await owner.first(lambda frame: isinstance(frame, binary.Ticks) and frame.symbol == SYMBOL, 60)
    assert (await ask(session, Call.ACCOUNT_INFO)).result.currency
