"""Scheduled downloads observed through the hub's entry point, peers and history volume."""

import asyncio
import json
import logging
import socket
import threading
import time
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from types import SimpleNamespace

import numpy as np
import pytest
from ea_wire import FakeEa, answer, arguments_of
from hub_doubles import Consumer, eventually, health, verified
from mirror_samples import CLOCK

from mt5connector.server import ws_server
from mt5connector.server.hub_resync import Cause, Resync, TickStore
from mt5connector.server.wire import binary, calls, mirror
from mt5connector.server.wire.history_wire import TICK_TIMEFRAME
from mt5connector.server.wire.push_wire import ChartRole

SWEEP_BOUND_S = 8


def epoch(text):
    return int(datetime.fromisoformat(text).replace(tzinfo=UTC).timestamp())


FIRST_TICK = epoch("2026-11-01T00:00:00")


@pytest.fixture
def clock(monkeypatch):
    now = [epoch("2026-11-10T01:30:00"), 1_000.0]
    monkeypatch.setattr(
        ws_server, "time", SimpleNamespace(time=lambda: now[0], monotonic=lambda: now[1])
    )
    return now


@pytest.fixture
def run_server(monkeypatch):
    @asynccontextmanager
    async def run(*, history_store, chart_idle_seconds=900, resync_reopen_seconds=600):
        with socket.socket() as reserved:
            reserved.bind(("127.0.0.1", 0))
            port = reserved.getsockname()[1]
        tasks = []

        class Runner:
            def run(self, coroutine):
                tasks.append(asyncio.create_task(coroutine))

            def __getattr__(self, name):
                return getattr(asyncio, name)

        def listening():
            try:
                return health(port)[0] == 503
            except OSError:
                return False

        with monkeypatch.context() as startup:
            run.closed = set()
            original_drop = ws_server.Hub._drop

            def drop(hub, ws):
                original_drop(hub, ws)
                run.closed.add(ws.remote_address)

            startup.setattr(ws_server.Hub, "_drop", drop)
            startup.setenv("MT5_HUB_PORT", str(port))
            startup.setenv("MT5_HISTORY_STORE", str(history_store))
            startup.setenv("MT5_CHART_IDLE_SECONDS", str(chart_idle_seconds))
            startup.setenv("MT5_RESYNC_REOPEN_SECONDS", str(resync_reopen_seconds))
            startup.setenv("MT5_CLOCK_SAMPLE_MAX_AGE_SECONDS", "1")
            startup.setattr(ws_server, "_IDLE_SWEEP_S", 0.05)
            startup.setattr(ws_server, "asyncio", Runner())
            ws_server.main()
            try:
                await eventually(listening, timeout=2)
                yield port
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), 2)

    return run


def month_file(store, month="202611", *, symbol="EURUSD"):
    path = store / "broker" / "ticks" / symbol / f"{month}.tkc"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"recorded live")
    return path


def record(store, months, *, outcome="downloaded", month="", reason="", symbols=("EURUSD",)):
    path = store / ".mt5-connector" / "resync.json"
    path.parent.mkdir(exist_ok=True)
    path.write_text(
        json.dumps(
            {
                symbol: {"months": months, "outcome": outcome, "month": month, "reason": reason}
                for symbol in symbols
            }
        )
    )


def stored(store):
    return json.loads((store / ".mt5-connector" / "resync.json").read_text())["EURUSD"]


@pytest.fixture
def first_months_listed(monkeypatch):
    listed_files = asyncio.Event()
    listing = TickStore.listing

    async def observed_listing(store):
        result = await listing(store)
        if {"202610", "202611"} <= result.get("EURUSD", set()):
            listed_files.set()
        return result

    monkeypatch.setattr(TickStore, "listing", observed_listing)
    return listed_files


async def no_chart_message(spawner):
    with pytest.raises(TimeoutError):
        await spawner.received(timeout=SWEEP_BOUND_S)


async def requests_finished(port):
    await eventually(lambda: health(port)[1]["result"]["in_flight"] == 0)


async def chart_closed(ea, run_server):
    address = ea.ws.local_address
    await ea.close()
    await eventually(lambda: address in run_server.closed)


async def measured(ea, *, first_tick=FIRST_TICK, symbol="EURUSD"):
    request = await ea.request(timeout=SWEEP_BOUND_S)
    assert arguments_of(request) == (calls.Call.RANGES, {"symbol": symbol})
    await ea.send(
        answer(
            request.request_id,
            calls.Call.RANGES,
            (100_000, [(TICK_TIMEFRAME, CLOCK.to_broker(first_tick))], CLOCK.to_broker(first_tick)),
        )
    )


async def verify_month(ea, *, empty=False, failure=False):
    request = await ea.request()
    call, arguments = arguments_of(request)
    assert call is calls.Call.COPY_RATES_RANGE
    assert arguments["timeframe"] == mirror.TIMEFRAME_M1
    # Broker Nov 9 23:59 is the final minute of the ended broker days on Nov 10.
    opened = epoch("2026-11-09T23:59:00")
    bars = np.array(
        [] if empty else [(opened, 1.0, 1.0, 1.0, 1.0, 1, 1, 0)],
        dtype=np.dtype(list(mirror.RATES.dtype)),
    )
    await ea.send(answer(request.request_id, call, bars))
    if not empty:
        request = await ea.request()
        call, arguments = arguments_of(request)
        assert call is calls.Call.COPY_TICKS_RANGE
        assert arguments["date_from"] == opened
        assert arguments["date_to"] == opened + 59
        assert arguments["flags"] == mirror.COPY_TICKS_INFO
        if failure:
            await ea.send(
                binary.Answer(
                    request.request_id,
                    0,
                    True,
                    0,
                    error=binary.AnswerError(
                        binary.ErrorKind.EA_ERROR,
                        "EURUSD ticks: minute broker 1794268740 holds a bar and no tick",
                    ),
                )
            )
        else:
            ticks = np.array(
                [(opened, 1.0, 1.1, 0.0, 0, opened * 1000, mirror.TICK_FLAG_BID, 0.0)],
                dtype=np.dtype(list(mirror.TICKS.dtype)),
            )
            await ea.send(answer(request.request_id, call, ticks))


async def listed(port, ea, *, first_tick=FIRST_TICK):
    consumer = await Consumer.connect(port)
    try:
        await consumer.ask(1, calls.Call.RANGES, symbol="EURUSD")
        await measured(ea, first_tick=first_tick)
        return calls.read_hub_result(calls.CALLS[calls.Call.RANGES], await consumer.received())
    finally:
        await consumer.close()


async def crash_after_an_unrecorded_month_is_deleted(store, clock, run_server):
    october = month_file(store, "202610")
    month_file(store)
    record(store, {"202611": clock[0]})
    async with run_server(history_store=store) as port, verified(port) as spawner:
        await eventually(lambda: not october.exists(), timeout=SWEEP_BOUND_S)
        clock[1] += 600
        assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.OpenChart(
            ChartRole.HISTORY, "EURUSD"
        )
        assert not october.exists()
        assert stored(store)["months"] == {"202611": clock[0]}


async def test_a_forgotten_deleted_month_holds_back_its_days_and_every_later_day(
    tmp_path, clock, run_server
):
    await crash_after_an_unrecorded_month_is_deleted(tmp_path, clock, run_server)
    async with run_server(history_store=tmp_path) as port, verified(port):
        ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
        try:
            ranges = await listed(port, ea, first_tick=epoch("2026-10-01T00:00:00"))
            assert ranges.claimable_through == epoch("2026-10-01T00:00:00")
            assert ranges.resync_failure == ""
        finally:
            await ea.close()


async def test_the_next_due_pass_records_a_forgotten_month_restored_by_the_full_sync(
    tmp_path, clock, run_server
):
    await crash_after_an_unrecorded_month_is_deleted(tmp_path, clock, run_server)
    async with run_server(history_store=tmp_path) as port, verified(port) as spawner:
        await no_chart_message(spawner)
        clock[0] = epoch("2026-11-10T23:00:00")
        await eventually(lambda: not (tmp_path / "broker/ticks/EURUSD/202611.tkc").exists())
        clock[1] += 600
        assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.OpenChart(
            ChartRole.HISTORY, "EURUSD"
        )
        ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
        try:
            month_file(tmp_path, "202610").write_bytes(b"downloaded")
            month_file(tmp_path).write_bytes(b"downloaded")
            await measured(ea, first_tick=epoch("2026-10-01T00:00:00"))
            await verify_month(ea)
            await eventually(lambda: stored(tmp_path)["months"].get("202610") == clock[0])
            assert stored(tmp_path)["months"] == {"202610": clock[0], "202611": clock[0]}
            ranges = await listed(port, ea, first_tick=epoch("2026-10-01T00:00:00"))
            assert ranges.claimable_through == epoch("2026-11-10T00:00:00")
        finally:
            await ea.close()


async def test_start_deletes_downloads_verifies_and_claims_only_ended_broker_blocks(
    tmp_path, clock, run_server
):
    path = month_file(tmp_path)
    async with run_server(history_store=tmp_path) as port, verified(port) as spawner:
        await eventually(lambda: not path.exists(), timeout=SWEEP_BOUND_S)
        clock[1] += 600
        assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.OpenChart(
            ChartRole.HISTORY, "EURUSD"
        )
        assert not path.exists()
        ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
        try:
            path.write_bytes(b"downloaded")
            await measured(ea)
            await verify_month(ea)
            await eventually(lambda: (tmp_path / ".mt5-connector" / "resync.json").exists())
            assert stored(tmp_path)["months"] == {"202611": clock[0]}
            ranges = await listed(port, ea)
            assert ranges.first_tick == epoch("2026-11-01T00:00:00")
            assert ranges.claimable_through == epoch("2026-11-09T00:00:00")
            assert ranges.resync_failure == ""
        finally:
            await ea.close()


@pytest.mark.parametrize("december", [False, True])
async def test_a_utc_day_at_the_month_boundary_needs_both_broker_months(
    tmp_path, clock, december, run_server
):
    month_file(tmp_path)
    if december:
        month_file(tmp_path, "202612")
    clock[0] = epoch("2026-12-02T00:30:00")
    months = {"202611": clock[0]}
    if december:
        months["202612"] = clock[0]
    record(tmp_path, months)
    async with run_server(history_store=tmp_path) as port, verified(port):
        ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
        try:
            ranges = await listed(port, ea)
            expected = "2026-12-01T00:00:00" if december else "2026-11-30T00:00:00"
            assert ranges.claimable_through == epoch(expected)
        finally:
            await ea.close()


async def test_due_months_include_the_previous_month_until_its_last_day_is_verified(
    tmp_path, clock, run_server
):
    october = month_file(tmp_path, "202610")
    november = month_file(tmp_path)
    december = month_file(tmp_path, "202612")
    record(
        tmp_path,
        {"202610": epoch("2026-11-01T01:00:00"), "202611": epoch("2026-11-20T01:00:00")},
    )
    clock[0] = epoch("2026-12-02T01:00:00")
    async with run_server(history_store=tmp_path) as port, verified(port) as spawner:
        await eventually(lambda: not november.exists(), timeout=SWEEP_BOUND_S)
        clock[1] += 600
        assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.OpenChart(
            ChartRole.HISTORY, "EURUSD"
        )
        assert october.exists()
        assert not november.exists()
        assert not december.exists()


@pytest.mark.parametrize("returned", [False, True], ids=["missing-file", "tick-era-error"])
async def test_three_failed_attempts_are_reported_and_a_later_pass_clears_them(
    tmp_path, clock, returned, run_server
):
    path = month_file(tmp_path)
    async with (
        run_server(history_store=tmp_path, chart_idle_seconds=10, resync_reopen_seconds=20) as port,
        verified(port) as spawner,
    ):
        await eventually(lambda: not path.exists(), timeout=SWEEP_BOUND_S)
        clock[1] += 20
        for attempt in range(3):
            assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.OpenChart(
                ChartRole.HISTORY, "EURUSD"
            )
            ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
            if returned:
                path.write_bytes(b"live flush")
            await measured(ea)
            if returned:
                await verify_month(ea, failure=True)
            await requests_finished(port)
            if attempt < 2:
                assert not (tmp_path / ".mt5-connector" / "resync.json").exists()
            clock[1] += 10
            assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.CloseChart(
                ChartRole.HISTORY, "EURUSD"
            )
            await chart_closed(ea, run_server)
            clock[1] += 20
        await eventually(lambda: (tmp_path / ".mt5-connector/resync.json").exists())
        assert stored(tmp_path)["months"] == {}
        assert stored(tmp_path)["outcome"] == "failed"
        assert "202611" in stored(tmp_path)["month"]
        ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
        ranges = await listed(port, ea)
        assert "202611" in ranges.resync_failure
        clock[1] += 40
        assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.CloseChart(
            ChartRole.HISTORY, "EURUSD"
        )
        await chart_closed(ea, run_server)
        clock[0] = epoch("2026-11-10T23:00:00")
        clock[1] += 100
        assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.OpenChart(
            ChartRole.HISTORY, "EURUSD"
        )
        ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
        try:
            path.write_bytes(b"downloaded")
            await measured(ea)
            await verify_month(ea, empty=True)
            await eventually(lambda: stored(tmp_path)["outcome"] == "downloaded")
            assert (await listed(port, ea)).resync_failure == ""
        finally:
            await ea.close()


async def test_a_running_read_finishes_and_the_chart_then_closes_at_once(
    tmp_path, clock, run_server
):
    path = month_file(tmp_path)
    record(tmp_path, {"202611": clock[0]})
    async with (
        run_server(history_store=tmp_path, resync_reopen_seconds=20) as port,
        verified(port) as spawner,
    ):
        ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
        consumer = await Consumer.connect(port)
        await consumer.ask(
            1,
            calls.Call.COPY_TICKS_RANGE,
            symbol="EURUSD",
            date_from=clock[0] - 120,
            date_to=clock[0] - 60,
            flags=mirror.COPY_TICKS_INFO,
        )
        request = await ea.request()
        clock[0] = epoch("2026-11-10T23:00:00")
        second = await Consumer.connect(port)
        await second.ask(2, calls.Call.RANGES, symbol="EURUSD")
        assert (await second.received()).error.kind is binary.ErrorKind.SYNCING
        await no_chart_message(spawner)
        assert path.exists()
        ticks = np.zeros(0, dtype=np.dtype(list(mirror.TICKS.dtype)))
        await ea.send(answer(request.request_id, calls.Call.COPY_TICKS_RANGE, ticks))
        assert (await consumer.received()).error is None
        await consumer.close()
        await second.close()
        assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.CloseChart(
            ChartRole.HISTORY, "EURUSD"
        )
        await chart_closed(ea, run_server)
        await eventually(lambda: not path.exists(), timeout=SWEEP_BOUND_S)
        clock[1] += 19
        await no_chart_message(spawner)
        clock[1] += 1
        assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.OpenChart(
            ChartRole.HISTORY, "EURUSD"
        )
        assert not path.exists()


async def test_the_pass_runs_once_at_broker_one_each_day_after_the_start_pass(
    tmp_path, clock, run_server
):
    path = month_file(tmp_path)
    record(tmp_path, {"202611": clock[0]})
    async with run_server(history_store=tmp_path) as port, verified(port) as spawner:
        await no_chart_message(spawner)
        clock[0] = epoch("2026-11-10T22:59:00")
        await no_chart_message(spawner)
        assert path.exists()
        clock[0] += 60
        await eventually(lambda: not path.exists(), timeout=SWEEP_BOUND_S)
        clock[1] += 600
        assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.OpenChart(
            ChartRole.HISTORY, "EURUSD"
        )
        assert not path.exists()
        ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
        path.write_bytes(b"downloaded")
        await measured(ea)
        await verify_month(ea, empty=True)
        await eventually(lambda: stored(tmp_path)["months"]["202611"] == clock[0])
        clock[1] += 900
        assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.CloseChart(
            ChartRole.HISTORY, "EURUSD"
        )
        await chart_closed(ea, run_server)
        clock[1] += 700
        clock[0] = epoch("2026-11-11T03:00:00")
        # An unrecorded month would be due if the scheduler incorrectly began another pass.
        october = month_file(tmp_path, "202610")
        await no_chart_message(spawner)
        assert path.exists()
        assert october.exists()
        clock[0] = epoch("2026-11-11T23:00:00")
        assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.OpenChart(
            ChartRole.HISTORY, "EURUSD"
        )
        assert not path.exists()
        assert not october.exists()


async def test_an_empty_store_has_no_tick_era_claim_or_failure(tmp_path, clock, run_server):
    async with run_server(history_store=tmp_path) as port, verified(port):
        ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
        try:
            ranges = await listed(port, ea, first_tick=0)
            assert ranges.first_tick == 0
            assert ranges.claimable_through == 0
            assert ranges.resync_failure == ""
        finally:
            await ea.close()


async def test_a_newest_month_without_ended_bars_is_verified_without_a_tick_read(
    tmp_path, clock, run_server
):
    path = month_file(tmp_path)
    async with run_server(history_store=tmp_path) as port, verified(port) as spawner:
        await eventually(lambda: not path.exists(), timeout=SWEEP_BOUND_S)
        clock[1] += 600
        assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.OpenChart(
            ChartRole.HISTORY, "EURUSD"
        )
        ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
        try:
            path.write_bytes(b"downloaded")
            await measured(ea)
            await verify_month(ea, empty=True)
            await eventually(lambda: (tmp_path / ".mt5-connector" / "resync.json").exists())
            assert stored(tmp_path)["outcome"] == "downloaded"
            assert (await listed(port, ea)).claimable_through == epoch("2026-11-09T00:00:00")
        finally:
            await ea.close()


@pytest.mark.parametrize(
    "call",
    [call for call, row in calls.CALLS.items() if row.role is ChartRole.HISTORY],
)
@pytest.mark.parametrize("source", [None, 0], ids=["default-source", "explicit-server-source"])
async def test_a_due_history_request_is_answered_syncing_and_reaches_no_ea(
    tmp_path, clock, run_server, call, source
):
    month_file(tmp_path)
    async with run_server(history_store=tmp_path) as port, verified(port):
        ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
        consumer = await Consumer.connect(port)
        try:
            arguments = {"symbol": "EURUSD"}
            if call is calls.Call.COPY_RATES_RANGE:
                arguments |= {
                    "timeframe": mirror.TIMEFRAME_M1,
                    "date_from": clock[0] - 120,
                    "date_to": clock[0] - 60,
                }
            elif call is calls.Call.COPY_TICKS_RANGE:
                arguments |= {
                    "date_from": clock[0] - 120,
                    "date_to": clock[0] - 60,
                    "flags": mirror.COPY_TICKS_INFO,
                }
            if source is not None:
                arguments["source"] = source
            await consumer.ask(1, call, **arguments)
            response = await consumer.received()
            assert response.error.kind is binary.ErrorKind.SYNCING
            assert response.error.retry_after_ms == 5000
            assert "EURUSD" in response.error.message
            assert "202611" in response.error.message
            with pytest.raises(TimeoutError):
                await ea.request(timeout=0.1)
        finally:
            await consumer.close()
            await ea.close()


async def test_a_due_symbol_is_deleted_at_the_charts_close_and_reopened_after_the_reopen_wait(
    tmp_path, clock, run_server
):
    path = month_file(tmp_path)
    async with (
        run_server(history_store=tmp_path, resync_reopen_seconds=20) as port,
        verified(port) as spawner,
    ):
        await eventually(lambda: not path.exists(), timeout=SWEEP_BOUND_S)
        clock[1] += 19
        await no_chart_message(spawner)
        clock[1] += 1
        assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.OpenChart(
            ChartRole.HISTORY, "EURUSD"
        )


async def test_two_symbols_re_sync_independently(tmp_path, clock, run_server):
    eurusd = month_file(tmp_path)
    audcad = tmp_path / "broker" / "ticks" / "AUDCAD" / "202611.tkc"
    audcad.parent.mkdir(parents=True)
    audcad.write_bytes(b"recorded live")
    async with (
        run_server(history_store=tmp_path, resync_reopen_seconds=20) as port,
        verified(port) as spawner,
    ):
        await eventually(lambda: not eurusd.exists() and not audcad.exists(), timeout=SWEEP_BOUND_S)
        clock[1] += 20
        opened = [await spawner.received(timeout=SWEEP_BOUND_S) for _ in range(2)]
        assert set(opened) == {
            binary.OpenChart(ChartRole.HISTORY, "EURUSD"),
            binary.OpenChart(ChartRole.HISTORY, "AUDCAD"),
        }
        slow = await FakeEa.connect(port, ChartRole.HISTORY, "AUDCAD")
        fast = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
        try:
            await slow.request(timeout=SWEEP_BOUND_S)
            eurusd.write_bytes(b"downloaded")
            await measured(fast)
            await verify_month(fast, empty=True)
            await eventually(
                lambda: (tmp_path / ".mt5-connector" / "resync.json").exists(),
                timeout=SWEEP_BOUND_S,
            )
            assert stored(tmp_path)["months"] == {"202611": clock[0]}
            consumer = await Consumer.connect(port)
            try:
                await consumer.ask(1, calls.Call.RANGES, symbol="AUDCAD")
                assert (await consumer.received()).error.kind is binary.ErrorKind.SYNCING
                assert (await listed(port, fast)).resync_failure == ""
            finally:
                await consumer.close()
        finally:
            await slow.close()
            await fast.close()


@pytest.mark.parametrize("bars_first", [False, True])
@pytest.mark.parametrize("files_listed_before_answer", [False, True])
async def test_a_store_the_first_read_creates_keeps_its_ended_months_and_re_syncs_the_live_one(
    tmp_path, clock, run_server, bars_first, files_listed_before_answer, first_months_listed
):
    async with (
        run_server(history_store=tmp_path, resync_reopen_seconds=20) as port,
        verified(port) as spawner,
    ):
        ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
        consumer = await Consumer.connect(port)
        try:
            if bars_first:
                await consumer.ask(
                    1,
                    calls.Call.COPY_RATES_RANGE,
                    symbol="EURUSD",
                    timeframe=mirror.TIMEFRAME_M1,
                    date_from=clock[0] - 120,
                    date_to=clock[0] - 60,
                )
            else:
                await consumer.ask(1, calls.Call.RANGES, symbol="EURUSD")
            request = await ea.request()
            october = month_file(tmp_path, "202610")
            november = month_file(tmp_path)
            if files_listed_before_answer:
                await asyncio.wait_for(first_months_listed.wait(), SWEEP_BOUND_S)
                await no_chart_message(spawner)
                assert october.exists() and november.exists()
            if bars_first:
                rows = np.zeros(0, dtype=np.dtype(list(mirror.RATES.dtype)))
                await ea.send(answer(request.request_id, calls.Call.COPY_RATES_RANGE, rows))
            else:
                await ea.send(
                    answer(
                        request.request_id,
                        calls.Call.RANGES,
                        (100_000, [], CLOCK.to_broker(epoch("2026-10-01T00:00:00"))),
                    )
                )
            response = await consumer.received()
            if bars_first:
                assert response.error is None
            else:
                assert response.error.kind is binary.ErrorKind.SYNCING
            await eventually(lambda: (tmp_path / ".mt5-connector/resync.json").exists())
            assert stored(tmp_path)["synced_through"] == clock[0]
            assert stored(tmp_path)["months"] == {"202610": clock[0]}
            assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.CloseChart(
                ChartRole.HISTORY, "EURUSD"
            )
            await chart_closed(ea, run_server)
            await eventually(lambda: not november.exists())
            assert october.exists()
            await consumer.ask(2, calls.Call.RANGES, symbol="EURUSD")
            assert (await consumer.received()).error.kind is binary.ErrorKind.SYNCING
            clock[1] += 20
            assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.OpenChart(
                ChartRole.HISTORY, "EURUSD"
            )
            ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
            november.write_bytes(b"downloaded")
            await measured(ea, first_tick=epoch("2026-10-01T00:00:00"))
            await verify_month(ea, empty=True)
            await eventually(lambda: len(stored(tmp_path)["months"]) == 2)
            assert october.read_bytes() == b"recorded live"
            assert (await listed(port, ea)).claimable_through == epoch("2026-11-09T00:00:00")
        finally:
            await consumer.close()
            await ea.close()


async def test_a_consumers_ranges_answer_composed_after_the_day_turned_is_syncing(
    tmp_path, clock, run_server
):
    month_file(tmp_path)
    record(tmp_path, {"202611": clock[0]})
    async with run_server(history_store=tmp_path) as port, verified(port):
        ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
        consumer = await Consumer.connect(port)
        try:
            await consumer.ask(1, calls.Call.RANGES, symbol="EURUSD")
            request = await ea.request()
            clock[0] = epoch("2026-11-10T23:00:00")
            await ea.send(
                answer(
                    request.request_id,
                    calls.Call.RANGES,
                    (100_000, [], CLOCK.to_broker(FIRST_TICK)),
                )
            )
            response = await consumer.received()
            assert response.error.kind is binary.ErrorKind.SYNCING
            assert "202611" in response.error.message
        finally:
            await consumer.close()
            await ea.close()


@pytest.mark.parametrize("elapsed", [15, 700])
async def test_a_chart_closed_before_falling_due_reopens_at_the_remainder(
    tmp_path, clock, run_server, elapsed
):
    path = month_file(tmp_path)
    record(tmp_path, {"202611": clock[0]})
    async with (
        run_server(history_store=tmp_path, resync_reopen_seconds=20) as port,
        verified(port) as spawner,
    ):
        ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
        await listed(port, ea)
        clock[1] += 900
        assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.CloseChart(
            ChartRole.HISTORY, "EURUSD"
        )
        await chart_closed(ea, run_server)
        clock[1] += elapsed
        clock[0] = epoch("2026-11-10T23:00:00")
        await eventually(lambda: not path.exists())
        if elapsed < 20:
            clock[1] += 4
            await no_chart_message(spawner)
            clock[1] += 1
        assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.OpenChart(
            ChartRole.HISTORY, "EURUSD"
        )


async def test_the_passs_measurement_deferred_syncing_is_asked_again_without_losing_an_attempt(
    tmp_path, clock, run_server, caplog
):
    path = month_file(tmp_path)
    async with (
        run_server(history_store=tmp_path, resync_reopen_seconds=20) as port,
        verified(port) as spawner,
    ):
        await eventually(lambda: not path.exists())
        clock[1] += 20
        await spawner.received(timeout=SWEEP_BOUND_S)
        ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
        try:
            request = await ea.request(timeout=SWEEP_BOUND_S)
            for _ in range(2):
                await ea.send(
                    binary.Answer(
                        request.request_id,
                        0,
                        True,
                        0,
                        error=binary.AnswerError(binary.ErrorKind.SYNCING, "still loading", 50),
                    )
                )
                await requests_finished(port)
                next_request = asyncio.create_task(ea.request(timeout=SWEEP_BOUND_S))

                def retried(asked=next_request):
                    clock[1] += 1
                    return asked.done()

                await eventually(retried)
                request = await next_request
            path.write_bytes(b"downloaded")
            await ea.send(
                answer(
                    request.request_id,
                    calls.Call.RANGES,
                    (
                        100_000,
                        [(TICK_TIMEFRAME, CLOCK.to_broker(FIRST_TICK))],
                        CLOCK.to_broker(FIRST_TICK),
                    ),
                )
            )
            await verify_month(ea, empty=True)
            await eventually(lambda: (tmp_path / ".mt5-connector/resync.json").exists())
            assert stored(tmp_path)["outcome"] == "downloaded"
            assert "failed" not in caplog.text
        finally:
            await ea.close()


async def test_the_sweeps_file_work_delays_no_request(tmp_path, clock, run_server, monkeypatch):
    entered = threading.Event()
    enabled = threading.Event()
    released = threading.Event()

    async def listing(store):
        def slow():
            if enabled.is_set():
                entered.set()
                released.wait(2)
            return store.snapshot()

        return await asyncio.to_thread(slow)

    monkeypatch.setattr(TickStore, "listing", listing)
    async with run_server(history_store=tmp_path) as port, verified(port):
        ea = await FakeEa.connect(port, ChartRole.PUBLISHER, "AUDCAD")
        consumer = await Consumer.connect(port)
        try:
            enabled.set()
            started = time.monotonic()
            await eventually(entered.is_set)
            assert time.monotonic() - started < 0.5
            await consumer.ask(1, calls.Call.SYMBOL_INFO_TICK, symbol="AUDCAD")
            request = await ea.request(timeout=0.2)
            await ea.send(
                binary.Answer(
                    request.request_id,
                    0,
                    True,
                    0,
                    error=binary.AnswerError(binary.ErrorKind.EA_ERROR, "test response"),
                )
            )
            assert (await consumer.received(timeout=0.2)).error.message == "test response"
        finally:
            released.set()
            await consumer.close()
            await ea.close()


@pytest.mark.parametrize(
    "delete_error",
    [None, PermissionError, RuntimeError],
    ids=["slow-delete", "os-error", "runtime-error"],
)
async def test_deleting_one_symbols_months_does_not_wait_for_anothers(
    tmp_path, clock, run_server, monkeypatch, delete_error
):
    eurusd = month_file(tmp_path)
    audcad = month_file(tmp_path, symbol="AUDCAD")
    entered = threading.Event()
    released = threading.Event()
    unlink = type(eurusd).unlink

    def remove(path, *args, **kwargs):
        if path == audcad:
            entered.set()
            released.wait(SWEEP_BOUND_S * 3)
            if delete_error is not None:
                raise delete_error("AUDCAD month cannot be deleted")
        return unlink(path, *args, **kwargs)

    monkeypatch.setattr(type(eurusd), "unlink", remove)
    async with (
        run_server(history_store=tmp_path, resync_reopen_seconds=20) as port,
        verified(port) as spawner,
    ):
        try:
            await eventually(entered.is_set, timeout=SWEEP_BOUND_S)
            await eventually(lambda: not eurusd.exists(), timeout=1)
            assert audcad.exists()
            clock[1] += 20
            assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.OpenChart(
                ChartRole.HISTORY, "EURUSD"
            )
            ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
            try:
                eurusd.write_bytes(b"downloaded")
                await measured(ea)
                await verify_month(ea, empty=True)
                await eventually(lambda: (tmp_path / ".mt5-connector/resync.json").exists())
                assert stored(tmp_path)["months"] == {"202611": clock[0]}
                assert audcad.exists()
                assert (await listed(port, ea)).resync_failure == ""
                released.set()
                if delete_error is not None:

                    def failed():
                        records = json.loads((tmp_path / ".mt5-connector/resync.json").read_text())
                        return records.get("AUDCAD", {}).get("outcome") == "failed"

                    await eventually(failed, timeout=SWEEP_BOUND_S)
                    assert audcad.exists()
                    assert stored(tmp_path)["months"] == {"202611": clock[0]}
                    assert (await asyncio.to_thread(health, port))[0] == 200
                else:
                    await eventually(lambda: not audcad.exists())
            finally:
                await ea.close()
        finally:
            released.set()


def test_dispatch_on_a_symbol_that_has_a_machine_raises(tmp_path):
    from hub_doubles import settings

    resync = Resync(
        CLOCK,
        settings(history_store=tmp_path),
        ready=lambda: True,
        release=lambda symbol: True,
        closed_at=lambda symbol: 0,
        monotonic=lambda: 0,
        request=lambda call, arguments: False,
    )
    resync.dispatch("EURUSD", {"202611"}, Cause.PASS)
    resync.dispatch("AUDCAD", {"202611"}, Cause.PASS)
    with pytest.raises(ValueError, match="EURUSD already has a re-sync"):
        resync.dispatch("EURUSD", {"202610"}, Cause.PASS)


async def test_terminal_source_gates_only_the_deleted_window_and_symbols_are_independent(
    tmp_path, clock, run_server, monkeypatch
):
    eurusd = month_file(tmp_path)
    audcad = month_file(tmp_path, symbol="AUDCAD")
    entered = threading.Event()
    released = threading.Event()
    unlink = type(eurusd).unlink

    def remove(path, *args, **kwargs):
        if path == audcad:
            entered.set()
            released.wait(SWEEP_BOUND_S * 3)
        return unlink(path, *args, **kwargs)

    monkeypatch.setattr(type(eurusd), "unlink", remove)
    async with (
        run_server(history_store=tmp_path, resync_reopen_seconds=20) as port,
        verified(port) as spawner,
    ):
        consumer = await Consumer.connect(port)
        try:
            await eventually(entered.is_set, timeout=SWEEP_BOUND_S)
            await eventually(lambda: not eurusd.exists(), timeout=1)
            for request_id, symbol in enumerate(("AUDCAD", "EURUSD"), start=1):
                await consumer.ask(request_id, calls.Call.RANGES, symbol=symbol, source=1)
                assert (await consumer.received()).error.kind is binary.ErrorKind.SYNCING
            clock[1] += 20
            assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.OpenChart(
                ChartRole.HISTORY, "EURUSD"
            )
            ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
            try:
                measurement = await ea.request(timeout=SWEEP_BOUND_S)
                assert arguments_of(measurement) == (calls.Call.RANGES, {"symbol": "EURUSD"})
                await consumer.ask(3, calls.Call.RANGES, symbol="EURUSD", source=1)
                assert (await consumer.received()).error.kind is binary.ErrorKind.SYNCING
                eurusd.write_bytes(b"downloaded")
                await ea.send(
                    answer(
                        measurement.request_id,
                        calls.Call.RANGES,
                        (
                            100_000,
                            [(TICK_TIMEFRAME, CLOCK.to_broker(FIRST_TICK))],
                            CLOCK.to_broker(FIRST_TICK),
                        ),
                    )
                )
                verification = await ea.request(timeout=SWEEP_BOUND_S)
                assert arguments_of(verification)[0] is calls.Call.COPY_RATES_RANGE
                arguments = {
                    "symbol": "EURUSD",
                    "timeframe": mirror.TIMEFRAME_M1,
                    "date_from": clock[0] - 120,
                    "date_to": clock[0] - 60,
                }
                await consumer.ask(4, calls.Call.COPY_RATES_RANGE, **arguments, source=1)
                routed = await ea.request(timeout=SWEEP_BOUND_S)
                assert arguments_of(routed) == (
                    calls.Call.COPY_RATES_RANGE,
                    arguments
                    | {
                        "date_from": CLOCK.to_broker(arguments["date_from"]),
                        "date_to": CLOCK.to_broker(arguments["date_to"]),
                    },
                )
                rows = np.zeros(0, dtype=np.dtype(list(mirror.RATES.dtype)))
                await ea.send(answer(routed.request_id, calls.Call.COPY_RATES_RANGE, rows))
                assert (await consumer.received()).error is None
                await consumer.ask(5, calls.Call.RANGES, symbol="EURUSD")
                assert (await consumer.received()).error.kind is binary.ErrorKind.SYNCING
                await consumer.ask(6, calls.Call.RANGES, symbol="AUDCAD", source=1)
                assert (await consumer.received()).error.kind is binary.ErrorKind.SYNCING
                assert audcad.exists()
                await ea.send(answer(verification.request_id, calls.Call.COPY_RATES_RANGE, rows))
                await eventually(lambda: (tmp_path / ".mt5-connector/resync.json").exists())
                assert (await listed(port, ea)).resync_failure == ""
            finally:
                await ea.close()
        finally:
            released.set()
            await consumer.close()


async def test_two_charts_close_anchors_stay_independent_during_a_slow_delete(
    tmp_path, clock, run_server, monkeypatch
):
    eurusd = month_file(tmp_path)
    audcad = month_file(tmp_path, symbol="AUDCAD")
    record(tmp_path, {"202611": clock[0]}, symbols=("AUDCAD", "EURUSD"))
    entered = threading.Event()
    released = threading.Event()
    unlink = type(eurusd).unlink

    def remove(path, *args, **kwargs):
        if path == audcad:
            entered.set()
            released.wait(SWEEP_BOUND_S * 3)
        return unlink(path, *args, **kwargs)

    monkeypatch.setattr(type(eurusd), "unlink", remove)
    async with run_server(history_store=tmp_path) as port, verified(port) as spawner:
        eur = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
        await listed(port, eur)
        clock[1] += 685
        aud = await FakeEa.connect(port, ChartRole.HISTORY, "AUDCAD")
        consumer = await Consumer.connect(port)
        try:
            await consumer.ask(1, calls.Call.RANGES, symbol="AUDCAD")
            await measured(aud, symbol="AUDCAD")
            assert (await consumer.received()).error is None
            clock[1] += 215
            assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.CloseChart(
                ChartRole.HISTORY, "EURUSD"
            )
            await chart_closed(eur, run_server)
            clock[1] += 685
            assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.CloseChart(
                ChartRole.HISTORY, "AUDCAD"
            )
            await chart_closed(aud, run_server)
            clock[1] += 15
            clock[0] = epoch("2026-11-10T23:00:00")
            await eventually(entered.is_set, timeout=SWEEP_BOUND_S)
            await eventually(lambda: not eurusd.exists(), timeout=1)
            assert audcad.exists()
            assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.OpenChart(
                ChartRole.HISTORY, "EURUSD"
            )
            eur = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
            eurusd.write_bytes(b"downloaded")
            await measured(eur)
            await verify_month(eur, empty=True)
            await eventually(lambda: stored(tmp_path)["months"] == {"202611": clock[0]})
            released.set()
            await eventually(lambda: not audcad.exists())
            await no_chart_message(spawner)
            clock[1] += 584
            await no_chart_message(spawner)
            clock[1] += 1
            assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.OpenChart(
                ChartRole.HISTORY, "AUDCAD"
            )
        finally:
            released.set()
            await consumer.close()
            await eur.close()
            await aud.close()


async def test_a_months_record_is_stamped_at_the_fresh_eas_hello(tmp_path, clock, run_server):
    path = month_file(tmp_path)
    async with (
        run_server(history_store=tmp_path, resync_reopen_seconds=20) as port,
        verified(port) as spawner,
    ):
        await eventually(lambda: not path.exists())
        clock[1] += 20
        await spawner.received(timeout=SWEEP_BOUND_S)
        ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
        try:
            path.write_bytes(b"downloaded")
            fetched_at = clock[0]
            await measured(ea)
            clock[0] += 120
            await verify_month(ea, empty=True)
            await eventually(lambda: (tmp_path / ".mt5-connector/resync.json").exists())
            assert stored(tmp_path)["months"] == {"202611": fetched_at}
        finally:
            await ea.close()


@pytest.mark.parametrize("failed_open", [False, True])
async def test_an_ea_gone_in_use_is_not_a_close_and_the_due_re_sync_waits_for_its_reopen(
    tmp_path, clock, run_server, failed_open
):
    path = month_file(tmp_path)
    record(tmp_path, {"202611": clock[0]})
    async with (
        run_server(history_store=tmp_path, resync_reopen_seconds=20) as port,
        verified(port) as spawner,
    ):
        ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
        await listed(port, ea)
        await chart_closed(ea, run_server)
        assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.OpenChart(
            ChartRole.HISTORY, "EURUSD"
        )
        clock[1] += 100
        clock[0] = epoch("2026-11-10T23:00:00")
        consumer = await Consumer.connect(port)
        try:
            await consumer.ask(1, calls.Call.RANGES, symbol="EURUSD")
            assert (await consumer.received()).error.kind is binary.ErrorKind.SYNCING
            assert path.exists()
            await no_chart_message(spawner)
            if failed_open:
                await spawner.send(binary.ChartFailed(ChartRole.HISTORY, "EURUSD", 4102))
                await eventually(lambda: not path.exists())
                clock[1] += 9
                await no_chart_message(spawner)
                clock[1] += 1
                assert stored(tmp_path)["outcome"] == "downloaded"
            else:
                ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
                assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.CloseChart(
                    ChartRole.HISTORY, "EURUSD"
                )
                await chart_closed(ea, run_server)
                await eventually(lambda: not path.exists())
                clock[1] += 19
                await no_chart_message(spawner)
                clock[1] += 1
            assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.OpenChart(
                ChartRole.HISTORY, "EURUSD"
            )
        finally:
            await consumer.close()


async def test_a_terminal_request_is_routed_until_its_months_are_deleted_and_syncing_until_measured(
    tmp_path, clock, run_server
):
    path = month_file(tmp_path)
    record(tmp_path, {"202611": clock[0]})
    bars = np.zeros(0, dtype=np.dtype(list(mirror.RATES.dtype)))
    ticks = np.zeros(0, dtype=np.dtype(list(mirror.TICKS.dtype)))
    async with (
        run_server(history_store=tmp_path, resync_reopen_seconds=20) as port,
        verified(port) as spawner,
    ):
        ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
        first = await Consumer.connect(port)
        second = await Consumer.connect(port)
        arguments = {
            "symbol": "EURUSD",
            "timeframe": mirror.TIMEFRAME_M1,
            "date_from": clock[0] - 120,
            "date_to": clock[0] - 60,
        }
        try:
            await first.ask(
                1,
                calls.Call.COPY_TICKS_RANGE,
                symbol="EURUSD",
                flags=mirror.COPY_TICKS_INFO,
                date_from=clock[0] - 120,
                date_to=clock[0] - 60,
            )
            running = await ea.request()
            clock[0] = epoch("2026-11-10T23:00:00")
            await second.ask(2, calls.Call.COPY_RATES_RANGE, **arguments)
            assert (await second.received()).error.kind is binary.ErrorKind.SYNCING
            await second.ask(3, calls.Call.COPY_RATES_RANGE, **arguments, source=1)
            terminal = await ea.request()
            assert arguments_of(terminal)[1].keys() == arguments.keys()
            await ea.send(answer(terminal.request_id, calls.Call.COPY_RATES_RANGE, bars))
            await ea.send(answer(running.request_id, calls.Call.COPY_TICKS_RANGE, ticks))
            assert (await first.received()).error is None
            assert (await second.received()).error is None
            assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.CloseChart(
                ChartRole.HISTORY, "EURUSD"
            )
            await chart_closed(ea, run_server)
            await eventually(lambda: not path.exists())
            await second.ask(4, calls.Call.COPY_RATES_RANGE, **arguments, source=1)
            refusal = await second.received()
            assert refusal.error.kind is binary.ErrorKind.SYNCING
            assert "deleted" in refusal.error.message
            clock[1] += 20
            assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.OpenChart(
                ChartRole.HISTORY, "EURUSD"
            )
            ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
            path.write_bytes(b"downloaded")
            await measured(ea)
            verification = await ea.request(timeout=SWEEP_BOUND_S)
            assert arguments_of(verification)[0] is calls.Call.COPY_RATES_RANGE
            await second.ask(5, calls.Call.COPY_RATES_RANGE, **arguments, source=1)
            terminal = await ea.request()
            await ea.send(answer(terminal.request_id, calls.Call.COPY_RATES_RANGE, bars))
            assert (await second.received()).error is None
            await second.ask(6, calls.Call.COPY_RATES_RANGE, **arguments)
            assert (await second.received()).error.kind is binary.ErrorKind.SYNCING
            await ea.send(answer(verification.request_id, calls.Call.COPY_RATES_RANGE, bars))
            await eventually(lambda: stored(tmp_path)["months"]["202611"] == clock[0])
            for request_id, source in [(7, 0), (8, 1)]:
                await second.ask(
                    request_id, calls.Call.COPY_RATES_RANGE, **arguments, source=source
                )
                routed = await ea.request()
                await ea.send(answer(routed.request_id, calls.Call.COPY_RATES_RANGE, bars))
                assert (await second.received()).error is None
        finally:
            await first.close()
            await second.close()
            await ea.close()


async def test_an_unknown_source_is_refused_with_the_frame(tmp_path, clock, run_server):
    async with run_server(history_store=tmp_path) as port, verified(port):
        consumer = await Consumer.connect(port)
        try:
            await consumer.ask(
                1,
                calls.Call.COPY_RATES_RANGE,
                symbol="EURUSD",
                timeframe=mirror.TIMEFRAME_M1,
                date_from=clock[0] - 120,
                date_to=clock[0] - 60,
                source=7,
            )
            refusal = await consumer.received()
            assert isinstance(refusal, binary.Refusal)
            assert "source" in refusal.message
        finally:
            await consumer.close()


@pytest.mark.parametrize("single_requests", [False, True])
async def test_a_batch_at_the_hour_is_n_single_dispatches(
    tmp_path, clock, run_server, single_requests, caplog
):
    caplog.set_level(logging.INFO)
    paths = []
    for symbol in ("AUDCAD", "EURUSD", "USDJPY"):
        paths.append(month_file(tmp_path, symbol=symbol))
    record(tmp_path, {"202611": clock[0]}, symbols=("AUDCAD", "EURUSD", "USDJPY"))
    requested = []
    async with (
        run_server(history_store=tmp_path, resync_reopen_seconds=20) as port,
        verified(port) as spawner,
    ):
        consumer = await Consumer.connect(port)
        eas = []
        try:
            clock[0] = epoch("2026-11-10T23:00:00")
            if single_requests:
                for request_id, symbol in enumerate(("AUDCAD", "EURUSD", "USDJPY"), start=1):
                    await consumer.ask(request_id, calls.Call.RANGES, symbol=symbol)
                    assert (await consumer.received()).error.kind is binary.ErrorKind.SYNCING
            await eventually(
                lambda: all(not path.exists() for path in paths), timeout=SWEEP_BOUND_S
            )
            assert all(not path.exists() for path in paths)
            assert requested == []
            await no_chart_message(spawner)
            clock[1] += 20
            opened = [await spawner.received(timeout=SWEEP_BOUND_S) for _ in range(3)]
            assert set(opened) == {
                binary.OpenChart(ChartRole.HISTORY, symbol)
                for symbol in ("AUDCAD", "EURUSD", "USDJPY")
            }
            for symbol in ("AUDCAD", "EURUSD", "USDJPY"):
                ea = await FakeEa.connect(port, ChartRole.HISTORY, symbol)
                eas.append(ea)
                requested.append(arguments_of(await ea.request(timeout=SWEEP_BOUND_S)))
            assert requested == [
                (calls.Call.RANGES, {"symbol": symbol}) for symbol in ("AUDCAD", "EURUSD", "USDJPY")
            ]
            for symbol in ("AUDCAD", "EURUSD", "USDJPY"):
                assert (
                    f"re-sync of {symbol}: due 202611 (attempt 1 of 3); the scheduled pass"
                    in caplog.text
                )
        finally:
            for ea in eas:
                await ea.close()
            await consumer.close()


@pytest.mark.parametrize("late_month", [False, True], ids=["during-wait", "during-measurement"])
async def test_months_found_due_while_a_machine_runs_start_a_new_machine_after_it_ends(
    tmp_path, clock, run_server, late_month
):
    november = month_file(tmp_path)
    record(tmp_path, {"202611": clock[0]})
    async with (
        run_server(history_store=tmp_path, resync_reopen_seconds=20) as port,
        verified(port) as spawner,
    ):
        clock[0] = epoch("2026-11-30T23:00:00")
        await eventually(lambda: not november.exists())
        clock[1] += 10
        december = tmp_path / "broker/ticks/EURUSD/202612.tkc"
        if not late_month:
            month_file(tmp_path, "202612")
        await no_chart_message(spawner)
        assert december.exists() is (not late_month)
        clock[1] += 10
        assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.OpenChart(
            ChartRole.HISTORY, "EURUSD"
        )
        ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
        try:
            november.write_bytes(b"downloaded")
            if late_month:
                request = await ea.request(timeout=SWEEP_BOUND_S)
                month_file(tmp_path, "202612")
                await ea.send(
                    answer(
                        request.request_id,
                        calls.Call.RANGES,
                        (
                            100_000,
                            [(TICK_TIMEFRAME, CLOCK.to_broker(FIRST_TICK))],
                            CLOCK.to_broker(FIRST_TICK),
                        ),
                    )
                )
            else:
                await measured(ea)
            await verify_month(ea, empty=True)
            assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.CloseChart(
                ChartRole.HISTORY, "EURUSD"
            )
            await chart_closed(ea, run_server)
            await eventually(lambda: not december.exists())
            assert november.exists()
            assert stored(tmp_path)["months"] == {"202611": clock[0]}
            consumer = await Consumer.connect(port)
            try:
                await consumer.ask(1, calls.Call.RANGES, symbol="EURUSD")
                assert (await consumer.received()).error.kind is binary.ErrorKind.SYNCING
            finally:
                await consumer.close()
            clock[1] += 20
            assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.OpenChart(
                ChartRole.HISTORY, "EURUSD"
            )
            ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
            december.write_bytes(b"downloaded")
            await measured(ea)
            await eventually(lambda: len(stored(tmp_path)["months"]) == 2)
            assert (await listed(port, ea)).resync_failure == ""
        finally:
            await ea.close()


async def test_a_failed_chart_open_does_not_delay_another_symbols_re_sync(
    tmp_path, clock, run_server
):
    eurusd = month_file(tmp_path)
    audcad = tmp_path / "broker" / "ticks" / "AUDCAD" / "202611.tkc"
    audcad.parent.mkdir(parents=True)
    audcad.write_bytes(b"recorded live")
    async with (
        run_server(history_store=tmp_path, resync_reopen_seconds=20) as port,
        verified(port) as spawner,
    ):
        await eventually(lambda: not eurusd.exists() and not audcad.exists())
        clock[1] += 20
        for _ in range(2):
            await spawner.received(timeout=SWEEP_BOUND_S)
        await spawner.send(binary.ChartFailed(ChartRole.HISTORY, "AUDCAD", 4102))
        ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
        try:
            eurusd.write_bytes(b"downloaded")
            await measured(ea)
            await verify_month(ea, empty=True)
            await eventually(lambda: (tmp_path / ".mt5-connector/resync.json").exists())
            assert (await listed(port, ea)).resync_failure == ""
            consumer = await Consumer.connect(port)
            try:
                await consumer.ask(1, calls.Call.RANGES, symbol="AUDCAD")
                refusal = await consumer.received()
                assert refusal.error.kind is binary.ErrorKind.SYNCING
                assert "attempt 2 of 3" in refusal.error.message
            finally:
                await consumer.close()
        finally:
            await ea.close()


async def test_a_store_present_before_any_request_is_never_taken_as_server_data(
    tmp_path, clock, run_server
):
    october = month_file(tmp_path, "202610")
    november = month_file(tmp_path)
    async with (
        run_server(history_store=tmp_path, resync_reopen_seconds=20) as port,
        verified(port) as spawner,
    ):
        consumer = await Consumer.connect(port)
        try:
            await consumer.ask(1, calls.Call.RANGES, symbol="EURUSD")
            assert (await consumer.received()).error.kind is binary.ErrorKind.SYNCING
            await eventually(lambda: not october.exists() and not november.exists())
            clock[1] += 20
            assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.OpenChart(
                ChartRole.HISTORY, "EURUSD"
            )
            ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
            try:
                october.write_bytes(b"downloaded")
                november.write_bytes(b"downloaded")
                await measured(ea)
                await verify_month(ea, empty=True)
                await eventually(lambda: (tmp_path / ".mt5-connector/resync.json").exists())
                assert stored(tmp_path)["months"] == {"202610": clock[0], "202611": clock[0]}
                assert "synced_through" not in stored(tmp_path)
            finally:
                await ea.close()
        finally:
            await consumer.close()


@pytest.mark.parametrize("reask", [False, True], ids=["hold-expires", "reasked"])
async def test_a_first_download_deferral_holds_examination_for_the_reask(
    tmp_path, clock, run_server, first_months_listed, reask
):
    async with (
        run_server(history_store=tmp_path, resync_reopen_seconds=20) as port,
        verified(port) as spawner,
    ):
        ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
        consumer = await Consumer.connect(port)
        try:
            await consumer.ask(1, calls.Call.RANGES, symbol="EURUSD")
            request = await ea.request()
            october = month_file(tmp_path, "202610")
            november = month_file(tmp_path)
            await asyncio.wait_for(first_months_listed.wait(), SWEEP_BOUND_S)
            clock[1] += 900
            await ea.send(
                binary.Answer(
                    request.request_id,
                    0,
                    True,
                    0,
                    error=binary.AnswerError(binary.ErrorKind.SYNCING, "still measuring", 50),
                )
            )
            assert (await consumer.received()).error.kind is binary.ErrorKind.SYNCING
            await no_chart_message(spawner)
            assert october.exists() and november.exists()
            if reask:
                await consumer.ask(2, calls.Call.RANGES, symbol="EURUSD")
                request = await ea.request()
                await ea.send(
                    answer(
                        request.request_id,
                        calls.Call.RANGES,
                        (100_000, [], CLOCK.to_broker(epoch("2026-10-01T00:00:00"))),
                    )
                )
                assert (await consumer.received()).error.kind is binary.ErrorKind.SYNCING
                await eventually(lambda: (tmp_path / ".mt5-connector/resync.json").exists())
                assert stored(tmp_path)["months"] == {"202610": clock[0]}
            else:
                clock[1] += 11
            assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.CloseChart(
                ChartRole.HISTORY, "EURUSD"
            )
            await chart_closed(ea, run_server)
            await eventually(lambda: not november.exists())
            assert october.exists() is reask
        finally:
            await consumer.close()
            await ea.close()


@pytest.mark.parametrize(
    "kind", [binary.ErrorKind.EA_ERROR, binary.ErrorKind.TIMEOUT, binary.ErrorKind.EA_GONE]
)
async def test_a_first_download_without_a_result_releases_the_store(
    tmp_path, clock, run_server, first_months_listed, monkeypatch, kind
):
    if kind is binary.ErrorKind.TIMEOUT:
        monkeypatch.setattr(calls, "HISTORY_BUSY_S", 0.5)
    async with (
        run_server(history_store=tmp_path, resync_reopen_seconds=20) as port,
        verified(port) as spawner,
    ):
        ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
        consumer = await Consumer.connect(port)
        try:
            await consumer.ask(1, calls.Call.RANGES, symbol="EURUSD")
            request = await ea.request()
            october = month_file(tmp_path, "202610")
            november = month_file(tmp_path)
            await asyncio.wait_for(first_months_listed.wait(), SWEEP_BOUND_S)
            if kind is binary.ErrorKind.EA_ERROR:
                await ea.send(
                    binary.Answer(
                        request.request_id,
                        0,
                        True,
                        4401,
                        error=binary.AnswerError(kind, "the measurement failed"),
                    )
                )
            elif kind is binary.ErrorKind.EA_GONE:
                await ea.close()
            assert (await consumer.received()).error.kind is kind
            if kind is binary.ErrorKind.EA_GONE:
                assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.OpenChart(
                    ChartRole.HISTORY, "EURUSD"
                )
                ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
            assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.CloseChart(
                ChartRole.HISTORY, "EURUSD"
            )
            await chart_closed(ea, run_server)
            await eventually(lambda: not october.exists() and not november.exists())
            assert not (tmp_path / ".mt5-connector/resync.json").exists()
        finally:
            await consumer.close()
            await ea.close()


async def test_a_consumer_leaving_first_download_does_not_lose_its_end(
    tmp_path, clock, run_server, first_months_listed
):
    async with (
        run_server(history_store=tmp_path, resync_reopen_seconds=20) as port,
        verified(port) as spawner,
    ):
        ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
        consumer = await Consumer.connect(port)
        try:
            await consumer.ask(1, calls.Call.RANGES, symbol="EURUSD")
            request = await ea.request()
            october = month_file(tmp_path, "202610")
            november = month_file(tmp_path)
            await asyncio.wait_for(first_months_listed.wait(), SWEEP_BOUND_S)
            await consumer.close()
            await ea.send(
                answer(
                    request.request_id,
                    calls.Call.RANGES,
                    (100_000, [], CLOCK.to_broker(epoch("2026-10-01T00:00:00"))),
                )
            )
            await eventually(lambda: (tmp_path / ".mt5-connector/resync.json").exists())
            assert stored(tmp_path)["synced_through"] == clock[0]
            assert stored(tmp_path)["months"] == {"202610": clock[0]}
            assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.CloseChart(
                ChartRole.HISTORY, "EURUSD"
            )
            await chart_closed(ea, run_server)
            await eventually(lambda: not november.exists())
            assert october.exists()
        finally:
            await consumer.close()
            await ea.close()


async def test_first_ranges_before_its_store_listing_is_syncing_until_listed(
    tmp_path, clock, run_server, first_months_listed
):
    async with (
        run_server(history_store=tmp_path, resync_reopen_seconds=20) as port,
        verified(port) as spawner,
    ):
        ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
        consumer = await Consumer.connect(port)
        try:
            await consumer.ask(1, calls.Call.RANGES, symbol="EURUSD")
            request = await ea.request()
            await ea.send(
                answer(
                    request.request_id,
                    calls.Call.RANGES,
                    (100_000, [], CLOCK.to_broker(epoch("2026-10-01T00:00:00"))),
                )
            )
            response = await consumer.received()
            assert response.error.kind is binary.ErrorKind.SYNCING
            assert "being created" in response.error.message
            await consumer.ask(2, calls.Call.RANGES, symbol="EURUSD")
            assert "being created" in (await consumer.received()).error.message
            october = month_file(tmp_path, "202610")
            november = month_file(tmp_path)
            await asyncio.wait_for(first_months_listed.wait(), SWEEP_BOUND_S)
            await consumer.ask(3, calls.Call.RANGES, symbol="EURUSD")
            assert "202611" in (await consumer.received()).error.message
            assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.CloseChart(
                ChartRole.HISTORY, "EURUSD"
            )
            await chart_closed(ea, run_server)
            await eventually(lambda: not november.exists())
            assert october.exists()
            clock[1] += 20
            assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.OpenChart(
                ChartRole.HISTORY, "EURUSD"
            )
            ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
            november.write_bytes(b"downloaded")
            await measured(ea, first_tick=epoch("2026-10-01T00:00:00"))
            await verify_month(ea, empty=True)
            await eventually(lambda: len(stored(tmp_path)["months"]) == 2)
            assert (await listed(port, ea)).claimable_through == epoch("2026-11-09T00:00:00")
        finally:
            await consumer.close()
            await ea.close()


async def test_a_late_ended_month_keeps_the_first_download_stamp_after_a_live_pass(tmp_path, clock):
    from hub_doubles import settings

    month_file(tmp_path, "202610")
    month_file(tmp_path)
    record(tmp_path, {"202611": clock[0] + 60})
    path = tmp_path / ".mt5-connector/resync.json"
    records = json.loads(path.read_text())
    records["EURUSD"]["synced_through"] = clock[0]
    path.write_text(json.dumps(records))
    resync = Resync(
        CLOCK,
        settings(history_store=tmp_path),
        ready=lambda: True,
        release=lambda symbol: True,
        closed_at=lambda symbol: clock[1],
        monotonic=lambda: clock[1],
        request=lambda call, arguments: False,
    )
    assert resync.unsettled("EURUSD", clock[0]) == ""
    await resync.sweep(clock[0])
    assert stored(tmp_path)["months"] == {"202610": clock[0], "202611": clock[0] + 60}


async def test_one_pending_first_download_does_not_delay_another_symbols_record(
    tmp_path, clock, run_server, first_months_listed
):
    async with (
        run_server(history_store=tmp_path, resync_reopen_seconds=20) as port,
        verified(port) as spawner,
    ):
        slow = await FakeEa.connect(port, ChartRole.HISTORY, "AUDCAD")
        fast = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
        first = await Consumer.connect(port)
        second = await Consumer.connect(port)
        try:
            await first.ask(1, calls.Call.RANGES, symbol="AUDCAD")
            await slow.request()
            await second.ask(1, calls.Call.RANGES, symbol="EURUSD")
            request = await fast.request()
            slow_months = []
            for month in ("202610", "202611"):
                path = tmp_path / "broker/ticks/AUDCAD" / f"{month}.tkc"
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(b"recorded live")
                slow_months.append(path)
            october = month_file(tmp_path, "202610")
            november = month_file(tmp_path)
            await asyncio.wait_for(first_months_listed.wait(), SWEEP_BOUND_S)
            await fast.send(
                answer(
                    request.request_id,
                    calls.Call.RANGES,
                    (100_000, [], CLOCK.to_broker(epoch("2026-10-01T00:00:00"))),
                )
            )
            assert (await second.received()).error.kind is binary.ErrorKind.SYNCING
            assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.CloseChart(
                ChartRole.HISTORY, "EURUSD"
            )
            await chart_closed(fast, run_server)
            await eventually(lambda: not november.exists())
            clock[1] += 20
            assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.OpenChart(
                ChartRole.HISTORY, "EURUSD"
            )
            fast = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
            november.write_bytes(b"downloaded")
            await measured(fast, first_tick=epoch("2026-10-01T00:00:00"))
            await verify_month(fast, empty=True)
            await eventually(lambda: len(stored(tmp_path)["months"]) == 2)
            assert (await listed(port, fast)).claimable_through == epoch("2026-11-09T00:00:00")
            assert october.exists() and all(path.exists() for path in slow_months)
            records = json.loads((tmp_path / ".mt5-connector/resync.json").read_text())
            assert "AUDCAD" not in records
        finally:
            await first.close()
            await second.close()
            await slow.close()
            await fast.close()


@pytest.mark.parametrize("download_seconds", [0, 180], ids=["within-idle-grace", "past-idle-grace"])
async def test_an_ea_gone_mid_re_sync_is_re_requested_closed_and_waits_from_that_close(
    tmp_path, clock, run_server, download_seconds
):
    path = month_file(tmp_path)
    async with (
        run_server(history_store=tmp_path, chart_idle_seconds=60) as port,
        verified(port) as spawner,
    ):
        await eventually(lambda: not path.exists(), timeout=SWEEP_BOUND_S)
        clock[1] += 600
        assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.OpenChart(
            ChartRole.HISTORY, "EURUSD"
        )
        ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
        await ea.request(timeout=SWEEP_BOUND_S)
        path.write_bytes(b"partly restored before the EA left")
        clock[1] += download_seconds
        await chart_closed(ea, run_server)
        assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.OpenChart(
            ChartRole.HISTORY, "EURUSD"
        )
        assert path.exists()
        await no_chart_message(spawner)
        clock[1] += 100
        ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
        try:
            assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.CloseChart(
                ChartRole.HISTORY, "EURUSD"
            )
            assert path.exists()
            await chart_closed(ea, run_server)
            await eventually(lambda: not path.exists(), timeout=SWEEP_BOUND_S)
            clock[1] += 599
            await no_chart_message(spawner)
            clock[1] += 1
            assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.OpenChart(
                ChartRole.HISTORY, "EURUSD"
            )
        finally:
            await ea.close()


async def test_a_month_written_back_during_the_wait_is_deleted_again_before_reopening(
    tmp_path, clock, run_server, monkeypatch, caplog
):
    caplog.set_level(logging.INFO, logger="mt5connector.server.hub_resync")
    listed_files = asyncio.Event()
    listing = TickStore.listing

    async def observed_listing(store):
        result = await listing(store)
        if "202611" in result.get("EURUSD", set()):
            listed_files.set()
        return result

    monkeypatch.setattr(TickStore, "listing", observed_listing)
    path = month_file(tmp_path)
    async with run_server(history_store=tmp_path) as port, verified(port) as spawner:
        await eventually(lambda: not path.exists(), timeout=SWEEP_BOUND_S)
        listed_files.clear()
        path.write_bytes(b"the terminal recreated its month while the chart was closed")
        await asyncio.wait_for(listed_files.wait(), SWEEP_BOUND_S)
        clock[1] += 600
        assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.OpenChart(
            ChartRole.HISTORY, "EURUSD"
        )
        assert not path.exists()
        assert "202611 was written back during the wait; deleted again" in caplog.text
        ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
        try:
            path.write_bytes(b"downloaded by the fresh EA")
            await measured(ea)
            await verify_month(ea, empty=True)
            await eventually(lambda: (tmp_path / ".mt5-connector/resync.json").exists())
            assert stored(tmp_path)["months"] == {"202611": clock[0]}
            assert (await listed(port, ea)).claimable_through == epoch("2026-11-09T00:00:00")
            await no_chart_message(spawner)
        finally:
            await ea.close()


async def test_a_measurement_syncing_past_idle_grace_keeps_its_ea_for_the_reask(
    tmp_path, clock, run_server
):
    path = month_file(tmp_path)
    async with run_server(history_store=tmp_path) as port, verified(port) as spawner:
        await eventually(lambda: not path.exists(), timeout=SWEEP_BOUND_S)
        clock[1] += 600
        assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.OpenChart(
            ChartRole.HISTORY, "EURUSD"
        )
        ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
        try:
            request = await ea.request(timeout=SWEEP_BOUND_S)
            assert arguments_of(request) == (calls.Call.RANGES, {"symbol": "EURUSD"})
            clock[1] += 900
            await ea.send(
                binary.Answer(
                    request.request_id,
                    0,
                    True,
                    0,
                    error=binary.AnswerError(
                        binary.ErrorKind.SYNCING,
                        "EURUSD: the tick history is still syncing after 900 s",
                        5000,
                    ),
                )
            )
            await requests_finished(port)
            await no_chart_message(spawner)
            with pytest.raises(TimeoutError):
                await ea.request(timeout=0.1)
            clock[1] += 5
            path.write_bytes(b"downloaded by the same history EA")
            await measured(ea)
            await verify_month(ea, empty=True)
            await eventually(lambda: (tmp_path / ".mt5-connector/resync.json").exists())
            assert stored(tmp_path)["months"] == {"202611": clock[0]}
            clock[1] += 900
            assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.CloseChart(
                ChartRole.HISTORY, "EURUSD"
            )
        finally:
            await ea.close()


async def test_a_long_measurement_keeps_its_history_chart_through_verification(
    tmp_path, clock, run_server
):
    path = month_file(tmp_path)
    async with (
        run_server(history_store=tmp_path, chart_idle_seconds=60) as port,
        verified(port) as spawner,
    ):
        await eventually(lambda: not path.exists(), timeout=SWEEP_BOUND_S)
        clock[1] += 600
        assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.OpenChart(
            ChartRole.HISTORY, "EURUSD"
        )
        ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
        try:
            request = await ea.request(timeout=SWEEP_BOUND_S)
            assert arguments_of(request) == (calls.Call.RANGES, {"symbol": "EURUSD"})
            clock[1] += 180
            path.write_bytes(b"downloaded")
            await ea.send(
                answer(
                    request.request_id,
                    calls.Call.RANGES,
                    (
                        100_000,
                        [(TICK_TIMEFRAME, CLOCK.to_broker(FIRST_TICK))],
                        CLOCK.to_broker(FIRST_TICK),
                    ),
                )
            )
            await no_chart_message(spawner)
            await verify_month(ea, empty=True)
            await eventually(lambda: (tmp_path / ".mt5-connector/resync.json").exists())
            assert stored(tmp_path)["months"] == {"202611": clock[0]}
            assert (await listed(port, ea)).claimable_through == epoch("2026-11-09T00:00:00")
        finally:
            await ea.close()


async def test_a_pending_idle_close_is_not_the_resyncs_own_history_close(
    tmp_path, clock, run_server, caplog
):
    caplog.set_level(logging.INFO, logger="mt5connector.server.hub_resync")
    async with (
        run_server(history_store=tmp_path, chart_idle_seconds=60) as port,
        verified(port) as spawner,
    ):
        ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
        consumer = await Consumer.connect(port)
        try:
            await consumer.ask(1, calls.Call.RANGES, symbol="EURUSD", source=calls.Source.TERMINAL)
            request = await ea.request(timeout=SWEEP_BOUND_S)
            await ea.send(answer(request.request_id, calls.Call.RANGES, (100_000, [], 0)))
            assert (await consumer.received()).error is None
            clock[1] += 60
            assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.CloseChart(
                ChartRole.HISTORY, "EURUSD"
            )
            path = month_file(tmp_path)
            await eventually(lambda: "re-sync of EURUSD: due 202611" in caplog.text)
            assert path.exists()
            await chart_closed(ea, run_server)
            await eventually(lambda: not path.exists(), timeout=SWEEP_BOUND_S)
            await no_chart_message(spawner)
            clock[1] += 599
            await no_chart_message(spawner)
            clock[1] += 1
            assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.OpenChart(
                ChartRole.HISTORY, "EURUSD"
            )
        finally:
            await consumer.close()
            await ea.close()


async def deferred_measurement(path, clock, port, spawner):
    await eventually(lambda: not path.exists(), timeout=SWEEP_BOUND_S)
    clock[1] += 600
    assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.OpenChart(
        ChartRole.HISTORY, "EURUSD"
    )
    ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
    request = await ea.request(timeout=SWEEP_BOUND_S)
    assert arguments_of(request) == (calls.Call.RANGES, {"symbol": "EURUSD"})
    await ea.send(
        binary.Answer(
            request.request_id,
            0,
            True,
            0,
            error=binary.AnswerError(binary.ErrorKind.SYNCING, "still syncing", 5000),
        )
    )
    await requests_finished(port)
    await no_chart_message(spawner)
    return ea


async def test_an_ea_gone_between_measurement_asks_fails_and_restarts_the_attempt(
    tmp_path, clock, run_server, caplog
):
    caplog.set_level(logging.INFO, logger="mt5connector.server.hub_resync")
    path = month_file(tmp_path)
    async with run_server(history_store=tmp_path) as port, verified(port) as spawner:
        ea = await deferred_measurement(path, clock, port, spawner)
        try:
            path.write_bytes(b"partly restored before the EA left")
            await chart_closed(ea, run_server)
            assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.OpenChart(
                ChartRole.HISTORY, "EURUSD"
            )
            ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
            assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.CloseChart(
                ChartRole.HISTORY, "EURUSD"
            )
            assert "attempt 1 of 3 failed" in caplog.text
            assert "due 202611 (attempt 2 of 3)" in caplog.text
            clock[1] += 50
            await chart_closed(ea, run_server)
            await eventually(lambda: not path.exists(), timeout=SWEEP_BOUND_S)
            clock[1] += 599
            await no_chart_message(spawner)
            clock[1] += 1
            assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.OpenChart(
                ChartRole.HISTORY, "EURUSD"
            )
            ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
            path.write_bytes(b"downloaded by the next attempt")
            await measured(ea)
            await verify_month(ea, empty=True)
            await eventually(lambda: (tmp_path / ".mt5-connector/resync.json").exists())
            assert stored(tmp_path)["outcome"] == "downloaded"
        finally:
            await ea.close()


async def test_failed_reopens_after_a_deferred_measurements_ea_leaves_end_the_resync(
    tmp_path, clock, run_server, caplog
):
    caplog.set_level(logging.INFO, logger="mt5connector.server.hub_resync")
    path = month_file(tmp_path)
    async with run_server(history_store=tmp_path) as port, verified(port) as spawner:
        ea = await deferred_measurement(path, clock, port, spawner)
        await chart_closed(ea, run_server)
        assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.OpenChart(
            ChartRole.HISTORY, "EURUSD"
        )
        await spawner.send(binary.ChartFailed(ChartRole.HISTORY, "EURUSD", 4102))
        await no_chart_message(spawner)
        clock[1] += 9
        await no_chart_message(spawner)
        clock[1] += 1
        assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.OpenChart(
            ChartRole.HISTORY, "EURUSD"
        )
        await spawner.send(binary.ChartFailed(ChartRole.HISTORY, "EURUSD", 4102))
        await eventually(
            lambda: (tmp_path / ".mt5-connector/resync.json").exists()
            and stored(tmp_path)["outcome"] == "failed",
            timeout=SWEEP_BOUND_S,
        )
        assert "attempt 1 of 3 failed" in caplog.text
        assert "attempt 2 of 3 failed" in caplog.text
        assert "attempt 3 of 3 failed" in caplog.text
        consumer = await Consumer.connect(port)
        try:
            await consumer.ask(1, calls.Call.RANGES, symbol="EURUSD")
            response = await consumer.received()
            assert response.error.kind is binary.ErrorKind.EA_ERROR
            assert "4102" in response.error.message
        finally:
            await consumer.close()


@pytest.mark.parametrize("call", [calls.Call.RANGES, calls.Call.COPY_RATES_RANGE])
async def test_a_chart_failure_during_a_pending_pass_read_fails_its_attempt(
    tmp_path, clock, run_server, caplog, call
):
    caplog.set_level(logging.INFO, logger="mt5connector.server.hub_resync")
    path = month_file(tmp_path)
    async with run_server(history_store=tmp_path) as port, verified(port) as spawner:
        await eventually(lambda: not path.exists(), timeout=SWEEP_BOUND_S)
        clock[1] += 600
        assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.OpenChart(
            ChartRole.HISTORY, "EURUSD"
        )
        ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
        try:
            path.write_bytes(b"downloaded")
            if call is calls.Call.COPY_RATES_RANGE:
                await measured(ea)
            request = await ea.request(timeout=SWEEP_BOUND_S)
            assert arguments_of(request)[0] is call
            await spawner.send(binary.ChartFailed(ChartRole.HISTORY, "EURUSD", 4102))
            assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.CloseChart(
                ChartRole.HISTORY, "EURUSD"
            )
            if call is calls.Call.RANGES:
                result = (100_000, [], CLOCK.to_broker(FIRST_TICK))
            else:
                result = np.zeros(0, dtype=np.dtype(list(mirror.RATES.dtype)))
            await ea.send(answer(request.request_id, call, result))
            await chart_closed(ea, run_server)
            await eventually(lambda: not path.exists(), timeout=SWEEP_BOUND_S)
            clock[1] += 600
            assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.OpenChart(
                ChartRole.HISTORY, "EURUSD"
            )
            ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
            path.write_bytes(b"downloaded on attempt two")
            await measured(ea)
            await verify_month(ea, empty=True)
            await eventually(lambda: (tmp_path / ".mt5-connector/resync.json").exists())
            assert stored(tmp_path)["outcome"] == "downloaded"
            assert caplog.text.count("attempt 1 of 3 failed") == 1
            assert "attempt 2 of 3 failed" not in caplog.text
        finally:
            await ea.close()


@pytest.mark.parametrize("rehello", [False, True], ids=["idle-close", "idle-drop-and-hello"])
@pytest.mark.parametrize("other_use", [False, True], ids=["resync-only", "recent-terminal-read"])
async def test_an_idle_close_pending_at_dispatch_spends_no_attempt(
    tmp_path, clock, run_server, caplog, rehello, other_use
):
    caplog.set_level(logging.INFO, logger="mt5connector.server.hub_resync")
    async with (
        run_server(history_store=tmp_path, chart_idle_seconds=60) as port,
        verified(port) as spawner,
    ):
        ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
        consumer = await Consumer.connect(port)
        try:
            await consumer.ask(1, calls.Call.RANGES, symbol="EURUSD", source=calls.Source.TERMINAL)
            request = await ea.request(timeout=SWEEP_BOUND_S)
            await ea.send(answer(request.request_id, calls.Call.RANGES, (100_000, [], 0)))
            assert (await consumer.received()).error is None
            clock[1] += 60
            assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.CloseChart(
                ChartRole.HISTORY, "EURUSD"
            )
            path = month_file(tmp_path)
            await eventually(lambda: "due 202611 (attempt 1 of 3)" in caplog.text)
            if other_use:
                await consumer.ask(
                    2, calls.Call.RANGES, symbol="EURUSD", source=calls.Source.TERMINAL
                )
                await ea.request(timeout=SWEEP_BOUND_S)
            await chart_closed(ea, run_server)
            if other_use:
                assert (await consumer.received()).error.kind is binary.ErrorKind.EA_GONE
                assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.OpenChart(
                    ChartRole.HISTORY, "EURUSD"
                )
                if rehello:
                    ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
                    await chart_closed(ea, run_server)
                    assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.OpenChart(
                        ChartRole.HISTORY, "EURUSD"
                    )
                ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
                clock[1] += 30
                assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.CloseChart(
                    ChartRole.HISTORY, "EURUSD"
                )
                await chart_closed(ea, run_server)
            else:
                if rehello:
                    ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
                    await chart_closed(ea, run_server)
                await no_chart_message(spawner)
            await eventually(lambda: not path.exists(), timeout=SWEEP_BOUND_S)
            assert "released 202611 (attempt 1 of 3)" in caplog.text
            assert "attempt 1 of 3 failed" not in caplog.text
            assert "attempt 2 of 3 failed" not in caplog.text
            clock[1] += 600
            assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.OpenChart(
                ChartRole.HISTORY, "EURUSD"
            )
            ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
            path.write_bytes(b"downloaded on the first attempt")
            await measured(ea)
            await verify_month(ea, empty=True)
            await eventually(lambda: (tmp_path / ".mt5-connector/resync.json").exists())
            assert stored(tmp_path)["outcome"] == "downloaded"
        finally:
            await consumer.close()
            await ea.close()


async def test_an_own_close_stays_pending_across_a_sweep_and_rehello(
    tmp_path, clock, run_server, caplog
):
    caplog.set_level(logging.INFO, logger="mt5connector.server.hub_resync")
    path = month_file(tmp_path)
    async with run_server(history_store=tmp_path) as port, verified(port) as spawner:
        await eventually(lambda: not path.exists(), timeout=SWEEP_BOUND_S)
        clock[1] += 600
        assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.OpenChart(
            ChartRole.HISTORY, "EURUSD"
        )
        ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
        try:
            path.write_bytes(b"restored before a failed measurement")
            request = await ea.request(timeout=SWEEP_BOUND_S)
            await ea.send(
                binary.Answer(
                    request.request_id,
                    0,
                    True,
                    0,
                    error=binary.AnswerError(binary.ErrorKind.EA_ERROR, "measurement failed"),
                )
            )
            assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.CloseChart(
                ChartRole.HISTORY, "EURUSD"
            )
            await chart_closed(ea, run_server)
            await eventually(
                lambda: "waiting 202611 (attempt 2 of 3)" in caplog.text and not path.exists(),
                timeout=SWEEP_BOUND_S,
            )
            clock[1] += 1
            ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
            await chart_closed(ea, run_server)
            await no_chart_message(spawner)
            assert "attempt 2 of 3 failed" not in caplog.text
            clock[1] += 599
            await no_chart_message(spawner)
            clock[1] += 1
            assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.OpenChart(
                ChartRole.HISTORY, "EURUSD"
            )
            ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
            path.write_bytes(b"restored after the full wait")
            await measured(ea)
            await verify_month(ea, empty=True)
            await eventually(lambda: (tmp_path / ".mt5-connector/resync.json").exists())
            assert stored(tmp_path)["outcome"] == "downloaded"
            assert caplog.text.count("attempt 1 of 3 failed") == 1
            assert "attempt 2 of 3 failed" not in caplog.text
        finally:
            await ea.close()


@pytest.mark.parametrize("pending", [False, True], ids=["completed", "running"])
@pytest.mark.parametrize("old_error", [False, True], ids=["old-result", "old-error"])
async def test_a_failed_attempt_retires_its_deletion_before_a_fresh_delete(
    tmp_path, clock, run_server, monkeypatch, caplog, pending, old_error
):
    caplog.set_level(logging.INFO, logger="mt5connector.server.hub_resync")
    path = month_file(tmp_path)
    first_started = threading.Event()
    finish_old = threading.Event()
    old_done = threading.Event()
    listing_paused = asyncio.Event()
    resume_listing = asyncio.Event()
    fresh_delete = asyncio.Event()
    deletions = []
    listing = TickStore.listing
    delete = TickStore.delete

    async def pause_listing(store):
        result = await listing(store)
        if first_started.is_set() and not resume_listing.is_set():
            listing_paused.set()
            await resume_listing.wait()
        return result

    async def controlled_delete(store, symbol, months):
        if symbol != "EURUSD":
            await delete(store, symbol, months)
            return
        deletions.append((symbol, tuple(months)))
        if len(deletions) == 1:

            def work():
                first_started.set()
                try:
                    finish_old.wait()
                    if old_error:
                        raise OSError("the old attempt's deletion failed")
                finally:
                    old_done.set()

            await asyncio.to_thread(work)
        else:
            fresh_delete.set()
            await delete(store, symbol, months)

    monkeypatch.setattr(TickStore, "listing", pause_listing)
    monkeypatch.setattr(TickStore, "delete", controlled_delete)
    async with run_server(history_store=tmp_path) as port, verified(port) as spawner:
        try:
            await eventually(first_started.is_set, timeout=SWEEP_BOUND_S)
            await asyncio.wait_for(listing_paused.wait(), SWEEP_BOUND_S)
            if not pending:
                finish_old.set()
                await eventually(old_done.is_set, timeout=SWEEP_BOUND_S)
            await spawner.send(binary.ChartFailed(ChartRole.HISTORY, "EURUSD", 4102))
            await eventually(lambda: "attempt 1 of 3 failed" in caplog.text)
            resume_listing.set()
            if pending:
                other = month_file(tmp_path, symbol="AUDCAD")
                await eventually(lambda: not other.exists(), timeout=SWEEP_BOUND_S)
                assert not old_done.is_set()
                assert not fresh_delete.is_set()
                finish_old.set()
            await asyncio.wait_for(fresh_delete.wait(), SWEEP_BOUND_S)
            assert old_done.is_set()
            assert deletions == [("EURUSD", ("202611",)), ("EURUSD", ("202611",))]
            await eventually(lambda: not path.exists(), timeout=SWEEP_BOUND_S)
            assert "attempt 2 of 3 failed" not in caplog.text
            clock[1] += 600
            if pending:
                opened = [await spawner.received(timeout=SWEEP_BOUND_S) for _ in range(2)]
                assert set(opened) == {
                    binary.OpenChart(ChartRole.HISTORY, "EURUSD"),
                    binary.OpenChart(ChartRole.HISTORY, "AUDCAD"),
                }
            else:
                assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.OpenChart(
                    ChartRole.HISTORY, "EURUSD"
                )
            ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
            try:
                path.write_bytes(b"downloaded after a fresh deletion")
                await measured(ea)
                await verify_month(ea, empty=True)
                await eventually(lambda: (tmp_path / ".mt5-connector/resync.json").exists())
                assert stored(tmp_path)["outcome"] == "downloaded"
                assert caplog.text.count("attempt 1 of 3 failed") == 1
                assert "attempt 2 of 3 failed" not in caplog.text
            finally:
                await ea.close()
        finally:
            finish_old.set()
            resume_listing.set()


async def failed_measurements_own_close(path, clock, port, spawner):
    await eventually(lambda: not path.exists(), timeout=SWEEP_BOUND_S)
    clock[1] += 600
    assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.OpenChart(
        ChartRole.HISTORY, "EURUSD"
    )
    ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
    path.write_bytes(b"restored before a failed measurement")
    request = await ea.request(timeout=SWEEP_BOUND_S)
    await ea.send(
        binary.Answer(
            request.request_id,
            0,
            True,
            0,
            error=binary.AnswerError(binary.ErrorKind.EA_ERROR, "measurement failed"),
        )
    )
    assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.CloseChart(
        ChartRole.HISTORY, "EURUSD"
    )
    return ea


async def test_a_rehello_that_stays_has_its_own_close_renewed_before_reopening(
    tmp_path, clock, run_server, caplog
):
    caplog.set_level(logging.INFO, logger="mt5connector.server.hub_resync")
    path = month_file(tmp_path)
    async with run_server(history_store=tmp_path) as port, verified(port) as spawner:
        ea = await failed_measurements_own_close(path, clock, port, spawner)
        try:
            await chart_closed(ea, run_server)
            await eventually(lambda: "waiting 202611 (attempt 2 of 3)" in caplog.text)
            ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
            path.write_bytes(b"written back while the closing EA stays")
            clock[1] += 29
            await no_chart_message(spawner)
            clock[1] += 1
            assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.CloseChart(
                ChartRole.HISTORY, "EURUSD"
            )
            clock[1] += 40
            await chart_closed(ea, run_server)
            await eventually(lambda: not path.exists(), timeout=SWEEP_BOUND_S)
            clock[1] += 599
            await no_chart_message(spawner)
            clock[1] += 1
            assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.OpenChart(
                ChartRole.HISTORY, "EURUSD"
            )
            ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
            path.write_bytes(b"restored after the renewed close")
            await measured(ea)
            await verify_month(ea, empty=True)
            await eventually(lambda: (tmp_path / ".mt5-connector/resync.json").exists())
            assert stored(tmp_path)["outcome"] == "downloaded"
            assert "attempt 2 of 3 failed" not in caplog.text
        finally:
            await ea.close()


async def test_an_ended_resync_restores_normal_chart_re_requests_and_idle_closes(
    tmp_path, clock, run_server, monkeypatch, caplog
):
    caplog.set_level(logging.INFO, logger="mt5connector.server.hub_resync")
    path = month_file(tmp_path)
    delete = TickStore.delete
    deletions = []

    async def failing_delete(store, symbol, months):
        deletions.append(months)
        if len(deletions) == 1:
            await delete(store, symbol, months)
        else:
            raise OSError("the store refused deletion")

    monkeypatch.setattr(TickStore, "delete", failing_delete)
    async with (
        run_server(history_store=tmp_path, chart_idle_seconds=60) as port,
        verified(port) as spawner,
    ):
        ea = await failed_measurements_own_close(path, clock, port, spawner)
        consumer = await Consumer.connect(port)
        try:
            await chart_closed(ea, run_server)
            await eventually(
                lambda: (tmp_path / ".mt5-connector/resync.json").exists()
                and stored(tmp_path)["outcome"] == "failed",
                timeout=SWEEP_BOUND_S,
            )
            await consumer.ask(1, calls.Call.RANGES, symbol="EURUSD", source=calls.Source.TERMINAL)
            assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.OpenChart(
                ChartRole.HISTORY, "EURUSD"
            )
            await consumer.received()
            ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
            await consumer.ask(2, calls.Call.RANGES, symbol="EURUSD", source=calls.Source.TERMINAL)
            request = await ea.request(timeout=SWEEP_BOUND_S)
            await ea.send(answer(request.request_id, calls.Call.RANGES, (100_000, [], 0)))
            assert (await consumer.received()).error is None
            await chart_closed(ea, run_server)
            assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.OpenChart(
                ChartRole.HISTORY, "EURUSD"
            )
            ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
            await consumer.ask(3, calls.Call.RANGES, symbol="EURUSD", source=calls.Source.TERMINAL)
            request = await ea.request(timeout=SWEEP_BOUND_S)
            await ea.send(answer(request.request_id, calls.Call.RANGES, (100_000, [], 0)))
            assert (await consumer.received()).error is None
            clock[1] += 120
            assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.CloseChart(
                ChartRole.HISTORY, "EURUSD"
            )
        finally:
            await consumer.close()
            await ea.close()


async def test_an_own_close_survives_a_spawner_reconnect_between_its_drops(
    tmp_path, clock, run_server, caplog
):
    caplog.set_level(logging.INFO, logger="mt5connector.server.hub_resync")
    path = month_file(tmp_path)
    async with run_server(history_store=tmp_path) as port, verified(port) as spawner:
        ea = await failed_measurements_own_close(path, clock, port, spawner)
        try:
            await chart_closed(ea, run_server)
            await eventually(lambda: "waiting 202611 (attempt 2 of 3)" in caplog.text)
            await chart_closed(spawner, run_server)
            async with verified(port) as replacement:
                clock[1] += 1
                ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
                await chart_closed(ea, run_server)
                await no_chart_message(replacement)
                assert "attempt 2 of 3 failed" not in caplog.text
                clock[1] += 599
                await no_chart_message(replacement)
                clock[1] += 1
                assert await replacement.received(timeout=SWEEP_BOUND_S) == binary.OpenChart(
                    ChartRole.HISTORY, "EURUSD"
                )
                ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
                path.write_bytes(b"restored after a spawner reconnect")
                await measured(ea)
                await verify_month(ea, empty=True)
                await eventually(lambda: (tmp_path / ".mt5-connector/resync.json").exists())
                assert stored(tmp_path)["outcome"] == "downloaded"
                assert "attempt 2 of 3 failed" not in caplog.text
        finally:
            await ea.close()


async def test_a_terminal_read_stays_gated_after_a_deleted_months_closing_rehello(
    tmp_path, clock, run_server, caplog
):
    caplog.set_level(logging.INFO, logger="mt5connector.server.hub_resync")
    path = month_file(tmp_path)
    async with run_server(history_store=tmp_path) as port, verified(port) as spawner:
        ea = await failed_measurements_own_close(path, clock, port, spawner)
        consumer = await Consumer.connect(port)
        try:
            await chart_closed(ea, run_server)
            await eventually(lambda: "waiting 202611 (attempt 2 of 3)" in caplog.text)
            ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
            await eventually(lambda: "its closing history EA returned" in caplog.text)
            assert not path.exists()
            routed = []

            async def answer_if_routed():
                request = await ea.request(timeout=SWEEP_BOUND_S)
                routed.append(request)
                await ea.send(answer(request.request_id, calls.Call.RANGES, (100_000, [], 0)))

            answering = asyncio.create_task(answer_if_routed())
            try:
                await consumer.ask(
                    7, calls.Call.RANGES, symbol="EURUSD", source=calls.Source.TERMINAL
                )
                response = await consumer.received(timeout=SWEEP_BOUND_S)
            finally:
                answering.cancel()
                await asyncio.gather(answering, return_exceptions=True)
            assert response.error is not None
            assert response.error.kind is binary.ErrorKind.SYNCING
            assert not routed
            clock[1] += 30
            assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.CloseChart(
                ChartRole.HISTORY, "EURUSD"
            )
            await chart_closed(ea, run_server)
            clock[1] += 600
            assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.OpenChart(
                ChartRole.HISTORY, "EURUSD"
            )
            ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
            path.write_bytes(b"restored and confirmed by measurement")
            await measured(ea)
            await verify_month(ea, empty=True)
            await eventually(lambda: (tmp_path / ".mt5-connector/resync.json").exists())
            assert stored(tmp_path)["outcome"] == "downloaded"
            assert (await listed(port, ea)).resync_failure == ""
        finally:
            await consumer.close()
            await ea.close()


@pytest.mark.parametrize("retired", [False, True], ids=["active", "retiring"])
async def test_shutdown_cancels_deletion_waiters_without_waiting_for_their_threads(
    tmp_path, run_server, monkeypatch, caplog, retired
):
    caplog.set_level(logging.INFO, logger="mt5connector.server.hub_resync")
    month_file(tmp_path)
    started = threading.Event()
    release = threading.Event()
    finished = threading.Event()
    cancelled = asyncio.Event()

    async def held_delete(store, symbol, months):
        def work():
            started.set()
            try:
                release.wait()
            finally:
                finished.set()

        try:
            await asyncio.to_thread(work)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    monkeypatch.setattr(TickStore, "delete", held_delete)
    try:
        async with run_server(history_store=tmp_path) as port, verified(port) as spawner:
            await eventually(started.is_set, timeout=SWEEP_BOUND_S)
            if retired:
                await spawner.send(binary.ChartFailed(ChartRole.HISTORY, "EURUSD", 4102))
                await eventually(lambda: "attempt 1 of 3 failed" in caplog.text)
        await asyncio.wait_for(cancelled.wait(), SWEEP_BOUND_S)
        assert not finished.is_set()
    finally:
        release.set()
        await eventually(finished.is_set, timeout=SWEEP_BOUND_S)


async def test_a_hanging_deletion_and_its_retirement_end_failed_after_three_history_bounds(
    tmp_path, clock, run_server, monkeypatch, caplog
):
    caplog.set_level(logging.INFO, logger="mt5connector.server.hub_resync")
    month_file(tmp_path)
    other = month_file(tmp_path, symbol="AUDCAD")
    started = threading.Event()
    release = threading.Event()
    finished = threading.Event()
    workers = []
    delete = TickStore.delete

    async def held_delete(store, symbol, months):
        if symbol != "EURUSD":
            await delete(store, symbol, months)
            return
        workers.append(symbol)

        def work():
            started.set()
            try:
                release.wait()
            finally:
                finished.set()

        await asyncio.to_thread(work)

    monkeypatch.setattr(TickStore, "delete", held_delete)
    async with run_server(history_store=tmp_path) as port, verified(port) as spawner:
        consumer = await Consumer.connect(port)
        try:
            await eventually(started.is_set, timeout=SWEEP_BOUND_S)
            await eventually(lambda: not other.exists(), timeout=SWEEP_BOUND_S)
            for attempt in (1, 2, 3):
                await eventually(
                    lambda attempt=attempt: f"released 202611 (attempt {attempt} of 3)"
                    in caplog.text,
                    timeout=SWEEP_BOUND_S,
                )
                clock[1] += 899
                if attempt == 1:
                    assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.OpenChart(
                        ChartRole.HISTORY, "AUDCAD"
                    )
                else:
                    await no_chart_message(spawner)
                assert f"attempt {attempt} of 3 failed" not in caplog.text
                await consumer.ask(attempt, calls.Call.RANGES, symbol="EURUSD")
                response = await consumer.received()
                assert response.error.kind is binary.ErrorKind.SYNCING
                assert f"attempt {attempt} of 3" in response.error.message
                clock[1] += 1
                await eventually(
                    lambda attempt=attempt: f"attempt {attempt} of 3 failed" in caplog.text,
                    timeout=SWEEP_BOUND_S,
                )
            await eventually(lambda: (tmp_path / ".mt5-connector/resync.json").exists())
            assert stored(tmp_path)["outcome"] == "failed"
            assert "900" in stored(tmp_path)["reason"]
            assert workers == ["EURUSD"]
            assert not finished.is_set()
            await consumer.ask(100, calls.Call.RANGES, symbol="EURUSD")
            assert (await consumer.received()).error.kind is binary.ErrorKind.NO_PUBLISHER
            assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.OpenChart(
                ChartRole.HISTORY, "EURUSD"
            )
            ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
            try:
                ranges = await listed(port, ea)
                assert "202611" in ranges.resync_failure
                assert "900" in ranges.resync_failure
            finally:
                await ea.close()
        finally:
            release.set()
            await eventually(finished.is_set, timeout=SWEEP_BOUND_S)
            await consumer.close()


async def test_a_deletion_completed_after_its_history_bound_still_fails_the_attempt(
    tmp_path, clock, run_server, monkeypatch, caplog
):
    caplog.set_level(logging.INFO, logger="mt5connector.server.hub_resync")
    path = month_file(tmp_path)
    started = threading.Event()
    release = threading.Event()
    completed = asyncio.Event()
    listing_paused = asyncio.Event()
    resume_listing = asyncio.Event()
    deletions = []
    listing = TickStore.listing
    delete = TickStore.delete

    async def pause_listing(store):
        result = await listing(store)
        if started.is_set() and not resume_listing.is_set():
            listing_paused.set()
            await resume_listing.wait()
        return result

    async def delayed_delete(store, symbol, months):
        deletions.append(months)
        if len(deletions) == 1:

            def work():
                started.set()
                release.wait()

            await asyncio.to_thread(work)
            completed.set()
        else:
            await delete(store, symbol, months)

    monkeypatch.setattr(TickStore, "listing", pause_listing)
    monkeypatch.setattr(TickStore, "delete", delayed_delete)
    async with run_server(history_store=tmp_path) as port, verified(port) as spawner:
        try:
            await eventually(started.is_set, timeout=SWEEP_BOUND_S)
            await asyncio.wait_for(listing_paused.wait(), SWEEP_BOUND_S)
            clock[1] += 901
            release.set()
            await asyncio.wait_for(completed.wait(), SWEEP_BOUND_S)
            resume_listing.set()
            await eventually(lambda: "attempt 1 of 3 failed" in caplog.text)
            assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.OpenChart(
                ChartRole.HISTORY, "EURUSD"
            )
            assert len(deletions) == 2
            ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
            try:
                path.write_bytes(b"restored after the late deletion failed")
                await measured(ea)
                await verify_month(ea, empty=True)
                await eventually(lambda: (tmp_path / ".mt5-connector/resync.json").exists())
                assert stored(tmp_path)["outcome"] == "downloaded"
                assert "attempt 2 of 3 failed" not in caplog.text
            finally:
                await ea.close()
        finally:
            release.set()
            resume_listing.set()


async def test_a_deletion_deadline_survives_a_closing_rehello_back_to_due(
    tmp_path, clock, run_server, monkeypatch, caplog
):
    caplog.set_level(logging.INFO, logger="mt5connector.server.hub_resync")
    path = month_file(tmp_path)
    started = threading.Event()
    release = threading.Event()
    finished = threading.Event()
    deletions = []
    delete = TickStore.delete

    async def held_second_delete(store, symbol, months):
        deletions.append(months)
        if len(deletions) == 1:
            await delete(store, symbol, months)
        else:

            def work():
                started.set()
                try:
                    release.wait()
                finally:
                    finished.set()

            await asyncio.to_thread(work)

    monkeypatch.setattr(TickStore, "delete", held_second_delete)
    async with run_server(history_store=tmp_path) as port, verified(port) as spawner:
        ea = await failed_measurements_own_close(path, clock, port, spawner)
        consumer = await Consumer.connect(port)
        try:
            await chart_closed(ea, run_server)
            await eventually(started.is_set, timeout=SWEEP_BOUND_S)
            ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
            await eventually(lambda: "its closing history EA returned" in caplog.text)
            for attempt in (2, 3):
                clock[1] += 899
                assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.CloseChart(
                    ChartRole.HISTORY, "EURUSD"
                )
                assert f"attempt {attempt} of 3 failed" not in caplog.text
                await consumer.ask(attempt, calls.Call.RANGES, symbol="EURUSD")
                response = await consumer.received()
                assert response.error.kind is binary.ErrorKind.SYNCING
                assert f"attempt {attempt} of 3" in response.error.message
                clock[1] += 1
                await eventually(
                    lambda attempt=attempt: f"attempt {attempt} of 3 failed" in caplog.text,
                    timeout=SWEEP_BOUND_S,
                )
            await eventually(
                lambda: (tmp_path / ".mt5-connector/resync.json").exists()
                and stored(tmp_path)["outcome"] == "failed"
            )
            assert len(deletions) == 2
            assert not finished.is_set()
            assert "900" in (await listed(port, ea)).resync_failure
        finally:
            release.set()
            await eventually(finished.is_set, timeout=SWEEP_BOUND_S)
            await consumer.close()
            await ea.close()


async def test_a_new_daily_dispatch_stays_gated_while_an_old_deletion_thread_drains(
    tmp_path, clock, run_server, monkeypatch, caplog
):
    caplog.set_level(logging.INFO, logger="mt5connector.server.hub_resync")
    month_file(tmp_path)
    started = threading.Event()
    release = threading.Event()
    finished = threading.Event()
    workers = []

    async def held_delete(store, symbol, months):
        workers.append(symbol)

        def work():
            started.set()
            try:
                release.wait()
            finally:
                finished.set()

        await asyncio.to_thread(work)

    monkeypatch.setattr(TickStore, "delete", held_delete)
    async with run_server(history_store=tmp_path) as port, verified(port) as spawner:
        consumer = await Consumer.connect(port)
        ea = None
        try:
            await eventually(started.is_set, timeout=SWEEP_BOUND_S)
            for attempt in (1, 2, 3):
                clock[1] += 900
                await eventually(
                    lambda attempt=attempt: f"attempt {attempt} of 3 failed" in caplog.text,
                    timeout=SWEEP_BOUND_S,
                )
            await eventually(
                lambda: (tmp_path / ".mt5-connector/resync.json").exists()
                and stored(tmp_path)["outcome"] == "failed"
            )
            ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
            assert "900" in (await listed(port, ea)).resync_failure
            clock[0] += 86_400
            assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.CloseChart(
                ChartRole.HISTORY, "EURUSD"
            )
            assert caplog.text.count("due 202611 (attempt 1 of 3)") == 2
            assert workers == ["EURUSD"]
            assert not finished.is_set()
            routed = []

            async def answer_if_routed():
                request = await ea.request(timeout=SWEEP_BOUND_S)
                routed.append(request)
                await ea.send(answer(request.request_id, calls.Call.RANGES, (100_000, [], 0)))

            answering = asyncio.create_task(answer_if_routed())
            try:
                await consumer.ask(
                    7, calls.Call.RANGES, symbol="EURUSD", source=calls.Source.TERMINAL
                )
                response = await consumer.received(timeout=SWEEP_BOUND_S)
            finally:
                answering.cancel()
                await asyncio.gather(answering, return_exceptions=True)
            assert response.error is not None
            assert response.error.kind is binary.ErrorKind.SYNCING
            assert not routed
            assert not finished.is_set()
        finally:
            release.set()
            await eventually(finished.is_set, timeout=SWEEP_BOUND_S)
            await consumer.close()
            if ea is not None:
                await ea.close()
