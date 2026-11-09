"""Scheduled downloads observed through the hub's entry point, peers and history volume."""

import asyncio
import json
import socket
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from types import SimpleNamespace

import numpy as np
import pytest
from ea_wire import FakeEa, answer, arguments_of
from hub_doubles import Consumer, eventually, health, verified
from mirror_samples import CLOCK

from mt5connector.server import ws_server
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
    async def run(*, history_store, chart_idle_seconds=900, resync_idle_seconds=600):
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
            startup.setenv("MT5_HUB_PORT", str(port))
            startup.setenv("MT5_HISTORY_STORE", str(history_store))
            startup.setenv("MT5_CHART_IDLE_SECONDS", str(chart_idle_seconds))
            startup.setenv("MT5_RESYNC_IDLE_SECONDS", str(resync_idle_seconds))
            startup.setenv("MT5_CLOCK_SAMPLE_MAX_AGE_SECONDS", "1")
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


def month_file(store, month="202611"):
    path = store / "broker" / "ticks" / "EURUSD" / f"{month}.tkc"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"recorded live")
    return path


def record(store, months, *, outcome="downloaded", month="", reason=""):
    path = store / ".mt5-connector" / "resync.json"
    path.parent.mkdir(exist_ok=True)
    path.write_text(
        json.dumps(
            {"EURUSD": {"months": months, "outcome": outcome, "month": month, "reason": reason}}
        )
    )


def stored(store):
    return json.loads((store / ".mt5-connector" / "resync.json").read_text())["EURUSD"]


async def no_chart_message(spawner):
    with pytest.raises(TimeoutError):
        await spawner.received(timeout=SWEEP_BOUND_S)


async def requests_finished(port):
    await eventually(lambda: health(port)[1]["result"]["in_flight"] == 0)


async def measured(ea, *, first_tick=FIRST_TICK):
    request = await ea.request(timeout=SWEEP_BOUND_S)
    assert arguments_of(request) == (calls.Call.RANGES, {"symbol": "EURUSD"})
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
        run_server(history_store=tmp_path, chart_idle_seconds=10, resync_idle_seconds=20) as port,
        verified(port) as spawner,
    ):
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
            await ea.close()
            clock[1] += 20
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
        await ea.close()
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


async def test_a_recent_or_served_history_chart_waits_for_the_full_idle_margin(
    tmp_path, clock, run_server
):
    path = month_file(tmp_path)
    async with (
        run_server(history_store=tmp_path, chart_idle_seconds=10, resync_idle_seconds=20) as port,
        verified(port) as spawner,
    ):
        ea = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
        consumer = await Consumer.connect(port)
        await consumer.ask(1, calls.Call.RANGES, symbol="EURUSD")
        request = await ea.request()
        clock[1] += 100
        await no_chart_message(spawner)
        assert path.exists()
        await ea.send(answer(request.request_id, calls.Call.RANGES, (100_000, [], 0)))
        await consumer.received()
        await consumer.ask(2, calls.Call.RANGES, symbol="EURUSD")
        await measured(ea)
        await consumer.received()
        await consumer.close()
        clock[1] += 10
        assert await spawner.received(timeout=SWEEP_BOUND_S) == binary.CloseChart(
            ChartRole.HISTORY, "EURUSD"
        )
        await ea.close()
        clock[1] += 19
        await no_chart_message(spawner)
        assert path.exists()
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
        await ea.close()
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
