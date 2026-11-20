"""Per-call busy and a paused terminal on a fresh owned history volume."""

import asyncio
import json
import os
import re
import time

import pytest
from certification import until
from test_ea_scheduling import observed as observed
from test_hub_certification import symbols_for
from test_hub_certification import throwaway as throwaway
from test_hub_live import SYMBOL, synced

from mt5connector.client.errors import EaGone, HubTimeout
from mt5connector.wire import binary, calls
from mt5connector.wire.push_wire import PING_INTERVAL_S, PING_TIMEOUT_INTERVALS

pytestmark = pytest.mark.skipif(SYMBOL is None, reason="MT5_LIVE_HUB_SYMBOL is not set")

CALL_BUSY_S = 60
QUEUE_BUSY_S = 15
WAIT_BUSY_S = 16
PAUSE_S = 120


async def cold_symbol(session, control, symbol=None):
    if symbol is None:
        symbol = (await symbols_for(session, 1, control))[0]
    else:
        assert symbol in await symbols_for(session, None, control)
    saved = json.loads(
        await control.run(
            "exec",
            control.container,
            "python3",
            "-c",
            "import json,os,sys; "
            "from mt5connector.server.settings import read_hub_settings; "
            "root=read_hub_settings(os.environ).history_store; symbol=sys.argv[1]; "
            "print(json.dumps({'ticks':list(map(str,root.glob('*/ticks/'+symbol+'/*.tkc'))),"
            "'bars':list(map(str,root.glob('*/history/'+symbol+'/*')))}))",
            symbol,
        )
    )
    assert not saved["ticks"] and not saved["bars"], (
        "the per-call busy case requires a symbol never read on its fresh volume",
        saved,
    )
    session.received.frames.clear()
    session.received.handed.clear()
    return symbol


def busy_records(session):
    return [
        (at, frame.request_id, frame.bound_s)
        for at, frame in zip(session.received.handed, session.received.frames, strict=True)
        if isinstance(frame, binary.Busy)
    ]


async def test_a_fresh_volumes_cold_ranges_survives_under_per_call_busy(observed, throwaway):
    symbol = await cold_symbol(observed, throwaway)
    since = time.time()
    answer = await synced(observed, calls.Call.RANGES, symbol=symbol)
    assert answer.result.first_tick > 0
    records = busy_records(observed)
    print(json.dumps({"symbol": symbol, "hub_busy_frames": records}))
    bounds = {bound for _at, _request, bound in records}
    assert {QUEUE_BUSY_S, CALL_BUSY_S} <= bounds <= {QUEUE_BUSY_S, CALL_BUSY_S, WAIT_BUSY_S}
    assert "pong is overdue" not in await throwaway.logs(since)


async def test_a_cold_measurement_polling_over_seventy_seconds_keeps_its_request(
    observed, throwaway
):
    symbol = os.environ.get("MT5_CERT_LONG_MEASUREMENT_SYMBOL")
    if symbol is None:
        pytest.skip("MT5_CERT_LONG_MEASUREMENT_SYMBOL names the prepared long-backfill symbol")
    await cold_symbol(observed, throwaway, symbol)
    answer = await synced(observed, calls.Call.RANGES, symbol=symbol)
    assert answer.result.first_tick > 0
    start = None
    request = None
    longest = 0
    records = busy_records(observed)
    for at, request_id, bound in records:
        if bound == WAIT_BUSY_S:
            if start is None or request_id != request:
                start = at
                request = request_id
            longest = max(longest, at - start)
        else:
            start = None
    print(json.dumps({"symbol": symbol, "hub_busy_frames": records, "longest_poll_s": longest}))
    # A series-call busy would reset this window; only consecutive polling waits count.
    assert (
        longest > CALL_BUSY_S + calls.BUSY_MARGIN_S
    ), "the fixture did not exercise a measurement polling gap beyond the old series-call deadline"


async def test_a_paused_history_call_expires_at_its_own_busy_bound(observed, throwaway):
    symbol = await cold_symbol(observed, throwaway)
    since = time.time()
    pending = asyncio.create_task(synced(observed, calls.Call.RANGES, symbol=symbol))
    stopped = False
    try:
        busy = await observed.received.first(
            lambda frame: isinstance(frame, binary.Busy) and frame.bound_s > QUEUE_BUSY_S,
            calls.HISTORY_BUSY_S,
        )
        assert busy.bound_s == CALL_BUSY_S
        assert not pending.done(), "the fresh call returned before the pause could exercise it"
        busy_at = observed.received.when(lambda frame: frame is busy)
        await throwaway.run("exec", throwaway.container, "pkill", "-STOP", "-x", "terminal64.exe")
        stopped = True
        paused_at = time.monotonic()
        assert not pending.done(), "the cold call completed before the terminal was paused"

        # The call's bound + the ordinary overdue-pong interval; the request expires first.
        expires_at = busy_at + busy.bound_s
        pong_at = expires_at + PING_INTERVAL_S * PING_TIMEOUT_INTERVALS
        with pytest.raises((HubTimeout, EaGone)):
            await asyncio.wait_for(
                asyncio.shield(pending), max(0, expires_at - time.monotonic()) + calls.BUSY_MARGIN_S
            )
        connected = re.compile(rf"history EA for {re.escape(symbol)} connected: (.+)$")
        log = await throwaway.logs(since)
        endpoint = next(
            match.group(1)
            for line in log.splitlines()
            if (match := connected.search(line)) is not None
        )

        async def closed():
            return await throwaway.logs(since)

        deadline = pong_at - time.monotonic()
        assert deadline > 0, "the history EA was not closed within the per-call deadline"
        await until(
            closed,
            lambda text: f"closing connection {endpoint}: its pong is overdue" in text,
            deadline,
            "the paused history EA's overdue-pong close",
        )
        print(json.dumps({"symbol": symbol, "hub_busy_frames": busy_records(observed)}))
        await asyncio.sleep(max(0, paused_at + PAUSE_S - time.monotonic()))
    finally:
        if stopped:
            await throwaway.run(
                "exec", throwaway.container, "pkill", "-CONT", "-x", "terminal64.exe"
            )
        if not pending.done():
            pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)
    assert (await synced(observed, calls.Call.RANGES, symbol=symbol)).result.first_tick > 0
