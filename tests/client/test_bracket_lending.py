"""Sibling requests carry intended and proven bracket levels before NT applies their events."""

import asyncio

import pytest
from nautilus_trader.model.objects import Price
from test_execution_exits import (
    POSITION,
    cancel,
    connected,
    holding,
    long_position,
    modify,
    occupied,
    sent_requests,
    stop,
    submit,
    target,
)
from venue_doubles import trade_result

from mt5connector.client import errors, execution
from mt5connector.wire import calls, mirror


async def test_a_done_amend_lends_its_level_before_nt_applies_its_event(exec_venue):
    h, stop, target = await occupied(exec_venue)
    await modify(h, stop, trigger_price=Price.from_str("1.08300"))
    original = h.push.request
    reading = asyncio.Event()
    release = asyncio.Event()

    async def request(call, **arguments):
        if call is calls.Call.POSITIONS_GET:
            reading.set()
            await release.wait()
        return await original(call, **arguments)

    h.push.request = request
    changing = asyncio.create_task(modify(h, target, price=Price.from_str("1.10500")))
    try:
        await asyncio.wait_for(reading.wait(), 1)
        await h.verdict(request_id=1)
        assert stop.trigger_price == Price.from_str("1.08000")
    finally:
        release.set()
        await changing
    assert sent_requests(h)[-1]["sl"] == 1.083


async def test_latest_started_wins_when_the_later_clear_is_sent_before_the_amend(exec_venue):
    h, stop, target = await occupied(exec_venue)
    original = h.push.request
    reading = asyncio.Event()
    release = asyncio.Event()
    reads = 0

    async def request(call, **arguments):
        nonlocal reads
        if call is calls.Call.POSITIONS_GET:
            reads += 1
            if reads == 1:
                reading.set()
                await release.wait()
        return await original(call, **arguments)

    h.push.request = request
    amending = asyncio.create_task(modify(h, stop, trigger_price=Price.from_str("1.08300")))
    try:
        await asyncio.wait_for(reading.wait(), 1)
        await cancel(h, stop)
    finally:
        release.set()
        await amending
    await modify(h, target, price=Price.from_str("1.10500"))
    assert [request["sl"] for request in sent_requests(h)] == [0.0, 1.083, 0.0]


@pytest.mark.parametrize(
    "retcode", [mirror.TRADE_RETCODE_INVALID_STOPS, mirror.TRADE_RETCODE_CONNECTION]
)
async def test_an_unproven_amend_releases_its_level_to_nts_stop(exec_venue, retcode):
    h, stop, target = await occupied(exec_venue)
    await modify(h, stop, trigger_price=Price.from_str("1.08300"))
    await h.verdict(retcode, request_id=1)
    await modify(h, target, price=Price.from_str("1.10500"))
    assert sent_requests(h)[-1]["sl"] == 1.08


@pytest.mark.parametrize(
    ("retcode", "expected"),
    [
        (mirror.TRADE_RETCODE_INVALID_STOPS, 1.083),
        (mirror.TRADE_RETCODE_CONNECTION, 1.08),
    ],
)
async def test_a_later_verdict_preserves_or_releases_a_previously_proven_level(
    exec_venue, retcode, expected
):
    h, stop, target = await occupied(exec_venue)
    await modify(h, stop, trigger_price=Price.from_str("1.08300"))
    await h.verdict(request_id=1)
    await modify(h, stop, trigger_price=Price.from_str("1.08400"))
    await h.verdict(retcode, request_id=2)
    await modify(h, target, price=Price.from_str("1.10500"))
    assert sent_requests(h)[-1]["sl"] == expected
    if retcode == mirror.TRADE_RETCODE_CONNECTION:
        assert (POSITION, execution.Slot.SL) not in h.client._proven


@pytest.mark.parametrize(
    ("failure", "expected"),
    [
        (errors.NotSent("write refused"), 1.083),
        (errors.ResponseLost("answer lost"), 1.08),
        (trade_result(retcode=mirror.TRADE_RETCODE_INVALID_STOPS), 1.083),
    ],
    ids=["not-sent", "lost", "refused"],
)
async def test_an_unsent_answer_preserves_or_releases_the_proven_level(
    exec_venue, failure, expected
):
    h, stop, target = await occupied(exec_venue)
    await modify(h, stop, trigger_price=Price.from_str("1.08300"))
    await h.verdict(request_id=1)
    h.venue.order_send.side_effect = [failure, trade_result()]
    await modify(h, stop, trigger_price=Price.from_str("1.08400"))
    await modify(h, target, price=Price.from_str("1.10500"))
    assert sent_requests(h)[-1]["sl"] == expected


async def test_a_done_clear_lends_no_stop_before_nt_applies_its_event(exec_venue):
    h, stop, target = await occupied(exec_venue)
    await cancel(h, stop)
    await h.verdict(request_id=1)
    await modify(h, target, price=Price.from_str("1.10500"))
    assert sent_requests(h)[-1]["sl"] == 0.0


async def test_a_done_placement_lends_its_proven_level_before_nt_applies_acceptance(exec_venue):
    h = await connected(exec_venue)
    h.venue.positions_get.side_effect = holding(long_position(sl=0.0, tp=0.0))
    await submit(h, stop(h))
    await h.verdict(request_id=1)
    await submit(h, target(h, price="1.10500"))
    assert sent_requests(h)[-1]["sl"] == 1.08


async def test_disconnect_releases_proven_bracket_levels(exec_venue):
    h, stop, _ = await occupied(exec_venue)
    await modify(h, stop, trigger_price=Price.from_str("1.08300"))
    await h.verdict(request_id=1)
    assert h.client._proven[(POSITION, execution.Slot.SL)] == Price.from_str("1.08300")
    h.client._account_task = asyncio.create_task(asyncio.sleep(60))
    await h.client._disconnect()
    assert h.client._proven == {}
