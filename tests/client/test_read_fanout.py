"""A failed read ends its sibling work without changing the exception the caller catches."""

import asyncio

import pytest
from exec_harness import build, ours_deal
from test_execution_reports import fill_reports
from test_instrument_definitions import provider_for
from venue_doubles import SymbolVenue, symbol_info

from mt5connector.client import errors
from mt5connector.client.concurrency import _fan_out
from mt5connector.wire import calls


async def test_cancellation_cleanup_cannot_replace_the_original_read_failure():
    started = asyncio.Event()
    first = None
    original_failure = ValueError("second input")

    async def first_read():
        nonlocal first
        first = asyncio.current_task()
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            raise errors.MT5InstrumentError("first input") from None

    async def second_read():
        await started.wait()
        raise original_failure

    try:
        with pytest.raises(ValueError, match="second input") as refused:
            await _fan_out(first_read(), second_read())
        assert refused.value is original_failure
        assert isinstance(first.exception(), errors.MT5InstrumentError)
    finally:
        if first is not None and not first.done():
            first.cancel()
            await asyncio.gather(first, return_exceptions=True)


async def test_a_refused_definition_cancels_a_siblings_unbounded_deferrals():
    venue = SymbolVenue()
    for symbol in ("EURUSD", "GBPUSD"):
        venue.add(symbol_info(name=symbol), bid=1.08, ask=1.09)
    provider = provider_for(venue, "EURUSD", "GBPUSD")
    original = venue.request
    asked_twice = asyncio.Event()
    sibling = None
    asks = 0

    async def request(call, **arguments):
        nonlocal sibling, asks
        if call is calls.Call.SYMBOL_INFO:
            if arguments["symbol"] == "EURUSD":
                await asked_twice.wait()
                raise errors.MT5InstrumentError("definition refused")
            else:
                sibling = asyncio.current_task()
                asks += 1
                if asks == 2:
                    asked_twice.set()
                raise errors.NoPublisher("publisher not ready", 0, 0.005)
        return await original(call, **arguments)

    venue.request = request
    try:
        with pytest.raises(errors.MT5InstrumentError, match="definition refused"):
            await asyncio.wait_for(provider._load(["EURUSD", "GBPUSD"]), 1)
        assert sibling.cancelled()
        assert asks == 2
    finally:
        if sibling is not None and not sibling.done():
            sibling.cancel()
            await asyncio.gather(sibling, return_exceptions=True)


async def test_a_failed_fill_report_cancels_other_reports_before_their_next_read(exec_venue):
    h = build(exec_venue)
    await h.connect()
    h.venue.history_deals_get.return_value = (
        ours_deal(ticket=7001, order=9001),
        ours_deal(ticket=7002, order=9002),
    )
    original_report = h.client._fill_report
    original_request = h.push.request
    reading = asyncio.Event()
    release = asyncio.Event()
    sibling = None

    async def report(deal, owner):
        nonlocal sibling
        if deal.ticket == 7001:
            await reading.wait()
            raise errors.MT5InstrumentError("fill refused")
        else:
            sibling = asyncio.current_task()
            return await original_report(deal, owner)

    async def request(call, **arguments):
        if call is calls.Call.ORDERS_GET and arguments.get("ticket") == 9002:
            reading.set()
            await release.wait()
        return await original_request(call, **arguments)

    h.client._fill_report = report
    h.push.request = request
    try:
        with pytest.raises(errors.MT5InstrumentError, match="fill refused"):
            await asyncio.wait_for(fill_reports(h), 1)
        assert sibling.cancelled()
        h.venue.orders_get.assert_not_called()
        h.venue.history_orders_get.assert_not_called()
    finally:
        release.set()
        if sibling is not None and not sibling.done():
            sibling.cancel()
            await asyncio.gather(sibling, return_exceptions=True)
