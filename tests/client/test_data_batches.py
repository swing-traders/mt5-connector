"""Large history responses keep their scalar fields while yielding to unrelated loop work."""

import asyncio
from unittest.mock import patch

import numpy as np
import pytest
from nautilus_trader.model.data import QuoteTick
from test_data import (
    BARS,
    TICKS,
    make_client,
    minute_bars_request,
    ticks_request,
)

from mt5connector.client.parsing import quote_ticks_from_rows, venue_bar

STAMP = 1_760_000_000_000_000_000


@pytest.mark.parametrize("bars", [False, True], ids=["quotes", "bars"])
async def test_large_history_keeps_every_field_and_yields_between_batches(bars):
    client, conn, provider, loop = make_client()
    instrument = provider.get_instrument()
    count = 100_000
    if bars:
        rows = np.zeros(count, dtype=BARS)
        rows["time"] = 1_700_000_000 + np.arange(count) * 60
        rows["time_close"] = rows["time"] + 60
        rows["open"], rows["high"], rows["low"], rows["close"] = 1.085, 1.09, 1.08, 1.088
        rows["tick_volume"] = 1234
        request = minute_bars_request(int(rows["time_close"][0]), int(rows["time_close"][-1]))
        expected = [venue_bar(row, request.bar_type, instrument) for row in rows]
    else:
        rows = np.zeros(count, dtype=TICKS)
        rows["time_msc"] = 1_700_000_000_000 + np.arange(count)
        rows["time"] = rows["time_msc"] // 1000
        rows["bid"] = 1.085 + (np.arange(count) % 20) / 100_000
        rows["ask"] = rows["bid"] + 0.00002
        request = ticks_request(int(rows["time"][0]), int(rows["time"][-1]))
        expected = [
            QuoteTick(
                instrument.id,
                instrument.make_price(row["bid"]),
                instrument.make_price(row["ask"]),
                instrument.max_quantity,
                instrument.max_quantity,
                int(row["time_msc"]) * 1_000_000,
                STAMP,
            )
            for row in rows
        ]
    running = True
    turns = 0

    async def counter():
        nonlocal turns
        while running:
            turns += 1
            await asyncio.sleep(0)

    ticking = asyncio.create_task(counter())
    try:
        with (
            patch("mt5connector.client.history.bars", return_value=rows),
            patch("mt5connector.client.history.ticks", return_value=rows),
            patch("time.time_ns", return_value=STAMP),
        ):
            if bars:
                await client._request_bars(request)
                delivered = client._handle_bars.call_args.args[1]
            else:
                await client._request_quote_ticks(request)
                delivered = client._handle_quote_ticks.call_args.args[1]
    finally:
        running = False
        await ticking
    assert delivered == expected
    assert turns >= 9


async def test_a_readonly_decoded_tick_array_can_be_constructed():
    client, conn, provider, loop = make_client()
    rows = np.zeros(2, dtype=TICKS)
    rows["time_msc"] = [1_700_000_000_000, 1_700_000_000_001]
    rows["bid"], rows["ask"] = 1.085, 1.08502
    rows.setflags(write=False)
    quotes = quote_ticks_from_rows(rows, provider.get_instrument(), STAMP)
    assert [quote.ts_event for quote in quotes] == [
        1_700_000_000_000_000_000,
        1_700_000_000_001_000_000,
    ]
    assert all(quote.bid_price == provider.get_instrument().make_price(1.085) for quote in quotes)


@pytest.mark.parametrize("milliseconds", [-1, 18_446_744_073_710])
async def test_a_timestamp_outside_nts_range_raises(milliseconds):
    client, conn, provider, loop = make_client()
    rows = np.zeros(1, dtype=TICKS)
    rows["time_msc"] = milliseconds
    rows["bid"], rows["ask"] = 1.085, 1.08502
    with pytest.raises(OverflowError, match="nanosecond range"):
        quote_ticks_from_rows(rows, provider.get_instrument(), STAMP)
