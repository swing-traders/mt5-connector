"""The history reads on a hub session against a hub double: the call each asks with its arguments,
the rows or ranges it returns, a deferring answer asked again and any other error raised."""

import asyncio
from unittest.mock import MagicMock

import numpy as np
import pytest
from hub_double import eventually

from mt5connector.client import history
from mt5connector.client.config import MT5Config
from mt5connector.client.errors import InvalidWindow
from mt5connector.client.hub import HubSession
from mt5connector.wire import binary, calls, mirror
from mt5connector.wire.binary import ErrorKind
from mt5connector.wire.history_wire import BARS, Series, TickFlags

Call = calls.Call
_BARS = np.dtype(list(BARS.dtype))
_TICKS = np.dtype(list(mirror.TICKS.dtype))
NINE = 1_752_570_000


def rows(count: int) -> np.ndarray:
    bars = np.zeros(count, dtype=_BARS)
    bars["time"] = [NINE + 60 * index for index in range(count)]
    bars["time_close"] = bars["time"] + 60
    bars["close"] = 1.085
    return bars


@pytest.fixture
async def session(hub):
    config = MT5Config(
        account=12345678,
        server="Broker-Demo",
        ws_url=f"ws://127.0.0.1:{hub.port}",
    )
    hub_session = HubSession(config, asyncio.get_running_loop(), MagicMock(), None, MagicMock())
    await hub_session.connect()
    yield hub_session
    await hub_session.disconnect()


async def test_bars_are_the_rows_the_hub_answers_asked_by_the_series_timeframe(hub, session):
    hub.answers[Call.COPY_RATES_RANGE] = [rows(3)]

    read = await asyncio.wait_for(history.bars(session, "X", Series.M1, NINE, NINE + 179), 2)

    (request,) = hub.asked(Call.COPY_RATES_RANGE)
    assert calls.decode_arguments(calls.CALLS[Call.COPY_RATES_RANGE], request) == {
        "symbol": "X",
        "timeframe": mirror.TIMEFRAME_M1,
        "date_from": NINE,
        "date_to": NINE + 179,
    }
    assert read.dtype == _BARS
    assert read.tolist() == rows(3).tolist()


async def test_bars_the_hub_defers_syncing_are_asked_again_after_its_delay(hub, session):
    syncing = binary.AnswerError(ErrorKind.SYNCING, "unproven", 50)
    hub.answers[Call.COPY_RATES_RANGE] = [syncing, rows(3)]

    read = await asyncio.wait_for(history.bars(session, "X", Series.M1, NINE, NINE + 179), 2)

    assert len(read) == 3
    assert len(hub.asked(Call.COPY_RATES_RANGE)) == 2


async def test_bars_the_hub_answers_invalid_window_raise_and_are_never_asked_again(hub, session):
    hub.answers[Call.COPY_RATES_RANGE] = [binary.AnswerError(ErrorKind.INVALID_WINDOW, "future")]

    with pytest.raises(InvalidWindow, match="^copy_rates_range: future$"):
        await asyncio.wait_for(history.bars(session, "X", Series.H1, NINE, NINE + 3_599), 2)
    await asyncio.sleep(0.1)

    assert len(hub.asked(Call.COPY_RATES_RANGE)) == 1


async def test_ticks_are_the_rows_the_hub_answers_asked_by_the_flags_value(hub, session):
    ticks = np.zeros(2, dtype=_TICKS)
    ticks["time_msc"] = [NINE * 1000 + 1, NINE * 1000 + 2]
    hub.answers[Call.COPY_TICKS_RANGE] = [ticks]

    read = await asyncio.wait_for(history.ticks(session, "X", NINE, NINE + 59), 2)
    every = await asyncio.wait_for(history.ticks(session, "X", NINE, NINE, TickFlags.ALL), 2)

    first, second = hub.asked(Call.COPY_TICKS_RANGE)
    row = calls.CALLS[Call.COPY_TICKS_RANGE]
    assert calls.decode_arguments(row, first) == {
        "symbol": "X",
        "date_from": NINE,
        "date_to": NINE + 59,
        "flags": mirror.COPY_TICKS_INFO,
    }
    assert calls.decode_arguments(row, second)["flags"] == mirror.COPY_TICKS_ALL
    assert read["time_msc"].tolist() == [NINE * 1000 + 1, NINE * 1000 + 2]
    assert len(every) == 2


async def test_ranges_are_the_hubs_ranges_of_the_symbol(hub, session):
    held = calls.Ranges(100_000, {Series.M1: calls.SeriesRange(NINE, NINE + 5, 2)})
    hub.answers[Call.RANGES] = [held]

    read = await asyncio.wait_for(history.ranges(session, "X"), 2)

    await eventually(lambda: hub.asked(Call.RANGES))
    (request,) = hub.asked(Call.RANGES)
    assert calls.decode_arguments(calls.CALLS[Call.RANGES], request) == {"symbol": "X"}
    assert read == held
