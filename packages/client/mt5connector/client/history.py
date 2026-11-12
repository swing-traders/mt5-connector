"""The hub's history reads on a session, asked again while the hub defers them, and the bars a
window of closes keeps."""

from __future__ import annotations

from enum import StrEnum
from typing import TYPE_CHECKING

from mt5connector.wire import calls
from mt5connector.wire.history_wire import (
    BAR_CLOSE,
    BAR_PERIOD_S,
    BAR_TIMEFRAME,
    TICK_FLAGS,
    Series,
    TickFlags,
)

if TYPE_CHECKING:
    import numpy as np

    from mt5connector.client.hub import HubSession


class OnSyncing(StrEnum):
    WAIT = "wait"
    RAISE = "raise"


def _source_arguments(source: calls.Source) -> dict:
    if not isinstance(source, calls.Source):
        raise ValueError("source is not a Source member")
    if source is calls.Source.TERMINAL:
        return {"source": source}
    return {}


async def bars(
    session: HubSession,
    symbol: str,
    series: Series,
    first_open_s: int,
    last_open_s: int,
    *,
    on_syncing: OnSyncing = OnSyncing.WAIT,
    source: calls.Source = calls.Source.SERVER_SYNCED,
) -> np.ndarray:
    """The bars of `series` opened in [first_open_s, last_open_s], true-UTC epoch seconds, each with
    its open and its close."""
    answered = await session.deferring(
        calls.Call.COPY_RATES_RANGE,
        on_syncing=on_syncing,
        symbol=symbol,
        timeframe=BAR_TIMEFRAME[series],
        date_from=first_open_s,
        date_to=last_open_s,
        **_source_arguments(source),
    )
    return answered.result


def opens_closing(series: Series, first_close: int, last_close: int) -> tuple[int, int]:
    """The opens, true-UTC seconds, between which every bar of `series` closing in [first_close,
    last_close] opened: a bar opens before it closes, and within two of its periods, since a clock
    change lengthens one only on the broker day's grid, by less than its period."""
    return first_close - 2 * BAR_PERIOD_S[series], last_close - 1


def closing(rows: np.ndarray, first_close: int, last_close: int) -> np.ndarray:
    """The rows of the bars closing in [first_close, last_close]."""
    closes = rows[BAR_CLOSE]
    return rows[(closes >= first_close) & (closes <= last_close)]


async def ticks(
    session: HubSession,
    symbol: str,
    start_s: int,
    end_s: int,
    flags: TickFlags = TickFlags.INFO,
    *,
    on_syncing: OnSyncing = OnSyncing.WAIT,
    source: calls.Source = calls.Source.SERVER_SYNCED,
) -> np.ndarray:
    """The ticks from the start of second `start_s` through the end of second `end_s`, true-UTC
    epoch seconds."""
    answered = await session.deferring(
        calls.Call.COPY_TICKS_RANGE,
        on_syncing=on_syncing,
        symbol=symbol,
        date_from=start_s,
        date_to=end_s,
        flags=TICK_FLAGS[flags],
        **_source_arguments(source),
    )
    return answered.result


async def ranges(
    session: HubSession,
    symbol: str,
    *,
    on_syncing: OnSyncing = OnSyncing.WAIT,
    source: calls.Source = calls.Source.SERVER_SYNCED,
) -> calls.Ranges:
    """The terminal's MaxBars and the floors the hub holds for the symbol."""
    answered = await session.deferring(
        calls.Call.RANGES, on_syncing=on_syncing, symbol=symbol, **_source_arguments(source)
    )
    return answered.result
