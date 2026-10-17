"""A bar's close in true UTC, from its open on the broker's clock."""

import numpy as np

from mt5connector.server.eras import Eras
from mt5connector.server.wire.broker_clock import BrokerClock
from mt5connector.server.wire.history_wire import BAR_PERIOD_S, Series

_BROKER_DAY_GRID = frozenset(
    {Series.H2, Series.H3, Series.H4, Series.H6, Series.H8, Series.H12, Series.D1, Series.W1}
)


def bar_closes(eras: Eras, series: Series, broker_opens: np.ndarray) -> np.ndarray:
    """The true-UTC close bar_close gives each bar of `series` opened at the broker epochs
    `broker_opens`."""
    if series in _BROKER_DAY_GRID:
        return eras.to_utc(broker_opens + BAR_PERIOD_S[series])
    else:
        return eras.to_utc(broker_opens) + BAR_PERIOD_S[series]


def bar_close(clock: BrokerClock, series: Series, broker_open: int) -> int:
    """The true-UTC close of a bar of `series` opened at broker epoch `broker_open`: a frame on the
    broker day's grid closes at the next grid open on the broker's clock, a finer one a period of
    real time after its open."""
    if series in _BROKER_DAY_GRID:
        return clock.to_utc(broker_open + BAR_PERIOD_S[series])
    else:
        return clock.to_utc(broker_open) + BAR_PERIOD_S[series]
