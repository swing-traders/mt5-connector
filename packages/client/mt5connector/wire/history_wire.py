"""The history protocol's vocabulary, shared by the hub and its client: the series and their
periods, a bar's row, the tick selections and what sets a floor."""

from enum import StrEnum

from . import mirror

# The terminal answers a range spanning more than MaxBars − 11 periods with nothing.
SPAN_MARGIN = 11


class Series(StrEnum):
    """A history series: a timeframe of the terminal's with a fixed period, or the ticks."""

    M1 = "M1"
    M2 = "M2"
    M3 = "M3"
    M4 = "M4"
    M5 = "M5"
    M6 = "M6"
    M10 = "M10"
    M12 = "M12"
    M15 = "M15"
    M20 = "M20"
    M30 = "M30"
    H1 = "H1"
    H2 = "H2"
    H3 = "H3"
    H4 = "H4"
    H6 = "H6"
    H8 = "H8"
    H12 = "H12"
    D1 = "D1"
    W1 = "W1"
    TICKS = "ticks"


class TickFlags(StrEnum):
    """Which ticks a tick read selects, named after the package's COPY_TICKS flags."""

    ALL = "ALL"
    INFO = "INFO"
    TRADE = "TRADE"


class FloorOrigin(StrEnum):
    """What set a floor: the stub, the one row the terminal answers for a window before any
    plausible history, or the first authentic row past a coarse prefix at the series' start."""

    STUB = "stub"
    COARSE = "coarse"


# Each bar series' period and the package's timeframe for it. MN1 has no fixed period, so no series
# serves it.
BAR_PERIOD_S: dict[Series, int] = {
    Series.M1: 60,
    Series.M2: 120,
    Series.M3: 180,
    Series.M4: 240,
    Series.M5: 300,
    Series.M6: 360,
    Series.M10: 600,
    Series.M12: 720,
    Series.M15: 900,
    Series.M20: 1_200,
    Series.M30: 1_800,
    Series.H1: 3_600,
    Series.H2: 7_200,
    Series.H3: 10_800,
    Series.H4: 14_400,
    Series.H6: 21_600,
    Series.H8: 28_800,
    Series.H12: 43_200,
    Series.D1: 86_400,
    Series.W1: 604_800,
}
BAR_TIMEFRAME: dict[Series, int] = {
    Series.M1: mirror.TIMEFRAME_M1,
    Series.M2: mirror.TIMEFRAME_M2,
    Series.M3: mirror.TIMEFRAME_M3,
    Series.M4: mirror.TIMEFRAME_M4,
    Series.M5: mirror.TIMEFRAME_M5,
    Series.M6: mirror.TIMEFRAME_M6,
    Series.M10: mirror.TIMEFRAME_M10,
    Series.M12: mirror.TIMEFRAME_M12,
    Series.M15: mirror.TIMEFRAME_M15,
    Series.M20: mirror.TIMEFRAME_M20,
    Series.M30: mirror.TIMEFRAME_M30,
    Series.H1: mirror.TIMEFRAME_H1,
    Series.H2: mirror.TIMEFRAME_H2,
    Series.H3: mirror.TIMEFRAME_H3,
    Series.H4: mirror.TIMEFRAME_H4,
    Series.H6: mirror.TIMEFRAME_H6,
    Series.H8: mirror.TIMEFRAME_H8,
    Series.H12: mirror.TIMEFRAME_H12,
    Series.D1: mirror.TIMEFRAME_D1,
    Series.W1: mirror.TIMEFRAME_W1,
}


# A bar as the bars route answers it and the hub pushes it: the package's MqlRates fields, its open
# among them, and its close.
BAR_CLOSE = "time_close"
BARS = mirror.Array(
    mirror.ArrayName.RATES, (*mirror.RATES.dtype, (BAR_CLOSE, "<i8")), mirror.RATES.epochs
)


TICK_FLAGS: dict[TickFlags, int] = {
    TickFlags.ALL: mirror.COPY_TICKS_ALL,
    TickFlags.INFO: mirror.COPY_TICKS_INFO,
    TickFlags.TRADE: mirror.COPY_TICKS_TRADE,
}


def bar_series(timeframe: int) -> Series:
    """The bar series of a package timeframe; raises ValueError for one no series serves."""
    for series, package_timeframe in BAR_TIMEFRAME.items():
        if package_timeframe == timeframe:
            return series
    raise ValueError(f"timeframe {timeframe} has no history series")
