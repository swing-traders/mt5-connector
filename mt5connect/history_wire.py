"""The history routes' vocabulary, shared by the server and its client: their paths, the series they
serve with each bar series' period, the tick selections, and the codes the server answers with
beside the package's own."""

from enum import IntEnum, StrEnum

from mt5connect import mirror

BARS_PATH = "/history/bars"
TICKS_PATH = "/history/ticks"
RANGES_PATH = "/history/ranges"

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


class ServerCode(IntEnum):
    """The error codes the server answers beside the package's own, outside their range."""

    # HTTP 503 with Retry-After: the terminal's answers do not prove the window yet.
    SYNCING = -20_001
    # HTTP 503 with Retry-After: every slot for a call that can reach the terminal is taken.
    BUSY = -20_002


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
