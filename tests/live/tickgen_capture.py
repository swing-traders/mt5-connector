"""The raw collector's captures, on the broker's clock, and the mixed fixture identified in one: a
minute holding real ticks followed by a later minute whose M1 bar records a spread and which holds
no tick."""

import csv
from dataclasses import dataclass
from pathlib import Path

MINUTE_S = 60

TICK_FIELDS = ("time", "time_msc", "bid", "ask", "last", "volume", "flags", "volume_real")
BAR_FIELDS = ("time", "open", "high", "low", "close", "tick_volume", "spread", "real_volume")


@dataclass(frozen=True)
class Capture:
    label: str
    ticks: list[tuple]
    bars: list[tuple]

    def minutes(self) -> dict[int, list[tuple]]:
        """Each minute's ticks in capture order, keyed by its broker open."""
        grouped: dict[int, list[tuple]] = {}
        for tick in self.ticks:
            grouped.setdefault(tick[1] // 60_000 * MINUTE_S, []).append(tick)
        return grouped


def _tick(row: dict) -> tuple:
    return (
        int(row["time"]),
        int(row["time_msc"]),
        float(row["bid"]),
        float(row["ask"]),
        float(row["last"]),
        int(row["volume"]),
        int(row["flags"]),
        float(row["volume_real"]),
    )


def _bar(row: dict) -> tuple:
    return (
        int(row["time"]),
        float(row["open"]),
        float(row["high"]),
        float(row["low"]),
        float(row["close"]),
        int(row["tick_volume"]),
        int(row["spread"]),
        int(row["real_volume"]),
    )


def load(directory: Path, label: str) -> Capture:
    with open(directory / f"{label}-ticks.csv", newline="") as file:
        ticks = [_tick(row) for row in csv.DictReader(file)]
    with open(directory / f"{label}-bars.csv", newline="") as file:
        bars = [_bar(row) for row in csv.DictReader(file)]
    return Capture(label, ticks, bars)


def points(price: float, digits: int) -> int:
    return round(price * 10**digits)


@dataclass(frozen=True)
class Fixture:
    """The broker window [start, end] a mixed request reads, and the raw-empty minute in it."""

    start: int
    end: int
    empty: int


def identify(capture: Capture, digits: int) -> Fixture | None:
    """The first raw-empty spread-bearing minute after a real-tick minute, with a non-doji bar
    before it in the capture; its window runs from the nearest real-tick minute before it to the
    end of the nearest after it, or of the empty minute itself where none follows."""
    held = capture.minutes()
    real = sorted(held)
    moved = False
    for bar in capture.bars:
        opened = bar[0]
        before = [minute for minute in real if minute < opened]
        if before and moved and bar[6] > 0 and opened not in held:
            after = [minute for minute in real if minute > opened]
            if after:
                end = after[0] + MINUTE_S - 1
            else:
                end = opened + MINUTE_S - 1
            return Fixture(before[-1], end, opened)
        moved = moved or points(bar[4], digits) != points(bar[1], digits)
    return None


def counts(capture: Capture) -> dict[str, int]:
    """What an inspection of a capture found, for a failure naming it."""
    real = capture.minutes()
    return {
        "ticks": len(capture.ticks),
        "bars": len(capture.bars),
        "real_minutes": len(real),
        "raw_empty_spread_minutes": sum(
            1 for bar in capture.bars if bar[0] not in real and bar[6] > 0
        ),
    }
