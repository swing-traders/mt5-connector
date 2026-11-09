"""The every-tick generator's historical acceptance against the candidate hub tests/live/
prepare_tickgen.py starts: the tester's exported weeks, a doji read alone, the venue's real ticks,
and a later tickless minute rejected inside the tick era. It runs only when MT5_TICK_EXPORT_DIR and
MT5_TICK_RAW_DIR are set; MT5_TICK_ACCEPTANCE_REQUIRED=1 fails every case whose inputs or hub are
missing instead of skipping it."""

import asyncio
import csv
import hashlib
import json
import os
import subprocess
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
import tickgen_capture
import tickgen_oracle as oracle

from mt5connector.client.config import MT5Config
from mt5connector.client.errors import EaError
from mt5connector.client.hub import HubSession
from mt5connector.wire import calls, mirror
from mt5connector.wire.broker_clock import BrokerClock

CHECKOUT = Path(__file__).resolve().parents[2]
SYMBOL = os.environ.get("MT5_LIVE_HUB_SYMBOL")
HUB_URL = os.environ.get("MT5_LIVE_HUB_URL", "ws://127.0.0.1:9000")
EXPORT_DIR = os.environ.get("MT5_TICK_EXPORT_DIR")
RAW_DIR = os.environ.get("MT5_TICK_RAW_DIR")
REQUIRED = os.environ.get("MT5_TICK_ACCEPTANCE_REQUIRED") == "1"
CLOCK = BrokerClock(ZoneInfo("America/New_York"), timedelta(hours=7))
EA_SOURCE = "packages/server/mt5connector/server/mql5/Expert/ticks.mq5"
MINUTE_S = 60
WEEK_S = 7 * 86_400
# How long one history read may stay deferred while the candidate's history EA reads.
SYNCED_S = 2 * calls.HISTORY_BUSY_S


@dataclass(frozen=True)
class Week:
    """A tester export's identity and the figures registered for it."""

    job: str
    sha256: dict[str, str]
    minutes: int
    full_volume: int
    deficit: int
    scaffolds: int
    within_one_point: int


WEEKS = (
    Week(
        "ticks-202301",
        {
            "ticks.csv": "d7da4b6574e1609b8be6fabc1075eafa9d1617d09a98eb85a73e04eabcc9778f",
            "bars.csv": "3b9632f32335dcfade8d47e8ad53a5a386201b7e38dd9de6c7dd332fa9b33bd7",
            "metadata.csv": "9d80528d44c3a43a597701dbeae98f7f2e1ffba0c21c9cff53cce8b15fd29657",
        },
        minutes=7188,
        full_volume=6543,
        deficit=645,
        scaffolds=6401,
        within_one_point=6543,
    ),
    Week(
        "ticks-202310",
        {
            "ticks.csv": "4f821cc254ce0f4848927dc352c0d01da2cb877045065848e31fdadd54e755a2",
            "bars.csv": "ce24e6bf8be125e0eb04e500b04913f2d13ff430c69964ee3ac3c3c6072205fa",
            "metadata.csv": "78022247eec4f8c6d53748ae807940f73d1eb56ad60d1a942f31189a6186d14f",
        },
        minutes=7189,
        full_volume=6236,
        deficit=953,
        scaffolds=6065,
        within_one_point=6229,
    ),
)
# The January minute, by raw broker epoch, whose path the worked rising-doji example registers.
WORKED_MINUTE = 1673308440
WORKED_PATH = [92543, 92545, 92544, 92546, 92545, 92547, 92546, 92550, 92549, 92543]


def unavailable(reason: str):
    if REQUIRED:
        pytest.fail(reason)
    else:
        pytest.skip(reason)


class Log:
    def debug(self, message: str) -> None:
        pass

    def info(self, message: str) -> None:
        pass

    def warning(self, message: str) -> None:
        print(message)

    def error(self, message: str) -> None:
        print(message)

    def exception(self, message: str, failure: BaseException) -> None:
        print(f"{message}: {failure!r}")


@dataclass
class Export:
    """A week's tester export: its M1 inputs and its ticks by raw broker minute, prices in
    points."""

    week: Week
    digits: int
    bars: list[tuple]
    ticks: dict[int, list[tuple]]


@dataclass
class Read:
    """What the candidate answered for a week."""

    bars: list[tuple]
    last_direction: int
    minutes: dict[int, list[tuple]]


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_export(directory: Path, week: Week) -> Export:
    out = directory / week.job / "out"
    for name, digest in week.sha256.items():
        if not (out / name).is_file():
            unavailable(f"{out / name} is missing")
        assert sha256(out / name) == digest, f"{out / name} is not the registered export"
    with open(out / "metadata.csv", newline="") as file:
        metadata = {row["key"]: row["value"] for row in csv.DictReader(file)}
    digits = int(metadata["digits"])
    with open(out / "bars.csv", newline="") as file:
        bars = [
            (
                int(row["time"]),
                tickgen_capture.points(float(row["open"]), digits),
                tickgen_capture.points(float(row["high"]), digits),
                tickgen_capture.points(float(row["low"]), digits),
                tickgen_capture.points(float(row["close"]), digits),
                int(row["tick_volume"]),
                int(row["spread"]),
            )
            for row in csv.DictReader(file)
        ]
    ticks: dict[int, list[tuple]] = {}
    with open(out / "ticks.csv", newline="") as file:
        for row in csv.DictReader(file):
            stamp = int(row["time_msc"])
            ticks.setdefault(stamp // 60_000 * MINUTE_S, []).append(
                (stamp, tickgen_capture.points(float(row["bid"]), digits))
            )
    return Export(week, digits, bars, ticks)


def bar_points(bar, digits: int) -> tuple:
    return (
        CLOCK.to_broker(int(bar["time"])),
        tickgen_capture.points(float(bar["open"]), digits),
        tickgen_capture.points(float(bar["high"]), digits),
        tickgen_capture.points(float(bar["low"]), digits),
        tickgen_capture.points(float(bar["close"]), digits),
        int(bar["tick_volume"]),
        int(bar["spread"]),
    )


def tick_row(tick) -> tuple:
    return (
        int(tick["time"]),
        int(tick["time_msc"]),
        float(tick["bid"]),
        float(tick["ask"]),
        float(tick["last"]),
        int(tick["volume"]),
        int(tick["flags"]),
        float(tick["volume_real"]),
    )


def by_minute(ticks) -> dict[int, list[tuple]]:
    """Each true-UTC minute's ticks, in answer order."""
    grouped: dict[int, list[tuple]] = {}
    for tick in ticks:
        row = tick_row(tick)
        grouped.setdefault(row[1] // 60_000 * MINUTE_S, []).append(row)
    return grouped


async def session_reads(reads):
    session = HubSession(
        MT5Config(account=1, server="unused", ws_url=HUB_URL),
        asyncio.get_running_loop(),
        lambda frame: None,
        None,
        Log(),
    )
    await session.connect()
    try:
        return await reads(session)
    finally:
        await session.disconnect()


async def m1(session: HubSession, start: int, end: int):
    answered = await asyncio.wait_for(
        session.deferring(
            calls.Call.COPY_RATES_RANGE,
            symbol=SYMBOL,
            timeframe=mirror.TIMEFRAME_M1,
            date_from=start,
            date_to=end,
        ),
        SYNCED_S,
    )
    return answered.result


async def every_tick(session: HubSession, start: int, end: int):
    answered = await asyncio.wait_for(
        session.deferring(
            calls.Call.COPY_TICKS_RANGE,
            symbol=SYMBOL,
            date_from=start,
            date_to=end,
            flags=mirror.COPY_TICKS_ALL,
        ),
        SYNCED_S,
    )
    return answered.result


def last_non_doji(bars: list[tuple]) -> int | None:
    for _t, o, _h, _l, c, _v, _s in reversed(bars):
        if c != o:
            return oracle.sign(c - o)
    return None


@pytest.fixture(scope="module")
def provenance() -> dict:
    import mt5connector.client

    if SYMBOL is None or EXPORT_DIR is None or RAW_DIR is None:
        unavailable("MT5_LIVE_HUB_SYMBOL, MT5_TICK_EXPORT_DIR and MT5_TICK_RAW_DIR must be set")
    assert Path(mt5connector.client.__file__).resolve().is_relative_to(CHECKOUT)
    path = Path(RAW_DIR) / "provenance.json"
    if not path.is_file():
        unavailable(f"{path} is missing")
    prepared = json.loads(path.read_text())
    image_id = subprocess.run(
        ["podman", "image", "inspect", "--format", "{{.Id}}", prepared["image"]],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    running = subprocess.run(
        ["podman", "inspect", "--format", "{{.Image}} {{.State.Running}}", prepared["container"]],
        capture_output=True,
        text=True,
    )
    if running.returncode != 0:
        unavailable(f"the candidate container {prepared['container']} is not there")
    assert running.stdout.split() == [prepared["image_id"], "true"]
    assert image_id == prepared["image_id"]
    assert sha256(CHECKOUT / EA_SOURCE) == prepared["source_sha256"][EA_SOURCE]
    assert prepared["image_ticks_sha256"] == prepared["source_sha256"][EA_SOURCE]
    assert HUB_URL.rstrip("/").endswith(f":{prepared['port']}")
    assert prepared["symbol"] == SYMBOL
    return prepared


@pytest.fixture(scope="module")
def weeks(provenance) -> list[tuple[Export, Read]]:
    exports = [load_export(Path(EXPORT_DIR), week) for week in WEEKS]

    async def reads(session: HubSession) -> list[Read]:
        answered = []
        for export in exports:
            first, last = export.bars[0][0], export.bars[-1][0]
            start, end = CLOCK.to_utc(first), CLOCK.to_utc(last) + MINUTE_S - 1
            bars = [bar_points(bar, export.digits) for bar in await m1(session, start, end)]
            before = [
                bar_points(bar, export.digits)
                for bar in await m1(session, start - WEEK_S, start - 1)
            ]
            direction = last_non_doji(before)
            assert direction is not None, f"no non-doji in the week before {export.week.job}"
            ticks = await every_tick(session, start, end)
            answered.append(Read(bars, direction, by_minute(ticks)))
        return answered

    try:
        answered = asyncio.run(session_reads(reads))
    except OSError as failure:
        unavailable(f"no hub answers at {HUB_URL}: {failure}")
    return list(zip(exports, answered, strict=True))


def fitted(export: Export, read: Read) -> list[tuple[tuple, int]]:
    """Each exported bar with its direction, the week before resolving the first."""
    directions = oracle.directions([bar[1:6] for bar in export.bars], read.last_direction)
    return list(zip(export.bars, directions, strict=True))


def candidate_minute(read: Read, broker_open: int) -> list[tuple]:
    return read.minutes.get(CLOCK.to_utc(broker_open), [])


def bid_points(rows: list[tuple], digits: int) -> list[int]:
    return [tickgen_capture.points(row[2], digits) for row in rows]


def test_the_terminals_m1_bars_equal_each_exports_inputs_field_for_field(weeks):
    for export, read in weeks:
        exported = {bar[0]: bar for bar in export.bars}
        held = {bar[0]: bar for bar in read.bars}

        assert len(exported) == len(export.bars) == export.week.minutes
        assert len(held) == len(read.bars)
        assert held == exported


def test_every_exported_minute_emits_the_fitted_rules_count_and_bids(weeks):
    for export, read in weeks:
        deficits = 0
        for bar, d in fitted(export, read):
            t, o, h, low, c, v, _spread = bar
            expected = oracle.bids(o, h, low, c, v, d)
            emitted = bid_points(candidate_minute(read, t), export.digits)

            assert emitted == expected, f"{export.week.job} minute {t}"
            deficits += len(export.ticks[t]) == v - 1

        assert deficits == export.week.deficit
        assert sum(len(rows) for rows in read.minutes.values()) == sum(
            len(oracle.bids(*bar[1:6], d)) for bar, d in fitted(export, read)
        )


def embeds(points: list[int], path: list[int]) -> bool:
    remaining = iter(path)
    return all(any(price == point for price in remaining) for point in points)


def test_full_volume_minutes_meet_the_export_within_a_point_and_embed_its_scaffold(weeks):
    for export, read in weeks:
        scored = 0
        within = 0
        scaffolds = 0
        embedded = 0
        for bar, d in fitted(export, read):
            t, o, h, low, c, v, _spread = bar
            observed = [bid for _stamp, bid in export.ticks[t]]
            if len(observed) == v:
                scored += 1
                emitted = bid_points(candidate_minute(read, t), export.digits)
                within += len(emitted) == len(observed) and all(
                    abs(a - b) <= 1 for a, b in zip(emitted, observed, strict=True)
                )
                if v >= 4:
                    scaffolds += 1
                    embedded += embeds(oracle.scaffold(o, h, low, c, v, d), observed)

        assert scored == export.week.full_volume
        assert (scaffolds, embedded) == (export.week.scaffolds, export.week.scaffolds)
        if export.week.within_one_point == export.week.full_volume:
            assert within == scored
        else:
            assert within >= export.week.within_one_point


def test_every_minute_follows_its_emitted_count_schedule_and_meets_equal_count_export_stamps(
    weeks,
):
    for export, read in weeks:
        compared = 0
        for bar, _d in fitted(export, read):
            t, v = bar[0], bar[5]
            emitted = [row[1] for row in candidate_minute(read, t)]
            schedule = [
                CLOCK.to_utc_msc(t * 1000 + offset)
                for offset in oracle.offsets_ms(t, v, len(emitted))
            ]
            observed = [CLOCK.to_utc_msc(stamp) for stamp, _bid in export.ticks[t]]

            assert emitted == schedule, f"{export.week.job} minute {t}"
            if len(observed) == len(emitted):
                compared += 1
                assert all(
                    abs(a - b) <= 1 for a, b in zip(emitted, observed, strict=True)
                ), f"{export.week.job} minute {t}"

        assert compared > 0


def test_the_worked_rising_doji_minute_emits_its_registered_path(weeks):
    export, read = weeks[0]

    assert bid_points(candidate_minute(read, WORKED_MINUTE), export.digits) == WORKED_PATH


def test_every_emitted_ask_is_its_bid_plus_the_bars_spread_at_the_symbols_digits(weeks):
    for export, read in weeks:
        point = 10.0**-export.digits
        for bar in export.bars:
            for row in candidate_minute(read, bar[0]):
                bid, ask = row[2], row[3]

                assert ask == round(ask, export.digits)
                assert tickgen_capture.points(ask, export.digits) == tickgen_capture.points(
                    oracle.ask(bid, bar[6], point, export.digits), export.digits
                ), f"{export.week.job} minute {bar[0]}"


def test_a_doji_read_alone_goes_opposite_the_rising_bar_before_its_window(weeks):
    export, _read = weeks[0]
    selected = None
    last_moving = None
    for bar in export.bars:
        t, o, h, low, c, v, _spread = bar
        if (
            selected is None
            and c == o
            and v >= 4
            and h - o > 1
            and o - low > 1
            and last_moving is not None
            and last_moving[4] > last_moving[1]
            and oracle.bids(o, h, low, c, v, -1) != oracle.bids(o, h, low, c, v, 1)
        ):
            selected = (last_moving, bar)
        if c != o:
            last_moving = bar
    if selected is None:
        pytest.fail(f"no qualifying doji in {export.week.job}")
    predecessor, doji = selected
    opened = CLOCK.to_utc(doji[0])

    async def reads(session: HubSession):
        narrow = await every_tick(session, opened, opened + MINUTE_S - 1)
        wide = await every_tick(session, CLOCK.to_utc(predecessor[0]), opened + MINUTE_S - 1)
        return by_minute(narrow).get(opened, []), by_minute(wide).get(opened, [])

    narrow, wide = asyncio.run(session_reads(reads))

    assert narrow == wide
    assert bid_points(narrow, export.digits) == oracle.bids(*doji[1:6], -1)


def test_a_tick_era_gap_fails_and_the_preceding_real_minutes_answer_the_venues_ticks(
    provenance, weeks
):
    fixture = provenance["fixture"]
    capture = tickgen_capture.load(Path(RAW_DIR) / fixture["directory"], fixture["label"])
    raw = capture.minutes()
    start, empty = fixture["start"], fixture["empty"]
    last_real = max(minute for minute in raw if start <= minute < empty)

    async def reads(session: HubSession):
        with pytest.raises(EaError, match=f"minute broker {empty} holds a bar and no tick"):
            await every_tick(session, CLOCK.to_utc(start), CLOCK.to_utc(empty) + MINUTE_S - 1)
        return await every_tick(
            session, CLOCK.to_utc(start), CLOCK.to_utc(last_real) + MINUTE_S - 1
        )

    answered = by_minute(asyncio.run(session_reads(reads)))
    real = {minute: rows for minute, rows in raw.items() if start <= minute <= last_real}
    assert set(answered) == {CLOCK.to_utc(minute) for minute in real}
    for minute, rows in real.items():
        venue = [(CLOCK.to_utc(tick[0]), CLOCK.to_utc_msc(tick[1]), *tick[2:]) for tick in rows]

        assert answered[CLOCK.to_utc(minute)] == venue, f"broker minute {minute}"
