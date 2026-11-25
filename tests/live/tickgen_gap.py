"""Frozen native gap inputs and independent installed-input verification."""

import asyncio
import csv
import gzip
import hashlib
import io
import json
from dataclasses import dataclass
from pathlib import Path

import tickgen_capture

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures/tickgen-gap"


def no_repeat_resync(printed: str, symbol: str) -> None:
    events = [line for line in printed.splitlines() if f"re-sync of {symbol}:" in line]
    if events:
        raise RuntimeError("hub re-synced after native tick reload: " + "\n".join(events))


async def subscribed_reads(container, symbol, reads, *, timeout):
    # Log replay covers events between process creation and the follow stream attaching.
    stream = await asyncio.create_subprocess_exec(
        "podman",
        "logs",
        "--follow",
        container,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )

    async def watch():
        while line := await stream.stdout.readline():
            no_repeat_resync(line.decode(errors="replace"), symbol)
        raise RuntimeError("the re-sync event subscription ended")

    guard = asyncio.create_task(watch())
    operation = asyncio.create_task(reads())
    try:
        async with asyncio.timeout(timeout):
            done, _pending = await asyncio.wait(
                (operation, guard), return_when=asyncio.FIRST_COMPLETED
            )
            if guard in done:
                await guard
            return await operation
    finally:
        for task in (operation, guard):
            task.cancel()
        await asyncio.gather(operation, guard, return_exceptions=True)
        if stream.returncode is None:
            stream.terminate()
            try:
                await asyncio.wait_for(stream.wait(), 5)
            except TimeoutError:
                stream.kill()
                await stream.wait()


@dataclass(frozen=True)
class GapInput:
    provenance: dict
    broker: tickgen_capture.Capture
    native: tickgen_capture.Capture
    preceding: list[tuple]


def data(directory: Path, provenance: dict, name: str) -> bytes:
    content = gzip.decompress((directory / f"{name}.gz").read_bytes())
    if hashlib.sha256(content).hexdigest() != provenance["files"][name]:
        raise ValueError(f"gap fixture checksum mismatch: {name}")
    return content


def _rows(content: bytes, convert) -> list[tuple]:
    return [convert(row) for row in csv.DictReader(io.StringIO(content.decode()))]


def verify_broker(original: list[tuple], imported: list[tuple], count: int) -> None:
    if len(original) != count or len(imported) != count:
        raise ValueError("gap input has a missing or extra tick")
    for index, (source, native) in enumerate(zip(original, imported, strict=True)):
        if source[:6] + source[7:] != native[:6] + native[7:]:
            raise ValueError(f"gap input changed broker tick {index}")
        if native[6] not in (source[6], source[6] & ~128):
            raise ValueError(f"gap input changed broker tick {index} flags beyond cleared bit 128")


def load(directory: Path = FIXTURE) -> GapInput:
    provenance = json.loads((directory / "provenance.json").read_text())
    blobs = {name: data(directory, provenance, name) for name in provenance["files"]}
    bars = _rows(blobs["bars.csv"], tickgen_capture._bar)
    broker = tickgen_capture.Capture(
        "broker", _rows(blobs["broker-ticks.csv"], tickgen_capture._tick), bars
    )
    native = tickgen_capture.Capture(
        "gap", _rows(blobs["native-ticks.csv"], tickgen_capture._tick), bars
    )
    preceding = _rows(blobs["native-preceding.csv"], tickgen_capture._tick)
    counts = provenance["counts"]
    gap = provenance["removed_minute_broker"]
    retained = [row for row in broker.ticks if not gap * 1000 <= row[1] < (gap + 60) * 1000]
    before = [row for row in broker.ticks if row[1] < gap * 1000]
    if len(broker.ticks) != counts["broker_ticks"] or len(bars) != counts["bars"]:
        raise ValueError("gap fixture broker capture count differs")
    if len(broker.ticks) - len(retained) != counts["removed_ticks"]:
        raise ValueError("gap fixture removed tick count differs")
    if [row[0] for row in bars] != list(
        range(provenance["range"]["from_broker"], provenance["range"]["to_broker"] + 1, 60)
    ):
        raise ValueError("gap fixture M1 bars are not continuous from the capture start")
    verify_broker(retained, native.ticks, counts["retained_ticks"])
    verify_broker(before, preceding, counts["preceding_ticks"])
    if preceding != [row for row in native.ticks if row[1] < gap * 1000]:
        raise ValueError("gap fixture native preceding read differs from its full read")
    gap_bar = [row for row in bars if row[0] == gap]
    if len(gap_bar) != 1 or gap_bar[0][5] <= 0 or gap_bar[0][6] <= 0:
        raise ValueError("gap fixture has no active spread-bearing gap bar")
    if _rows(blobs["native-gap-bar.csv"], tickgen_capture._bar) != gap_bar:
        raise ValueError("gap fixture native bar differs from its broker input")
    return GapInput(provenance, broker, native, preceding)


def verify_native(directory: Path, fixture: GapInput, *, read_only: bool = True) -> dict:
    with (directory / "metadata.csv").open(newline="") as file:
        facts = {row["key"]: int(row["value"]) for row in csv.DictReader(file)}
    if facts["terminal_build"] != fixture.provenance["terminal_build"] or facts["custom"] != 1:
        raise ValueError("gap native snapshot has a different terminal or symbol kind")
    if (
        facts["gap_minute"] != fixture.provenance["removed_minute_broker"]
        or facts["gap_ticks"] != 0
        or facts["gap_ticks_error"] != 0
        or facts["gap_bars"] != 1
        or facts["gap_bars_error"] != 0
    ):
        raise ValueError("gap native snapshot does not hold the active bar and zero ticks")
    if read_only and (
        facts["read_only"] != 1
        or any(facts[name] for name in ("written_ticks", "deleted_ticks", "written_bars"))
    ):
        raise ValueError("gap native snapshot was not read-only")
    actual = _rows((directory / "all.csv").read_bytes(), tickgen_capture._tick)
    before = _rows((directory / "preceding.csv").read_bytes(), tickgen_capture._tick)
    bar = _rows((directory / "gap-bar.csv").read_bytes(), tickgen_capture._bar)
    gap = fixture.provenance["removed_minute_broker"]
    retained = [row for row in fixture.broker.ticks if not gap * 1000 <= row[1] < (gap + 60) * 1000]
    source_before = [row for row in fixture.broker.ticks if row[1] < gap * 1000]
    verify_broker(retained, actual, fixture.provenance["counts"]["retained_ticks"])
    verify_broker(source_before, before, fixture.provenance["counts"]["preceding_ticks"])
    if actual != fixture.native.ticks or before != fixture.preceding:
        raise ValueError("gap native snapshot differs from its frozen post-import reference")
    if bar != [row for row in fixture.broker.bars if row[0] == gap]:
        raise ValueError("gap native snapshot changed the original active bar")
    return facts
