"""Terminal-free checks of the frozen gap input and native snapshot verifier."""

import asyncio
import csv
import gzip
import io
import shutil

import pytest
import tickgen_gap


@pytest.fixture(scope="module")
def fixture():
    return tickgen_gap.load()


def test_frozen_gap_has_the_recorded_native_counts_and_day_boundary(fixture):
    assert len(fixture.broker.ticks) == fixture.provenance["counts"]["broker_ticks"]
    assert len(fixture.native.ticks) == fixture.provenance["counts"]["retained_ticks"]
    assert len(fixture.preceding) == fixture.provenance["counts"]["preceding_ticks"]
    assert fixture.native.bars[0][0] == fixture.provenance["native_first_dates"]["terminal_first"]
    assert fixture.provenance["removed_minute_utc"] == "2026-11-20T00:02:00+00:00"


@pytest.mark.parametrize("field", range(8))
def test_every_tick_field_change_is_rejected(fixture, field):
    original = [fixture.broker.ticks[0]]
    changed = list(original[0])
    changed[field] += 1
    with pytest.raises(ValueError, match="changed broker tick"):
        tickgen_gap.verify_broker(original, [tuple(changed)], 1)


@pytest.mark.parametrize("extra", [False, True])
def test_missing_and_extra_ticks_are_rejected(fixture, extra):
    source = fixture.broker.ticks[:2]
    changed = source + source[:1] if extra else source[:1]
    with pytest.raises(ValueError, match="missing or extra"):
        tickgen_gap.verify_broker(source, changed, 2)


def test_reordering_is_rejected(fixture):
    source = fixture.broker.ticks[:2]
    with pytest.raises(ValueError, match="changed broker tick"):
        tickgen_gap.verify_broker(source, source[::-1], 2)


def test_only_clearing_flag_bit_128_is_accepted(fixture):
    source = list(fixture.broker.ticks[0])
    source[6] = 134
    cleared = [*source[:6], 6, source[7]]
    tickgen_gap.verify_broker([tuple(source)], [tuple(cleared)], 1)
    tickgen_gap.verify_broker([tuple(source)], [tuple(source)], 1)
    with pytest.raises(ValueError, match="flags beyond"):
        tickgen_gap.verify_broker([tuple(cleared)], [tuple(source)], 1)


def test_changed_frozen_bytes_are_rejected(tmp_path):
    shutil.copytree(tickgen_gap.FIXTURE, tmp_path / "fixture")
    path = tmp_path / "fixture/native-ticks.csv.gz"
    path.write_bytes(gzip.compress(b"changed"))
    with pytest.raises(ValueError, match="checksum"):
        tickgen_gap.load(path.parent)


@pytest.fixture
def snapshot(tmp_path, fixture):
    for target, name in (
        ("all.csv", "native-ticks.csv"),
        ("preceding.csv", "native-preceding.csv"),
        ("gap-bar.csv", "native-gap-bar.csv"),
        ("metadata.csv", "native-metadata.csv"),
    ):
        (tmp_path / target).write_bytes(
            tickgen_gap.data(tickgen_gap.FIXTURE, fixture.provenance, name)
        )
    return tmp_path


def test_independent_read_only_native_snapshot_passes(snapshot, fixture):
    facts = tickgen_gap.verify_native(snapshot, fixture)
    assert facts["read_only"] == 1


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("gap_ticks", 1),
        ("gap_ticks_error", 4403),
        ("gap_bars", 0),
        ("terminal_build", 1),
        ("read_only", 0),
        ("written_ticks", 1),
    ],
)
def test_native_snapshot_metadata_failures_are_loud(snapshot, fixture, field, value):
    path = snapshot / "metadata.csv"
    parsed = list(csv.DictReader(io.StringIO(path.read_text())))
    for row in parsed:
        if row["key"] == field:
            row["value"] = str(value)
    with path.open("w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=["key", "value"])
        writer.writeheader()
        writer.writerows(parsed)
    with pytest.raises(ValueError):
        tickgen_gap.verify_native(snapshot, fixture)


def test_native_snapshot_still_requires_exact_post_import_flags(snapshot, fixture):
    path = snapshot / "preceding.csv"
    parsed = list(csv.DictReader(io.StringIO(path.read_text())))
    parsed[0]["flags"] = "134"
    with path.open("w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=tickgen_gap.tickgen_capture.TICK_FIELDS)
        writer.writeheader()
        writer.writerows(parsed)
    with pytest.raises(ValueError, match="frozen post-import"):
        tickgen_gap.verify_native(snapshot, fixture)


def test_a_new_resync_of_the_reloaded_symbol_stops_acceptance():
    with pytest.raises(RuntimeError, match="after native tick reload"):
        tickgen_gap.no_repeat_resync(
            "re-sync of BTCUSD.a.GAPTEST: due 202611 (attempt 1 of 3)", "BTCUSD.a.GAPTEST"
        )


def test_other_symbols_resync_does_not_stop_the_reloaded_input():
    tickgen_gap.no_repeat_resync(
        "re-sync of BTCUSD.a: due 202611 (attempt 1 of 3)", "BTCUSD.a.GAPTEST"
    )


@pytest.mark.parametrize("resync", [False, True])
async def test_log_subscription_precedes_reads_and_stops_on_a_resync(monkeypatch, resync):
    class Stream:
        def __init__(self):
            self.stdout = asyncio.StreamReader()
            self.returncode = None

        def terminate(self):
            self.returncode = 0

        async def wait(self):
            return self.returncode

    stream = Stream()
    subscribed = False

    async def subscribe(*arguments, **options):
        nonlocal subscribed
        assert arguments == ("podman", "logs", "--follow", "candidate")
        subscribed = True
        return stream

    async def reads():
        assert subscribed
        if resync:
            stream.stdout.feed_data(b"re-sync of SYMBOL: due 202611\n")
            await asyncio.Event().wait()
        return "answered"

    monkeypatch.setattr(asyncio, "create_subprocess_exec", subscribe)
    if resync:
        with pytest.raises(RuntimeError, match="after native tick reload"):
            await tickgen_gap.subscribed_reads("candidate", "SYMBOL", reads, timeout=1)
    else:
        assert (
            await tickgen_gap.subscribed_reads("candidate", "SYMBOL", reads, timeout=1)
            == "answered"
        )
    assert stream.returncode == 0
