import argparse
import json
import shutil
import subprocess
from datetime import timedelta
from zoneinfo import ZoneInfo

import prepare_live
import pytest

from mt5connector.wire.broker_clock import BrokerClock


@pytest.fixture
def native_capture(tmp_path):
    captured = tmp_path / "raw-live"
    frame = tmp_path / "raw-capped"
    captured.mkdir()
    frame.mkdir()
    (captured / "outcome.txt").write_text(
        "done\nterminal_build 6316\nm1_first 100\nmaxbars 100000000\n"
        "first_tick 1795132800\nwindow_five-days 1795132860 1795564859\n"
        "five-days 10000 7200\nfirst_tick 1795132800\n"
        "window_minute-1424 1795357440 1795357499\nminute-1424 20 1\n"
    )
    (frame / "outcome.txt").write_text(
        "done\nterminal_build 6316\nm1_first 1795317000\nmaxbars 5000\n"
    )
    return captured, frame


@pytest.mark.parametrize(("timezone", "offset"), [("America/New_York", 7), ("Europe/London", 2)])
def test_the_recorded_windows_and_cap_come_from_native_outcomes(native_capture, timezone, offset):
    captured, frame = native_capture
    clock = BrokerClock(ZoneInfo(timezone), timedelta(hours=offset))
    record = prepare_live.provenance(captured.parent, captured, frame, clock)
    assert record["windows"]["five-days"] == {
        "from_broker": 1795132860,
        "through_broker": 1795564859,
        "start_utc": clock.to_utc(1795132860),
        "end_utc_exclusive": clock.to_utc(1795564860),
        "ticks": 10000,
        "bars": 7200,
    }
    assert record["windows"]["minute-1424"]["bars"] == 1
    assert record["cap"] == {
        "requested": 5000,
        "native": 5000,
        "m1_first_broker": 1795317000,
    }
    assert record["captures"]["raw-live"]["outcome.txt"] == prepare_live.native.sha256(
        captured / "outcome.txt"
    )
    assert json.loads(json.dumps(record)) == record
    assert record["clock"] == {"timezone": timezone, "offset_hours": offset}


@pytest.mark.parametrize(
    "outcome",
    [
        "done\nterminal_build 6316\nm1_first 1795317000\nmaxbars 100000\n",
        "done\nterminal_build 6316\nm1_first 1795132860\nmaxbars 5000\n",
    ],
    ids=["ineffective-cap", "insufficient-gap"],
)
def test_an_unusable_native_cap_fails_before_candidate_tests(native_capture, outcome):
    captured, frame = native_capture
    (frame / "outcome.txt").write_text(outcome)
    with pytest.raises(prepare_live.native.Unprepared, match="interior floor-gap"):
        prepare_live.provenance(
            captured.parent,
            captured,
            frame,
            BrokerClock(ZoneInfo("America/New_York"), timedelta(hours=7)),
        )


@pytest.mark.parametrize("persisted", [False, True], ids=["fresh-seed", "persisted-seed"])
def test_a_seed_is_capped_from_the_closed_native_capture(
    monkeypatch, tmp_path, native_capture, persisted
):
    captured, frame = native_capture
    live_outcome = (captured / "outcome.txt").read_text()
    capped_outcome = (frame / "outcome.txt").read_text()
    seed = tmp_path / "seed"
    history = seed / "server/history/BTCUSD.a"
    history.mkdir(parents=True)
    (history / "2026.hcc").write_bytes(b"settled bar store")
    if persisted:
        (history / "cache").mkdir()
        (history / "cache/M1.hc").write_bytes(b"previous seed cache")
    volumes = {"seed": seed}
    monkeypatch.setattr(prepare_live.native, "CHECKOUT", tmp_path)
    source = tmp_path / "tests/live/tickgen_raw.mq5"
    source.parent.mkdir(parents=True)
    source.write_text("native collector")
    monkeypatch.setattr(prepare_live.native, "seed_settings", lambda name: ({}, "seed"))
    monkeypatch.setattr(
        prepare_live.native, "compile_sources", lambda *args: {"tickgen_raw": "clean"}
    )

    def clone_history(image, name, source, clone):
        destination = tmp_path / clone
        shutil.copytree(volumes[source], destination)
        volumes[clone] = destination
        return clone

    def collect(image, name, env_file, clone, windows, capture, output, *, maxbars=100_000_000):
        cache = volumes[clone] / "server/history/BTCUSD.a/cache"
        directory = output / f"raw-{capture}"
        directory.mkdir()
        if capture == "live":
            cache.mkdir(exist_ok=True)
            (cache / "M1.hc").write_bytes(b"native cache persisted on close")
            (directory / "outcome.txt").write_text(live_outcome)
        else:
            assert (cache / "M1.hc.full").read_bytes() == b"native cache persisted on close"
            assert maxbars == prepare_live.CAP
            (cache / "M1.hc").write_bytes(b"rebuilt capped cache")
            (directory / "outcome.txt").write_text(capped_outcome)
        return directory

    def podman(*arguments):
        if arguments[0] == "image":
            return "image-id"
        volume = arguments[arguments.index("-v") + 1].split(":")[0]
        done = subprocess.run(
            ["sh", "-c", arguments[-1].replace("/bases", str(volumes[volume]))],
            capture_output=True,
            text=True,
            check=False,
        )
        if done.returncode:
            raise prepare_live.native.Unprepared(done.stderr)
        return done.stdout

    monkeypatch.setattr(prepare_live.native, "clone_history", clone_history)
    monkeypatch.setattr(prepare_live.native, "collect", collect)
    monkeypatch.setattr(prepare_live.native, "podman", podman)
    output = tmp_path / "scripts/live"
    prepare_live.prepare(
        argparse.Namespace(image="image", seed_container="seed", name="probe", output=output)
    )
    recorded = json.loads((output / "provenance.json").read_text())
    assert recorded["capped_volume"] == "probe-capped-bases"
    assert (history / "2026.hcc").read_bytes() == b"settled bar store"
    if persisted:
        assert (history / "cache/M1.hc").read_bytes() == b"previous seed cache"
    assert (
        volumes["probe-native-bases"] / "server/history/BTCUSD.a/cache/M1.hc"
    ).read_bytes() == b"native cache persisted on close"
