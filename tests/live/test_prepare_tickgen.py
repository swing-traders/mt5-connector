"""The tick-generator acceptance's preparation driver refusing before it starts anything: a name it
would create that already exists, and a flag whose value cannot be used."""

import argparse
import json
import sys

import prepare_tickgen
import pytest


class Started(Exception):
    """A container command the driver reached."""


@pytest.fixture
def commands(monkeypatch) -> list[tuple]:
    """Every container command the driver sends, none of which runs."""
    sent: list[tuple] = []

    def podman(*arguments: str, check: bool = True) -> str:
        sent.append(arguments)
        raise Started(arguments)

    monkeypatch.setattr(prepare_tickgen, "podman", podman)
    return sent


def test_an_existing_container_named_like_its_compile_container_refuses_before_any_command(
    monkeypatch, commands
):
    monkeypatch.setattr(prepare_tickgen, "exists", lambda kind, name: name == "probe-compile")
    arguments = argparse.Namespace(
        image="localhost/candidate",
        seed_container="seed",
        name="probe",
        port=21588,
        output="scripts/tickgen-test",
        cleanup=False,
    )

    with pytest.raises(prepare_tickgen.Unprepared, match="probe-compile"):
        prepare_tickgen.prepare(arguments)

    assert commands == []


def test_a_port_outside_the_port_range_is_refused_before_any_command(monkeypatch, commands):
    monkeypatch.setattr(prepare_tickgen, "exists", lambda kind, name: False)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "prepare_tickgen.py",
            "--image",
            "localhost/candidate",
            "--seed-container",
            "seed",
            "--name",
            "probe",
            "--port",
            "-1",
            "--output",
            "scripts/tickgen-test",
        ],
    )

    with pytest.raises(SystemExit) as refused:
        prepare_tickgen.main()

    assert refused.value.code == 2
    assert commands == []


def test_shorten_cache_with_cleanup_is_refused_before_any_command(monkeypatch, commands):
    monkeypatch.setattr(
        sys,
        "argv",
        ["prepare_tickgen.py", "--name", "probe", "--shorten-cache", "--cleanup"],
    )

    with pytest.raises(SystemExit) as refused:
        prepare_tickgen.main()

    assert refused.value.code == 2
    assert commands == []


def test_an_output_directory_holding_an_earlier_capture_is_refused_before_any_command(
    monkeypatch, commands, tmp_path
):
    earlier = tmp_path / "scripts" / "tickgen-acceptance" / "raw-day-20240703"
    earlier.mkdir(parents=True)
    (earlier / "day-20240703-ticks.csv").write_text("time\n")
    monkeypatch.setattr(prepare_tickgen, "CHECKOUT", tmp_path)
    monkeypatch.setattr(prepare_tickgen, "exists", lambda kind, name: False)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "prepare_tickgen.py",
            "--image",
            "localhost/candidate",
            "--seed-container",
            "seed",
            "--name",
            "probe",
            "--port",
            "21588",
            "--output",
            "scripts/tickgen-acceptance",
        ],
    )

    with pytest.raises(SystemExit) as refused:
        prepare_tickgen.main()

    assert refused.value.code == 2
    assert commands == []


def test_shortened_cache_preparation_needs_a_native_read_but_no_tick_era_hole(
    monkeypatch, tmp_path
):
    import mt5connector.client

    monkeypatch.setattr(prepare_tickgen, "CHECKOUT", tmp_path)
    monkeypatch.setattr(mt5connector.client, "__file__", str(tmp_path / "client.py"))
    monkeypatch.setattr(prepare_tickgen, "exists", lambda *args: False)
    monkeypatch.setattr(prepare_tickgen, "podman", lambda *args, **kwargs: "image-id")
    monkeypatch.setattr(prepare_tickgen, "seed_settings", lambda seed: ({}, "seed-volume"))
    monkeypatch.setattr(
        prepare_tickgen,
        "compile_sources",
        lambda *args: {
            "image_ticks_sha256": "source-hash",
            "ticks": "Result: 0 errors, 0 warnings",
            "tickgen_raw": "Result: 0 errors, 0 warnings",
            "terminal_build": "test-build",
        },
    )
    monkeypatch.setattr(prepare_tickgen, "clone_history", lambda *args: "private-clone")
    monkeypatch.setattr(prepare_tickgen, "m1_cache_bytes", lambda *args: 100)
    monkeypatch.setattr(prepare_tickgen, "start_candidate", lambda *args: None)
    monkeypatch.setattr(prepare_tickgen, "sha256", lambda path: "source-hash")
    monkeypatch.setattr(prepare_tickgen, "outcome_m1_first", lambda *args: 1_800_000_000)
    captured = []

    def collect(image, name, env_file, clone, windows, capture, output, *, maxbars=100_000_000):
        captured.append((capture, maxbars))
        directory = output / f"raw-{capture}"
        directory.mkdir()
        return directory

    def missing_hole(*arguments):
        raise prepare_tickgen.Unprepared("no native tick-era hole")

    async def settled(port):
        return {
            "terminal_build": "test-build",
            "account": {"trade_allowed": True, "trade_mode": 0},
            "ranges": {},
            "maxbars": 100_000_000,
        }

    monkeypatch.setattr(prepare_tickgen, "collect", collect)
    monkeypatch.setattr(prepare_tickgen, "mixed_fixture", missing_hole)
    monkeypatch.setattr(prepare_tickgen, "settled", settled)
    prepare_tickgen.prepare(
        argparse.Namespace(
            image="candidate",
            seed_container="seed",
            name="probe",
            port=12345,
            output="scripts/shortened",
            shorten_cache=True,
        )
    )
    prepared = json.loads((tmp_path / "scripts/shortened/provenance.json").read_text())
    assert captured == [("baseline", 100_000), ("m1-first", 100_000)]
    assert prepared["m1_first_utc"] == 1_800_000_000
    assert prepared["collector_maxbars"] == 100_000
    assert prepared["fixture"] == {"directory": "raw-baseline", "label": "baseline"}
