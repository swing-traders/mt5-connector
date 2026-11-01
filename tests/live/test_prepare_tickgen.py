"""The tick-generator acceptance's preparation driver refusing before it starts anything: a name it
would create that already exists, and a flag whose value cannot be used."""

import argparse
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
