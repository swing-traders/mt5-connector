"""Materialize native exports and run historical acceptance with its controlled gap input."""

import gzip
import hashlib
import json
import socket
import sys

from . import ROOT

FIXTURES = ROOT / "tests/fixtures/tickgen-btc"


def materialize(destination):
    reference = json.loads((FIXTURES / "manifest.json").read_text())
    for week in [
        *reference["weeks"],
        *reference["floor_tester_jobs"],
        {"job": "floors", "sha256": reference["native_floor_files"]},
    ]:
        output = destination / week["job"] / "out"
        output.mkdir(parents=True)
        for name, digest in week["sha256"].items():
            data = gzip.decompress((FIXTURES / week["job"] / f"{name}.gz").read_bytes())
            if hashlib.sha256(data).hexdigest() != digest:
                raise ValueError("a registered native tester export has an invalid checksum")
            (output / name).write_bytes(data)
        (output / "provenance.json").write_bytes(
            (FIXTURES / week["job"] / "provenance.json").read_bytes()
        )
    return reference


def run(runtime, image, seed, stem, output, environment, owned):
    for shortened in (True, False):
        name = f"{stem}-shortened" if shortened else f"{stem}-history"
        capture = output / ("shortened" if shortened else "history")
        with socket.socket() as available:
            available.bind(("127.0.0.1", 0))
            port = available.getsockname()[1]
        owned.append(name)
        arguments = [
            sys.executable,
            "tests/live/prepare_tickgen.py",
            "--image",
            image,
            "--seed-container",
            seed,
            "--name",
            name,
            "--port",
            str(port),
            "--output",
            str(capture.relative_to(ROOT)),
        ]
        if shortened:
            arguments.append("--shorten-cache")
        runtime.stream(arguments, env=environment)
        gap_name = None
        test_environment = {
            **environment,
            "MT5_LIVE_HUB_URL": f"ws://127.0.0.1:{port}",
            "MT5_TICK_RAW_DIR": str(capture),
            "MT5_TICK_ACCEPTANCE_REQUIRED": "1",
        }
        if not shortened:
            gap_name = f"{stem}-gap"
            gap_capture = output / "gap"
            with socket.socket() as available:
                available.bind(("127.0.0.1", 0))
                gap_port = available.getsockname()[1]
            owned.append(gap_name)
            runtime.stream(
                [
                    sys.executable,
                    "tests/live/prepare_gap.py",
                    "--image",
                    image,
                    "--name",
                    gap_name,
                    "--port",
                    str(gap_port),
                    "--output",
                    str(gap_capture),
                    "--env-file",
                    environment["MT5_FUNCTIONAL_ENV_FILE"],
                ],
                env=environment,
            )
            test_environment["MT5_GAP_PREPARED_DIR"] = str(gap_capture)
        selected = (
            "tests/live/test_tickgen_shortened_cache.py"
            if shortened
            else "tests/live/test_tickgen_history.py"
        )
        runtime.stream(
            [sys.executable, "-m", "pytest", selected, "-v", "--tb=short"],
            env=test_environment,
        )
        if gap_name is not None:
            runtime.stream(
                [sys.executable, "tests/live/prepare_tickgen.py", "--cleanup", "--name", gap_name],
                env=environment,
            )
            owned.remove(gap_name)
        runtime.stream(
            [sys.executable, "tests/live/prepare_tickgen.py", "--cleanup", "--name", name],
            env=environment,
        )
        owned.remove(name)
