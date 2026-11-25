"""Materialize registered native exports and run the two historical acceptance lanes."""

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
        selected = (
            "tests/live/test_tickgen_shortened_cache.py"
            if shortened
            else "tests/live/test_tickgen_history.py"
        )
        runtime.stream(
            [sys.executable, "-m", "pytest", selected, "-v", "--tb=short"],
            env={
                **environment,
                "MT5_LIVE_HUB_URL": f"ws://127.0.0.1:{port}",
                "MT5_TICK_RAW_DIR": str(capture),
                "MT5_TICK_ACCEPTANCE_REQUIRED": "1",
            },
        )
        runtime.stream(
            [sys.executable, "tests/live/prepare_tickgen.py", "--cleanup", "--name", name],
            env=environment,
        )
        owned.remove(name)
