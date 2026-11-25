"""Install a native gap, complete normal re-sync, and reload its ticks before acceptance."""

import argparse
import asyncio
import json
import tempfile
import time
from enum import StrEnum
from pathlib import Path

import prepare_tickgen
import tickgen_gap

from functional.__main__ import connected
from functional.runtime import BASES, Runtime
from functional.settings import Redactor, load, write
from mt5connector.server.hub_resync import Outcome
from mt5connector.wire import calls

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
EA_SOURCE = "packages/server/mt5connector/server/mql5/Expert/ticks.mq5"


class Mode(StrEnum):
    INSTALL = "install"
    READ = "read"
    RELOAD = "reload"


def native(runtime, image, name, env_file, volume, output, mode):
    job = output / f"native-{mode}"
    job.mkdir()
    (job / "input").mkdir()
    (job / "out").mkdir(mode=0o777)
    (job / "out").chmod(0o777)
    fixture = tickgen_gap.load()
    for target, source in (
        ("native-ticks.csv", "broker-ticks.csv"),
        ("native-bars.csv", "bars.csv"),
    ):
        (job / "input" / target).write_bytes(
            tickgen_gap.data(tickgen_gap.FIXTURE, fixture.provenance, source)
        )
    container = f"{name}-native-{mode}"
    runtime.containers.append(container)
    runtime.command(
        "podman",
        "run",
        "-d",
        "--no-healthcheck",
        "--name",
        container,
        "--label",
        f"{prepare_tickgen.LABEL}={name}",
        "--env-file",
        str(env_file),
        "--security-opt",
        "label=disable",
        "-e",
        f"PROBE_MODE={mode}",
        "--stop-timeout",
        "190",
        "-v",
        f"{volume}:{BASES}",
        "-v",
        f"{job}:/job",
        "-v",
        f"{HERE / 'gap_native.mq5'}:/src/NativeHistoryExport.mq5:ro",
        "-v",
        f"{HERE / 'gap_native.sh'}:/opt/probe/install.sh:ro",
        "--entrypoint",
        "/usr/bin/tini",
        image,
        "--",
        "/bin/bash",
        "/opt/probe/install.sh",
    )
    deadline = time.monotonic() + 1200
    while True:
        state = json.loads(runtime.command("podman", "inspect", container))[0]["State"]
        if not state["Running"]:
            printed = runtime.logs(container)
            print(runtime.redact(printed), flush=True)
            if state["ExitCode"] or "CUSTOM_PROBE_NATIVE_INSTALL_FINISHED" not in printed:
                raise RuntimeError(f"native gap {mode} did not complete")
            break
        if time.monotonic() > deadline:
            raise TimeoutError(f"native gap {mode} exceeded its bound")
        time.sleep(1)
    runtime.stop(container)
    facts = tickgen_gap.verify_native(job / "out", fixture, read_only=mode is Mode.READ)
    if mode is Mode.RELOAD and (
        facts["reload"] != 1
        or facts["written_bars"] != fixture.provenance["counts"]["bars"]
        or facts["written_ticks"] != fixture.provenance["counts"]["broker_ticks"]
        or facts["deleted_ticks"] != fixture.provenance["counts"]["removed_ticks"]
    ):
        raise ValueError("native gap reload did not restore its captured input")
    print(f"GAP_NATIVE_{mode.upper()}_VERIFIED " + json.dumps(facts), flush=True)
    return job / "out"


async def completed(runtime, port, symbol):
    session = await connected(runtime, port)
    try:
        account = (await asyncio.wait_for(session.request(calls.Call.ACCOUNT_INFO), 120)).result
        runtime.redact.hide(account.name, account.company)
        answered = await asyncio.wait_for(
            session.deferring(calls.Call.RANGES, symbol=symbol),
            3 * (600 + calls.HISTORY_BUSY_S) + 60,
        )
        if answered.result.resync_failure:
            raise RuntimeError(f"gap history re-sync failed: {answered.result.resync_failure}")
        return {
            "first_tick": answered.result.first_tick,
            "claimable_through": answered.result.claimable_through,
            "resync_failure": answered.result.resync_failure,
        }
    finally:
        await session.disconnect()


def record(runtime, container, symbol):
    source = (
        "import json; from pathlib import Path; "
        f"p=Path({str(Path(BASES) / '.mt5-connector/resync.json')!r}); "
        "print(json.dumps(json.loads(p.read_text()) if p.exists() else {}))"
    )
    return json.loads(runtime.command("podman", "exec", container, "python", "-c", source)).get(
        symbol
    )


def logs(runtime, container):
    import subprocess

    captured = subprocess.run(
        ["podman", "logs", container],
        capture_output=True,
        text=True,
        check=True,
        errors="replace",
    )
    return runtime.redact(captured.stdout + captured.stderr)


def prepare(arguments, runtime, values):
    output = arguments.output.resolve()
    output.mkdir(parents=True)
    fixture = tickgen_gap.load()
    symbol = fixture.provenance["symbol"]
    volume = arguments.name + "-bases"
    runtime.command(
        "podman",
        "volume",
        "create",
        "--label",
        f"{prepare_tickgen.LABEL}={arguments.name}",
        volume,
    )
    with tempfile.TemporaryDirectory(prefix="gap-") as private:
        env_file = Path(private) / ".env"
        write(env_file, values)
        native(runtime, arguments.image, arguments.name, env_file, volume, output, Mode.INSTALL)
        prepare_tickgen.start_candidate(
            arguments.image, arguments.name, env_file, volume, arguments.port
        )
        runtime.healthy(arguments.name, arguments.port)
        completion = asyncio.run(completed(runtime, arguments.port, symbol))
        deadline = time.monotonic() + 60
        persisted = record(runtime, arguments.name, symbol)
        while persisted is None or Outcome(persisted["outcome"]) is not Outcome.DOWNLOADED:
            if time.monotonic() > deadline:
                raise TimeoutError("hub did not persist its completed gap re-sync")
            time.sleep(0.1)
            persisted = record(runtime, arguments.name, symbol)
        (output / "completed-resync.json").write_text(json.dumps(persisted, indent=2) + "\n")
        (output / "initial-hub.log").write_text(logs(runtime, arguments.name))
        print("GAP_HUB_RESYNC_COMPLETED " + json.dumps(completion), flush=True)
        prepare_tickgen.podman("stop", "--time", "190", arguments.name)
        prepare_tickgen.podman("rm", arguments.name)
        snapshot = native(
            runtime, arguments.image, arguments.name, env_file, volume, output, Mode.RELOAD
        )
        prepare_tickgen.start_candidate(
            arguments.image, arguments.name, env_file, volume, arguments.port
        )
        runtime.healthy(arguments.name, arguments.port)
        tickgen_gap.no_repeat_resync(logs(runtime, arguments.name), symbol)
        if record(runtime, arguments.name, symbol) != persisted:
            raise RuntimeError("hub changed its completed re-sync record after native tick reload")
    reference = output / "reference"
    reference.mkdir()
    for target, source in (("gap-ticks.csv", "native-ticks.csv"), ("gap-bars.csv", "bars.csv")):
        (reference / target).write_bytes(
            tickgen_gap.data(tickgen_gap.FIXTURE, fixture.provenance, source)
        )
    provenance = {
        "symbol": symbol,
        "container": arguments.name,
        "image": arguments.image,
        "image_id": prepare_tickgen.podman(
            "image", "inspect", "--format", "{{.Id}}", arguments.image
        ).strip(),
        "port": arguments.port,
        "fixture": {
            "directory": reference.name,
            "label": "gap",
            "start": fixture.provenance["range"]["from_broker"],
            "empty": fixture.provenance["removed_minute_broker"],
        },
        "snapshot": str(snapshot.relative_to(output)),
        "completion": completion,
        "resync_record": persisted,
        "source_sha256": {EA_SOURCE: prepare_tickgen.sha256(ROOT / EA_SOURCE)},
    }
    (output / "provenance.json").write_text(json.dumps(provenance, indent=2) + "\n")
    print("GAP_READY_AFTER_NATIVE_RELOAD " + str(output), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True)
    parser.add_argument("--name", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, required=True)
    arguments = parser.parse_args()
    if arguments.port not in prepare_tickgen.PORTS:
        parser.error("--port is not a valid port")
    if arguments.output.exists() and any(arguments.output.iterdir()):
        parser.error("--output is not empty")
    if prepare_tickgen.owned(arguments.name) != {"containers": [], "volumes": []}:
        parser.error("--name already owns native gap resources")
    prepare_tickgen.podman("image", "inspect", arguments.image)
    values = load(arguments.env_file)
    runtime = Runtime(Redactor(values))
    try:
        prepare(arguments, runtime, values)
    except BaseException:
        prepare_tickgen.cleanup(arguments.name)
        raise


if __name__ == "__main__":
    main()
