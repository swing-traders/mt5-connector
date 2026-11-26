"""Prepare isolated history clones, native frame witnesses and candidate hub provenance.

The shortened-cache setup rebuilds M1 at MaxBars=100000. The seed is only read.
"""

import argparse
import asyncio
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from tickgen_reference import REFERENCE

from functional.runtime import Runtime
from functional.settings import Redactor, load
from mt5connector.client.config import MT5Config
from mt5connector.client.hub import HubSession
from mt5connector.wire import calls
from mt5connector.wire.broker_clock import BrokerClock

CHECKOUT = Path(__file__).resolve().parents[2]
SYMBOL = REFERENCE["symbol"]
LABEL = "io.swing-traders.tickgen-acceptance"
BASES = "/home/mt5/.wine/drive_c/Program Files/MetaTrader 5/Bases"
TERMINAL = "/home/mt5/.wine/drive_c/Program Files/MetaTrader 5"
CREDENTIALS = ("MT5_LOGIN", "MT5_PASSWORD", "MT5_SERVER", "MT5_SPAWNER_SYMBOL")
CLOCK_SETTINGS = ("MT5_BROKER_TZ", "MT5_BROKER_OFFSET_HOURS")
HEALTH_BOUND_S = 1800
RANGES_BOUND_S = 4 * 3600
COLLECTOR_BOUND_S = 2 * 3600
POLL_S = 10
# What a run for a name creates, each refused up front when it already exists.
CONTAINERS = ("{name}", "{name}-compile", "{name}-raw")
VOLUMES = ("{name}-bases",)
PORTS = range(1, 65536)
SOURCES = (
    "packages/server/mt5connector/server/mql5/Expert/ticks.mq5",
    "tests/live/tickgen_raw.mq5",
    "tests/live/tickgen_oracle.py",
    "tests/live/tickgen_reference.py",
    "tests/fixtures/tickgen-btc/manifest.json",
    "tests/live/tickgen_capture.py",
    "tests/live/prepare_tickgen.py",
    "tests/live/test_tickgen_history.py",
    "tests/live/test_tickgen_shortened_cache.py",
)


class Unprepared(Exception):
    pass


def podman(*arguments: str, check: bool = True) -> str:
    """The command's standard output; a failure raises with its standard error, never its
    arguments' environment."""
    command = ["podman", *arguments]
    if arguments[0] == "run":
        command.insert(2, "--no-healthcheck")
    done = subprocess.run(command, capture_output=True, text=True)
    if check and done.returncode != 0:
        raise Unprepared(f"podman {arguments[0]} failed ({done.returncode}): {done.stderr.strip()}")
    return done.stdout


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def log(message: str) -> None:
    print(f"prepare_tickgen: {message}", flush=True)


def exists(kind: str, name: str) -> bool:
    """Whether a container or volume of that name exists, whoever made it."""
    return subprocess.run(["podman", kind, "exists", name], capture_output=True).returncode == 0


def owned(name: str) -> dict[str, list[str]]:
    """The containers and volumes carrying this driver's label for `name`."""
    selector = f"label={LABEL}={name}"
    containers = podman("ps", "-a", "--filter", selector, "--format", "{{.Names}}").split()
    volumes = podman("volume", "ls", "--filter", selector, "--format", "{{.Name}}").split()
    return {"containers": containers, "volumes": volumes}


def cleanup(name: str) -> None:
    found = owned(name)
    for container in found["containers"]:
        count = int(podman("inspect", "--format", "{{.RestartCount}}", container).strip())
        for ordinal in range(1, count + 1):
            log(f"{container}: restart {ordinal} observed before cleanup")
        podman("stop", "--time", "190", container, check=False)
        podman("rm", "--force", container)
        log(f"removed container {container}")
    for volume in found["volumes"]:
        podman("volume", "rm", volume)
        log(f"removed volume {volume}")


def seed_settings(seed: str) -> tuple[dict[str, str], str]:
    """The seed container's login settings and clock settings, and its history volume."""
    inspected = json.loads(podman("inspect", seed))[0]
    environment = dict(entry.split("=", 1) for entry in inspected["Config"]["Env"])
    missing = [name for name in CREDENTIALS if not environment.get(name)]
    if missing:
        raise Unprepared(f"the seed container {seed} sets no {', '.join(missing)}")
    volumes = [
        mount["Name"]
        for mount in inspected["Mounts"]
        if mount["Type"] == "volume" and mount["Destination"] == BASES
    ]
    if len(volumes) != 1:
        raise Unprepared(f"the seed container {seed} mounts no history volume at {BASES}")
    settings = {
        name: environment[name] for name in CREDENTIALS + CLOCK_SETTINGS if name in environment
    }
    return settings, volumes[0]


def broker_clock(settings: dict[str, str]) -> BrokerClock:
    return BrokerClock(
        ZoneInfo(settings.get("MT5_BROKER_TZ", "America/New_York")),
        timedelta(hours=int(settings.get("MT5_BROKER_OFFSET_HOURS", "7"))),
    )


def compile_sources(image: str, name: str, output: Path) -> dict[str, str]:
    """The collector compiled from the checkout and the image's own EA compiled again, in a
    throwaway container from the image; each one's MetaEditor result line, the image EA source's
    hash and the image's terminal build."""
    container = f"{name}-compile"
    script = r"""
set -e
terminal="$WINEPREFIX/drive_c/Program Files/MetaTrader 5"
sources=$(python -c 'import os, mt5connector.server as s; print(os.path.dirname(s.__file__))')/mql5
stage="$WINEPREFIX/drive_c/mt5build"
mkdir -p "$stage"
cp "$sources/Expert/ticks.mq5" /src/tickgen_raw.mq5 "$stage/"
cp -R "$sources/Include" "$stage/"
python -m mt5connector.server.mql5gen "$stage/Include/hub_wire.mqh" >/dev/null
for source in ticks tickgen_raw; do
    wine "$terminal/MetaEditor64.exe" "/compile:C:\\mt5build\\$source.mq5" \
        "/log:C:\\mt5build\\$source.log" || :
    wineserver -w
    echo "$source $(iconv -f UTF-16 -t UTF-8 "$stage/$source.log" | tr -d '\r' | grep '^Result:')"
done
echo "image_ticks_sha256 $(sha256sum "$sources/Expert/ticks.mq5" | cut -d' ' -f1)"
echo "terminal_build $(cat "$HOME/.terminal-build")"
"""
    podman(
        "run",
        "--name",
        container,
        "--label",
        f"{LABEL}={name}",
        "--entrypoint",
        "tini",
        "-v",
        f"{CHECKOUT / 'tests/live/tickgen_raw.mq5'}:/src/tickgen_raw.mq5:ro,z",
        image,
        "--",
        "xvfb-run",
        "-a",
        "sh",
        "-c",
        script,
    )
    lines = podman("logs", container).splitlines()
    facts = dict(line.split(" ", 1) for line in lines if " " in line)
    collector = output / "collector"
    collector.mkdir(parents=True, exist_ok=True)
    podman("cp", f"{container}:/home/mt5/.wine/drive_c/mt5build/tickgen_raw.ex5", str(collector))
    podman("rm", "--force", container)
    for source in ("ticks", "tickgen_raw"):
        if not facts.get(source, "").startswith("Result: 0 errors, 0 warnings"):
            raise Unprepared(f"{source}.mq5 did not compile clean: {facts.get(source)}")
    return facts


def clone_history(image: str, name: str, seed_volume: str, clone: str) -> str:
    """A new volume labelled for `name` holding a copy of the seed's history."""
    podman("volume", "create", "--label", f"{LABEL}={name}", clone)
    log(f"copying {seed_volume} into {clone}")
    podman(
        "run",
        "--rm",
        "--user",
        "root",
        "--entrypoint",
        "rsync",
        "-v",
        f"{seed_volume}:/from:ro",
        "-v",
        f"{clone}:/to",
        image,
        "-a",
        "/from/",
        "/to/",
    )
    return clone


def m1_cache_bytes(image: str, clone: str) -> int:
    """The size of the symbol's M1 frame cache in the clone."""
    found = podman(
        "run",
        "--rm",
        "--user",
        "root",
        "--entrypoint",
        "sh",
        "-v",
        f"{clone}:/bases:ro",
        image,
        "-c",
        f"stat -c %s /bases/*/history/{SYMBOL}/cache/M1.hc",
    ).split()
    if len(found) != 1:
        raise Unprepared(f"{clone} holds {len(found)} {SYMBOL} M1 frame caches")
    return int(found[0])


def collect(
    image: str,
    name: str,
    env_file: Path,
    clone: str,
    windows: list[tuple[str, int, int]],
    capture: str,
    output: Path,
    *,
    maxbars: int = 100_000_000,
) -> Path:
    """The collector's capture of each broker window, read in its own terminal on the clone, under
    raw-`capture`."""
    container = f"{name}-raw"
    collector = output / "collector"
    parameters = collector / "tickgen_raw.txt"
    parameters.write_text(
        SYMBOL + "\n" + "".join(f"{label} {lo} {hi}\n" for label, lo, hi in windows)
    )
    baked_ini = podman("run", "--rm", "--entrypoint", "cat", image, "/opt/mt5/setup.ini")
    setup = collector / "setup.ini"
    setup.write_text(baked_ini.replace("Script=ticks_setup", "Script=tickgen_raw"))
    entrypoint = podman("run", "--rm", "--entrypoint", "cat", image, "/opt/mt5/entrypoint.sh")
    definitions, boundary, _ = entrypoint.partition(
        "# A boot step failing because a supervised process stopped"
    )
    if not boundary:
        raise Unprepared("the image has no native bootstrap definitions")
    native_boot = collector / "native-entrypoint.sh"
    # A reference collector runs only the terminal; it neither attaches an EA nor starts a hub.
    native_boot.write_text(definitions + """
trap end_with_stopped EXIT
trap stop_on_request TERM INT
require_settings
start_display
restore_baked_terminal
write_inis
acquire_server_list
allow_webrequest
supervise
""")
    baked = "/opt/mt5-baked/terminal/MQL5"
    podman(
        "run",
        "-d",
        "--name",
        container,
        "--label",
        f"{LABEL}={name}",
        "--entrypoint",
        "tini",
        "--env-file",
        str(env_file),
        "-e",
        f"MT5_MAX_BARS={maxbars}",
        "-v",
        f"{clone}:{BASES}",
        "-v",
        f"{setup}:/opt/mt5/setup.ini:ro,z",
        "-v",
        f"{native_boot}:/opt/mt5/native-entrypoint.sh:ro,z",
        "-v",
        f"{collector / 'tickgen_raw.ex5'}:{baked}/Scripts/tickgen_raw.ex5:ro,z",
        "-v",
        f"{parameters}:{baked}/Files/tickgen_raw.txt:ro,z",
        "--stop-timeout",
        "190",
        image,
        "--",
        "bash",
        "/opt/mt5/native-entrypoint.sh",
    )
    outcome_path = f"{TERMINAL}/MQL5/Files/tickgen-raw/outcome.txt"
    deadline = time.monotonic() + COLLECTOR_BOUND_S
    outcome = ""
    while outcome == "":
        state = podman("inspect", "--format", "{{.State.Running}}", container).strip()
        if state != "true":
            raise Unprepared(f"the collector container {container} stopped before its outcome")
        elif time.monotonic() > deadline:
            raise Unprepared(f"the collector wrote no outcome within {COLLECTOR_BOUND_S} s")
        outcome = podman("exec", container, "cat", outcome_path, check=False)
        if outcome == "":
            time.sleep(POLL_S)
    target = output / f"raw-{capture}"
    podman("cp", f"{container}:{TERMINAL}/MQL5/Files/tickgen-raw", str(target))
    podman("stop", "--time", "190", container)
    podman("rm", container)
    if not outcome.startswith("done"):
        raise Unprepared(f"the collector {outcome.strip()}")
    log(f"captured {capture}: {', '.join(label for label, _, _ in windows)}")
    return target


def outcome_m1_first(directory: Path, clock: BrokerClock) -> int:
    """The first date of the symbol's M1 frame a collector capture's outcome names, in true UTC:
    as its terminal held it before any read."""
    lines = (directory / "outcome.txt").read_text().splitlines()
    broker = [int(line.split()[1]) for line in lines if line.startswith("m1_first ")]
    if len(broker) != 1:
        raise Unprepared(f"the collector's outcome names {len(broker)} M1 first dates")
    return clock.to_utc(broker[0])


def start_candidate(image: str, name: str, env_file: Path, clone: str, port: int) -> None:
    podman(
        "run",
        "-d",
        "--restart",
        "unless-stopped",
        "--name",
        name,
        "--label",
        f"{LABEL}={name}",
        "--env-file",
        str(env_file),
        "-p",
        f"127.0.0.1:{port}:9000",
        "-v",
        f"{clone}:{BASES}",
        "--stop-timeout",
        "190",
        image,
    )
    Runtime(Redactor(load(env_file))).healthy(name, port, seconds=HEALTH_BOUND_S)
    log("the candidate's hub is healthy")


class Log:
    def debug(self, message: str) -> None:
        pass

    def info(self, message: str) -> None:
        pass

    def warning(self, message: str) -> None:
        log(message)

    def error(self, message: str) -> None:
        log(message)

    def exception(self, message: str, failure: BaseException) -> None:
        log(f"{message}: {failure!r}")


async def settled(port: int) -> dict:
    """The candidate's symbol ranges, asked until its history EA has measured the symbol, with the
    account's permission and the running terminal's build."""
    session = HubSession(
        MT5Config(account=1, server="unused", ws_url=f"ws://127.0.0.1:{port}"),
        asyncio.get_running_loop(),
        lambda frame: None,
        None,
        Log(),
    )
    await session.connect()
    try:
        ranges = await asyncio.wait_for(
            session.deferring(calls.Call.RANGES, symbol=SYMBOL), RANGES_BOUND_S
        )
        account = (await session.deferring(calls.Call.ACCOUNT_INFO)).result
        terminal = (await session.deferring(calls.Call.TERMINAL_INFO)).result
    finally:
        await session.disconnect()
    return {
        "ranges": {series.name: held.floor for series, held in ranges.result.series.items()},
        "maxbars": ranges.result.maxbars,
        "account": {"trade_allowed": account.trade_allowed, "trade_mode": account.trade_mode},
        "terminal_build": terminal.build,
    }


def prepare(arguments: argparse.Namespace) -> None:
    taken = [
        name
        for kind, names in (
            ("container", CONTAINERS),
            ("volume", VOLUMES),
        )
        for name in (pattern.format(name=arguments.name) for pattern in names)
        if exists(kind, name)
    ]
    if taken:
        raise Unprepared(f"{', '.join(taken)} already exist")

    import mt5connector.client

    if not Path(mt5connector.client.__file__).resolve().is_relative_to(CHECKOUT):
        raise Unprepared(f"mt5connector.client resolves outside {CHECKOUT}")
    output = (CHECKOUT / arguments.output).resolve()
    if not output.is_relative_to(CHECKOUT / "scripts"):
        raise Unprepared(f"--output {output} is not under the checkout's scripts/")
    image_id = podman("image", "inspect", "--format", "{{.Id}}", arguments.image).strip()
    settings, seed_volume = seed_settings(arguments.seed_container)
    clock = broker_clock(settings)
    output.mkdir(parents=True, exist_ok=True)
    compiled = compile_sources(arguments.image, arguments.name, output)
    private = Path(tempfile.mkdtemp(prefix="tickgen-"))
    try:
        env_file = private / "env"
        descriptor = os.open(env_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w") as file:
            file.write("".join(f"{key}={value}\n" for key, value in settings.items()))
        clone = clone_history(
            arguments.image, arguments.name, seed_volume, f"{arguments.name}-bases"
        )
        if arguments.shorten_cache:
            cloned = m1_cache_bytes(arguments.image, clone)
            podman(
                "run",
                "--rm",
                "--user",
                "root",
                "--entrypoint",
                "sh",
                "-v",
                f"{clone}:/bases",
                arguments.image,
                "-c",
                f"for cache in /bases/*/history/{SYMBOL}/cache/M1.hc; "
                'do mv "$cache" "$cache.full"; done',
            )
            # MT5 rebuilds the frame when its M1 chart opens at the native cap.
            # The probe must not expand the capped frame before the candidate starts.
            probe = collect(
                arguments.image,
                arguments.name,
                env_file,
                clone,
                [],
                "m1-first",
                output,
                maxbars=100_000,
            )
            collected = m1_cache_bytes(arguments.image, clone)
            first = outcome_m1_first(probe, clock)
            log(
                f"{SYMBOL} M1 frame cache: {cloned} bytes cloned, {collected} collected, "
                f"first date UTC {first}"
            )
        start_candidate(arguments.image, arguments.name, env_file, clone, arguments.port)
    finally:
        for path in private.iterdir():
            path.unlink()
        private.rmdir()
    running = asyncio.run(settled(arguments.port))
    provenance = {
        "prepared_utc": datetime.now(UTC).isoformat(timespec="seconds"),
        "symbol": SYMBOL,
        "image": arguments.image,
        "image_id": image_id,
        "container": arguments.name,
        "port": arguments.port,
        "source_sha256": {source: sha256(CHECKOUT / source) for source in SOURCES},
        "image_ticks_sha256": compiled["image_ticks_sha256"],
        "compiled": {"ticks.mq5": compiled["ticks"], "tickgen_raw.mq5": compiled["tickgen_raw"]},
        "terminal_build_image": compiled["terminal_build"],
        "terminal_build_running": running["terminal_build"],
        "account": running["account"],
        "ranges": running["ranges"],
        "maxbars": running["maxbars"],
        "volume": {
            "seed": seed_volume,
            "clone": clone,
            "clone_created": podman(
                "volume", "inspect", "--format", "{{.CreatedAt}}", clone
            ).strip(),
        },
        "clock": {
            "tz": settings.get("MT5_BROKER_TZ", "America/New_York"),
            "offset_hours": int(settings.get("MT5_BROKER_OFFSET_HOURS", "7")),
        },
    }
    if arguments.shorten_cache:
        provenance["m1_cache_bytes"] = {"cloned": cloned, "collected": collected}
        provenance["collector_maxbars"] = 100_000
        provenance["m1_first_utc"] = first
    (output / "provenance.json").write_text(json.dumps(provenance, indent=1) + "\n")
    log(f"prepared: {output / 'provenance.json'}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", help="The candidate image.")
    parser.add_argument("--seed-container", help="The container whose history volume is copied.")
    parser.add_argument("--name", required=True, help="The candidate container's name.")
    parser.add_argument("--port", type=int, help="The loopback port the candidate's hub binds.")
    parser.add_argument("--output", help="The capture and provenance directory, under scripts/.")
    parser.add_argument(
        "--shorten-cache",
        action="store_true",
        help="Collect on the candidate's own copy and record its M1 first date. Default: off.",
    )
    parser.add_argument(
        "--cleanup", action="store_true", help="Remove what a run for --name started."
    )
    arguments = parser.parse_args()
    output = CHECKOUT / (arguments.output or "")
    try:
        if arguments.cleanup and arguments.shorten_cache:
            parser.error("--shorten-cache does not combine with --cleanup")
        elif arguments.cleanup:
            cleanup(arguments.name)
        elif None in (arguments.image, arguments.seed_container, arguments.port, arguments.output):
            parser.error("--image, --seed-container, --port and --output are required")
        elif arguments.port not in PORTS:
            parser.error(f"--port {arguments.port} is not a port")
        elif output.exists() and (not output.is_dir() or any(output.iterdir())):
            parser.error(f"--output {output} is not an empty directory")
        else:
            prepare(arguments)
    except Unprepared as failure:
        log(f"failed: {failure}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
