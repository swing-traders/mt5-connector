"""Prepares the tick-generator acceptance: two private copies of a seed container's synced history,
the venue's raw ticks and M1 bars of a mixed window captured on one by a native collector, a hub
from the candidate image on the other, healthy with its symbol's ranges settled, and the provenance
of all of it. The seed container is only read. `--cleanup` removes the containers and the volumes
this driver labels with its name."""

import argparse
import asyncio
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import tickgen_capture

from mt5connector.client.config import MT5Config
from mt5connector.client.hub import HubSession
from mt5connector.wire import calls
from mt5connector.wire.broker_clock import BrokerClock

CHECKOUT = Path(__file__).resolve().parents[2]
SYMBOL = "AUDCAD+"
# The symbol's digits, which the fixture's doji test reads; the tester exports record them.
DIGITS = 5
LABEL = "io.swing-traders.tickgen-acceptance"
BASES = "/home/mt5/.wine/drive_c/Program Files/MetaTrader 5/Bases"
TERMINAL = "/home/mt5/.wine/drive_c/Program Files/MetaTrader 5"
CREDENTIALS = ("MT5_LOGIN", "MT5_PASSWORD", "MT5_SERVER", "MT5_SPAWNER_SYMBOL")
CLOCK_SETTINGS = ("MT5_BROKER_TZ", "MT5_BROKER_OFFSET_HOURS")
BASELINE = (datetime(2024, 6, 23, tzinfo=UTC), datetime(2024, 6, 26, tzinfo=UTC))
FALLBACK_LAST_DAY = datetime(2024, 12, 31, tzinfo=UTC)
# The fallback's UTC days captured per collector run.
FALLBACK_BATCH_DAYS = 7
DAY = timedelta(days=1)
HEALTH_BOUND_S = 1800
RANGES_BOUND_S = 4 * 3600
COLLECTOR_BOUND_S = 2 * 3600
POLL_S = 10
# What a run for a name creates, each refused up front when it already exists.
CONTAINERS = ("{name}", "{name}-compile", "{name}-raw")
VOLUMES = ("{name}-bases", "{name}-raw-bases")
PORTS = range(1, 65536)
SOURCES = (
    "packages/server/mt5connector/server/mql5/Expert/ticks.mq5",
    "tests/live/tickgen_raw.mq5",
    "tests/live/tickgen_oracle.py",
    "tests/live/tickgen_capture.py",
    "tests/live/prepare_tickgen.py",
    "tests/live/test_tickgen_history.py",
)


class Unprepared(Exception):
    pass


def podman(*arguments: str, check: bool = True) -> str:
    """The command's standard output; a failure raises with its standard error, never its
    arguments' environment."""
    done = subprocess.run(["podman", *arguments], capture_output=True, text=True)
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


def collect(
    image: str,
    name: str,
    env_file: Path,
    clone: str,
    windows: list[tuple[str, int, int]],
    output: Path,
) -> Path:
    """The collector's capture of each broker window, read in its own terminal on the clone."""
    container = f"{name}-raw"
    collector = output / "collector"
    parameters = collector / "tickgen_raw.txt"
    parameters.write_text(
        SYMBOL + "\n" + "".join(f"{label} {lo} {hi}\n" for label, lo, hi in windows)
    )
    baked_ini = podman("run", "--rm", "--entrypoint", "cat", image, "/opt/mt5/setup.ini")
    setup = collector / "setup.ini"
    setup.write_text(baked_ini.replace("Script=ticks_setup", "Script=tickgen_raw"))
    baked = "/opt/mt5-baked/terminal/MQL5"
    podman(
        "run",
        "-d",
        "--name",
        container,
        "--label",
        f"{LABEL}={name}",
        "--env-file",
        str(env_file),
        "-v",
        f"{clone}:{BASES}",
        "-v",
        f"{setup}:/opt/mt5/setup.ini:ro,z",
        "-v",
        f"{collector / 'tickgen_raw.ex5'}:{baked}/Scripts/tickgen_raw.ex5:ro,z",
        "-v",
        f"{parameters}:{baked}/Files/tickgen_raw.txt:ro,z",
        "--stop-timeout",
        "190",
        image,
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
    target = output / f"raw-{windows[0][0]}"
    podman("cp", f"{container}:{TERMINAL}/MQL5/Files/tickgen-raw", str(target))
    podman("stop", "--time", "190", container)
    podman("rm", container)
    if not outcome.startswith("done"):
        raise Unprepared(f"the collector {outcome.strip()}")
    log(f"captured {', '.join(label for label, _, _ in windows)}")
    return target


def utc_epoch(moment: datetime) -> int:
    return int(moment.timestamp())


def window(clock: BrokerClock, label: str, start: datetime, end: datetime) -> tuple[str, int, int]:
    """A UTC window [start, end) as the broker window the collector reads."""
    return label, clock.to_broker(utc_epoch(start)), clock.to_broker(utc_epoch(end)) - 1


def mixed_fixture(image, name, env_file, clone, clock, output) -> dict:
    """The baseline capture's fixture, else the first later UTC day's through the fallback's last
    day; fails naming every inspected window without one."""
    inspected = []
    baseline = window(clock, "baseline", *BASELINE)
    directory = collect(image, name, env_file, clone, [baseline], output)
    capture = tickgen_capture.load(directory, "baseline")
    found = tickgen_capture.identify(capture, DIGITS)
    inspected.append({"window": baseline, **tickgen_capture.counts(capture)})
    day = BASELINE[1]
    while found is None and day <= FALLBACK_LAST_DAY:
        batch = []
        while len(batch) < FALLBACK_BATCH_DAYS and day <= FALLBACK_LAST_DAY:
            batch.append(window(clock, f"day-{day:%Y%m%d}", day, day + DAY))
            day += DAY
        directory = collect(image, name, env_file, clone, batch, output)
        for label, lo, hi in batch:
            if found is None:
                capture = tickgen_capture.load(directory, label)
                found = tickgen_capture.identify(capture, DIGITS)
                inspected.append({"window": (label, lo, hi), **tickgen_capture.counts(capture)})
    if found is None:
        raise Unprepared(f"no mixed fixture through {FALLBACK_LAST_DAY:%Y-%m-%d}: {inspected}")
    return {
        "directory": directory.name,
        "label": capture.label,
        "start": found.start,
        "end": found.end,
        "empty": found.empty,
        "inspected": inspected,
    }


def start_candidate(image: str, name: str, env_file: Path, clone: str, port: int) -> None:
    podman(
        "run",
        "-d",
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
    deadline = time.monotonic() + HEALTH_BOUND_S
    healthy = False
    while not healthy:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=5) as answer:
                healthy = answer.status == 200
        except (urllib.error.URLError, ConnectionError):
            healthy = False
        if not healthy and time.monotonic() > deadline:
            raise Unprepared(f"the candidate's hub was not healthy within {HEALTH_BOUND_S} s")
        elif not healthy:
            time.sleep(POLL_S)
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
        # The collector's terminal rewrites its volume's M1 frame cache short, so it reads its own
        # copy of the history and the candidate another.
        raw_clone = clone_history(
            arguments.image, arguments.name, seed_volume, f"{arguments.name}-raw-bases"
        )
        clone = clone_history(
            arguments.image, arguments.name, seed_volume, f"{arguments.name}-bases"
        )
        fixture = mixed_fixture(arguments.image, arguments.name, env_file, raw_clone, clock, output)
        start_candidate(arguments.image, arguments.name, env_file, clone, arguments.port)
    finally:
        for path in private.iterdir():
            path.unlink()
        private.rmdir()
    running = asyncio.run(settled(arguments.port))
    raw = output / fixture["directory"]
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
        "server": settings["MT5_SERVER"],
        "account": running["account"],
        "ranges": running["ranges"],
        "maxbars": running["maxbars"],
        "volume": {
            "seed": seed_volume,
            "clone": clone,
            "collector_clone": raw_clone,
            "clone_created": podman(
                "volume", "inspect", "--format", "{{.CreatedAt}}", clone
            ).strip(),
        },
        "clock": {
            "tz": settings.get("MT5_BROKER_TZ", "America/New_York"),
            "offset_hours": int(settings.get("MT5_BROKER_OFFSET_HOURS", "7")),
        },
        "fixture": fixture,
        "raw_sha256": {path.name: sha256(path) for path in sorted(raw.iterdir())},
    }
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
        "--cleanup", action="store_true", help="Remove what a run for --name started."
    )
    arguments = parser.parse_args()
    output = CHECKOUT / (arguments.output or "")
    try:
        if arguments.cleanup:
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
