"""Record native live windows and rebuild an isolated capped M1 frame."""

import argparse
import json
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

import prepare_tickgen as native

from functional.settings import write

CAP = 5000
DAY = 86_400


def facts(path):
    rows = path.read_text().splitlines()
    if rows[0] != "done":
        raise native.Unprepared("the native live capture did not complete")
    recorded = {}
    for line in rows[1:]:
        key, *values = line.split()
        integers = tuple(map(int, values))
        if key in recorded and recorded[key] != integers:
            raise native.Unprepared(f"native {key} changed between windows")
        recorded[key] = integers
    return recorded


def provenance(output, captured, capped, clock):
    live = facts(captured / "outcome.txt")
    frame = facts(capped / "outcome.txt")
    first = live["first_tick"][0]
    floor = frame["m1_first"][0]
    if frame["maxbars"] != (CAP,) or floor < first + 2 * DAY:
        raise native.Unprepared("the native capped frame leaves no full interior floor-gap day")
    windows = {}
    for label in ("five-days", "minute-1424"):
        start, through = live[f"window_{label}"]
        ticks, bars = live[label]
        if start <= first or through < start or ticks <= 0 or bars <= 0:
            raise native.Unprepared(f"{label} has no complete native real-tick input")
        windows[label] = {
            "from_broker": start,
            "through_broker": through,
            "start_utc": clock.to_utc(start),
            "end_utc_exclusive": clock.to_utc(through + 1),
            "ticks": ticks,
            "bars": bars,
        }
    return {
        "prepared_utc": datetime.now(UTC).isoformat(timespec="seconds"),
        "symbol": native.SYMBOL,
        "clock": {"timezone": clock.tz.key, "offset_hours": clock.offset // timedelta(hours=1)},
        "terminal_build": live["terminal_build"][0],
        "first_tick_broker": first,
        "windows": windows,
        "cap": {"requested": CAP, "native": frame["maxbars"][0], "m1_first_broker": floor},
        "captures": {
            directory.name: {path.name: native.sha256(path) for path in sorted(directory.iterdir())}
            for directory in (captured, capped)
        },
        "source_sha256": native.sha256(native.CHECKOUT / "tests/live/tickgen_raw.mq5"),
    }


def prepare(arguments):
    output = arguments.output.resolve()
    if not output.is_relative_to(native.CHECKOUT / "scripts"):
        raise native.Unprepared("the live preparation output is not under scripts/")
    settings, seed = native.seed_settings(arguments.seed_container)
    clock = native.broker_clock(settings)
    output.mkdir(parents=True)
    compiled = native.compile_sources(arguments.image, arguments.name, output)
    raw = native.clone_history(
        arguments.image, arguments.name, seed, f"{arguments.name}-native-bases"
    )
    capped = native.clone_history(
        arguments.image, arguments.name, seed, f"{arguments.name}-capped-bases"
    )
    native.podman(
        "run",
        "--rm",
        "--user",
        "root",
        "--entrypoint",
        "sh",
        "-v",
        f"{capped}:/bases",
        arguments.image,
        "-c",
        f"for frame in /bases/*/history/{native.SYMBOL}/cache/M1.hc; "
        'do mv "$frame" "$frame.full"; done',
    )
    with tempfile.TemporaryDirectory(prefix="native-live-") as private:
        env_file = Path(private) / ".env"
        write(env_file, settings)
        captured = native.collect(
            arguments.image,
            arguments.name,
            env_file,
            raw,
            [("five-days", 0, 0), ("minute-1424", 0, 0)],
            "live",
            output,
        )
        frame = native.collect(
            arguments.image,
            arguments.name,
            env_file,
            capped,
            [],
            "capped",
            output,
            maxbars=CAP,
        )
    recorded = provenance(output, captured, frame, clock)
    recorded["image_id"] = native.podman(
        "image", "inspect", "--format", "{{.Id}}", arguments.image
    ).strip()
    recorded["capped_volume"] = capped
    recorded["compiled"] = compiled["tickgen_raw"]
    (output / "provenance.json").write_text(json.dumps(recorded, indent=2) + "\n")
    print("LIVE_NATIVE_INPUT_VERIFIED " + json.dumps(recorded["windows"]), flush=True)
    print("LIVE_NATIVE_CAP_VERIFIED " + json.dumps(recorded["cap"]), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True)
    parser.add_argument("--seed-container", required=True)
    parser.add_argument("--name", required=True)
    parser.add_argument("--output", required=True, type=Path)
    prepare(parser.parse_args())


if __name__ == "__main__":
    main()
