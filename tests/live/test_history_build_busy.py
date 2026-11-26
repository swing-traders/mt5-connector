"""Native history builders renewing the real hub's pending read under a deterministic work clock."""

import csv
import os
import shutil
import signal
import subprocess
import uuid
from pathlib import Path
from types import SimpleNamespace

import prepare_tickgen as native
import pytest

from functional.settings import load, write
from mt5connector.server import hub_requests
from mt5connector.server.wire import calls

HERE = Path(__file__).parent
ROOT = HERE.parents[1]
IMAGE = os.environ.get("MT5_IMAGE_TEST")
ENV_FILE = os.environ.get("MT5_FUNCTIONAL_ENV_FILE")
PHASES = ("append", "merge", "keep_ticks", "keep_rates", "off_grid", "decrease", "bids", "offsets")


def instrument(source):
    edits = (
        ("void SendBusy(", "void UnusedSendBusy(", True),
        ("#property script_show_inputs", "", True),
        ("\ninput ", "\n", False),
        ("ArraySize(", "BuildSize(", False),
        ("BuildSize(g_history)", "ArraySize(g_history)", False),
        ("k < na + nb;", "k < na + nb && BuildStep();", True),
    )
    for before, after, unique in edits:
        matches = source.count(before)
        assert matches > 0, f"native probe edit {before!r} matched no source"
        if unique:
            assert matches == 1, f"native probe edit {before!r} matched {matches} times"
        source = source.replace(before, after)
    return source


@pytest.fixture(scope="module")
def native_builds(tmp_path_factory):
    if IMAGE is None or ENV_FILE is None:
        pytest.skip("functional certification supplies the native probe image and settings")
    directory = tmp_path_factory.mktemp("native-build-busy")
    directory.chmod(0o755)
    source = ROOT / "packages/server/mt5connector/server/mql5/Expert/ticks.mq5"
    # Capture busy frames without opening a socket; execute the original builder and renewal.
    (directory / "ticks_under_test.mqh").write_text(instrument(source.read_text()))
    for name in ("history_build_busy.mq5", "history_build_busy.sh"):
        shutil.copyfile(HERE / name, directory / name)
    owner = "mt5-build-busy-" + uuid.uuid4().hex[:12]
    container = owner + "-compile"
    private = directory / ".env"
    previous = signal.getsignal(signal.SIGTERM)

    def interrupted(_signal, _frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, interrupted)
    try:
        compiled = subprocess.run(
            [
                "podman",
                "run",
                "--name",
                container,
                "--label",
                f"{native.LABEL}={owner}",
                "--network",
                "none",
                "--entrypoint",
                "tini",
                "-v",
                f"{directory}:/probe:ro,z",
                IMAGE,
                "--",
                "xvfb-run",
                "-a",
                "bash",
                "/probe/history_build_busy.sh",
            ],
            capture_output=True,
            text=True,
            timeout=180,
        )
        assert compiled.returncode == 0, compiled.stdout + compiled.stderr
        assert "Result: 0 errors, 0 warnings" in compiled.stdout
        collector = directory / "collector"
        collector.mkdir()
        subprocess.run(
            [
                "podman",
                "cp",
                f"{container}:/tmp/tickgen_raw.ex5",
                str(collector / "tickgen_raw.ex5"),
            ],
            check=True,
            capture_output=True,
        )
        clone = native.clone_history(IMAGE, owner, "mt5-functional-main-bases", owner + "-bases")
        settings = load(Path(ENV_FILE))
        write(private, settings)
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(native, "COLLECTOR_BOUND_S", 600)
            captured = native.collect(IMAGE, owner, private, clone, [], "busy-build", directory)
        with (captured / "history-build-busy.csv").open() as recorded:
            rows = list(csv.reader(recorded))
        return {phase: [row for row in rows if row[0] == phase] for phase in PHASES}
    finally:
        try:
            native.cleanup(owner)
        finally:
            private.unlink(missing_ok=True)
            signal.signal(signal.SIGTERM, previous)


def replay(monkeypatch, rows, *, renew):
    clock = SimpleNamespace(now=0.0)
    timers = []
    expired = []

    def later(delay, callback, pending):
        handle = SimpleNamespace(at=clock.now + delay, cancelled=False)
        handle.cancel = lambda: setattr(handle, "cancelled", True)
        timers.append((handle, callback, pending))
        return handle

    monkeypatch.setattr(hub_requests, "time", SimpleNamespace(monotonic=lambda: clock.now))
    monkeypatch.setattr(
        hub_requests,
        "asyncio",
        SimpleNamespace(get_running_loop=lambda: SimpleNamespace(call_later=later)),
    )
    requests = hub_requests.Requests(expire=expired.append)
    served = SimpleNamespace(wait=calls.Wait.HISTORY)
    ea = object()
    pending = requests.add(object(), 1, served, None, ea, None)
    initial_deadline = None
    for _, event, stamp, bound in rows:
        clock.now = int(stamp) / 1000
        for handle, callback, held in timers.copy():
            if not handle.cancelled and handle.at <= clock.now:
                handle.cancelled = True
                callback(held)
        if event == "busy" and (renew or initial_deadline is None):
            if requests.get(ea, pending.hub_id) is not None:
                requests.busy(pending, int(bound))
                if initial_deadline is None:
                    initial_deadline = pending.deadline
    assert clock.now > initial_deadline
    return requests.get(ea, pending.hub_id), expired


@pytest.mark.parametrize("phase", PHASES)
def test_native_build_renews_its_read_past_the_initial_deadline(native_builds, monkeypatch, phase):
    rows = native_builds[phase]
    pending, expired = replay(monkeypatch, rows, renew=True)
    assert pending is not None, f"{phase}: the hub expired the read during its native build"
    assert expired == []
    without_renewal, missed = replay(monkeypatch, rows, renew=False)
    assert without_renewal is None
    assert len(missed) == 1
