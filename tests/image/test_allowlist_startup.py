"""The entrypoint's first failed allowlist attempt ends startup."""

import os
import subprocess
from pathlib import Path

import pytest


@pytest.mark.parametrize(
    ("window_ready", "dismissal_ready"),
    [(True, True), (False, True), (True, False)],
    ids=["driver-failed", "window-failed", "dialog-dismissal-failed"],
)
def test_the_first_allowlist_failure_stops_before_the_hub(tmp_path, window_ready, dismissal_ready):
    entrypoint = Path(__file__).resolve().parents[2] / "image/entrypoint.sh"
    definitions, boundary, boot = entrypoint.read_text().partition(
        "# A boot step failing because a supervised process stopped"
    )
    functions = tmp_path / "functions.sh"
    functions.write_text(definitions)
    attempts = tmp_path / "attempts"
    attempts.write_text("")
    script = """
source "$1"
require_settings() { :; }
start_display() { :; }
restore_baked_terminal() { :; }
write_inis() { :; }
acquire_server_list() { :; }
start_terminal() { terminal_pid=1; echo TERMINAL_STARTED; }
start_log_tail() { tail_pid=1; }
await_terminal_window() {
    await_error="test terminal window unavailable"
    [[ "$TEST_WINDOW_READY" == yes ]]
}
end_with_stopped() { :; }
sleep() { :; }
gui() {
    if [[ "$1" == dismiss-dialogs ]]; then
        if [[ "$TEST_DISMISSAL_READY" == yes ]]; then return 0; fi
        echo "mt5: Options dialog unavailable"
        return 1
    fi
    printf 'called\\n' >> "$TEST_ATTEMPTS"
    echo "mt5: WebRequest checkbox unavailable" >&2
    return 1
}
await_spawner() { :; }
start_hub() { echo HUB_STARTED; }
supervise() { :; }
"""
    result = subprocess.run(
        ["bash", "-c", script + "\n# " + boundary + boot, "allowlist-test", str(functions)],
        env={
            **os.environ,
            "WINEPREFIX": str(tmp_path),
            "MT5_PYTHON_DIR": "",
            "TEST_WINDOW_READY": "yes" if window_ready else "no",
            "TEST_ATTEMPTS": str(attempts),
            "TEST_DISMISSAL_READY": "yes" if dismissal_ready else "no",
        },
        capture_output=True,
        text=True,
        timeout=10,
    )
    output = result.stdout + result.stderr
    assert result.returncode != 0
    assert output.splitlines().count("TERMINAL_STARTED") == 1
    assert len(attempts.read_text().splitlines()) == int(window_ready and dismissal_ready)
    if not window_ready:
        failure = "the WebRequest allowlist: test terminal window unavailable"
    elif not dismissal_ready:
        failure = "the WebRequest allowlist: Options dialog unavailable"
    else:
        failure = "the WebRequest allowlist: WebRequest checkbox unavailable"
    assert output.splitlines().count("mt5: " + failure) == 1
    assert "HUB_STARTED" not in output
