"""The entrypoint's startup decision over native script outcomes."""

import os
import subprocess
from pathlib import Path

import pytest


@pytest.mark.parametrize("opened", [False, True], ids=["chart-failed", "chart-opened"])
def test_the_hub_starts_only_after_the_spawner_chart_opens(tmp_path, opened):
    entrypoint = Path(__file__).resolve().parents[2] / "image/entrypoint.sh"
    definitions, boundary, boot = entrypoint.read_text().partition(
        "# A boot step failing because a supervised process stopped"
    )
    functions = tmp_path / "functions.sh"
    functions.write_text(definitions)
    failure = "ChartOpen failed for 'TESTUSD': 4102"
    script = """
source "$1"
require_settings() { :; }
start_display() { :; }
restore_baked_terminal() { :; }
write_inis() { :; }
acquire_server_list() { :; }
allow_webrequest() {
    mkdir -p "$terminal_dir/MQL5/Files"
    if [[ "$TEST_OPENED" == yes ]]; then
        touch "$terminal_dir/MQL5/Files/ticks_setup.ready"
    else
        printf "%s\\n" "$TEST_FAILURE" > "$terminal_dir/MQL5/Files/ticks_setup.error"
    fi
}
start_hub() { echo HUB_STARTED; }
supervise() { :; }
stop_everything() { :; }
"""
    done = subprocess.run(
        ["bash", "-c", script + "\n# " + boundary + boot, "startup-test", str(functions)],
        env={
            **os.environ,
            "WINEPREFIX": str(tmp_path),
            "MT5_PYTHON_DIR": "",
            "TEST_OPENED": "yes" if opened else "no",
            "TEST_FAILURE": failure,
        },
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert (done.returncode == 0) is opened, done.stdout + done.stderr
    assert ("HUB_STARTED" in done.stdout) is opened
    if not opened:
        assert (done.stdout + done.stderr).splitlines().count("mt5: " + failure) == 1
