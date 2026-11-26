"""The entrypoint's real boot order under test-owned process doubles."""

import os
import subprocess
from pathlib import Path

from entrypoint_double import boot_script


def test_startup_orders_the_search_allowlist_spawner_and_hub(tmp_path):
    source = Path(__file__).resolve().parents[2] / "image/entrypoint.sh"
    doubles = """
require_settings() { echo SETTINGS; }
start_display() { echo DISPLAY; }
restore_baked_terminal() { echo RESTORE; }
write_inis() { echo INIS; }
acquire_server_list() { echo SEARCH; }
allow_webrequest() { echo ALLOWLIST; }
await_spawner() { echo SPAWNER; }
start_hub() { echo HUB; }
supervise() { echo SUPERVISE; }
end_with_stopped() { :; }
"""
    done = subprocess.run(
        ["bash", "-c", boot_script(source.read_text(), doubles)],
        env={**os.environ, "WINEPREFIX": str(tmp_path), "MT5_PYTHON_DIR": ""},
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert done.returncode == 0, done.stdout + done.stderr
    assert [line for line in done.stdout.splitlines() if not line.startswith("mt5: ")] == [
        "SETTINGS",
        "DISPLAY",
        "RESTORE",
        "INIS",
        "SEARCH",
        "ALLOWLIST",
        "SPAWNER",
        "HUB",
        "SUPERVISE",
    ]
