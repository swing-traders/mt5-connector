"""The startup search gate over Win32 doubles, without Wine or a terminal."""

from __future__ import annotations

import ctypes
import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from test_mt5_image import _Win32Library

_ROOT = Path(__file__).resolve().parents[2]
_REQUESTED = "ExampleBroker-Demo"
_OTHER = "ExampleOther-Demo"


@pytest.mark.parametrize(
    ("results", "searches", "proceeds", "window_failures"),
    [
        ([[_OTHER, _REQUESTED]], 1, True, 0),
        ([[_OTHER], [_OTHER, _REQUESTED]], 2, True, 0),
        ([[_OTHER], [_OTHER], [_OTHER]], 3, False, 0),
        ([[_OTHER], [_OTHER]], 2, False, 1),
    ],
    ids=[
        "present-selects",
        "absent-then-present-retries",
        "absent-stops-startup",
        "window-failure-is-not-a-search",
    ],
)
def test_startup_requires_the_requested_server(
    tmp_path, results, searches, proceeds, window_failures
):
    state = tmp_path / "terminal.json"
    state.write_text(json.dumps({"results": results, "searches": 0, "selected": 0}))
    functions = tmp_path / "entrypoint-functions.sh"
    functions.write_text(
        (_ROOT / "image/entrypoint.sh")
        .read_text()
        .split("# A boot step failing because a supervised process stopped")[0]
    )
    script = """
source "$1"
start_terminal() { terminal_pid=1; }
window_failures="$TEST_WINDOW_FAILURES"
await_terminal_window() {
    if ((window_failures > 0)); then
        ((window_failures -= 1)) || :
        await_error="test window unavailable"
        return 1
    fi
}
end_with_stopped() { :; }
discard_terminal() { :; }
close_terminal() { :; }
sleep() { :; }
gui() {
    if [[ "$1" == dismiss-dialogs ]]; then return 0; fi
    "$TEST_PYTHON" "$TEST_DRIVER" "$@"
}
acquire_server_list
echo HUB_MAY_START
"""
    result = subprocess.run(
        ["bash", "-c", script, "server-search-test", str(functions)],
        env={
            **os.environ,
            "WINEPREFIX": str(tmp_path),
            "MT5_PYTHON_DIR": "",
            "MT5_SERVER": _REQUESTED,
            "MT5_LOGIN": "test-login-must-stay-private",
            "MT5_PASSWORD": "test-password-must-stay-private",
            "TEST_PYTHON": sys.executable,
            "TEST_DRIVER": str(Path(__file__).resolve()),
            "TEST_STATE": str(state),
            "TEST_WINDOW_FAILURES": str(window_failures),
        },
        capture_output=True,
        text=True,
        timeout=20,
    )

    held = json.loads(state.read_text())
    assert (result.returncode == 0) is proceeds, result.stdout + result.stderr
    assert ("HUB_MAY_START" in result.stdout) is proceeds
    assert held["searches"] == searches
    if proceeds:
        assert held["results"][searches - 1][held["selected"]] == _REQUESTED
    else:
        assert (
            f"trade server search: requested {_REQUESTED}, "
            f"terminal holds [{_OTHER}] after {searches} searches" in result.stdout.splitlines()[-1]
        )
    assert "test-login-must-stay-private" not in result.stdout + result.stderr
    assert "test-password-must-stay-private" not in result.stdout + result.stderr


def _drive() -> None:
    """Expose only the terminal's search and server ComboBox messages to the real GUI driver."""
    ctypes.WinDLL = _Win32Library
    ctypes.WINFUNCTYPE = ctypes.CFUNCTYPE
    spec = importlib.util.spec_from_file_location("terminal_gui", _ROOT / "image/terminal_gui.py")
    gui = importlib.util.module_from_spec(spec)
    sys.modules["terminal_gui"] = gui
    spec.loader.exec_module(gui)
    state_path = Path(os.environ["TEST_STATE"])
    state = json.loads(state_path.read_text())
    enabled = iter([False, True])
    gui._enabled_main_window = lambda: 1
    gui._open_dialog = lambda *args: 2
    gui._current_page = lambda *args: 3
    gui._control = lambda parent, control, *args: control
    gui._caption = lambda window: "Login"
    gui._set_text = lambda *args: None
    gui.user32.IsWindowEnabled = lambda window: next(enabled)

    def close(dialog, command, name):
        assert command == gui.Command.CANCEL

    def send(window, message, wparam=0, lparam=0):
        match int(message):
            case 0x00F5:  # BM_CLICK: one search acquires the corresponding response.
                state["searches"] += 1
                state["selected"] = 0
                return 0
            case 0x0146:  # CB_GETCOUNT
                return len(state["results"][state["searches"] - 1])
            case 0x0147:  # CB_GETCURSEL
                return state["selected"]
            case 0x0148 | 0x0149:  # CB_GETLBTEXT / CB_GETLBTEXTLEN
                text = state["results"][state["searches"] - 1][wparam]
                if int(message) == 0x0148:
                    buffer = (ctypes.c_wchar * (len(text) + 1)).from_address(lparam)
                    buffer.value = text
                return len(text)
            case 0x014E:  # CB_SETCURSEL
                state["selected"] = wparam
                return wparam
            case 0x0111:  # WM_COMMAND: server selection notification.
                return 0
            case _:
                raise AssertionError(f"unexpected terminal message {message}")

    gui._close_dialog = close
    gui._send = send
    try:
        gui.main()
    finally:
        state_path.write_text(json.dumps(state))


if __name__ == "__main__":
    _drive()
