"""The MT5 server image, opt-in: `MT5_IMAGE_TEST=localhost/mt5-connector-server pytest tests/image`
starts the image it names. The driver's menu and tab lookups and the log tail run without one."""

from __future__ import annotations

import contextlib
import ctypes
import importlib.util
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import time
import types
from pathlib import Path

import pytest
from nautilus_trader.model.identifiers import InstrumentId, Venue

from mt5connector.client import factories as mt5_factories
from mt5connector.client import remote_mt5
from mt5connector.client.config import MT5Config
from mt5connector.client.connection import ConnectionState, MT5Connection
from mt5connector.client.errors import MT5ConnectionError

_IMAGE = os.environ.get("MT5_IMAGE_TEST")
_needs_image = pytest.mark.skipif(_IMAGE is None, reason="MT5_IMAGE_TEST names no image to run")

# Each distinctive enough that a stray print of it cannot hide in the output.
_ACCOUNT = {
    "MT5_LOGIN": "31415926",
    "MT5_PASSWORD": "mt5-image-test-not-a-secret",
    "MT5_SERVER": "MT5ImageTest-Not-A-Server",
    "MT5_SPAWNER_SYMBOL": "EURUSD",
}
# The boot steps, the server's login timeout and its clock-bootstrap window, with room to spare.
_START_BOUND_SECONDS = 600
# The stop timeout the image's contract asks for, over the entrypoint's own stop bounds together.
_STOP_TIMEOUT_SECONDS = 190
_API_PORT = 5000
_HUB_PORT = 9000
# On the host's loopback, at host ports the runtime picks.
_PUBLISHED_PORTS = (f"127.0.0.1::{_API_PORT}", f"127.0.0.1::{_HUB_PORT}")
_IMAGE_FILES = Path(__file__).resolve().parents[2] / "image"
_BASES = "/home/mt5/.wine/drive_c/Program Files/MetaTrader 5/Bases"
_CONFIG = "/home/mt5/.wine/drive_c/Program Files/MetaTrader 5/Config"
_COMMON_FILES = "/home/mt5/.wine/drive_c/users/mt5/AppData/Roaming/MetaQuotes/Terminal/Common/Files"
_USER_REGISTRY = "/home/mt5/.wine/user.reg"
_TERMINAL_KEY = "HKCU\\Software\\MetaQuotes Software"
_WRITTEN_VALUE = "mt5-image-test-written-by-a-run"
_SCRIPTS = "/mt5-image-test"
# A file the terminal never writes, and its baked copy.
_INCLUDE = "/home/mt5/.wine/drive_c/Program Files/MetaTrader 5/MQL5/Include/JAson.mqh"
_BAKED_INCLUDE = "/opt/mt5-baked/terminal/MQL5/Include/JAson.mqh"
# Overwrites the first byte of "$1" with another, then gives "$1" the timestamp of "$2".
_REWRITE_IN_PLACE = """
first=$(head -c 1 "$1")
if [ "$first" = x ]; then replacement=y; else replacement=x; fi
printf %s "$replacement" | dd of="$1" bs=1 count=1 conv=notrunc 2>/dev/null
touch -r "$2" "$1"
"""


class _Win32Library:
    """A Windows DLL off Windows: the driver declares its functions' prototypes at import, and the
    lookups under test call none of them."""

    def __init__(self, name: str, use_last_error: bool = False) -> None:
        pass

    def __getattr__(self, function: str) -> types.SimpleNamespace:
        return types.SimpleNamespace()


@pytest.fixture
def gui(monkeypatch):
    """The driver module, imported over Win32 doubles."""
    monkeypatch.setattr(ctypes, "WinDLL", _Win32Library, raising=False)
    monkeypatch.setattr(ctypes, "WINFUNCTYPE", ctypes.CFUNCTYPE, raising=False)
    spec = importlib.util.spec_from_file_location("terminal_gui", _IMAGE_FILES / "terminal_gui.py")
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, "terminal_gui", module)
    spec.loader.exec_module(module)
    return module


def _tools_menu(gui, options_id: int):
    return gui.MenuEntry("&Tools", None, (gui.MenuEntry("&Options\tCtrl+O", options_id, ()),))


def _menus(gui, options_id: int, *, also_options: int | None = None) -> list:
    """File and Tools in one menu and Help in another, and Tools again in a third when
    `also_options` names its Options command."""
    file_menu = gui.MenuEntry(
        "&File",
        None,
        (
            gui.MenuEntry("Open an &Account", 32919, ()),
            gui.MenuEntry("", 0, ()),
            gui.MenuEntry("Pro&files", None, (gui.MenuEntry("&Next\tCtrl+F5", 32867, ()),)),
        ),
    )
    help_menu = gui.MenuEntry("&Help", None, (gui.MenuEntry("Terms && Conditions", 53601, ()),))
    menus = [(file_menu, _tools_menu(gui, options_id)), (help_menu,)]
    if also_options is not None:
        menus.append((_tools_menu(gui, also_options),))
    return menus


def test_a_menu_path_reads_through_mnemonics_and_accelerators(gui) -> None:
    menus = _menus(gui, 32849)

    assert gui.command_id(menus, ("Tools", "Options")) == 32849
    assert gui.command_id(menus, ("File", "Open an Account")) == 32919
    assert gui.command_id(menus, ("File", "Profiles", "Next")) == 32867
    assert gui.command_id(menus, ("Help", "Terms & Conditions")) == 53601


def test_a_command_every_menu_holding_it_agrees_on_resolves(gui) -> None:
    menus = _menus(gui, 32849, also_options=32849)

    assert gui.command_id(menus, ("Tools", "Options")) == 32849


def test_menus_disagreeing_on_a_command_fail_naming_it(gui) -> None:
    menus = _menus(gui, 32849, also_options=32850)

    with pytest.raises(gui.DialogError, match="Tools > Options"):
        gui.command_id(menus, ("Tools", "Options"))


def test_a_path_no_menu_holds_fails_naming_it(gui) -> None:
    with pytest.raises(gui.DialogError, match="Tools > Preferences"):
        gui.command_id(_menus(gui, 32849), ("Tools", "Preferences"))


def test_a_path_ending_at_a_submenu_names_no_command(gui) -> None:
    with pytest.raises(gui.DialogError, match="File > Profiles"):
        gui.command_id(_menus(gui, 32849), ("File", "Profiles"))


def test_a_tab_is_found_by_its_name(gui) -> None:
    names = ["Server", "Charts", "Trade", "Experts", "GPU", "Events"]

    assert gui.tab_index(names, "Experts") == 3


def test_an_absent_tab_fails_naming_it(gui) -> None:
    with pytest.raises(gui.DialogError, match="Experts"):
        gui.tab_index(["Server", "Charts", "Trade"], "Experts")


def test_two_tabs_of_one_name_fail_naming_it(gui) -> None:
    with pytest.raises(gui.DialogError, match="Experts"):
        gui.tab_index(["Experts", "Charts", "Experts"], "Experts")


_TAIL = _IMAGE_FILES / "tail_logs.py"


def _append_utf16(path: Path, text: str) -> None:
    """Appends ``text`` as the terminal writes its logs, a byte-order mark opening a new file."""
    with path.open("ab") as log:
        if log.tell() == 0:
            log.write(b"\xff\xfe")
        log.write(text.encode("utf-16-le"))


@contextlib.contextmanager
def _tailing(terminal: Path, out: Path, *args: str):
    """The log tail over ``terminal`` with no account in its environment, its stdout written to
    ``out``; killed if still running."""
    with out.open("wb") as sink:
        tail = subprocess.Popen(
            [sys.executable, str(_TAIL), *args, str(terminal)],
            stdout=sink,
            env={name: value for name, value in os.environ.items() if name not in _ACCOUNT},
        )
    try:
        yield tail
    finally:
        tail.kill()
        tail.wait(timeout=10)


def _await_output(out: Path, line: str, *, seconds: float = 10) -> None:
    deadline = time.monotonic() + seconds
    while line not in out.read_bytes().decode():
        assert time.monotonic() < deadline, f"no {line!r} within {seconds}s"
        time.sleep(0.1)


def _stop(tail: subprocess.Popen) -> None:
    tail.send_signal(signal.SIGTERM)
    assert tail.wait(timeout=10) == 0


@pytest.fixture
def terminal(tmp_path) -> Path:
    """A terminal directory holding the journal's and the experts log's directories, empty."""
    terminal = tmp_path / "terminal"
    (terminal / "logs").mkdir(parents=True)
    (terminal / "MQL5" / "logs").mkdir(parents=True)
    return terminal


def test_the_tail_follows_the_journal_onto_the_next_day(terminal, tmp_path) -> None:
    out = tmp_path / "out"
    _append_utf16(terminal / "logs" / "metaeditor.log", "not a day's journal\r\n")
    _append_utf16(terminal / "logs" / "20260101.log", "first day\r\n")
    with _tailing(terminal, out) as tail:
        _await_output(out, "terminal: first day\n")
        _append_utf16(terminal / "logs" / "20260102.log", "second day\r\n")
        _await_output(out, "terminal: second day\n")
        _append_utf16(terminal / "MQL5" / "logs" / "20260102.log", "an expert\r\n")
        _stop(tail)

    assert out.read_bytes().decode().splitlines() == [
        "terminal: first day",
        "terminal: second day",
        "experts: an expert",
    ]


def test_the_tail_writes_each_line_verbatim_as_utf8_under_its_source(terminal, tmp_path) -> None:
    out = tmp_path / "out"
    _append_utf16(
        terminal / "logs" / "20260101.log",
        "'12345678': authorized on Example-Demo through Access Server\r\n"
        "login 2718281828 refused\r\n"
        "Network '12345678': previous successful authorization performed from 203.0.113.57 on"
        " 2026.01.01 10:00:00\r\n"
        "EURUSD at 1.08 €\r\n",
    )
    with _tailing(terminal, out) as tail:
        _await_output(out, "€")
        _stop(tail)

    assert out.read_bytes().decode("utf-8").splitlines() == [
        "terminal: '12345678': authorized on Example-Demo through Access Server",
        "terminal: login 2718281828 refused",
        "terminal: Network '12345678': previous successful authorization performed from"
        " 203.0.113.57 on 2026.01.01 10:00:00",
        "terminal: EURUSD at 1.08 €",
    ]


def test_a_tail_from_the_end_skips_what_the_logs_held_at_its_start(terminal, tmp_path) -> None:
    out = tmp_path / "out"
    _append_utf16(terminal / "logs" / "20260101.log", "before\r\n")
    after = "after\r\n".encode("utf-16-le")
    with _tailing(terminal, out, "--from-end") as tail:
        # The tail's first read, which decides what it skips, is well within this.
        time.sleep(2)
        with (terminal / "logs" / "20260101.log").open("ab") as log:
            log.write(after[:3])
        time.sleep(1)
        with (terminal / "logs" / "20260101.log").open("ab") as log:
            log.write(after[3:])
        _append_utf16(terminal / "MQL5" / "logs" / "20260101.log", "an expert\r\n")
        _await_output(out, "experts: an expert\n")
        _stop(tail)

    assert out.read_bytes().decode().splitlines() == ["terminal: after", "experts: an expert"]


def test_a_tail_from_the_end_of_a_half_written_character_reads_on(terminal, tmp_path) -> None:
    out = tmp_path / "out"
    journal = terminal / "logs" / "20260101.log"
    _append_utf16(journal, "before\r\nha")
    rest = "lf\r\nafter\r\n".encode("utf-16-le")
    with journal.open("ab") as log:
        log.write(rest[:1])
    with _tailing(terminal, out, "--from-end") as tail:
        # The tail's first read, which decides what it skips, is well within this.
        time.sleep(2)
        with journal.open("ab") as log:
            log.write(rest[1:])
        _await_output(out, "terminal: after\n")
        _stop(tail)

    assert out.read_bytes().decode().splitlines() == ["terminal: lf", "terminal: after"]


def test_a_stop_on_a_half_written_character_streams_the_whole_lines(terminal, tmp_path) -> None:
    out = tmp_path / "out"
    journal = terminal / "logs" / "20260101.log"
    _append_utf16(journal, "whole\r\n")
    with journal.open("ab") as log:
        log.write("x".encode("utf-16-le")[:1])
    with _tailing(terminal, out) as tail:
        _await_output(out, "terminal: whole\n")
        _stop(tail)

    assert out.read_bytes().decode().splitlines() == ["terminal: whole"]


def _runtime() -> str:
    runtime = shutil.which("docker") or shutil.which("podman")
    assert runtime is not None, "no container runtime on PATH"
    return runtime


def _account_env() -> list[str]:
    return [token for name in _ACCOUNT for token in ("-e", name)]


def _logs(runtime: str, name: str) -> str:
    output = subprocess.run([runtime, "logs", name], capture_output=True, text=True, timeout=30)
    return output.stdout + output.stderr


def _await_log(runtime: str, name: str, line: str, *, count: int, seconds: float) -> None:
    deadline = time.monotonic() + seconds
    while _logs(runtime, name).count(line) < count:
        assert time.monotonic() < deadline, f"no {line!r} #{count} within {seconds}s"
        time.sleep(1)


@_needs_image
def test_a_start_on_an_unknown_account_ends_through_the_server_and_prints_no_password() -> None:
    result = subprocess.run(
        [_runtime(), "run", "--rm", *_account_env(), _IMAGE],
        env={**os.environ, **_ACCOUNT},
        capture_output=True,
        text=True,
        timeout=_START_BOUND_SECONDS,
    )

    output = result.stdout + result.stderr
    assert result.returncode != 0
    assert "mt5: restored the terminal's baked state, its history store aside" in output
    assert "mt5: fetching the trade server's broker into the server list" in output
    assert "mt5: the account dialog searched the trade server" in output
    assert "mt5: adding 127.0.0.1 to the WebRequest allowlist" in output
    assert "mt5: the WebRequest allowlist holds 127.0.0.1 alone" in output
    assert "mt5: server exited with status" in output
    assert _ACCOUNT["MT5_PASSWORD"] not in output


def _start_with(tmp_path, stub_at: str, stub: str, *, timeout: int) -> subprocess.CompletedProcess:
    """Start the image on the unknown account, an executable ``stub`` mounted over ``stub_at``; the
    container is removed however the start ends."""
    runtime = _runtime()
    name = f"mt5-image-stub-test-{os.getpid()}"
    script = tmp_path / "stub"
    script.write_text(stub)
    script.chmod(0o755)
    try:
        return subprocess.run(
            [
                runtime,
                "run",
                "--rm",
                "--name",
                name,
                "-v",
                f"{script}:{stub_at}:ro,z",
                *_account_env(),
                _IMAGE,
            ],
            env={**os.environ, **_ACCOUNT},
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    finally:
        subprocess.run([runtime, "rm", "-f", name], capture_output=True, timeout=60)


@_needs_image
def test_the_first_process_to_exit_ends_the_container_with_its_status(tmp_path) -> None:
    result = _start_with(
        tmp_path, "/opt/conda/bin/mt5-connector-hub", "#!/bin/sh\nexit 42\n", timeout=300
    )

    assert result.returncode == 42
    assert "mt5: hub exited with status 42" in result.stdout + result.stderr


@_needs_image
def test_a_display_that_never_comes_up_fails_the_start_naming_its_step(tmp_path) -> None:
    result = _start_with(tmp_path, "/usr/bin/Xvfb", "#!/bin/sh\nexec sleep 3600\n", timeout=180)

    assert result.returncode != 0
    assert "mt5: no display within 30s" in result.stdout + result.stderr


@_needs_image
def test_a_restore_that_never_returns_fails_the_start_naming_its_step(tmp_path) -> None:
    result = _start_with(tmp_path, "/usr/bin/rsync", "#!/bin/sh\nexec sleep 3600\n", timeout=180)

    assert result.returncode != 0
    assert (
        "mt5: the restore of the terminal's baked state did not finish within 40s"
        in result.stdout + result.stderr
    )


@_needs_image
def test_a_driver_action_that_never_returns_fails_the_start_naming_its_step(tmp_path) -> None:
    stub = "import sys\nimport time\n\nif sys.argv[1] != 'main-window':\n    time.sleep(3600)\n"

    result = _start_with(tmp_path, "/opt/mt5/terminal_gui.py", stub, timeout=300)

    assert result.returncode != 0
    assert "mt5: the server search did not finish within 40s" in result.stdout + result.stderr


@_needs_image
def test_an_x_server_that_exits_ends_the_container_with_its_status(tmp_path) -> None:
    result = _start_with(tmp_path, "/usr/bin/Xvfb", "#!/bin/sh\nexit 42\n", timeout=180)

    assert result.returncode == 42
    assert "mt5: Xvfb exited with status 42" in result.stdout + result.stderr


# Kills every process of the terminal, retried until there is one: the step's log line precedes the
# launch it announces. `[.]` keeps the pattern from matching this script's own command line.
_KILL_TERMINAL = """
until
    for p in /proc/[0-9]*; do
        if tr "\\0" " " < "$p/cmdline" 2>/dev/null | grep -q "terminal64[.]exe"; then
            kill -KILL "${p#/proc/}" && found=1
        fi
    done
    [ -n "$found" ]
do
    sleep 0.2
done
"""


@_needs_image
def test_a_terminal_that_exits_during_the_boot_ends_the_container_with_its_status() -> None:
    runtime = _runtime()
    name = f"mt5-image-test-{os.getpid()}"
    subprocess.run(
        [runtime, "run", "-d", "--name", name, *_account_env(), _IMAGE],
        env={**os.environ, **_ACCOUNT},
        check=True,
        timeout=60,
    )
    try:
        _await_log(runtime, name, "mt5: launching the terminal", count=1, seconds=120)
        subprocess.run([runtime, "exec", name, "sh", "-c", _KILL_TERMINAL], check=True, timeout=60)
        status = subprocess.run(
            [runtime, "wait", name], capture_output=True, text=True, timeout=300
        ).stdout.strip()
        printed = _logs(runtime, name)
    finally:
        subprocess.run([runtime, "rm", "-f", name], capture_output=True, timeout=60)

    assert status == "137"
    assert "mt5: terminal exited with status 137" in printed


@_needs_image
def test_a_stop_closes_the_terminal_and_ends_the_container_within_the_stop_timeout() -> None:
    runtime = _runtime()
    name = f"mt5-image-stop-test-{os.getpid()}"
    stop_timeout = _STOP_TIMEOUT_SECONDS
    subprocess.run(
        [
            runtime,
            "run",
            "-d",
            "--name",
            name,
            "--stop-timeout",
            str(stop_timeout),
            *_account_env(),
            _IMAGE,
        ],
        env={**os.environ, **_ACCOUNT},
        check=True,
        timeout=60,
    )
    try:
        _await_log(runtime, name, "mt5: starting the server", count=1, seconds=300)
        started = time.monotonic()
        subprocess.run([runtime, "stop", name], capture_output=True, timeout=stop_timeout + 30)
        stopped_after = time.monotonic() - started
        status = subprocess.run(
            [runtime, "inspect", "--format", "{{.State.ExitCode}}", name],
            capture_output=True,
            text=True,
            timeout=30,
        ).stdout.strip()
        printed = _logs(runtime, name)
    finally:
        subprocess.run([runtime, "rm", "-f", name], capture_output=True, timeout=60)

    assert stopped_after < stop_timeout
    assert status == "0"
    assert "mt5: stop requested" in printed
    assert printed.index("mt5: the terminal closed") < printed.index("mt5: the log tail stopped")
    assert "mt5: killing" not in printed


# Reads the Options dialog's WebRequest checkbox and URL count through the image's own driver, and
# cancels the dialog. The URL list's last row is the one that adds a URL.
_READ_ALLOWLIST = r"""
import sys

sys.path.insert(0, r"Z:\opt\mt5")
import terminal_gui as gui
from terminal_gui import Command, Message, WindowClass

dialog = gui._open_dialog(gui._enabled_main_window(), gui.OPTIONS_MENU_PATH, "the Options dialog")
tabs = gui._control(dialog, gui.OPTIONS_TABS_ID, WindowClass.TAB_CONTROL, "the Options dialog")
page = gui._select_tab(dialog, tabs, gui.EXPERTS_TAB)
checkbox = gui._control(page, gui.WEBREQUEST_CHECKBOX_ID, WindowClass.BUTTON, "the Experts page")
url_list = gui._control(page, gui.WEBREQUEST_LIST_ID, WindowClass.LIST_VIEW, "the Experts page")
print("checked", gui._send(checkbox, Message.BM_GETCHECK))
print("urls", gui._send(url_list, Message.LVM_GETITEMCOUNT) - 1)
gui._close_dialog(dialog, Command.CANCEL, "the Options dialog")
"""

# The driver's own allowlist action, run against an Options dialog whose Experts page has no control
# of the checkbox's id.
_ABSENT_CHECKBOX = r"""
import sys

sys.path.insert(0, r"Z:\opt\mt5")
import terminal_gui as gui

gui.WEBREQUEST_CHECKBOX_ID = 32767
sys.argv = ["terminal_gui.py", "allow-webrequest", "127.0.0.1"]
gui.main()
"""


@contextlib.contextmanager
def _booted(runtime: str, tmp_path, name: str):
    """A container of the image booted up to its server, its history store a volume of its own and
    the scripts above mounted; removed with its volume on exit."""
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    scripts.chmod(0o755)
    for script, text in (("read_allowlist.py", _READ_ALLOWLIST), ("absent.py", _ABSENT_CHECKBOX)):
        (scripts / script).write_text(text)
        (scripts / script).chmod(0o644)
    subprocess.run(
        [
            runtime,
            "run",
            "-d",
            "--name",
            name,
            "--stop-timeout",
            str(_STOP_TIMEOUT_SECONDS),
            "-v",
            _BASES,
            "-v",
            f"{scripts}:{_SCRIPTS}:ro,z",
            *_account_env(),
            _IMAGE,
        ],
        env={**os.environ, **_ACCOUNT},
        check=True,
        timeout=60,
    )
    try:
        _await_log(runtime, name, "mt5: starting the server", count=1, seconds=300)
        yield name
    finally:
        subprocess.run([runtime, "rm", "-f", "-v", name], capture_output=True, timeout=180)


def _windows_python(runtime: str, name: str) -> str:
    directory = subprocess.run(
        [runtime, "exec", name, "printenv", "MT5_PYTHON_DIR"],
        capture_output=True,
        text=True,
        timeout=60,
        check=True,
    ).stdout.strip()
    return directory + "\\python.exe"


def _run_script(runtime: str, name: str, script: str) -> subprocess.CompletedProcess:
    windows_path = "Z:" + _SCRIPTS.replace("/", "\\") + "\\" + script
    return subprocess.run(
        [
            runtime,
            "exec",
            "-e",
            "DISPLAY=:0",
            name,
            "wine",
            _windows_python(runtime, name),
            windows_path,
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )


def _wine(runtime: str, name: str, *command: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [runtime, "exec", name, "wine", *command], capture_output=True, text=True, timeout=120
    )


def _allowlist(runtime: str, name: str) -> dict[str, int]:
    result = _run_script(runtime, name, "read_allowlist.py")
    assert result.returncode == 0, result.stderr
    read = {}
    for line in result.stdout.splitlines():
        key, value = line.split()
        read[key] = int(value)
    return read


@_needs_image
def test_the_main_window_action_reports_the_window(tmp_path) -> None:
    runtime = _runtime()
    with _booted(runtime, tmp_path, f"mt5-image-main-window-test-{os.getpid()}") as name:
        result = subprocess.run(
            [
                runtime,
                "exec",
                "-e",
                "DISPLAY=:0",
                name,
                "wine",
                _windows_python(runtime, name),
                "Z:\\opt\\mt5\\terminal_gui.py",
                "main-window",
            ],
            capture_output=True,
            text=True,
            timeout=120,
        )

    assert result.returncode == 0
    assert result.stdout == "mt5: the terminal's main window is up\n"


@_needs_image
def test_a_boot_leaves_webrequest_allowed_for_one_url(tmp_path) -> None:
    runtime = _runtime()
    with _booted(runtime, tmp_path, f"mt5-image-allowlist-test-{os.getpid()}") as name:
        allowlist = _allowlist(runtime, name)

    assert allowlist == {"checked": 1, "urls": 1}


@_needs_image
def test_a_restart_restores_the_baked_state_outside_the_history_store(tmp_path) -> None:
    runtime = _runtime()
    with _booted(runtime, tmp_path, f"mt5-image-restart-test-{os.getpid()}") as name:
        before = _allowlist(runtime, name)
        subprocess.run(
            [
                runtime,
                "exec",
                name,
                "touch",
                f"{_CONFIG}/written-by-a-run",
                f"{_COMMON_FILES}/written-by-a-run",
                f"{_BASES}/synced",
            ],
            check=True,
            timeout=30,
        )
        written = _wine(
            runtime, name, "reg", "add", _TERMINAL_KEY, "/v", _WRITTEN_VALUE, "/d", "1", "/f"
        )
        assert written.returncode == 0
        # The wineserver saves the registry to its files on a timer of its own.
        deadline = time.monotonic() + 120
        while subprocess.run(
            [runtime, "exec", name, "grep", "-q", _WRITTEN_VALUE, _USER_REGISTRY], timeout=30
        ).returncode:
            assert time.monotonic() < deadline, "the written value never reached user.reg"
            time.sleep(2)
        # A same-length rewrite carrying the baked file's own timestamp.
        subprocess.run(
            [runtime, "exec", name, "sh", "-c", _REWRITE_IN_PLACE, "sh", _INCLUDE, _BAKED_INCLUDE],
            check=True,
            timeout=30,
        )
        subprocess.run(
            [runtime, "restart", "-t", str(_STOP_TIMEOUT_SECONDS), name],
            capture_output=True,
            timeout=_STOP_TIMEOUT_SECONDS + 60,
        )
        _await_log(runtime, name, "mt5: starting the server", count=2, seconds=300)
        after = _allowlist(runtime, name)
        kept = subprocess.run(
            [runtime, "exec", name, "ls", _CONFIG, _COMMON_FILES, _BASES],
            capture_output=True,
            text=True,
            timeout=30,
        ).stdout
        queried = _wine(runtime, name, "reg", "query", _TERMINAL_KEY, "/v", _WRITTEN_VALUE)
        restored = subprocess.run(
            [runtime, "exec", name, "cmp", _BAKED_INCLUDE, _INCLUDE], timeout=30
        ).returncode
        printed = _logs(runtime, name)

    assert before == after == {"checked": 1, "urls": 1}
    assert restored == 0
    assert printed.count("mt5: restored the terminal's baked state, its history store aside") == 2
    assert "written-by-a-run" not in kept
    assert "synced" in kept
    assert queried.returncode != 0


# Writes each argument as a line of a journal dated past any real day, as the terminal writes one.
_LATER_JOURNAL = r"""
journal="$WINEPREFIX/drive_c/Program Files/MetaTrader 5/logs/29991231.log"
{ printf '\377\376'; printf '%s\r\n' "$@" | iconv -f UTF-8 -t UTF-16LE; } > "$journal"
"""


@_needs_image
def test_the_terminal_logs_reach_the_container_log_verbatim(tmp_path) -> None:
    runtime = _runtime()
    login = _ACCOUNT["MT5_LOGIN"]
    server = _ACCOUNT["MT5_SERVER"]
    with _booted(runtime, tmp_path, f"mt5-image-log-test-{os.getpid()}") as name:
        subprocess.run(
            [
                runtime,
                "exec",
                name,
                "sh",
                "-c",
                _LATER_JOURNAL,
                "sh",
                f"'{login}': authorized on {server} through Access Server",
            ],
            check=True,
            timeout=30,
        )
        written = f"terminal: '{login}': authorized on {server} through Access Server"
        _await_log(runtime, name, written, count=1, seconds=30)
        printed = _logs(runtime, name)

    assert re.search(r"^terminal: .*MetaTrader 5 x64 build \d+ started", printed, re.MULTILINE)
    assert re.search(r"^experts: ", printed, re.MULTILINE)
    assert "\x00" not in printed


@_needs_image
def test_a_dialog_without_the_expected_control_fails_the_driver_naming_it(tmp_path) -> None:
    runtime = _runtime()
    with _booted(runtime, tmp_path, f"mt5-image-absent-test-{os.getpid()}") as name:
        result = _run_script(runtime, name, "absent.py")

    assert result.returncode != 0
    assert "mt5: the Options dialog's Experts page has no Button 32767" in result.stderr


# ── the client against the image, whose server no made-up account brings up ──

_EURUSD = InstrumentId.from_str("EURUSD.MT5_TEST")


def _client_config(server_url: str) -> MT5Config:
    return MT5Config(
        account=int(_ACCOUNT["MT5_LOGIN"]),
        password=_ACCOUNT["MT5_PASSWORD"],
        server=_ACCOUNT["MT5_SERVER"],
        server_url=server_url,
        venue=Venue("MT5_TEST"),
    )


def _loopback_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def _refused_at_once(connection: MT5Connection) -> None:
    """`connection`'s connect raises the client's error for a request no server answered, within the
    client's own bound on reaching a server, carrying no credential, and leaves it disconnected."""
    started = time.monotonic()
    with pytest.raises(MT5ConnectionError) as refused:
        connection.connect()
    assert time.monotonic() - started < remote_mt5.CONNECT_TIMEOUT_S
    said = " ".join([str(refused.value), *getattr(refused.value, "__notes__", [])])
    for name in ("MT5_LOGIN", "MT5_PASSWORD", "MT5_SERVER"):
        assert _ACCOUNT[name] not in said
    assert connection._state is ConnectionState.DISCONNECTED


@_needs_image
def test_a_client_is_refused_at_once_by_a_server_that_never_comes_up(monkeypatch) -> None:
    # The terminal cannot log the made-up account in, so the server exits without ever serving: a
    # connect meets no server while the image boots or once it has exited.
    runtime = _runtime()
    port = _loopback_port()
    name = f"mt5-image-client-test-{os.getpid()}"
    monkeypatch.setattr(mt5_factories, "_mt5_config_registry", {})
    transports: list[remote_mt5.RemoteMT5] = []

    class _RecordedTransport(remote_mt5.RemoteMT5):
        def __init__(self, server_url: str) -> None:
            super().__init__(server_url)
            transports.append(self)

    monkeypatch.setattr("mt5connector.client.connection.RemoteMT5", _RecordedTransport)
    config = _client_config(f"http://127.0.0.1:{port}")
    subprocess.run(
        [
            runtime,
            "run",
            "-d",
            "--name",
            name,
            "-p",
            f"127.0.0.1:{port}:{_API_PORT}",
            *_account_env(),
            _IMAGE,
        ],
        env={**os.environ, **_ACCOUNT},
        check=True,
        timeout=60,
    )
    try:
        _await_log(runtime, name, "mt5: starting the server", count=1, seconds=300)
        _refused_at_once(MT5Connection(config))

        subprocess.run([runtime, "wait", name], check=True, timeout=_START_BOUND_SECONDS)
        _refused_at_once(MT5Connection(config))

        # Building the clients' configs asks no server at all.
        node = mt5_factories.build_mt5_node_config(
            config, data_instruments=frozenset({_EURUSD}), exec_instruments=frozenset({_EURUSD})
        )
        assert node.data_clients["MT5_TEST"].instrument_provider.load_ids == frozenset({_EURUSD})
        assert node.exec_clients["MT5_TEST"].instrument_provider.load_ids == frozenset({_EURUSD})
        assert len(transports) == 2
    finally:
        subprocess.run([runtime, "rm", "-f", "-v", name], capture_output=True, timeout=180)


def _published_port(runtime: str, name: str, port: int) -> int:
    """The host port the runtime published the container `name`'s `port` on."""
    reply = subprocess.run(
        [runtime, "port", name, f"{port}/tcp"],
        capture_output=True,
        text=True,
        timeout=30,
        check=True,
    ).stdout
    return int(reply.split()[0].rpartition(":")[2])


@_needs_image
def test_a_published_server_is_reachable_on_the_loopback_at_the_port_the_runtime_picked() -> None:
    runtime = _runtime()
    name = f"mt5-image-publish-test-{os.getpid()}"
    publishes = [token for spec in _PUBLISHED_PORTS for token in ("--publish", spec)]
    subprocess.run(
        [runtime, "run", "-d", "--name", name, *publishes, *_account_env(), _IMAGE],
        env={**os.environ, **_ACCOUNT},
        check=True,
        timeout=60,
    )
    try:
        rest = _published_port(runtime, name, _API_PORT)
        hub = _published_port(runtime, name, _HUB_PORT)
        _await_log(runtime, name, "mt5: starting the server", count=1, seconds=300)

        assert rest != hub
        # The runtime holds the host side of the REST port on the loopback while the server boots.
        with socket.create_connection(("127.0.0.1", rest), timeout=5):
            pass
    finally:
        subprocess.run([runtime, "rm", "-f", "-v", name], capture_output=True, timeout=180)
