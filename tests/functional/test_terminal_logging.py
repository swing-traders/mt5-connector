import contextlib
import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest
from test_mt5_image import _await_log, _logs, _needs_image

from functional.runtime import Runtime
from functional.settings import PRIVATE, Redactor, load, write

pytestmark = pytest.mark.skipif(
    os.environ.get("MT5_FUNCTIONAL") != "1", reason="MT5_FUNCTIONAL is not enabled"
)
_ACCOUNT = {}


@pytest.fixture(autouse=True)
def _account_settings(monkeypatch):
    for key in (*PRIVATE, "MT5_SPAWNER_SYMBOL"):
        monkeypatch.setitem(_ACCOUNT, key, os.environ[key])


def _runtime() -> str:
    runtime = shutil.which("podman")
    assert runtime is not None, "no Podman on PATH"
    return runtime


@contextlib.contextmanager
def _booted(runtime: str, tmp_path, name: str):
    values = load(Path(os.environ["MT5_FUNCTIONAL_ENV_FILE"]))
    env_file = tmp_path / "private.env"
    write(env_file, values)
    owner = Runtime(Redactor(values), runtime)
    try:
        port = owner.boot(os.environ["MT5_IMAGE_TEST"], name, env_file)
        owner.healthy(name, port)
        yield name
    finally:
        try:
            owner.cleanup()
        finally:
            env_file.unlink()


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
        _await_log(
            runtime,
            name,
            "trading allowed: terminal=true, expert=true",
            count=1,
            seconds=60,
        )
        printed = _logs(runtime, name)

    assert re.search(r"^terminal: .*MetaTrader 5 x64 build \d+ started", printed, re.MULTILINE)
    assert "\x00" not in printed
