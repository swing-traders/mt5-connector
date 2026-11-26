import json
from types import SimpleNamespace

import pytest

from functional.runtime import PERMISSIONS, SERVER_SELECTED, Runtime


class Response:
    status = 200

    def __enter__(self):
        return self

    def __exit__(self, *_arguments):
        pass


@pytest.fixture
def runtime(monkeypatch):
    owner = Runtime(lambda text: text)
    clock = SimpleNamespace(now=0.0)
    monkeypatch.setattr(
        "functional.runtime.time",
        SimpleNamespace(
            monotonic=lambda: clock.now,
            sleep=lambda seconds: setattr(clock, "now", clock.now + seconds),
        ),
    )
    monkeypatch.setattr(
        owner,
        "command",
        lambda *args, **kwargs: json.dumps(
            {"RestartCount": 0, "State": {"Running": True, "StartedAt": "current"}}
        ),
    )
    monkeypatch.setattr(owner, "logs", lambda name, **kwargs: SERVER_SELECTED)
    monkeypatch.setattr("functional.runtime.urllib.request.urlopen", lambda *a, **kw: Response())
    return owner


def test_health_requires_both_http_200_and_the_requested_server_check(runtime, monkeypatch):
    runtime.healthy("owned", 9000, seconds=1)
    monkeypatch.setattr(runtime, "logs", lambda name, **kwargs: "hub started")
    with pytest.raises(RuntimeError, match="requested server check"):
        runtime.healthy("owned", 9000, seconds=1)


def test_authentication_rejection_is_reported_at_the_health_bound(runtime, monkeypatch):
    clock = SimpleNamespace(now=0.0)
    monkeypatch.setattr(
        "functional.runtime.time",
        SimpleNamespace(
            monotonic=lambda: clock.now,
            sleep=lambda seconds: setattr(clock, "now", clock.now + seconds),
        ),
    )
    monkeypatch.setattr(
        "functional.runtime.urllib.request.urlopen",
        lambda *a, **kw: (_ for _ in ()).throw(ConnectionRefusedError()),
    )
    monkeypatch.setattr(runtime, "logs", lambda name, **kwargs: "mt5: authorization failed")
    with pytest.raises(RuntimeError, match="owned.*authorization failed"):
        runtime.healthy("owned", 9000, seconds=1)


def test_boot_reuses_the_existing_history_cache(runtime, monkeypatch, tmp_path):
    def podman(*arguments, **kwargs):
        if arguments[1:3] == ("volume", "create") and "--ignore" not in arguments:
            raise RuntimeError("volume already exists")
        if arguments[1] == "port":
            return "127.0.0.1:12345\n"
        return ""

    monkeypatch.setattr(runtime, "command", podman)
    assert runtime.boot("image", "owned", tmp_path / ".env", volume="history-cache") == 12345


def test_boot_uses_the_production_restart_policy(runtime, monkeypatch, tmp_path):
    sent = []

    def command(*arguments, **kwargs):
        sent.append(arguments)
        return "127.0.0.1:12345" if arguments[1] == "port" else ""

    monkeypatch.setattr(runtime, "command", command)
    runtime.boot("image", "owned", tmp_path / ".env")
    run = next(arguments for arguments in sent if arguments[1] == "run")
    assert run[run.index("--restart") + 1] == "unless-stopped"


def test_health_waits_through_restarts_and_reports_each_one(runtime, monkeypatch, capsys):
    clock = SimpleNamespace(now=0.0)
    states = iter(
        (
            {"RestartCount": 0, "State": {"Running": False, "StartedAt": "first"}},
            {"RestartCount": 2, "State": {"Running": True, "StartedAt": "third"}},
        )
    )
    monkeypatch.setattr(runtime, "command", lambda *args, **kwargs: json.dumps(next(states)))
    monkeypatch.setattr(
        "functional.runtime.time",
        SimpleNamespace(
            monotonic=lambda: clock.now,
            sleep=lambda seconds: setattr(clock, "now", clock.now + seconds),
        ),
    )
    monkeypatch.setattr(runtime, "logs", lambda name, **kwargs: SERVER_SELECTED)

    runtime.healthy("owned", 9000, seconds=5)

    printed = capsys.readouterr().out
    assert "owned: restart 1" in printed
    assert "owned: restart 2" in printed
    assert clock.now < 5


def test_never_healthy_names_the_container_and_last_startup_line(runtime, monkeypatch):
    clock = SimpleNamespace(now=0.0)
    monkeypatch.setattr(
        "functional.runtime.time",
        SimpleNamespace(
            monotonic=lambda: clock.now,
            sleep=lambda seconds: setattr(clock, "now", clock.now + seconds),
        ),
    )
    monkeypatch.setattr(
        runtime,
        "command",
        lambda *args, **kwargs: json.dumps(
            {"RestartCount": 1, "State": {"Running": False, "StartedAt": "second"}}
        ),
    )
    monkeypatch.setattr(
        runtime, "logs", lambda name, **kwargs: "mt5: launching\nmt5: startup probe failed\n"
    )

    with pytest.raises(RuntimeError, match="owned.*startup probe failed"):
        runtime.healthy("owned", 9000, seconds=3)

    assert clock.now == 3


def test_health_reads_only_the_current_startup(runtime, monkeypatch):
    monkeypatch.setattr(
        runtime,
        "command",
        lambda *args, **kwargs: json.dumps(
            {"RestartCount": 1, "State": {"Running": True, "StartedAt": "current-start"}}
        ),
    )
    inspected = []

    def logs(name, **kwargs):
        inspected.append(kwargs)
        return SERVER_SELECTED if kwargs.get("since") == "current-start" else "stale boot"

    monkeypatch.setattr(runtime, "logs", logs)
    runtime.healthy("owned", 9000, seconds=1)
    assert inspected == [{"since": "current-start"}]


def test_startup_logs_include_stderr_and_are_limited_to_the_current_start(monkeypatch):
    commands = []

    def run(arguments, **kwargs):
        commands.append(arguments)
        return SimpleNamespace(returncode=0, stdout="mt5: launching\n", stderr="mt5: failed\n")

    monkeypatch.setattr("functional.runtime.subprocess.run", run)
    runtime = Runtime(lambda text: text)
    assert runtime.logs("owned", since="current-start") == "mt5: launching\nmt5: failed\n"
    assert commands == [("podman", "logs", "--since", "current-start", "owned")]


def test_native_true_permission_flags_are_required(runtime, monkeypatch, capsys):
    monkeypatch.setattr(runtime, "logs", lambda name, **kwargs: f"experts: native {PERMISSIONS}")
    runtime.permissions("owned", seconds=1)
    assert capsys.readouterr().out.strip() == f"experts: {PERMISSIONS}"


def test_a_false_native_permission_is_reported_and_never_certified(runtime, monkeypatch):
    clock = iter((0.0, 0.0, 2.0))
    monkeypatch.setattr(
        "functional.runtime.time",
        SimpleNamespace(monotonic=lambda: next(clock), sleep=lambda seconds: None),
    )
    monkeypatch.setattr(
        runtime,
        "logs",
        lambda name, **kwargs: "experts: native trading allowed: terminal=false, expert=true",
    )
    with pytest.raises(RuntimeError, match="terminal=false, expert=true"):
        runtime.permissions("owned", seconds=1)
