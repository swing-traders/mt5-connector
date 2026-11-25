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
    monkeypatch.setattr(owner, "command", lambda *args, **kwargs: "true")
    monkeypatch.setattr(owner, "logs", lambda name: SERVER_SELECTED)
    monkeypatch.setattr("functional.runtime.urllib.request.urlopen", lambda *a, **kw: Response())
    return owner


def test_health_requires_both_http_200_and_the_requested_server_check(runtime, monkeypatch):
    runtime.healthy("owned", 9000, seconds=1)
    monkeypatch.setattr(runtime, "logs", lambda name: "hub started")
    with pytest.raises(RuntimeError, match="requested server check"):
        runtime.healthy("owned", 9000, seconds=1)


def test_authentication_rejection_fails_before_waiting_for_health(runtime, monkeypatch):
    monkeypatch.setattr(runtime, "logs", lambda name: "authorization failed")
    with pytest.raises(RuntimeError, match="rejected the configured login"):
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


def test_native_true_permission_flags_are_required(runtime, monkeypatch, capsys):
    monkeypatch.setattr(runtime, "logs", lambda name: f"experts: native {PERMISSIONS}")
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
        lambda name: "experts: native trading allowed: terminal=false, expert=true",
    )
    with pytest.raises(RuntimeError, match="terminal=false, expert=true"):
        runtime.permissions("owned", seconds=1)
