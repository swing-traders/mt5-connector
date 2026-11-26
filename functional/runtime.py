"""Owned Podman containers, redacted process output, and authenticated boot checks."""

import json
import re
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

from . import ROOT

BASES = "/home/mt5/.wine/drive_c/Program Files/MetaTrader 5/Bases"
PERMISSIONS = "trading allowed: terminal=true, expert=true"
SERVER_SELECTED = "the Login server dropdown selected the requested trade server"


class Runtime:
    def __init__(self, redact, program="podman"):
        self.redact = redact
        self.program = program
        self.containers = []
        self._restarts = {}

    def command(self, *arguments, timeout=60, combined=False):
        done = subprocess.run(
            arguments, capture_output=True, text=True, timeout=timeout, errors="replace"
        )
        if done.returncode:
            print(self.redact(done.stdout + done.stderr), flush=True)
            raise RuntimeError(f"{Path(arguments[0]).name} failed with status {done.returncode}")
        return done.stdout + done.stderr if combined else done.stdout

    def stream(self, arguments, *, env=None):
        process = subprocess.Popen(
            arguments,
            cwd=ROOT,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            errors="replace",
        )
        try:
            for line in process.stdout:
                print(self.redact(line), end="", flush=True)
            status = process.wait()
            if status:
                raise RuntimeError(f"{Path(arguments[0]).name} failed with status {status}")
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()

    def boot(self, image, name, env_file, *, maxbars=100_000_000, volume=None):
        mounts = ["-v", BASES if volume is None else f"{volume}:{BASES}"]
        if volume is not None:
            self.command(self.program, "volume", "create", "--ignore", volume)
        self.containers.append(name)
        self.command(
            self.program,
            "run",
            "-d",
            "--no-healthcheck",
            "--restart",
            "unless-stopped",
            "--name",
            name,
            "--env-file",
            str(env_file),
            "-e",
            f"MT5_MAX_BARS={maxbars}",
            "-p",
            "127.0.0.1::9000",
            *mounts,
            "--stop-timeout",
            "190",
            image,
            timeout=120,
        )
        return int(self.command(self.program, "port", name, "9000/tcp").strip().rsplit(":", 1)[1])

    def logs(self, name, *, since=None):
        arguments = [self.program, "logs"]
        if since is not None:
            arguments.extend(("--since", since))
        return self.command(*arguments, name, combined=True)

    def observe(self, name):
        inspected = json.loads(
            self.command(self.program, "inspect", "--format", "{{json .}}", name)
        )
        count = inspected["RestartCount"]
        for ordinal in range(self._restarts.get(name, 0) + 1, count + 1):
            print(f"{name}: restart {ordinal}", flush=True)
        self._restarts[name] = count
        return inspected["State"]

    def healthy(self, name, port, *, seconds=600):
        deadline = time.monotonic() + seconds
        last_startup = "no startup output"
        while time.monotonic() < deadline:
            state = self.observe(name)
            printed = self.logs(name, since=state["StartedAt"])
            for line in printed.splitlines():
                if line.startswith("mt5: "):
                    last_startup = line
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            if state["Running"]:
                try:
                    with urllib.request.urlopen(
                        f"http://127.0.0.1:{port}/health", timeout=min(5, remaining)
                    ) as response:
                        if response.status == 200:
                            if SERVER_SELECTED not in printed:
                                raise RuntimeError(
                                    f"{name}: the requested server check did not pass; "
                                    f"last startup: {self.redact(last_startup)}"
                                )
                            return
                except (urllib.error.URLError, ConnectionError, TimeoutError):
                    pass
            time.sleep(min(2, max(0, deadline - time.monotonic())))
        raise RuntimeError(
            f"{name}: the hub did not become healthy within the startup bound; "
            f"last startup: {self.redact(last_startup)}"
        )

    def permissions(self, name, *, seconds=90):
        deadline = time.monotonic() + seconds
        reported = None
        while time.monotonic() < deadline:
            state = self.observe(name)
            printed = self.logs(name, since=state["StartedAt"]) if state["Running"] else ""
            for line in printed.splitlines():
                if line.startswith("experts: "):
                    flags = re.search(
                        r"trading allowed: terminal=(true|false), expert=(true|false)$", line
                    )
                    if flags is not None:
                        reported = flags.group(0)
                        if reported == PERMISSIONS:
                            print(f"experts: {reported}", flush=True)
                            return
            time.sleep(2)
        raise RuntimeError(
            f"the EA did not report both trading permissions as true; observed: {reported}"
        )

    def stop(self, name):
        try:
            self.observe(name)
            self.command(self.program, "stop", "--time", "190", name, timeout=210)
        finally:
            self.command(self.program, "rm", "-f", "-v", name)
            self.containers.remove(name)

    def cleanup(self):
        failures = []
        for name in self.containers.copy():
            try:
                self.stop(name)
            except (RuntimeError, subprocess.TimeoutExpired) as failure:
                failures.append(type(failure).__name__)
        if failures:
            raise RuntimeError("functional container cleanup failed")
