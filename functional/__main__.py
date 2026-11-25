"""Build the checkout and certify its image on the configured demo login."""

import argparse
import asyncio
import fcntl
import os
import shutil
import signal
import sys
import tempfile
import uuid
from pathlib import Path

from . import ROOT, history
from .certify import CALL_SECONDS, certify
from .runtime import BASES, Runtime
from .settings import Redactor, load, write


class Log:
    def __init__(self, runtime):
        self.runtime = runtime

    def debug(self, message):
        pass

    def info(self, message):
        print(self.runtime.redact(message), flush=True)

    warning = error = info

    def exception(self, message, failure):
        self.info(f"{message}: {type(failure).__name__}")


async def connected(runtime, port):
    from mt5connector.client.config import MT5Config
    from mt5connector.client.hub import HubSession

    session = HubSession(
        MT5Config(account=1, server="unused", ws_url=f"ws://127.0.0.1:{port}"),
        asyncio.get_running_loop(),
        lambda frame: None,
        None,
        Log(runtime),
    )
    await asyncio.wait_for(session.connect(), CALL_SECONDS)
    return session


async def round_trip(runtime, port, values):
    from mt5connector.wire import calls

    session = await connected(runtime, port)
    try:
        account = (
            await asyncio.wait_for(session.deferring(calls.Call.ACCOUNT_INFO), CALL_SECONDS)
        ).result
        runtime.redact.hide(account.name, account.company)
        await certify(
            session,
            login=int(values["MT5_LOGIN"]),
            server=values["MT5_SERVER"],
            symbol=values["MT5_LIVE_HUB_SYMBOL"],
        )
    finally:
        await session.disconnect()
    print("Minimum-lot market buy closed; login flat with no orders.", flush=True)


async def prime(runtime, port, symbol):
    from mt5connector.wire import calls

    print(f"Connecting for {symbol} history synchronization.", flush=True)
    session = await connected(runtime, port)
    print(f"Requesting settled {symbol} history ranges.", flush=True)
    try:
        await asyncio.wait_for(session.deferring(calls.Call.RANGES, symbol=symbol), 4 * 3600)
    finally:
        await session.disconnect()


async def confirm_flat(runtime, port):
    from mt5connector.wire import calls

    session = await connected(runtime, port)
    try:
        positions = (
            await asyncio.wait_for(session.deferring(calls.Call.POSITIONS_GET), CALL_SECONDS)
        ).result
        orders = (
            await asyncio.wait_for(session.deferring(calls.Call.ORDERS_GET), CALL_SECONDS)
        ).result
        if positions or orders:
            raise RuntimeError("the login is not flat after the functional suites")
    finally:
        await session.disconnect()
    print("Final login state: flat, no orders.", flush=True)


def interrupted(_signal, _frame):
    raise KeyboardInterrupt


def run(values, runtime):
    if shutil.which("podman") is None:
        raise RuntimeError("Podman is not available")
    stem = f"mt5-functional-{uuid.uuid4().hex[:12]}"
    output = ROOT / "scripts" / stem
    reference = history.materialize(output / "exports")
    if reference["symbol"] != values["MT5_LIVE_HUB_SYMBOL"]:
        raise ValueError("the historical references name a different symbol")
    image = f"localhost/mt5-functional:{stem}"
    environment = {
        **os.environ,
        **values,
        "PYTHONPATH": os.pathsep.join(
            (
                str(ROOT),
                str(ROOT / "packages/client"),
                str(ROOT / "packages/server"),
                os.environ.get("PYTHONPATH", ""),
            )
        ),
        "MT5_FUNCTIONAL": "1",
        "MT5_IMAGE_TEST": image,
        "MT5_TICK_EXPORT_DIR": str(output / "exports"),
    }
    cache = Path.home() / ".cache" / "mt5-connector"
    cache.mkdir(parents=True, exist_ok=True)
    owned_history = []
    built = False
    with (
        (cache / "functional.lock").open("w") as lock,
        tempfile.TemporaryDirectory(prefix="mt5-functional-") as private,
    ):
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        env_file = Path(private) / ".env"
        write(env_file, values)
        environment["MT5_FUNCTIONAL_ENV_FILE"] = str(env_file)
        try:
            runtime.stream(["podman", "build", "--format=docker", "-t", image, str(ROOT)])
            built = True
            main_name = f"{stem}-main"
            port = runtime.boot(image, main_name, env_file, volume="mt5-functional-main-bases")
            runtime.healthy(main_name, port)
            runtime.permissions(main_name)
            asyncio.run(round_trip(runtime, port, values))
            runtime.stream(
                [
                    sys.executable,
                    "-m",
                    "pytest",
                    "tests/functional/test_terminal_logging.py",
                    "-v",
                    "--tb=short",
                ],
                env=environment,
            )
            asyncio.run(prime(runtime, port, values["MT5_LIVE_HUB_SYMBOL"]))
            runtime.command("podman", "volume", "create", "--ignore", "mt5-functional-capped-bases")
            clone_name = f"{stem}-clone"
            runtime.containers.append(clone_name)
            runtime.stream(
                [
                    "podman",
                    "run",
                    "--no-healthcheck",
                    "--name",
                    clone_name,
                    "--user",
                    "root",
                    "--entrypoint",
                    "rsync",
                    "-v",
                    f"mt5-functional-main-bases:{BASES}:ro",
                    "-v",
                    "mt5-functional-capped-bases:/to",
                    image,
                    "-a",
                    f"{BASES}/",
                    "/to/",
                ]
            )
            runtime.stop(clone_name)
            capped_name = f"{stem}-capped"
            capped_port = runtime.boot(
                image,
                capped_name,
                env_file,
                maxbars=100_000,
                volume="mt5-functional-capped-bases",
            )
            runtime.healthy(capped_name, capped_port)
            runtime.permissions(capped_name)
            asyncio.run(prime(runtime, capped_port, values["MT5_LIVE_HUB_SYMBOL"]))
            environment["MT5_LIVE_HUB_URL"] = f"ws://127.0.0.1:{port}"
            environment["MT5_LIVE_CAPPED_HUB_URL"] = f"ws://127.0.0.1:{capped_port}"
            runtime.stream(
                [sys.executable, "-m", "pytest", "tests/live/test_hub_live.py", "-v", "--tb=short"],
                env=environment,
            )
            history.run(runtime, image, main_name, stem, output, environment, owned_history)
            asyncio.run(confirm_flat(runtime, port))
        finally:
            try:
                for name in owned_history:
                    runtime.stream(
                        [
                            sys.executable,
                            "tests/live/prepare_tickgen.py",
                            "--cleanup",
                            "--name",
                            name,
                        ],
                        env=environment,
                    )
            finally:
                try:
                    runtime.cleanup()
                finally:
                    if built:
                        runtime.command("podman", "image", "rm", image)
    print("Functional certification passed.", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--env-file",
        type=Path,
        default=Path(".env"),
        help="Read the private demo connection settings. Default: .env.",
    )
    arguments = parser.parse_args()
    values = load(arguments.env_file)
    runtime = Runtime(Redactor(values))
    try:
        run(values, runtime)
    except Exception as failure:
        print(
            runtime.redact(f"Functional certification failed: {type(failure).__name__}: {failure}"),
            flush=True,
        )
        raise SystemExit(1) from None


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, interrupted)
    try:
        main()
    except Exception as failure:
        print(f"Functional certification failed: {type(failure).__name__}: {failure}", flush=True)
        raise SystemExit(1) from None
