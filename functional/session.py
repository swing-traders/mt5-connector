"""Hub sessions with redacted certification logging."""

import asyncio

from .certify import CALL_SECONDS


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
