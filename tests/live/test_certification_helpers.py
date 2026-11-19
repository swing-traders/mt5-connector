"""Terminal-free tests of certification's client-side preparation."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

from certification import Throwaway, publisher_ready

from mt5connector.wire import calls


async def test_publisher_preparation_keeps_the_sessions_deferral_lifetime():
    entered = asyncio.Event()
    released = asyncio.Event()
    answer = object()

    async def deferred(call, **arguments):
        entered.set()
        await released.wait()
        return answer

    session = AsyncMock()
    session.deferring.side_effect = deferred
    pending = asyncio.create_task(publisher_ready(session, "TEST"))
    try:
        await entered.wait()
        assert not pending.done()
        released.set()
        assert await pending is answer
        session.deferring.assert_awaited_once_with(calls.Call.SYMBOL_INFO_TICK, symbol="TEST")
    finally:
        if not pending.done():
            pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)


async def test_container_logs_keep_hub_stderr_with_terminal_stdout(monkeypatch):
    process = SimpleNamespace(
        communicate=AsyncMock(return_value=(b"terminal line\nhub line\n", None)),
        returncode=0,
    )
    spawn = AsyncMock(return_value=process)
    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    control = object.__new__(Throwaway)
    control.runtime = "runtime"
    control.container = "owned-test"

    assert await control.logs(123) == "terminal line\nhub line\n"
    assert spawn.call_args.kwargs["stderr"] is asyncio.subprocess.STDOUT
    assert spawn.call_args.args == (
        "runtime",
        "logs",
        "--timestamps",
        "--since",
        "123",
        "owned-test",
    )
