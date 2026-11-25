"""The controlled gap setup waits for the hub's own history completion."""

import asyncio
from types import SimpleNamespace

import prepare_gap
import pytest


def test_gap_setup_waits_for_the_hubs_ranges_completion(monkeypatch):
    async def exercise():
        ready = asyncio.Event()
        requested = asyncio.Event()

        class Session:
            disconnected = False

            async def request(self, call):
                return SimpleNamespace(result=SimpleNamespace(name="name", company="company"))

            async def deferring(self, call, **arguments):
                assert call is prepare_gap.calls.Call.RANGES
                assert arguments == {"symbol": "BTCUSD.a.GAPTEST"}
                requested.set()
                await ready.wait()
                return SimpleNamespace(
                    result=SimpleNamespace(first_tick=0, claimable_through=123, resync_failure="")
                )

            async def disconnect(self):
                self.disconnected = True

        session = Session()

        async def connected(runtime, port):
            assert port == 12345
            return session

        monkeypatch.setattr(prepare_gap, "connected", connected)
        runtime = SimpleNamespace(redact=SimpleNamespace(hide=lambda *values: None))
        operation = asyncio.create_task(prepare_gap.completed(runtime, 12345, "BTCUSD.a.GAPTEST"))
        await requested.wait()
        assert not operation.done()
        ready.set()
        assert await operation == {
            "first_tick": 0,
            "claimable_through": 123,
            "resync_failure": "",
        }
        assert session.disconnected

    asyncio.run(exercise())


def test_a_hub_reported_resync_failure_refuses_the_reload(monkeypatch):
    class Session:
        async def request(self, call):
            return SimpleNamespace(result=SimpleNamespace(name="name", company="company"))

        async def deferring(self, call, **arguments):
            return SimpleNamespace(result=SimpleNamespace(resync_failure="month failed"))

        async def disconnect(self):
            pass

    async def connected(runtime, port):
        return Session()

    monkeypatch.setattr(prepare_gap, "connected", connected)
    runtime = SimpleNamespace(redact=SimpleNamespace(hide=lambda *values: None))
    with pytest.raises(RuntimeError, match="month failed"):
        asyncio.run(prepare_gap.completed(runtime, 1, "BTCUSD.a.GAPTEST"))
