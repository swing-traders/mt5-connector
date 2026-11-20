"""Terminal-free tests of certification's client-side preparation."""

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from certification import (
    ProbeCommand,
    ProbeEvent,
    Throwaway,
    container_log_time,
    journal_after,
    journal_event_time,
    publisher_ready,
    retained_publisher_closes,
)
from test_hub_certification import close_backlog

from mt5connector.wire import calls
from mt5connector.wire.push_wire import ChartRole


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


def test_journal_boundary_uses_event_time_after_subscription_acknowledgement():
    delivered = "2026-11-20T05:54:27.067377+10:00 experts: AA\t0\t"
    log = "\n".join(
        [
            f"{delivered}19:54:26.{millis}\tticks (OLD,M1)\tticks: closed the publisher chart"
            for millis in ("470", "500", "531")
        ]
        + [f"{delivered}19:54:26.655\tticks (FRESH,M1)\tticks: opened the publisher chart"]
    )
    acknowledged = datetime(2026, 11, 19, 19, 54, 26, 541000, tzinfo=UTC).timestamp()
    assert journal_after(log, acknowledged) == [log.splitlines()[-1]]


def test_delayed_journal_delivery_across_midnight_keeps_the_previous_day():
    line = (
        "2026-11-20T00:00:01+00:00 experts: AA\t0\t23:59:59.999\t"
        "ticks (SYMBOL,M1)\tticks: closed the publisher chart"
    )
    assert (
        journal_event_time(line)
        == datetime(2026, 11, 19, 23, 59, 59, 999000, tzinfo=UTC).timestamp()
    )


def test_acknowledged_backlog_uses_caught_up_event_times_and_excludes_later_requests():
    log = "\n".join(
        [
            "2026-11-19T19:54:26.100Z INFO: the publisher chart of FIRST "
            "out of use for 2 s: closing it",
            "2026-11-19T19:54:26.100Z INFO: the publisher chart of SECOND "
            "out of use for 2 s: closing it",
            "2026-11-19T19:54:26.600Z INFO: the publisher chart of LATE "
            "out of use for 2 s: closing it",
            "2026-11-19T19:54:27Z experts: AA\t0\t19:54:26.531\t"
            "ticks (FIRST,M1)\tticks: closed the publisher chart of 'FIRST'",
            "2026-11-19T19:54:27Z experts: AA\t0\t19:54:26.700\t"
            "ticks (SECOND,M1)\tticks: closed the publisher chart of 'SECOND'",
        ]
    )
    acknowledged = datetime(2026, 11, 19, 19, 54, 26, 541000, tzinfo=UTC).timestamp()
    requested, waiting = close_backlog(
        log, ["FIRST", "SECOND", "LATE"], acknowledged_at=acknowledged
    )
    assert requested == {(ChartRole.PUBLISHER, "FIRST"), (ChartRole.PUBLISHER, "SECOND")}
    assert waiting == {(ChartRole.PUBLISHER, "SECOND")}


def test_retained_closes_include_renewals_and_expire_at_the_owning_bound():
    log = "\n".join(
        [
            "2026-11-19T19:54:26Z INFO: the publisher chart of FIRST "
            "out of use for 2 s: closing it",
            "2026-11-19T19:54:49Z WARNING: the publisher chart of RENEWED neither closed nor was "
            "kept within 30 s: closing it again",
            "2026-11-19T19:54:49Z INFO: publisher EA for LEFT "
            "left its chart out of use: closing it",
            "2026-11-19T19:54:00Z INFO: the publisher chart of OLD out of use for 2 s: closing it",
        ]
    )
    now = datetime(2026, 11, 19, 19, 54, 50, tzinfo=UTC).timestamp()
    assert retained_publisher_closes(log, now, 30) == {"FIRST", "RENEWED", "LEFT"}


def test_close_backlog_does_not_treat_a_renewal_as_a_new_idle_close():
    log = (
        "2026-11-19T19:54:49Z WARNING: the publisher chart of RETAINED neither closed nor "
        "was kept within 30 s: closing it again"
    )
    assert close_backlog(log, ["RETAINED"]) == (set(), set())


def test_log_time_refuses_a_timestamp_without_its_timezone():
    with pytest.raises(ValueError, match="timestamp has no timezone"):
        container_log_time("2026-11-19T19:54:26 INFO: a hub event")


async def test_a_closed_terminal_trade_session_is_a_normal_candidate_refusal():
    control = object.__new__(Throwaway)
    control.command = AsyncMock(return_value=SimpleNamespace(token="session"))
    control.observations = AsyncMock(
        return_value=[SimpleNamespace(token="session", event=ProbeEvent.TRADE_CLOSED)]
    )
    assert await control.trade_session_open("SYMBOL") is False
    control.command.assert_awaited_once_with(ProbeCommand.TRADE_SESSION, "SYMBOL")
