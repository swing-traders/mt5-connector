"""The broker clock's verification against the clock symbol's live tick, and the readiness it gates.
Under EDT the broker's clock runs 10,800 s ahead of UTC."""

import logging
import os
import threading
import time

import pytest
import requests
from mirror_samples import CLOCK, struct_sample

from mt5connect import mirror
from mt5server.app import clock_check
from mt5server.app.app import create_app
from mt5server.app.clock_check import ClockCheck

# The server's clock in these tests: 2025-07-15T09:00:00Z, which the broker's clock reads as
# 1,752,580,800.
NOW = 1_752_570_000
BROKER_NOW = 1_752_580_800


def tick(broker_time: int, time_msc: int) -> tuple:
    return struct_sample(mirror.StructName.TICK)._replace(time=broker_time, time_msc=time_msc)


@pytest.fixture(autouse=True)
def no_read_gap(monkeypatch):
    monkeypatch.setattr(clock_check, "_READ_GAP_S", 0)


@pytest.fixture
def server_now(monkeypatch):
    monkeypatch.setattr(time, "time", lambda: float(NOW))


@pytest.fixture
def exits(monkeypatch):
    codes = []
    monkeypatch.setattr(os, "_exit", codes.append)
    return codes


@pytest.fixture
def check(terminal):
    return ClockCheck(terminal, CLOCK, "EURUSD")


@pytest.fixture
def health(stub, terminal, commissions, check):
    stub.terminal_info.return_value = struct_sample(mirror.StructName.TERMINAL_INFO)
    client = create_app(terminal, commissions, CLOCK, check.ready).test_client()
    return lambda: client.get("/health")


def run_through_the_first_check(check: ClockCheck) -> None:
    """Runs the check until the stub's read after the first verification raises SystemExit, which
    stands in for the process ending."""
    with pytest.raises(SystemExit):
        check.run(0)


def messages(caplog, level: int) -> list[str]:
    return [record.getMessage() for record in caplog.records if record.levelno == level]


def test_health_is_unavailable_until_the_clock_is_verified(stub, health):
    response = health()
    assert response.status_code == 503
    assert response.json == {
        "ok": False,
        "error": {"code": -1, "message": "the broker clock is not verified"},
    }
    stub.terminal_info.assert_not_called()


def test_a_fresh_tick_near_the_server_clock_makes_the_server_ready(
    stub, check, health, server_now, exits, caplog
):
    caplog.set_level(logging.INFO, logger="mt5server.app.clock_check")
    stub.symbol_info_tick.side_effect = [
        tick(BROKER_NOW + 29, (BROKER_NOW + 29) * 1000 + 400),
        tick(BROKER_NOW + 30, (BROKER_NOW + 30) * 1000 + 100),
        SystemExit(),
    ]

    run_through_the_first_check(check)

    assert check.ready.is_set()
    assert health().status_code == 200
    assert exits == []
    assert messages(caplog, logging.INFO) == [
        "broker clock measured on EURUSD: tick at 2025-07-15T09:00:30+00:00, +30 s from the "
        "server clock; offset +10800 s"
    ]


def test_a_fresh_tick_an_hour_off_exits_with_both_times_and_the_offset(
    stub, check, health, server_now, exits, caplog
):
    caplog.set_level(logging.INFO, logger="mt5server.app.clock_check")
    stub.symbol_info_tick.side_effect = [
        tick(BROKER_NOW + 3599, (BROKER_NOW + 3599) * 1000),
        tick(BROKER_NOW + 3600, (BROKER_NOW + 3600) * 1000),
    ]

    check.run(300)

    assert exits == [1]
    assert not check.ready.is_set()
    assert health().status_code == 503
    assert messages(caplog, logging.INFO) == [
        "broker clock measured on EURUSD: tick at 2025-07-15T10:00:00+00:00, +3600 s from the "
        "server clock; offset +10800 s"
    ]
    assert messages(caplog, logging.CRITICAL) == [
        "broker clock: EURUSD tick at broker epoch 1752584400 reads 2025-07-15T10:00:00+00:00, "
        "+3600 s from the server clock's 2025-07-15T09:00:00+00:00, under offset +10800 s"
    ]


def test_a_still_tick_defers_and_makes_the_server_ready(
    stub, check, health, server_now, exits, caplog
):
    caplog.set_level(logging.INFO, logger="mt5server.app.clock_check")
    still = tick(BROKER_NOW - 86_400, (BROKER_NOW - 86_400) * 1000)
    stub.symbol_info_tick.side_effect = [still, still, SystemExit()]

    run_through_the_first_check(check)

    assert check.ready.is_set()
    assert health().status_code == 200
    assert exits == []
    assert messages(caplog, logging.INFO) == [
        "broker clock verification deferred: EURUSD did not tick in 0 s, the market is closed; "
        "offset +10800 s"
    ]


def test_no_tick_at_connect_exits(stub, check, server_now, exits, caplog):
    stub.symbol_info_tick.return_value = None
    stub.last_error.return_value = (-4, "Terminal: Not found")

    check.run(300)

    assert exits == [1]
    assert not check.ready.is_set()
    assert messages(caplog, logging.CRITICAL) == [
        "broker clock: no EURUSD tick: (-4, 'Terminal: Not found')"
    ]


def test_no_tick_on_a_later_check_warns(stub, check, server_now, caplog):
    stub.symbol_info_tick.return_value = None
    stub.last_error.return_value = (-4, "Terminal: Not found")

    check.verify(at_connect=False)

    assert messages(caplog, logging.WARNING) == [
        "broker clock verification deferred: no EURUSD tick: (-4, 'Terminal: Not found')"
    ]


def test_a_later_mismatch_exits(stub, check, server_now, exits):
    stub.symbol_info_tick.side_effect = [
        tick(BROKER_NOW, BROKER_NOW * 1000),
        tick(BROKER_NOW + 1, (BROKER_NOW + 1) * 1000),
        tick(BROKER_NOW + 7199, (BROKER_NOW + 7199) * 1000),
        tick(BROKER_NOW + 7200, (BROKER_NOW + 7200) * 1000),
    ]

    check.run(0)

    assert check.ready.is_set()
    assert exits == [1]


def test_an_unexpected_failure_exits(stub, check, server_now, exits, caplog):
    stub.symbol_info_tick.side_effect = RuntimeError("IPC broke")

    check.run(300)

    assert exits == [1]
    assert messages(caplog, logging.CRITICAL) == ["broker clock: the verification failed"]


def test_the_periodic_check_never_overlaps_a_route_call(stub, check, served, exits):
    spans = []
    still = tick(BROKER_NOW, BROKER_NOW * 1000)
    # Three deferred checks, then a fresh tick a year behind the server's clock, which ends the
    # check's thread through its exit.
    ticks = iter([still] * 7 + [tick(BROKER_NOW, BROKER_NOW * 1000 + 1)])

    def read_tick(symbol):
        entered = time.monotonic()
        time.sleep(0.05)
        spans.append((entered, time.monotonic()))
        return next(ticks)

    def positions_total():
        entered = time.monotonic()
        time.sleep(0.05)
        spans.append((entered, time.monotonic()))
        return 0

    stub.symbol_info_tick.side_effect = read_tick
    stub.positions_total.side_effect = positions_total
    threads = [threading.Thread(target=check.run, args=(0,))] + [
        threading.Thread(target=requests.post, args=(f"{served}/mt5/positions_total",))
        for _ in range(3)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    assert exits == [1]
    assert len(spans) == 11
    ordered = sorted(spans)
    for earlier, later in zip(ordered, ordered[1:], strict=False):
        assert earlier[1] <= later[0]
