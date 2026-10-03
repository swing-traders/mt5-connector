"""The broker clock's verification against the trade-server time the EA relays, and the routes it
gates. Under EDT the broker's clock runs 10,800 s ahead of UTC: broker 1,752,580,800 is
2025-07-15T09:00:00Z."""

import logging
import math
import os
import time
from collections.abc import Callable
from dataclasses import dataclass

import pytest
from mirror_samples import CLOCK

from mt5server.app.app import create_app
from mt5server.app.clock_check import ClockCheck
from mt5server.app.history import FloorStore, History
from mt5server.app.server_time import Received, ServerTimeSample, ServerTimeSink

BROKER_EPOCH = 1_752_580_800
UTC_EPOCH = 1_752_570_000
UNVERIFIED = {"ok": False, "error": {"code": -1, "message": "the broker clock is not verified"}}


@dataclass(frozen=True)
class _Event:
    at: float
    sample: ServerTimeSample | None = None
    probe: Callable[[], None] | None = None


class Timeline:
    """The server's clocks, advanced only while the check waits on this stand-in for its sink, and
    the samples and probes due on them."""

    def __init__(self, wall_at_start: int) -> None:
        self.now = 0.0
        self._wall_at_start = wall_at_start
        self._events: list[_Event] = []
        self._latest: Received | None = None

    def monotonic(self) -> float:
        return self.now

    def time(self) -> float:
        return self._wall_at_start + self.now

    def arrive(self, at: float, sample: ServerTimeSample) -> None:
        self._events = sorted([*self._events, _Event(at, sample=sample)], key=lambda e: e.at)

    def probe(self, at: float, probe: Callable[[], None]) -> None:
        self._events = sorted([*self._events, _Event(at, probe=probe)], key=lambda e: e.at)

    def wait_newer(self, than: Received | None, timeout: float | None) -> Received | None:
        if timeout is None:
            end = math.inf
        else:
            end = self.now + max(timeout, 0.0)
        while self._latest is than:
            if self._events and self._events[0].at <= end:
                event = self._events.pop(0)
                self.now = max(self.now, event.at)
                if event.sample is not None:
                    self._latest = Received(event.sample, self.now)
                else:
                    event.probe()
            elif end == math.inf:
                # Nothing is left to happen: the check's run ends here.
                raise SystemExit
            else:
                self.now = end
                break
        return self._latest


def sample(trade_server: int, connected: bool = True) -> ServerTimeSample:
    """EURUSD's sample with the last quote 2 s before the trade server's time."""
    return ServerTimeSample(
        symbol="EURUSD",
        trade_server=trade_server,
        current=trade_server - 2,
        gmt=1_752_570_029,
        connected=connected,
    )


@pytest.fixture
def timeline_at(monkeypatch):
    """Starts a timeline whose wall clock reads the given epoch at its start, and puts the server's
    clocks on it."""

    def start(wall_at_start: int) -> Timeline:
        timeline = Timeline(wall_at_start)
        monkeypatch.setattr(time, "monotonic", timeline.monotonic)
        monkeypatch.setattr(time, "time", timeline.time)
        return timeline

    return start


@pytest.fixture
def exits(monkeypatch):
    codes = []
    monkeypatch.setattr(os, "_exit", codes.append)
    return codes


@pytest.fixture(autouse=True)
def check_logs(caplog):
    caplog.set_level(logging.INFO, logger="mt5server.app.clock_check")


def clock_check(timeline: Timeline) -> ClockCheck:
    return ClockCheck(timeline, CLOCK, max_age_s=30, check_s=300, bootstrap_s=120)


def routes(terminal, commissions, check: ClockCheck):
    history = History(terminal, CLOCK, FloorStore(), retry_s=0.01, floor_ttl_s=900)
    return create_app(
        terminal, commissions, CLOCK, ServerTimeSink(), check.status, history
    ).test_client()


def run_to_its_end(check: ClockCheck) -> None:
    with pytest.raises(SystemExit):
        check.run()


def messages(caplog, level: int) -> list[str]:
    return [record.getMessage() for record in caplog.records if record.levelno == level]


def test_a_fresh_sample_near_the_server_clock_verifies_the_clock(
    timeline_at, terminal, commissions, stub, exits, caplog
):
    timeline = timeline_at(1_752_570_030)
    check = clock_check(timeline)
    client = routes(terminal, commissions, check)
    answers = []
    timeline.arrive(0, sample(BROKER_EPOCH))
    timeline.probe(1, lambda: answers.append(client.get("/health")))

    run_to_its_end(check)

    assert exits == []
    assert answers[0].status_code == 200
    assert answers[0].json == {
        "ok": True,
        "result": {
            "symbol": "EURUSD",
            "trade_server": 1_752_570_000,
            "current": 1_752_569_998,
            "gmt": 1_752_570_029,
            "skew_s": -30,
            "offset_s": 10_800,
        },
    }
    stub.terminal_info.assert_not_called()
    assert messages(caplog, logging.INFO) == [
        "broker clock measured on EURUSD: trade server at 2025-07-15T09:00:00+00:00, -30 s from "
        "the server clock; offset +10800 s",
        "broker clock verified",
        "broker clock unverified: the latest server-time sample arrived 30 s ago, connected=True",
    ]


def test_a_fresh_sample_an_hour_off_exits_with_both_times_and_the_offset(
    timeline_at, exits, caplog
):
    timeline = timeline_at(1_752_573_600)
    check = clock_check(timeline)
    timeline.arrive(0, sample(BROKER_EPOCH))

    check.run()

    assert exits == [1]
    assert check.status.read() is None
    assert messages(caplog, logging.INFO) == [
        "broker clock measured on EURUSD: trade server at 2025-07-15T09:00:00+00:00, -3600 s from "
        "the server clock; offset +10800 s"
    ]
    assert messages(caplog, logging.CRITICAL) == [
        "broker clock: EURUSD trade server at broker epoch 1752580800 reads "
        "2025-07-15T09:00:00+00:00, -3600 s from the server clock's 2025-07-15T10:00:00+00:00, "
        "under offset +10800 s"
    ]


def test_a_disconnected_sample_is_not_fresh_and_the_check_keeps_waiting(
    timeline_at, terminal, commissions, exits
):
    timeline = timeline_at(UTC_EPOCH + 30)
    check = clock_check(timeline)
    client = routes(terminal, commissions, check)
    statuses = []
    timeline.arrive(0, sample(BROKER_EPOCH, connected=False))
    timeline.probe(10, lambda: statuses.append(client.get("/health").status_code))
    timeline.arrive(20, sample(BROKER_EPOCH + 20))
    timeline.probe(21, lambda: statuses.append(client.get("/health").status_code))

    run_to_its_end(check)

    assert statuses == [503, 200]
    assert exits == []


def test_no_fresh_sample_within_the_bootstrap_window_exits(timeline_at, exits, caplog):
    timeline = timeline_at(UTC_EPOCH)
    check = clock_check(timeline)
    timeline.arrive(5, sample(BROKER_EPOCH + 5, connected=False))
    timeline.arrive(121, sample(BROKER_EPOCH + 121))

    check.run()

    assert exits == [1]
    assert timeline.now == 120
    assert check.status.read() is None
    assert messages(caplog, logging.CRITICAL) == [
        "broker clock: no fresh server-time sample in 120 s"
    ]


def test_a_stale_sample_unverifies_the_clock_until_a_fresh_one_verifies_it_again(
    timeline_at, terminal, commissions, stub, exits, caplog
):
    stub.positions_total.return_value = 0
    timeline = timeline_at(UTC_EPOCH + 30)
    check = clock_check(timeline)
    client = routes(terminal, commissions, check)
    answers = {}

    def observe():
        answers[timeline.now] = (client.get("/health"), client.post("/mt5/positions_total"))

    timeline.arrive(0, sample(BROKER_EPOCH))
    timeline.probe(29, observe)
    timeline.probe(31, observe)
    timeline.arrive(40, sample(BROKER_EPOCH + 40))
    timeline.probe(41, observe)

    run_to_its_end(check)

    assert exits == []
    health, positions = answers[29]
    assert (health.status_code, positions.status_code) == (200, 200)
    health, positions = answers[31]
    assert (health.status_code, health.json) == (503, UNVERIFIED)
    assert (positions.status_code, positions.json) == (503, UNVERIFIED)
    health, positions = answers[41]
    assert (health.status_code, positions.status_code) == (200, 200)
    assert stub.positions_total.call_count == 2
    stub.terminal_info.assert_not_called()
    assert messages(caplog, logging.INFO) == [
        "broker clock measured on EURUSD: trade server at 2025-07-15T09:00:00+00:00, -30 s from "
        "the server clock; offset +10800 s",
        "broker clock verified",
        "broker clock unverified: the latest server-time sample arrived 30 s ago, connected=True",
        "broker clock measured on EURUSD: trade server at 2025-07-15T09:00:40+00:00, -30 s from "
        "the server clock; offset +10800 s",
        "broker clock verified",
        "broker clock unverified: the latest server-time sample arrived 30 s ago, connected=True",
    ]


def test_a_disconnected_sample_unverifies_the_clock_at_once(
    timeline_at, terminal, commissions, exits, caplog
):
    timeline = timeline_at(UTC_EPOCH + 30)
    check = clock_check(timeline)
    client = routes(terminal, commissions, check)
    statuses = []
    timeline.arrive(0, sample(BROKER_EPOCH))
    timeline.arrive(10, sample(BROKER_EPOCH + 10, connected=False))
    timeline.probe(11, lambda: statuses.append(client.get("/health").status_code))
    timeline.arrive(15, sample(BROKER_EPOCH + 15))
    timeline.probe(16, lambda: statuses.append(client.get("/health").status_code))

    run_to_its_end(check)

    assert statuses == [503, 200]
    assert exits == []
    assert messages(caplog, logging.INFO)[2] == (
        "broker clock unverified: the latest server-time sample arrived 0 s ago, connected=False"
    )


def test_the_check_reverifies_the_latest_sample_every_interval(timeline_at, exits, caplog):
    timeline = timeline_at(UTC_EPOCH + 30)
    check = clock_check(timeline)
    for at in range(0, 321, 20):
        timeline.arrive(at, sample(BROKER_EPOCH + at))

    run_to_its_end(check)

    assert exits == []
    assert messages(caplog, logging.INFO) == [
        "broker clock measured on EURUSD: trade server at 2025-07-15T09:00:00+00:00, -30 s from "
        "the server clock; offset +10800 s",
        "broker clock verified",
        "broker clock measured on EURUSD: trade server at 2025-07-15T09:05:00+00:00, -30 s from "
        "the server clock; offset +10800 s",
        "broker clock unverified: the latest server-time sample arrived 30 s ago, connected=True",
    ]


def test_a_later_mismatch_exits(timeline_at, exits, caplog):
    timeline = timeline_at(UTC_EPOCH + 30)
    check = clock_check(timeline)
    for at in range(0, 300, 20):
        timeline.arrive(at, sample(BROKER_EPOCH + at))
    timeline.arrive(300, sample(BROKER_EPOCH + 300 + 7200))

    check.run()

    assert exits == [1]
    assert timeline.now == 300
    assert messages(caplog, logging.CRITICAL) == [
        "broker clock: EURUSD trade server at broker epoch 1752588300 reads "
        "2025-07-15T11:05:00+00:00, +7170 s from the server clock's 2025-07-15T09:05:30+00:00, "
        "under offset +10800 s"
    ]


def test_a_trade_server_time_its_zone_skips_exits(timeline_at, exits, caplog):
    # 2025-03-09T09:30 on the broker's clock is New York's 02:30, which its spring change skips.
    timeline = timeline_at(UTC_EPOCH)
    check = clock_check(timeline)
    timeline.arrive(0, sample(1_741_512_600))

    check.run()

    assert exits == [1]
    assert messages(caplog, logging.CRITICAL) == ["broker clock: the verification failed"]
