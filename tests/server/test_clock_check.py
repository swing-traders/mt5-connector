"""The broker clock's check against the trade-server time the EA relays, and the routes it gates."""

import calendar
import logging
import math
import os
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

import pytest
from chart_posts import ChartPosts, publishers_on
from mirror_samples import CLOCK

from mt5connector.server.app import create_app
from mt5connector.server.clock_check import ClockCheck
from mt5connector.server.encoding import RepeatedHours
from mt5connector.server.history import FloorStore, History
from mt5connector.server.server_time import Received, ServerTimeSample, ServerTimeSink

BROKER_EPOCH = 1_752_580_800  # an EDT date: the broker clock, New York + 7 h, runs 3 h ahead of UTC
UTC_EPOCH = 1_752_570_000
MAX_AGE_S = 30
UNVERIFIED = {"ok": False, "error": {"code": -1, "message": "the broker clock is not verified"}}


@dataclass(frozen=True)
class _Event:
    at: float
    sample: ServerTimeSample | None = None
    probe: Callable[[], None] | None = None


class Timeline:
    """The server's clocks, advanced only while the check waits on this stand-in for its sink, and
    the samples and probes due on them; a real sink keeps the samples as they arrive."""

    def __init__(self, wall_at_start: int) -> None:
        self.now = 0.0
        self._wall_at_start = wall_at_start
        self._events: list[_Event] = []
        self._sink = ServerTimeSink(max_age_s=MAX_AGE_S)

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
        while self._sink.latest() is than:
            if self._events and self._events[0].at <= end:
                event = self._events.pop(0)
                self.now = max(self.now, event.at)
                if event.sample is not None:
                    self._sink.write(event.sample)
                else:
                    event.probe()
            elif end == math.inf:
                # Nothing is left to happen: the check's run ends here.
                raise SystemExit
            else:
                self.now = end
                break
        return self._sink.latest()


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
    caplog.set_level(logging.INFO, logger="mt5connector.server.clock_check")


def clock_check(timeline: Timeline) -> ClockCheck:
    return ClockCheck(timeline, CLOCK, max_age_s=MAX_AGE_S, check_s=300, bootstrap_s=120)


def routes(terminal, commissions, check: ClockCheck):
    repeated_hours = RepeatedHours()
    history = History(terminal, CLOCK, repeated_hours, FloorStore(), retry_s=0.01, floor_ttl_s=900)
    return create_app(
        terminal,
        commissions,
        CLOCK,
        repeated_hours,
        ServerTimeSink(max_age_s=MAX_AGE_S),
        check.status,
        history,
        publishers_on(ChartPosts()),
        workers=3,
        retry_s=5,
    ).test_client()


def run_to_its_end(check: ClockCheck) -> None:
    with pytest.raises(SystemExit):
        check.run()


def messages(caplog, level: int) -> list[str]:
    return [record.getMessage() for record in caplog.records if record.levelno == level]


def health_at(timeline: Timeline, client, times: list[float]) -> dict:
    """Probes /health at each of the times, after any sample already scheduled for the same time;
    the answers fill in as the check's run reaches them."""
    answers = {}

    def probe():
        answers[timeline.now] = client.get("/health")

    for at in times:
        timeline.probe(at, probe)
    return answers


def statuses(answers: dict) -> dict[float, int]:
    return {at: answer.status_code for at, answer in answers.items()}


def test_a_fresh_sample_near_the_server_clock_verifies_the_clock(
    timeline_at, terminal, commissions, stub, exits, caplog
):
    timeline = timeline_at(UTC_EPOCH)
    check = clock_check(timeline)
    client = routes(terminal, commissions, check)
    answers = []
    for at in range(0, 31, 10):
        timeline.arrive(at, sample(BROKER_EPOCH - 30 + at))
    timeline.probe(31, lambda: answers.append(client.get("/health")))

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
            "in_flight": 0,
            "peak_in_flight": 0,
            "refusals": 0,
            "workers": 3,
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
    timeline = timeline_at(1_752_573_570)
    check = clock_check(timeline)
    for at in range(0, 31, 10):
        timeline.arrive(at, sample(BROKER_EPOCH - 30 + at))

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
    for at in range(20, 51, 15):
        timeline.arrive(at, sample(BROKER_EPOCH + at))
    timeline.probe(51, lambda: statuses.append(client.get("/health").status_code))

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

    for at in [*range(0, 31, 10), *range(70, 101, 10)]:
        timeline.arrive(at, sample(BROKER_EPOCH + at))
    timeline.probe(59, observe)
    timeline.probe(61, observe)
    timeline.probe(101, observe)

    run_to_its_end(check)

    assert exits == []
    health, positions = answers[59]
    assert (health.status_code, positions.status_code) == (200, 200)
    health, positions = answers[61]
    assert (health.status_code, health.json) == (503, UNVERIFIED)
    assert (positions.status_code, positions.json) == (503, UNVERIFIED)
    health, positions = answers[101]
    assert (health.status_code, positions.status_code) == (200, 200)
    assert stub.positions_total.call_count == 2
    stub.terminal_info.assert_not_called()
    assert messages(caplog, logging.INFO) == [
        "broker clock measured on EURUSD: trade server at 2025-07-15T09:00:30+00:00, -30 s from "
        "the server clock; offset +10800 s",
        "broker clock verified",
        "broker clock unverified: the latest server-time sample arrived 30 s ago, connected=True",
        "broker clock measured on EURUSD: trade server at 2025-07-15T09:01:40+00:00, -30 s from "
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
    for at in range(0, 31, 10):
        timeline.arrive(at, sample(BROKER_EPOCH + at))
    timeline.arrive(35, sample(BROKER_EPOCH + 35, connected=False))
    timeline.arrive(40, sample(BROKER_EPOCH + 40))
    answers = health_at(timeline, client, [34, 35, 41])

    run_to_its_end(check)

    assert statuses(answers) == {34: 200, 35: 503, 41: 503}
    assert exits == []
    assert messages(caplog, logging.INFO)[2] == (
        "broker clock unverified: the latest server-time sample arrived 0 s ago, connected=False"
    )


def test_the_check_reverifies_the_latest_sample_every_interval(timeline_at, exits, caplog):
    timeline = timeline_at(UTC_EPOCH + 30)
    check = clock_check(timeline)
    for at in range(0, 331, 10):
        timeline.arrive(at, sample(BROKER_EPOCH + at))

    run_to_its_end(check)

    assert exits == []
    assert messages(caplog, logging.INFO) == [
        "broker clock measured on EURUSD: trade server at 2025-07-15T09:00:30+00:00, -30 s from "
        "the server clock; offset +10800 s",
        "broker clock verified",
        "broker clock measured on EURUSD: trade server at 2025-07-15T09:05:30+00:00, -30 s from "
        "the server clock; offset +10800 s",
        "broker clock unverified: the latest server-time sample arrived 30 s ago, connected=True",
    ]


def test_a_later_mismatch_exits(timeline_at, exits, caplog):
    timeline = timeline_at(UTC_EPOCH + 30)
    check = clock_check(timeline)
    for at in range(0, 330, 10):
        timeline.arrive(at, sample(BROKER_EPOCH + at))
    timeline.arrive(330, sample(BROKER_EPOCH + 330 + 7200))

    check.run()

    assert exits == [1]
    assert timeline.now == 330
    assert messages(caplog, logging.CRITICAL) == [
        "broker clock: EURUSD trade server at broker epoch 1752588330 reads "
        "2025-07-15T11:05:30+00:00, +7170 s from the server clock's 2025-07-15T09:06:00+00:00, "
        "under offset +10800 s"
    ]


def test_a_trade_server_time_its_zone_skips_exits(timeline_at, exits, caplog):
    # 2025-03-09T09:30 on the broker's clock is New York's 02:30, which its spring change skips.
    timeline = timeline_at(UTC_EPOCH)
    check = clock_check(timeline)
    for at in range(0, 31, 10):
        timeline.arrive(at, sample(1_741_512_600 + at))

    check.run()

    assert exits == [1]
    assert messages(caplog, logging.CRITICAL) == ["broker clock: the verification failed"]


def test_the_clock_is_verified_once_the_terminal_has_been_connected_for_the_maximum_age(
    timeline_at, terminal, commissions, exits
):
    timeline = timeline_at(UTC_EPOCH + 30)
    check = clock_check(timeline)
    client = routes(terminal, commissions, check)
    for at in range(0, 41, 5):
        timeline.arrive(at, sample(BROKER_EPOCH + at))
    answers = health_at(timeline, client, [0, 15, 29, 30])

    run_to_its_end(check)

    assert exits == []
    assert statuses(answers) == {0: 503, 15: 503, 29: 503, 30: 200}
    assert [answers[at].json for at in (0, 15, 29)] == [UNVERIFIED] * 3


def test_a_disconnected_sample_restarts_the_connected_run(
    timeline_at, terminal, commissions, exits
):
    timeline = timeline_at(UTC_EPOCH + 30)
    check = clock_check(timeline)
    client = routes(terminal, commissions, check)
    for at in range(0, 16, 5):
        timeline.arrive(at, sample(BROKER_EPOCH + at))
    timeline.arrive(20, sample(BROKER_EPOCH + 20, connected=False))
    for at in range(25, 61, 5):
        timeline.arrive(at, sample(BROKER_EPOCH + at))
    answers = health_at(timeline, client, [21, 30, 54, 55])

    run_to_its_end(check)

    assert exits == []
    assert statuses(answers) == {21: 503, 30: 503, 54: 503, 55: 200}


def test_a_stale_stream_restarts_the_connected_run(timeline_at, terminal, commissions, exits):
    timeline = timeline_at(UTC_EPOCH + 30)
    check = clock_check(timeline)
    client = routes(terminal, commissions, check)
    for at in [*range(0, 41, 5), *range(80, 121, 5)]:
        timeline.arrive(at, sample(BROKER_EPOCH + at))
    answers = health_at(timeline, client, [30, 69, 71, 80, 109, 110])

    run_to_its_end(check)

    assert exits == []
    assert statuses(answers) == {30: 200, 69: 200, 71: 503, 80: 503, 109: 503, 110: 200}


def test_a_run_maturing_between_samples_verifies_the_clock_at_its_maturity(
    timeline_at, terminal, commissions, exits
):
    timeline = timeline_at(UTC_EPOCH + 30)
    check = clock_check(timeline)
    client = routes(terminal, commissions, check)
    for at in range(0, 31, 10):
        timeline.arrive(at, sample(BROKER_EPOCH + at))
    timeline.arrive(40, sample(BROKER_EPOCH + 40, connected=False))
    for at in [45, 55, 65, 74, 84]:
        timeline.arrive(at, sample(BROKER_EPOCH + at))
    answers = health_at(timeline, client, [31, 74, 75.5])

    run_to_its_end(check)

    assert exits == []
    assert statuses(answers) == {31: 200, 74: 503, 75.5: 200}


def test_a_run_maturing_inside_the_bootstrap_window_verifies_the_clock(
    timeline_at, terminal, commissions, exits
):
    timeline = timeline_at(UTC_EPOCH + 30)
    check = clock_check(timeline)
    client = routes(terminal, commissions, check)
    for at in [0, 10, 20, 29, 45, 55]:
        timeline.arrive(at, sample(BROKER_EPOCH + at))
    answers = health_at(timeline, client, [29, 30.5])

    run_to_its_end(check)

    assert exits == []
    assert statuses(answers) == {29: 503, 30.5: 200}


def test_a_run_not_mature_at_the_end_of_the_bootstrap_window_exits(timeline_at, exits, caplog):
    timeline = timeline_at(UTC_EPOCH + 30)
    check = clock_check(timeline)
    for at in range(100, 141, 5):
        timeline.arrive(at, sample(BROKER_EPOCH + at))

    check.run()

    assert exits == [1]
    assert timeline.now == 120
    assert check.status.read() is None
    assert messages(caplog, logging.CRITICAL) == [
        "broker clock: the terminal was not connected for 30 s without a break in 120 s"
    ]


# New York's 2026 autumn change: 01:00-02:00 local, broker 08:00-09:00, runs under EDT from 05:00Z
# and again under EST from 06:00Z. Each case: the true-UTC instant the clock is checked at, the
# broker epoch a correct clock samples then, and the offset in effect.
AUTUMN_CHECKS = [
    (calendar.timegm((2026, 11, 1, 5, 30, 0)), calendar.timegm((2026, 11, 1, 8, 30, 0)), 10_800),
    (calendar.timegm((2026, 11, 1, 6, 30, 0)), calendar.timegm((2026, 11, 1, 8, 30, 0)), 7_200),
    (calendar.timegm((2026, 11, 1, 7, 30, 0)), calendar.timegm((2026, 11, 1, 9, 30, 0)), 7_200),
]
AUTUMN_IDS = ["first-occurrence", "second-occurrence", "after-the-hour"]


def connected_samples_until(timeline: Timeline, broker_at_check: int) -> None:
    """Connected samples every 10 s through t=30, the last one naming `broker_at_check`."""
    for at in range(0, 31, 10):
        timeline.arrive(at, sample(broker_at_check - 30 + at))


@pytest.mark.parametrize(("utc", "broker", "offset_s"), AUTUMN_CHECKS, ids=AUTUMN_IDS)
def test_a_correct_clock_verifies_through_the_repeated_autumn_hour(
    utc, broker, offset_s, timeline_at, exits, caplog
):
    timeline = timeline_at(utc - 30)
    check = clock_check(timeline)
    connected_samples_until(timeline, broker)

    run_to_its_end(check)

    assert exits == []
    iso = datetime.fromtimestamp(utc, UTC).isoformat()
    assert messages(caplog, logging.INFO)[:2] == [
        f"broker clock measured on EURUSD: trade server at {iso}, +0 s from the server clock; "
        f"offset +{offset_s} s",
        "broker clock verified",
    ]


@pytest.mark.parametrize("error_s", [3_600, -3_600], ids=["an-hour-ahead", "an-hour-behind"])
@pytest.mark.parametrize(("utc", "broker", "offset_s"), AUTUMN_CHECKS, ids=AUTUMN_IDS)
def test_a_clock_an_hour_off_exits_through_the_repeated_autumn_hour(
    utc, broker, offset_s, error_s, timeline_at, exits, caplog
):
    timeline = timeline_at(utc - 30)
    check = clock_check(timeline)
    connected_samples_until(timeline, broker + error_s)

    check.run()

    assert exits == [1]
    assert check.status.read() is None
    (critical,) = messages(caplog, logging.CRITICAL)
    assert critical.startswith(
        f"broker clock: EURUSD trade server at broker epoch {broker + error_s} reads "
    )


def test_a_sample_from_before_the_autumn_change_checked_after_it_verifies_the_clock(
    timeline_at, exits, caplog
):
    # Sampled at 05:59:50Z, broker 08:59:50 under EDT; checked at 06:00:10Z, under EST.
    sampled = calendar.timegm((2026, 11, 1, 5, 59, 50))
    timeline = timeline_at(sampled - 10)
    check = clock_check(timeline)
    timeline.arrive(0, sample(calendar.timegm((2026, 11, 1, 8, 59, 40))))
    timeline.arrive(10, sample(calendar.timegm((2026, 11, 1, 8, 59, 50))))

    run_to_its_end(check)

    assert exits == []
    assert messages(caplog, logging.INFO)[:2] == [
        "broker clock measured on EURUSD: trade server at 2026-11-01T05:59:50+00:00, -20 s from "
        "the server clock; offset +7200 s",
        "broker clock verified",
    ]
