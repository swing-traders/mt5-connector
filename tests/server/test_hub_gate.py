"""The hub's clock gate under the HTTP server's sample rules: fresh for the max-age, from a terminal
connected without a break for that long, within the tolerance; a missed bootstrap or a contradicting
sample shuts it and never ends the process."""

import asyncio
import logging
import time

from mirror_samples import CLOCK

from mt5connector.server.hub_gate import TOLERANCE_S, ClockGate
from mt5connector.server.wire import binary

MAX_AGE_S = 0.1


def gate(**overrides) -> ClockGate:
    values = {"max_age_s": MAX_AGE_S, "check_s": 300.0, "bootstrap_s": 120.0} | overrides
    return ClockGate(CLOCK, **values)


def sample(*, connected=True, skew_s=0, symbol="EURUSD") -> binary.ServerTime:
    now = int(time.time())
    broker = CLOCK.to_broker(now + skew_s)
    return binary.ServerTime(symbol, broker, broker - 2, now, connected)


async def relayed(clock_gate: ClockGate, count: int, every_s: float, **fields) -> None:
    for _ in range(count):
        clock_gate.write(sample(**fields))
        await asyncio.sleep(every_s)


async def test_a_run_of_samples_as_long_as_the_max_age_verifies_the_clock():
    clock_gate = gate()

    await relayed(clock_gate, 4, MAX_AGE_S / 3)

    verification = clock_gate.verification
    assert verification is not None
    assert (verification.symbol, abs(verification.skew_s) <= 1) == ("EURUSD", True)
    assert verification.offset_s == CLOCK.offset_at(int(time.time())).total_seconds()
    assert clock_gate.is_fresh()


async def test_a_lone_sample_goes_stale_as_its_run_matures_and_never_verifies():
    clock_gate = gate()

    clock_gate.write(sample())
    await asyncio.sleep(MAX_AGE_S * 1.5)

    assert (clock_gate.verification, clock_gate.is_fresh()) == (None, False)


async def test_a_disconnected_sample_breaks_the_run():
    clock_gate = gate()

    await relayed(clock_gate, 2, MAX_AGE_S / 3)
    await relayed(clock_gate, 1, MAX_AGE_S / 3, connected=False)
    await relayed(clock_gate, 2, MAX_AGE_S / 3)

    assert clock_gate.verification is None


async def test_the_verification_clears_once_the_latest_sample_goes_stale(caplog):
    caplog.set_level(logging.INFO, logger="mt5connector.server.hub_gate")
    clock_gate = gate()
    await relayed(clock_gate, 4, MAX_AGE_S / 3)
    assert clock_gate.verification is not None

    await asyncio.sleep(MAX_AGE_S * 1.2)

    assert clock_gate.verification is None
    assert any("broker clock unverified" in record.getMessage() for record in caplog.records)


async def test_a_sample_past_the_tolerance_shuts_the_gate_with_one_critical_log(caplog):
    caplog.set_level(logging.CRITICAL, logger="mt5connector.server.hub_gate")
    clock_gate = gate()

    await relayed(clock_gate, 6, MAX_AGE_S / 3, skew_s=TOLERANCE_S + 60)

    assert clock_gate.verification is None
    assert len(caplog.records) == 1
    assert "trade server" in caplog.records[0].getMessage()


async def test_a_missed_bootstrap_is_logged_once_and_a_later_run_still_verifies(caplog):
    caplog.set_level(logging.CRITICAL, logger="mt5connector.server.hub_gate")
    clock_gate = gate(bootstrap_s=0.05)
    clock_gate.start()

    await asyncio.sleep(0.1)
    missed = (clock_gate.bootstrap_missed, clock_gate.verification)
    await relayed(clock_gate, 4, MAX_AGE_S / 3)

    assert missed == (True, None)
    assert clock_gate.verification is not None
    assert [record.getMessage() for record in caplog.records] == [
        "broker clock: no fresh server-time sample in 0.05 s"
    ]


async def test_a_bootstrap_whose_run_has_not_matured_is_missed_naming_the_connected_run(caplog):
    caplog.set_level(logging.CRITICAL, logger="mt5connector.server.hub_gate")
    clock_gate = gate(bootstrap_s=0.05, max_age_s=1.0)
    clock_gate.start()

    await relayed(clock_gate, 3, 0.03)

    assert clock_gate.bootstrap_missed
    assert [record.getMessage() for record in caplog.records] == [
        "broker clock: the terminal was not connected for 1.0 s without a break in 0.05 s"
    ]


async def test_a_verified_clock_is_measured_again_once_its_check_interval_passes(caplog):
    caplog.set_level(logging.CRITICAL, logger="mt5connector.server.hub_gate")
    clock_gate = gate(check_s=0.15)
    await relayed(clock_gate, 4, MAX_AGE_S / 3)
    assert clock_gate.verification is not None

    await relayed(clock_gate, 8, MAX_AGE_S / 3, skew_s=TOLERANCE_S + 60)

    assert clock_gate.verification is None
    assert len(caplog.records) == 1


def test_no_sample_has_no_age():
    assert gate().sample_age_s() is None


class CountingClock:
    """The broker clock, counting the samples the gate measures."""

    def __init__(self):
        self.measured = 0

    def offset_at(self, utc_epoch):
        return CLOCK.offset_at(utc_epoch)

    def to_utc_near(self, broker_epoch, utc_epoch):
        self.measured += 1
        return CLOCK.to_utc_near(broker_epoch, utc_epoch)


async def test_a_contradicted_clock_is_measured_again_on_a_new_sample_alone():
    clock = CountingClock()
    clock_gate = ClockGate(clock, max_age_s=MAX_AGE_S, check_s=300.0, bootstrap_s=120.0)

    for _ in range(6):
        clock_gate.write(sample(skew_s=TOLERANCE_S + 60))
        await asyncio.sleep(MAX_AGE_S / 3)

    assert clock_gate.verification is None
    # Six samples, the run maturing during the fourth: at most one measurement each.
    assert clock.measured <= 6
