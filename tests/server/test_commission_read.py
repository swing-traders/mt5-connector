"""The shim's read of the commission schedules the server relays, through waitress and the real
route."""

import dataclasses
import json

import pytest

from mt5connect.errors import ServerUnreachable
from mt5server.app.commissions import CommissionRule, CommissionSchedule, CommissionTier

SCHEDULE = CommissionSchedule(
    ret=1,
    last_error=0,
    rules=(
        CommissionRule(
            currency="UST",
            mode_range=0,
            mode_charge=2,
            mode_entry=1,
            mode_direction=0,
            mode_profit=0,
            tiers=(
                CommissionTier(
                    mode=0,
                    volume_type=1,
                    value=6.0,
                    min_value=0.0,
                    max_value=0.0,
                    range_from=0.0,
                    range_to=1000.0,
                    currency="UST",
                ),
            ),
        ),
    ),
)


def test_a_relayed_schedule_is_read_as_the_server_keeps_it(remote, commissions):
    commissions.write("EURUSD+", SCHEDULE)
    as_served = json.loads(json.dumps(dataclasses.asdict(SCHEDULE)))
    assert remote.commission_schedule("EURUSD+") == as_served


def test_a_symbol_with_no_relayed_schedule_reads_as_none(remote):
    assert remote.commission_schedule("EURUSD.a") is None


def test_a_read_while_the_clock_is_not_verified_raises_server_unreachable(remote, clock_status):
    clock_status.clear()
    with pytest.raises(ServerUnreachable):
        remote.commission_schedule("EURUSD+")
