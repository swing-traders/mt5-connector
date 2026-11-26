import json
from datetime import timedelta
from zoneinfo import ZoneInfo

import prepare_live
import pytest

from mt5connector.wire.broker_clock import BrokerClock


@pytest.fixture
def native_capture(tmp_path):
    captured = tmp_path / "raw-live"
    frame = tmp_path / "raw-capped"
    captured.mkdir()
    frame.mkdir()
    (captured / "outcome.txt").write_text(
        "done\nterminal_build 6316\nm1_first 100\nmaxbars 100000000\n"
        "first_tick 1795132800\nwindow_five-days 1795132860 1795564859\n"
        "five-days 10000 7200\nfirst_tick 1795132800\n"
        "window_minute-1424 1795357440 1795357499\nminute-1424 20 1\n"
    )
    (frame / "outcome.txt").write_text(
        "done\nterminal_build 6316\nm1_first 1795317000\nmaxbars 5000\n"
    )
    return captured, frame


@pytest.mark.parametrize(("timezone", "offset"), [("America/New_York", 7), ("Europe/London", 2)])
def test_the_recorded_windows_and_cap_come_from_native_outcomes(native_capture, timezone, offset):
    captured, frame = native_capture
    clock = BrokerClock(ZoneInfo(timezone), timedelta(hours=offset))
    record = prepare_live.provenance(captured.parent, captured, frame, clock)
    assert record["windows"]["five-days"] == {
        "from_broker": 1795132860,
        "through_broker": 1795564859,
        "start_utc": clock.to_utc(1795132860),
        "end_utc_exclusive": clock.to_utc(1795564860),
        "ticks": 10000,
        "bars": 7200,
    }
    assert record["windows"]["minute-1424"]["bars"] == 1
    assert record["cap"] == {
        "requested": 5000,
        "native": 5000,
        "m1_first_broker": 1795317000,
    }
    assert record["captures"]["raw-live"]["outcome.txt"] == prepare_live.native.sha256(
        captured / "outcome.txt"
    )
    assert json.loads(json.dumps(record)) == record
    assert record["clock"] == {"timezone": timezone, "offset_hours": offset}


@pytest.mark.parametrize(
    "outcome",
    [
        "done\nterminal_build 6316\nm1_first 1795317000\nmaxbars 100000\n",
        "done\nterminal_build 6316\nm1_first 1795132860\nmaxbars 5000\n",
    ],
    ids=["ineffective-cap", "insufficient-gap"],
)
def test_an_unusable_native_cap_fails_before_candidate_tests(native_capture, outcome):
    captured, frame = native_capture
    (frame / "outcome.txt").write_text(outcome)
    with pytest.raises(prepare_live.native.Unprepared, match="interior floor-gap"):
        prepare_live.provenance(
            captured.parent,
            captured,
            frame,
            BrokerClock(ZoneInfo("America/New_York"), timedelta(hours=7)),
        )
