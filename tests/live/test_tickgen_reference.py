"""The worked native doji retains a direction-sensitive path."""

from test_tickgen_history import (
    CLOCK,
    WEEKS,
    WORKED_MINUTE,
    Read,
    load_export,
)
from test_tickgen_history import (
    test_the_worked_rising_doji_minute_emits_its_registered_path as check_worked,
)

from functional.history import materialize


def test_registered_worked_doji_is_rising_and_matches_native_ticks(tmp_path):
    materialize(tmp_path)
    export = load_export(tmp_path, WEEKS[0])
    read = Read(
        [],
        1,
        {
            CLOCK.to_utc(WORKED_MINUTE): [
                (CLOCK.to_utc(stamp // 1000), CLOCK.to_utc_msc(stamp), bid / 100)
                for stamp, bid in export.ticks[WORKED_MINUTE]
            ]
        },
    )
    check_worked([(export, read)])
