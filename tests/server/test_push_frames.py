"""The EA's tick, bar and trade-transaction frames as the hub passes them on: the struct fields
verbatim, the subscription each reaches, and every epoch in true UTC."""

import pytest
from mirror_samples import BROKER_EPOCH, CLOCK, UTC_EPOCH

from mt5connector.server.push_frames import FrameError, published
from mt5connector.server.wire import mirror
from mt5connector.server.wire.history_wire import Series
from mt5connector.server.wire.push_wire import (
    RESULT_FIELDS,
    TRANSACTION_FIELDS,
    Stream,
    Subscription,
)

# 2025-11-02 01:30 in New York happens twice; the broker's clock shows 08:30 both times.
REPEATED_BROKER_EPOCH = 1_762_072_200

TICK = {
    "v": 1,
    "type": "tick",
    "symbol": "EURUSD",
    "time": BROKER_EPOCH,
    "bid": 1.085,
    "ask": 1.08512,
    "last": 0.0,
    "volume": 0,
    "time_msc": BROKER_EPOCH * 1000 + 123,
    "flags": 6,
    "volume_real": 0.0,
}
BAR = {
    "v": 1,
    "type": "bar",
    "symbol": "EURUSD",
    "timeframe": mirror.TIMEFRAME_M1,
    "time": BROKER_EPOCH,
    "open": 1.085,
    "high": 1.0852,
    "low": 1.0849,
    "close": 1.0851,
    "tick_volume": 42,
    "spread": 12,
    "real_volume": 0,
}


def transaction_frame(**transaction):
    request = {name: 0 for name in mirror.STRUCTS[mirror.StructName.TRADE_REQUEST].fields}
    request |= {"symbol": "EURUSD", "comment": "", "expiration": BROKER_EPOCH}
    result = {name: 0 for name in RESULT_FIELDS} | {"comment": ""}
    fields = {name: 0 for name in TRANSACTION_FIELDS} | {"symbol": "EURUSD"}
    return {
        "v": 1,
        "type": "trade_transaction",
        "transaction": fields | transaction,
        "request": request,
        "result": result,
    }


def test_a_ticks_epochs_reach_its_subscribers_in_true_utc():
    passed = published(TICK, CLOCK)

    assert passed.subscription == Subscription(Stream.TICKS, "EURUSD")
    assert passed.frame == TICK | {"time": UTC_EPOCH, "time_msc": UTC_EPOCH * 1000 + 123}
    assert passed.ambiguous is None


def test_a_tick_at_broker_time_msc_1_752_580_800_000_reaches_them_at_1_752_570_000_000():
    passed = published(TICK | {"time_msc": 1_752_580_800_000}, CLOCK)

    assert passed.frame["time_msc"] == 1_752_570_000_000


def test_a_bars_open_reaches_its_subscribers_in_true_utc_under_its_series_name():
    passed = published(BAR, CLOCK)

    assert passed.subscription == Subscription(Stream.BARS, "EURUSD", Series.M1)
    assert passed.frame == BAR | {"timeframe": "M1", "time": UTC_EPOCH}


def test_a_transactions_expirations_reach_its_subscribers_in_true_utc_and_zero_stays_zero():
    frame = transaction_frame(time_expiration=BROKER_EPOCH, deal=7001, type=6)
    passed = published(frame, CLOCK)

    assert passed.subscription == Subscription(Stream.TRADE_TRANSACTIONS)
    assert passed.frame["transaction"] == frame["transaction"] | {"time_expiration": UTC_EPOCH}
    assert passed.frame["request"] == frame["request"] | {"expiration": UTC_EPOCH}
    assert passed.frame["result"] == frame["result"]
    unexpiring = published(transaction_frame(), CLOCK)
    assert unexpiring.frame["transaction"]["time_expiration"] == 0


def test_an_epoch_in_the_brokers_repeated_hour_is_read_as_its_first_occurrence_and_named():
    passed = published(TICK | {"time": REPEATED_BROKER_EPOCH}, CLOCK)

    assert passed.frame["time"] == REPEATED_BROKER_EPOCH - 10_800
    assert passed.ambiguous == ("tick.time", REPEATED_BROKER_EPOCH)


@pytest.mark.parametrize(
    ("frame", "message"),
    [
        (TICK | {"extra": 1}, "tick: unknown field: extra"),
        ({k: v for k, v in TICK.items() if k != "flags"}, "tick: missing field: flags"),
        (TICK | {"time_msc": 1.5}, "tick: time_msc 1.5 is not an integer epoch"),
        (TICK | {"symbol": ""}, "tick: not a symbol: ''"),
        (BAR | {"timeframe": mirror.TIMEFRAME_MN1}, "bar: timeframe 49153 has no series"),
        (BAR | {"time": "1752580800"}, "bar: time '1752580800' is not an integer epoch"),
        (
            transaction_frame() | {"result": {}},
            "trade_transaction: result: missing field: " + ", ".join(RESULT_FIELDS),
        ),
        ({"v": 1, "type": "server_time"}, "server_time is not a published frame"),
    ],
    ids=[
        "tick-extra",
        "tick-missing",
        "tick-float-epoch",
        "tick-empty-symbol",
        "bar-month",
        "bar-string-epoch",
        "transaction-empty-result",
        "server-time",
    ],
)
def test_a_frame_out_of_its_kinds_shape_is_refused_naming_what_is_wrong(frame, message):
    with pytest.raises(FrameError) as refused:
        published(frame, CLOCK)

    assert str(refused.value) == message
