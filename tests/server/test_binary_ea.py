"""The hub protocol's EA half: each frame an EA sends, laid out as the MQL5 EA packs it, decoded as
the hub reads it, and each frame the hub sends an EA read back as the EA reads it."""

import pytest
from ea_wire import decode_to_ea, encode_ea, frame, zeroed

from mt5connector.server.wire import binary, mirror
from mt5connector.server.wire.binary import CodecError, ErrorKind, FrameKind, Sender
from mt5connector.server.wire.push_wire import (
    ChartRole,
    CommissionEntryMode,
    CommissionMode,
    CommissionRangeMode,
    CommissionVolumeType,
)

TICK = {
    "time": 1_752_580_800,
    "bid": 1.085,
    "ask": 1.08512,
    "last": 0.0,
    "volume": 0,
    "time_msc": 1_752_580_800_123,
    "flags": 6,
    "volume_real": 0.0,
}
RATES = {
    "time": 1_752_580_800,
    "open": 1.085,
    "high": 1.0852,
    "low": 1.0849,
    "close": 1.0851,
    "tick_volume": 42,
    "spread": 12,
    "real_volume": 0,
}
SCHEDULE = {
    "ret": 1,
    "last_error": 0,
    "rules": [
        {
            "currency": "EUR",
            "mode_range": "SYMBOL_COMMISSION_RANGE_VOLUME",
            "mode_charge": "SYMBOL_COMMISSION_CHARGE_INSTANT",
            "mode_entry": "SYMBOL_COMMISSION_ENTRY_IN",
            "mode_direction": "SYMBOL_COMMISSION_DIRECTION_BOTH",
            "mode_profit": "SYMBOL_COMMISSION_PROFIT_ALL",
            "tiers": [
                {
                    "mode": "SYMBOL_COMMISSION_MONEY_DEPOSIT",
                    "volume_type": "SYMBOL_COMMISSION_VOLUME_TYPE_VOLUME",
                    "value": 3.5,
                    "min_value": 0.0,
                    "max_value": 0.0,
                    "range_from": 0.0,
                    "range_to": 0.0,
                    "currency": "EUR",
                }
            ],
        }
    ],
}
WEEK = [[[64_800, 0]]] + [[[0, 61_140], [64_800, 0]]] * 4 + [[[0, 61_020]], []]


@pytest.mark.parametrize(
    "sent",
    [
        binary.EaHello(ChartRole.SPAWNER, "EURUSD"),
        binary.ChartOpened(ChartRole.PUBLISHER, "US500#"),
        binary.ChartFailed(ChartRole.PUBLISHER, "XYZ", 4301),
        binary.ChartKept(ChartRole.PUBLISHER, "USDJPY", 1, 2),
        binary.Ticks("EURUSD", (TICK, TICK | {"bid": 1.08501})),
        binary.RatesBar("EURUSD", mirror.TIMEFRAME_H1, RATES),
        binary.TradeTransaction(
            zeroed(binary.TRADE_TRANSACTION, symbol="EURUSD", type=10, deal=2**63 - 1),
            zeroed(binary.TRADE_REQUEST, symbol="EURUSD", comment="ünï €"),
            zeroed(binary.TRADE_RESULT, retcode=10009, request_id=7),
        ),
        binary.ServerTime("EURUSD", 1_752_580_800, 1_752_580_798, 1_752_570_000, False),
        binary.Commissions("EURUSD", SCHEDULE),
        binary.Commissions("EURUSD", {"ret": 0, "last_error": 4301, "rules": []}),
        binary.Sessions("EURUSD", {"quote": WEEK, "trade": WEEK, "last_error": 0}),
        binary.Answer(11, 0, True, 0, 1, b"\x01"),
        binary.Answer(11, 0, True, 4301, error=binary.AnswerError(ErrorKind.EA_ERROR, "")),
        binary.Answer(11, 0, True, 0, error=binary.AnswerError(ErrorKind.SYNCING, "", 2_000)),
        binary.Answer(11, 0, True, 0, error=binary.AnswerError(ErrorKind.INVALID_WINDOW, "")),
        binary.Busy(11, 90),
    ],
    ids=[
        "hello",
        "chart-opened",
        "chart-failed",
        "chart-kept",
        "ticks",
        "bar",
        "transaction",
        "server-time",
        "commissions",
        "no-commissions",
        "sessions",
        "answer",
        "ea-error",
        "syncing",
        "invalid-window",
        "busy",
    ],
)
def test_each_frame_an_ea_sends_decodes_as_the_ea_packs_it(sent):
    assert binary.decode(encode_ea(sent), Sender.EA) == sent


def test_an_eas_hello_is_read_as_its_first_frame():
    hello = binary.EaHello(ChartRole.PUBLISHER, "EURUSD")

    assert binary.decode_hello(encode_ea(hello)) == hello


@pytest.mark.parametrize(
    "sent",
    [
        binary.Wanted("EURUSD", True, (mirror.TIMEFRAME_M1, mirror.TIMEFRAME_H1)),
        binary.Wanted("EURUSD", False, ()),
        binary.OpenChart(ChartRole.PUBLISHER, "EURUSD"),
        binary.CloseChart(ChartRole.PUBLISHER, "EURUSD"),
        binary.Duplicate(ChartRole.SPAWNER, "EURUSD"),
        binary.Request(5, 13, 2, b"\x00\x06\x00EURUSD\x01\x01\x00\x00\x00\x00\x00\x00\x00"),
        binary.Refusal("a text frame"),
    ],
    ids=["wanted", "nothing-wanted", "open", "close", "duplicate", "request", "refusal"],
)
def test_each_frame_the_hub_sends_an_ea_reads_back_as_the_ea_reads_it(sent):
    assert decode_to_ea(binary.encode(sent)) == sent


@pytest.mark.parametrize(
    "kind", [ErrorKind.NOT_READY, ErrorKind.NO_PUBLISHER, ErrorKind.EA_GONE, ErrorKind.TIMEOUT]
)
def test_an_ea_answering_a_kind_the_hub_alone_answers_is_refused(kind):
    if kind in binary.DEFERRING:
        error = binary.AnswerError(kind, "", 1_000)
    else:
        error = binary.AnswerError(kind, "")

    with pytest.raises(CodecError, match=f"offset 28: an EA answering {kind.name.lower()}"):
        binary.decode(encode_ea(binary.Answer(1, 0, True, 0, error=error)), Sender.EA)


def test_an_eas_overflow_is_refused_too():
    error = binary.AnswerError(ErrorKind.OVERFLOW, "")

    with pytest.raises(CodecError, match="an EA answering overflow"):
        binary.decode(encode_ea(binary.Answer(1, 0, True, 0, error=error)), Sender.EA)


def test_a_commission_schedule_naming_a_member_its_enum_lacks_is_refused_whole():
    rule = SCHEDULE["rules"][0] | {"mode_entry": "SYMBOL_COMMISSION_ENTRY_BOTH"}

    with pytest.raises(CodecError, match="rules\\[0\\].mode_entry 'SYMBOL_COMMISSION_ENTRY_BOTH'"):
        binary.decode(
            encode_ea(binary.Commissions("EURUSD", SCHEDULE | {"rules": [rule]})), Sender.EA
        )


def test_an_eas_bar_is_laid_out_unlike_the_hubs():
    raw = encode_ea(binary.RatesBar("EURUSD", mirror.TIMEFRAME_H1, RATES))

    with pytest.raises(CodecError):
        binary.decode(raw, Sender.HUB)


@pytest.mark.parametrize(
    ("raw", "message"),
    [
        (frame(FrameKind.SUBSCRIBE, b""), "offset 1: a subscribe frame from the ea"),
        (frame(FrameKind.SERVER_TIME, b"\x01\x00X" + b"\x00" * 24 + b"\x02"), "connected 2"),
        (
            frame(FrameKind.HELLO, b"\x02\x00ea\x07\x00manager\x01\x00X"),
            "chart_role 'manager' is not a ChartRole",
        ),
    ],
    ids=["op-from-an-ea", "connected-2", "unknown-chart-role"],
)
def test_a_frame_an_ea_may_not_send_is_refused(raw, message):
    with pytest.raises(CodecError, match=message):
        binary.decode(raw, Sender.EA)


def test_a_commission_schedules_names_are_read_as_their_enum_members():
    decoded = binary.decode(encode_ea(binary.Commissions("EURUSD", SCHEDULE)), Sender.EA)

    (rule,) = decoded.schedule["rules"]
    (tier,) = rule["tiers"]
    assert type(rule["mode_entry"]) is CommissionEntryMode
    assert type(rule["mode_range"]) is CommissionRangeMode
    assert type(tier["mode"]) is CommissionMode
    assert type(tier["volume_type"]) is CommissionVolumeType
    assert decoded.schedule == SCHEDULE
