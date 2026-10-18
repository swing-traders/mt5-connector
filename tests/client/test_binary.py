"""The hub protocol's codec as the hub and its consumers use it: the header, the plain-old-data
layouts as StructToCharArray packs them, the struct layouts as the MQL5 property-types table types
them, every frame a consumer sends or receives round-tripped, and malformed frames refused naming
their offset."""

import json
import struct

import pytest

from mt5connector.wire import binary, mirror
from mt5connector.wire.binary import CodecError, ErrorKind, FrameKind, Sender
from mt5connector.wire.history_wire import Series
from mt5connector.wire.push_wire import ChartRole, Stream, Subscription

# Each struct's members that travel, in the package's order, with the MQL5 accessor family the
# property-types table reads each by — integer, double, string — and its members declared bool.
TABLE = {
    "AccountInfo": (
        "login trade_mode leverage limit_orders margin_so_mode trade_allowed trade_expert "
        "margin_mode currency_digits fifo_close balance credit profit equity margin margin_free "
        "margin_level margin_so_call margin_so_so margin_initial margin_maintenance assets "
        "liabilities commission_blocked name server currency company ".split(),
        "IIIIIIIIIIDDDDDDDDDDDDDDSSSS",
        ["fifo_close", "trade_allowed", "trade_expert"],
    ),
    "TerminalInfo": (
        "community_account community_connection connected dlls_allowed trade_allowed "
        "email_enabled ftp_enabled notifications_enabled mqid build maxbars codepage ping_last "
        "retransmission company name language path data_path commondata_path".split(),
        "IIIIIIIIIIIIIDSSSSSS",
        [
            "community_account",
            "community_connection",
            "connected",
            "dlls_allowed",
            "email_enabled",
            "ftp_enabled",
            "mqid",
            "notifications_enabled",
            "trade_allowed",
        ],
    ),
    "SymbolInfo": (
        "custom chart_mode select visible session_deals session_buy_orders session_sell_orders "
        "volume volumehigh volumelow time digits spread spread_float ticks_bookdepth "
        "trade_calc_mode trade_mode start_time expiration_time trade_stops_level "
        "trade_freeze_level trade_exemode swap_mode swap_rollover3days margin_hedged_use_leg "
        "expiration_mode filling_mode order_mode order_gtc_mode option_mode option_right bid "
        "bidhigh bidlow ask askhigh asklow last lasthigh lastlow volume_real volumehigh_real "
        "volumelow_real option_strike point trade_tick_value trade_tick_value_profit "
        "trade_tick_value_loss trade_tick_size trade_contract_size trade_accrued_interest "
        "trade_face_value trade_liquidity_rate volume_min volume_max volume_step volume_limit "
        "swap_long swap_short margin_initial margin_maintenance session_volume session_turnover "
        "session_interest session_buy_orders_volume session_sell_orders_volume session_open "
        "session_close session_aw session_price_settlement session_price_limit_min "
        "session_price_limit_max margin_hedged price_change price_volatility price_theoretical "
        "price_greeks_delta price_greeks_theta price_greeks_gamma price_greeks_vega "
        "price_greeks_rho price_greeks_omega price_sensitivity basis category currency_base "
        "currency_profit currency_margin bank description exchange formula isin page path ".split(),
        "IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDSSS"
        "SSSSSSSSS",
        ["custom", "margin_hedged_use_leg", "select", "spread_float", "visible"],
    ),
    "TradePosition": (
        "ticket time time_msc time_update time_update_msc type magic identifier reason volume "
        "price_open sl tp price_current swap profit symbol comment external_id ".split(),
        "IIIIIIIIIDDDDDDDSSS",
        [],
    ),
    "TradeOrder": (
        "ticket time_setup time_setup_msc time_done time_done_msc time_expiration type "
        "type_time type_filling state magic position_id position_by_id reason volume_initial "
        "volume_current price_open sl tp price_current price_stoplimit symbol comment "
        "external_id ".split(),
        "IIIIIIIIIIIIIIDDDDDDDSSS",
        [],
    ),
    "TradeDeal": (
        "ticket order time time_msc type entry magic position_id reason volume price commission "
        "swap profit fee symbol comment external_id ".split(),
        "IIIIIIIIIDDDDDDSSS",
        [],
    ),
    "TradeRequest": (
        "action magic order symbol volume price stoplimit sl tp deviation type type_filling "
        "type_time expiration comment position position_by ".split(),
        "IIISDDDDDIIIIISII",
        [],
    ),
    "OrderSendResult": (
        "retcode deal order volume price bid ask comment request_id retcode_external ".split(),
        "IIIDDDDSII",
        [],
    ),
    "Transaction": (
        "deal order symbol type order_type order_state deal_type time_type time_expiration "
        "price price_trigger price_sl price_tp volume position position_by ".split(),
        "IISIIIIIIDDDDDII",
        [],
    ),
}


_STRUCTS = {
    "AccountInfo": binary.layout(mirror.STRUCTS[mirror.StructName.ACCOUNT_INFO]),
    "TerminalInfo": binary.layout(mirror.STRUCTS[mirror.StructName.TERMINAL_INFO]),
    "SymbolInfo": binary.layout(mirror.STRUCTS[mirror.StructName.SYMBOL_INFO]),
    "TradePosition": binary.layout(mirror.STRUCTS[mirror.StructName.TRADE_POSITION]),
    "TradeOrder": binary.layout(mirror.STRUCTS[mirror.StructName.TRADE_ORDER]),
    "TradeDeal": binary.layout(mirror.STRUCTS[mirror.StructName.TRADE_DEAL]),
    "TradeRequest": binary.TRADE_REQUEST,
    "OrderSendResult": binary.TRADE_RESULT,
    "Transaction": binary.TRADE_TRANSACTION,
}
_ACCESSORS = {
    "I": mirror.Accessor.INTEGER,
    "D": mirror.Accessor.DOUBLE,
    "S": mirror.Accessor.STRING,
}
_CODES = {mirror.Accessor.INTEGER: "q", mirror.Accessor.DOUBLE: "d"}
# A tick as the probe captured it from StructToCharArray on the pinned terminal.
PROBE = struct.Struct("<qdddQqId")
PROBE_TICK = {
    "time": 1_752_580_800,
    "bid": 1.08501,
    "ask": 1.08513,
    "last": 0.0,
    "volume": 0,
    "time_msc": 1_752_580_800_123,
    "flags": 6,
    "volume_real": 0.0,
}


def tick(msc=0):
    return PROBE_TICK | {"time_msc": PROBE_TICK["time_msc"] + msc}


def bar(**fields):
    return {
        "time": 1_752_570_000,
        "open": 1.085,
        "high": 1.0852,
        "low": 1.0849,
        "close": 1.0851,
        "tick_volume": 42,
        "spread": 12,
        "real_volume": 0,
        "time_close": 1_752_570_060,
    } | fields


def zeroed(layout: binary.Layout, **values) -> dict:
    record = {}
    for member in layout.members:
        if member.name not in layout.fields:
            continue
        elif member.declared is None or member.declared is mirror.MqlType.STRING:
            record[member.name] = ""
        elif member.declared is mirror.MqlType.DOUBLE:
            record[member.name] = 0.0
        elif member.declared is mirror.MqlType.BOOL:
            record[member.name] = False
        else:
            record[member.name] = 0
    return record | values


def header(kind, payload=b"", *, version=2, flags=0, correlation=0, count=0, length=None):
    if length is None:
        length = len(payload)
    return binary.HEADER.pack(version, kind, flags, correlation, count, length) + payload


# ── The header and the plain-old-data layouts ─────────────────────────────────


def test_the_header_is_twenty_bytes_and_no_frame_opens_as_json():
    raw = binary.encode(binary.Ack(7))

    assert binary.HEADER.size == 20
    assert raw[0] == binary.VERSION == 2
    assert chr(raw[0]) not in ' \t\r\n{["-0123456789tfn'
    with pytest.raises(ValueError):
        json.loads(raw)


def test_mqltick_and_mqlrates_pack_to_sixty_bytes_in_the_reference_order():
    assert (binary.TICK.size, binary.RATES.size) == (60, 60)
    assert binary.TICK.format == "<qdddQqId"
    assert binary.RATES.format == "<qddddQiQ"
    assert mirror.TICKS.names == (
        "time",
        "bid",
        "ask",
        "last",
        "volume",
        "time_msc",
        "flags",
        "volume_real",
    )


def test_a_captured_sixty_byte_tick_decodes_to_the_values_the_probe_read():
    captured = PROBE.pack(*PROBE_TICK.values())
    symbol = b"EURUSD"
    payload = struct.pack("<H", len(symbol)) + symbol + captured

    decoded = binary.decode(header(FrameKind.TICK, payload, count=1), Sender.HUB)

    assert len(captured) == 60
    assert decoded == binary.Ticks("EURUSD", (PROBE_TICK,))


# ── The struct layouts ────────────────────────────────────────────────────────


@pytest.mark.parametrize("name", list(TABLE))
def test_each_struct_travels_as_the_property_types_table_types_its_members(name):
    fields, codes, bools = TABLE[name]
    layout = _STRUCTS[name]

    typed = [member for member in layout.members if member.declared is not None]
    assert [member.name for member in typed] == fields
    assert [mirror.accessor(member.declared) for member in typed] == [
        _ACCESSORS[code] for code in codes
    ]
    assert sorted(m.name for m in typed if m.declared is mirror.MqlType.BOOL) == bools


# A symbol's definition, in SymbolInfo's order: every member but its market state, which the
# terminal refuses for a symbol outside Market Watch — the sixteen it named first, `visible` to
# `last`, `bank`, and the market and session values the reference describes beside them.
LISTING_FIELDS = (
    "custom chart_mode select digits spread_float ticks_bookdepth trade_calc_mode trade_mode "
    "start_time expiration_time trade_stops_level trade_freeze_level trade_exemode swap_mode "
    "swap_rollover3days margin_hedged_use_leg expiration_mode filling_mode order_mode "
    "order_gtc_mode option_mode option_right option_strike point trade_tick_value "
    "trade_tick_value_profit trade_tick_value_loss trade_tick_size trade_contract_size "
    "trade_accrued_interest trade_face_value trade_liquidity_rate volume_min volume_max "
    "volume_step volume_limit swap_long swap_short margin_initial margin_maintenance "
    "margin_hedged basis category currency_base currency_profit currency_margin description "
    "exchange formula isin name page path".split()
)


def test_a_symbol_listing_carries_a_symbols_name_and_its_definition_in_order():
    listing = binary.SYMBOL_LISTING
    values = zeroed(listing, name="EURUSD.a", digits=5, trade_mode=4, currency_profit="USD")
    writer = binary.Writer()

    binary.write_record(writer, listing, values)
    raw = writer.payload()

    assert (listing.name, listing.key) == ("SymbolListing", "name")
    assert listing.fields == tuple(LISTING_FIELDS)
    assert listing.epochs == {
        "start_time": mirror.EpochUnit.SECONDS,
        "expiration_time": mirror.EpochUnit.SECONDS,
    }
    assert raw[:10] == struct.pack("<H", 8) + b"EURUSD.a"
    assert binary.read_record(binary.Reader(raw), listing) == values


def test_a_symbols_name_travels_as_the_key_ahead_of_its_typed_members():
    layout = _STRUCTS["SymbolInfo"]
    values = zeroed(layout, name="EURUSD.a", digits=5, bid=1.08501, select=True)
    writer = binary.Writer()

    binary.write_record(writer, layout, values)
    raw = writer.payload()

    assert raw[:10] == struct.pack("<H", 8) + b"EURUSD.a"
    assert binary.read_record(binary.Reader(raw), layout) == values
    members = mirror.STRUCTS[mirror.StructName.SYMBOL_INFO].members
    assert layout.fields.index("name") == [member.name for member in members].index("name")


def test_a_struct_packs_each_member_by_its_accessor_in_order():
    fields, codes, _ = TABLE["OrderSendResult"]
    values = zeroed(
        binary.TRADE_RESULT,
        retcode=10008,
        deal=0,
        order=91,
        volume=0.1,
        price=1.085,
        comment="Request executed",
        request_id=7,
        retcode_external=0,
    )
    writer = binary.Writer()

    binary.write_record(writer, binary.TRADE_RESULT, values)

    expected = b""
    for field, code in zip(fields, codes, strict=True):
        if code == "S":
            raw = values[field].encode()
            expected += struct.pack("<H", len(raw)) + raw
        else:
            expected += struct.pack("<" + _CODES[_ACCESSORS[code]], values[field])
    assert writer.payload() == expected


def test_a_bool_member_travels_as_an_int64_and_reads_back_a_bool():
    layout = _STRUCTS["AccountInfo"]
    values = zeroed(layout, trade_allowed=True, login=12345678)
    writer = binary.Writer()

    binary.write_record(writer, layout, values)
    decoded = binary.read_record(binary.Reader(writer.payload()), layout)

    assert decoded["trade_allowed"] is True
    assert decoded == values


def test_a_bool_member_of_any_value_but_zero_or_one_is_refused():
    layout = _STRUCTS["AccountInfo"]
    writer = binary.Writer()
    binary.write_record(writer, layout, zeroed(layout))
    raw = bytearray(writer.payload())
    offset = 5 * 8  # login, trade_mode, leverage, limit_orders, margin_so_mode
    raw[offset : offset + 8] = struct.pack("<q", 2)

    with pytest.raises(CodecError, match=f"offset {offset}: AccountInfo.trade_allowed 2"):
        binary.read_record(binary.Reader(bytes(raw)), layout)


# ── Round trips ───────────────────────────────────────────────────────────────

TRANSACTION = binary.TradeTransaction(
    zeroed(binary.TRADE_TRANSACTION, deal=7001, order=9001, symbol="EURUSD", type=6, price=1.085),
    zeroed(binary.TRADE_REQUEST, symbol="EURUSD", comment="nt-ünï €", magic=2**62),
    zeroed(binary.TRADE_RESULT, retcode=10009, request_id=4_000_000_000),
)


@pytest.mark.parametrize(
    "frame",
    [
        binary.Ack(7),
        binary.Refusal(""),
        binary.Refusal("ünïcødé € and 中文"),
        binary.Ticks("EURUSD", ()),
        binary.Ticks("EURUSD.a", (tick(1), tick(2))),
        binary.Bar("EURUSD", Series.M5, bar()),
        TRANSACTION,
        binary.Answer(2**63, 0, True, 0, 1, b"\x01" + b"\x00" * 7),
        binary.Answer(3, 4, False, 4301, 2, b"rows"),
        binary.Answer(3, 0, True, 0, error=binary.AnswerError(ErrorKind.SYNCING, "s", 5_000)),
        binary.Answer(3, 2, True, 9, error=binary.AnswerError(ErrorKind.EA_GONE, "gone")),
        binary.Busy(5, 90),
    ],
    ids=[
        "ack",
        "refusal-empty",
        "refusal-non-ascii",
        "no-ticks",
        "ticks",
        "bar",
        "transaction",
        "answer-ok",
        "answer-part",
        "answer-deferring",
        "answer-error",
        "busy",
    ],
)
def test_a_frame_the_hub_sends_a_consumer_round_trips(frame):
    assert binary.decode(binary.encode(frame), Sender.HUB) == frame


@pytest.mark.parametrize(
    "frame",
    [
        binary.Subscribe(1, Subscription(Stream.TICKS, "EURUSD")),
        binary.Subscribe(2, Subscription(Stream.BARS, "EURUSD", Series.W1)),
        binary.Unsubscribe(3, Subscription(Stream.TRADE_TRANSACTIONS)),
        binary.Request(9, 12, 1, b"\x00" + b"\x05\x00EURUSD"),
    ],
    ids=["ticks", "bars", "transactions", "request"],
)
def test_a_frame_a_consumer_sends_the_hub_round_trips(frame):
    assert binary.decode(binary.encode(frame), Sender.ADAPTER) == frame


def test_a_consumers_hello_is_read_as_its_first_frame():
    assert binary.decode_hello(binary.encode(binary.AdapterHello())) == binary.AdapterHello()


@pytest.mark.parametrize(
    ("frame", "sender"),
    [
        (binary.Wanted("EURUSD", True, (1, 16385)), Sender.HUB),
        (binary.OpenChart("publisher", "EURUSD"), Sender.HUB),
        (binary.CloseChart(ChartRole.PUBLISHER, "EURUSD"), Sender.HUB),
        (binary.Duplicate("spawner", "EURUSD"), Sender.HUB),
        (binary.Request(1, 1, 0, b""), Sender.HUB),
    ],
    ids=["wanted", "open-chart", "close-chart", "duplicate", "request"],
)
def test_a_frame_the_hub_sends_an_ea_is_no_frame_a_consumer_takes(frame, sender):
    with pytest.raises(CodecError, match="offset 1: "):
        binary.decode(binary.encode(frame), sender)


def test_a_string_past_its_length_prefix_is_refused():
    with pytest.raises(CodecError, match="65536 bytes"):
        binary.encode(binary.Refusal("x" * 65_536))


# ── Malformed frames ──────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("raw", "sender", "message"),
    [
        (b"\x02\x04\x00", Sender.HUB, "offset 0: 3 bytes hold no header"),
        (header(FrameKind.ACK, version=1), Sender.HUB, "offset 0: version 1 is not 2"),
        (header(FrameKind.ACK, length=4), Sender.HUB, "offset 16: a payload of 4 bytes"),
        (header(FrameKind.HELLO), Sender.HUB, "offset 1: a hello frame from the hub"),
        (header(FrameKind.ACK, flags=1), Sender.HUB, "offset 2: flags 0x1 on a ack frame"),
        (header(FrameKind.ACK, count=1), Sender.HUB, "offset 12: a count of 1 on a ack frame"),
        (header(FrameKind.ACK, b"\x00"), Sender.HUB, "offset 20: 1 bytes past the end"),
        (
            header(FrameKind.ERROR, b"\x02\x00\xff\xfe"),
            Sender.HUB,
            "offset 22: message is not UTF-8",
        ),
        (header(FrameKind.ERROR, b"\x09\x00ab"), Sender.HUB, "offset 22: message runs past"),
        (
            header(FrameKind.TICK, b"\x00\x00", count=0),
            Sender.HUB,
            "offset 20: an empty symbol",
        ),
        (
            header(FrameKind.TICK, b"\x01\x00X" + b"\x00" * 59, count=1),
            Sender.HUB,
            "offset 23: ticks runs past the payload",
        ),
        (
            header(FrameKind.ANSWER, b"\x00" * 8 + b"\x08\x00\x00", flags=0),
            Sender.HUB,
            "offset 2: an error answer that is not the last part",
        ),
        (
            header(FrameKind.ANSWER, b"\x00" * 8 + b"\x63\x00\x00", flags=2),
            Sender.HUB,
            "offset 28: unknown error kind 99",
        ),
        (
            header(FrameKind.SUBSCRIBE, b"\x05\x00ticks\x00\x00"),
            Sender.ADAPTER,
            "offset 20: not a subscription",
        ),
    ],
    ids=[
        "short",
        "version-1",
        "length",
        "hello-from-hub",
        "flags",
        "count",
        "trailing",
        "not-utf8",
        "string-past-payload",
        "empty-symbol",
        "short-tick",
        "error-not-last",
        "unknown-error-kind",
        "subscription-without-symbol",
    ],
)
def test_a_malformed_frame_is_refused_naming_its_offset(raw, sender, message):
    with pytest.raises(CodecError, match=f"^{message}"):
        binary.decode(raw, sender)


def test_a_frame_of_an_unknown_kind_is_its_own_refusal():
    with pytest.raises(binary.UnknownKind, match="offset 1: unknown kind 99"):
        binary.decode(header(99), Sender.HUB)


def test_a_first_frame_other_than_a_hello_is_refused():
    with pytest.raises(CodecError, match="offset 1: a frame of kind 2 before the connection"):
        binary.decode_hello(binary.encode(binary.Subscribe(1, Subscription(Stream.TICKS, "X"))))


def test_an_answer_with_a_flag_bit_no_flag_names_is_refused():
    raw = header(FrameKind.ANSWER, b"\x00" * 8, flags=0x8003)

    with pytest.raises(CodecError, match="^offset 2: unknown answer flags 0x8003"):
        binary.decode(raw, Sender.HUB)


@pytest.mark.parametrize(
    ("raw", "kind", "correlation"),
    [
        (b"\x02\x14" + b"\x00" * 8, None, None),
        (header(FrameKind.ANSWER, version=1, correlation=9), None, None),
        (header(FrameKind.ANSWER, b"\x00\x00\x00", flags=3, correlation=9), FrameKind.ANSWER, 9),
        (header(FrameKind.TICK, b"\x01\x00X", count=1), FrameKind.TICK, 0),
        (header(FrameKind.ACK, length=4, correlation=5), FrameKind.ACK, 5),
    ],
    ids=["short", "version-1", "short-answer", "short-tick", "length"],
)
def test_a_codec_error_carries_the_kind_and_correlation_its_header_held(raw, kind, correlation):
    with pytest.raises(CodecError) as failure:
        binary.decode(raw, Sender.HUB)

    assert (failure.value.kind, failure.value.correlation) == (kind, correlation)
