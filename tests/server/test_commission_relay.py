"""The commission relay route: an EA's commissions frame, posted by the hub over loopback under the
EA's symbol, kept per symbol with every enum field by its MQL5 name, or its refusal kept instead."""

import pytest

from mt5connector.server.commissions import (
    CommissionRule,
    CommissionSchedule,
    CommissionTier,
    RelayRefusal,
)
from mt5connector.server.wire.push_wire import (
    CommissionChargeMode,
    CommissionDirectionMode,
    CommissionEntryMode,
    CommissionMode,
    CommissionProfitMode,
    CommissionRangeMode,
    CommissionVolumeType,
)

TIER = {
    "mode": "SYMBOL_COMMISSION_MONEY_DEPOSIT",
    "volume_type": "SYMBOL_COMMISSION_VOLUME_TYPE_VOLUME",
    "value": 3.5,
    "min_value": 0,
    "max_value": 0,
    "range_from": 0,
    "range_to": 1000000,
    "currency": "USD",
}
RULE = {
    "currency": "USD",
    "mode_range": "SYMBOL_COMMISSION_RANGE_VOLUME",
    "mode_charge": "SYMBOL_COMMISSION_CHARGE_INSTANT",
    "mode_entry": "SYMBOL_COMMISSION_ENTRY_INOUT",
    "mode_direction": "SYMBOL_COMMISSION_DIRECTION_BOTH",
    "mode_profit": "SYMBOL_COMMISSION_PROFIT_ALL",
    "tiers": [TIER],
}
FRAME = {
    "v": 1,
    "type": "commissions",
    "symbol": "EURUSD.a",
    "ret": 1,
    "last_error": 0,
    "rules": [RULE],
}

# Every member the MQL5 reference lists for SymbolInfoCommissions' enums, by the name EnumToString
# gives it, under the rule or tier field that carries it.
RULE_MEMBERS = {
    "mode_range": [
        "SYMBOL_COMMISSION_RANGE_VOLUME",
        "SYMBOL_COMMISSION_RANGE_TURNOVER_MONEY",
        "SYMBOL_COMMISSION_RANGE_TURNOVER_VOLUME",
        "SYMBOL_COMMISSION_RANGE_VALUE",
        "SYMBOL_COMMISSION_RANGE_PROFIT",
    ],
    "mode_charge": [
        "SYMBOL_COMMISSION_CHARGE_DAILY",
        "SYMBOL_COMMISSION_CHARGE_MONTHLY",
        "SYMBOL_COMMISSION_CHARGE_INSTANT",
    ],
    "mode_entry": [
        "SYMBOL_COMMISSION_ENTRY_INOUT",
        "SYMBOL_COMMISSION_ENTRY_IN",
        "SYMBOL_COMMISSION_ENTRY_OUT",
    ],
    "mode_direction": [
        "SYMBOL_COMMISSION_DIRECTION_BOTH",
        "SYMBOL_COMMISSION_DIRECTION_BUY",
        "SYMBOL_COMMISSION_DIRECTION_SELL",
    ],
    "mode_profit": [
        "SYMBOL_COMMISSION_PROFIT_ALL",
        "SYMBOL_COMMISSION_PROFIT_PROFIT",
        "SYMBOL_COMMISSION_PROFIT_LOSS",
    ],
}
TIER_MEMBERS = {
    "mode": [
        "SYMBOL_COMMISSION_DISABLED",
        "SYMBOL_COMMISSION_MONEY_DEPOSIT",
        "SYMBOL_COMMISSION_MONEY_SYMBOL_BASE",
        "SYMBOL_COMMISSION_MONEY_SYMBOL_PROFIT",
        "SYMBOL_COMMISSION_MONEY_SYMBOL_MARGIN",
        "SYMBOL_COMMISSION_PIPS",
        "SYMBOL_COMMISSION_PERCENT",
        "SYMBOL_COMMISSION_MONEY_SPECIFIED",
        "SYMBOL_COMMISSION_PERCENT_PROFIT",
    ],
    "volume_type": [
        "SYMBOL_COMMISSION_VOLUME_TYPE_TRADE",
        "SYMBOL_COMMISSION_VOLUME_TYPE_VOLUME",
        "SYMBOL_COMMISSION_VOLUME_TYPE_TURNOVER",
    ],
}
TYPO = "SYMBOL_COMMISSION_TYPO"


def test_a_frame_relayed_over_loopback_is_kept_under_its_symbol(client, commissions):
    response = client.post("/relay/commissions/EURUSD.a", json=FRAME)

    assert (response.status_code, response.json) == (200, {"ok": True, "result": None})
    assert commissions.read("EURUSD.a") == CommissionSchedule(
        ret=1,
        last_error=0,
        rules=(
            CommissionRule(
                currency="USD",
                mode_range=CommissionRangeMode.VOLUME,
                mode_charge=CommissionChargeMode.INSTANT,
                mode_entry=CommissionEntryMode.INOUT,
                mode_direction=CommissionDirectionMode.BOTH,
                mode_profit=CommissionProfitMode.ALL,
                tiers=(
                    CommissionTier(
                        mode=CommissionMode.MONEY_DEPOSIT,
                        volume_type=CommissionVolumeType.VOLUME,
                        value=3.5,
                        min_value=0,
                        max_value=0,
                        range_from=0,
                        range_to=1000000,
                        currency="USD",
                    ),
                ),
            ),
        ),
    )


def test_a_failed_read_and_a_symbol_without_a_rule_are_kept_as_relayed(client, commissions):
    client.post(
        "/relay/commissions/EURUSD.a", json=FRAME | {"ret": -1, "last_error": 4301, "rules": []}
    )
    client.post(
        "/relay/commissions/DE40.a", json=FRAME | {"symbol": "DE40.a", "ret": 0, "rules": []}
    )

    assert commissions.read("EURUSD.a") == CommissionSchedule(ret=-1, last_error=4301, rules=())
    assert commissions.read("DE40.a") == CommissionSchedule(ret=0, last_error=0, rules=())
    response = client.get("/commissions/DE40.a")
    assert (response.status_code, response.json) == (
        200,
        {"ok": True, "result": {"ret": 0, "last_error": 0, "rules": []}},
    )


def test_a_frame_from_another_host_is_refused(client, commissions):
    response = client.post(
        "/relay/commissions/EURUSD.a", json=FRAME, environ_base={"REMOTE_ADDR": "10.0.0.5"}
    )

    assert response.status_code == 403
    assert response.json == {
        "ok": False,
        "error": {"code": -1, "message": "10.0.0.5 is not loopback"},
    }
    assert commissions.read("EURUSD.a") is None


def test_the_relay_answers_while_the_clock_is_not_verified(client, clock_status, commissions):
    clock_status.clear()

    response = client.post("/relay/commissions/EURUSD.a", json=FRAME)

    assert response.status_code == 200
    assert commissions.read("EURUSD.a") is not None


@pytest.mark.parametrize(
    ("body", "message"),
    [
        ([1], "request body is not a JSON object"),
        ({k: v for k, v in FRAME.items() if k != "ret"}, "missing field: ret"),
        (FRAME | {"tick": 1}, "unknown field: tick"),
        (FRAME | {"v": 2}, "unsupported frame version: 2"),
        (FRAME | {"type": "server_time"}, "not a commissions frame: 'server_time'"),
        (FRAME | {"symbol": ""}, "not a symbol: ''"),
        (FRAME | {"symbol": "DE40.a"}, "symbol 'DE40.a' is not the relay's 'EURUSD.a'"),
        (FRAME | {"ret": 1.0}, "ret is not an integer: 1.0"),
        (FRAME | {"last_error": True}, "last_error is not an integer: True"),
        (FRAME | {"rules": {}}, "rules is not a list: {}"),
        (FRAME | {"rules": [RULE | {"mode_entry": 0}]}, "rules[0].mode_entry is unknown: 0"),
        (
            FRAME | {"rules": [{k: v for k, v in RULE.items() if k != "tiers"}]},
            "rules[0]: missing field: tiers",
        ),
        (
            FRAME | {"rules": [RULE | {"tiers": [TIER | {"value": "3.5"}]}]},
            "rules[0].tiers[0].value is not a number: '3.5'",
        ),
        (
            FRAME | {"rules": [RULE | {"tiers": [TIER | {"mode": 0}]}]},
            "rules[0].tiers[0].mode is unknown: 0",
        ),
        (
            FRAME | {"rules": [RULE | {"tiers": [TIER | {"currency": None}]}]},
            "rules[0].tiers[0].currency is not a string: None",
        ),
    ],
    ids=[
        "array",
        "missing-ret",
        "unknown-field",
        "version-2",
        "server-time",
        "empty-symbol",
        "another-symbol",
        "float-ret",
        "bool-last-error",
        "rules-object",
        "integer-entry",
        "rule-without-tiers",
        "string-value",
        "integer-mode",
        "null-currency",
    ],
)
def test_a_body_out_of_the_frames_shape_is_refused_and_kept_as_refused_under_the_relays_symbol(
    client, commissions, body, message
):
    response = client.post("/relay/commissions/EURUSD.a", json=body)

    assert response.status_code == 400
    assert response.json == {"ok": False, "error": {"code": -2, "message": message}}
    assert commissions.read("EURUSD.a") == RelayRefusal(message)


@pytest.mark.parametrize("field", list(RULE_MEMBERS))
def test_a_rule_naming_no_member_of_a_fields_enum_is_refused_whole_naming_the_field(
    client, commissions, field
):
    response = client.post(
        "/relay/commissions/EURUSD.a", json=FRAME | {"rules": [RULE, RULE | {field: TYPO}]}
    )

    message = f"rules[1].{field} is unknown: {TYPO!r}"
    assert (response.status_code, response.json["error"]["message"]) == (400, message)
    assert commissions.read("EURUSD.a") == RelayRefusal(message)


@pytest.mark.parametrize("field", list(TIER_MEMBERS))
def test_a_tier_naming_no_member_of_a_fields_enum_is_refused_whole_naming_the_field(
    client, commissions, field
):
    rule = RULE | {"tiers": [TIER, TIER | {field: TYPO}]}

    response = client.post("/relay/commissions/EURUSD.a", json=FRAME | {"rules": [rule]})

    message = f"rules[0].tiers[1].{field} is unknown: {TYPO!r}"
    assert (response.status_code, response.json["error"]["message"]) == (400, message)
    assert commissions.read("EURUSD.a") == RelayRefusal(message)


def test_every_member_of_the_schedules_enums_is_kept_as_one_and_read_back_by_its_name(
    client, commissions
):
    # A tier per commission mode and a rule per range mode, the other fields cycling their members.
    volume_types = TIER_MEMBERS["volume_type"]
    tiers = []
    for index, mode in enumerate(TIER_MEMBERS["mode"]):
        tiers.append(TIER | {"mode": mode, "volume_type": volume_types[index % len(volume_types)]})
    rules = []
    for index in range(len(RULE_MEMBERS["mode_range"])):
        named = {field: members[index % len(members)] for field, members in RULE_MEMBERS.items()}
        rules.append(RULE | named | {"tiers": tiers})

    relayed = client.post("/relay/commissions/EURUSD.a", json=FRAME | {"rules": rules})
    read = client.get("/commissions/EURUSD.a")

    assert relayed.status_code == 200
    assert read.json["result"]["rules"] == rules
    for rule in commissions.read("EURUSD.a").rules:
        assert isinstance(rule.mode_range, CommissionRangeMode)
        assert isinstance(rule.mode_charge, CommissionChargeMode)
        assert isinstance(rule.mode_entry, CommissionEntryMode)
        assert isinstance(rule.mode_direction, CommissionDirectionMode)
        assert isinstance(rule.mode_profit, CommissionProfitMode)
        for tier in rule.tiers:
            assert isinstance(tier.mode, CommissionMode)
            assert isinstance(tier.volume_type, CommissionVolumeType)


def test_a_refused_relay_answers_the_read_with_its_refusal_until_a_frame_is_accepted(client):
    client.post("/relay/commissions/EURUSD.a", json=FRAME | {"ret": 1.0})

    refused = client.get("/commissions/EURUSD.a")
    client.post("/relay/commissions/EURUSD.a", json=FRAME | {"rules": []})
    accepted = client.get("/commissions/EURUSD.a")

    message = "the commission relay for EURUSD.a was refused: ret is not an integer: 1.0"
    assert (refused.status_code, refused.json) == (
        422,
        {"ok": False, "error": {"code": -20003, "message": message}},
    )
    assert (accepted.status_code, accepted.json) == (
        200,
        {"ok": True, "result": {"ret": 1, "last_error": 0, "rules": []}},
    )
