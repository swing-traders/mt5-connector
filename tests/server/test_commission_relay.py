"""The commission relay route: an EA's commissions frame, posted by the hub over loopback under the
EA's symbol, kept per symbol with every enum field by its MQL5 name, or its refusal kept instead."""

import pytest

from mt5connector.server.commissions import (
    CommissionRule,
    CommissionSchedule,
    CommissionTier,
    RelayRefusal,
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


def test_a_frame_relayed_over_loopback_is_kept_under_its_symbol(client, commissions):
    response = client.post("/relay/commissions/EURUSD.a", json=FRAME)

    assert (response.status_code, response.json) == (200, {"ok": True, "result": None})
    assert commissions.read("EURUSD.a") == CommissionSchedule(
        ret=1,
        last_error=0,
        rules=(
            CommissionRule(
                currency="USD",
                mode_range="SYMBOL_COMMISSION_RANGE_VOLUME",
                mode_charge="SYMBOL_COMMISSION_CHARGE_INSTANT",
                mode_entry="SYMBOL_COMMISSION_ENTRY_INOUT",
                mode_direction="SYMBOL_COMMISSION_DIRECTION_BOTH",
                mode_profit="SYMBOL_COMMISSION_PROFIT_ALL",
                tiers=(
                    CommissionTier(
                        mode="SYMBOL_COMMISSION_MONEY_DEPOSIT",
                        volume_type="SYMBOL_COMMISSION_VOLUME_TYPE_VOLUME",
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
        (FRAME | {"rules": [RULE | {"mode_entry": 0}]}, "rules[0].mode_entry is not a name: 0"),
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
            "rules[0].tiers[0].mode is not a name: 0",
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
