"""The sessions relay route and its read: an EA's sessions frame, posted by the hub over loopback
under the EA's symbol, kept per symbol and served back as relayed, or its refusal kept instead."""

import pytest

from mt5connector.server.commissions import RelayRefusal
from mt5connector.server.sessions import TradingSessions

# Sunday first: a session from 18:00 Sunday to 16:57 Friday, broken from 16:59 to 18:00 each day.
QUOTE = [
    [[64800, 0]],
    [[0, 61140], [64800, 0]],
    [[0, 61140], [64800, 0]],
    [[0, 61140], [64800, 0]],
    [[0, 61140], [64800, 0]],
    [[0, 61020]],
    [],
]
# Trading opens five minutes after quoting and closes two minutes sooner on Friday.
TRADE = [
    [[65100, 0]],
    [[0, 61140], [65100, 0]],
    [[0, 61140], [65100, 0]],
    [[0, 61140], [65100, 0]],
    [[0, 61140], [65100, 0]],
    [[0, 60900]],
    [],
]
FRAME = {
    "symbol": "XAUUSD.a",
    "quote": QUOTE,
    "trade": TRADE,
    "last_error": 4307,
}


def test_a_relayed_frame_is_served_back_as_relayed(client, sessions):
    relayed = client.post("/relay/sessions/XAUUSD.a", json=FRAME)
    read = client.get("/sessions/XAUUSD.a")

    assert (relayed.status_code, relayed.json) == (200, {"ok": True, "result": None})
    assert (read.status_code, read.json) == (
        200,
        {"ok": True, "result": {"quote": QUOTE, "trade": TRADE, "last_error": 4307}},
    )
    assert isinstance(sessions.read("XAUUSD.a"), TradingSessions)


def test_a_read_before_any_frame_answers_the_retry_envelope(client):
    response = client.get("/sessions/XAUUSD.a")

    assert (response.status_code, response.headers["Retry-After"]) == (503, "5")
    assert response.json == {
        "ok": False,
        "error": {"code": -20001, "message": "no trading sessions relayed for XAUUSD.a"},
    }


def test_a_week_of_empty_days_is_kept_as_relayed(client):
    empty = [[] for _ in range(7)]

    client.post("/relay/sessions/XAUUSD.a", json=FRAME | {"quote": empty, "trade": empty})
    read = client.get("/sessions/XAUUSD.a")

    assert read.json["result"] == {"quote": empty, "trade": empty, "last_error": 4307}


def test_a_frame_from_another_host_is_refused(client, sessions):
    response = client.post(
        "/relay/sessions/XAUUSD.a", json=FRAME, environ_base={"REMOTE_ADDR": "10.0.0.5"}
    )

    assert response.status_code == 403
    assert response.json == {
        "ok": False,
        "error": {"code": -1, "message": "10.0.0.5 is not loopback"},
    }
    assert sessions.read("XAUUSD.a") is None


def test_the_relay_answers_while_the_clock_is_not_verified(client, clock_status, sessions):
    clock_status.clear()

    response = client.post("/relay/sessions/XAUUSD.a", json=FRAME)

    assert response.status_code == 200
    assert sessions.read("XAUUSD.a") is not None


def _with_day(week, index, day):
    return week[:index] + [day] + week[index + 1 :]


@pytest.mark.parametrize(
    ("body", "message"),
    [
        ([1], "request body is not a JSON object"),
        ({k: v for k, v in FRAME.items() if k != "trade"}, "missing field: trade"),
        (FRAME | {"ret": 0}, "unknown field: ret"),
        (FRAME | {"symbol": ""}, "not a symbol: ''"),
        (FRAME | {"symbol": "XAGUSD.a"}, "symbol 'XAGUSD.a' is not the relay's 'XAUUSD.a'"),
        (FRAME | {"last_error": 0.0}, "last_error is not an integer: 0.0"),
        (FRAME | {"quote": {}}, "quote is not a list: {}"),
        (FRAME | {"quote": QUOTE[:6]}, "quote holds 6 days, not 7"),
        (FRAME | {"trade": TRADE + [[]]}, "trade holds 8 days, not 7"),
        (FRAME | {"trade": _with_day(TRADE, 6, None)}, "trade[6] is not a list: None"),
        (
            FRAME | {"quote": _with_day(QUOTE, 1, [[0, 61140], [-1, 0]])},
            "quote[1][1] holds a negative second: [-1, 0]",
        ),
        (
            FRAME | {"trade": _with_day(TRADE, 5, [[0, -60900]])},
            "trade[5][0] holds a negative second: [0, -60900]",
        ),
        (
            FRAME | {"quote": _with_day(QUOTE, 5, [[0, 61020.0]])},
            "quote[5][0] is not two integers: [0, 61020.0]",
        ),
        (
            FRAME | {"quote": _with_day(QUOTE, 5, [[0, "61020"]])},
            "quote[5][0] is not two integers: [0, '61020']",
        ),
        (
            FRAME | {"quote": _with_day(QUOTE, 5, [[0, True]])},
            "quote[5][0] is not two integers: [0, True]",
        ),
        (
            FRAME | {"quote": _with_day(QUOTE, 5, [[0, 61020, 0]])},
            "quote[5][0] is not two integers: [0, 61020, 0]",
        ),
        (
            FRAME | {"trade": _with_day(TRADE, 2, [[61140, 0], [64800, 3600]])},
            "trade[2][1] does not end after it starts: [64800, 3600]",
        ),
        (
            FRAME | {"trade": _with_day(TRADE, 2, [[3600, 3600]])},
            "trade[2][0] does not end after it starts: [3600, 3600]",
        ),
    ],
    ids=[
        "array",
        "missing-trade",
        "unknown-field",
        "empty-symbol",
        "another-symbol",
        "float-last-error",
        "quote-object",
        "six-days",
        "eight-days",
        "null-day",
        "negative-from",
        "negative-to",
        "float-second",
        "string-second",
        "bool-second",
        "three-seconds",
        "from-after-to",
        "from-at-to",
    ],
)
def test_a_body_out_of_the_frames_shape_is_refused_and_kept_as_refused_under_the_relays_symbol(
    client, sessions, body, message
):
    response = client.post("/relay/sessions/XAUUSD.a", json=body)

    assert response.status_code == 400
    assert response.json == {"ok": False, "error": {"code": -2, "message": message}}
    assert sessions.read("XAUUSD.a") == RelayRefusal(message)


def test_a_session_ending_at_0_is_kept_whatever_its_start(client):
    quote = _with_day(QUOTE, 0, [[0, 0], [86399, 0]])

    response = client.post("/relay/sessions/XAUUSD.a", json=FRAME | {"quote": quote})

    assert response.status_code == 200
    assert client.get("/sessions/XAUUSD.a").json["result"]["quote"] == quote


def test_a_refused_relay_answers_the_read_with_its_refusal_until_a_frame_is_accepted(client):
    client.post("/relay/sessions/XAUUSD.a", json=FRAME)
    accepted_first = client.get("/sessions/XAUUSD.a")
    client.post("/relay/sessions/XAUUSD.a", json=FRAME | {"quote": QUOTE[:6]})

    refused = client.get("/sessions/XAUUSD.a")
    client.post("/relay/sessions/XAUUSD.a", json=FRAME | {"last_error": 0})
    accepted = client.get("/sessions/XAUUSD.a")

    message = "the sessions relay for XAUUSD.a was refused: quote holds 6 days, not 7"
    assert accepted_first.status_code == 200
    assert (refused.status_code, refused.json) == (
        422,
        {"ok": False, "error": {"code": -20003, "message": message}},
    )
    assert (accepted.status_code, accepted.json) == (
        200,
        {"ok": True, "result": {"quote": QUOTE, "trade": TRADE, "last_error": 0}},
    )
