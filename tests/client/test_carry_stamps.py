"""The generic carry metadata decoded from the terminal's swap facts."""

import json
from decimal import Decimal
from pathlib import Path

import pytest
from nautilus_trader.model.identifiers import Venue
from venue_doubles import account_info, relayed_sessions, symbol_info

from mt5connector.client.connection import AccountSnapshot
from mt5connector.client.errors import MT5InstrumentError
from mt5connector.client.parsing import parse_symbol_info
from mt5connector.client.sessions import parse_sessions
from mt5connector.wire import calls, mirror


@pytest.mark.parametrize(
    ("mode", "basis", "currency"),
    [
        (0, "DISABLED", "CAD"),
        (1, "POINTS", "CAD"),
        (2, "BASE_CURRENCY", "AUD"),
        (3, "MARGIN_CURRENCY", "EUR"),
        (4, "DEPOSIT_CURRENCY", "USD"),
        (5, "INTEREST_CURRENT", "CAD"),
        (6, "INTEREST_OPEN", "CAD"),
        (7, "REOPEN_CURRENT", "CAD"),
        (8, "REOPEN_BID", "CAD"),
        (9, "PROFIT_CURRENCY", "CAD"),
    ],
)
def test_each_swap_mode_stamps_its_basis_currency_rates_and_weekdays(mode, basis, currency):
    info = symbol_info(
        currency_base="AUD",
        currency_profit="CAD",
        currency_margin="EUR",
        trade_tick_size=0.00005,
        point=0.00001,
        swap_mode=mode,
        swap_long=4.0,
        swap_short=-7.2,
        swap_sunday=0.0,
        swap_monday=1.0,
        swap_tuesday=1.0,
        swap_wednesday=3.0,
        swap_thursday=1.0,
        swap_friday=1.0,
        swap_saturday=0.0,
    )

    instrument = parse_symbol_info(
        info,
        Venue("MT5_TEST"),
        AccountSnapshot.from_mt5(account_info(currency="USD")),
        Decimal(0),
        {},
        parse_sessions(relayed_sessions()),
        0,
    )

    assert instrument.info["point"] == "0.00001"
    assert instrument.info["carry"] == [
        {
            "kind": "SWAP",
            "basis": basis,
            "rates": {"long": "4", "short": "-7.2"},
            "currency": currency,
            "schedule": {
                "tz": "America/New_York",
                "times": ["17:00"],
                "weekday_ratios": {
                    "MONDAY": "1",
                    "TUESDAY": "1",
                    "WEDNESDAY": "3",
                    "THURSDAY": "1",
                    "FRIDAY": "1",
                    "SATURDAY": "0",
                    "SUNDAY": "0",
                },
            },
            "settlement": "AT_CLOSE",
        }
    ]


def test_a_whole_point_and_seven_distinct_weekday_ratios_keep_their_values():
    info = symbol_info(
        point=1.0,
        swap_mode=1,
        swap_sunday=0.0,
        swap_monday=1.0,
        swap_tuesday=2.0,
        swap_wednesday=3.0,
        swap_thursday=4.0,
        swap_friday=5.0,
        swap_saturday=6.0,
    )

    instrument = parse_symbol_info(
        info,
        Venue("MT5_TEST"),
        AccountSnapshot.from_mt5(account_info()),
        Decimal(0),
        {},
        parse_sessions(relayed_sessions()),
        0,
    )

    assert instrument.info["point"] == "1"
    # Sun 0.0 -> "0", Mon 1.0 -> "1", Tue 2.0 -> "2", Wed 3.0 -> "3",
    # Thu 4.0 -> "4", Fri 5.0 -> "5", Sat 6.0 -> "6": only the decimal suffix is removed.
    assert instrument.info["carry"][0]["schedule"]["weekday_ratios"] == {
        "MONDAY": "1",
        "TUESDAY": "2",
        "WEDNESDAY": "3",
        "THURSDAY": "4",
        "FRIDAY": "5",
        "SATURDAY": "6",
        "SUNDAY": "0",
    }


def test_a_disabled_symbol_keeps_its_profit_currency_and_zero_rates():
    instrument = parse_symbol_info(
        symbol_info(currency_profit="CAD", swap_mode=0, swap_long=0, swap_short=0),
        Venue("MT5_TEST"),
        AccountSnapshot.from_mt5(account_info()),
        Decimal(0),
        {},
        parse_sessions(relayed_sessions()),
        0,
    )

    rule = instrument.info["carry"][0]
    assert (rule["basis"], rule["currency"], rule["rates"]) == (
        "DISABLED",
        "CAD",
        {"long": "0", "short": "0"},
    )


def test_an_unknown_swap_mode_refuses_the_definition():
    with pytest.raises(MT5InstrumentError, match="swap_mode"):
        parse_symbol_info(
            symbol_info(swap_mode=10),
            Venue("MT5_TEST"),
            AccountSnapshot.from_mt5(account_info()),
            Decimal(0),
            {},
            parse_sessions(relayed_sessions()),
            0,
        )


def test_the_profit_currency_mode_has_the_terminal_value():
    assert mirror.SYMBOL_SWAP_MODE_CURRENCY_PROFIT == 9


def test_a_captured_symbol_listing_and_account_decode_to_the_generic_carry_contract():
    captured = json.loads(
        (Path(__file__).parent / "fixtures" / "mt5_definition_0_18_0.json").read_text()
    )
    listing_type = calls.STRUCT_TYPES[calls.CALLS[calls.Call.SYMBOLS_GET].layout.name]
    info = listing_type(**captured["symbols_get"][0])
    account = AccountSnapshot.from_mt5(account_info(**captured["account_info"]))

    instrument = parse_symbol_info(
        info,
        Venue("MT5_TEST"),
        account,
        Decimal(0),
        {},
        parse_sessions(relayed_sessions()),
        0,
    )

    assert {
        "carry": instrument.info["carry"],
        "point": instrument.info["point"],
    } == captured["info"]
