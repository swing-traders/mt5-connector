"""The terminal's margin and stop-out facts on instrument definitions."""

import json
from decimal import Decimal
from pathlib import Path

import pytest
from nautilus_trader.model.identifiers import Venue
from venue_doubles import account_info, relayed_sessions, symbol_info, symbol_margin_rates

from mt5connector.client.connection import AccountSnapshot, StopOutMode
from mt5connector.client.errors import MT5ConnectionError
from mt5connector.client.parsing import parse_symbol_info
from mt5connector.client.sessions import parse_sessions
from mt5connector.wire import calls, mirror


@pytest.mark.parametrize("symbol", ["AUDCAD+", "USDCAD+"])
def test_captured_terminal_answers_decode_to_the_margin_account_and_liquidation_contract(symbol):
    captured = json.loads(
        (Path(__file__).parent / "fixtures" / "mt5_definition_0_19_0.json").read_text()
    )
    listing_type = calls.STRUCT_TYPES[calls.CALLS[calls.Call.SYMBOLS_GET].layout.name]
    rates_type = calls.STRUCT_TYPES[mirror.StructName.SYMBOL_MARGIN_RATE]
    info = listing_type(**next(row for row in captured["symbols_get"] if row["name"] == symbol))
    account = AccountSnapshot.from_mt5(account_info(**captured["account_info"]))

    instrument = parse_symbol_info(
        info,
        Venue("MT5_TEST"),
        account,
        Decimal(0),
        {},
        parse_sessions(relayed_sessions()),
        0,
        rates_type(**captured["symbol_margin_rate"][symbol]),
    )

    # 100000 per lot x 0.0005 = 50 in the margin currency; 1 / 0.0005 = 2000.
    assert {
        "margin": instrument.info["margin"],
        "account": instrument.info["account"],
        "liquidation": instrument.info["liquidation"],
    } == captured["info"][symbol]


@pytest.mark.parametrize(
    ("mode", "basis", "divides", "leverage"),
    [
        (0, "CONTRACT", True, "30"),
        (5, "CONTRACT", False, "500"),
        (2, "CONTRACT_VALUE", False, "500"),
        (4, "CONTRACT_VALUE", True, "30"),
        (3, "CONTRACT_VALUE_TICK", False, "500"),
    ],
)
def test_each_calculation_mode_declares_its_margin_basis(mode, basis, divides, leverage):
    info = symbol_info(
        trade_calc_mode=mode,
        margin_hedged=100000.0,
        trade_tick_value=1.0,
        trade_tick_size=0.00001,
    )
    rates = symbol_margin_rates(buy_initial=0.002, sell_initial=0.004)

    instrument = parse_symbol_info(
        info,
        Venue("MT5_TEST"),
        AccountSnapshot.from_mt5(account_info(leverage=30)),
        Decimal(0),
        {},
        parse_sessions(relayed_sessions()),
        0,
        rates,
    )

    expected = {
        "basis": basis,
        "account_leverage_divides": divides,
        "valued_at": "OPEN_PRICE",
        "amount": {"initial": "0", "maintenance": "0", "hedged": "100000"},
        "leverage": leverage,
        "currency": "EUR",
        "rates": {
            "BUY": {"initial": "0.002", "maintenance": "1"},
            "SELL": {"initial": "0.004", "maintenance": "1"},
            "BUY_LIMIT": {"initial": "0", "maintenance": "1"},
            "SELL_LIMIT": {"initial": "0", "maintenance": "1"},
            "BUY_STOP": {"initial": "0", "maintenance": "1"},
            "SELL_STOP": {"initial": "0", "maintenance": "1"},
            "BUY_STOP_LIMIT": {"initial": "0", "maintenance": "1"},
            "SELL_STOP_LIMIT": {"initial": "0", "maintenance": "1"},
        },
        "hedging": "SUM_SIDES_COVERED_AT_HEDGED_AMOUNT",
        "fee_terms": "NONE",
        "untriggered_reserve": "NONE",
        "trigger_check": "FREE_MARGIN",
    }
    if mode == 3:
        expected.update({"tick_value": "1", "tick_size": "0.00001"})
    assert instrument.info["margin"] == expected


@pytest.mark.parametrize(
    ("rate", "leverage"), [(0.03, "33.33333333333333333333333333"), (2, "0.5")]
)
def test_fixed_margin_keeps_amounts_larger_side_and_the_rate_reciprocal(rate, leverage):
    info = symbol_info(
        trade_calc_mode=mirror.SYMBOL_CALC_MODE_FOREX_NO_LEVERAGE,
        margin_initial=100000.0,
        margin_maintenance=50000.0,
        margin_hedged=25000.0,
        margin_hedged_use_leg=True,
    )

    instrument = parse_symbol_info(
        info,
        Venue("MT5_TEST"),
        AccountSnapshot.from_mt5(account_info()),
        Decimal(0),
        {},
        parse_sessions(relayed_sessions()),
        0,
        symbol_margin_rates(buy_initial=rate),
    )

    assert instrument.info["margin"]["basis"] == "FIXED_AMOUNT"
    assert instrument.info["margin"]["amount"] == {
        "initial": "100000",
        "maintenance": "50000",
        "hedged": "25000",
    }
    assert instrument.info["margin"]["hedging"] == "LARGER_SIDE"
    # 1 / 0.03 repeats at 28 digits; 1 / 2 terminates exactly.
    assert instrument.info["margin"]["leverage"] == leverage


@pytest.mark.parametrize(("raw", "mode"), [(0, StopOutMode.PERCENT), (1, StopOutMode.MONEY)])
def test_the_account_snapshot_and_stamp_keep_the_terminal_stop_out_settings(raw, mode):
    account = AccountSnapshot.from_mt5(
        account_info(margin_so_mode=raw, margin_so_call=100.5, margin_so_so=50.25)
    )

    instrument = parse_symbol_info(
        symbol_info(),
        Venue("MT5_TEST"),
        account,
        Decimal(0),
        {},
        parse_sessions(relayed_sessions()),
        0,
        symbol_margin_rates(),
    )

    assert account.stop_out_mode is mode
    assert account.margin_call_level == Decimal("100.5")
    assert account.stop_out_level == Decimal("50.25")
    assert instrument.info["account"] == {
        "leverage": "500",
        "margin_mode": "RETAIL_HEDGING",
        "stop_out_mode": mode.value,
        "margin_call_level": "100.5",
        "stop_out_level": "50.25",
        "currency": "USD",
    }
    assert instrument.info["liquidation"] == {"rule": "STOP_OUT_LEVEL"}


def test_an_unknown_stop_out_mode_refuses_the_account_snapshot():
    with pytest.raises(MT5ConnectionError):
        AccountSnapshot.from_mt5(account_info(margin_so_mode=2))
