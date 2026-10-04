"""The precision a venue currency code is built at: NT's own table, else the account's digits for
the account currency, else its ISO 4217 minor units, else refused by name."""

import pytest
from nautilus_trader.model.enums import CurrencyType
from nautilus_trader.model.objects import Currency
from venue_doubles import account_info

from mt5connector.client import currencies
from mt5connector.client.connection import AccountSnapshot
from mt5connector.client.errors import MT5InstrumentError


def _account(currency="USD", currency_digits=2) -> AccountSnapshot:
    return AccountSnapshot.from_mt5(
        account_info(currency=currency, currency_digits=currency_digits)
    )


def test_a_code_in_nts_table_keeps_nts_precision_even_as_the_account_currency():
    assert currencies.venue_currency("USD", _account("USD", currency_digits=3)).precision == 2


def test_the_account_currency_outside_nts_table_takes_the_accounts_digits():
    assert currencies.venue_currency("UST", _account("UST", currency_digits=2)).precision == 2


def test_a_pre_minted_code_is_built_by_the_ladder_not_the_registry():
    Currency.register(Currency("UST", 8, 0, "UST", CurrencyType.CRYPTO), overwrite=True)
    assert currencies.venue_currency("UST", _account("UST", currency_digits=2)).precision == 2


def test_a_code_nt_ships_beyond_its_python_module_keeps_nts_precision():
    assert currencies.venue_currency("XPT", _account()).precision == 2


def test_a_shipped_code_and_a_pre_minted_one_resolve_together():
    Currency.register(Currency("UST", 8, 0, "UST", CurrencyType.CRYPTO), overwrite=True)
    account = _account("UST", currency_digits=2)
    assert currencies.venue_currency("XPT", account).precision == 2
    assert currencies.venue_currency("UST", account).precision == 2


@pytest.mark.parametrize(("code", "minor_units"), [("CLP", 0), ("KWD", 3), ("ISK", 0)])
def test_an_iso_fiat_outside_nts_table_takes_its_minor_units(code, minor_units):
    assert currencies.venue_currency(code, _account()).precision == minor_units


def test_a_code_neither_in_nts_table_nor_iso_nor_the_accounts_is_refused_naming_it():
    with pytest.raises(MT5InstrumentError, match="XYZ"):
        currencies.venue_currency("XYZ", _account())


def test_building_a_currency_registers_nothing():
    currencies.venue_currency("TND", _account())
    assert Currency.from_internal_map("TND") is None
