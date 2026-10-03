"""The currency a venue code names, built at the precision NT, the account or ISO 4217 states for
it, and its registration with NT.

State: what NT held for each code before this module first registered it, None where it held
nothing; kept for the process's life and written by a registration alone."""

from __future__ import annotations

from typing import TYPE_CHECKING

from nautilus_trader.core import nautilus_pyo3
from nautilus_trader.model.currencies import register_currency
from nautilus_trader.model.enums import CurrencyType
from nautilus_trader.model.objects import Currency

from mt5connect.errors import MT5InstrumentError

if TYPE_CHECKING:
    from mt5connect.connection import AccountSnapshot

# NT's maps hold this module's registrations afterwards, and NT's reconciliation registers too.
_NT_BEFORE_REGISTRATION: dict[str, Currency | None] = {}

# ISO 4217 List One (SIX Group, published 2026-09-17): code → minor units, fund codes excluded.
ISO_4217_MINOR_UNITS: dict[str, int] = {
    "AED": 2,
    "AFN": 2,
    "ALL": 2,
    "AMD": 2,
    "AOA": 2,
    "ARS": 2,
    "AUD": 2,
    "AWG": 2,
    "AZN": 2,
    "BAM": 2,
    "BBD": 2,
    "BDT": 2,
    "BHD": 3,
    "BIF": 0,
    "BMD": 2,
    "BND": 2,
    "BOB": 2,
    "BRL": 2,
    "BSD": 2,
    "BTN": 2,
    "BWP": 2,
    "BYN": 2,
    "BZD": 2,
    "CAD": 2,
    "CDF": 2,
    "CHF": 2,
    "CLP": 0,
    "CNY": 2,
    "COP": 2,
    "CRC": 2,
    "CUP": 2,
    "CVE": 2,
    "CZK": 2,
    "DJF": 0,
    "DKK": 2,
    "DOP": 2,
    "DZD": 2,
    "EGP": 2,
    "ERN": 2,
    "ETB": 2,
    "EUR": 2,
    "FJD": 2,
    "FKP": 2,
    "GBP": 2,
    "GEL": 2,
    "GHS": 2,
    "GIP": 2,
    "GMD": 2,
    "GNF": 0,
    "GTQ": 2,
    "GYD": 2,
    "HKD": 2,
    "HNL": 2,
    "HTG": 2,
    "HUF": 2,
    "IDR": 2,
    "ILS": 2,
    "INR": 2,
    "IQD": 3,
    "IRR": 2,
    "ISK": 0,
    "JMD": 2,
    "JOD": 3,
    "JPY": 0,
    "KES": 2,
    "KGS": 2,
    "KHR": 2,
    "KMF": 0,
    "KPW": 2,
    "KRW": 0,
    "KWD": 3,
    "KYD": 2,
    "KZT": 2,
    "LAK": 2,
    "LBP": 2,
    "LKR": 2,
    "LRD": 2,
    "LSL": 2,
    "LYD": 3,
    "MAD": 2,
    "MDL": 2,
    "MGA": 2,
    "MKD": 2,
    "MMK": 2,
    "MNT": 2,
    "MOP": 2,
    "MRU": 2,
    "MUR": 2,
    "MVR": 2,
    "MWK": 2,
    "MXN": 2,
    "MYR": 2,
    "MZN": 2,
    "NAD": 2,
    "NGN": 2,
    "NIO": 2,
    "NOK": 2,
    "NPR": 2,
    "NZD": 2,
    "OMR": 3,
    "PAB": 2,
    "PEN": 2,
    "PGK": 2,
    "PHP": 2,
    "PKR": 2,
    "PLN": 2,
    "PYG": 0,
    "QAR": 2,
    "RON": 2,
    "RSD": 2,
    "RUB": 2,
    "RWF": 0,
    "SAR": 2,
    "SBD": 2,
    "SCR": 2,
    "SDG": 2,
    "SEK": 2,
    "SGD": 2,
    "SHP": 2,
    "SLE": 2,
    "SOS": 2,
    "SRD": 2,
    "SSP": 2,
    "STN": 2,
    "SVC": 2,
    "SYP": 2,
    "SZL": 2,
    "THB": 2,
    "TJS": 2,
    "TMT": 2,
    "TND": 3,
    "TOP": 2,
    "TRY": 2,
    "TTD": 2,
    "TWD": 2,
    "TZS": 2,
    "UAH": 2,
    "UGX": 0,
    "USD": 2,
    "UYU": 2,
    "UZS": 2,
    "VED": 2,
    "VES": 2,
    "VND": 0,
    "VUV": 0,
    "WST": 2,
    "XAF": 0,
    "XCD": 2,
    "XCG": 2,
    "XOF": 0,
    "XPF": 0,
    "YER": 2,
    "ZAR": 2,
    "ZMW": 2,
    "ZWG": 2,
}


def register_venue_currency(currency: Currency) -> None:
    """Registers a currency in both of NT's maps over what they hold, first remembering what NT held
    for its code so the ladder keeps reading NT's own definition of it."""
    if currency.code not in _NT_BEFORE_REGISTRATION:
        _NT_BEFORE_REGISTRATION[currency.code] = _nt_currency(currency.code)
    register_currency(currency, overwrite=True)


def venue_currency(code: str, account: AccountSnapshot) -> Currency:
    """The currency `code` names, at NT's precision, else at the account's digits when it is the
    account currency, else at its ISO 4217 minor units; registers nothing. Raises
    MT5InstrumentError for a code none of those states a precision for."""
    currency = _by_the_ladder(code, account)
    if currency is None:
        raise MT5InstrumentError(f"currency {code}: no precision is known for it")
    return currency


def base_currency(code: str, account: AccountSnapshot) -> Currency:
    """The currency a base-only code names: by the same ladder where it reaches, else as NT builds a
    code it does not know, at precision 8; registers nothing, and no Money mints in it."""
    currency = _by_the_ladder(code, account)
    if currency is not None:
        return currency
    else:
        return Currency(code, 8, 0, code, CurrencyType.CRYPTO)


def _by_the_ladder(code: str, account: AccountSnapshot) -> Currency | None:
    shipped = _nt_currency(code)
    if shipped is not None:
        return shipped
    elif code == account.currency:
        return Currency(code, account.currency_digits, 0, code, _currency_type(code))
    elif code in ISO_4217_MINOR_UNITS:
        return Currency(code, ISO_4217_MINOR_UNITS[code], 0, code, CurrencyType.FIAT)
    else:
        return None


def _nt_currency(code: str) -> Currency | None:
    """NT's own definition of a code: what it held before this module first registered the code,
    else its PyO3 map's, which never holds the precision-8 guesses NT's non-strict lookup mints."""
    if code in _NT_BEFORE_REGISTRATION:
        return _NT_BEFORE_REGISTRATION[code]
    try:
        currency = nautilus_pyo3.Currency.from_str(code, strict=True)
    except ValueError:
        return None
    return Currency(
        currency.code,
        currency.precision,
        currency.iso4217,
        currency.name,
        CurrencyType[currency.currency_type.name],
    )


def _currency_type(code: str) -> CurrencyType:
    # NT types every code outside its own table as CRYPTO.
    if code in ISO_4217_MINOR_UNITS:
        return CurrencyType.FIAT
    else:
        return CurrencyType.CRYPTO
