"""The instrument provider against a hub session double: what it loads and how it builds each
definition, its currencies, the conversion pairs it finds, and the taker fees it derives."""

import asyncio
import itertools
import threading
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from hub_double import eventually
from nautilus_trader.common.component import TestClock
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Venue as NtVenue
from nautilus_trader.model.instruments import Cfd, CurrencyPair
from nautilus_trader.model.objects import Price, Quantity
from venue_doubles import (
    SymbolVenue,
    account_info,
    relayed_sessions,
    serve,
    symbol_info,
)

from mt5connector.client.commissions import parse_schedule
from mt5connector.client.connection import AccountSnapshot, MT5Connection
from mt5connector.client.constants import MT5_VENUE
from mt5connector.client.errors import (
    EaError,
    MT5ConfigError,
    MT5ConnectionError,
    MT5InstrumentError,
    MT5SymbolNotFoundError,
    NoPublisher,
    NotReady,
    Syncing,
)
from mt5connector.client.providers import MT5InstrumentProvider
from mt5connector.wire import calls, mirror
from mt5connector.wire.push_wire import (
    CommissionChargeMode,
    CommissionDirectionMode,
    CommissionEntryMode,
    CommissionMode,
    CommissionProfitMode,
    CommissionRangeMode,
    CommissionVolumeType,
    MqlError,
)

Call = calls.Call

NOW_NS = 1_760_000_000_000_000_000

TRADED_MODES = {
    mirror.SYMBOL_CALC_MODE_FOREX: CurrencyPair,
    mirror.SYMBOL_CALC_MODE_FOREX_NO_LEVERAGE: CurrencyPair,
    mirror.SYMBOL_CALC_MODE_CFD: Cfd,
    mirror.SYMBOL_CALC_MODE_CFDINDEX: Cfd,
    mirror.SYMBOL_CALC_MODE_CFDLEVERAGE: Cfd,
}
REFUSED_MODES = [
    name
    for name, value in mirror.CONSTANTS.items()
    if name.startswith("SYMBOL_CALC_MODE_") and value not in TRADED_MODES
]


def schedule(value, currency, mode=CommissionMode.MONEY_DEPOSIT, entry=CommissionEntryMode.INOUT):
    """One relayed SymbolInfoCommissions answer, as the session reads it: one rule with one volume
    tier."""
    return {
        "ret": 1,
        "last_error": 0,
        "rules": [
            {
                "currency": currency,
                "mode_range": CommissionRangeMode.VOLUME,
                "mode_charge": CommissionChargeMode.INSTANT,
                "mode_entry": entry,
                "mode_direction": CommissionDirectionMode.BOTH,
                "mode_profit": CommissionProfitMode.ALL,
                "tiers": [
                    {
                        "mode": mode,
                        "volume_type": CommissionVolumeType.VOLUME,
                        "value": value,
                        "min_value": 0.0,
                        "max_value": 0.0,
                        "range_from": 0.0,
                        "range_to": 1000000.0,
                        "currency": currency,
                    }
                ],
            }
        ],
    }


@pytest.fixture
def venue():
    return SymbolVenue()


def connection_of(venue: SymbolVenue, **account) -> MT5Connection:
    """A connected connection whose hub session is `venue`, its account under `account`."""
    venue.account = account_info(**account)
    conn = MagicMock(spec=MT5Connection)
    serve(conn, venue)
    conn.account_info.return_value = AccountSnapshot.from_mt5(venue.account)
    return conn


def provider_of(
    venue: SymbolVenue, config: InstrumentProviderConfig, **account
) -> MT5InstrumentProvider:
    conn = connection_of(venue, **account)
    clock = TestClock()
    clock.set_time(NOW_NS)
    return MT5InstrumentProvider(connection=conn, venue=MT5_VENUE, clock=clock, config=config)


def provider_for(venue: SymbolVenue, *symbols, **account):
    """A provider over `venue` whose config names `symbols`."""
    return provider_of(
        venue,
        InstrumentProviderConfig(
            load_ids=frozenset(InstrumentId.from_str(f"{s}.MT5") for s in symbols)
        ),
        **account,
    )


async def loaded(venue, info, **account):
    venue.add(info)
    provider = provider_for(venue, info.name, **account)
    await provider.initialize()
    return provider.find(InstrumentId.from_str(f"{info.name}.MT5"))


# ── Typing ───────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(("calc_mode", "kind"), list(TRADED_MODES.items()))
async def test_the_calc_mode_types_the_instrument(venue, calc_mode, kind):
    instrument = await loaded(venue, symbol_info(trade_calc_mode=calc_mode))
    assert type(instrument) is kind


async def test_a_crypto_cfd_is_a_cfd(venue):
    info = symbol_info(
        name="BTCUSD",
        digits=2,
        point=0.01,
        trade_tick_size=0.01,
        trade_contract_size=1.0,
        currency_base="BTC",
        currency_profit="USD",
        trade_calc_mode=mirror.SYMBOL_CALC_MODE_CFD,
    )
    assert type(await loaded(venue, info)) is Cfd


@pytest.mark.parametrize("calc_mode", REFUSED_MODES)
async def test_a_calc_mode_this_adapter_does_not_trade_fails_the_load_naming_it(venue, calc_mode):
    venue.add(symbol_info(name="ZN", trade_calc_mode=mirror.CONSTANTS[calc_mode]))
    provider = provider_for(venue, "ZN")
    with pytest.raises(MT5InstrumentError) as refused:
        await provider.initialize()
    assert "ZN" in str(refused.value)
    assert calc_mode.removeprefix("SYMBOL_CALC_MODE_") in str(refused.value)


# ── Fields ───────────────────────────────────────────────────────────────────


async def test_a_pair_carries_the_venues_grid_limits_contract_and_currencies(venue):
    instrument = await loaded(
        venue,
        symbol_info(
            name="EURUSD.a",
            digits=5,
            trade_tick_size=0.00001,
            volume_step=0.01,
            volume_min=0.01,
            volume_max=200.0,
            trade_contract_size=100000.0,
            currency_base="EUR",
            currency_profit="USD",
            margin_initial=1000.0,
            margin_maintenance=500.0,
        ),
    )
    assert instrument.price_precision == 5
    assert instrument.price_increment == Price.from_str("0.00001")
    assert instrument.size_increment == Quantity.from_str("0.01")
    assert instrument.size_precision == 2
    assert instrument.min_quantity == Quantity.from_str("0.01")
    assert instrument.max_quantity == Quantity.from_str("200.00")
    assert instrument.multiplier == Quantity.from_str("100000")
    assert instrument.base_currency.code == "EUR"
    assert instrument.quote_currency.code == "USD"
    assert instrument.margin_init == Decimal(0)
    assert instrument.margin_maint == Decimal(0)
    assert instrument.maker_fee == Decimal(0)
    assert instrument.ts_event == NOW_NS
    assert instrument.ts_init == NOW_NS


async def test_a_cfd_carries_its_contract_size_and_settles_in_its_profit_currency(venue):
    instrument = await loaded(
        venue,
        symbol_info(
            name="DE40",
            digits=1,
            point=0.1,
            trade_tick_size=0.5,
            volume_step=0.1,
            volume_min=0.1,
            trade_contract_size=25.0,
            currency_base="USD",
            currency_profit="EUR",
            trade_calc_mode=mirror.SYMBOL_CALC_MODE_CFDINDEX,
        ),
    )
    assert instrument.price_increment == Price.from_str("0.5")
    assert instrument.size_precision == 1
    assert instrument.multiplier == Quantity.from_str("25")
    assert instrument.base_currency.code == "USD"
    assert instrument.quote_currency.code == "EUR"


async def test_the_price_increment_is_the_tick_size_not_a_digit(venue):
    instrument = await loaded(
        venue, symbol_info(name="US500", digits=2, point=0.01, trade_tick_size=0.25)
    )
    assert instrument.price_precision == 2
    assert instrument.price_increment == Price.from_str("0.25")


async def test_a_zero_tick_size_fails_the_load_naming_the_symbol(venue):
    venue.add(symbol_info(name="BROKEN", trade_tick_size=0.0))
    provider = provider_for(venue, "BROKEN")
    with pytest.raises(MT5InstrumentError, match="BROKEN"):
        await provider.initialize()


async def test_the_info_carries_the_venue_facts_and_bar_calendar(venue):
    instrument = await loaded(
        venue,
        symbol_info(
            chart_mode=mirror.SYMBOL_CHART_MODE_BID,
            filling_mode=3,
            trade_calc_mode=mirror.SYMBOL_CALC_MODE_FOREX,
            trade_mode=mirror.SYMBOL_TRADE_MODE_FULL,
            trade_stops_level=10,
            trade_freeze_level=5,
            volume_limit=50.0,
            currency_margin="EUR",
        ),
    )
    assert instrument.info == {
        "chart_mode": "BID",
        "filling_mode": 3,
        "trade_calc_mode": "FOREX",
        "trade_mode": "FULL",
        "trade_stops_level": 10,
        "trade_freeze_level": 5,
        "volume_limit": 50.0,
        "currency_margin": "EUR",
        "bar_tz": "America/New_York",
        "bar_day_open": "17:00",
        "bar_week_open": "SUNDAY",
        "bar_volume": "tick_count",
        "quote_sessions": relayed_sessions()["quote"],
        "trade_sessions": relayed_sessions()["trade"],
    }


async def test_the_info_carries_the_quote_and_trade_sessions_exactly_as_relayed(venue):
    quote = [
        [[64800, 0]],
        [[0, 61140], [64800, 0]],
        [[0, 61140], [64800, 0]],
        [[0, 61140], [64800, 0]],
        [[0, 61140], [64800, 0]],
        [[0, 61020]],
        [],
    ]
    # The trade week differs from the quote week, so each key is seen to carry its own.
    trade = [
        [[65100, 0]],
        [[0, 61140], [65100, 0]],
        [[0, 61140], [65100, 0]],
        [[0, 61140], [65100, 0]],
        [[0, 61140], [65100, 0]],
        [[0, 60900]],
        [],
    ]
    info = symbol_info(name="XAUUSD", chart_mode=mirror.SYMBOL_CHART_MODE_BID)
    venue.add(info, sessions={"quote": quote, "trade": trade, "last_error": 4307})
    provider = provider_for(venue, "XAUUSD")
    await provider.initialize()
    instrument = provider.find(InstrumentId.from_str("XAUUSD.MT5"))

    assert instrument.info["quote_sessions"] == [
        [[64800, 0]],
        [[0, 61140], [64800, 0]],
        [[0, 61140], [64800, 0]],
        [[0, 61140], [64800, 0]],
        [[0, 61140], [64800, 0]],
        [[0, 61020]],
        [],
    ]
    assert instrument.info["trade_sessions"] == [
        [[65100, 0]],
        [[0, 61140], [65100, 0]],
        [[0, 61140], [65100, 0]],
        [[0, 61140], [65100, 0]],
        [[0, 61140], [65100, 0]],
        [[0, 60900]],
        [],
    ]
    assert (
        instrument.info["chart_mode"],
        instrument.info["bar_tz"],
        instrument.info["bar_day_open"],
    ) == ("BID", "America/New_York", "17:00")


async def test_a_last_built_symbol_names_its_chart_mode(venue):
    instrument = await loaded(
        venue,
        symbol_info(
            chart_mode=mirror.SYMBOL_CHART_MODE_LAST, trade_mode=mirror.SYMBOL_TRADE_MODE_CLOSEONLY
        ),
    )
    assert instrument.info["chart_mode"] == "LAST"
    assert instrument.info["trade_mode"] == "CLOSEONLY"


# ── Loading ──────────────────────────────────────────────────────────────────


def three_symbols(venue):
    venue.add(symbol_info(name="EURUSDm"))
    venue.add(symbol_info(name="GBPUSDm", currency_base="GBP"))
    venue.add(symbol_info(name="USDJPYm", currency_base="USD", currency_profit="JPY"))


async def test_initializing_loads_exactly_the_ids_the_config_names(venue):
    three_symbols(venue)
    provider = provider_for(venue, "EURUSDm", "GBPUSDm")
    await provider.initialize()
    assert {instrument.id.value for instrument in provider.list_all()} == {
        "EURUSDm.MT5",
        "GBPUSDm.MT5",
    }


async def test_initializing_with_load_all_loads_every_symbol_the_terminal_serves(venue):
    three_symbols(venue)
    provider = provider_of(venue, InstrumentProviderConfig(load_all=True))
    await provider.initialize()
    assert {instrument.id.value for instrument in provider.list_all()} == {
        "EURUSDm.MT5",
        "GBPUSDm.MT5",
        "USDJPYm.MT5",
    }
    for symbol in ("EURUSDm", "GBPUSDm", "USDJPYm"):
        assert Call.SYMBOL_INFO in venue.asked_of(symbol)


async def test_loading_every_symbol_lists_the_terminals_symbols_once_for_the_load_and_its_pairs(
    venue,
):
    three_symbols(venue)
    provider = provider_of(venue, InstrumentProviderConfig(load_all=True))
    await provider.load_all_async()
    assert [call for call, _ in venue.asked].count(Call.SYMBOLS_GET) == 1
    assert provider.quoted_pairs() == {("USD", "JPY"): "USDJPYm"}
    assert Call.SYMBOL_INFO_TICK not in [call for call, _ in venue.asked]


@pytest.mark.parametrize(
    "config",
    [InstrumentProviderConfig(), InstrumentProviderConfig(load_ids=frozenset())],
    ids=["no-ids", "empty-ids"],
)
async def test_initializing_a_config_that_names_nothing_to_load_is_refused(venue, config):
    three_symbols(venue)
    provider = provider_of(venue, config)
    with pytest.raises(MT5ConfigError, match="instrument_provider"):
        await provider.initialize()
    assert provider.list_all() == []
    assert venue.asked == []


async def test_a_symbol_is_read_by_its_definition_schedule_and_sessions_alone(venue):
    venue.add(symbol_info(name="EURUSDm"))
    provider = provider_for(venue, "EURUSDm")
    await provider.initialize()
    assert venue.asked_of("EURUSDm") == [
        Call.SYMBOL_INFO,
        Call.COMMISSION_SCHEDULE,
        Call.TRADING_SESSIONS,
    ]


async def test_a_symbol_the_venue_does_not_know_fails_the_load_naming_it(venue):
    venue.add(symbol_info(name="EURUSD"))
    provider = provider_for(venue, "EURUSD", "NOSUCH")
    with pytest.raises(MT5SymbolNotFoundError, match="NOSUCH"):
        await provider.initialize()


async def test_loading_ids_loads_the_symbols_named_with_their_broker_casing(venue):
    venue.add(symbol_info(name="XAUUSDm", digits=2, trade_tick_size=0.01))
    provider = provider_for(venue)
    await provider.load_ids_async([InstrumentId.from_str("XAUUSDm.MT5")])
    assert provider.get_instrument("XAUUSDm").id.value == "XAUUSDm.MT5"


async def test_a_provider_of_a_named_venue_builds_and_finds_its_instruments_at_that_venue(venue):
    venue.add(symbol_info(name="XAUUSD", digits=2, trade_tick_size=0.01))
    clock = TestClock()
    clock.set_time(NOW_NS)
    provider = MT5InstrumentProvider(
        connection=connection_of(venue),
        venue=NtVenue("MT5_ALPHA"),
        clock=clock,
        config=InstrumentProviderConfig(
            load_ids=frozenset({InstrumentId.from_str("XAUUSD.MT5_ALPHA")})
        ),
    )
    await provider.initialize()
    assert [instrument.id.value for instrument in provider.list_all()] == ["XAUUSD.MT5_ALPHA"]
    assert provider.get_instrument("XAUUSD").id.value == "XAUUSD.MT5_ALPHA"


# ── Currencies ───────────────────────────────────────────────────────────────


@pytest.mark.parametrize(("code", "precision"), [("CLP", 0), ("KWD", 3), ("USD", 2), ("JPY", 0)])
async def test_a_settlement_currency_takes_its_precision_from_the_ladder(venue, code, precision):
    instrument = await loaded(venue, symbol_info(name=f"USD{code}", currency_profit=code))
    assert instrument.quote_currency.precision == precision


async def test_the_account_currency_settles_at_the_accounts_digits(venue):
    instrument = await loaded(
        venue,
        symbol_info(name="EURUST", currency_profit="UST"),
        currency="UST",
        currency_digits=2,
    )
    assert instrument.quote_currency.precision == 2


async def test_a_currency_neither_iso_nor_the_accounts_fails_the_load_naming_it(venue):
    venue.add(symbol_info(name="ODDPAIR", currency_profit="XYZ"))
    provider = provider_for(venue, "ODDPAIR")
    with pytest.raises(MT5InstrumentError, match="XYZ"):
        await provider.initialize()


async def test_a_metal_whose_base_nt_ships_beyond_its_python_module_loads(venue):
    instrument = await loaded(
        venue,
        symbol_info(
            name="XPTUSD",
            digits=2,
            point=0.01,
            trade_tick_size=0.01,
            trade_contract_size=100.0,
            currency_base="XPT",
            currency_profit="USD",
            trade_calc_mode=mirror.SYMBOL_CALC_MODE_CFD,
        ),
    )
    assert instrument.base_currency.code == "XPT"
    assert instrument.base_currency.precision == 2


async def test_a_metal_whose_base_has_no_known_precision_still_loads(venue):
    instrument = await loaded(
        venue,
        symbol_info(
            name="XPDUSD",
            digits=2,
            point=0.01,
            trade_tick_size=0.01,
            trade_contract_size=100.0,
            currency_base="XPD",
            currency_profit="USD",
            trade_calc_mode=mirror.SYMBOL_CALC_MODE_CFD,
        ),
    )
    assert instrument.base_currency.code == "XPD"
    assert instrument.quote_currency.code == "USD"


# ── Conversion pairs ─────────────────────────────────────────────────────────

CURRENCIES = (
    "AUD",
    "CAD",
    "CHF",
    "CNH",
    "CZK",
    "DKK",
    "EUR",
    "GBP",
    "HKD",
    "HUF",
    "JPY",
    "MXN",
    "NOK",
    "NZD",
    "PLN",
    "SEK",
    "SGD",
    "USD",
    "ZAR",
)


def forex(name, base, profit, trade_mode=mirror.SYMBOL_TRADE_MODE_FULL):
    """A FOREX symbol quoting `base` in `profit`."""
    return symbol_info(
        name=name,
        currency_base=base,
        currency_profit=profit,
        currency_margin=base,
        trade_mode=trade_mode,
    )


def index_cfd(name, settlement):
    """An index CFD settling in `settlement`."""
    return symbol_info(
        name=name,
        digits=2,
        point=0.01,
        trade_tick_size=0.01,
        trade_contract_size=1.0,
        currency_base=settlement,
        currency_profit=settlement,
        trade_calc_mode=mirror.SYMBOL_CALC_MODE_CFDINDEX,
    )


def listed_pairs(count):
    """The first `count` pairs of CURRENCIES, in alphabetical order."""
    return list(itertools.islice(itertools.combinations(CURRENCIES, 2), count))


def asked_beyond(venue, *symbols):
    """The symbols outside `symbols` the provider asked anything of."""
    return [name for name in venue.definitions if name not in symbols and venue.asked_of(name)]


def loaded_ids(provider):
    return sorted(instrument.id.value for instrument in provider.list_all())


async def test_a_run_settling_in_the_account_currency_asks_nothing_of_the_listed_pairs(venue):
    pairs = listed_pairs(75)
    for base, profit in pairs:
        venue.add(
            forex(f"{base}{profit}", base, profit, mirror.SYMBOL_TRADE_MODE_CLOSEONLY),
            bid=1.0,
            ask=1.0,
        )
    for base, profit in pairs[-2:]:
        venue.add(forex(f"{base}{profit}.a", base, profit))
    venue.add(
        symbol_info(
            name="BTCUSD.a",
            digits=2,
            point=0.01,
            trade_tick_size=0.01,
            trade_contract_size=1.0,
            currency_base="BTC",
            currency_profit="USD",
            trade_calc_mode=mirror.SYMBOL_CALC_MODE_CFD,
        )
    )
    provider = provider_for(venue, "BTCUSD.a")
    await provider.initialize()
    assert asked_beyond(venue, "BTCUSD.a") == []
    assert provider.quoted_pairs() == {}


async def test_an_account_in_another_currency_resolves_and_loads_the_one_pair_its_settlement_needs(
    venue,
):
    for base, profit in listed_pairs(101):
        venue.add(forex(f"{base}{profit}.r", base, profit))
    venue.add(index_cfd("US30", "USD"))
    provider = provider_for(venue, "US30", currency="AUD")
    await provider.initialize()
    assert provider.quoted_pairs() == {("AUD", "USD"): "AUDUSD.r"}
    assert loaded_ids(provider) == ["AUDUSD.r.MT5", "US30.MT5"]
    assert venue.asked_of("AUDUSD.r") == [
        Call.SYMBOL_INFO,
        Call.COMMISSION_SCHEDULE,
        Call.TRADING_SESSIONS,
    ]
    assert asked_beyond(venue, "US30", "AUDUSD.r") == []


async def test_a_settlement_two_pairs_from_the_account_currency_resolves_and_loads_both(venue):
    venue.add(forex("GBPUSD", "GBP", "USD"))
    venue.add(forex("AUDUSD", "AUD", "USD"))
    venue.add(index_cfd("UK100", "GBP"))
    provider = provider_for(venue, "UK100", currency="AUD")
    await provider.initialize()
    assert provider.quoted_pairs() == {("GBP", "USD"): "GBPUSD", ("AUD", "USD"): "AUDUSD"}
    assert loaded_ids(provider) == ["AUDUSD.MT5", "GBPUSD.MT5", "UK100.MT5"]
    assert Call.SYMBOL_INFO_TICK not in [call for call, _ in venue.asked]


async def test_a_quoting_symbols_own_conversions_are_resolved_and_loaded_in_turn(venue):
    venue.add(
        forex("AUDUSD", "AUD", "USD"),
        bid=0.65,
        ask=0.65,
        schedule=schedule(
            2.0, "EUR", mode=CommissionMode.MONEY_SPECIFIED, entry=CommissionEntryMode.INOUT
        ),
    )
    venue.add(forex("EURUSD", "EUR", "USD"), bid=1.25, ask=1.25)
    venue.add(forex("GBPUSD", "GBP", "USD"), bid=1.5, ask=1.5)
    venue.add(index_cfd("US500", "USD"))
    provider = provider_for(venue, "US500", currency="AUD")
    await provider.initialize()
    assert provider.quoted_pairs() == {("AUD", "USD"): "AUDUSD", ("EUR", "USD"): "EURUSD"}
    assert loaded_ids(provider) == ["AUDUSD.MT5", "EURUSD.MT5", "US500.MT5"]
    assert venue.asked_of("GBPUSD") == []
    fee = provider.get_instrument("AUDUSD").taker_fee
    value, rate, price = Decimal(2), Decimal("1.25"), Decimal("0.65")
    assert abs(fee - value * rate / price) < Decimal("1e-18")


async def test_successive_loads_keep_every_instrument_and_the_last_loads_quoting_symbols(venue):
    venue.add(forex("EURUSD", "EUR", "USD"))
    venue.add(forex("GBPUSD", "GBP", "USD"))
    venue.add(index_cfd("DE40", "EUR"))
    venue.add(index_cfd("UK100", "GBP"))
    provider = provider_for(venue)
    await provider.load_ids_async([InstrumentId.from_str("DE40.MT5")])
    assert provider.quoted_pairs() == {("EUR", "USD"): "EURUSD"}
    await provider.load_ids_async([InstrumentId.from_str("UK100.MT5")])
    assert provider.quoted_pairs() == {("GBP", "USD"): "GBPUSD"}
    assert loaded_ids(provider) == ["DE40.MT5", "EURUSD.MT5", "GBPUSD.MT5", "UK100.MT5"]


async def test_a_pair_prefers_the_fully_tradable_symbol_over_a_disabled_one(venue):
    venue.add(symbol_info(name="EURUSD", trade_mode=mirror.SYMBOL_TRADE_MODE_DISABLED))
    venue.add(symbol_info(name="EURUSD+", trade_mode=mirror.SYMBOL_TRADE_MODE_FULL))
    venue.add(symbol_info(name="GBPUSD+", currency_base="GBP"))
    provider = provider_for(venue, "GBPUSD+", currency="EUR")
    await provider.initialize()
    assert provider.quoted_pairs()[("EUR", "USD")] == "EURUSD+"
    assert venue.asked_of("EURUSD") == []


async def test_a_pair_no_symbol_trades_fully_is_the_first_that_prices(venue):
    venue.add(
        symbol_info(
            name="USTUSD",
            currency_base="UST",
            currency_profit="USD",
            trade_mode=mirror.SYMBOL_TRADE_MODE_DISABLED,
        ),
        bid=1.0,
        ask=1.0,
    )
    venue.add(symbol_info(name="EURUSD+"))
    provider = provider_for(venue, "EURUSD+", currency="UST")
    await provider.initialize()
    assert provider.quoted_pairs() == {("UST", "USD"): "USTUSD"}
    assert venue.asked_of("USTUSD") == [
        Call.SYMBOL_INFO_TICK,
        Call.SYMBOL_INFO,
        Call.COMMISSION_SCHEDULE,
        Call.TRADING_SESSIONS,
    ]


async def test_a_pair_whose_candidates_all_quote_zeros_has_no_quoting_symbol(venue):
    venue.add(
        symbol_info(
            name="USTUSD",
            currency_base="UST",
            currency_profit="USD",
            trade_mode=mirror.SYMBOL_TRADE_MODE_DISABLED,
        )
    )
    venue.add(symbol_info(name="EURUSD+"))
    provider = provider_for(venue, "EURUSD+", currency="UST")
    await provider.initialize()
    assert provider.quoted_pairs() == {}
    assert venue.asked_of("USTUSD") == [Call.SYMBOL_INFO_TICK]


async def test_a_pair_the_run_trades_is_its_own_quote(venue):
    venue.add(symbol_info(name="EURUSD+", trade_mode=mirror.SYMBOL_TRADE_MODE_FULL))
    venue.add(symbol_info(name="EURUSD.a", trade_mode=mirror.SYMBOL_TRADE_MODE_FULL))
    provider = provider_for(venue, "EURUSD.a", currency="EUR")
    await provider.initialize()
    assert provider.quoted_pairs()[("EUR", "USD")] == "EURUSD.a"


async def test_only_forex_symbols_quote_a_pair(venue):
    venue.add(
        symbol_info(
            name="EURUSD",
            currency_base="EUR",
            currency_profit="USD",
            trade_calc_mode=mirror.SYMBOL_CALC_MODE_CFD,
        )
    )
    venue.add(forex("EURUSDx", "EUR", "USD"))
    venue.add(index_cfd("US500", "USD"))
    provider = provider_for(venue, "US500", currency="EUR")
    await provider.initialize()
    assert provider.quoted_pairs() == {("EUR", "USD"): "EURUSDx"}


async def test_the_account_currency_is_the_accounts(venue):
    provider = provider_for(venue, "EURUSD", currency="UST")
    assert await provider.account_currency() == "UST"


# ── Fees ─────────────────────────────────────────────────────────────────────


async def test_a_relayed_schedule_without_a_rule_is_a_zero_taker_fee(venue):
    instrument = await loaded(venue, symbol_info(name="DE40.a"))
    assert instrument.taker_fee == Decimal(0)


async def test_a_deposit_currency_rule_charged_on_both_legs_is_a_whole_fee_per_fill(venue):
    venue.add(
        symbol_info(name="EURUSD.a", trade_contract_size=100000.0),
        bid=1.08490,
        ask=1.08510,
        schedule=schedule(
            3.5, "USD", mode=CommissionMode.MONEY_DEPOSIT, entry=CommissionEntryMode.INOUT
        ),
    )
    provider = provider_for(venue, "EURUSD.a")
    await provider.initialize()
    fee = provider.get_instrument("EURUSD.a").taker_fee
    value, contract_size, price = Decimal("3.5"), Decimal("100000"), Decimal("1.08500")
    assert isinstance(fee, Decimal)
    assert abs(fee - value / (contract_size * price)) < Decimal("1e-18")
    assert fee.quantize(Decimal("0.0000001")) == Decimal("0.0000323")


async def test_an_entry_only_rule_in_the_deposit_currency_is_halved_and_converted(venue):
    venue.add(
        symbol_info(
            name="USTUSD",
            currency_base="UST",
            currency_profit="USD",
            trade_mode=mirror.SYMBOL_TRADE_MODE_DISABLED,
        ),
        bid=1.0,
        ask=1.0,
    )
    venue.add(
        symbol_info(name="EURUSD+", trade_contract_size=100000.0),
        bid=1.08500,
        ask=1.08500,
        schedule=schedule(
            6.0, "UST", mode=CommissionMode.MONEY_DEPOSIT, entry=CommissionEntryMode.IN
        ),
    )
    provider = provider_for(venue, "EURUSD+", currency="UST", currency_digits=2)
    await provider.initialize()
    fee = provider.get_instrument("EURUSD+").taker_fee
    value, rate, contract_size, price = Decimal(6), Decimal(1), Decimal(100000), Decimal("1.085")
    assert abs(fee - value * rate / 2 / (contract_size * price)) < Decimal("1e-18")
    assert fee.quantize(Decimal("0.0000001")) == Decimal("0.0000276")


async def test_a_per_share_rule_in_a_named_currency_is_divided_by_the_price(venue):
    venue.add(
        symbol_info(
            name="AAPL",
            digits=2,
            point=0.01,
            trade_tick_size=0.01,
            volume_step=1.0,
            volume_min=1.0,
            trade_contract_size=1.0,
            currency_base="USD",
            currency_profit="USD",
            trade_calc_mode=mirror.SYMBOL_CALC_MODE_CFD,
        ),
        bid=230.0,
        ask=230.0,
        schedule=schedule(
            0.02, "USD", mode=CommissionMode.MONEY_SPECIFIED, entry=CommissionEntryMode.IN
        ),
    )
    provider = provider_for(venue, "AAPL")
    await provider.initialize()
    fee = provider.get_instrument("AAPL").taker_fee
    assert abs(fee - Decimal("0.02") / 2 / Decimal(230)) < Decimal("1e-18")
    assert fee.quantize(Decimal("0.0000001")) == Decimal("0.0000435")


async def test_a_rule_in_a_currency_the_venue_quotes_inversely_is_converted_through_that_pair(
    venue,
):
    venue.add(symbol_info(name="EURUSD", trade_mode=mirror.SYMBOL_TRADE_MODE_FULL), 1.25, 1.25)
    venue.add(
        symbol_info(
            name="DE40",
            digits=1,
            point=0.1,
            trade_tick_size=0.1,
            trade_contract_size=1.0,
            currency_base="EUR",
            currency_profit="EUR",
            trade_calc_mode=mirror.SYMBOL_CALC_MODE_CFDINDEX,
        ),
        bid=20000.0,
        ask=20000.0,
        schedule=schedule(
            5.0, "USD", mode=CommissionMode.MONEY_DEPOSIT, entry=CommissionEntryMode.INOUT
        ),
    )
    provider = provider_for(venue, "DE40")
    await provider.initialize()
    fee = provider.get_instrument("DE40").taker_fee
    assert abs(fee - Decimal(5) / Decimal("1.25") / (Decimal(1) * Decimal(20000))) < Decimal(
        "1e-18"
    )


async def test_a_money_rule_in_another_currency_resolves_its_pair_and_converts_through_its_symbol(
    venue,
):
    venue.add(forex("EURUSD", "EUR", "USD"), bid=1.25, ask=1.25)
    venue.add(forex("GBPUSD", "GBP", "USD"), bid=1.5, ask=1.5)
    venue.add(
        index_cfd("US500", "USD"),
        bid=5000.0,
        ask=5000.0,
        schedule=schedule(
            0.5, "EUR", mode=CommissionMode.MONEY_SPECIFIED, entry=CommissionEntryMode.INOUT
        ),
    )
    provider = provider_for(venue, "US500")
    await provider.initialize()
    assert provider.quoted_pairs() == {("EUR", "USD"): "EURUSD"}
    assert loaded_ids(provider) == ["EURUSD.MT5", "US500.MT5"]
    assert venue.asked_of("GBPUSD") == []
    fee = provider.get_instrument("US500").taker_fee
    value, rate, price = Decimal("0.5"), Decimal("1.25"), Decimal(5000)
    assert abs(fee - value * rate / price) < Decimal("1e-18")


async def test_a_money_rule_with_no_direct_pair_to_the_settlement_converts_along_the_walk(venue):
    venue.add(
        forex("USTUSD", "UST", "USD", mirror.SYMBOL_TRADE_MODE_DISABLED),
        bid=1.0,
        ask=1.0,
    )
    venue.add(forex("USDJPY", "USD", "JPY"), bid=150.0, ask=150.0)
    venue.add(
        index_cfd("JP225", "JPY"),
        bid=38000.0,
        ask=38000.0,
        schedule=schedule(
            2.0, "UST", mode=CommissionMode.MONEY_DEPOSIT, entry=CommissionEntryMode.INOUT
        ),
    )
    provider = provider_for(venue, "JP225", currency="UST")
    await provider.initialize()
    assert provider.quoted_pairs() == {("UST", "USD"): "USTUSD", ("USD", "JPY"): "USDJPY"}
    assert loaded_ids(provider) == ["JP225.MT5", "USDJPY.MT5", "USTUSD.MT5"]
    fee = provider.get_instrument("JP225").taker_fee
    value, ust_usd, usd_jpy = Decimal(2), Decimal("1.0"), Decimal("150.0")
    contract_size, price = Decimal(1), Decimal(38000)
    assert abs(fee - value * ust_usd * usd_jpy / (contract_size * price)) < Decimal("1e-18")


async def test_a_money_rule_converts_through_a_hop_the_venue_quotes_the_other_way_round(venue):
    """The second hop runs from GBPUSD's profit currency to its base and is therefore inverted."""
    venue.add(forex("EURUSD", "EUR", "USD"), bid=1.25, ask=1.25)
    venue.add(forex("GBPUSD", "GBP", "USD"), bid=1.5, ask=1.5)
    venue.add(
        index_cfd("UK100", "GBP"),
        bid=8000.0,
        ask=8000.0,
        schedule=schedule(
            0.5, "EUR", mode=CommissionMode.MONEY_SPECIFIED, entry=CommissionEntryMode.INOUT
        ),
    )
    provider = provider_for(venue, "UK100")
    await provider.initialize()
    assert provider.quoted_pairs() == {("EUR", "USD"): "EURUSD", ("GBP", "USD"): "GBPUSD"}
    fee = provider.get_instrument("UK100").taker_fee
    value, rate, price = Decimal("0.5"), Decimal("1.25") / Decimal("1.5"), Decimal(8000)
    assert abs(fee - value * rate / price) < Decimal("1e-18")


async def test_a_conversion_symbol_whose_quote_the_hub_fails_fails_the_load_with_its_error(venue):
    venue.add(symbol_info(name="EURUSD", trade_mode=mirror.SYMBOL_TRADE_MODE_FULL), 1.25, 1.25)
    venue.add(
        symbol_info(
            name="DE40",
            digits=1,
            point=0.1,
            trade_tick_size=0.1,
            trade_contract_size=1.0,
            currency_base="EUR",
            currency_profit="EUR",
            trade_calc_mode=mirror.SYMBOL_CALC_MODE_CFDINDEX,
        ),
        bid=20000.0,
        ask=20000.0,
        schedule=schedule(
            5.0, "USD", mode=CommissionMode.MONEY_DEPOSIT, entry=CommissionEntryMode.INOUT
        ),
    )
    failure = EaError("symbol_info_tick: the publisher chart of EURUSD failed to open", 4105)
    venue.refusals[(Call.SYMBOL_INFO_TICK, "EURUSD")] = [failure]
    provider = provider_for(venue, "DE40")
    with pytest.raises(EaError) as refused:
        await provider.initialize()
    assert refused.value is failure
    assert provider.list_all() == []


async def test_a_rule_in_a_currency_the_venue_cannot_convert_fails_the_load_naming_it(venue):
    venue.add(
        symbol_info(name="EURUSD.a"),
        bid=1.085,
        ask=1.085,
        schedule=schedule(
            3.5, "GBP", mode=CommissionMode.MONEY_DEPOSIT, entry=CommissionEntryMode.INOUT
        ),
    )
    provider = provider_for(venue, "EURUSD.a")
    with pytest.raises(MT5InstrumentError, match="EURUSD.a") as refused:
        await provider.initialize()
    assert "GBP" in str(refused.value)


async def test_a_percent_rule_charged_on_both_legs_is_its_percentage_of_the_notional(venue):
    venue.add(
        symbol_info(name="EURUSD.a"),
        bid=1.08490,
        ask=1.08510,
        schedule=schedule(0.002, "", mode=CommissionMode.PERCENT, entry=CommissionEntryMode.INOUT),
    )
    provider = provider_for(venue, "EURUSD.a")
    await provider.initialize()
    fee = provider.get_instrument("EURUSD.a").taker_fee
    pct = Decimal("0.002")
    assert fee == pct / 100


async def test_a_percent_rule_charged_on_entry_alone_is_halved(venue):
    venue.add(
        symbol_info(name="EURUSD.a"),
        bid=1.085,
        ask=1.085,
        schedule=schedule(0.004, "", mode=CommissionMode.PERCENT, entry=CommissionEntryMode.IN),
    )
    provider = provider_for(venue, "EURUSD.a")
    await provider.initialize()
    fee = provider.get_instrument("EURUSD.a").taker_fee
    pct = Decimal("0.004")
    assert fee == pct / 100 / 2


async def test_a_points_rule_is_its_points_in_price_over_the_price(venue):
    venue.add(
        symbol_info(name="EURUSD.a", point=0.00001),
        bid=1.08490,
        ask=1.08510,
        schedule=schedule(30.0, "", mode=CommissionMode.PIPS, entry=CommissionEntryMode.INOUT),
    )
    provider = provider_for(venue, "EURUSD.a")
    await provider.initialize()
    fee = provider.get_instrument("EURUSD.a").taker_fee
    pts, point, price = Decimal(30), Decimal("0.00001"), Decimal("1.08500")
    assert abs(fee - pts * point / price) < Decimal("1e-18")
    assert fee.quantize(Decimal("0.0000001")) == Decimal("0.0002765")


async def test_a_points_rule_charged_on_entry_alone_is_halved(venue):
    venue.add(
        symbol_info(name="EURUSD.a", point=0.00001),
        bid=1.085,
        ask=1.085,
        schedule=schedule(30.0, "", mode=CommissionMode.PIPS, entry=CommissionEntryMode.IN),
    )
    provider = provider_for(venue, "EURUSD.a")
    await provider.initialize()
    fee = provider.get_instrument("EURUSD.a").taker_fee
    assert abs(fee - Decimal(30) * Decimal("0.00001") / Decimal("1.085") / 2) < Decimal("1e-18")


@pytest.mark.parametrize(
    ("mode", "entry", "named"),
    [
        (CommissionMode.PERCENT_PROFIT, CommissionEntryMode.INOUT, CommissionMode.PERCENT_PROFIT),
        (CommissionMode.MONEY_DEPOSIT, CommissionEntryMode.OUT, CommissionEntryMode.OUT),
    ],
    ids=["mode-without-derivation", "exit-only"],
)
async def test_a_commission_rule_without_a_fee_derivation_fails_the_load_naming_it(
    venue, mode, entry, named
):
    venue.add(
        symbol_info(name="EURUSD.a"),
        bid=1.085,
        ask=1.085,
        schedule=schedule(0.001, "USD", mode=mode, entry=entry),
    )
    provider = provider_for(venue, "EURUSD.a")
    with pytest.raises(MT5InstrumentError, match="EURUSD.a") as refused:
        await provider.initialize()
    assert named in str(refused.value)


async def test_a_non_finite_commission_value_fails_the_load_naming_the_symbol(venue):
    venue.add(
        symbol_info(name="EURUSD.a"),
        bid=1.085,
        ask=1.085,
        schedule=schedule(
            float("nan"), "USD", mode=CommissionMode.MONEY_DEPOSIT, entry=CommissionEntryMode.INOUT
        ),
    )
    provider = provider_for(venue, "EURUSD.a")
    with pytest.raises(MT5InstrumentError, match="EURUSD.a"):
        await provider.initialize()


async def test_a_commission_rule_without_a_tier_fails_the_load_naming_the_symbol(venue):
    relayed = schedule(0.001, "USD")
    relayed["rules"][0]["tiers"] = []
    venue.add(symbol_info(name="EURUSD.a"), bid=1.085, ask=1.085, schedule=relayed)
    provider = provider_for(venue, "EURUSD.a")
    with pytest.raises(MT5InstrumentError, match="EURUSD.a: the commission rule has no tier"):
        await provider.initialize()


async def test_a_non_finite_quote_fails_the_load_naming_the_symbol(venue):
    venue.add(
        symbol_info(name="EURUSD.a"),
        bid=float("nan"),
        ask=1.085,
        schedule=schedule(
            3.5, "USD", mode=CommissionMode.MONEY_DEPOSIT, entry=CommissionEntryMode.INOUT
        ),
    )
    provider = provider_for(venue, "EURUSD.a")
    with pytest.raises(MT5InstrumentError, match="EURUSD.a"):
        await provider.initialize()


async def test_a_reload_recomputes_the_fee_from_the_current_schedule(venue):
    venue.add(symbol_info(name="EURUSD.a"), bid=1.085, ask=1.085)
    provider = provider_for(venue, "EURUSD.a")
    await provider.initialize()
    assert provider.get_instrument("EURUSD.a").taker_fee == Decimal(0)
    venue.schedules["EURUSD.a"] = schedule(
        3.5, "USD", mode=CommissionMode.MONEY_DEPOSIT, entry=CommissionEntryMode.INOUT
    )
    await provider.initialize(reload=True)
    fee = provider.get_instrument("EURUSD.a").taker_fee
    assert fee.quantize(Decimal("0.0000001")) == Decimal("0.0000323")


# ── Provider mechanics ───────────────────────────────────────────────────────


async def test_every_load_refuses_a_lost_connection(venue):
    venue.add(symbol_info(name="EURUSD"))
    provider = provider_for(venue, "EURUSD")
    provider._conn.ensure_connected.side_effect = MT5ConnectionError("not connected")
    with pytest.raises(MT5ConnectionError):
        await provider.load_all_async()
    with pytest.raises(MT5ConnectionError):
        await provider.load_ids_async([InstrumentId.from_str("EURUSD.MT5")])
    with pytest.raises(MT5ConnectionError):
        await provider.load_symbol("EURUSD")


async def test_loading_one_symbol_answers_and_holds_its_instrument(venue):
    venue.add(symbol_info(name="EURUSDm"))
    provider = provider_for(venue)
    instrument = await provider.load_symbol("EURUSDm")
    assert instrument.id.value == "EURUSDm.MT5"
    assert provider.get_instrument("EURUSDm") is instrument
    assert provider.get_instrument("EURUSDM") is None


async def test_a_definition_the_hub_fails_as_an_unknown_symbol_is_not_found(venue):
    venue.refusals[(Call.SYMBOL_INFO, "NOSUCH")] = [
        EaError(
            "symbol_info: SymbolExist found no such symbol", MqlError.MARKET_UNKNOWN_SYMBOL.value
        )
    ]
    provider = provider_for(venue)
    with pytest.raises(MT5SymbolNotFoundError) as refused:
        await provider.load_symbol("NOSUCH")
    assert refused.value.symbol == "NOSUCH"
    assert str(refused.value) == "symbol 'NOSUCH' not found"


async def test_a_definition_the_hub_fails_with_another_error_raises_that_error(venue):
    failure = EaError("symbol_info: SymbolInfoString failed", 4756)
    venue.refusals[(Call.SYMBOL_INFO, "NOSUCH")] = [failure]
    provider = provider_for(venue)
    with pytest.raises(EaError) as refused:
        await provider.load_symbol("NOSUCH")
    assert refused.value is failure


async def test_a_definition_deferred_while_its_chart_opens_is_asked_again_and_loads(venue):
    venue.add(symbol_info(name="EURUSD"))
    venue.refusals[(Call.SYMBOL_INFO, "EURUSD")] = [
        NoPublisher("symbol_info: no publisher EA for EURUSD; chart requested", 0, 0.05)
    ]
    provider = provider_for(venue)
    instrument = await provider.load_symbol("EURUSD")
    assert instrument.id.value == "EURUSD.MT5"
    assert venue.asked_of("EURUSD").count(Call.SYMBOL_INFO) == 2


async def test_a_schedule_deferred_until_it_is_relayed_is_asked_again_and_loads(venue):
    venue.add(symbol_info(name="EURUSD"))
    venue.refusals[(Call.COMMISSION_SCHEDULE, "EURUSD")] = [
        Syncing("commission_schedule: no commission_schedule relayed for EURUSD", 0, 0.05)
    ]
    provider = provider_for(venue)
    instrument = await provider.load_symbol("EURUSD")
    assert instrument.taker_fee == Decimal(0)
    assert venue.asked_of("EURUSD").count(Call.COMMISSION_SCHEDULE) == 2


async def test_a_load_the_hub_defers_leaves_its_callers_loop_running_and_adds_on_it(venue):
    venue.add(symbol_info(name="EURUSD"))
    venue.refusals[(Call.SYMBOL_INFO, "EURUSD")] = [
        NotReady("symbol_info: the broker clock is not verified", 0, 0.25)
    ]
    provider = provider_for(venue)
    added_on = []

    def add(instrument):
        added_on.append(threading.current_thread())
        MT5InstrumentProvider.add(provider, instrument)

    provider.add = add
    order = []
    asyncio.get_running_loop().call_later(0.025, order.append, "callback")

    await provider.load_ids_async([InstrumentId.from_str("EURUSD.MT5")])
    order.append("loaded")

    assert order == ["callback", "loaded"]
    assert added_on == [threading.current_thread()]


async def test_cancelling_a_load_the_hub_defers_ends_its_reads_and_adds_nothing(venue):
    venue.add(symbol_info(name="EURUSD"))
    venue.refusals[(Call.SYMBOL_INFO, "EURUSD")] = [
        NotReady("symbol_info: the broker clock is not verified", 0, 0.25)
    ]
    provider = provider_for(venue)
    load = asyncio.create_task(provider.load_ids_async([InstrumentId.from_str("EURUSD.MT5")]))
    await eventually(lambda: Call.SYMBOL_INFO in venue.asked_of("EURUSD"))

    load.cancel()
    with pytest.raises(asyncio.CancelledError):
        await load
    await asyncio.sleep(0.5)

    assert venue.asked_of("EURUSD") == [Call.SYMBOL_INFO]
    assert provider.list_all() == []


async def test_a_relayed_schedule_reaches_the_provider_with_its_selectors_members(venue):
    venue.add(symbol_info(name="EURUSD"), schedule=schedule(3.5, "USD"))
    relayed = await venue.request(Call.COMMISSION_SCHEDULE, symbol="EURUSD")
    rule = parse_schedule(relayed.result)
    assert (type(rule.entry), type(rule.mode)) == (CommissionEntryMode, CommissionMode)


async def test_a_failed_load_adds_none_of_its_symbols(venue):
    venue.add(symbol_info(name="EURUSD"))
    venue.add(symbol_info(name="ODDPAIR", currency_profit="XYZ"))
    provider = provider_for(venue)
    with pytest.raises(MT5InstrumentError):
        await provider._load(["EURUSD", "ODDPAIR"])
    assert provider.list_all() == []


async def test_loads_accumulate_and_a_reload_replaces_the_instrument(venue):
    venue.add(symbol_info(name="EURUSD"))
    venue.add(symbol_info(name="GBPUSD", currency_base="GBP"))
    provider = provider_for(venue)
    await provider.load_symbol("EURUSD")
    await provider.load_symbol("GBPUSD")
    await provider.load_symbol("EURUSD")
    assert sorted(instrument.id.value for instrument in provider.list_all()) == [
        "EURUSD.MT5",
        "GBPUSD.MT5",
    ]


async def test_a_listing_the_hub_fails_fails_the_load_with_its_error(venue):
    venue.add(symbol_info(name="EURUSD"))
    failure = EaError("symbols_get: SymbolName failed", 4756)
    venue.refusals[(Call.SYMBOLS_GET, None)] = [failure]
    provider = provider_for(venue)
    with pytest.raises(EaError) as refused:
        await provider.load_symbol("EURUSD")
    assert refused.value is failure
    assert provider.list_all() == []
