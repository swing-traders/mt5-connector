"""The instrument provider against a venue double: what it loads and how it builds each definition,
its currencies, the conversion pairs it finds, and the taker fees it derives."""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
from nautilus_trader.common.component import TestClock
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Venue as NtVenue
from nautilus_trader.model.instruments import Cfd, CurrencyPair
from nautilus_trader.model.objects import Price, Quantity
from venue_doubles import account_info, symbol_info, tick

from mt5connector.client.connection import AccountSnapshot, MT5Connection
from mt5connector.client.constants import MT5_VENUE
from mt5connector.client.errors import (
    MT5ConfigError,
    MT5ConnectionError,
    MT5InstrumentError,
    MT5SymbolNotFoundError,
)
from mt5connector.client.providers import MT5InstrumentProvider
from mt5connector.wire import mirror

NOW_NS = 1_760_000_000_000_000_000

# The names EnumToString gives the commission enumerations' members, as the EA relays them.
MONEY_DEPOSIT = "SYMBOL_COMMISSION_MONEY_DEPOSIT"
MONEY_SPECIFIED = "SYMBOL_COMMISSION_MONEY_SPECIFIED"
PERCENT = "SYMBOL_COMMISSION_PERCENT"
POINTS = "SYMBOL_COMMISSION_PIPS"
PERCENT_PROFIT = "SYMBOL_COMMISSION_PERCENT_PROFIT"
ENTRY_INOUT = "SYMBOL_COMMISSION_ENTRY_INOUT"
ENTRY_IN = "SYMBOL_COMMISSION_ENTRY_IN"
ENTRY_OUT = "SYMBOL_COMMISSION_ENTRY_OUT"

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


class Venue:
    """The terminal behind the shim: symbol definitions, quotes and relayed commission schedules,
    and the calls made on it in order."""

    def __init__(self) -> None:
        self.symbols = {}
        self.ticks = {}
        self.schedules = {}
        self.unselectable = set()
        self.calls = []

    def add(self, info, bid=None, ask=None, schedule=None):
        self.symbols[info.name] = info
        if bid is not None:
            self.ticks[info.name] = tick(bid, ask)
        if schedule is not None:
            self.schedules[info.name] = schedule

    def symbol_select(self, name, enable):
        self.calls.append(("symbol_select", name))
        return name in self.symbols and name not in self.unselectable

    def symbol_info(self, name):
        self.calls.append(("symbol_info", name))
        return self.symbols.get(name)

    def symbols_get(self, group=None):
        return tuple(self.symbols.values())

    def symbol_info_tick(self, name):
        if name in self.ticks:
            return self.ticks[name]
        else:
            return tick(0.0, 0.0, time=0)

    def commission_schedule(self, name):
        return self.schedules.get(name, {"ret": 0, "last_error": 0, "rules": []})


def schedule(value, currency, mode=MONEY_DEPOSIT, entry=ENTRY_INOUT):
    """One relayed SymbolInfoCommissions answer: one rule with one volume tier."""
    return {
        "ret": 1,
        "last_error": 0,
        "rules": [
            {
                "currency": currency,
                "mode_range": "SYMBOL_COMMISSION_RANGE_VOLUME",
                "mode_charge": "SYMBOL_COMMISSION_CHARGE_INSTANT",
                "mode_entry": entry,
                "mode_direction": "SYMBOL_COMMISSION_DIRECTION_BOTH",
                "mode_profit": "SYMBOL_COMMISSION_PROFIT_ALL",
                "tiers": [
                    {
                        "mode": mode,
                        "volume_type": "SYMBOL_COMMISSION_VOLUME_TYPE_VOLUME",
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
    double = Venue()
    package = MagicMock()
    package.symbol_select.side_effect = double.symbol_select
    package.symbol_info.side_effect = double.symbol_info
    package.symbols_get.side_effect = double.symbols_get
    package.symbol_info_tick.side_effect = double.symbol_info_tick
    package.commission_schedule.side_effect = double.commission_schedule
    package.last_error.return_value = (1, "Success")
    with patch("mt5connector.client.providers.mt5", package):
        yield double


def provider_of(config: InstrumentProviderConfig, **account) -> MT5InstrumentProvider:
    conn = MagicMock(spec=MT5Connection)
    conn.get_account_info.return_value = AccountSnapshot.from_mt5(account_info(**account))
    clock = TestClock()
    clock.set_time(NOW_NS)
    return MT5InstrumentProvider(connection=conn, venue=MT5_VENUE, clock=clock, config=config)


def provider_for(*symbols, **account):
    """A provider whose config names `symbols`."""
    return provider_of(
        InstrumentProviderConfig(
            load_ids=frozenset(InstrumentId.from_str(f"{s}.MT5") for s in symbols)
        ),
        **account,
    )


async def loaded(venue, info, **account):
    venue.add(info)
    provider = provider_for(info.name, **account)
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
    provider = provider_for("ZN")
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
    provider = provider_for("BROKEN")
    with pytest.raises(MT5InstrumentError, match="BROKEN"):
        await provider.initialize()


async def test_the_info_carries_the_venue_facts_and_session_calendar(venue):
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
        "session_tz": "America/New_York",
        "session_day_open": "17:00",
        "session_week_open": "SUNDAY",
        "bar_volume": "tick_count",
    }


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
    provider = provider_for("EURUSDm", "GBPUSDm")
    await provider.initialize()
    assert {instrument.id.value for instrument in provider.list_all()} == {
        "EURUSDm.MT5",
        "GBPUSDm.MT5",
    }


async def test_initializing_with_load_all_loads_every_symbol_the_terminal_serves(venue):
    three_symbols(venue)
    provider = provider_of(InstrumentProviderConfig(load_all=True))
    await provider.initialize()
    assert {instrument.id.value for instrument in provider.list_all()} == {
        "EURUSDm.MT5",
        "GBPUSDm.MT5",
        "USDJPYm.MT5",
    }
    for symbol in ("EURUSDm", "GBPUSDm", "USDJPYm"):
        assert ("symbol_select", symbol) in venue.calls


@pytest.mark.parametrize(
    "config",
    [InstrumentProviderConfig(), InstrumentProviderConfig(load_ids=frozenset())],
    ids=["no-ids", "empty-ids"],
)
async def test_initializing_a_config_that_names_nothing_to_load_is_refused(venue, config):
    three_symbols(venue)
    provider = provider_of(config)
    with pytest.raises(MT5ConfigError, match="instrument_provider"):
        await provider.initialize()
    assert provider.list_all() == []
    assert venue.calls == []


async def test_each_symbol_is_selected_before_its_definition_is_read(venue):
    venue.add(symbol_info(name="EURUSDm"))
    provider = provider_for("EURUSDm")
    await provider.initialize()
    definition_calls = [call for call in venue.calls if call[1] == "EURUSDm"]
    assert definition_calls[:2] == [("symbol_select", "EURUSDm"), ("symbol_info", "EURUSDm")]


async def test_a_symbol_the_venue_does_not_know_fails_the_load_naming_it(venue):
    venue.add(symbol_info(name="EURUSD"))
    provider = provider_for("EURUSD", "NOSUCH")
    with pytest.raises(MT5SymbolNotFoundError, match="NOSUCH"):
        await provider.initialize()


async def test_loading_ids_loads_the_symbols_named_with_their_broker_casing(venue):
    venue.add(symbol_info(name="XAUUSDm", digits=2, trade_tick_size=0.01))
    provider = provider_for()
    await provider.load_ids_async([InstrumentId.from_str("XAUUSDm.MT5")])
    assert provider.get_instrument("XAUUSDm").id.value == "XAUUSDm.MT5"


async def test_a_provider_of_a_named_venue_builds_and_finds_its_instruments_at_that_venue(venue):
    venue.add(symbol_info(name="XAUUSD", digits=2, trade_tick_size=0.01))
    conn = MagicMock(spec=MT5Connection)
    conn.get_account_info.return_value = AccountSnapshot.from_mt5(account_info())
    clock = TestClock()
    clock.set_time(NOW_NS)
    provider = MT5InstrumentProvider(
        connection=conn,
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
    provider = provider_for("ODDPAIR")
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


async def test_a_pair_prefers_the_fully_tradable_symbol_over_a_disabled_one(venue):
    venue.add(symbol_info(name="EURUSD", trade_mode=mirror.SYMBOL_TRADE_MODE_DISABLED))
    venue.add(symbol_info(name="EURUSD+", trade_mode=mirror.SYMBOL_TRADE_MODE_FULL))
    venue.add(symbol_info(name="GBPUSD+", currency_base="GBP"))
    provider = provider_for("GBPUSD+")
    await provider.initialize()
    assert provider.quoted_pairs()[("EUR", "USD")] == "EURUSD+"


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
    provider = provider_for("EURUSD+", currency="UST")
    await provider.initialize()
    assert provider.quoted_pairs()[("UST", "USD")] == "USTUSD"
    assert ("symbol_select", "USTUSD") in venue.calls


async def test_a_pair_the_run_trades_is_its_own_quote(venue):
    venue.add(symbol_info(name="EURUSD+", trade_mode=mirror.SYMBOL_TRADE_MODE_FULL))
    venue.add(symbol_info(name="EURUSD.a", trade_mode=mirror.SYMBOL_TRADE_MODE_FULL))
    provider = provider_for("EURUSD.a")
    await provider.initialize()
    assert provider.quoted_pairs()[("EUR", "USD")] == "EURUSD.a"


async def test_only_forex_symbols_quote_a_pair(venue):
    venue.add(symbol_info(name="EURUSD"))
    venue.add(
        symbol_info(
            name="XAUUSD",
            currency_base="XAU",
            currency_profit="USD",
            trade_calc_mode=mirror.SYMBOL_CALC_MODE_CFD,
        )
    )
    provider = provider_for("EURUSD")
    await provider.initialize()
    assert set(provider.quoted_pairs()) == {("EUR", "USD")}


def test_the_account_currency_is_the_accounts():
    provider = provider_for("EURUSD", currency="UST")
    assert provider.account_currency() == "UST"


# ── Fees ─────────────────────────────────────────────────────────────────────


async def test_a_relayed_schedule_without_a_rule_is_a_zero_taker_fee(venue):
    instrument = await loaded(venue, symbol_info(name="DE40.a"))
    assert instrument.taker_fee == Decimal(0)


async def test_a_deposit_currency_rule_charged_on_both_legs_is_a_whole_fee_per_fill(venue):
    venue.add(
        symbol_info(name="EURUSD.a", trade_contract_size=100000.0),
        bid=1.08490,
        ask=1.08510,
        schedule=schedule(3.5, "USD", mode=MONEY_DEPOSIT, entry=ENTRY_INOUT),
    )
    provider = provider_for("EURUSD.a")
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
        schedule=schedule(6.0, "UST", mode=MONEY_DEPOSIT, entry=ENTRY_IN),
    )
    provider = provider_for("EURUSD+", currency="UST", currency_digits=2)
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
        schedule=schedule(0.02, "USD", mode=MONEY_SPECIFIED, entry=ENTRY_IN),
    )
    provider = provider_for("AAPL")
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
        schedule=schedule(5.0, "USD", mode=MONEY_DEPOSIT, entry=ENTRY_INOUT),
    )
    provider = provider_for("DE40")
    await provider.initialize()
    fee = provider.get_instrument("DE40").taker_fee
    assert abs(fee - Decimal(5) / Decimal("1.25") / (Decimal(1) * Decimal(20000))) < Decimal(
        "1e-18"
    )


async def test_a_conversion_symbol_the_venue_refuses_to_select_fails_the_load_naming_it(venue):
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
        schedule=schedule(5.0, "USD", mode=MONEY_DEPOSIT, entry=ENTRY_INOUT),
    )
    venue.unselectable.add("EURUSD")
    provider = provider_for("DE40")
    with pytest.raises(MT5InstrumentError, match="EURUSD") as refused:
        await provider.initialize()
    assert "DE40" in str(refused.value)
    assert provider.list_all() == []


async def test_a_rule_in_a_currency_the_venue_cannot_convert_fails_the_load_naming_it(venue):
    venue.add(
        symbol_info(name="EURUSD.a"),
        bid=1.085,
        ask=1.085,
        schedule=schedule(3.5, "GBP", mode=MONEY_DEPOSIT, entry=ENTRY_INOUT),
    )
    provider = provider_for("EURUSD.a")
    with pytest.raises(MT5InstrumentError, match="EURUSD.a") as refused:
        await provider.initialize()
    assert "GBP" in str(refused.value)


async def test_a_percent_rule_charged_on_both_legs_is_its_percentage_of_the_notional(venue):
    venue.add(
        symbol_info(name="EURUSD.a"),
        bid=1.08490,
        ask=1.08510,
        schedule=schedule(0.002, "", mode=PERCENT, entry=ENTRY_INOUT),
    )
    provider = provider_for("EURUSD.a")
    await provider.initialize()
    fee = provider.get_instrument("EURUSD.a").taker_fee
    pct = Decimal("0.002")
    assert fee == pct / 100


async def test_a_percent_rule_charged_on_entry_alone_is_halved(venue):
    venue.add(
        symbol_info(name="EURUSD.a"),
        bid=1.085,
        ask=1.085,
        schedule=schedule(0.004, "", mode=PERCENT, entry=ENTRY_IN),
    )
    provider = provider_for("EURUSD.a")
    await provider.initialize()
    fee = provider.get_instrument("EURUSD.a").taker_fee
    pct = Decimal("0.004")
    assert fee == pct / 100 / 2


async def test_a_points_rule_is_its_points_in_price_over_the_price(venue):
    venue.add(
        symbol_info(name="EURUSD.a", point=0.00001),
        bid=1.08490,
        ask=1.08510,
        schedule=schedule(30.0, "", mode=POINTS, entry=ENTRY_INOUT),
    )
    provider = provider_for("EURUSD.a")
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
        schedule=schedule(30.0, "", mode=POINTS, entry=ENTRY_IN),
    )
    provider = provider_for("EURUSD.a")
    await provider.initialize()
    fee = provider.get_instrument("EURUSD.a").taker_fee
    assert abs(fee - Decimal(30) * Decimal("0.00001") / Decimal("1.085") / 2) < Decimal("1e-18")


@pytest.mark.parametrize(
    ("mode", "entry", "named"),
    [
        ("SYMBOL_COMMISSION_BOGUS", ENTRY_INOUT, "SYMBOL_COMMISSION_BOGUS"),
        (PERCENT_PROFIT, ENTRY_INOUT, PERCENT_PROFIT),
        (MONEY_DEPOSIT, ENTRY_OUT, ENTRY_OUT),
        (MONEY_DEPOSIT, "SYMBOL_COMMISSION_ENTRY_BOGUS", "SYMBOL_COMMISSION_ENTRY_BOGUS"),
    ],
    ids=["unknown-mode", "mode-without-derivation", "exit-only", "unknown-entry"],
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
    provider = provider_for("EURUSD.a")
    with pytest.raises(MT5InstrumentError, match="EURUSD.a") as refused:
        await provider.initialize()
    assert named in str(refused.value)


async def test_a_non_finite_commission_value_fails_the_load_naming_the_symbol(venue):
    venue.add(
        symbol_info(name="EURUSD.a"),
        bid=1.085,
        ask=1.085,
        schedule=schedule(float("nan"), "USD", mode=MONEY_DEPOSIT, entry=ENTRY_INOUT),
    )
    provider = provider_for("EURUSD.a")
    with pytest.raises(MT5InstrumentError, match="EURUSD.a"):
        await provider.initialize()


async def test_a_non_finite_quote_fails_the_load_naming_the_symbol(venue):
    venue.add(
        symbol_info(name="EURUSD.a"),
        bid=float("nan"),
        ask=1.085,
        schedule=schedule(3.5, "USD", mode=MONEY_DEPOSIT, entry=ENTRY_INOUT),
    )
    provider = provider_for("EURUSD.a")
    with pytest.raises(MT5InstrumentError, match="EURUSD.a"):
        await provider.initialize()


async def test_a_reload_recomputes_the_fee_from_the_current_schedule(venue):
    venue.add(symbol_info(name="EURUSD.a"), bid=1.085, ask=1.085)
    provider = provider_for("EURUSD.a")
    await provider.initialize()
    assert provider.get_instrument("EURUSD.a").taker_fee == Decimal(0)
    venue.schedules["EURUSD.a"] = schedule(3.5, "USD", mode=MONEY_DEPOSIT, entry=ENTRY_INOUT)
    await provider.initialize(reload=True)
    fee = provider.get_instrument("EURUSD.a").taker_fee
    assert fee.quantize(Decimal("0.0000001")) == Decimal("0.0000323")


# ── Provider mechanics ───────────────────────────────────────────────────────


async def test_every_load_refuses_a_lost_connection(venue):
    venue.add(symbol_info(name="EURUSD"))
    provider = provider_for("EURUSD")
    provider._conn.ensure_connected.side_effect = MT5ConnectionError("not connected")
    with pytest.raises(MT5ConnectionError):
        await provider.load_all_async()
    with pytest.raises(MT5ConnectionError):
        await provider.load_ids_async([InstrumentId.from_str("EURUSD.MT5")])
    with pytest.raises(MT5ConnectionError):
        provider.load_symbol("EURUSD")


def test_loading_one_symbol_answers_and_holds_its_instrument(venue):
    venue.add(symbol_info(name="EURUSDm"))
    provider = provider_for()
    instrument = provider.load_symbol("EURUSDm")
    assert instrument.id.value == "EURUSDm.MT5"
    assert provider.get_instrument("EURUSDm") is instrument
    assert provider.get_instrument("EURUSDM") is None


def test_a_symbol_whose_definition_reads_as_none_is_not_found(venue):
    venue.add(symbol_info(name="EURUSD"))
    provider = provider_for()
    with patch("mt5connector.client.providers.mt5.symbol_info", return_value=None):
        with pytest.raises(MT5SymbolNotFoundError, match="EURUSD"):
            provider.load_symbol("EURUSD")


def test_a_failed_load_adds_none_of_its_symbols(venue):
    venue.add(symbol_info(name="EURUSD"))
    venue.add(symbol_info(name="ODDPAIR", currency_profit="XYZ"))
    provider = provider_for()
    with pytest.raises(MT5InstrumentError):
        provider._load(["EURUSD", "ODDPAIR"])
    assert provider.list_all() == []


def test_loads_accumulate_and_a_reload_replaces_the_instrument(venue):
    venue.add(symbol_info(name="EURUSD"))
    venue.add(symbol_info(name="GBPUSD", currency_base="GBP"))
    provider = provider_for()
    provider.load_symbol("EURUSD")
    provider.load_symbol("GBPUSD")
    provider.load_symbol("EURUSD")
    assert sorted(instrument.id.value for instrument in provider.list_all()) == [
        "EURUSD.MT5",
        "GBPUSD.MT5",
    ]


def test_a_venue_listing_that_reads_as_none_fails_the_load(venue):
    venue.add(symbol_info(name="EURUSD"))
    provider = provider_for()
    with patch("mt5connector.client.providers.mt5.symbols_get", return_value=None):
        with pytest.raises(MT5ConnectionError, match="symbols_get"):
            provider.load_symbol("EURUSD")
