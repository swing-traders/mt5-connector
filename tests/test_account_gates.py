"""The account the execution client connects to: its snapshot as the connection reads it, the gates
the client holds it to before anything is reported or sent, the identity it books under, the
currencies it registers, and the magic that marks its orders."""

import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from nautilus_trader.cache.transformers import transform_instrument_to_pyo3
from nautilus_trader.common.component import LiveClock, MessageBus
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.core import nautilus_pyo3
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.messages import GeneratePositionStatusReports
from nautilus_trader.model.enums import CurrencyType, OmsType
from nautilus_trader.model.identifiers import AccountId, InstrumentId, PositionId, Symbol, TraderId
from nautilus_trader.model.instruments import CurrencyPair
from nautilus_trader.model.objects import Currency, Price, Quantity
from nautilus_trader.test_kit.stubs.component import TestComponentStubs
from venue_doubles import account_info, symbol_info, trade_position

from mt5connect import connection, execution, mirror
from mt5connect.config import MT5Config
from mt5connect.connection import AccountSnapshot, MT5Connection
from mt5connect.errors import MT5ConfigError, MT5ConnectionError, MT5InstrumentError
from mt5connect.execution import MT5LiveExecutionClient
from mt5connect.providers import MT5InstrumentProvider


def _config() -> MT5Config:
    return MT5Config(
        account=12345678,
        password="p",
        server="Broker-Demo",
        symbols=["EURUSD"],
        server_url="http://127.0.0.1:5000",
    )


def _pair(symbol: str, base: Currency, quote: Currency) -> CurrencyPair:
    return CurrencyPair(
        instrument_id=InstrumentId.from_str(f"{symbol}.MT5"),
        raw_symbol=Symbol(symbol),
        base_currency=base,
        quote_currency=quote,
        price_precision=5,
        size_precision=2,
        price_increment=Price.from_str("0.00001"),
        size_increment=Quantity.from_str("0.01"),
        ts_event=0,
        ts_init=0,
    )


def _provider(instruments) -> MT5InstrumentProvider:
    provider = MagicMock(spec=MT5InstrumentProvider)
    provider.initialize = AsyncMock()
    provider.list_all.return_value = list(instruments)
    return provider


def _client(
    raw_account,
    terminal_trade_allowed=True,
    instruments=(),
    trader_id="TESTER-001",
) -> MT5LiveExecutionClient:
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
    conn = MagicMock(spec=MT5Connection)
    conn.get_account_info.return_value = AccountSnapshot.from_mt5(raw_account)
    conn.get_terminal_info.return_value = {
        "connected": True,
        "trade_allowed": terminal_trade_allowed,
    }
    clock = LiveClock()
    client = MT5LiveExecutionClient(
        loop=loop,
        connection=conn,
        msgbus=MessageBus(trader_id=TraderId(trader_id), clock=clock),
        cache=TestComponentStubs.cache(),
        clock=clock,
        instrument_provider=_provider(instruments),
        config=_config(),
    )
    client.generate_account_state = MagicMock()
    return client


@pytest.fixture
def venue():
    with patch("mt5connect.execution.mt5") as package:
        package.orders_get.return_value = ()
        package.positions_get.return_value = ()
        package.history_deals_get.return_value = ()
        yield package


async def _stop_polling(client):
    if client._exec_poll_task is not None:
        client._exec_poll_task.cancel()
        try:
            await client._exec_poll_task
        except asyncio.CancelledError:
            pass


# ── The snapshot ─────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("raw", "member"),
    [
        (mirror.ACCOUNT_MARGIN_MODE_RETAIL_NETTING, "RETAIL_NETTING"),
        (mirror.ACCOUNT_MARGIN_MODE_EXCHANGE, "EXCHANGE"),
        (mirror.ACCOUNT_MARGIN_MODE_RETAIL_HEDGING, "RETAIL_HEDGING"),
    ],
)
def test_the_snapshot_carries_the_margin_mode_as_this_repos_member(raw, member):
    snapshot = AccountSnapshot.from_mt5(account_info(margin_mode=raw))
    assert snapshot.margin_mode is connection.MarginMode[member]


def test_the_snapshot_carries_trading_permission_currency_digits_and_credit():
    snapshot = AccountSnapshot.from_mt5(
        account_info(trade_allowed=False, currency_digits=3, credit=12.5)
    )
    assert snapshot.trade_allowed is False
    assert snapshot.currency_digits == 3
    assert snapshot.credit == Decimal("12.5")


def test_the_snapshots_leverage_is_decimal():
    snapshot = AccountSnapshot.from_mt5(account_info(leverage=500))
    assert snapshot.leverage == Decimal(500)
    assert isinstance(snapshot.leverage, Decimal)


@pytest.mark.parametrize(
    "field", ["balance", "equity", "margin", "margin_free", "margin_level", "credit", "profit"]
)
def test_a_non_finite_account_amount_is_refused_naming_it(field):
    with pytest.raises(MT5ConnectionError, match=field):
        AccountSnapshot.from_mt5(account_info(**{field: float("nan")}))


def test_the_snapshots_money_is_decimal_from_the_boundary_on():
    snapshot = AccountSnapshot.from_mt5(
        account_info(
            balance=10000.0, equity=10050.25, margin=100.0, margin_free=9950.25, profit=50.25
        )
    )
    assert snapshot.balance == Decimal("10000.0")
    assert snapshot.equity == Decimal("10050.25")
    assert snapshot.margin == Decimal("100.0")
    assert snapshot.margin_free == Decimal("9950.25")
    assert snapshot.credit == Decimal("0.0")
    assert snapshot.profit == Decimal("50.25")
    money = (
        snapshot.balance,
        snapshot.equity,
        snapshot.margin,
        snapshot.margin_free,
        snapshot.credit,
        snapshot.profit,
    )
    assert all(isinstance(value, Decimal) for value in money)


def test_the_terminal_info_exposes_trading_permission_and_connection(config, mock_mt5):
    mock_mt5.terminal_info.return_value.trade_allowed = True
    mock_mt5.terminal_info.return_value.connected = True
    conn = MT5Connection(config)
    conn.connect()
    info = conn.get_terminal_info()
    assert info["trade_allowed"] is True
    assert info["connected"] is True


# ── The position model ───────────────────────────────────────────────────────


def test_the_client_declares_hedging():
    client = _client(account_info())
    assert client.oms_type == OmsType.HEDGING


@pytest.mark.parametrize(
    ("raw", "named"),
    [
        (mirror.ACCOUNT_MARGIN_MODE_RETAIL_NETTING, "RETAIL_NETTING"),
        (mirror.ACCOUNT_MARGIN_MODE_EXCHANGE, "EXCHANGE"),
    ],
)
async def test_a_non_hedging_account_fails_the_connect_naming_both_modes_before_anything_runs(
    venue, raw, named
):
    client = _client(account_info(margin_mode=raw))
    with pytest.raises(MT5ConfigError) as refused:
        await client._connect()
    assert "HEDGING" in str(refused.value)
    assert named in str(refused.value)
    client.generate_account_state.assert_not_called()
    venue.orders_get.assert_not_called()
    venue.positions_get.assert_not_called()
    venue.order_send.assert_not_called()
    assert client._exec_poll_task is None


async def test_a_hedging_account_connects(venue):
    client = _client(account_info(margin_mode=mirror.ACCOUNT_MARGIN_MODE_RETAIL_HEDGING))
    await client._connect()
    client.generate_account_state.assert_called_once()
    assert not client._exec_poll_task.done()
    await _stop_polling(client)


# ── The investor gate ────────────────────────────────────────────────────────


async def test_a_read_only_session_fails_the_connect_before_anything_runs(venue):
    client = _client(account_info(trade_allowed=False), terminal_trade_allowed=True)
    with pytest.raises(MT5ConfigError, match="read-only"):
        await client._connect()
    client.generate_account_state.assert_not_called()
    venue.orders_get.assert_not_called()
    venue.positions_get.assert_not_called()
    venue.order_send.assert_not_called()
    assert client._exec_poll_task is None


async def test_a_session_the_account_and_terminal_both_allow_to_trade_connects(venue):
    client = _client(account_info(trade_allowed=True), terminal_trade_allowed=True)
    await client._connect()
    assert not client._exec_poll_task.done()
    await _stop_polling(client)


# ── The account id ───────────────────────────────────────────────────────────


async def test_the_account_id_is_the_venue_and_the_login_the_account_reports(venue):
    client = _client(account_info(login=7654321))
    await client._connect()
    assert client.account_id == AccountId("MT5-7654321")
    await _stop_polling(client)


# ── Currency registration ────────────────────────────────────────────────────


async def test_a_pre_minted_account_currency_is_re_registered_at_the_accounts_digits(venue):
    Currency.register(Currency("UST", 8, 0, "UST", CurrencyType.CRYPTO), overwrite=True)
    client = _client(account_info(currency="UST", currency_digits=2))
    await client._connect()
    assert Currency.from_str("UST", strict=True).precision == 2
    assert nautilus_pyo3.Currency.from_str("UST", strict=True).precision == 2
    client.generate_account_state.assert_called_once()
    await _stop_polling(client)


async def test_the_settlement_currency_registers_and_a_base_only_code_does_not(venue):
    settlement = Currency("CLP", 0, 0, "CLP", CurrencyType.FIAT)
    base_only = Currency("BHD", 3, 0, "BHD", CurrencyType.FIAT)
    client = _client(account_info(), instruments=[_pair("BHDCLP", base_only, settlement)])
    await client._connect()
    assert Currency.from_str("CLP", strict=True).precision == 0
    assert Currency.from_internal_map("BHD") is None
    await _stop_polling(client)


@pytest.fixture
def settlement_venue(venue):
    venue.symbol_select.return_value = True
    venue.commission_schedule.return_value = None
    venue.symbol_info.return_value = symbol_info(
        currency_base="XPT", currency_profit="UST", currency_margin="BHD"
    )
    venue.symbols_get.side_effect = lambda: (venue.symbol_info.return_value,)
    with patch("mt5connect.providers.mt5", venue):
        yield venue


async def test_sequential_accounts_use_their_own_digits_in_money_and_instruments(settlement_venue):
    seen = []
    for digits in (8, 2):
        client = _client(account_info(currency="UST", currency_digits=digits))
        client._provider = MT5InstrumentProvider(connection=client._conn, clock=LiveClock())
        try:
            await client._connect()
            instrument = client._provider.get_instrument("EURUSD")
            balance = client.generate_account_state.call_args.kwargs["balances"][0]
            seen.append(
                (
                    balance.total.currency.precision,
                    instrument.quote_currency.precision,
                    transform_instrument_to_pyo3(instrument).quote_currency.precision,
                    Currency.from_str("UST", strict=True).precision,
                    nautilus_pyo3.Currency.from_str("UST", strict=True).precision,
                )
            )
            assert instrument.base_currency.precision == 2
        finally:
            await _stop_polling(client)
    assert seen == [(8, 8, 8, 8, 8), (2, 2, 2, 2, 2)]


@pytest.mark.parametrize(("code", "precision"), [("XYZ", None), ("CLP", 0), ("KWD", 3)])
async def test_an_earlier_account_does_not_define_a_later_accounts_settlement(
    settlement_venue, code, precision
):
    first = _client(account_info(currency=code, currency_digits=8))
    try:
        await first._connect()
    finally:
        await _stop_polling(first)

    settlement_venue.symbol_info.return_value = symbol_info(currency_profit=code)
    later = _client(account_info(currency="USD"))
    later._provider = MT5InstrumentProvider(connection=later._conn, clock=LiveClock())
    try:
        if precision is None:
            with pytest.raises(MT5InstrumentError, match=f"EURUSD: currency {code}"):
                await later._connect()
            later.generate_account_state.assert_not_called()
        else:
            await later._connect()
            instrument = later._provider.get_instrument("EURUSD")
            assert instrument.quote_currency.precision == precision
            assert Currency.from_str(code, strict=True).precision == precision
            assert nautilus_pyo3.Currency.from_str(code, strict=True).precision == precision
    finally:
        await _stop_polling(later)


# ── The magic ────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("trader_id", "magic"),
    [
        ("TRADER-001", 3181061856910866440),
        # The digest's top bit is set, so the mask decides this one.
        ("TRADER-002", 2268196824564769654),
    ],
)
def test_the_magic_is_the_trader_ids_digest(trader_id, magic):
    assert execution.magic_for(TraderId(trader_id)) == magic
    assert magic < 2**63


def test_two_trader_ids_give_two_magics():
    assert execution.magic_for(TraderId("ALPHA-001")) != execution.magic_for(TraderId("BETA-001"))


async def test_the_client_owns_exactly_the_positions_its_trader_ids_magic_marks(venue):
    instrument = _pair("EURUSD", Currency.from_str("EUR"), Currency.from_str("USD"))
    client = _client(account_info(), instruments=[instrument], trader_id="TRADER-001")
    client._provider.get_instrument.side_effect = {"EURUSD": instrument}.get
    await client._connect()
    await _stop_polling(client)
    venue.positions_get.return_value = (
        trade_position(identifier=1, magic=3181061856910866440),
        trade_position(identifier=2, magic=2268196824564769654),
    )
    reports = await client.generate_position_status_reports(
        GeneratePositionStatusReports(
            instrument_id=None, start=None, end=None, command_id=UUID4(), ts_init=0
        )
    )
    assert [report.venue_position_id for report in reports] == [PositionId("1")]


async def test_the_currencies_register_before_the_first_account_state(venue):
    Currency.register(Currency("UST", 8, 0, "UST", CurrencyType.CRYPTO), overwrite=True)
    client = _client(account_info(currency="UST", currency_digits=2))
    seen = []
    client.generate_account_state = MagicMock(
        side_effect=lambda **_: seen.append(Currency.from_str("UST", strict=True).precision)
    )
    await client._connect()
    assert seen == [2]
    await _stop_polling(client)


async def test_each_client_loads_its_own_configs_symbols_from_a_shared_provider(venue):
    definitions = {
        "EURUSD": symbol_info(name="EURUSD"),
        "GBPUSD": symbol_info(name="GBPUSD", currency_base="GBP"),
    }
    package = MagicMock()
    package.symbol_select.return_value = True
    package.symbol_info.side_effect = definitions.get
    package.symbols_get.return_value = tuple(definitions.values())
    package.commission_schedule.return_value = None
    conn = MagicMock(spec=MT5Connection)
    conn.get_account_info.return_value = AccountSnapshot.from_mt5(account_info())
    conn.get_terminal_info.return_value = {"connected": True, "trade_allowed": True}
    clock = LiveClock()
    provider = MT5InstrumentProvider(
        connection=conn,
        clock=clock,
        config=InstrumentProviderConfig(load_ids=frozenset({InstrumentId.from_str("EURUSD.MT5")})),
    )
    config = MT5Config(
        account=12345678,
        password="p",
        server="Broker-Demo",
        symbols=["GBPUSD"],
        server_url="http://127.0.0.1:5000",
    )
    client = MT5LiveExecutionClient(
        loop=asyncio.get_running_loop(),
        connection=conn,
        msgbus=MessageBus(trader_id=TraderId("TESTER-001"), clock=clock),
        cache=TestComponentStubs.cache(),
        clock=clock,
        instrument_provider=provider,
        config=config,
    )
    client.generate_account_state = MagicMock()
    with patch("mt5connect.providers.mt5", package):
        await client._connect()
    assert provider.get_instrument("GBPUSD") is not None
    await _stop_polling(client)


# ── The lifecycle ────────────────────────────────────────────────────────────


async def test_a_connect_on_a_connected_client_is_refused(venue):
    client = _client(account_info())
    await client._connect()
    try:
        with pytest.raises(RuntimeError, match="already connected"):
            await client._connect()
    finally:
        await _stop_polling(client)


async def test_a_disconnect_on_a_client_never_connected_is_refused(venue):
    client = _client(account_info())
    with pytest.raises(RuntimeError, match="not connected"):
        await client._disconnect()
