"""The data client's connection, its subscriptions and its history requests; its pushed frames are
tests/client/test_data_push.py's."""

import asyncio
import threading
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import ANY, AsyncMock, MagicMock, patch

import numpy as np
import pytest
from nautilus_trader.common.component import LiveClock
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.data.messages import DataResponse, RequestInstrument, RequestInstruments
from nautilus_trader.model.identifiers import ClientId, InstrumentId, Venue
from nautilus_trader.model.instruments import CurrencyPair
from nautilus_trader.model.objects import Price, Quantity
from nautilus_trader.test_kit.stubs.component import TestComponentStubs
from push_double import PushDouble
from venue_doubles import account_info, symbol_info

from mt5connector.client import data
from mt5connector.client.connection import AccountSnapshot, MT5Connection
from mt5connector.client.constants import MT5_VENUE
from mt5connector.client.data import MT5DataClient, _epoch_s
from mt5connector.client.errors import MT5ConfigError, MT5ConnectionError, MT5InstrumentError
from mt5connector.client.providers import MT5InstrumentProvider
from mt5connector.wire import mirror
from mt5connector.wire.history_wire import Series
from mt5connector.wire.push_wire import Stream, Subscription

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────


def make_instrument(symbol="EURUSDm"):
    from nautilus_trader.model.currencies import Currency
    from nautilus_trader.model.identifiers import Symbol

    return CurrencyPair(
        instrument_id=InstrumentId.from_str(f"{symbol}.MT5"),
        raw_symbol=Symbol(symbol),
        base_currency=Currency.from_str("EUR"),
        quote_currency=Currency.from_str("USD"),
        price_precision=5,
        size_precision=2,
        price_increment=Price(0.00001, 5),
        size_increment=Quantity(0.01, 2),
        lot_size=Quantity(100000, 0),
        max_quantity=Quantity(1000.0, 2),
        min_quantity=Quantity(0.01, 2),
        max_notional=None,
        min_notional=None,
        max_price=None,
        min_price=None,
        margin_init=Decimal("0.03"),
        margin_maint=Decimal("0.03"),
        maker_fee=Decimal("0"),
        taker_fee=Decimal("0"),
        ts_event=0,
        ts_init=0,
    )


def make_raw_rate(
    time_s=1_700_000_000, open_=1.085, high=1.090, low=1.080, close=1.088, tick_volume=1000
):
    dtype = np.dtype(
        [
            ("time", np.int64),
            ("open", np.float64),
            ("high", np.float64),
            ("low", np.float64),
            ("close", np.float64),
            ("tick_volume", np.int64),
            ("spread", np.int32),
            ("real_volume", np.int64),
        ]
    )
    arr = np.array([(time_s, open_, high, low, close, tick_volume, 2, 0)], dtype=dtype)
    return arr[0]


def make_config(ws_url=None, venue=MT5_VENUE):
    from mt5connector.client.config import MT5Config

    return MT5Config(
        account=12345678,
        password="test",
        server="Exness-MT5Trial1",
        server_url="http://127.0.0.1:5000",
        ws_url=ws_url,
        venue=venue,
        reconnect_initial_delay_s=0.01,
        reconnect_max_delay_s=0.05,
        reconnect_max_attempts=2,
    )


def make_conn(connected=True):
    conn = MagicMock()
    conn.ensure_connected = (
        MagicMock() if connected else MagicMock(side_effect=MT5ConnectionError("not connected"))
    )
    conn.reconnect_async = AsyncMock(return_value=connected)
    return conn


def make_provider(instrument=None):
    """A real MT5InstrumentProvider, whose type NT checks, its config naming `instrument`, answering
    it without the venue."""
    conn = MagicMock(spec=MT5Connection)
    conn.ensure_connected = MagicMock()

    inst = instrument or make_instrument("EURUSDm")

    provider = MT5InstrumentProvider.__new__(MT5InstrumentProvider)
    from nautilus_trader.common.providers import InstrumentProvider

    InstrumentProvider.__init__(provider, InstrumentProviderConfig(load_ids=frozenset({inst.id})))
    provider._conn = conn
    provider._venue = MT5_VENUE
    provider._failed_symbols = []

    provider.get_instrument = MagicMock(return_value=inst)
    provider.load_symbol = MagicMock(return_value=inst)
    provider.list_all = MagicMock(return_value=[inst])
    provider.load_ids_async = AsyncMock()
    provider.load_all_async = AsyncMock()

    return provider


class RecordedDataClient(MT5DataClient):
    """The data client with its log calls recorded."""

    def __init__(self, *args, **kwargs):
        self.recorded_log = MagicMock()
        super().__init__(*args, **kwargs)

    @property
    def _log(self):
        return self.recorded_log


def make_client(
    connected=True,
    instrument=None,
    client_class=MT5DataClient,
    nt_handlers=False,
    ws_url=None,
    venue=MT5_VENUE,
):
    """Build a fully wired MT5DataClient with real NautilusTrader components, its handlers recorded
    unless `nt_handlers` keeps NT's own."""
    from nautilus_trader.common.component import LiveClock
    from nautilus_trader.test_kit.stubs.component import TestComponentStubs

    # Use the running event loop (pytest-asyncio's loop) to avoid cross-loop errors
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()

    config = make_config(ws_url=ws_url, venue=venue)
    conn = make_conn(connected)
    provider = make_provider(instrument)

    # NautilusTrader requires real component types — MagicMock fails PyCondition checks
    msgbus = TestComponentStubs.msgbus()
    cache = TestComponentStubs.cache()
    clock = LiveClock()

    with patch.object(data, "PushClient", PushDouble):
        client = client_class(
            loop=loop,
            connection=conn,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=provider,
            config=config,
        )
    if not nt_handlers:
        client._handle_data = MagicMock()
        client._handle_quote_ticks = MagicMock()
        client._handle_bars = MagicMock()
        client._handle_instrument = MagicMock()
        client._handle_instruments = MagicMock()

    return client, conn, provider, loop


@pytest.fixture
def client():
    c, conn, prov, loop = make_client()
    yield c
    loop.close()


@pytest.fixture
def client_with_loop():
    c, conn, prov, loop = make_client()
    yield c, conn, prov, loop
    loop.close()


# ═════════════════════════════════════════════════════════════════════════════
# 1. Helpers
# ═════════════════════════════════════════════════════════════════════════════


class TestEpochS:

    def test_nanoseconds_become_seconds(self):
        assert _epoch_s(1_700_000_000_500_000_000) == 1_700_000_000

    def test_a_datetime_becomes_its_epoch_seconds(self):
        assert _epoch_s(datetime(2023, 11, 14, 22, 13, 20, tzinfo=UTC)) == 1_700_000_000


# ═════════════════════════════════════════════════════════════════════════════
# 2. Initial state
# ═════════════════════════════════════════════════════════════════════════════


class TestInitialState:

    def test_client_id_is_mt5(self, client):
        assert (client.id, client.venue) == (ClientId("MT5"), Venue("MT5"))

    def test_the_client_id_and_venue_are_the_configs_venue(self):
        client, conn, provider, loop = make_client(venue=Venue("MT5_ALPHA"))
        assert (client.id, client.venue) == (ClientId("MT5_ALPHA"), Venue("MT5_ALPHA"))
        loop.close()

    def test_no_subscribed_ticks_initially(self, client):
        assert client.subscribed_quote_ticks() == []


# ═════════════════════════════════════════════════════════════════════════════
# 3. _connect()
# ═════════════════════════════════════════════════════════════════════════════


class TestConnect:

    @pytest.mark.asyncio
    async def test_connect_checks_connection(self):
        c, conn, prov, loop = make_client()
        await c._connect()
        conn.ensure_connected.assert_called()
        await c._disconnect()

    @pytest.mark.asyncio
    async def test_connect_loads_the_ids_its_provider_config_names(self):
        c, conn, prov, loop = make_client()
        await c._connect()
        assert prov.load_ids_async.await_args.args[0] == [InstrumentId.from_str("EURUSDm.MT5")]
        await c._disconnect()

    @pytest.mark.asyncio
    async def test_connect_logs_the_push_channel_without_its_credentials(self):
        c, conn, prov, loop = make_client(
            client_class=RecordedDataClient,
            ws_url="ws://hub:SYNTHETIC_SECRET@127.0.0.1:9000/push",
        )
        await c._connect()
        await c._disconnect()
        assert not any("SYNTHETIC_SECRET" in str(call) for call in c.recorded_log.mock_calls)
        c.recorded_log.info.assert_any_call(
            "MT5DataClient: connected, the push channel at ws://127.0.0.1:9000/push"
        )

    @pytest.mark.asyncio
    async def test_connect_registers_the_settlement_currency_of_each_instrument_it_hands(self):
        from nautilus_trader.model.enums import CurrencyType
        from nautilus_trader.model.objects import Currency

        Currency.register(Currency("CLP", 8, 0, "CLP", CurrencyType.FIAT), overwrite=True)
        clp = Currency("CLP", 0, 0, "CLP", CurrencyType.FIAT)
        c, conn, prov, loop = make_client(instrument=clp_instrument(clp))
        precisions = []
        c._handle_data.side_effect = lambda *_: precisions.append(
            Currency.from_str("CLP", strict=True).precision
        )

        await c._connect()

        assert precisions == [0]
        await c._disconnect()

    @pytest.mark.asyncio
    async def test_connect_emits_instruments(self):
        c, conn, prov, loop = make_client()
        await c._connect()
        c._handle_data.assert_called()
        assert c._push.connected
        await c._disconnect()


# ═════════════════════════════════════════════════════════════════════════════
# 4. _disconnect()
# ═════════════════════════════════════════════════════════════════════════════


class TestDisconnect:

    @pytest.mark.asyncio
    async def test_disconnect_closes_the_push_channel_and_keeps_what_is_wanted(self):
        c, conn, prov, loop = make_client()
        await c._connect()
        cmd = MagicMock()
        cmd.instrument_id.symbol.value = "EURUSDm"
        await c._subscribe_quote_ticks(cmd)
        await c._disconnect()
        assert not c._push.connected
        assert c._push.wanted == [Subscription(Stream.TICKS, "EURUSDm")]


# ═════════════════════════════════════════════════════════════════════════════
# 5. Subscribe / unsubscribe quote ticks
# ═════════════════════════════════════════════════════════════════════════════


class TestSubscribeQuoteTicks:

    @pytest.mark.asyncio
    async def test_subscribe_wants_the_symbols_ticks(self, client):
        cmd = MagicMock()
        cmd.instrument_id.symbol.value = "EURUSDm"
        await client._subscribe_quote_ticks(cmd)
        assert client._push.wanted == [Subscription(Stream.TICKS, "EURUSDm")]

    @pytest.mark.asyncio
    async def test_subscribe_multiple_symbols(self, client):
        for sym in ["EURUSDm", "XAUUSDm", "BTCUSDm"]:
            cmd = MagicMock()
            cmd.instrument_id.symbol.value = sym
            await client._subscribe_quote_ticks(cmd)
        assert client._push.wanted == [
            Subscription(Stream.TICKS, sym) for sym in ["EURUSDm", "XAUUSDm", "BTCUSDm"]
        ]

    @pytest.mark.asyncio
    async def test_unsubscribe_stops_wanting_the_symbols_ticks(self, client):
        cmd = MagicMock()
        cmd.instrument_id.symbol.value = "EURUSDm"
        await client._subscribe_quote_ticks(cmd)
        await client._unsubscribe_quote_ticks(cmd)
        assert client._push.wanted == []

    @pytest.mark.asyncio
    async def test_unsubscribe_non_subscribed_does_not_raise(self, client):
        cmd = MagicMock()
        cmd.instrument_id.symbol.value = "FAKESYM"
        await client._unsubscribe_quote_ticks(cmd)


# ═════════════════════════════════════════════════════════════════════════════
# 7. _request_quote_ticks()
# ═════════════════════════════════════════════════════════════════════════════


RATES = np.dtype(list(mirror.RATES.dtype))
TICKS = np.dtype(list(mirror.TICKS.dtype))


def rate_rows(*opens):
    return np.array([(t, 1.085, 1.09, 1.08, 1.088, 1000, 2, 0) for t in opens], dtype=RATES)


def tick_rows(*times_msc):
    return np.array([(t // 1000, 1.085, 1.0852, 0.0, 0, t, 6, 0.0) for t in times_msc], dtype=TICKS)


def minute_bars_request(first_close, last_close):
    from nautilus_trader.model.data import BarType

    request = MagicMock()
    request.bar_type = BarType.from_str("EURUSDm.MT5-1-MINUTE-LAST-EXTERNAL")
    request.start = first_close * 1_000_000_000
    request.end = last_close * 1_000_000_000
    request.id = "req-bars"
    return request


def ticks_request(start, end):
    request = MagicMock()
    request.instrument_id.symbol.value = "EURUSDm"
    request.start = start * 1_000_000_000
    request.end = end * 1_000_000_000
    request.id = "req-ticks"
    return request


@pytest.fixture
def recorded():
    c, conn, prov, loop = make_client(client_class=RecordedDataClient)
    yield c
    loop.close()


class TestRequestQuoteTicks:

    @pytest.mark.asyncio
    async def test_asks_the_server_for_the_window_and_delivers_its_ticks(self, recorded):
        rows = tick_rows(*((1_752_570_000 + i) * 1000 + 250 for i in range(10)))
        with patch("mt5connector.client.history.ticks", return_value=rows) as ticks:
            await recorded._request_quote_ticks(ticks_request(1_752_570_000, 1_752_573_600))

        ticks.assert_called_once_with("EURUSDm", 1_752_570_000, 1_752_573_600, cancel=ANY)
        delivered = recorded._handle_quote_ticks.call_args[0][1]
        assert [tick.ts_event for tick in delivered] == [
            ((1_752_570_000 + i) * 1000 + 250) * 1_000_000 for i in range(10)
        ]
        assert recorded._log.warning.call_count == 0

    @pytest.mark.asyncio
    async def test_an_empty_answer_completes_the_request_with_nothing_and_no_warning(
        self, recorded
    ):
        with patch("mt5connector.client.history.ticks", return_value=tick_rows()):
            await recorded._request_quote_ticks(ticks_request(1_752_570_000, 1_752_573_600))

        assert recorded._handle_quote_ticks.call_args[0][1] == []
        assert recorded._log.warning.call_count == 0

    @pytest.mark.asyncio
    async def test_a_failed_window_is_an_error_and_delivers_nothing(self, recorded):
        with patch("mt5connector.client.history.ticks", return_value=None):
            await recorded._request_quote_ticks(ticks_request(1_752_570_000, 1_752_573_600))

        recorded._handle_quote_ticks.assert_not_called()
        assert recorded._log.error.call_count == 1
        assert "2025-07-15T09:00:00+00:00" in recorded._log.error.call_args[0][0]

    @pytest.mark.asyncio
    async def test_both_sizes_are_the_instruments_largest_order(self, recorded):
        rows = tick_rows(1_752_570_000_250)
        with patch("mt5connector.client.history.ticks", return_value=rows):
            await recorded._request_quote_ticks(ticks_request(1_752_570_000, 1_752_573_600))

        [tick] = recorded._handle_quote_ticks.call_args[0][1]
        assert (tick.bid_size, tick.ask_size) == (Quantity(1000, 2), Quantity(1000, 2))

    @pytest.mark.asyncio
    async def test_raises_naming_a_symbol_not_loaded(self, client):
        client._provider.get_instrument.return_value = None

        with patch("mt5connector.client.history.ticks") as ticks:
            with pytest.raises(MT5InstrumentError, match="EURUSDm"):
                await client._request_quote_ticks(ticks_request(1_752_570_000, 1_752_573_600))

        ticks.assert_not_called()


# ═════════════════════════════════════════════════════════════════════════════
# 8. _request_bars()
# ═════════════════════════════════════════════════════════════════════════════


class TestRequestBars:

    @pytest.mark.asyncio
    async def test_asks_the_server_for_the_opens_of_the_closes_requested(self, recorded):
        c0, c1 = 1_752_570_060, 1_752_570_300
        rows = rate_rows(*range(c0 - 60, c1, 60))
        with patch("mt5connector.client.history.bars", return_value=rows) as bars:
            await recorded._request_bars(minute_bars_request(c0, c1))

        bars.assert_called_once_with("EURUSDm", Series.M1, c0 - 60, c1 - 60, cancel=ANY)
        delivered = recorded._handle_bars.call_args[0][1]
        assert [bar.ts_event for bar in delivered] == [
            close * 1_000_000_000 for close in range(c0, c1 + 1, 60)
        ]
        assert recorded._log.warning.call_count == 0

    @pytest.mark.asyncio
    async def test_an_empty_answer_completes_the_request_with_nothing_and_no_warning(
        self, recorded
    ):
        with patch("mt5connector.client.history.bars", return_value=rate_rows()):
            await recorded._request_bars(minute_bars_request(1_752_570_060, 1_752_570_300))

        assert recorded._handle_bars.call_args[0][1] == []
        assert recorded._log.warning.call_count == 0

    @pytest.mark.asyncio
    async def test_a_failed_window_is_an_error_and_delivers_nothing(self, recorded):
        with patch("mt5connector.client.history.bars", return_value=None):
            await recorded._request_bars(minute_bars_request(1_752_570_060, 1_752_570_300))

        recorded._handle_bars.assert_not_called()
        assert recorded._log.error.call_count == 1
        error = recorded._log.error.call_args[0][0]
        assert "2025-07-15T09:01:00+00:00" in error and "2025-07-15T09:05:00+00:00" in error

    @pytest.mark.asyncio
    async def test_cancelling_the_request_stops_the_history_calls_retries(self, recorded):
        handed = []
        reading = threading.Event()

        def syncing(*args, cancel):
            handed.append(cancel)
            reading.set()
            cancel.wait(5)
            return None

        with patch("mt5connector.client.history.bars", side_effect=syncing):
            request = asyncio.ensure_future(
                recorded._request_bars(minute_bars_request(1_752_570_060, 1_752_570_300))
            )
            await asyncio.to_thread(reading.wait, 5)
            request.cancel()
            with pytest.raises(asyncio.CancelledError):
                await request

        assert handed[0].is_set()
        recorded._handle_bars.assert_not_called()

    @pytest.mark.asyncio
    async def test_bars_are_the_bar_type_requested(self, recorded):
        from nautilus_trader.model.data import BarType

        request = minute_bars_request(1_752_570_060, 1_752_570_120)
        request.bar_type = BarType.from_str("EURUSDm.MT5-1-MINUTE-BID-EXTERNAL")
        with patch("mt5connector.client.history.bars", return_value=rate_rows(1_752_570_000)):
            await recorded._request_bars(request)

        assert recorded._handle_bars.call_args[0][0] == request.bar_type
        [bar] = recorded._handle_bars.call_args[0][1]
        assert bar.bar_type == request.bar_type

    @pytest.mark.parametrize("step", ["1-SECOND", "2-DAY", "1-MONTH"])
    @pytest.mark.asyncio
    async def test_a_step_the_terminal_has_no_timeframe_for_is_refused(self, recorded, step):
        from nautilus_trader.model.data import BarType

        request = minute_bars_request(1_752_570_060, 1_752_570_300)
        request.bar_type = BarType.from_str(f"EURUSDm.MT5-{step}-BID-EXTERNAL")
        with patch("mt5connector.client.history.bars") as bars:
            with pytest.raises(ValueError, match="no timeframe"):
                await recorded._request_bars(request)

        bars.assert_not_called()

    @pytest.mark.asyncio
    async def test_raises_naming_a_symbol_not_loaded(self, client):
        client._provider.get_instrument.return_value = None

        with patch("mt5connector.client.history.bars") as bars:
            with pytest.raises(MT5InstrumentError, match="EURUSDm"):
                await client._request_bars(minute_bars_request(1_752_570_060, 1_752_570_300))

        bars.assert_not_called()


# ═════════════════════════════════════════════════════════════════════════════
# 9. Instrument requests
# ═════════════════════════════════════════════════════════════════════════════


def responses_of(client) -> list:
    """What the client sends NT's data engine as responses, recorded."""
    responses = []
    client._msgbus.register(endpoint="DataEngine.response", handler=responses.append)
    return responses


def instrument_request(symbol="EURUSDm"):
    return RequestInstrument(
        instrument_id=InstrumentId.from_str(f"{symbol}.MT5"),
        start=None,
        end=None,
        client_id=ClientId("MT5"),
        venue=None,
        callback=None,
        request_id=UUID4(),
        ts_init=0,
        params=None,
    )


class TestRequestInstruments:

    @pytest.mark.asyncio
    async def test_an_instrument_request_reaches_the_data_engine_as_its_response(self):
        client, conn, provider, loop = make_client(nt_handlers=True)
        responses = responses_of(client)
        request = instrument_request()

        await client._request_instrument(request)

        [response] = responses
        assert isinstance(response, DataResponse)
        assert (response.correlation_id, response.data) == (request.id, [provider.load_symbol()])

    @pytest.mark.asyncio
    async def test_an_instruments_request_reaches_the_data_engine_as_its_response(self):
        client, conn, provider, loop = make_client(nt_handlers=True)
        responses = responses_of(client)
        request = RequestInstruments(
            start=None,
            end=None,
            client_id=ClientId("MT5"),
            venue=Venue("MT5"),
            callback=None,
            request_id=UUID4(),
            ts_init=0,
            params=None,
        )

        await client._request_instruments(request)

        [response] = responses
        assert isinstance(response, DataResponse)
        assert (response.correlation_id, response.venue) == (request.id, Venue("MT5"))
        assert response.data == provider.list_all()

    @pytest.mark.asyncio
    async def test_an_instruments_request_answers_at_the_configs_venue(self):
        client, conn, provider, loop = make_client(nt_handlers=True, venue=Venue("MT5_ALPHA"))
        responses = responses_of(client)
        request = RequestInstruments(
            start=None,
            end=None,
            client_id=ClientId("MT5_ALPHA"),
            venue=Venue("MT5_ALPHA"),
            callback=None,
            request_id=UUID4(),
            ts_init=0,
            params=None,
        )

        await client._request_instruments(request)

        [response] = responses
        assert (response.correlation_id, response.venue) == (request.id, Venue("MT5_ALPHA"))

    @pytest.mark.asyncio
    async def test_an_instrument_loaded_later_registers_its_settlement_currency_first(self):
        from nautilus_trader.core import nautilus_pyo3
        from nautilus_trader.model.enums import CurrencyType
        from nautilus_trader.model.objects import Currency

        Currency.register(Currency("CLP", 8, 0, "CLP", CurrencyType.FIAT), overwrite=True)
        clp = Currency("CLP", 0, 0, "CLP", CurrencyType.FIAT)
        client, conn, provider, loop = make_client(instrument=clp_instrument(clp))
        precisions = []
        client._handle_instrument.side_effect = lambda *_: precisions.append(
            Currency.from_str("CLP", strict=True).precision
        )

        await client._request_instrument(instrument_request("USDCLP"))

        assert precisions == [0]
        assert nautilus_pyo3.Currency.from_str("CLP", strict=True).precision == 0


def clp_instrument(clp):
    from nautilus_trader.model.currencies import Currency
    from nautilus_trader.model.identifiers import Symbol

    return CurrencyPair(
        instrument_id=InstrumentId.from_str("USDCLP.MT5"),
        raw_symbol=Symbol("USDCLP"),
        base_currency=Currency.from_str("USD"),
        quote_currency=clp,
        price_precision=2,
        size_precision=2,
        price_increment=Price(0.01, 2),
        size_increment=Quantity(0.01, 2),
        max_quantity=Quantity(100.0, 2),
        min_quantity=Quantity(0.01, 2),
        maker_fee=Decimal("0"),
        taker_fee=Decimal("0"),
        ts_event=0,
        ts_init=0,
    )


# ═════════════════════════════════════════════════════════════════════════════
# 10. The instruments NT names
# ═════════════════════════════════════════════════════════════════════════════

EURUSD = InstrumentId.from_str("EURUSD.MT5")
GBPUSD = InstrumentId.from_str("GBPUSD.MT5")
USDJPY = InstrumentId.from_str("USDJPY.MT5")


@pytest.fixture
def terminal():
    """The package behind the provider's shim, serving EURUSD, GBPUSD and USDJPY, none of them
    charging a commission."""
    definitions = {
        "EURUSD": symbol_info(name="EURUSD"),
        "GBPUSD": symbol_info(name="GBPUSD", currency_base="GBP"),
        "USDJPY": symbol_info(name="USDJPY", currency_base="USD", currency_profit="JPY"),
    }
    package = MagicMock()
    package.symbol_select.side_effect = lambda name, enable: name in definitions
    package.symbol_info.side_effect = definitions.get
    package.symbols_get.return_value = tuple(definitions.values())
    package.commission_schedule.return_value = {"ret": 0, "last_error": 0, "rules": []}
    with patch("mt5connector.client.providers.mt5", package):
        yield package


def served_client(provider_config: InstrumentProviderConfig):
    """A data client over a real provider whose config is `provider_config`, what it hands NT as
    data recorded."""
    conn = MagicMock(spec=MT5Connection)
    conn.get_account_info.return_value = AccountSnapshot.from_mt5(account_info())
    provider = MT5InstrumentProvider(
        conn, venue=MT5_VENUE, clock=LiveClock(), config=provider_config
    )
    with patch.object(data, "PushClient", PushDouble):
        client = MT5DataClient(
            loop=asyncio.get_running_loop(),
            connection=conn,
            msgbus=TestComponentStubs.msgbus(),
            cache=TestComponentStubs.cache(),
            clock=LiveClock(),
            instrument_provider=provider,
            config=make_config(),
        )
    client._handle_data = MagicMock()
    return client, provider


def handed_ids(client) -> list[str]:
    return sorted(call.args[0].id.value for call in client._handle_data.call_args_list)


def loaded_ids(provider) -> list[str]:
    return sorted(instrument.id.value for instrument in provider.list_all())


class TestInstrumentsNtNames:

    @pytest.mark.asyncio
    async def test_connect_loads_and_hands_nt_exactly_the_ids_its_provider_config_names(
        self, terminal
    ):
        client, provider = served_client(
            InstrumentProviderConfig(load_ids=frozenset({EURUSD, GBPUSD}))
        )

        await client._connect()

        assert handed_ids(client) == ["EURUSD.MT5", "GBPUSD.MT5"]
        assert loaded_ids(provider) == ["EURUSD.MT5", "GBPUSD.MT5"]
        assert client._push.connected
        await client._disconnect()

    @pytest.mark.asyncio
    async def test_connect_with_load_all_hands_nt_every_symbol_the_terminal_serves(self, terminal):
        client, provider = served_client(InstrumentProviderConfig(load_all=True))

        await client._connect()

        assert handed_ids(client) == ["EURUSD.MT5", "GBPUSD.MT5", "USDJPY.MT5"]
        await client._disconnect()

    @pytest.mark.parametrize(
        "provider_config",
        [InstrumentProviderConfig(), InstrumentProviderConfig(load_ids=frozenset())],
        ids=["no-ids", "empty-ids"],
    )
    @pytest.mark.asyncio
    async def test_connect_refuses_a_provider_config_that_names_nothing(
        self, terminal, provider_config
    ):
        client, provider = served_client(provider_config)

        with pytest.raises(MT5ConfigError, match="instrument_provider"):
            await client._connect()

        assert provider.list_all() == []
        client._handle_data.assert_not_called()
        assert not client._push.connected

    @pytest.mark.asyncio
    async def test_a_symbol_no_config_named_is_loaded_and_answered_on_request(self, terminal):
        client, provider = served_client(InstrumentProviderConfig(load_ids=frozenset({EURUSD})))
        await client._connect()
        responses = responses_of(client)
        request = instrument_request("GBPUSD")

        await client._request_instrument(request)

        [response] = responses
        assert response.correlation_id == request.id
        assert [instrument.id for instrument in response.data] == [GBPUSD]
        assert loaded_ids(provider) == ["EURUSD.MT5", "GBPUSD.MT5"]
        await client._disconnect()


# ═════════════════════════════════════════════════════════════════════════════
# 11. No-op methods — don't raise
# ═════════════════════════════════════════════════════════════════════════════


class TestNoOpMethods:

    @pytest.mark.asyncio
    async def test_subscribe_does_not_raise(self, client):
        await client._subscribe(MagicMock())

    @pytest.mark.asyncio
    async def test_unsubscribe_does_not_raise(self, client):
        await client._unsubscribe(MagicMock())

    @pytest.mark.asyncio
    async def test_subscribe_instruments_does_not_raise(self, client):
        await client._subscribe_instruments(MagicMock())

    @pytest.mark.asyncio
    async def test_subscribe_instrument_does_not_raise(self, client):
        await client._subscribe_instrument(MagicMock())

    @pytest.mark.asyncio
    async def test_subscribe_trade_ticks_does_not_raise(self, client):
        await client._subscribe_trade_ticks(MagicMock())

    @pytest.mark.asyncio
    async def test_subscribe_funding_rates_does_not_raise(self, client):
        await client._subscribe_funding_rates(MagicMock())

    @pytest.mark.asyncio
    async def test_request_trade_ticks_does_not_raise(self, client):
        await client._request_trade_ticks(MagicMock())

    @pytest.mark.asyncio
    async def test_request_funding_rates_does_not_raise(self, client):
        await client._request_funding_rates(MagicMock())

    @pytest.mark.asyncio
    async def test_request_order_book_snapshot_does_not_raise(self, client):
        await client._request_order_book_snapshot(MagicMock())


# ═════════════════════════════════════════════════════════════════════════════
# 12. Properties
# ═════════════════════════════════════════════════════════════════════════════


class TestProperties:

    def test_subscribed_quote_ticks_empty_initially(self, client):
        assert client.subscribed_quote_ticks() == []
