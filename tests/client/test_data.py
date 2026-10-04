"""The data client's connection, its subscriptions and its history requests; its pushed frames are
tests/client/test_data_push.py's."""

import asyncio
import threading
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import ANY, AsyncMock, MagicMock, patch

import numpy as np
import pytest
from nautilus_trader.model.identifiers import ClientId, InstrumentId
from nautilus_trader.model.instruments import CurrencyPair
from nautilus_trader.model.objects import Price, Quantity
from push_double import PushDouble

from mt5connector.client import data
from mt5connector.client.connection import ConnectionState
from mt5connector.client.data import MT5DataClient, _bar_spec_to_mt5_timeframe, _epoch_s
from mt5connector.client.errors import MT5ConnectionError
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


def make_config(symbols=None):
    from mt5connector.client.config import MT5Config

    return MT5Config(
        account=12345678,
        password="test",
        server="Exness-MT5Trial1",
        symbols=symbols or ["EURUSDm"],
        server_url="http://127.0.0.1:5000",
        reconnect_initial_delay_s=0.01,
        reconnect_max_delay_s=0.05,
        reconnect_max_attempts=2,
    )


def make_conn(connected=True):
    conn = MagicMock()
    conn.is_connected = connected
    conn.state = ConnectionState.CONNECTED if connected else ConnectionState.DISCONNECTED
    conn.ensure_connected = (
        MagicMock() if connected else MagicMock(side_effect=MT5ConnectionError("not connected"))
    )
    conn.reconnect_async = AsyncMock(return_value=connected)
    return conn


def make_provider(instrument=None):
    """
    Build a real MT5InstrumentProvider subclass — NautilusTrader's
    PyCondition.type() rejects MagicMock, so we must use a real subclass.
    We override the methods to return test data without touching MT5.
    """
    from mt5connector.client.connection import MT5Connection
    from mt5connector.client.providers import MT5InstrumentProvider

    # Minimal connection mock that satisfies MT5Connection's interface
    conn = MagicMock(spec=MT5Connection)
    conn.ensure_connected = MagicMock()

    inst = instrument or make_instrument("EURUSDm")

    provider = MT5InstrumentProvider.__new__(MT5InstrumentProvider)
    # Manually initialise the InstrumentProvider base
    from nautilus_trader.common.providers import InstrumentProvider

    InstrumentProvider.__init__(provider)
    # Set our test attributes
    provider._conn = conn
    provider._failed_symbols = []

    # Override methods to return test data
    provider.get_instrument = MagicMock(return_value=inst)
    provider.load_symbol = MagicMock(return_value=inst)
    provider.list_all = MagicMock(return_value=[inst])
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


def make_client(symbols=None, connected=True, instrument=None, client_class=MT5DataClient):
    """Build a fully wired MT5DataClient with real NautilusTrader components."""
    from nautilus_trader.common.component import LiveClock
    from nautilus_trader.test_kit.stubs.component import TestComponentStubs

    # Use the running event loop (pytest-asyncio's loop) to avoid cross-loop errors
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()

    config = make_config(symbols)
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
    # Patch internal handler methods so we can track emitted data
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


class TestBarSpecToMt5Timeframe:

    def _make_bar_type(self, step, aggregation_name):
        from nautilus_trader.model.enums import BarAggregation

        bar_spec = MagicMock()
        bar_spec.step = step
        bar_spec.aggregation = getattr(BarAggregation, aggregation_name)
        bar_type = MagicMock()
        bar_type.spec = bar_spec
        return bar_type

    def test_m1_returns_1(self):
        bt = self._make_bar_type(1, "MINUTE")
        with patch("mt5connector.client.data.mt5") as mock_mt5:
            mock_mt5.TIMEFRAME_H1 = 16385
            result = _bar_spec_to_mt5_timeframe(bt)
        assert result == 1

    def test_m5_returns_5(self):
        bt = self._make_bar_type(5, "MINUTE")
        with patch("mt5connector.client.data.mt5") as mock_mt5:
            mock_mt5.TIMEFRAME_H1 = 16385
            result = _bar_spec_to_mt5_timeframe(bt)
        assert result == 5

    def test_m15_returns_15(self):
        bt = self._make_bar_type(15, "MINUTE")
        with patch("mt5connector.client.data.mt5") as mock_mt5:
            mock_mt5.TIMEFRAME_H1 = 16385
            result = _bar_spec_to_mt5_timeframe(bt)
        assert result == 15

    def test_h1_returns_16385(self):
        bt = self._make_bar_type(1, "HOUR")
        with patch("mt5connector.client.data.mt5") as mock_mt5:
            mock_mt5.TIMEFRAME_H1 = 16385
            mock_mt5.TIMEFRAME_H4 = 16388
            result = _bar_spec_to_mt5_timeframe(bt)
        assert result == 16385

    def test_h4_returns_16388(self):
        bt = self._make_bar_type(4, "HOUR")
        with patch("mt5connector.client.data.mt5") as mock_mt5:
            mock_mt5.TIMEFRAME_H1 = 16385
            mock_mt5.TIMEFRAME_H4 = 16388
            result = _bar_spec_to_mt5_timeframe(bt)
        assert result == 16388

    def test_d1_returns_correct(self):
        bt = self._make_bar_type(1, "DAY")
        with patch("mt5connector.client.data.mt5") as mock_mt5:
            mock_mt5.TIMEFRAME_D1 = 16408
            mock_mt5.TIMEFRAME_H1 = 16385
            result = _bar_spec_to_mt5_timeframe(bt)
        assert result == 16408

    def test_unknown_falls_back_to_h1(self):
        bt = self._make_bar_type(999, "MINUTE")
        with patch("mt5connector.client.data.mt5") as mock_mt5:
            mock_mt5.TIMEFRAME_H1 = 16385
            result = _bar_spec_to_mt5_timeframe(bt)
        assert result == 16385


# ═════════════════════════════════════════════════════════════════════════════
# 2. Initial state
# ═════════════════════════════════════════════════════════════════════════════


class TestInitialState:

    def test_client_id_is_mt5(self, client):
        assert client.id == ClientId("MT5")

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
    async def test_connect_loads_instruments(self):
        c, conn, prov, loop = make_client()
        await c._connect()
        prov.get_instrument.assert_called()
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
        await client._unsubscribe_quote_ticks(cmd)  # must not raise


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
    async def test_skips_when_instrument_not_found(self, client):
        client._provider.get_instrument.return_value = None

        with patch("mt5connector.client.history.ticks") as ticks:
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
    async def test_skips_when_instrument_not_found(self, client):
        client._provider.get_instrument.return_value = None

        with patch("mt5connector.client.history.bars") as bars:
            await client._request_bars(minute_bars_request(1_752_570_060, 1_752_570_300))

        bars.assert_not_called()


# ═════════════════════════════════════════════════════════════════════════════
# 9. No-op methods — don't raise
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
# 10. Properties
# ═════════════════════════════════════════════════════════════════════════════


class TestProperties:

    def test_subscribed_quote_ticks_empty_initially(self, client):
        assert client.subscribed_quote_ticks() == []
