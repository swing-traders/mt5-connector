"""The data client's push consumption: its streams, a pushed tick and bar as NT data, and the bars a
reconnect missed read back once."""

import asyncio
import threading
from decimal import Decimal
from unittest.mock import ANY, AsyncMock, MagicMock, patch

import numpy as np
import pytest
from nautilus_trader.common.component import TestClock
from nautilus_trader.common.providers import InstrumentProvider
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.data.messages import (
    SubscribeBars,
    SubscribeMarkPrices,
    SubscribeQuoteTicks,
    UnsubscribeBars,
    UnsubscribeMarkPrices,
    UnsubscribeQuoteTicks,
)
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.data import Bar, BarType, MarkPriceUpdate, QuoteTick
from nautilus_trader.model.identifiers import ClientId, InstrumentId, Symbol
from nautilus_trader.model.instruments import CurrencyPair
from nautilus_trader.model.objects import Currency, Price, Quantity
from nautilus_trader.test_kit.stubs.component import TestComponentStubs
from push_double import PushDouble

from mt5connector.client import data
from mt5connector.client.config import MT5Config
from mt5connector.client.data import MT5DataClient
from mt5connector.client.errors import MT5InstrumentError, ServerUnreachable
from mt5connector.client.providers import MT5InstrumentProvider
from mt5connector.wire import mirror
from mt5connector.wire.history_wire import Series
from mt5connector.wire.push_wire import Stream, Subscription

EURUSD = InstrumentId.from_str("EURUSD.MT5")
# 2025-07-15T09:00:00Z
NINE = 1_752_570_000
SECOND_NS = 1_000_000_000
M1_BID = BarType.from_str("EURUSD.MT5-1-MINUTE-BID-EXTERNAL")
M5_BID = BarType.from_str("EURUSD.MT5-5-MINUTE-BID-EXTERNAL")
W1_BID = BarType.from_str("EURUSD.MT5-1-WEEK-BID-EXTERNAL")
# The broker's weekly opens either side of New York's 2025 spring change, 167 hours apart in UTC:
# 2025-03-02T22:00Z and 2025-03-09T21:00Z.
WEEK_BEFORE = 1_740_952_800
WEEK_AFTER = 1_741_554_000
RATES = np.dtype(list(mirror.RATES.dtype))


def instrument():
    """A five-digit pair whose largest order is 100 lots."""
    return CurrencyPair(
        instrument_id=EURUSD,
        raw_symbol=Symbol("EURUSD"),
        base_currency=Currency.from_str("EUR"),
        quote_currency=USD,
        price_precision=5,
        size_precision=2,
        price_increment=Price.from_str("0.00001"),
        size_increment=Quantity.from_str("0.01"),
        multiplier=Quantity.from_str("100000"),
        min_quantity=Quantity.from_str("0.01"),
        max_quantity=Quantity.from_str("100.00"),
        maker_fee=Decimal(0),
        taker_fee=Decimal(0),
        ts_event=0,
        ts_init=0,
    )


class RecordedDataClient(MT5DataClient):
    """The data client with its log calls recorded."""

    def __init__(self, *args, **kwargs):
        self.recorded_log = MagicMock()
        super().__init__(*args, **kwargs)

    @property
    def _log(self):
        return self.recorded_log


class Client:
    """A data client over the push double, its clock at `now` and what it hands NT recorded."""

    def __init__(self, now):
        self.clock = TestClock()
        self.clock.set_time(now * SECOND_NS)
        provider = MT5InstrumentProvider.__new__(MT5InstrumentProvider)
        InstrumentProvider.__init__(
            provider, InstrumentProviderConfig(load_ids=frozenset({EURUSD}))
        )
        provider.add(instrument())
        provider.load_ids_async = AsyncMock()
        config = MT5Config(
            account=12345678,
            password="p",
            server="Broker-Demo",
            server_url="http://127.0.0.1:5000",
        )
        with patch.object(data, "PushClient", PushDouble):
            self.client = RecordedDataClient(
                asyncio.get_running_loop(),
                MagicMock(),
                TestComponentStubs.msgbus(),
                TestComponentStubs.cache(),
                self.clock,
                provider,
                config,
            )
        self.client._handle_data = MagicMock()
        self.push = self.client._push

    def handed(self) -> list:
        return [call.args[0] for call in self.client._handle_data.call_args_list]

    def bars(self) -> list:
        return [item for item in self.handed() if isinstance(item, Bar)]

    def at(self, now):
        self.clock.set_time(now * SECOND_NS)


def quotes(instrument_id=EURUSD):
    return SubscribeQuoteTicks(
        instrument_id=instrument_id,
        client_id=ClientId("MT5"),
        venue=None,
        command_id=UUID4(),
        ts_init=0,
    )


def unquotes(instrument_id=EURUSD):
    return UnsubscribeQuoteTicks(
        instrument_id=instrument_id,
        client_id=ClientId("MT5"),
        venue=None,
        command_id=UUID4(),
        ts_init=0,
    )


def marks(instrument_id=EURUSD):
    return SubscribeMarkPrices(
        instrument_id=instrument_id,
        client_id=ClientId("MT5"),
        venue=None,
        command_id=UUID4(),
        ts_init=0,
    )


def unmarks(instrument_id=EURUSD):
    return UnsubscribeMarkPrices(
        instrument_id=instrument_id,
        client_id=ClientId("MT5"),
        venue=None,
        command_id=UUID4(),
        ts_init=0,
    )


def bars(bar_type):
    return SubscribeBars(
        bar_type=bar_type, client_id=ClientId("MT5"), venue=None, command_id=UUID4(), ts_init=0
    )


def unbars(bar_type):
    return UnsubscribeBars(
        bar_type=bar_type, client_id=ClientId("MT5"), venue=None, command_id=UUID4(), ts_init=0
    )


def tick(bid, ask, time_msc=NINE * 1000 + 123):
    return {
        "v": 1,
        "type": "tick",
        "symbol": "EURUSD",
        "time": time_msc // 1000,
        "bid": bid,
        "ask": ask,
        "last": 0.0,
        "volume": 0,
        "time_msc": time_msc,
        "flags": 6,
        "volume_real": 0.0,
    }


def bar(open_time, timeframe="M1", **prices):
    return {
        "v": 1,
        "type": "bar",
        "symbol": "EURUSD",
        "timeframe": timeframe,
        "time": open_time,
        "open": 1.085,
        "high": 1.0852,
        "low": 1.0849,
        "close": 1.0851,
        "tick_volume": 42,
        "spread": 12,
        "real_volume": 0,
    } | prices


def rate_rows(*opens):
    return np.array([(t, 1.085, 1.0852, 1.0849, 1.0851, 42, 12, 0) for t in opens], dtype=RATES)


async def settle():
    for _ in range(20):
        await asyncio.sleep(0)


# ── Subscriptions ────────────────────────────────────────────────────────────


async def test_connect_hands_nt_the_instruments_its_provider_holds_and_connects_the_push_channel():
    c = Client(NINE)

    await c.client._connect()

    assert c.push.connected
    assert [item.id for item in c.handed()] == [EURUSD]
    await c.client._disconnect()
    assert not c.push.connected


async def test_quotes_and_marks_share_the_symbols_tick_stream_until_neither_is_subscribed():
    c = Client(NINE)
    ticks = Subscription(Stream.TICKS, "EURUSD")

    await c.client._subscribe_quote_ticks(quotes())
    await c.client._subscribe_mark_prices(marks())
    assert c.push.wanted == [ticks]
    await c.client._unsubscribe_quote_ticks(unquotes())
    assert c.push.wanted == [ticks]
    await c.client._unsubscribe_mark_prices(unmarks())
    assert c.push.wanted == []


async def test_external_bars_subscribe_their_symbols_series_once_for_every_price_type():
    c = Client(NINE)
    m1_last = BarType.from_str("EURUSD.MT5-1-MINUTE-LAST-EXTERNAL")

    await c.client._subscribe_bars(bars(M1_BID))
    await c.client._subscribe_bars(bars(m1_last))
    assert c.push.wanted == [Subscription(Stream.BARS, "EURUSD", Series.M1)]
    await c.client._unsubscribe_bars(unbars(M1_BID))
    assert c.push.wanted == [Subscription(Stream.BARS, "EURUSD", Series.M1)]
    await c.client._unsubscribe_bars(unbars(m1_last))
    assert c.push.wanted == []


async def test_a_bar_type_the_terminal_has_no_timeframe_for_is_refused():
    c = Client(NINE)

    with pytest.raises(ValueError, match="7-MINUTE"):
        await c.client._subscribe_bars(bars(BarType.from_str("EURUSD.MT5-7-MINUTE-BID-EXTERNAL")))
    assert c.push.wanted == []


# ── Ticks ────────────────────────────────────────────────────────────────────


async def test_a_tick_is_a_quote_tick_sized_at_the_largest_order_and_a_mark_at_its_mid():
    c = Client(NINE + 1)

    c.push.deliver(tick(1.08500, 1.08512))

    quote, mark = c.handed()
    assert isinstance(quote, QuoteTick)
    assert (quote.instrument_id, quote.bid_price, quote.ask_price) == (
        EURUSD,
        Price.from_str("1.08500"),
        Price.from_str("1.08512"),
    )
    assert (quote.bid_size, quote.ask_size) == (Quantity.from_str("100.00"),) * 2
    assert (quote.ts_event, quote.ts_init) == (
        (NINE * 1000 + 123) * 1_000_000,
        (NINE + 1) * SECOND_NS,
    )
    assert isinstance(mark, MarkPriceUpdate)
    assert mark.instrument_id == EURUSD
    assert mark.value.as_decimal() == Decimal("1.08506")
    assert (mark.ts_event, mark.ts_init) == (quote.ts_event, quote.ts_init)


async def test_a_mid_between_two_ticks_of_the_grid_is_kept_at_full_precision():
    c = Client(NINE + 1)

    c.push.deliver(tick(1.08500, 1.08511))

    assert c.handed()[1].value.as_decimal() == Decimal("1.085055")


async def test_a_tick_of_a_symbol_not_loaded_is_refused():
    c = Client(NINE + 1)

    c.push.deliver(tick(1.085, 1.0852) | {"symbol": "XAUUSD"})

    assert c.handed() == []
    (failed,) = c.client._log.exception.call_args_list
    assert isinstance(failed.args[1], MT5InstrumentError)
    assert "XAUUSD" in str(failed.args[1])


# ── Bars ─────────────────────────────────────────────────────────────────────


async def test_a_pushed_bar_is_the_venue_bar_stamped_at_its_open_plus_its_interval():
    c = Client(NINE + 1)
    await c.client._subscribe_bars(bars(M5_BID))

    c.at(NINE + 301)
    c.push.deliver(bar(NINE, "M5"))

    (pushed,) = c.bars()
    assert pushed.bar_type == M5_BID
    assert (pushed.open, pushed.high, pushed.low, pushed.close) == (
        Price.from_str("1.08500"),
        Price.from_str("1.08520"),
        Price.from_str("1.08490"),
        Price.from_str("1.08510"),
    )
    assert pushed.volume == Quantity.from_int(42)
    assert pushed.ts_event == pushed.ts_init == (NINE + 300) * SECOND_NS


async def test_a_bar_of_a_series_not_subscribed_hands_nothing():
    c = Client(NINE + 1)
    await c.client._subscribe_bars(bars(M1_BID))

    c.push.deliver(bar(NINE, "M5"))

    assert c.bars() == []


class History:
    """The history route's bars: each read blocks until a test releases it, then answers the opens
    of `opens` its window holds."""

    def __init__(self, *opens):
        self.opens = opens
        self.reads = []
        self.gates = []

    def __call__(self, symbol, series, start, end, cancel):
        gate = threading.Event()
        self.reads.append((symbol, series, start, end))
        self.gates.append(gate)
        gate.wait(5)
        return rate_rows(*(opened for opened in self.opens if start <= opened <= end))


async def until(condition):
    for _ in range(300):
        if condition():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("the condition never held")


def opens(c) -> list[int]:
    return [item.ts_event // SECOND_NS - 60 for item in c.bars()]


async def test_after_a_reconnect_the_first_pushed_bar_bounds_one_read_of_the_bars_missed():
    c = Client(NINE + 30)
    await c.client._subscribe_bars(bars(M1_BID))
    c.at(NINE + 65)
    c.push.deliver(bar(NINE))
    history = History(NINE + 60, NINE + 120, NINE + 180, NINE + 240, NINE + 300)

    with patch("mt5connector.client.history.bars", side_effect=history):
        c.at(NINE + 310)
        c.push.reconnect()
        c.at(NINE + 360)
        c.push.deliver(bar(NINE + 300))
        await until(lambda: history.reads)
        c.push.deliver(bar(NINE + 360))
        held = opens(c)
        history.gates[0].set()
        await until(lambda: len(c.bars()) == 7)

    assert history.reads == [("EURUSD", Series.M1, NINE + 1, NINE + 299)]
    assert held == [NINE]
    assert opens(c) == [NINE + 60 * minute for minute in range(7)]


async def test_a_bar_that_closed_before_the_hub_restored_the_subscription_is_read_back():
    c = Client(NINE + 30)
    await c.client._subscribe_bars(bars(M1_BID))
    c.at(NINE + 60)
    c.push.deliver(bar(NINE))
    history = History(NINE + 60, NINE + 120)

    with patch("mt5connector.client.history.bars", side_effect=history):
        c.at(NINE + 119)
        c.push.reconnect()
        c.at(NINE + 180)
        c.push.deliver(bar(NINE + 120))
        await until(lambda: history.reads)
        history.gates[0].set()
        await until(lambda: len(c.bars()) == 3)

    assert opens(c) == [NINE, NINE + 60, NINE + 120]


async def test_a_second_reconnect_during_a_read_back_leaves_the_last_read_to_hand_every_bar():
    c = Client(NINE + 30)
    await c.client._subscribe_bars(bars(M1_BID))
    c.at(NINE + 60)
    c.push.deliver(bar(NINE))
    history = History(NINE + 60, NINE + 120, NINE + 180, NINE + 240)

    with patch("mt5connector.client.history.bars", side_effect=history):
        c.at(NINE + 130)
        c.push.reconnect()
        c.at(NINE + 180)
        c.push.deliver(bar(NINE + 120))
        await until(lambda: len(history.reads) == 1)
        c.at(NINE + 250)
        c.push.reconnect()
        c.at(NINE + 300)
        c.push.deliver(bar(NINE + 240))
        await until(lambda: len(history.reads) == 2)
        history.gates[0].set()
        await asyncio.sleep(0.1)
        after_the_first = opens(c)
        history.gates[1].set()
        await until(lambda: len(c.bars()) == 5)

    assert history.reads == [
        ("EURUSD", Series.M1, NINE + 1, NINE + 119),
        ("EURUSD", Series.M1, NINE + 1, NINE + 239),
    ]
    assert after_the_first == [NINE]
    assert opens(c) == [NINE, NINE + 60, NINE + 120, NINE + 180, NINE + 240]


async def test_a_reconnect_with_no_bar_missed_reads_an_empty_window_and_hands_no_bar_twice():
    c = Client(NINE + 30)
    await c.client._subscribe_bars(bars(M1_BID))
    c.at(NINE + 60)
    c.push.deliver(bar(NINE))
    history = History(NINE, NINE + 60)

    with patch("mt5connector.client.history.bars", side_effect=history):
        c.push.reconnect()
        c.at(NINE + 120)
        c.push.deliver(bar(NINE + 60))
        await until(lambda: history.reads)
        history.gates[0].set()
        await until(lambda: len(c.bars()) == 2)

    assert history.reads == [("EURUSD", Series.M1, NINE + 1, NINE + 59)]
    assert opens(c) == [NINE, NINE + 60]


async def test_a_failed_read_back_is_an_error_and_the_held_bars_go_on():
    c = Client(NINE + 30)
    await c.client._subscribe_bars(bars(M1_BID))

    with patch("mt5connector.client.history.bars", return_value=None) as history_bars:
        c.at(NINE + 190)
        c.push.reconnect()
        c.at(NINE + 240)
        c.push.deliver(bar(NINE + 180))
        await until(lambda: c.bars())

    history_bars.assert_called_once_with("EURUSD", Series.M1, NINE - 29, NINE + 179, cancel=ANY)
    assert opens(c) == [NINE + 180]
    assert len(c.client._log.error.call_args_list) == 1


async def test_a_read_back_the_server_does_not_answer_is_an_error_and_the_held_bars_go_on():
    c = Client(NINE + 30)
    await c.client._subscribe_bars(bars(M1_BID))

    refused = ServerUnreachable("history/bars: refused")
    with patch("mt5connector.client.history.bars", side_effect=refused):
        c.at(NINE + 190)
        c.push.reconnect()
        c.at(NINE + 240)
        c.push.deliver(bar(NINE + 180))
        await until(lambda: c.bars())
        c.at(NINE + 300)
        c.push.deliver(bar(NINE + 240))

    assert opens(c) == [NINE + 180, NINE + 240]
    assert len(c.client._log.error.call_args_list) == 1


async def test_the_read_back_takes_every_bar_opened_before_the_first_pushed_one_across_dst():
    c = Client(WEEK_BEFORE + 3_600)
    await c.client._subscribe_bars(bars(W1_BID))
    history = History(WEEK_BEFORE, WEEK_AFTER)

    with patch("mt5connector.client.history.bars", side_effect=history):
        c.at(WEEK_AFTER + 60)
        c.push.reconnect()
        c.at(WEEK_AFTER + 604_800)
        c.push.deliver(bar(WEEK_AFTER, "W1"))
        await until(lambda: history.reads)
        history.gates[0].set()
        await until(lambda: len(c.bars()) == 2)

    assert [item.ts_event // SECOND_NS - 604_800 for item in c.bars()] == [WEEK_BEFORE, WEEK_AFTER]
