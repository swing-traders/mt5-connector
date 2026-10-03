"""
nautilus_mt5/data.py

MT5DataClient — streams live market data from MT5 into NautilusTrader.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING

try:
    import MetaTrader5 as mt5
except ImportError:  # pragma: no cover - Windows-only dependency
    mt5 = None  # bound to the real backend by mt5connect.backend.set_backend()

from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock, MessageBus
from nautilus_trader.data.messages import (
    RequestBars,
    RequestData,
    RequestQuoteTicks,
    SubscribeBars,
    SubscribeData,
    SubscribeQuoteTicks,
    UnsubscribeBars,
    UnsubscribeData,
    UnsubscribeQuoteTicks,
)
from nautilus_trader.live.data_client import LiveMarketDataClient
from nautilus_trader.model.identifiers import ClientId, InstrumentId, Symbol

from mt5connect import history
from mt5connect.constants import MT5_VENUE
from mt5connect.history_wire import BAR_PERIOD_S, bar_series
from mt5connect.parsing import parse_bar, parse_quote_tick

if TYPE_CHECKING:
    import numpy as np

    from mt5connect.config import MT5Config
    from mt5connect.connection import MT5Connection
    from mt5connect.providers import MT5InstrumentProvider
    from mt5connect.ws_stream import WSStreamClient

logger = logging.getLogger(__name__)


class MT5DataClient(LiveMarketDataClient):
    """
    Streams live market data from MT5 into NautilusTrader via polling.
    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        connection: MT5Connection,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        instrument_provider: MT5InstrumentProvider,
        config: MT5Config,
    ) -> None:
        super().__init__(
            loop=loop,
            client_id=ClientId(MT5_VENUE.value),
            venue=MT5_VENUE,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=instrument_provider,
        )
        self._conn = connection
        self._config = config
        self._provider = instrument_provider

        self._subscribed_symbols: set[str] = set()
        self._subscribed_bar_types: set[str] = set()
        self._poll_task: asyncio.Task | None = None
        self._ws: WSStreamClient | None = None
        self._last_tick_time: dict[str, int] = {}
        self._is_connected = False
        self._pending_subscriptions: set[str] = set()

    async def _connect(self) -> None:
        """Called by NautilusTrader on node startup."""
        self._conn.ensure_connected()
        self._is_connected = True

        for symbol in self._config.symbols:
            instrument = self._provider.get_instrument(symbol)
            if instrument is None:
                instrument = self._provider.load_symbol(symbol)
            self._handle_data(instrument)
            self._log.info(f"MT5DataClient: loaded instrument {symbol}")

            if symbol not in self._subscribed_symbols:
                self._subscribed_symbols.add(symbol)

        await asyncio.sleep(0.5)

        for symbol in list(self._pending_subscriptions):
            if symbol not in self._subscribed_symbols:
                self._subscribed_symbols.add(symbol)
            self._pending_subscriptions.discard(symbol)

        if self._config.backend == "remote":
            await self._start_ws_stream()
        else:
            self._poll_task = asyncio.get_event_loop().create_task(
                self._poll_loop(),
                name="MT5DataClient._poll_loop",
            )
            self._log.info(
                f"MT5DataClient: connected — polling every {self._config.poll_interval_ms}ms "
                f"for {len(self._config.symbols)} symbols"
            )

    async def _start_ws_stream(self) -> None:
        from mt5connect.ws_stream import WSStreamClient

        self._ws = WSStreamClient(
            url=self._config.ws_url,
            message_handler=self._ws_on_message,
            initial_delay_s=self._config.reconnect_initial_delay_s,
            max_delay_s=self._config.reconnect_max_delay_s,
        )
        await self._ws.start()
        self._log.info(f"MT5DataClient: connected — WS stream to {self._config.ws_url}")

    def _ws_on_message(self, payload: dict) -> None:
        if not ("symbol" in payload and ("bid" in payload or "ask" in payload)):
            return
        symbol = payload.get("symbol")
        if symbol not in self._subscribed_symbols:
            self._log.debug(f"MT5DataClient: ignoring tick for unsubscribed {symbol}")
            return
        instrument = self._provider.get_instrument(symbol)
        if instrument is None:
            self._log.debug(f"MT5DataClient: ignoring tick for unknown {symbol}")
            return
        from mt5connect.remote_mt5 import tick_from_ws

        tick = tick_from_ws(payload)
        quote = parse_quote_tick(tick, instrument)
        self._handle_data(quote)

    async def _disconnect(self) -> None:
        self._is_connected = False
        if self._ws is not None:
            await self._ws.stop()
            self._ws = None
        if self._poll_task and not self._poll_task.done():
            self._poll_task.cancel()
            await self._poll_task
            self._poll_task = None

        self._subscribed_symbols.clear()
        self._subscribed_bar_types.clear()
        self._last_tick_time.clear()
        self._pending_subscriptions.clear()
        self._log.info("MT5DataClient: disconnected")

    async def _subscribe_quote_ticks(self, command: SubscribeQuoteTicks) -> None:
        symbol = command.instrument_id.symbol.value

        if not self._is_connected:
            self._pending_subscriptions.add(symbol)
            return

        self._subscribed_symbols.add(symbol)
        self._log.debug(f"MT5DataClient: subscribed ticks → {symbol}")

        await self._push_subscribe_state()

    async def _unsubscribe_quote_ticks(self, command: UnsubscribeQuoteTicks) -> None:
        symbol = command.instrument_id.symbol.value
        self._subscribed_symbols.discard(symbol)
        self._last_tick_time.pop(symbol, None)
        self._pending_subscriptions.discard(symbol)
        self._log.debug(f"MT5DataClient: unsubscribed ticks → {symbol}")

        await self._push_subscribe_state()

    async def _subscribe_bars(self, command: SubscribeBars) -> None:
        bar_type_str = str(command.bar_type)
        self._subscribed_bar_types.add(bar_type_str)

        symbol = command.bar_type.instrument_id.symbol.value

        if not self._is_connected:
            self._pending_subscriptions.add(symbol)
            return

        if symbol not in self._subscribed_symbols:
            self._subscribed_symbols.add(symbol)
            self._log.debug(f"MT5DataClient: auto-subscribed ticks for bar aggregation → {symbol}")

        self._log.debug(f"MT5DataClient: subscribed bars → {bar_type_str}")

        await self._push_subscribe_state()

    async def _unsubscribe_bars(self, command: UnsubscribeBars) -> None:
        bar_type_str = str(command.bar_type)
        self._subscribed_bar_types.discard(bar_type_str)

        symbol = command.bar_type.instrument_id.symbol.value
        instrument_prefix = f"{command.bar_type.instrument_id.value}-"
        still_needed = any(bt.startswith(instrument_prefix) for bt in self._subscribed_bar_types)
        if not still_needed:
            self._subscribed_symbols.discard(symbol)
            self._last_tick_time.pop(symbol, None)
            self._pending_subscriptions.discard(symbol)
            self._log.debug(f"MT5DataClient: auto-unsubscribed ticks (no bars left) → {symbol}")

        self._log.debug(f"MT5DataClient: unsubscribed bars → {bar_type_str}")

        await self._push_subscribe_state()

    async def _push_subscribe_state(self) -> None:
        if self._config.backend == "remote" and self._ws is not None:
            await self._ws.send(
                {
                    "type": "subscribe",
                    "symbols": sorted(self._subscribed_symbols),
                }
            )

    async def _subscribe(self, command: SubscribeData) -> None:
        pass

    async def _unsubscribe(self, command: UnsubscribeData) -> None:
        pass

    async def _subscribe_instruments(self, command) -> None:
        pass

    async def _subscribe_instrument(self, command) -> None:
        pass

    async def _subscribe_order_book_deltas(self, command) -> None:
        self._log.warning("MT5 does not support order book data")

    async def _subscribe_order_book_depth(self, command) -> None:
        self._log.warning("MT5 does not support order book data")

    async def _subscribe_trade_ticks(self, command) -> None:
        self._log.warning("MT5 does not provide individual trade ticks")

    async def _subscribe_mark_prices(self, command) -> None:
        pass

    async def _subscribe_index_prices(self, command) -> None:
        pass

    async def _subscribe_funding_rates(self, command) -> None:
        self._log.warning("MT5 does not provide funding rates")

    async def _subscribe_instrument_status(self, command) -> None:
        pass

    async def _subscribe_instrument_close(self, command) -> None:
        pass

    async def _unsubscribe_instruments(self, command) -> None:
        pass

    async def _unsubscribe_instrument(self, command) -> None:
        pass

    async def _unsubscribe_order_book_deltas(self, command) -> None:
        pass

    async def _unsubscribe_order_book_depth(self, command) -> None:
        pass

    async def _unsubscribe_trade_ticks(self, command) -> None:
        pass

    async def _unsubscribe_mark_prices(self, command) -> None:
        pass

    async def _unsubscribe_index_prices(self, command) -> None:
        pass

    async def _unsubscribe_funding_rates(self, command) -> None:
        pass

    async def _unsubscribe_instrument_status(self, command) -> None:
        pass

    async def _unsubscribe_instrument_close(self, command) -> None:
        pass

    async def _request(self, request: RequestData) -> None:
        pass

    async def _request_instrument(self, request) -> None:
        symbol = request.instrument_id.symbol.value
        instrument = self._provider.load_symbol(symbol)
        self._handle_instrument(instrument, request.id)

    async def _request_instruments(self, request) -> None:
        await self._provider.load_all_async()
        instruments = self._provider.list_all()
        self._handle_instruments(instruments, MT5_VENUE, request.id)

    async def _request_quote_ticks(self, request: RequestQuoteTicks) -> None:
        symbol = request.instrument_id.symbol.value
        instrument = self._provider.get_instrument(symbol)
        if instrument is None:
            self._log.error(f"MT5DataClient: instrument not found for {symbol}")
            return

        self._conn.ensure_connected()
        start = _epoch_s(request.start)
        end = self._end_s(request.end)
        rows = await _read_in_thread(history.ticks, symbol, start, end)
        if rows is None:
            self._log.error(
                f"MT5DataClient: ticks for {symbol} {_iso(start)}..{_iso(end)} failed: "
                f"{mt5.last_error()}"
            )
        else:
            ticks = [parse_quote_tick(row, instrument) for row in rows]
            self._handle_quote_ticks(
                instrument.id, ticks, request.id, request.start, request.end, request.params
            )
            self._log.debug(f"MT5DataClient: delivered {len(ticks):,} ticks for {symbol}")

    async def _request_bars(self, request: RequestBars) -> None:
        bar_type = request.bar_type
        symbol = bar_type.instrument_id.symbol.value
        timeframe = _bar_spec_to_mt5_timeframe(bar_type)
        series = bar_series(timeframe)
        instrument = self._provider.get_instrument(symbol)
        if instrument is None:
            self._log.error(f"MT5DataClient: instrument not found for {symbol}")
            return

        self._conn.ensure_connected()
        # The request names closes; the server serves bars by their open.
        first_close = _epoch_s(request.start)
        last_close = self._end_s(request.end)
        rows = await _read_in_thread(
            history.bars,
            symbol,
            series,
            first_close - BAR_PERIOD_S[series],
            last_close - BAR_PERIOD_S[series],
        )
        if rows is None:
            self._log.error(
                f"MT5DataClient: {bar_type} bars closing {_iso(first_close)}..{_iso(last_close)} "
                f"failed: {mt5.last_error()}"
            )
        else:
            bars = [parse_bar(row, instrument, timeframe) for row in rows]
            self._handle_bars(
                bar_type, bars, request.id, request.start, request.end, request.params
            )
            self._log.debug(f"MT5DataClient: delivered {len(bars):,} bars for {symbol}")

    def _end_s(self, end) -> int:
        """A request's end in epoch seconds; a request without one ends now."""
        if end is None:
            return self._clock.timestamp_ns() // 1_000_000_000
        else:
            return _epoch_s(end)

    async def _request_order_book_snapshot(self, request) -> None:
        self._log.warning("MT5 does not support order book snapshots")

    async def _request_trade_ticks(self, request) -> None:
        self._log.warning("MT5 does not provide individual trade ticks")

    async def _request_funding_rates(self, request) -> None:
        self._log.warning("MT5 does not provide funding rates")

    async def _poll_loop(self) -> None:
        self._log.info("MT5DataClient: poll loop started")

        while True:
            await self._poll_once()
            await asyncio.sleep(self._config.poll_interval_s)

    async def _poll_once(self) -> None:
        """Single poll iteration — fetch one tick per subscribed symbol."""

        if not self._subscribed_symbols:
            return

        self._conn.ensure_connected()

        for symbol in list(self._subscribed_symbols):

            if not mt5.symbol_select(symbol, True):
                await asyncio.sleep(0.1)
                if not mt5.symbol_select(symbol, True):
                    continue

            raw_tick = None

            for _ in range(3):
                raw_tick = mt5.symbol_info_tick(symbol)
                if raw_tick is not None:
                    break
                await asyncio.sleep(0.05)

            if raw_tick is None:
                continue

            tick_time_ms = raw_tick.time_msc
            if self._last_tick_time.get(symbol) == tick_time_ms:
                continue
            self._last_tick_time[symbol] = tick_time_ms

            instrument = self._provider.get_instrument(symbol)
            if instrument is None:
                continue

            tick = parse_quote_tick(raw_tick, instrument)

            self._handle_data(tick)

    def subscribed_quote_ticks(self) -> list[InstrumentId]:
        """Currently subscribed symbols."""
        return [InstrumentId(Symbol(s), MT5_VENUE) for s in sorted(self._subscribed_symbols)]

    @property
    def is_polling(self) -> bool:
        """True if the poll loop task is running."""
        return self._poll_task is not None and not self._poll_task.done()


async def _read_in_thread(read: Callable[..., np.ndarray | None], *args) -> np.ndarray | None:
    """A history read run off the event loop, its retries stopped when the awaiting task is
    cancelled, since the thread outlives the task."""
    cancel = threading.Event()
    try:
        return await asyncio.to_thread(read, *args, cancel=cancel)
    except asyncio.CancelledError:
        cancel.set()
        raise


def _epoch_s(value) -> int:
    """A request bound — a datetime, or epoch nanoseconds — in epoch seconds."""
    if hasattr(value, "timestamp"):
        return int(value.timestamp())
    else:
        return value // 1_000_000_000


def _iso(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, UTC).isoformat(timespec="seconds")


def _bar_spec_to_mt5_timeframe(bar_type) -> int:
    from nautilus_trader.model.enums import BarAggregation

    spec = bar_type.spec
    agg = spec.aggregation
    step = spec.step

    if agg == BarAggregation.MINUTE:
        tf_map = {1: 1, 2: 2, 3: 3, 4: 4, 5: 5, 6: 6, 10: 10, 12: 12, 15: 15, 20: 20, 30: 30}
        return tf_map.get(step, mt5.TIMEFRAME_H1)

    if agg == BarAggregation.HOUR:
        tf_map = {
            1: mt5.TIMEFRAME_H1,
            2: 16386,
            3: 16387,
            4: mt5.TIMEFRAME_H4,
            6: 16390,
            8: 16392,
            12: 16396,
        }
        return tf_map.get(step, mt5.TIMEFRAME_H1)

    if agg == BarAggregation.DAY:
        return mt5.TIMEFRAME_D1
    if agg == BarAggregation.WEEK:
        return 32769
    if agg == BarAggregation.MONTH:
        return 49153

    return mt5.TIMEFRAME_H1


# fix
