"""The NT data client of an MT5 terminal: its quote ticks, a mark at the mid of each, and its closed
venue bars pushed over the hub, and its history read over HTTP.

State: the symbols whose quotes and whose marks NT subscribes, which share one tick stream; per
venue bar type, the earliest open the next bar handed to NT may have, and from a reconnect until the
bars it missed are read back, the bars pushed since."""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock, MessageBus
from nautilus_trader.data.messages import (
    RequestBars,
    RequestData,
    RequestInstrument,
    RequestInstruments,
    RequestQuoteTicks,
    SubscribeBars,
    SubscribeData,
    SubscribeMarkPrices,
    SubscribeQuoteTicks,
    UnsubscribeBars,
    UnsubscribeData,
    UnsubscribeMarkPrices,
    UnsubscribeQuoteTicks,
)
from nautilus_trader.live.data_client import LiveMarketDataClient
from nautilus_trader.model.data import BarType
from nautilus_trader.model.identifiers import ClientId

from mt5connector.client import history
from mt5connector.client import remote_mt5 as mt5
from mt5connector.client.currencies import register_venue_currency
from mt5connector.client.errors import MT5ConnectionError, MT5InstrumentError
from mt5connector.client.parsing import (
    InstrumentAny,
    mark_at_mid,
    parse_quote_tick,
    quote_tick_from_frame,
    venue_bar,
    venue_series,
)
from mt5connector.client.push import PushClient
from mt5connector.wire.history_wire import BAR_PERIOD_S, Series
from mt5connector.wire.push_wire import FrameType, Stream, Subscription

if TYPE_CHECKING:
    import numpy as np

    from mt5connector.client.config import MT5Config
    from mt5connector.client.connection import MT5Connection
    from mt5connector.client.providers import MT5InstrumentProvider


@dataclass(eq=False)
class _Recovery:
    """A venue bar type's recovery from a reconnect."""

    held: list[dict] = field(default_factory=list)
    reading: bool = False


class MT5DataClient(LiveMarketDataClient):
    """Streams an MT5 terminal's market data into NautilusTrader over the hub's push channel."""

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
            client_id=ClientId(config.venue.value),
            venue=config.venue,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=instrument_provider,
        )
        self._conn = connection
        self._config = config
        self._provider = instrument_provider
        self._push = PushClient(
            config, loop, self._on_push_frame, self._on_push_reconnect, self._log
        )
        self._quote_symbols: set[str] = set()
        self._mark_symbols: set[str] = set()
        self._next_opens: dict[BarType, int] = {}
        self._recoveries: dict[BarType, _Recovery] = {}
        self._read_backs: set[asyncio.Task] = set()

    async def _connect(self) -> None:
        """Loads what the instrument provider's config names, hands NT every instrument the provider
        holds and connects the push channel; raises MT5ConfigError for a config that names nothing
        to load."""
        self._conn.ensure_connected()
        await self._provider.initialize()
        instruments = self._provider.list_all()
        _register_settlement(instruments)
        for instrument in instruments:
            self._handle_data(instrument)
            self._log.info(f"MT5DataClient: loaded instrument {instrument.raw_symbol}")
        await self._push.connect()
        self._log.info(
            f"MT5DataClient: connected, the push channel at {self._config.ws_display_url}"
        )

    async def _disconnect(self) -> None:
        """Disconnects the push channel; what NT subscribes stays wanted for the next connect."""
        for task in self._read_backs:
            task.cancel()
        self._recoveries.clear()
        await self._push.disconnect()
        self._log.info("MT5DataClient: disconnected")

    # ── Quotes and marks ──────────────────────────────────────────────────────

    async def _subscribe_quote_ticks(self, command: SubscribeQuoteTicks) -> None:
        symbol = command.instrument_id.symbol.value
        self._quote_symbols.add(symbol)
        await self._push.subscribe(Subscription(Stream.TICKS, symbol))

    async def _unsubscribe_quote_ticks(self, command: UnsubscribeQuoteTicks) -> None:
        symbol = command.instrument_id.symbol.value
        self._quote_symbols.discard(symbol)
        if symbol not in self._mark_symbols:
            await self._push.unsubscribe(Subscription(Stream.TICKS, symbol))

    async def _subscribe_mark_prices(self, command: SubscribeMarkPrices) -> None:
        symbol = command.instrument_id.symbol.value
        self._mark_symbols.add(symbol)
        await self._push.subscribe(Subscription(Stream.TICKS, symbol))

    async def _unsubscribe_mark_prices(self, command: UnsubscribeMarkPrices) -> None:
        symbol = command.instrument_id.symbol.value
        self._mark_symbols.discard(symbol)
        if symbol not in self._quote_symbols:
            await self._push.unsubscribe(Subscription(Stream.TICKS, symbol))

    # ── Venue bars ────────────────────────────────────────────────────────────

    async def _subscribe_bars(self, command: SubscribeBars) -> None:
        """Subscribes a venue bar type's series; a bar that closed before the subscription is the
        history's to serve."""
        subscription = _bar_subscription(command.bar_type)
        now_s = self._clock.timestamp_ns() // 1_000_000_000
        self._next_opens[command.bar_type] = now_s - BAR_PERIOD_S[subscription.timeframe] + 1
        await self._push.subscribe(subscription)

    async def _unsubscribe_bars(self, command: UnsubscribeBars) -> None:
        subscription = _bar_subscription(command.bar_type)
        self._next_opens.pop(command.bar_type, None)
        self._recoveries.pop(command.bar_type, None)
        if subscription not in {_bar_subscription(bar_type) for bar_type in self._next_opens}:
            await self._push.unsubscribe(subscription)

    # ── The push channel ──────────────────────────────────────────────────────

    def _on_push_frame(self, frame: dict) -> None:
        """Hands NT a pushed tick as a quote tick and a mark at its mid, and a pushed bar as the
        venue bar of every bar type subscribed to its series."""
        kind = FrameType(frame["type"])
        if kind == FrameType.TICK:
            quote = quote_tick_from_frame(
                frame, self._instrument(frame["symbol"]), self._clock.timestamp_ns()
            )
            self._handle_data(quote)
            self._handle_data(mark_at_mid(quote))
        elif kind == FrameType.BAR:
            pushed = Subscription(Stream.BARS, frame["symbol"], Series(frame["timeframe"]))
            for bar_type in list(self._next_opens):
                if _bar_subscription(bar_type) == pushed:
                    self._on_pushed_bar(bar_type, frame)
        else:
            raise ValueError(f"a {kind} frame on the data channel")

    def _on_pushed_bar(self, bar_type: BarType, frame: dict) -> None:
        """Hands NT a pushed bar, or holds it while the bar type recovers from a reconnect; the
        first bar pushed after the reconnect starts the read of the bars missed before it."""
        recovery = self._recoveries.get(bar_type)
        if recovery is None:
            self._emit_bar(bar_type, frame)
        else:
            recovery.held.append(frame)
            if not recovery.reading:
                recovery.reading = True
                task = self._loop.create_task(
                    self._read_back(bar_type, recovery, int(frame["time"]))
                )
                self._read_backs.add(task)
                task.add_done_callback(self._read_backs.discard)

    def _emit_bar(self, bar_type: BarType, rates) -> None:
        """Hands NT a venue bar of `bar_type` unless one at or after its open was handed already."""
        next_open = self._next_opens.get(bar_type)
        opened = int(rates["time"])
        if next_open is not None and opened >= next_open:
            instrument = self._instrument(bar_type.instrument_id.symbol.value)
            self._handle_data(venue_bar(rates, bar_type, instrument))
            self._next_opens[bar_type] = opened + 1

    def _on_push_reconnect(self) -> None:
        """Starts every venue bar type's recovery before the channel subscribes it again, keeping
        the bars an earlier recovery still holds: the hub may restore a subscription only after a
        bar has closed, so the first bar pushed afterwards is what bounds the bars missed."""
        for bar_type in self._next_opens:
            earlier = self._recoveries.get(bar_type)
            if earlier is None:
                self._recoveries[bar_type] = _Recovery()
            else:
                self._recoveries[bar_type] = _Recovery(held=earlier.held)

    async def _read_back(self, bar_type: BarType, recovery: _Recovery, pushed_open: int) -> None:
        """Reads once the bars of `bar_type` opened from the next one owed until `pushed_open`, the
        first pushed after the reconnect, and hands NT them and then the bars the recovery holds —
        unless a later reconnect superseded the recovery, whose own read covers them all."""
        # None once NT unsubscribes the bar type.
        first_open = self._next_opens.get(bar_type)
        # Not the pushed open less the interval: a broker day or week spans an hour more or less of
        # UTC across a DST change.
        last_open = pushed_open - 1
        rows = ()
        try:
            if first_open is not None and first_open <= last_open:
                rows = await self._missed_bars(bar_type, first_open, last_open)
        finally:
            if self._recoveries.get(bar_type) is recovery:
                del self._recoveries[bar_type]
                for row in rows:
                    self._emit_bar(bar_type, row)
                for frame in recovery.held:
                    self._emit_bar(bar_type, frame)

    async def _missed_bars(self, bar_type: BarType, first_open: int, last_open: int):
        """The bars of `bar_type` opened from `first_open` through `last_open` as the history serves
        them; none, with an error logged, when the read fails or the server does not answer."""
        series = _bar_subscription(bar_type).timeframe
        symbol = bar_type.instrument_id.symbol.value
        try:
            rows = await _read_in_thread(history.bars, symbol, series, first_open, last_open)
            failure = mt5.last_error()
        except MT5ConnectionError as exc:
            rows = None
            failure = exc
        if rows is None:
            self._log.error(
                f"MT5DataClient: {bar_type} bars opened {_iso(first_open)}..{_iso(last_open)} "
                f"not read back: {failure}"
            )
            return ()
        else:
            return rows

    def _instrument(self, symbol: str) -> InstrumentAny:
        """The loaded instrument of a venue symbol; raises MT5InstrumentError for one the provider
        has not loaded."""
        instrument = self._provider.get_instrument(symbol)
        if instrument is None:
            raise MT5InstrumentError(f"{symbol} is not loaded")
        return instrument

    # ── What the venue does not have ──────────────────────────────────────────

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

    async def _unsubscribe_index_prices(self, command) -> None:
        pass

    async def _unsubscribe_funding_rates(self, command) -> None:
        pass

    async def _unsubscribe_instrument_status(self, command) -> None:
        pass

    async def _unsubscribe_instrument_close(self, command) -> None:
        pass

    # ── History ───────────────────────────────────────────────────────────────

    async def _request(self, request: RequestData) -> None:
        pass

    async def _request_instrument(self, request: RequestInstrument) -> None:
        instrument = self._provider.load_symbol(request.instrument_id.symbol.value)
        _register_settlement([instrument])
        self._handle_instrument(instrument, request.id, request.start, request.end, request.params)

    async def _request_instruments(self, request: RequestInstruments) -> None:
        await self._provider.load_all_async()
        instruments = self._provider.list_all()
        _register_settlement(instruments)
        self._handle_instruments(
            self._config.venue, instruments, request.id, request.start, request.end, request.params
        )

    async def _request_quote_ticks(self, request: RequestQuoteTicks) -> None:
        symbol = request.instrument_id.symbol.value
        instrument = self._instrument(symbol)
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
        """Hands NT the venue bars of the bar type requested; raises ValueError for a step the
        terminal has no timeframe for."""
        symbol = request.bar_type.instrument_id.symbol.value
        series = venue_series(request.bar_type)
        instrument = self._instrument(symbol)
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
                f"MT5DataClient: {request.bar_type} bars closing "
                f"{_iso(first_close)}..{_iso(last_close)} failed: {mt5.last_error()}"
            )
        else:
            bars = [venue_bar(row, request.bar_type, instrument) for row in rows]
            self._handle_bars(
                request.bar_type, bars, request.id, request.start, request.end, request.params
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


def _bar_subscription(bar_type: BarType) -> Subscription:
    """The push stream of a venue bar type: its symbol's bars of the terminal's series."""
    return Subscription(Stream.BARS, bar_type.instrument_id.symbol.value, venue_series(bar_type))


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


def _register_settlement(instruments: list[InstrumentAny]) -> None:
    """Registers each instrument's settlement currency over what NT holds: NT's cache registers it
    without overwriting, so a precision NT guessed for the code would stand."""
    for instrument in instruments:
        register_venue_currency(instrument.quote_currency)
