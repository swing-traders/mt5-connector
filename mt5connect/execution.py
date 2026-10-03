"""The NT execution client for an MT5 hedging account: one trader's orders at the venue, the order
events the venue confirms, and the reports NT's reconciliation reads.

State, none of which survives a restart on its own — connect rebuilds it from NT's cache and the
venue:

- the ticket index, venue order ticket ↔ client order id: rebuilt at connect from NT's orders the
  venue accepted, extended by each accepted submit and each comment the digest lane matches;
- the deals already seen, and the time of the last one, where the next deal read starts;
- the tickets of this trader's orders resting at the venue at the last poll, and those that left it
  with no final state in the venue's history yet;
- the tickets no NT order explains, logged once each;
- the poll steps awaiting recovery;
- whether an account report is owed, and when the next one is due."""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import IntFlag, StrEnum
from hashlib import sha256
from operator import attrgetter
from typing import TYPE_CHECKING

from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock, MessageBus
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.messages import (
    BatchCancelOrders,
    CancelAllOrders,
    CancelOrder,
    GenerateFillReports,
    GenerateOrderStatusReport,
    GenerateOrderStatusReports,
    GeneratePositionStatusReports,
    ModifyOrder,
    SubmitOrder,
    SubmitOrderList,
)
from nautilus_trader.execution.reports import (
    FillReport,
    OrderStatusReport,
    PositionStatusReport,
)
from nautilus_trader.live.execution_client import LiveExecutionClient
from nautilus_trader.model.enums import (
    AccountType,
    ContingencyType,
    LiquiditySide,
    OmsType,
    OrderSide,
    OrderStatus,
    OrderType,
    PositionSide,
    TimeInForce,
    TriggerType,
    order_type_to_str,
    time_in_force_to_str,
)
from nautilus_trader.model.identifiers import (
    AccountId,
    ClientId,
    ClientOrderId,
    InstrumentId,
    PositionId,
    Symbol,
    TradeId,
    TraderId,
    VenueOrderId,
)
from nautilus_trader.model.objects import AccountBalance, Currency, Money, Price

from mt5connect import mirror
from mt5connect import remote_mt5 as mt5
from mt5connect.connection import MarginMode
from mt5connect.constants import MT5_VENUE
from mt5connect.currencies import register_venue_currency, venue_currency
from mt5connect.errors import (
    MT5ConfigError,
    MT5ConnectionError,
    MT5InstrumentError,
    MT5OrderError,
    ResponseLost,
)
from mt5connect.parsing import InstrumentAny, finite_decimal

if TYPE_CHECKING:
    from nautilus_trader.model.orders import Order

    from mt5connect.config import MT5Config
    from mt5connect.connection import MT5Connection
    from mt5connect.providers import MT5InstrumentProvider


def magic_for(trader_id: TraderId) -> int:
    """The magic marking the orders of the trader `trader_id`: the first 8 bytes of its SHA-256,
    masked to 63 bits so the venue's ulong and long readings of it agree."""
    digest = sha256(trader_id.value.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") & 0x7FFF_FFFF_FFFF_FFFF


# The venue refuses a comment of 30 characters or more.
_COMMENT_LENGTH = 29


def order_comment(client_order_id: ClientOrderId) -> str:
    """The comment an order carries to the venue: its client order id's SHA-256 in hex, cut to the
    length the venue accepts."""
    return sha256(client_order_id.value.encode("utf-8")).hexdigest()[:_COMMENT_LENGTH]


class SymbolFilling(IntFlag):
    """The fillings a symbol allows besides RETURN, which every symbol allows and no flag marks:
    `symbol_info().filling_mode`."""

    FOK = 1
    IOC = 2
    BOC = 4


class OrderState(StrEnum):
    """An order's state at the venue: `TradeOrder.state`."""

    STARTED = "STARTED"
    PLACED = "PLACED"
    CANCELED = "CANCELED"
    PARTIAL = "PARTIAL"
    FILLED = "FILLED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    REQUEST_ADD = "REQUEST_ADD"
    REQUEST_MODIFY = "REQUEST_MODIFY"
    REQUEST_CANCEL = "REQUEST_CANCEL"


class PollStep(StrEnum):
    """The independently retried steps of an execution poll."""

    VENUE_READ = "venue read"
    ACCOUNT_REPORT = "account report"


class SendOutcome(StrEnum):
    """What the venue's answer to a trade request proves."""

    DONE = "DONE"
    REFUSED = "REFUSED"
    # No answer proves either way; the venue may have acted on the request.
    LOST = "LOST"
    NOT_SENT = "NOT_SENT"


@dataclass(frozen=True)
class _Sent:
    """A trade request's outcome, its reason, and the venue's result when one came back."""

    outcome: SendOutcome
    reason: str
    result: tuple | None = None


_ORDER_STATES = {
    mirror.ORDER_STATE_STARTED: OrderState.STARTED,
    mirror.ORDER_STATE_PLACED: OrderState.PLACED,
    mirror.ORDER_STATE_CANCELED: OrderState.CANCELED,
    mirror.ORDER_STATE_PARTIAL: OrderState.PARTIAL,
    mirror.ORDER_STATE_FILLED: OrderState.FILLED,
    mirror.ORDER_STATE_REJECTED: OrderState.REJECTED,
    mirror.ORDER_STATE_EXPIRED: OrderState.EXPIRED,
    mirror.ORDER_STATE_REQUEST_ADD: OrderState.REQUEST_ADD,
    mirror.ORDER_STATE_REQUEST_MODIFY: OrderState.REQUEST_MODIFY,
    mirror.ORDER_STATE_REQUEST_CANCEL: OrderState.REQUEST_CANCEL,
}
_STATUSES = {
    OrderState.STARTED: OrderStatus.ACCEPTED,
    OrderState.PLACED: OrderStatus.ACCEPTED,
    OrderState.CANCELED: OrderStatus.CANCELED,
    OrderState.PARTIAL: OrderStatus.PARTIALLY_FILLED,
    OrderState.FILLED: OrderStatus.FILLED,
    OrderState.REJECTED: OrderStatus.REJECTED,
    OrderState.EXPIRED: OrderStatus.EXPIRED,
    OrderState.REQUEST_ADD: OrderStatus.ACCEPTED,
    OrderState.REQUEST_MODIFY: OrderStatus.ACCEPTED,
    OrderState.REQUEST_CANCEL: OrderStatus.ACCEPTED,
}
# How a pending order ends other than filled; a fill's end is its deal's.
_ENDS = frozenset({OrderState.CANCELED, OrderState.EXPIRED, OrderState.REJECTED})

_VENUE_ORDER_TYPES = {
    mirror.ORDER_TYPE_BUY: (OrderSide.BUY, OrderType.MARKET),
    mirror.ORDER_TYPE_SELL: (OrderSide.SELL, OrderType.MARKET),
    mirror.ORDER_TYPE_BUY_LIMIT: (OrderSide.BUY, OrderType.LIMIT),
    mirror.ORDER_TYPE_SELL_LIMIT: (OrderSide.SELL, OrderType.LIMIT),
    mirror.ORDER_TYPE_BUY_STOP: (OrderSide.BUY, OrderType.STOP_MARKET),
    mirror.ORDER_TYPE_SELL_STOP: (OrderSide.SELL, OrderType.STOP_MARKET),
    mirror.ORDER_TYPE_BUY_STOP_LIMIT: (OrderSide.BUY, OrderType.STOP_LIMIT),
    mirror.ORDER_TYPE_SELL_STOP_LIMIT: (OrderSide.SELL, OrderType.STOP_LIMIT),
}
_PENDING_TYPES = {
    (OrderType.LIMIT, OrderSide.BUY): mirror.ORDER_TYPE_BUY_LIMIT,
    (OrderType.LIMIT, OrderSide.SELL): mirror.ORDER_TYPE_SELL_LIMIT,
    (OrderType.STOP_MARKET, OrderSide.BUY): mirror.ORDER_TYPE_BUY_STOP,
    (OrderType.STOP_MARKET, OrderSide.SELL): mirror.ORDER_TYPE_SELL_STOP,
    (OrderType.STOP_LIMIT, OrderSide.BUY): mirror.ORDER_TYPE_BUY_STOP_LIMIT,
    (OrderType.STOP_LIMIT, OrderSide.SELL): mirror.ORDER_TYPE_SELL_STOP_LIMIT,
}
_TIMES_IN_FORCE = {
    mirror.ORDER_TIME_GTC: TimeInForce.GTC,
    mirror.ORDER_TIME_DAY: TimeInForce.DAY,
    mirror.ORDER_TIME_SPECIFIED: TimeInForce.GTD,
    mirror.ORDER_TIME_SPECIFIED_DAY: TimeInForce.GTD,
}
_PENDING_ORDER_TYPES = frozenset({OrderType.LIMIT, OrderType.STOP_MARKET, OrderType.STOP_LIMIT})
_ORDER_TYPES = _PENDING_ORDER_TYPES | {OrderType.MARKET}
_TIMES_IN_FORCE_SENT = frozenset({TimeInForce.GTC, TimeInForce.GTD})

_DONE_RETCODES = frozenset(
    {mirror.TRADE_RETCODE_DONE, mirror.TRADE_RETCODE_PLACED, mirror.TRADE_RETCODE_DONE_PARTIAL}
)
_RETCODE_NAMES = {
    value: name for name, value in mirror.CONSTANTS.items() if name.startswith("TRADE_RETCODE_")
}
_FILL_ENTRIES = frozenset(
    {mirror.DEAL_ENTRY_IN, mirror.DEAL_ENTRY_OUT, mirror.DEAL_ENTRY_INOUT, mirror.DEAL_ENTRY_OUT_BY}
)


class MT5LiveExecutionClient(LiveExecutionClient):
    """Live execution client for an MT5 hedging account, trading for the trader it is built for."""

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
            oms_type=OmsType.HEDGING,
            account_type=AccountType.MARGIN,
            base_currency=None,  # MT5 accounts are multi-currency
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=instrument_provider,
        )
        self._conn = connection
        self._config = config
        self._provider = instrument_provider
        self._magic = magic_for(self.trader_id)
        self._account_currency: Currency | None = None
        self._exec_poll_task: asyncio.Task | None = None
        self._client_order_ids: dict[int, ClientOrderId] = {}
        self._tickets: dict[ClientOrderId, int] = {}
        self._seen_deals: set[int] = set()
        self._deals_since_ms = 0
        self._resting: set[int] = set()
        self._vanished: set[int] = set()
        self._unresolved: set[int] = set()
        self._account_owed = False
        self._account_due_ns = 0
        self._outages: set[PollStep] = set()

    # ── Connect / disconnect ──────────────────────────────────────────────────

    async def _connect(self) -> None:
        """Holds the account to a hedging, tradable session before anything is reported or sent,
        books under the account's login, loads the config's symbols and registers the currencies the
        account and the loaded instruments book in; then indexes NT's orders, takes in what the
        venue already holds, reports the account and starts polling. Raises MT5ConfigError for an
        account that books another way or a read-only session, RuntimeError on a connected client.
        """
        if self._exec_poll_task is not None:
            raise RuntimeError("execution client: already connected")
        self._conn.ensure_connected()
        account = self._conn.get_account_info()
        terminal = self._conn.get_terminal_info()
        if account.margin_mode != MarginMode.RETAIL_HEDGING:
            raise MT5ConfigError(
                f"the client declares {self.oms_type.name}, the account's margin mode is "
                f"{account.margin_mode}"
            )
        elif terminal["trade_allowed"] and not account.trade_allowed:
            raise MT5ConfigError("read-only (investor) session: the account does not allow trading")

        self._set_account_id(AccountId(f"{MT5_VENUE}-{account.login}"))
        await self._provider.load_ids_async(
            [InstrumentId(Symbol(symbol), MT5_VENUE) for symbol in self._config.symbols]
        )
        # Money mints at its currency's registered precision, which NT may hold at a guess.
        self._account_currency = venue_currency(account.currency, account)
        register_venue_currency(self._account_currency)
        for instrument in self._provider.list_all():
            register_venue_currency(instrument.quote_currency)

        self._index_nt_orders()
        self._take_in_venue()
        self._refresh_account()
        self._outages.clear()
        self._exec_poll_task = self._loop.create_task(
            self._exec_poll_loop(),
            name="MT5LiveExecutionClient._exec_poll_loop",
        )
        self._log.info(f"connected, polling every {self._config.exec_poll_interval_ms}ms")

    async def _disconnect(self) -> None:
        """Stops polling and drops the client's view of the venue; raises RuntimeError on a client
        never connected."""
        if self._exec_poll_task is None:
            raise RuntimeError("execution client: not connected")
        self._exec_poll_task.cancel()
        try:
            await self._exec_poll_task
        except asyncio.CancelledError:
            pass
        self._exec_poll_task = None
        self._client_order_ids.clear()
        self._tickets.clear()
        self._seen_deals.clear()
        self._resting.clear()
        self._vanished.clear()
        self._unresolved.clear()
        self._log.info("disconnected")

    def _index_nt_orders(self) -> None:
        """Rebuilds the ticket index from NT's orders at this venue that carry the venue's ticket;
        an order the venue never accepted has none and is reconciliation's to resolve."""
        self._client_order_ids.clear()
        self._tickets.clear()
        for order in self._cache.orders(venue=self.venue):
            if order.venue_order_id is not None:
                if order.venue_order_id.value.isdigit():
                    self._index(int(order.venue_order_id.value), order.client_order_id)
                else:
                    self._log.debug(f"{order.venue_order_id!r} is not a venue ticket")

    def _take_in_venue(self) -> None:
        """Marks every deal in the history over the lookback as seen — a deal present at connect is
        never emitted — and takes this trader's resting orders as the ones the poll watches."""
        now = self._clock.utc_now()
        since = now - timedelta(minutes=self._config.history_lookback_mins)
        deals = _answer("history_deals_get", mt5.history_deals_get(since, now))
        self._seen_deals = {deal.ticket for deal in deals}
        self._deals_since_ms = max(
            (deal.time_msc for deal in deals), default=since.value // 1_000_000
        )
        self._resting = {order.ticket for order in self._resting_orders()}
        self._vanished = set()
        self._unresolved = set()

    # ── The poll ──────────────────────────────────────────────────────────────

    async def _exec_poll_loop(self) -> None:
        """Polls the venue every poll interval, reconnecting a terminal session the connection
        reports lost; ends when a reconnect gives up."""
        while True:
            try:
                self._conn.ensure_connected()
            except MT5ConnectionError as exc:
                if not await self._reconnect(exc):
                    return
            else:
                self._poll_turn()
            await asyncio.sleep(self._config.exec_poll_interval_s)

    async def _reconnect(self, cause: MT5ConnectionError) -> bool:
        self._log.warning(f"terminal session lost: {cause}")
        reconnected = await self._conn.reconnect_async()
        if reconnected:
            self._log.info("terminal session reconnected")
        else:
            self._log.error("reconnect gave up: execution polling stops")
        return reconnected

    def _poll_turn(self) -> None:
        """Reads the venue once and emits what it confirms, then reports the account when a report
        is owed or the refresh period is up, whether or not the read completed."""
        with self._poll_step(PollStep.VENUE_READ):
            self._poll_venue()
        if self._account_owed or self._clock.timestamp_ns() >= self._account_due_ns:
            with self._poll_step(PollStep.ACCOUNT_REPORT):
                self._refresh_account()

    @contextmanager
    def _poll_step(self, step: PollStep):
        """Runs one step of a poll turn, which the next turn retries when it fails: a step the
        server does not answer is logged once per outage, any other failure with its stack."""
        try:
            yield
        except MT5ConnectionError as exc:
            if step not in self._outages:
                self._log.warning(f"execution poll {step}: {exc}")
            self._outages.add(step)
        except Exception as exc:
            self._log.exception(f"execution poll {step} failed", exc)
        else:
            if step in self._outages:
                self._outages.remove(step)
                self._log.info(f"execution poll {step}: the server answers again")

    def _poll_venue(self) -> None:
        """Emits the fills of the deals since the last one seen, accepts the resting orders the
        index learns, and ends the pending orders the venue ended. The deal read opens a second
        before the last deal, so one stamped in the same second as it is never missed."""
        since = datetime.fromtimestamp((self._deals_since_ms - 1_000) / 1_000, tz=UTC)
        deals = _answer("history_deals_get", mt5.history_deals_get(since, self._clock.utc_now()))
        for deal in sorted(deals, key=attrgetter("time_msc", "ticket")):
            self._on_deal(deal)
        resting = self._resting_orders()
        for venue_order in resting:
            self._on_resting(venue_order)
        tickets = {venue_order.ticket for venue_order in resting}
        self._vanished |= self._resting - tickets
        self._resting = tickets
        for ticket in sorted(self._vanished):
            self._on_vanished(ticket)

    def _on_deal(self, deal) -> None:
        """Emits the fill a deal of this trader carries, once per deal ticket. A deal whose order no
        order of NT's explains is logged and left to NT's reconciliation; one whose fill cannot be
        built raises before it counts as seen, so a later turn emits it."""
        if deal.ticket in self._seen_deals:
            return
        fill = self._is_fill(deal)
        fields = None
        if fill:
            self._learn_ticket(deal.order)
            order = self._indexed_order(deal.order)
            if order is not None:
                fields = self._fill_fields(order, deal)
        self._seen_deals.add(deal.ticket)
        self._deals_since_ms = max(self._deals_since_ms, deal.time_msc)
        if fields is not None:
            self.generate_order_filled(**fields)
            self._account_owed = True
        elif fill:
            self._log.info(
                f"deal {deal.ticket} of order {deal.order}: no order of this trader matches it, "
                "left to reconciliation"
            )

    def _on_resting(self, venue_order) -> None:
        """Indexes a resting order of this trader the index lacks when its comment is the digest of
        an in-flight NT order, and accepts that order under its ticket while NT holds it submitted.
        One matching nothing is logged once."""
        if venue_order.ticket in self._client_order_ids:
            return
        self._index_by_comment(venue_order.ticket, venue_order.comment)
        order = self._indexed_order(venue_order.ticket)
        if order is None:
            self._log_unresolved(venue_order.ticket)
        elif order.status == OrderStatus.SUBMITTED:
            self._accept(order, venue_order)

    def _log_unresolved(self, ticket: int) -> None:
        if ticket not in self._unresolved:
            self._unresolved.add(ticket)
            self._log.info(f"order {ticket}: no order of this trader matches it")

    def _on_vanished(self, ticket: int) -> None:
        """Emits the end of a pending order that left the venue's resting orders once the venue's
        history states it — cancelled, expired or rejected; a fill's end is its deal's. Until the
        history holds the order, the next turn asks again."""
        historical = _answer("history_orders_get", mt5.history_orders_get(ticket=ticket))
        if historical:
            self._vanished.discard(ticket)
            self._emit_end(historical[0])

    def _emit_end(self, venue_order) -> None:
        """Ends the NT order behind a venue order its history ended other than filled, through the
        index or the digest its comment carries, unless NT has closed it already. One matching no NT
        order is logged once."""
        state = _order_state(venue_order)
        if state in _ENDS:
            self._index_by_comment(venue_order.ticket, venue_order.comment)
            order = self._indexed_order(venue_order.ticket)
            if order is None:
                self._log_unresolved(venue_order.ticket)
            elif not order.is_closed:
                self._end(order, venue_order, state)

    def _accept(self, order: Order, venue_order) -> None:
        self.generate_order_accepted(
            order.strategy_id,
            order.instrument_id,
            order.client_order_id,
            VenueOrderId(str(venue_order.ticket)),
            venue_order.time_setup_msc * 1_000_000,
        )
        self._account_owed = True

    def _end(self, order: Order, venue_order, state: OrderState) -> None:
        # The venue placed the order before ending it, and NT expires only an accepted order.
        if order.status == OrderStatus.SUBMITTED and state != OrderState.REJECTED:
            self._accept(order, venue_order)
        venue_order_id = VenueOrderId(str(venue_order.ticket))
        ts_event = venue_order.time_done_msc * 1_000_000
        if state == OrderState.CANCELED:
            self.generate_order_canceled(
                order.strategy_id,
                order.instrument_id,
                order.client_order_id,
                venue_order_id,
                ts_event,
            )
        elif state == OrderState.EXPIRED:
            self.generate_order_expired(
                order.strategy_id,
                order.instrument_id,
                order.client_order_id,
                venue_order_id,
                ts_event,
            )
        else:
            self.generate_order_rejected(
                order.strategy_id,
                order.instrument_id,
                order.client_order_id,
                f"the venue rejected order {venue_order.ticket}",
                ts_event,
            )
        self._account_owed = True

    def _fill_fields(self, order: Order, deal) -> dict:
        """The OrderFilled a deal of an NT order carries, as generate_order_filled takes it; raises
        for a deal no fill can be built from."""
        instrument = self._instrument(deal.symbol)
        return {
            "strategy_id": order.strategy_id,
            "instrument_id": order.instrument_id,
            "client_order_id": order.client_order_id,
            "venue_order_id": VenueOrderId(str(deal.order)),
            "venue_position_id": PositionId(str(deal.position_id)),
            "trade_id": TradeId(str(deal.ticket)),
            "order_side": _deal_side(deal),
            "order_type": order.order_type,
            "last_qty": instrument.make_qty(deal.volume),
            "last_px": instrument.make_price(deal.price),
            "quote_currency": instrument.quote_currency,
            "commission": self._commission(deal),
            "liquidity_side": _liquidity_side(order.order_type),
            "ts_event": deal.time_msc * 1_000_000,
        }

    def _refresh_account(self) -> None:
        """Reports the account: its balance and credit in total, its margin locked and the rest
        free; NT's portfolio adds the open positions' unrealised P&L. A report that fails stays owed
        to the next poll turn."""
        self._account_owed = True
        account = self._conn.get_account_info()
        total = Money(account.balance + account.credit, self._account_currency)
        locked = Money(account.margin, self._account_currency)
        free = Money(total.as_decimal() - locked.as_decimal(), self._account_currency)
        self.generate_account_state(
            balances=[AccountBalance(total, locked, free)],
            margins=[],
            reported=True,
            ts_event=self._clock.timestamp_ns(),
        )
        self._account_owed = False
        self._account_due_ns = (
            self._clock.timestamp_ns() + self._config.account_refresh_seconds * 1_000_000_000
        )

    # ── Submit ────────────────────────────────────────────────────────────────

    async def _submit_order(self, command: SubmitOrder) -> None:
        """Sends an order as the venue's market deal or pending order, refusing before sending one
        the venue cannot hold as stated."""
        self.generate_order_submitted(
            command.order.strategy_id,
            command.order.instrument_id,
            command.order.client_order_id,
            self._clock.timestamp_ns(),
        )
        refusal = _refusal(command.order)
        if refusal is not None:
            self._reject(command.order, refusal)
        else:
            self._place(command.order)

    async def _submit_order_list(self, command: SubmitOrderList) -> None:
        """Refuses every order of a list: the venue links no order to another."""
        for order in command.order_list.orders:
            self.generate_order_submitted(
                order.strategy_id,
                order.instrument_id,
                order.client_order_id,
                self._clock.timestamp_ns(),
            )
            self._reject(order, "unsupported: order lists")

    def _place(self, order: Order) -> None:
        try:
            self._conn.ensure_connected()
            request = self._new_order_request(order)
        except (MT5ConnectionError, MT5InstrumentError, MT5OrderError) as exc:
            self._reject(order, f"not sent: {exc}")
        else:
            self._on_new_order_sent(order, _send(request))

    def _on_new_order_sent(self, order: Order, sent: _Sent) -> None:
        if sent.outcome == SendOutcome.DONE:
            self._index(sent.result.order, order.client_order_id)
            if order.order_type != OrderType.MARKET:
                self._resting.add(sent.result.order)
            self.generate_order_accepted(
                order.strategy_id,
                order.instrument_id,
                order.client_order_id,
                VenueOrderId(str(sent.result.order)),
                self._clock.timestamp_ns(),
            )
            self._refresh_account()
        elif sent.outcome == SendOutcome.REFUSED:
            self._reject(order, sent.reason)
            self._refresh_account()
        elif sent.outcome == SendOutcome.NOT_SENT:
            self._reject(order, sent.reason)
        else:
            self._log.warning(f"{order.client_order_id!r} submit: {sent.reason}; {_LOST}")

    def _new_order_request(self, order: Order) -> dict:
        """The trade request placing an order; raises when the venue cannot be asked for it."""
        instrument = self._instrument(order.instrument_id.symbol.value)
        request = {
            "symbol": instrument.raw_symbol.value,
            "volume": order.quantity.as_double(),
            "magic": self._magic,
            "comment": order_comment(order.client_order_id),
            "sl": 0.0,
            "tp": 0.0,
        }
        if order.order_type == OrderType.MARKET:
            if order.side == OrderSide.BUY:
                request["type"] = mirror.ORDER_TYPE_BUY
            else:
                request["type"] = mirror.ORDER_TYPE_SELL
            request["action"] = mirror.TRADE_ACTION_DEAL
            request["price"] = self._market_price(order)
            request["deviation"] = self._config.deviation_points
            request["type_filling"] = _market_filling(instrument)
        else:
            request["type"] = _PENDING_TYPES[(order.order_type, order.side)]
            request["action"] = mirror.TRADE_ACTION_PENDING
            request["type_filling"] = mirror.ORDER_FILLING_RETURN
            request |= _pending_prices(order, _limit_price(order), _trigger_price(order))
            request |= _expiry(order)
        return request

    def _market_price(self, order: Order) -> float:
        """The side's price of NT's cached quote, else of the terminal's last one: instant and
        request execution need a price, market execution ignores it. Raises MT5OrderError while the
        symbol has no quote."""
        quote = self._cache.quote_tick(order.instrument_id)
        if quote is not None:
            if order.side == OrderSide.BUY:
                return quote.ask_price.as_double()
            else:
                return quote.bid_price.as_double()
        last = _answer("symbol_info_tick", mt5.symbol_info_tick(order.instrument_id.symbol.value))
        if last.time == 0:
            raise MT5OrderError(f"{order.instrument_id.symbol} has no quote")
        elif order.side == OrderSide.BUY:
            return last.ask
        else:
            return last.bid

    def _reject(self, order: Order, reason: str) -> None:
        self.generate_order_rejected(
            order.strategy_id,
            order.instrument_id,
            order.client_order_id,
            reason,
            self._clock.timestamp_ns(),
        )

    # ── Modify ────────────────────────────────────────────────────────────────

    async def _modify_order(self, command: ModifyOrder) -> None:
        """Moves a pending order's prices at the venue, which cannot change an order's quantity."""
        order = self._cache.order(command.client_order_id)
        ticket = self._ticket(order)
        if command.quantity is not None and command.quantity != order.quantity:
            self._modify_rejected(order, ticket, "unsupported: quantity changes")
        elif order.order_type not in _PENDING_ORDER_TYPES:
            self._modify_rejected(
                order, ticket, f"unsupported: modifying a {order_type_to_str(order.order_type)}"
            )
        elif ticket is None:
            self._modify_rejected(order, ticket, "no venue order is known for it")
        else:
            self._modify_resting(order, ticket, command)

    def _modify_resting(self, order: Order, ticket: int, command: ModifyOrder) -> None:
        try:
            self._conn.ensure_connected()
            refusal = self._not_resting(ticket)
        except MT5ConnectionError as exc:
            self._modify_rejected(order, ticket, f"not sent: {exc}")
        else:
            if refusal is None:
                price = _stated(command.price, _limit_price(order))
                trigger = _stated(command.trigger_price, _trigger_price(order))
                request = {
                    "action": mirror.TRADE_ACTION_MODIFY,
                    "order": ticket,
                    "sl": 0.0,
                    "tp": 0.0,
                }
                request |= _pending_prices(order, price, trigger) | _expiry(order)
                self._on_modify_sent(order, ticket, price, trigger, _send(request))
            else:
                self._modify_rejected(order, ticket, refusal)
                self._refresh_account()

    def _on_modify_sent(
        self, order: Order, ticket: int, price: Price | None, trigger: Price | None, sent: _Sent
    ) -> None:
        if sent.outcome == SendOutcome.DONE:
            self.generate_order_updated(
                order.strategy_id,
                order.instrument_id,
                order.client_order_id,
                VenueOrderId(str(ticket)),
                order.quantity,
                price,
                trigger,
                self._clock.timestamp_ns(),
            )
            self._refresh_account()
        elif sent.outcome == SendOutcome.REFUSED:
            self._modify_rejected(order, ticket, sent.reason)
            self._refresh_account()
        elif sent.outcome == SendOutcome.NOT_SENT:
            self._modify_rejected(order, ticket, sent.reason)
        else:
            self._log.warning(f"{order.client_order_id!r} modify: {sent.reason}; {_LOST}")

    def _modify_rejected(self, order: Order, ticket: int | None, reason: str) -> None:
        self.generate_order_modify_rejected(
            order.strategy_id,
            order.instrument_id,
            order.client_order_id,
            _venue_order_id(ticket),
            reason,
            self._clock.timestamp_ns(),
        )

    # ── Cancel ────────────────────────────────────────────────────────────────

    async def _cancel_order(self, command: CancelOrder) -> None:
        """Removes a pending order from the venue; a cancel never closes a position."""
        self._cancel(self._cache.order(command.client_order_id))

    async def _cancel_all_orders(self, command: CancelAllOrders) -> None:
        """Removes the commanding strategy's pending orders on the instrument, of the command's side
        when it names one; the other strategies' orders under the trader stay."""
        self._conn.ensure_connected()
        venue_orders = _answer(
            "orders_get", mt5.orders_get(symbol=command.instrument_id.symbol.value)
        )
        ours = [venue_order for venue_order in venue_orders if venue_order.magic == self._magic]
        for venue_order in ours:
            order = self._order_behind(venue_order)
            if order is not None and _commanded(order, command):
                self._remove(order, venue_order.ticket)

    async def _batch_cancel_orders(self, command: BatchCancelOrders) -> None:
        """Removes each pending order the batch names, as a cancel of each would."""
        for cancel in command.cancels:
            self._cancel(self._cache.order(cancel.client_order_id))

    def _cancel(self, order: Order) -> None:
        ticket = self._ticket(order)
        if ticket is None:
            self._cancel_rejected(order, ticket, "no venue order is known for it")
        else:
            self._remove(order, ticket)

    def _remove(self, order: Order, ticket: int) -> None:
        try:
            self._conn.ensure_connected()
            refusal = self._not_resting(ticket)
        except MT5ConnectionError as exc:
            self._cancel_rejected(order, ticket, f"not sent: {exc}")
        else:
            if refusal is None:
                request = {"action": mirror.TRADE_ACTION_REMOVE, "order": ticket}
                self._on_remove_sent(order, ticket, _send(request))
            else:
                self._cancel_rejected(order, ticket, refusal)
                self._refresh_account()

    def _on_remove_sent(self, order: Order, ticket: int, sent: _Sent) -> None:
        if sent.outcome == SendOutcome.DONE:
            # The poll would otherwise cancel it a second time once it leaves the resting orders.
            self._resting.discard(ticket)
            self._vanished.discard(ticket)
            self.generate_order_canceled(
                order.strategy_id,
                order.instrument_id,
                order.client_order_id,
                VenueOrderId(str(ticket)),
                self._clock.timestamp_ns(),
            )
            self._refresh_account()
        elif sent.outcome == SendOutcome.REFUSED:
            self._cancel_rejected(order, ticket, sent.reason)
            self._refresh_account()
        elif sent.outcome == SendOutcome.NOT_SENT:
            self._cancel_rejected(order, ticket, sent.reason)
        else:
            self._log.warning(f"{order.client_order_id!r} cancel: {sent.reason}; {_LOST}")

    def _cancel_rejected(self, order: Order, ticket: int | None, reason: str) -> None:
        self.generate_order_cancel_rejected(
            order.strategy_id,
            order.instrument_id,
            order.client_order_id,
            _venue_order_id(ticket),
            reason,
            self._clock.timestamp_ns(),
        )

    def _not_resting(self, ticket: int) -> str | None:
        """None while the venue rests an order under the ticket; else why it cannot act on one — the
        state its history ended it in, or that it holds no such order."""
        if _answer("orders_get", mt5.orders_get(ticket=ticket)):
            return None
        historical = _answer("history_orders_get", mt5.history_orders_get(ticket=ticket))
        if historical:
            return f"the order is {_order_state(historical[0])}"
        else:
            return f"the venue holds no order {ticket}"

    # ── Reports ───────────────────────────────────────────────────────────────

    async def generate_order_status_report(
        self,
        command: GenerateOrderStatusReport,
    ) -> OrderStatusReport | None:
        """The venue's order an NT order names — by its ticket, else by its comment's digest among
        the resting orders and the lookback's history — or None when the venue holds none."""
        if command.client_order_id is None and command.venue_order_id is None:
            raise ValueError("an order status report needs a client or a venue order id")
        self._conn.ensure_connected()
        ticket = self._report_ticket(command)
        if ticket is not None:
            venue_order = self._venue_order(ticket)
        else:
            venue_order = self._venue_order_by_comment(order_comment(command.client_order_id))
        if venue_order is None:
            return None
        elif venue_order.magic != self._magic:
            raise MT5OrderError(f"order {venue_order.ticket} carries another trader's magic")
        else:
            return self._order_report(venue_order)

    async def generate_order_status_reports(
        self,
        command: GenerateOrderStatusReports,
    ) -> list[OrderStatusReport]:
        """This trader's resting orders, with its orders in the venue's history over the command's
        window unless it asks for open orders only; one report per ticket."""
        self._conn.ensure_connected()
        if command.instrument_id is None:
            resting = _answer("orders_get", mt5.orders_get())
        else:
            resting = _answer(
                "orders_get", mt5.orders_get(symbol=command.instrument_id.symbol.value)
            )
        venue_orders = {order.ticket: order for order in resting if order.magic == self._magic}
        if not command.open_only:
            date_from, date_to = self._window(command)
            historical = _answer("history_orders_get", mt5.history_orders_get(date_from, date_to))
            for order in historical:
                if order.magic == self._magic and _in_scope(order.symbol, command.instrument_id):
                    venue_orders.setdefault(order.ticket, order)
        return [self._order_report(venue_order) for venue_order in venue_orders.values()]

    async def generate_fill_reports(self, command: GenerateFillReports) -> list[FillReport]:
        """The fills of this trader's deals in the venue's history over the command's window."""
        self._conn.ensure_connected()
        date_from, date_to = self._window(command)
        reports = []
        for deal in _answer("history_deals_get", mt5.history_deals_get(date_from, date_to)):
            if (
                self._is_fill(deal)
                and _in_scope(deal.symbol, command.instrument_id)
                and _of_order(deal, command.venue_order_id)
            ):
                reports.append(self._fill_report(deal))
        return reports

    async def generate_position_status_reports(
        self,
        command: GeneratePositionStatusReports,
    ) -> list[PositionStatusReport]:
        """One report per venue position of this trader: the venue hedges each under its own
        identifier."""
        self._conn.ensure_connected()
        if command.instrument_id is None:
            positions = _answer("positions_get", mt5.positions_get())
        else:
            positions = _answer(
                "positions_get", mt5.positions_get(symbol=command.instrument_id.symbol.value)
            )
        reports = []
        for position in positions:
            if position.magic == self._magic:
                reports.append(self._position_report(position))
        return reports

    def _report_ticket(self, command: GenerateOrderStatusReport) -> int | None:
        if command.venue_order_id is not None and command.venue_order_id.value.isdigit():
            return int(command.venue_order_id.value)
        else:
            return self._tickets.get(command.client_order_id)

    def _window(self, command) -> tuple[datetime, datetime]:
        """The history window a report reads: the command's bounds, else the config's lookback up to
        now."""
        now = self._clock.utc_now()
        if command.start is not None:
            date_from = command.start
        else:
            date_from = now - timedelta(minutes=self._config.history_lookback_mins)
        if command.end is not None:
            date_to = command.end
        else:
            date_to = now
        return date_from, date_to

    def _venue_order_by_comment(self, comment: str):
        """This trader's venue order carrying the comment, resting or in the history over the
        lookback; None when neither holds one."""
        for venue_order in _answer("orders_get", mt5.orders_get()):
            if venue_order.magic == self._magic and venue_order.comment == comment:
                return venue_order
        now = self._clock.utc_now()
        since = now - timedelta(minutes=self._config.history_lookback_mins)
        for venue_order in _answer("history_orders_get", mt5.history_orders_get(since, now)):
            if venue_order.magic == self._magic and venue_order.comment == comment:
                return venue_order
        return None

    def _order_report(self, venue_order) -> OrderStatusReport:
        instrument = self._instrument(venue_order.symbol)
        side, order_type = _venue_order_type(venue_order)
        time_in_force = _time_in_force(venue_order)
        price, trigger = _report_prices(venue_order, order_type, instrument)
        if trigger is None:
            trigger_type = TriggerType.NO_TRIGGER
        else:
            trigger_type = TriggerType.DEFAULT
        if time_in_force == TimeInForce.GTD and venue_order.time_expiration != 0:
            expire_time = datetime.fromtimestamp(venue_order.time_expiration, tz=UTC)
        else:
            expire_time = None
        volume_initial = finite_decimal(venue_order.volume_initial, "volume_initial")
        volume_current = finite_decimal(venue_order.volume_current, "volume_current")
        return OrderStatusReport(
            account_id=self.account_id,
            instrument_id=instrument.id,
            venue_order_id=VenueOrderId(str(venue_order.ticket)),
            order_side=side,
            order_type=order_type,
            time_in_force=time_in_force,
            order_status=_STATUSES[_order_state(venue_order)],
            quantity=instrument.make_qty(volume_initial),
            filled_qty=instrument.make_qty(volume_initial - volume_current),
            report_id=UUID4(),
            ts_accepted=venue_order.time_setup_msc * 1_000_000,
            ts_last=_last_update_ms(venue_order) * 1_000_000,
            ts_init=self._clock.timestamp_ns(),
            client_order_id=self._client_order_id_of(venue_order),
            expire_time=expire_time,
            price=price,
            trigger_price=trigger,
            trigger_type=trigger_type,
            post_only=False,
            reduce_only=False,
        )

    def _fill_report(self, deal) -> FillReport:
        instrument = self._instrument(deal.symbol)
        client_order_id, order_type = self._deal_order(deal)
        return FillReport(
            account_id=self.account_id,
            instrument_id=instrument.id,
            venue_order_id=VenueOrderId(str(deal.order)),
            trade_id=TradeId(str(deal.ticket)),
            order_side=_deal_side(deal),
            last_qty=instrument.make_qty(deal.volume),
            last_px=instrument.make_price(deal.price),
            commission=self._commission(deal),
            liquidity_side=_liquidity_side(order_type),
            report_id=UUID4(),
            ts_event=deal.time_msc * 1_000_000,
            ts_init=self._clock.timestamp_ns(),
            client_order_id=client_order_id,
            venue_position_id=PositionId(str(deal.position_id)),
        )

    def _position_report(self, position) -> PositionStatusReport:
        instrument = self._instrument(position.symbol)
        if position.type == mirror.POSITION_TYPE_BUY:
            side = PositionSide.LONG
        elif position.type == mirror.POSITION_TYPE_SELL:
            side = PositionSide.SHORT
        else:
            raise MT5OrderError(f"position {position.identifier}: type {position.type} is unknown")
        return PositionStatusReport(
            account_id=self.account_id,
            instrument_id=instrument.id,
            position_side=side,
            quantity=instrument.make_qty(position.volume),
            report_id=UUID4(),
            ts_last=position.time_update_msc * 1_000_000,
            ts_init=self._clock.timestamp_ns(),
            venue_position_id=PositionId(str(position.identifier)),
        )

    def _deal_order(self, deal) -> tuple[ClientOrderId | None, OrderType | None]:
        """The client order id and order type behind a deal: NT's indexed order's, else the venue
        order's type with the in-flight NT order its comment digests, if any."""
        order = self._indexed_order(deal.order)
        if order is not None:
            return order.client_order_id, order.order_type
        venue_order = self._venue_order(deal.order)
        if venue_order is None:
            return None, None
        else:
            _, order_type = _venue_order_type(venue_order)
            return self._in_flight_by_comment().get(venue_order.comment), order_type

    # ── Identity ──────────────────────────────────────────────────────────────

    def _index(self, ticket: int, client_order_id: ClientOrderId) -> None:
        self._client_order_ids[ticket] = client_order_id
        self._tickets[client_order_id] = ticket

    def _learn_ticket(self, ticket: int) -> None:
        """Indexes a ticket the index lacks when its venue order's comment is the digest of an
        in-flight NT order."""
        if ticket not in self._client_order_ids:
            venue_order = self._venue_order(ticket)
            if venue_order is not None:
                self._index_by_comment(ticket, venue_order.comment)

    def _index_by_comment(self, ticket: int, comment: str) -> None:
        """Indexes a ticket the index lacks when the comment its venue order carries is the digest
        of an in-flight NT order."""
        if ticket not in self._client_order_ids:
            client_order_id = self._in_flight_by_comment().get(comment)
            if client_order_id is not None:
                self._index(ticket, client_order_id)

    def _indexed_order(self, ticket: int) -> Order | None:
        client_order_id = self._client_order_ids.get(ticket)
        if client_order_id is not None:
            return self._cache.order(client_order_id)
        else:
            return None

    def _order_behind(self, venue_order) -> Order | None:
        """NT's order behind a venue order, through the index or the digest its comment carries."""
        client_order_id = self._client_order_id_of(venue_order)
        if client_order_id is not None:
            return self._cache.order(client_order_id)
        else:
            return None

    def _client_order_id_of(self, venue_order) -> ClientOrderId | None:
        """The client order id of a venue order: the index's, else that of the in-flight NT order
        its comment digests."""
        client_order_id = self._client_order_ids.get(venue_order.ticket)
        if client_order_id is not None:
            return client_order_id
        else:
            return self._in_flight_by_comment().get(venue_order.comment)

    def _in_flight_by_comment(self) -> dict[str, ClientOrderId]:
        """NT's open and in-flight orders at this venue, by the comment each carries to it."""
        orders = self._cache.orders_open(venue=self.venue) + self._cache.orders_inflight(
            venue=self.venue
        )
        return {order_comment(order.client_order_id): order.client_order_id for order in orders}

    def _ticket(self, order: Order) -> int | None:
        """The venue ticket of an NT order: its venue order id, else the index's."""
        if order.venue_order_id is not None and order.venue_order_id.value.isdigit():
            return int(order.venue_order_id.value)
        else:
            return self._tickets.get(order.client_order_id)

    # ── Venue reads ───────────────────────────────────────────────────────────

    def _resting_orders(self) -> list:
        """This trader's orders resting at the venue."""
        resting = _answer("orders_get", mt5.orders_get())
        return [order for order in resting if order.magic == self._magic]

    def _venue_order(self, ticket: int):
        """The venue's order under a ticket — resting, else in its history — or None when it holds
        none."""
        resting = _answer("orders_get", mt5.orders_get(ticket=ticket))
        if resting:
            return resting[0]
        historical = _answer("history_orders_get", mt5.history_orders_get(ticket=ticket))
        if historical:
            return historical[0]
        else:
            return None

    def _is_fill(self, deal) -> bool:
        """Whether a deal is a fill of this trader's: a buy or a sell of some volume that enters or
        leaves a position."""
        return (
            deal.magic == self._magic
            and deal.type in (mirror.DEAL_TYPE_BUY, mirror.DEAL_TYPE_SELL)
            and deal.entry in _FILL_ENTRIES
            and deal.volume > 0
        )

    def _instrument(self, symbol: str) -> InstrumentAny:
        """The loaded instrument of a venue symbol; raises MT5InstrumentError for one the provider
        has not loaded."""
        instrument = self._provider.get_instrument(symbol)
        if instrument is None:
            raise MT5InstrumentError(f"{symbol} is not loaded")
        return instrument

    def _commission(self, deal) -> Money:
        """What a deal charged, in the account currency: the venue states a charge negative, so a
        positive commission is a rebate."""
        charge = finite_decimal(deal.commission, "commission") + finite_decimal(deal.fee, "fee")
        return Money(-charge, self._account_currency)


# ─────────────────────────────────────────────────────────────────────────────
# MODULE HELPERS
# ─────────────────────────────────────────────────────────────────────────────

_LOST = "the venue may have acted on it, left to reconciliation"


def _answer(function: str, value):
    """A read's answer; raises MT5ConnectionError for the package's failure, None."""
    if value is None:
        code, message = mt5.last_error()
        raise MT5ConnectionError(f"{function} failed — error {code}: {message}")
    return value


def _send(request: dict) -> _Sent:
    """Sends a trade request and classifies what the venue's answer to it proves."""
    try:
        result = mt5.order_send(request)
    except ResponseLost as exc:
        return _Sent(SendOutcome.LOST, str(exc))
    except MT5ConnectionError as exc:
        return _Sent(SendOutcome.NOT_SENT, f"not sent: {exc}")
    if result is None:
        code, message = mt5.last_error()
        return _Sent(SendOutcome.LOST, f"order_send failed — error {code}: {message}")
    elif result.retcode in _DONE_RETCODES:
        return _Sent(SendOutcome.DONE, _retcode_reason(result), result)
    elif result.retcode == mirror.TRADE_RETCODE_CONNECTION:
        return _Sent(SendOutcome.LOST, _retcode_reason(result), result)
    else:
        return _Sent(SendOutcome.REFUSED, _retcode_reason(result), result)


def _retcode_reason(result) -> str:
    if result.retcode in _RETCODE_NAMES:
        return f"{_RETCODE_NAMES[result.retcode]}: {result.comment}"
    else:
        return f"retcode {result.retcode}: {result.comment}"


def _refusal(order: Order) -> str | None:
    """Why the venue cannot hold an order as it is stated, or None when it can."""
    if order.is_reduce_only:
        return "unsupported: reduce-only orders (position exits are not translated)"
    elif order.order_type not in _ORDER_TYPES:
        return f"unsupported: order type {order_type_to_str(order.order_type)}"
    elif order.time_in_force not in _TIMES_IN_FORCE_SENT:
        return f"unsupported: time in force {time_in_force_to_str(order.time_in_force)}"
    elif order.is_post_only:
        return "unsupported: post-only"
    elif order.contingency_type != ContingencyType.NO_CONTINGENCY:
        return "unsupported: contingent orders"
    else:
        return None


def _market_filling(instrument: InstrumentAny) -> int:
    """The filling a market deal takes: IOC where the symbol allows it, else FOK, else RETURN."""
    allowed = SymbolFilling(instrument.info["filling_mode"])
    if SymbolFilling.IOC in allowed:
        return mirror.ORDER_FILLING_IOC
    elif SymbolFilling.FOK in allowed:
        return mirror.ORDER_FILLING_FOK
    else:
        return mirror.ORDER_FILLING_RETURN


def _pending_prices(order: Order, price: Price | None, trigger: Price | None) -> dict:
    """A pending request's prices: a limit rests at its price, a stop at its trigger, and a
    stop-limit at its trigger with its limit as the stoplimit."""
    if order.order_type == OrderType.LIMIT:
        return {"price": price.as_double()}
    elif order.order_type == OrderType.STOP_MARKET:
        return {"price": trigger.as_double()}
    else:
        return {"price": trigger.as_double(), "stoplimit": price.as_double()}


def _expiry(order: Order) -> dict:
    """A pending request's time in force: a GTD order expires at its UTC second, which the server
    converts to the broker's clock."""
    if order.time_in_force == TimeInForce.GTD:
        return {
            "type_time": mirror.ORDER_TIME_SPECIFIED,
            "expiration": order.expire_time_ns // 1_000_000_000,
        }
    else:
        return {"type_time": mirror.ORDER_TIME_GTC}


def _stated(value: Price | None, current: Price | None) -> Price | None:
    """The price a modify states, else the order's current one."""
    if value is not None:
        return value
    else:
        return current


def _limit_price(order: Order) -> Price | None:
    if order.has_price:
        return order.price
    else:
        return None


def _trigger_price(order: Order) -> Price | None:
    if order.has_trigger_price:
        return order.trigger_price
    else:
        return None


def _venue_order_id(ticket: int | None) -> VenueOrderId | None:
    if ticket is not None:
        return VenueOrderId(str(ticket))
    else:
        return None


def _in_scope(symbol: str, instrument_id: InstrumentId | None) -> bool:
    return instrument_id is None or instrument_id.symbol.value == symbol


def _of_order(deal, venue_order_id: VenueOrderId | None) -> bool:
    return venue_order_id is None or venue_order_id.value == str(deal.order)


def _commanded(order: Order, command: CancelAllOrders) -> bool:
    """Whether a cancel-all covers an order: the commanding strategy's, of the command's side when
    it names one."""
    return order.strategy_id == command.strategy_id and command.order_side in (
        OrderSide.NO_ORDER_SIDE,
        order.side,
    )


def _deal_side(deal) -> OrderSide:
    if deal.type == mirror.DEAL_TYPE_BUY:
        return OrderSide.BUY
    else:
        return OrderSide.SELL


def _liquidity_side(order_type: OrderType | None) -> LiquiditySide:
    """A resting limit's fill makes liquidity; a market or stop order's takes it."""
    if order_type in (OrderType.LIMIT, OrderType.STOP_LIMIT):
        return LiquiditySide.MAKER
    elif order_type in (OrderType.MARKET, OrderType.STOP_MARKET):
        return LiquiditySide.TAKER
    else:
        return LiquiditySide.NO_LIQUIDITY_SIDE


def _order_state(venue_order) -> OrderState:
    if venue_order.state not in _ORDER_STATES:
        raise MT5OrderError(f"order {venue_order.ticket}: state {venue_order.state} is unknown")
    return _ORDER_STATES[venue_order.state]


def _venue_order_type(venue_order) -> tuple[OrderSide, OrderType]:
    if venue_order.type not in _VENUE_ORDER_TYPES:
        raise MT5OrderError(f"order {venue_order.ticket}: type {venue_order.type} is not reported")
    return _VENUE_ORDER_TYPES[venue_order.type]


def _time_in_force(venue_order) -> TimeInForce:
    if venue_order.type_time not in _TIMES_IN_FORCE:
        raise MT5OrderError(
            f"order {venue_order.ticket}: time in force {venue_order.type_time} is unknown"
        )
    return _TIMES_IN_FORCE[venue_order.type_time]


def _report_prices(
    venue_order, order_type: OrderType, instrument: InstrumentAny
) -> tuple[Price | None, Price | None]:
    """A venue order's limit price and trigger: its open price is a limit's price and a stop's
    trigger, and a stop-limit carries its limit in its stoplimit price."""
    if order_type == OrderType.LIMIT:
        return instrument.make_price(venue_order.price_open), None
    elif order_type == OrderType.STOP_MARKET:
        return None, instrument.make_price(venue_order.price_open)
    elif order_type == OrderType.STOP_LIMIT:
        return (
            instrument.make_price(venue_order.price_stoplimit),
            instrument.make_price(venue_order.price_open),
        )
    else:
        return None, None


def _last_update_ms(venue_order) -> int:
    """When the venue last changed an order: when it ended, else when it was set up."""
    if venue_order.time_done_msc != 0:
        return venue_order.time_done_msc
    else:
        return venue_order.time_setup_msc
