"""The NT execution client for an MT5 hedging account: one trader's orders and position exits at the
venue, the order events the venue confirms, and the reports NT's reconciliation reads.

State, none of which survives a restart on its own — connect rebuilds it from NT's cache and the
venue:

- the ticket index, venue order ticket ↔ client order id: rebuilt at connect from every ticket NT's
  orders hold or held as their venue order id, extended by each accepted submit, each comment the
  digest lane matches and each execution an exit takes the ticket of;
- the deals already seen;
- the tickets of the orders whose end the client emitted;
- the tickets no NT order explains, logged once each;
- whether an account report is owed, when the next one is due, and whether the last one went
  unanswered.

An exit order keeps no state here: its synthetic venue order id,
`{identifier}-{SL|TP}-{generation}`, names the bracket it occupies, and it takes the ticket of each
order the venue executes that bracket with."""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
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
    TradeId,
    TraderId,
    VenueOrderId,
)
from nautilus_trader.model.objects import AccountBalance, MarginBalance, Money, Price, Quantity

from mt5connector.client import remote_mt5 as mt5
from mt5connector.client.connection import MarginMode
from mt5connector.client.constants import MT5_VENUE
from mt5connector.client.currencies import register_venue_currency, venue_currency
from mt5connector.client.errors import (
    MT5ConfigError,
    MT5ConnectionError,
    MT5InstrumentError,
    MT5OrderError,
    ResponseLost,
)
from mt5connector.client.parsing import InstrumentAny, finite_decimal
from mt5connector.client.push import PushClient
from mt5connector.wire import mirror
from mt5connector.wire.push_wire import FrameType, Stream, Subscription, TransactionType

if TYPE_CHECKING:
    from nautilus_trader.model.orders import Order

    from mt5connector.client.config import MT5Config
    from mt5connector.client.connection import MT5Connection
    from mt5connector.client.providers import MT5InstrumentProvider


def magic_for(trader_id: TraderId) -> int:
    """The magic marking the orders of the trader `trader_id`: the first 8 bytes of its SHA-256,
    masked to 63 bits so the venue's ulong and long readings of it agree."""
    digest = sha256(trader_id.value.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") & 0x7FFF_FFFF_FFFF_FFFF


def _account_id_of(login: int, magic: int) -> AccountId:
    """The account id a trader books under: the venue, the first 8 hex digits of the SHA-256 of the
    login's decimal string, so the login reaches no log, and the trader's magic, so the traders of
    one login book apart."""
    login_hash = sha256(str(login).encode("utf-8")).hexdigest()[:8]
    return AccountId(f"{MT5_VENUE}-{login_hash}-{magic}")


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


class DealType(StrEnum):
    """What a deal is: `TradeDeal.type`."""

    BUY = "BUY"
    SELL = "SELL"
    BALANCE = "BALANCE"
    CREDIT = "CREDIT"
    CHARGE = "CHARGE"
    CORRECTION = "CORRECTION"
    BONUS = "BONUS"
    COMMISSION = "COMMISSION"
    COMMISSION_DAILY = "COMMISSION_DAILY"
    COMMISSION_MONTHLY = "COMMISSION_MONTHLY"
    COMMISSION_AGENT_DAILY = "COMMISSION_AGENT_DAILY"
    COMMISSION_AGENT_MONTHLY = "COMMISSION_AGENT_MONTHLY"
    INTEREST = "INTEREST"
    BUY_CANCELED = "BUY_CANCELED"
    SELL_CANCELED = "SELL_CANCELED"
    DIVIDEND = "DIVIDEND"
    DIVIDEND_FRANKED = "DIVIDEND_FRANKED"
    TAX = "TAX"


class DealEntry(StrEnum):
    """How a deal stands to its position: `TradeDeal.entry`."""

    IN = "IN"
    OUT = "OUT"
    INOUT = "INOUT"
    OUT_BY = "OUT_BY"


class Reason(StrEnum):
    """What made a deal: `TradeDeal.reason`."""

    CLIENT = "CLIENT"
    MOBILE = "MOBILE"
    WEB = "WEB"
    EXPERT = "EXPERT"
    SL = "SL"
    TP = "TP"
    SO = "SO"
    ROLLOVER = "ROLLOVER"
    VMARGIN = "VMARGIN"
    SPLIT = "SPLIT"


class Slot(StrEnum):
    """A position's exit bracket at the venue, which holds one of each."""

    SL = "SL"
    TP = "TP"


@dataclass(frozen=True)
class _ExitId:
    """An exit order's synthetic venue order id: the position it exits, the bracket it occupies, and
    its place among the orders that have occupied that bracket."""

    identifier: int
    slot: Slot
    generation: int

    @property
    def venue_order_id(self) -> VenueOrderId:
        return VenueOrderId(f"{self.identifier}-{self.slot}-{self.generation}")


_EXIT_ID = re.compile(r"(\d+)-(SL|TP)-(\d+)")


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
# The venue answers a bracket request that sets what it already holds with "no changes".
_BRACKET_DONE_RETCODES = _DONE_RETCODES | {mirror.TRADE_RETCODE_NO_CHANGES}
_RETCODE_NAMES = {
    value: name for name, value in mirror.CONSTANTS.items() if name.startswith("TRADE_RETCODE_")
}
_DEAL_TYPES = {
    mirror.DEAL_TYPE_BUY: DealType.BUY,
    mirror.DEAL_TYPE_SELL: DealType.SELL,
    mirror.DEAL_TYPE_BALANCE: DealType.BALANCE,
    mirror.DEAL_TYPE_CREDIT: DealType.CREDIT,
    mirror.DEAL_TYPE_CHARGE: DealType.CHARGE,
    mirror.DEAL_TYPE_CORRECTION: DealType.CORRECTION,
    mirror.DEAL_TYPE_BONUS: DealType.BONUS,
    mirror.DEAL_TYPE_COMMISSION: DealType.COMMISSION,
    mirror.DEAL_TYPE_COMMISSION_DAILY: DealType.COMMISSION_DAILY,
    mirror.DEAL_TYPE_COMMISSION_MONTHLY: DealType.COMMISSION_MONTHLY,
    mirror.DEAL_TYPE_COMMISSION_AGENT_DAILY: DealType.COMMISSION_AGENT_DAILY,
    mirror.DEAL_TYPE_COMMISSION_AGENT_MONTHLY: DealType.COMMISSION_AGENT_MONTHLY,
    mirror.DEAL_TYPE_INTEREST: DealType.INTEREST,
    mirror.DEAL_TYPE_BUY_CANCELED: DealType.BUY_CANCELED,
    mirror.DEAL_TYPE_SELL_CANCELED: DealType.SELL_CANCELED,
    mirror.DEAL_DIVIDEND: DealType.DIVIDEND,
    mirror.DEAL_DIVIDEND_FRANKED: DealType.DIVIDEND_FRANKED,
    mirror.DEAL_TAX: DealType.TAX,
}
_DEAL_ENTRIES = {
    mirror.DEAL_ENTRY_IN: DealEntry.IN,
    mirror.DEAL_ENTRY_OUT: DealEntry.OUT,
    mirror.DEAL_ENTRY_INOUT: DealEntry.INOUT,
    mirror.DEAL_ENTRY_OUT_BY: DealEntry.OUT_BY,
}
_CLOSING_ENTRIES = frozenset({DealEntry.OUT, DealEntry.OUT_BY})
_DEAL_REASONS = {
    mirror.DEAL_REASON_CLIENT: Reason.CLIENT,
    mirror.DEAL_REASON_MOBILE: Reason.MOBILE,
    mirror.DEAL_REASON_WEB: Reason.WEB,
    mirror.DEAL_REASON_EXPERT: Reason.EXPERT,
    mirror.DEAL_REASON_SL: Reason.SL,
    mirror.DEAL_REASON_TP: Reason.TP,
    mirror.DEAL_REASON_SO: Reason.SO,
    mirror.DEAL_REASON_ROLLOVER: Reason.ROLLOVER,
    mirror.DEAL_REASON_VMARGIN: Reason.VMARGIN,
    mirror.DEAL_REASON_SPLIT: Reason.SPLIT,
}
# A stop-out is the venue closing the position itself, never its stop loss executing.
_BRACKET_SLOTS = {Reason.SL: Slot.SL, Reason.TP: Slot.TP}


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
        account = connection.get_account_info()
        super().__init__(
            loop=loop,
            client_id=ClientId(MT5_VENUE.value),
            venue=MT5_VENUE,
            oms_type=OmsType.HEDGING,
            account_type=AccountType.MARGIN,
            # The account keeps one balance, in its currency; NT takes it only at construction.
            base_currency=venue_currency(account.currency, account),
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=instrument_provider,
        )
        self._conn = connection
        self._config = config
        self._provider = instrument_provider
        self._magic = magic_for(self.trader_id)
        self._push = PushClient(config, loop, self._on_push_frame, None, self._log)
        self._account_task: asyncio.Task | None = None
        self._client_order_ids: dict[int, ClientOrderId] = {}
        self._tickets: dict[ClientOrderId, int] = {}
        self._seen_deals: set[int] = set()
        self._ended: set[int] = set()
        self._unresolved: set[int] = set()
        self._account_owed = False
        self._account_due_ns = 0
        self._account_unanswered = False

    # ── Connect / disconnect ──────────────────────────────────────────────────

    async def _connect(self) -> None:
        """Connects for this trader: refuses an account that is not a tradable hedging session
        before anything is reported or sent, then loads what the instrument provider's config names,
        takes in what the venue holds — emitting the bracket fills NT has yet to book — reports the
        account and subscribes its trade transactions. Raises MT5ConfigError for such an account or
        a provider config that names nothing to load, MT5OrderError naming a deal whose owed fill
        cannot be built, RuntimeError on a connected client."""
        if self._account_task is not None:
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

        self._set_account_id(_account_id_of(account.login, self._magic))
        await self._provider.initialize()
        # Money mints at its currency's registered precision, which NT may hold at a guess.
        register_venue_currency(self.base_currency)
        for instrument in self._provider.list_all():
            register_venue_currency(instrument.quote_currency)

        self._index_nt_orders()
        for deal in self._take_in_venue():
            try:
                self._on_deal(deal)
            except Exception as exc:
                raise MT5OrderError(
                    f"deal {deal.ticket}: cannot emit owed bracket fill: {exc}"
                ) from exc
        self._refresh_account()
        self._account_unanswered = False
        await self._push.subscribe(Subscription(Stream.TRADE_TRANSACTIONS))
        await self._push.connect()
        self._account_task = self._loop.create_task(
            self._account_loop(),
            name="MT5LiveExecutionClient._account_loop",
        )
        self._log.info(f"connected, the push channel at {self._config.ws_display_url}")

    async def _disconnect(self) -> None:
        """Stops the account loop and the push channel and drops the client's view of the venue;
        raises RuntimeError on a client never connected."""
        if self._account_task is None:
            raise RuntimeError("execution client: not connected")
        self._account_task.cancel()
        try:
            await self._account_task
        except asyncio.CancelledError:
            pass
        self._account_task = None
        await self._push.disconnect()
        self._client_order_ids.clear()
        self._tickets.clear()
        self._seen_deals.clear()
        self._ended.clear()
        self._unresolved.clear()
        self._log.info("disconnected")

    def _index_nt_orders(self) -> None:
        """Rebuilds the ticket index from every ticket NT's orders at this venue hold or held as
        their venue order id, so an exit keeps each execution its bracket took; an order the venue
        never accepted has none and is reconciliation's to resolve."""
        self._client_order_ids.clear()
        self._tickets.clear()
        for order in self._cache.orders(venue=self.venue):
            self._index_nt_order(order)

    def _index_nt_order(self, order: Order) -> None:
        venue_order_ids = [
            venue_order_id
            for venue_order_id in (*order.venue_order_ids, order.venue_order_id)
            if venue_order_id is not None
        ]
        for venue_order_id in venue_order_ids:
            if venue_order_id.value.isdigit():
                self._index(int(venue_order_id.value), order.client_order_id)
            else:
                self._log.debug(f"{venue_order_id!r} is not a venue ticket")

    def _take_in_venue(self) -> list:
        """Marks every deal in the history over the lookback as seen, so none present at connect is
        emitted — but a bracket's deal NT has not booked to the exit bound to its ticket or
        occupying its bracket, which it returns in the venue's order."""
        now = self._clock.utc_now()
        since = now - timedelta(minutes=self._config.history_lookback_mins)
        deals = _answer("history_deals_get", mt5.history_deals_get(since, now))
        owed = [deal for deal in deals if self._owed_bracket_fill(deal)]
        self._seen_deals = {deal.ticket for deal in deals} - {deal.ticket for deal in owed}
        self._ended = set()
        self._unresolved = set()
        return sorted(owed, key=attrgetter("time_msc", "ticket"))

    # ── The account ───────────────────────────────────────────────────────────

    async def _account_loop(self) -> None:
        """Takes an account turn every poll interval, reconnecting a terminal session the connection
        reports lost; ends when a reconnect gives up."""
        while True:
            try:
                self._conn.ensure_connected()
            except MT5ConnectionError as exc:
                if not await self._reconnect(exc):
                    return
            else:
                self._account_turn()
            await asyncio.sleep(self._config.exec_poll_interval_s)

    async def _reconnect(self, cause: MT5ConnectionError) -> bool:
        self._log.warning(f"terminal session lost: {cause}")
        reconnected = await self._conn.reconnect_async()
        if reconnected:
            self._log.info("terminal session reconnected")
        else:
            self._log.error("reconnect gave up: the account loop stops")
        return reconnected

    def _account_turn(self) -> None:
        """Reports the account when an event owes a report or the refresh period is up. A report the
        server does not answer stays owed and is logged once until one is answered, any other
        failure with its stack."""
        if self._account_owed or self._clock.timestamp_ns() >= self._account_due_ns:
            try:
                self._refresh_account()
            except MT5ConnectionError as exc:
                if not self._account_unanswered:
                    self._log.warning(f"account report: {exc}")
                self._account_unanswered = True
            except Exception as exc:
                self._log.exception("account report failed", exc)
            else:
                if self._account_unanswered:
                    self._account_unanswered = False
                    self._log.info("account report: the server answers again")

    # ── The push channel ──────────────────────────────────────────────────────

    def _on_push_frame(self, frame: dict) -> None:
        """Feeds a pushed trade transaction to the entry point its type names, as it arrives."""
        kind = FrameType(frame["type"])
        if kind == FrameType.TRADE_TRANSACTION:
            self._on_transaction(frame["transaction"], frame["request"], frame["result"])
        else:
            raise ValueError(f"a {kind} frame on the execution channel")

    def _on_transaction(self, transaction: dict, request: dict, result: dict) -> None:
        """A deal the venue added fills its order, an order the venue deleted or moved to its
        history ends, and a request the venue completed links the ticket it placed; no other
        transaction carries an event."""
        kind = TransactionType(transaction["type"])
        if kind == TransactionType.DEAL_ADD:
            self._on_deal_added(transaction["deal"])
        elif kind in (TransactionType.ORDER_DELETE, TransactionType.HISTORY_ADD):
            self._on_order_left(transaction["order"])
        elif kind == TransactionType.REQUEST:
            self._on_request(request, result)

    def _on_deal_added(self, ticket: int) -> None:
        """Emits the fill of a deal the venue added, read from its history by the ticket: the
        transaction carries none of the deal's time, magic, entry, reason or charges. A deal the
        history does not hold yet is left to reconciliation."""
        deals = _answer("history_deals_get", mt5.history_deals_get(ticket=ticket))
        if deals:
            self._on_deal(deals[0])
        else:
            self._log.info(f"deal {ticket}: not in the venue's history, left to reconciliation")

    def _on_order_left(self, ticket: int) -> None:
        """Ends, once, the NT order behind an order of this trader the venue ended other than
        filled, read from its history by the ticket; an order the history does not hold yet ends on
        the transaction that adds it there."""
        if ticket not in self._ended:
            historical = _answer("history_orders_get", mt5.history_orders_get(ticket=ticket))
            if historical and historical[0].magic == self._magic:
                self._emit_end(historical[0])

    def _on_request(self, request: dict, result: dict) -> None:
        """Indexes the ticket a completed request of this trader placed when the index lacks it and
        the request's comment is the digest of an in-flight NT order — the link a lost submit answer
        leaves undone — and accepts that order while NT holds it submitted."""
        if request["magic"] == self._magic and result["retcode"] in _DONE_RETCODES:
            self._index_by_comment(result["order"], request["comment"])
            order = self._indexed_order(result["order"])
            if order is not None and order.status == OrderStatus.SUBMITTED:
                self._accept_placed(order, result["order"])

    def _accept_placed(self, order: Order, ticket: int) -> None:
        """Accepts an order under the ticket the venue placed it as, when the venue set it up; one
        the venue holds no record of yet is reconciliation's to accept."""
        venue_order = self._venue_order(ticket)
        if venue_order is not None:
            self._accept(order, venue_order)

    def _on_deal(self, deal) -> None:
        """Emits the fill a deal carries, once per deal ticket, under the venue order the deal
        executed: a bracket's deal NT has not booked fills the exit bound to its ticket, else the
        order occupying the bracket, whatever magic the venue stamped on it, and any other deal of
        this trader the order its ticket or comment names. A deal of this trader no order of NT's
        explains is logged and left to NT's reconciliation; one whose fill cannot be built raises
        before it counts as seen, so a later delivery of it or NT's reconciliation emits it."""
        if deal.ticket in self._seen_deals:
            return
        slot = _bracket_slot(deal)
        fill = self._is_fill(deal)
        execution = VenueOrderId(str(deal.order))
        booked = None
        order = None
        if slot is not None:
            booked = _booked(self._cache.orders(venue=self.venue), deal, slot)
            if booked is None:
                order = self._bracket_owner(deal)
        elif fill:
            self._learn_ticket(deal.order)
            order = self._indexed_order(deal.order)
        fields = None
        if order is not None:
            fields = self._fill_fields(order, execution, deal)
        self._seen_deals.add(deal.ticket)
        if fields is not None:
            if slot is not None and self._cache.venue_order_id(order.client_order_id) != execution:
                self._bind_to_execution(order, execution, deal.time_msc * 1_000_000)
            self.generate_order_filled(**fields)
            self._account_owed = True
        elif booked is None and fill:
            self._log.info(
                f"deal {deal.ticket} of order {deal.order}: no order of this trader matches it, "
                "left to reconciliation"
            )

    def _bind_to_execution(self, order: Order, execution: VenueOrderId, ts_event: int) -> None:
        """Gives an exit the ticket of the order the venue executed its bracket with, in NT's index
        and the client's, then as its venue order id: NT applies a fill only under that id."""
        self._cache.add_venue_order_id(order.client_order_id, execution, overwrite=True)
        self._index(int(execution.value), order.client_order_id)
        self.generate_order_updated(
            order.strategy_id,
            order.instrument_id,
            order.client_order_id,
            execution,
            order.quantity,
            _limit_price(order),
            _trigger_price(order),
            ts_event,
            venue_order_id_modified=True,
        )

    def _log_unresolved(self, ticket: int) -> None:
        if ticket not in self._unresolved:
            self._unresolved.add(ticket)
            self._log.info(f"order {ticket}: no order of this trader matches it")

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
        self._ended.add(venue_order.ticket)
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

    def _fill_fields(self, order: Order, venue_order_id: VenueOrderId, deal) -> dict:
        """The OrderFilled a deal of an NT order carries, as generate_order_filled takes it; raises
        for a deal no fill can be built from."""
        instrument = self._instrument(deal.symbol)
        return {
            "strategy_id": order.strategy_id,
            "instrument_id": order.instrument_id,
            "client_order_id": order.client_order_id,
            "venue_order_id": venue_order_id,
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
        free, and one account-level margin — the margin the venue states as used as initial, the
        maintenance floor as maintenance; NT's portfolio adds the open positions' unrealised P&L. A
        report that fails stays owed to the next account turn."""
        self._account_owed = True
        account = self._conn.get_account_info()
        total = Money(account.balance + account.credit, self.base_currency)
        locked = Money(account.margin, self.base_currency)
        free = Money(total.as_decimal() - locked.as_decimal(), self.base_currency)
        margin = MarginBalance(
            initial=Money(account.margin, self.base_currency),
            maintenance=Money(account.margin_maintenance, self.base_currency),
            instrument_id=None,
        )
        self.generate_account_state(
            balances=[AccountBalance(total, locked, free)],
            margins=[margin],
            reported=True,
            ts_event=self._clock.timestamp_ns(),
        )
        self._account_owed = False
        self._account_due_ns = (
            self._clock.timestamp_ns() + self._config.account_refresh_seconds * 1_000_000_000
        )

    # ── Submit ────────────────────────────────────────────────────────────────

    async def _submit_order(self, command: SubmitOrder) -> None:
        """Sends an order as the venue's market deal or pending order, and a reduce-only one as an
        exit of the position the command names, refusing before sending one the venue cannot hold as
        stated."""
        self.generate_order_submitted(
            command.order.strategy_id,
            command.order.instrument_id,
            command.order.client_order_id,
            self._clock.timestamp_ns(),
        )
        refusal = _refusal(command.order, command.position_id)
        if refusal is not None:
            self._reject(command.order, refusal)
        elif command.order.is_reduce_only:
            self._place_exit(command.order, int(command.position_id.value))
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
        if order.order_type == OrderType.MARKET:
            request = self._deal_request(order, instrument)
        else:
            request = {
                "action": mirror.TRADE_ACTION_PENDING,
                "symbol": instrument.raw_symbol.value,
                "volume": order.quantity.as_double(),
                "type": _PENDING_TYPES[(order.order_type, order.side)],
                "magic": self._magic,
                "comment": order_comment(order.client_order_id),
                "type_filling": mirror.ORDER_FILLING_RETURN,
            }
            request |= _pending_prices(order, _limit_price(order), _trigger_price(order))
            request |= _expiry(order)
        return request | {"sl": 0.0, "tp": 0.0}

    def _deal_request(self, order: Order, instrument: InstrumentAny) -> dict:
        """A market deal of the order's side and quantity at the price that side trades at; raises
        MT5OrderError while the symbol has no quote."""
        request = {
            "action": mirror.TRADE_ACTION_DEAL,
            "symbol": instrument.raw_symbol.value,
            "volume": order.quantity.as_double(),
            "price": self._market_price(order),
            "deviation": self._config.deviation_points,
            "magic": self._magic,
            "comment": order_comment(order.client_order_id),
            "type_filling": _market_filling(instrument),
        }
        if order.side == OrderSide.BUY:
            request["type"] = mirror.ORDER_TYPE_BUY
        else:
            request["type"] = mirror.ORDER_TYPE_SELL
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

    # ── Exits ─────────────────────────────────────────────────────────────────

    def _place_exit(self, order: Order, identifier: int) -> None:
        """Sends a reduce-only order as the exit the venue holds for a position: a market order
        closes it, a stop becomes its stop loss and a limit its take profit."""
        if order.order_type == OrderType.MARKET:
            self._place_close(order, identifier)
        elif order.order_type == OrderType.STOP_MARKET:
            self._place_bracket(order, identifier, Slot.SL, order.trigger_price)
        else:
            self._place_bracket(order, identifier, Slot.TP, order.price)

    def _place_close(self, order: Order, identifier: int) -> None:
        try:
            self._conn.ensure_connected()
            request = self._close_request(order, identifier)
        except (MT5ConnectionError, MT5InstrumentError, MT5OrderError) as exc:
            self._reject(order, f"not sent: {exc}")
        else:
            self._on_new_order_sent(order, _send(request))

    def _close_request(self, order: Order, identifier: int) -> dict:
        """The deal closing the order's quantity of a position; raises MT5OrderError for a position
        the venue does not hold, and for a partial close while a target stands, which the venue's
        whole-position target cannot follow."""
        instrument = self._instrument(order.instrument_id.symbol.value)
        position = self._held_position(identifier, order.instrument_id.symbol.value)
        target = self._occupant(identifier, Slot.TP)
        if target is not None and order.quantity != instrument.make_qty(position.volume):
            raise MT5OrderError(
                f"a partial close of position {identifier} while {target.venue_order_id} stands"
            )
        return self._deal_request(order, instrument) | {"position": position.ticket}

    def _place_bracket(self, order: Order, identifier: int, slot: Slot, level: Price) -> None:
        try:
            self._conn.ensure_connected()
            position = self._held_position(identifier, order.instrument_id.symbol.value)
        except (MT5ConnectionError, MT5OrderError) as exc:
            self._reject(order, f"not sent: {exc}")
        else:
            venue_order_id = _next_exit_id(self._cache.orders(venue=self.venue), identifier, slot)
            displaced = self._occupant(identifier, slot)
            sent = _send(_sltp_request(position, slot, level, self._magic), _BRACKET_DONE_RETCODES)
            self._on_bracket_sent(order, venue_order_id, displaced, sent)

    def _on_bracket_sent(
        self, order: Order, venue_order_id: VenueOrderId, displaced: Order | None, sent: _Sent
    ) -> None:
        if sent.outcome == SendOutcome.DONE:
            ts_event = self._clock.timestamp_ns()
            self.generate_order_accepted(
                order.strategy_id,
                order.instrument_id,
                order.client_order_id,
                venue_order_id,
                ts_event,
            )
            # The venue holds one bracket of each kind: setting it took the place of the last.
            if displaced is not None:
                self.generate_order_canceled(
                    displaced.strategy_id,
                    displaced.instrument_id,
                    displaced.client_order_id,
                    displaced.venue_order_id,
                    ts_event,
                )
            self._refresh_account()
        elif sent.outcome == SendOutcome.REFUSED:
            self._reject(order, sent.reason)
            self._refresh_account()
        elif sent.outcome == SendOutcome.NOT_SENT:
            self._reject(order, sent.reason)
        else:
            self._log.warning(f"{order.client_order_id!r} exit: {sent.reason}; {_LOST}")

    def _modify_exit(self, order: Order, exit_id: _ExitId, command: ModifyOrder) -> None:
        """Moves the bracket an exit occupies to the level the modify states. The bracket is the
        whole position's, so the only quantity it takes is the order's fills and the position's
        volume together, and that one changes nothing at the venue."""
        refusal = self._not_occupying(order, exit_id)
        if refusal is not None:
            self._modify_rejected(order, order.venue_order_id, refusal)
        else:
            self._amend_bracket(order, exit_id, command)

    def _amend_bracket(self, order: Order, exit_id: _ExitId, command: ModifyOrder) -> None:
        try:
            self._conn.ensure_connected()
            instrument = self._instrument(order.instrument_id.symbol.value)
            position = self._held_position(exit_id.identifier, order.instrument_id.symbol.value)
        except (MT5ConnectionError, MT5InstrumentError, MT5OrderError) as exc:
            self._modify_rejected(order, order.venue_order_id, f"not sent: {exc}")
        else:
            level = _stated_level(command, exit_id.slot)
            whole = order.filled_qty + instrument.make_qty(position.volume)
            if command.quantity is not None and command.quantity != whole:
                self._modify_rejected(
                    order,
                    order.venue_order_id,
                    f"unsupported: quantity {command.quantity}, the whole position is {whole}",
                )
                self._refresh_account()
            elif level is None:
                self._exit_updated(order, exit_id.slot, command.quantity, level)
                self._refresh_account()
            else:
                sent = _send(
                    _sltp_request(position, exit_id.slot, level, self._magic),
                    _BRACKET_DONE_RETCODES,
                )
                self._on_amend_sent(order, exit_id.slot, command.quantity, level, sent)

    def _on_amend_sent(
        self, order: Order, slot: Slot, quantity: Quantity | None, level: Price, sent: _Sent
    ) -> None:
        if sent.outcome == SendOutcome.DONE:
            self._exit_updated(order, slot, quantity, level)
            self._refresh_account()
        elif sent.outcome == SendOutcome.REFUSED:
            self._modify_rejected(order, order.venue_order_id, sent.reason)
            self._refresh_account()
        elif sent.outcome == SendOutcome.NOT_SENT:
            self._modify_rejected(order, order.venue_order_id, sent.reason)
        else:
            self._log.warning(f"{order.client_order_id!r} modify: {sent.reason}; {_LOST}")

    def _exit_updated(
        self, order: Order, slot: Slot, quantity: Quantity | None, level: Price | None
    ) -> None:
        price, trigger = _exit_prices(order, slot, level)
        self.generate_order_updated(
            order.strategy_id,
            order.instrument_id,
            order.client_order_id,
            order.venue_order_id,
            _stated(quantity, order.quantity),
            price,
            trigger,
            self._clock.timestamp_ns(),
        )

    def _cancel_exit(self, order: Order, exit_id: _ExitId) -> None:
        """Clears the bracket an exit occupies; a cancel never closes a position."""
        refusal = self._not_occupying(order, exit_id)
        if refusal is not None:
            self._cancel_rejected(order, order.venue_order_id, refusal)
        else:
            self._clear_bracket(order, exit_id)

    def _clear_bracket(self, order: Order, exit_id: _ExitId) -> None:
        try:
            self._conn.ensure_connected()
            position = self._position(exit_id.identifier, order.instrument_id.symbol.value)
            deals = ()
            if position is None:
                deals = _answer(
                    "history_deals_get", mt5.history_deals_get(position=exit_id.identifier)
                )
        except MT5ConnectionError as exc:
            self._cancel_rejected(order, order.venue_order_id, f"not sent: {exc}")
        else:
            if position is not None:
                sent = _send(
                    _sltp_request(position, exit_id.slot, None, self._magic), _BRACKET_DONE_RETCODES
                )
                self._on_clear_sent(order, sent)
            else:
                self._cancel_closed(order, exit_id, deals)
                self._refresh_account()

    def _cancel_closed(self, order: Order, exit_id: _ExitId, deals) -> None:
        """Answers the cancel of an exit whose position the venue no longer holds, from the
        position's deals: the venue dropped the bracket with the position, so the exit is canceled —
        unless its bracket closed the position by a deal NT has yet to book to it."""
        closing = _closing_deal(deals)
        if closing is None:
            self._cancel_rejected(
                order, order.venue_order_id, f"the venue holds no position {exit_id.identifier}"
            )
        elif _bracket_slot(closing) == exit_id.slot and _owes_fills(
            self._cache.orders(venue=self.venue), deals, exit_id.slot
        ):
            self._cancel_rejected(
                order, order.venue_order_id, f"its bracket closed position {exit_id.identifier}"
            )
        else:
            self.generate_order_canceled(
                order.strategy_id,
                order.instrument_id,
                order.client_order_id,
                order.venue_order_id,
                closing.time_msc * 1_000_000,
            )

    def _on_clear_sent(self, order: Order, sent: _Sent) -> None:
        if sent.outcome == SendOutcome.DONE:
            self.generate_order_canceled(
                order.strategy_id,
                order.instrument_id,
                order.client_order_id,
                order.venue_order_id,
                self._clock.timestamp_ns(),
            )
            self._refresh_account()
        elif sent.outcome == SendOutcome.REFUSED:
            self._cancel_rejected(order, order.venue_order_id, sent.reason)
            self._refresh_account()
        elif sent.outcome == SendOutcome.NOT_SENT:
            self._cancel_rejected(order, order.venue_order_id, sent.reason)
        else:
            self._log.warning(f"{order.client_order_id!r} cancel: {sent.reason}; {_LOST}")

    def _not_occupying(self, order: Order, exit_id: _ExitId) -> str | None:
        """None while an exit order occupies its position's bracket; else why it does not: the state
        NT closed it in, or the later order that took its place."""
        occupant = self._occupant(exit_id.identifier, exit_id.slot)
        if not order.is_open:
            return f"the order is {order.status_string()}"
        elif occupant.client_order_id != order.client_order_id:
            return f"{occupant.venue_order_id} took its place"
        else:
            return None

    def _occupant(self, identifier: int, slot: Slot) -> Order | None:
        """The exit order occupying a position's bracket: of those NT holds open, the latest."""
        held = _bracket_orders(self._cache.orders_open(venue=self.venue), identifier, slot)
        if held:
            return held[max(held)]
        else:
            return None

    def _owed_bracket_fill(self, deal) -> bool:
        """Whether a deal is a bracket's that no order of NT's has booked, while an exit is bound to
        its ticket or occupies the bracket."""
        slot = _bracket_slot(deal)
        owner = None
        if slot is not None and _booked(self._cache.orders(venue=self.venue), deal, slot) is None:
            owner = self._bracket_owner(deal)
        return owner is not None

    # ── Modify ────────────────────────────────────────────────────────────────

    async def _modify_order(self, command: ModifyOrder) -> None:
        """Moves a pending order's prices at the venue, which cannot change an order's quantity, and
        an exit's level in its position's bracket."""
        order = self._cache.order(command.client_order_id)
        exit_id = _exit_id_of(order)
        ticket = self._ticket(order)
        if exit_id is not None:
            self._modify_exit(order, exit_id, command)
        elif command.quantity is not None and command.quantity != order.quantity:
            self._modify_rejected(order, _venue_order_id(ticket), "unsupported: quantity changes")
        elif order.order_type not in _PENDING_ORDER_TYPES:
            self._modify_rejected(
                order,
                _venue_order_id(ticket),
                f"unsupported: modifying a {order_type_to_str(order.order_type)}",
            )
        elif ticket is None:
            self._modify_rejected(order, None, "no venue order is known for it")
        else:
            self._modify_resting(order, ticket, command)

    def _modify_resting(self, order: Order, ticket: int, command: ModifyOrder) -> None:
        try:
            self._conn.ensure_connected()
            refusal = self._not_resting(ticket)
        except MT5ConnectionError as exc:
            self._modify_rejected(order, _venue_order_id(ticket), f"not sent: {exc}")
        else:
            if refusal is None:
                price = _stated(command.price, _limit_price(order))
                trigger = _stated(command.trigger_price, _trigger_price(order))
                # The symbol routes the request's transaction through that symbol's EA.
                request = {
                    "action": mirror.TRADE_ACTION_MODIFY,
                    "symbol": order.instrument_id.symbol.value,
                    "order": ticket,
                    "sl": 0.0,
                    "tp": 0.0,
                }
                request |= _pending_prices(order, price, trigger) | _expiry(order)
                self._on_modify_sent(order, ticket, price, trigger, _send(request))
            else:
                self._modify_rejected(order, _venue_order_id(ticket), refusal)
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
            self._modify_rejected(order, _venue_order_id(ticket), sent.reason)
            self._refresh_account()
        elif sent.outcome == SendOutcome.NOT_SENT:
            self._modify_rejected(order, _venue_order_id(ticket), sent.reason)
        else:
            self._log.warning(f"{order.client_order_id!r} modify: {sent.reason}; {_LOST}")

    def _modify_rejected(
        self, order: Order, venue_order_id: VenueOrderId | None, reason: str
    ) -> None:
        self.generate_order_modify_rejected(
            order.strategy_id,
            order.instrument_id,
            order.client_order_id,
            venue_order_id,
            reason,
            self._clock.timestamp_ns(),
        )

    # ── Cancel ────────────────────────────────────────────────────────────────

    async def _cancel_order(self, command: CancelOrder) -> None:
        """Removes a pending order from the venue, and an exit from its position's bracket; a cancel
        never closes a position."""
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
        exit_id = _exit_id_of(order)
        ticket = self._ticket(order)
        if exit_id is not None:
            self._cancel_exit(order, exit_id)
        elif ticket is None:
            self._cancel_rejected(order, None, "no venue order is known for it")
        else:
            self._remove(order, ticket)

    def _remove(self, order: Order, ticket: int) -> None:
        try:
            self._conn.ensure_connected()
            refusal = self._not_resting(ticket)
        except MT5ConnectionError as exc:
            self._cancel_rejected(order, _venue_order_id(ticket), f"not sent: {exc}")
        else:
            if refusal is None:
                # The symbol routes the request's transaction through that symbol's EA.
                request = {
                    "action": mirror.TRADE_ACTION_REMOVE,
                    "symbol": order.instrument_id.symbol.value,
                    "order": ticket,
                }
                self._on_remove_sent(order, ticket, _send(request))
            else:
                self._cancel_rejected(order, _venue_order_id(ticket), refusal)
                self._refresh_account()

    def _on_remove_sent(self, order: Order, ticket: int, sent: _Sent) -> None:
        if sent.outcome == SendOutcome.DONE:
            # Its ORDER_DELETE and HISTORY_ADD follow; the end is emitted once.
            self._ended.add(ticket)
            self.generate_order_canceled(
                order.strategy_id,
                order.instrument_id,
                order.client_order_id,
                VenueOrderId(str(ticket)),
                self._clock.timestamp_ns(),
            )
            self._refresh_account()
        elif sent.outcome == SendOutcome.REFUSED:
            self._cancel_rejected(order, _venue_order_id(ticket), sent.reason)
            self._refresh_account()
        elif sent.outcome == SendOutcome.NOT_SENT:
            self._cancel_rejected(order, _venue_order_id(ticket), sent.reason)
        else:
            self._log.warning(f"{order.client_order_id!r} cancel: {sent.reason}; {_LOST}")

    def _cancel_rejected(
        self, order: Order, venue_order_id: VenueOrderId | None, reason: str
    ) -> None:
        self.generate_order_cancel_rejected(
            order.strategy_id,
            order.instrument_id,
            order.client_order_id,
            venue_order_id,
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
        """The report of the order a command names: an exit's from the bracket it occupies, else
        that of the venue order the command's ticket, its exit's ticket, or its client order id's
        digest names among the resting orders and the lookback's history; None when the venue holds
        none."""
        if command.client_order_id is None and command.venue_order_id is None:
            raise ValueError("an order status report needs a client or a venue order id")
        self._conn.ensure_connected()
        exit_order = self._exit_named(command)
        exit_report = None
        if exit_order is not None:
            exit_report = self._exit_report(exit_order)
        if exit_report is not None:
            return exit_report
        else:
            return self._ticket_report(command, exit_order)

    async def generate_order_status_reports(
        self,
        command: GenerateOrderStatusReports,
    ) -> list[OrderStatusReport]:
        """This trader's resting orders, with its orders in the venue's history over the command's
        window unless it asks for open orders only, one report per ticket; and the exits NT holds
        open, each from the bracket it occupies. An exit the venue executed under one or more of
        those tickets reports once, in their place."""
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
                if self._is_ours(order) and _in_scope(order.symbol, command.instrument_id):
                    venue_orders.setdefault(order.ticket, order)
        reports = []
        executed = {}
        for venue_order in venue_orders.values():
            exit_order = self._bound_exit(venue_order.ticket)
            if exit_order is None:
                reports.append(self._venue_order_report(venue_order))
            else:
                executed[exit_order.client_order_id] = exit_order
        exit_reports = {
            report.client_order_id: report for report in self._exit_reports(command.instrument_id)
        }
        for client_order_id, exit_order in executed.items():
            if client_order_id not in exit_reports:
                exit_reports[client_order_id] = self._executed_exit_report(exit_order)
        for report in exit_reports.values():
            if report.is_open or not command.open_only:
                reports.append(report)
        return reports

    async def generate_fill_reports(self, command: GenerateFillReports) -> list[FillReport]:
        """The fills of this trader's deals in the venue's history over the command's window, each
        under the venue order the deal executed: a bracket's deal fills the order NT booked it to,
        else the exit bound to its ticket, else the order occupying the bracket, and answers a
        command naming that order's synthetic id too."""
        self._conn.ensure_connected()
        date_from, date_to = self._window(command)
        reports = []
        for deal in _answer("history_deals_get", mt5.history_deals_get(date_from, date_to)):
            owner = self._bracket_owner(deal)
            if (
                (owner is not None or self._is_fill(deal))
                and _in_scope(deal.symbol, command.instrument_id)
                and _of_order(deal, owner, command.venue_order_id)
            ):
                reports.append(self._fill_report(deal, owner))
        return reports

    async def generate_position_status_reports(
        self,
        command: GeneratePositionStatusReports,
    ) -> list[PositionStatusReport]:
        """One report per venue position of this trader: the venue hedges each under its own
        identifier."""
        self._conn.ensure_connected()
        reports = []
        for position in self._venue_positions(command.instrument_id):
            if position.magic == self._magic:
                reports.append(self._position_report(position))
        return reports

    def _ticket_report(
        self, command: GenerateOrderStatusReport, exit_order: Order | None
    ) -> OrderStatusReport | None:
        """The report of the venue order a command names by its ticket, else under the ticket of its
        exit order, else by its client order id's digest; None when the venue holds none."""
        ticket = self._report_ticket(command)
        if ticket is None and exit_order is not None:
            ticket = self._ticket(exit_order)
        venue_order = None
        if ticket is not None:
            venue_order = self._venue_order(ticket)
        elif command.client_order_id is not None:
            venue_order = self._venue_order_by_comment(order_comment(command.client_order_id))
        if venue_order is None:
            return None
        elif not self._is_ours(venue_order):
            raise MT5OrderError(f"order {venue_order.ticket} carries another trader's magic")
        else:
            return self._order_report(venue_order)

    def _exit_named(self, command: GenerateOrderStatusReport) -> Order | None:
        """NT's exit order a report command names: by its client order id, a venue order id NT
        indexes it under, or the synthetic id it holds or held, which NT's index no longer carries
        once rebuilt after the order took its execution's ticket."""
        client_order_id = command.client_order_id
        if client_order_id is None:
            client_order_id = self._cache.client_order_id(command.venue_order_id)
        exit_id = _exit_id(command.venue_order_id)
        order = None
        if client_order_id is not None:
            order = self._cache.order(client_order_id)
        elif exit_id is not None:
            in_bracket = _bracket_orders(
                self._cache.orders(venue=self.venue), exit_id.identifier, exit_id.slot
            )
            order = in_bracket.get(exit_id.generation)
        if order is not None and _exit_id_of(order) is not None:
            return order
        else:
            return None

    def _exit_report(self, order: Order) -> OrderStatusReport | None:
        """An exit order's report from the bracket it occupied; None when the bracket reports
        nothing for it, or the venue knows nothing of its position."""
        exit_id = _exit_id_of(order)
        position = self._position(exit_id.identifier, order.instrument_id.symbol.value)
        reports = self._bracket_reports(
            exit_id.identifier, exit_id.slot, self._cache.orders(venue=self.venue), position
        )
        for report in reports:
            if report.client_order_id == order.client_order_id:
                return report
        return None

    def _exit_reports(self, instrument_id: InstrumentId | None) -> list[OrderStatusReport]:
        """The exit orders NT holds open, each reported from the bracket it occupies."""
        orders = self._cache.orders(venue=self.venue, instrument_id=instrument_id)
        brackets = set()
        for order in orders:
            exit_id = _exit_id_of(order)
            if exit_id is not None and order.is_open:
                brackets.add((exit_id.identifier, exit_id.slot))
        reports = []
        if brackets:
            positions = {
                position.identifier: position for position in self._venue_positions(instrument_id)
            }
            for identifier, slot in sorted(brackets):
                reports += self._bracket_reports(
                    identifier, slot, orders, positions.get(identifier)
                )
        return reports

    def _bracket_reports(
        self, identifier: int, slot: Slot, orders: list[Order], position
    ) -> list[OrderStatusReport]:
        """Reports the exit orders among `orders` in one bracket of a position from what the venue
        holds for it, open or closed; a position the venue knows nothing of reports nothing."""
        in_bracket = _bracket_orders(orders, identifier, slot)
        deals = ()
        if position is None:
            deals = _answer("history_deals_get", mt5.history_deals_get(position=identifier))
        closing = _closing_deal(deals)
        if position is not None:
            return self._open_bracket_reports(identifier, slot, in_bracket, position)
        elif closing is not None:
            return self._closed_bracket_reports(identifier, slot, in_bracket, deals, closing)
        else:
            return []

    def _open_bracket_reports(
        self, identifier: int, slot: Slot, in_bracket: dict[int, Order], position
    ) -> list[OrderStatusReport]:
        """An open position's bracket: while the venue sets it, the latest order NT holds open
        occupies it at the venue's level; every other order NT holds open is canceled."""
        held = {generation: order for generation, order in in_bracket.items() if order.is_open}
        level = _level(position, slot)
        ts_last = self._clock.timestamp_ns()
        occupant = None
        standing = None
        if level != 0 and held:
            occupant = held[max(held)]
            standing = self._instrument(position.symbol).make_price(level)
        reports = []
        for order in held.values():
            if order is occupant:
                reports.append(
                    self._exit_order_report(
                        identifier, slot, order, _standing_status(order), ts_last, level=standing
                    )
                )
            else:
                reports.append(
                    self._exit_order_report(identifier, slot, order, OrderStatus.CANCELED, ts_last)
                )
        return reports

    def _closed_bracket_reports(
        self, identifier: int, slot: Slot, in_bracket: dict[int, Order], deals, closing
    ) -> list[OrderStatusReport]:
        """A closed position's bracket. When its own deals closed the position, the order bound to
        the execution that closed it reports FILLED by its executions' deals, and until an order is
        bound the occupant stands at its own level; every other order NT holds open, and every one
        when another deal closed the position, is canceled."""
        held = {generation: order for generation, order in in_bracket.items() if order.is_open}
        bound = None
        occupant = None
        if _bracket_slot(closing) == slot:
            bound = self._bound_exit(closing.order)
            # The order NT holds takes the ticket only once NT applies the bind's update.
            if bound is not None and _exit_id(bound.venue_order_id) is not None:
                bound = None
            if bound is None and held:
                occupant = held[max(held)]
        ts_last = closing.time_msc * 1_000_000
        reports = []
        for order in in_bracket.values():
            if order is bound:
                instrument = self._instrument(order.instrument_id.symbol.value)
                filled, avg_px = _execution_fills(self._executions(deals, order), instrument)
                reports.append(
                    self._exit_order_report(
                        identifier,
                        slot,
                        order,
                        OrderStatus.FILLED,
                        ts_last,
                        filled=filled,
                        avg_px=avg_px,
                    )
                )
            elif order is occupant:
                reports.append(
                    self._exit_order_report(
                        identifier, slot, order, _standing_status(order), ts_last
                    )
                )
            elif order.is_open:
                reports.append(
                    self._exit_order_report(identifier, slot, order, OrderStatus.CANCELED, ts_last)
                )
        return reports

    def _exit_order_report(
        self,
        identifier: int,
        slot: Slot,
        order: Order,
        status: OrderStatus,
        ts_last: int,
        level: Price | None = None,
        filled: Quantity | None = None,
        avg_px: Decimal | None = None,
    ) -> OrderStatusReport:
        """An exit order's report under its venue order id, for its whole quantity: at the venue's
        `level` when one is stated, else at its own; filled by what NT booked to it, or by the
        `filled` volume its bracket's executions closed at their average price when stated."""
        price, trigger = _exit_prices(order, slot, level)
        if trigger is None:
            trigger_type = TriggerType.NO_TRIGGER
        else:
            trigger_type = TriggerType.DEFAULT
        if filled is None:
            filled_qty = order.filled_qty
        else:
            filled_qty = filled
        return OrderStatusReport(
            account_id=self.account_id,
            instrument_id=order.instrument_id,
            venue_order_id=order.venue_order_id,
            venue_position_id=PositionId(str(identifier)),
            order_side=order.side,
            order_type=order.order_type,
            time_in_force=order.time_in_force,
            order_status=status,
            quantity=order.quantity,
            filled_qty=filled_qty,
            report_id=UUID4(),
            ts_accepted=order.ts_accepted,
            ts_last=ts_last,
            ts_init=self._clock.timestamp_ns(),
            client_order_id=order.client_order_id,
            price=price,
            trigger_price=trigger,
            trigger_type=trigger_type,
            avg_px=avg_px,
            post_only=False,
            reduce_only=True,
        )

    def _bracket_owner(self, deal) -> Order | None:
        """The exit order a bracket's deal fills: the order NT booked it to, else the exit bound to
        its ticket, else the order occupying the bracket; None for any other deal."""
        slot = _bracket_slot(deal)
        owner = None
        if slot is not None:
            owner = _booked(self._cache.orders(venue=self.venue), deal, slot)
            if owner is None:
                owner = self._bound_exit(deal.order)
            if owner is None:
                owner = self._occupant(deal.position_id, slot)
        return owner

    def _bound_exit(self, ticket: int) -> Order | None:
        """The exit order whose bracket the venue executed with the order under a ticket."""
        order = self._indexed_order(ticket)
        if order is not None and _exit_id_of(order) is not None:
            return order
        else:
            return None

    def _executions(self, deals, order: Order) -> list:
        """The deals among `deals` that executed an exit's bracket for it: those of the tickets
        bound to it, but those NT booked to another order of the bracket."""
        orders = self._cache.orders(venue=self.venue)
        slot = _exit_id_of(order).slot
        executions = []
        for deal in deals:
            booked = _booked(orders, deal, slot)
            if self._bound_exit(deal.order) is order and (booked is None or booked is order):
                executions.append(deal)
        return executions

    def _executed_exit_report(self, order: Order) -> OrderStatusReport:
        """The report of an exit the venue executed: from the bracket it occupied while the bracket
        reports it; else filled when its executions filled it and canceled when they did not, since
        its bracket no longer holds it."""
        report = self._exit_report(order)
        if report is None:
            exit_id = _exit_id_of(order)
            deals = _answer("history_deals_get", mt5.history_deals_get(position=exit_id.identifier))
            executions = self._executions(deals, order)
            instrument = self._instrument(order.instrument_id.symbol.value)
            filled, avg_px = _execution_fills(executions, instrument)
            if filled >= order.quantity:
                status = OrderStatus.FILLED
            else:
                status = OrderStatus.CANCELED
            report = self._exit_order_report(
                exit_id.identifier,
                exit_id.slot,
                order,
                status,
                max(deal.time_msc for deal in executions) * 1_000_000,
                filled=filled,
                avg_px=avg_px,
            )
        return report

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
        """A venue order's report: the report of the exit whose bracket it executed, when it
        executed one, else its own."""
        exit_order = self._bound_exit(venue_order.ticket)
        if exit_order is not None:
            return self._executed_exit_report(exit_order)
        else:
            return self._venue_order_report(venue_order)

    def _venue_order_report(self, venue_order) -> OrderStatusReport:
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

    def _fill_report(self, deal, bracket_order: Order | None) -> FillReport:
        """A deal's fill report under the venue order the deal executed, of the exit order it fills
        when it is a bracket's."""
        instrument = self._instrument(deal.symbol)
        if bracket_order is not None:
            client_order_id = bracket_order.client_order_id
            order_type = bracket_order.order_type
        else:
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

    def _position(self, identifier: int, symbol: str):
        """The venue's open position under an identifier, at whatever ticket the venue holds it
        under now — a service operation can move a position to a new ticket while its identifier
        stays — or None when it holds none."""
        for position in _answer("positions_get", mt5.positions_get(ticket=identifier)):
            if position.identifier == identifier:
                return position
        for position in _answer("positions_get", mt5.positions_get(symbol=symbol)):
            if position.identifier == identifier:
                return position
        return None

    def _held_position(self, identifier: int, symbol: str):
        """The venue's open position under an identifier; raises MT5OrderError for none."""
        position = self._position(identifier, symbol)
        if position is None:
            raise MT5OrderError(f"the venue holds no position {identifier}")
        return position

    def _venue_positions(self, instrument_id: InstrumentId | None) -> tuple:
        """The venue's open positions, of the instrument when one is named."""
        if instrument_id is None:
            return _answer("positions_get", mt5.positions_get())
        else:
            return _answer("positions_get", mt5.positions_get(symbol=instrument_id.symbol.value))

    def _is_fill(self, deal) -> bool:
        """Whether a deal is a fill of this trader's: a trade carrying its magic, or a stop-out of a
        position it opened, whatever magic the venue stamps on the stop-out."""
        return _is_trade(deal) and (
            deal.magic == self._magic or (_is_stop_out(deal) and self._opened(deal.position_id))
        )

    def _is_ours(self, venue_order) -> bool:
        """Whether a venue order is this trader's: it carries its magic, or it executed the stop-out
        of a position this trader opened, whatever magic the venue stamps on it."""
        return venue_order.magic == self._magic or (
            venue_order.reason == mirror.ORDER_REASON_SO and self._opened(venue_order.position_id)
        )

    def _opened(self, identifier: int) -> bool:
        """Whether this trader opened a position: the order the position's identifier is the ticket
        of carries its magic."""
        opening = self._venue_order(identifier)
        return opening is not None and opening.magic == self._magic

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
        return Money(-charge, self.base_currency)


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


def _send(request: dict, done: frozenset[int] = _DONE_RETCODES) -> _Sent:
    """Sends a trade request and classifies what the venue's answer to it proves, `done` naming the
    retcodes that confirm it."""
    try:
        result = mt5.order_send(request)
    except ResponseLost as exc:
        return _Sent(SendOutcome.LOST, str(exc))
    except MT5ConnectionError as exc:
        return _Sent(SendOutcome.NOT_SENT, f"not sent: {exc}")
    if result is None:
        code, message = mt5.last_error()
        return _Sent(SendOutcome.LOST, f"order_send failed — error {code}: {message}")
    elif result.retcode in done:
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


# The venue accepts a pending order bound to a position and ignores the binding: its fill opens a
# new, opposite position.
_BOUND_PENDING = "unsupported: a pending order bound to a position"


def _refusal(order: Order, position_id: PositionId | None) -> str | None:
    """Why the venue cannot hold an order as it is stated, sent against `position_id` when the
    command names one; None when it can."""
    if order.order_type not in _ORDER_TYPES:
        return f"unsupported: order type {order_type_to_str(order.order_type)}"
    elif order.time_in_force not in _TIMES_IN_FORCE_SENT:
        return f"unsupported: time in force {time_in_force_to_str(order.time_in_force)}"
    elif order.is_post_only:
        return "unsupported: post-only"
    elif order.contingency_type != ContingencyType.NO_CONTINGENCY:
        return "unsupported: contingent orders"
    elif order.is_reduce_only:
        return _exit_refusal(order, position_id)
    elif position_id is not None and order.order_type != OrderType.MARKET:
        return _BOUND_PENDING
    else:
        return None


def _exit_refusal(order: Order, position_id: PositionId | None) -> str | None:
    """Why a reduce-only order cannot be its position's exit at the venue — a close, or the stop
    loss or take profit, which never expire — or None when it can."""
    if position_id is None:
        return "unsupported: a reduce-only order naming no position"
    elif not position_id.value.isdigit():
        return f"unsupported: {position_id} is no venue position"
    elif order.order_type == OrderType.STOP_LIMIT:
        return _BOUND_PENDING
    elif order.order_type != OrderType.MARKET and order.time_in_force != TimeInForce.GTC:
        return f"unsupported: an exit's time in force {time_in_force_to_str(order.time_in_force)}"
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


def _stated(value: Price | Quantity | None, current: Price | Quantity | None):
    """The value a modify states, else the order's current one."""
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


def _of_order(deal, owner: Order | None, named: VenueOrderId | None) -> bool:
    """Whether a deal's fill answers a command naming the venue order `named`: the order the deal
    executed, or the synthetic id of the exit order it fills."""
    return (
        named is None
        or named.value == str(deal.order)
        or (owner is not None and named == _exit_id_of(owner).venue_order_id)
    )


def _commanded(order: Order, command: CancelAllOrders) -> bool:
    """Whether a cancel-all covers an order: the commanding strategy's, of the command's side when
    it names one."""
    return order.strategy_id == command.strategy_id and command.order_side in (
        OrderSide.NO_ORDER_SIDE,
        order.side,
    )


def _exit_id(venue_order_id: VenueOrderId | None) -> _ExitId | None:
    """The synthetic id an exit order's venue order id is; None for a venue ticket or no id."""
    match = None
    if venue_order_id is not None:
        match = _EXIT_ID.fullmatch(venue_order_id.value)
    if match is not None:
        return _ExitId(int(match[1]), Slot(match[2]), int(match[3]))
    else:
        return None


def _exit_id_of(order: Order) -> _ExitId | None:
    """The synthetic id of an exit order — its venue order id, or the one it held until it took the
    ticket of its bracket's execution — or None for any other order."""
    for venue_order_id in (order.venue_order_id, *order.venue_order_ids):
        exit_id = _exit_id(venue_order_id)
        if exit_id is not None:
            return exit_id
    return None


def _bracket_orders(orders: list[Order], identifier: int, slot: Slot) -> dict[int, Order]:
    """The orders among `orders` that have occupied a position's bracket, by generation."""
    found = {}
    for order in orders:
        exit_id = _exit_id_of(order)
        if exit_id is not None and exit_id.identifier == identifier and exit_id.slot == slot:
            found[exit_id.generation] = order
    return found


def _booked(orders: list[Order], deal, slot: Slot) -> Order | None:
    """The order of a deal's position's bracket NT booked the deal's fill to, if any."""
    trade_id = TradeId(str(deal.ticket))
    for order in _bracket_orders(orders, deal.position_id, slot).values():
        if trade_id in order.trade_ids:
            return order
    return None


def _execution_fills(executions, instrument: InstrumentAny) -> tuple[Quantity, Decimal]:
    """The volume a bracket's executions closed, and the average price they closed at."""
    volume = Quantity.zero(instrument.size_precision)
    weighted = Decimal(0)
    for deal in executions:
        closed = instrument.make_qty(deal.volume)
        volume += closed
        weighted += instrument.make_price(deal.price).as_decimal() * closed.as_decimal()
    return volume, weighted / volume.as_decimal()


def _owes_fills(orders: list[Order], deals, slot: Slot) -> bool:
    """Whether a position's bracket executed a deal no order of the bracket has booked yet."""
    for deal in deals:
        if _bracket_slot(deal) == slot and _booked(orders, deal, slot) is None:
            return True
    return False


def _next_exit_id(orders, identifier: int, slot: Slot) -> VenueOrderId:
    """The synthetic id one generation above the latest among `orders` for a position's bracket, the
    first generation when none has occupied it."""
    generation = max(_bracket_orders(orders, identifier, slot), default=0) + 1
    return _ExitId(identifier, slot, generation).venue_order_id


def _is_trade(deal) -> bool:
    """Whether a deal is a buy or a sell of some volume: a trade into or out of a position. Raises
    MT5OrderError for a deal whose type the package does not name."""
    return _deal_type(deal) in (DealType.BUY, DealType.SELL) and deal.volume > 0


def _is_stop_out(deal) -> bool:
    """Whether a trade is the venue's stop-out of its position; raises MT5OrderError for a trade
    whose entry or reason the package does not name."""
    return _deal_entry(deal) == DealEntry.OUT and _deal_reason(deal) == Reason.SO


def _bracket_slot(deal) -> Slot | None:
    """The bracket a deal executes — a trade out of a position by its stop loss or its take profit —
    or None for any other deal; raises MT5OrderError for a trade whose entry or reason the package
    does not name."""
    slot = None
    if _is_trade(deal) and _deal_entry(deal) == DealEntry.OUT:
        slot = _BRACKET_SLOTS.get(_deal_reason(deal))
    return slot


def _closing_deal(deals):
    """The deal that closed a position: the last of its deals out of it; None when it has none."""
    closing = [deal for deal in deals if _deal_entry(deal) in _CLOSING_ENTRIES]
    return max(closing, key=attrgetter("time_msc", "ticket"), default=None)


def _deal_type(deal) -> DealType:
    if deal.type not in _DEAL_TYPES:
        raise MT5OrderError(f"deal {deal.ticket}: type {deal.type} is unknown")
    return _DEAL_TYPES[deal.type]


def _deal_entry(deal) -> DealEntry:
    if deal.entry not in _DEAL_ENTRIES:
        raise MT5OrderError(f"deal {deal.ticket}: entry {deal.entry} is unknown")
    return _DEAL_ENTRIES[deal.entry]


def _deal_reason(deal) -> Reason:
    if deal.reason not in _DEAL_REASONS:
        raise MT5OrderError(f"deal {deal.ticket}: reason {deal.reason} is unknown")
    return _DEAL_REASONS[deal.reason]


def _level(position, slot: Slot) -> float:
    """The level the venue holds a position's bracket at, 0 when it sets none."""
    if slot == Slot.SL:
        return position.sl
    else:
        return position.tp


def _sltp_request(position, slot: Slot, level: Price | None, magic: int) -> dict:
    """The request setting a position's bracket to `level`, None clearing it, with the other bracket
    carried as the venue holds it: the venue sets both at once."""
    if level is None:
        value = 0.0
    else:
        value = level.as_double()
    request = {
        "action": mirror.TRADE_ACTION_SLTP,
        "symbol": position.symbol,
        "position": position.ticket,
        "magic": magic,
    }
    if slot == Slot.SL:
        request["sl"] = value
        request["tp"] = position.tp
    else:
        request["sl"] = position.sl
        request["tp"] = value
    return request


def _stated_level(command: ModifyOrder, slot: Slot) -> Price | None:
    """The level a modify states for an exit: a stop's trigger, a target's price."""
    if slot == Slot.SL:
        return command.trigger_price
    else:
        return command.price


def _exit_prices(
    order: Order, slot: Slot, level: Price | None
) -> tuple[Price | None, Price | None]:
    """An exit order's limit price and trigger: a stop triggers and a target rests at `level`, else
    at the order's own."""
    if slot == Slot.SL:
        return None, _stated(level, order.trigger_price)
    else:
        return _stated(level, order.price), None


def _standing_status(order: Order) -> OrderStatus:
    """The status of an exit whose bracket the venue still holds: partly filled once its bracket has
    executed some of the position."""
    if order.filled_qty > 0:
        return OrderStatus.PARTIALLY_FILLED
    else:
        return OrderStatus.ACCEPTED


def _deal_side(deal) -> OrderSide:
    if _deal_type(deal) == DealType.BUY:
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
