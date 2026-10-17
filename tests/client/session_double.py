"""Doubles of a client's hub session, handing each frame a test delivers to its owner, and the
frames the hub pushes."""

import asyncio

from mt5connector.client.hub import Answered, HubSession
from mt5connector.wire import binary, calls, mirror
from mt5connector.wire.push_wire import TransactionType


class SessionDouble:
    """Stands in for HubSession, taking its constructor's arguments."""

    def __init__(self, config, loop, on_frame, on_reconnect, log):
        self.on_frame = on_frame
        self.on_reconnect = on_reconnect
        self.log = log
        self.wanted = []
        self.connected = False

    async def connect(self):
        self.connected = True

    async def disconnect(self):
        self.connected = False

    async def subscribe(self, subscription):
        if subscription not in self.wanted:
            self.wanted.append(subscription)

    async def unsubscribe(self, subscription):
        if subscription in self.wanted:
            self.wanted.remove(subscription)

    def deliver(self, frame):
        """Hands the frame to the owner as the session does, logging one the owner fails on."""
        try:
            self.on_frame(frame)
        except Exception as exc:
            self.log.exception(f"hub session: a {type(frame).__name__} frame failed", exc)

    def reconnect(self):
        self.on_reconnect()


class ExecSession(SessionDouble):
    """Stands in for the execution client's HubSession, answering from the venue double it is built
    over."""

    deferring = HubSession.deferring

    def __init__(self, venue, config, loop, on_frame, on_reconnect, log):
        super().__init__(config, loop, on_frame, on_reconnect, log)
        self._log = log
        self.venue = venue
        self.sent: dict[int, dict] = {}
        self._next_request_id = 1

    async def connect(self):
        if self.connected:
            raise RuntimeError("hub session: already connected")
        self.connected = True

    async def disconnect(self):
        if not self.connected:
            raise RuntimeError("hub session: not connected")
        self.connected = False

    async def request(self, call: calls.Call, **arguments) -> Answered:
        """A call's answer on a later turn of the loop, once the registry's codec takes its
        arguments: the venue's method named after it, handed an order_send's request as the client
        gave it and a read's arguments by name. A command answered sent takes the next request id
        when its answer names none, and is kept whole under it."""
        if not self.connected:
            raise RuntimeError("hub session: not connected")
        row = calls.CALLS[call]
        if call is calls.Call.ORDER_SEND:
            whole = calls.trade_request(arguments["request"])
            calls.encode_arguments(row, {"request": whole})
            result = self.venue.order_send(arguments["request"])
            if result.retcode == mirror.TRADE_RETCODE_PLACED:
                if result.request_id == 0:
                    result = result._replace(request_id=self._next_request_id)
                    self._next_request_id += 1
                self.sent[result.request_id] = whole
        else:
            calls.encode_arguments(row, arguments)
            result = getattr(self.venue, call.name.lower())(**arguments)
        await asyncio.sleep(0)
        return Answered(result, 0)


def zeroed(layout: binary.Layout) -> dict[str, object]:
    """A record of the layout with every member zero, as MQL5 initialises a struct."""
    record = {}
    for member in layout.members:
        if member.declared is mirror.MqlType.STRING:
            record[member.name] = ""
        elif member.declared is mirror.MqlType.DOUBLE:
            record[member.name] = 0.0
        else:
            record[member.name] = 0
    return record


def transaction_frame(kind: TransactionType, request=None, result=None, **transaction):
    """A trade transaction of `kind` with its other members zero, and the request and result the EA
    passes with it, zero unless given."""
    return binary.TradeTransaction(
        zeroed(binary.TRADE_TRANSACTION) | {"type": int(kind)} | transaction,
        zeroed(binary.TRADE_REQUEST) | (request or {}),
        zeroed(binary.TRADE_RESULT) | (result or {}),
    )


def deal_added(deal):
    """The DEAL_ADD the venue pushes for a deal it added to its history."""
    return transaction_frame(
        TransactionType.DEAL_ADD,
        deal=deal.ticket,
        order=deal.order,
        position=deal.position_id,
        symbol=deal.symbol,
        price=deal.price,
        volume=deal.volume,
    )


def order_left(kind: TransactionType, order):
    """The ORDER_DELETE or HISTORY_ADD the venue pushes for an order it ended."""
    return transaction_frame(kind, order=order.ticket, symbol=order.symbol, order_state=order.state)


def verdict_of(request: dict, request_id: int, retcode: int, **result):
    """The REQUEST the venue pushes with its verdict on the trade request sent under
    `request_id`."""
    return transaction_frame(
        TransactionType.REQUEST,
        request=request,
        result={"retcode": retcode, "request_id": request_id} | result,
    )


def request_done(ticket: int, comment: str, magic: int, action=mirror.TRADE_ACTION_DEAL, **result):
    """The REQUEST the venue pushes after completing a trade request that placed `ticket`, which no
    command of the client's awaits."""
    return transaction_frame(
        TransactionType.REQUEST,
        request={"action": action, "magic": magic, "comment": comment, "symbol": "EURUSD"},
        result={"retcode": mirror.TRADE_RETCODE_DONE, "order": ticket} | result,
    )
