"""A double of the client's hub session, handing each frame a test delivers to its owner, and the
frames the hub pushes."""

from mt5connector.wire import binary, mirror
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


def request_done(ticket: int, comment: str, magic: int, action=mirror.TRADE_ACTION_DEAL, **result):
    """The REQUEST the venue pushes after completing a trade request that placed `ticket`."""
    return transaction_frame(
        TransactionType.REQUEST,
        request={"action": action, "magic": magic, "comment": comment, "symbol": "EURUSD"},
        result={"retcode": mirror.TRADE_RETCODE_DONE, "order": ticket} | result,
    )
