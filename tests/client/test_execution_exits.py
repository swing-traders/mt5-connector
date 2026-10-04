"""The execution client's exits: a position's stop, target and closes as the venue's brackets and
closing deals, and the fills and reports those answer for."""

import asyncio
from decimal import Decimal

import pytest
from exec_harness import (
    ACCOUNT_ID,
    TRADER_ID,
    build,
    gtd,
    instrument,
    ours_deal,
    ours_order,
    ours_position,
)
from nautilus_trader.common.enums import LogLevel
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.messages import (
    CancelOrder,
    GenerateFillReports,
    GenerateOrderStatusReport,
    GenerateOrderStatusReports,
    ModifyOrder,
    SubmitOrder,
)
from nautilus_trader.model.enums import OrderSide, OrderStatus, OrderType
from nautilus_trader.model.identifiers import InstrumentId, PositionId, TradeId, VenueOrderId
from nautilus_trader.model.objects import Price, Quantity
from nautilus_trader.test_kit.stubs.events import TestEventStubs
from venue_doubles import send_result

from mt5connector.client import execution
from mt5connector.wire import mirror

POSITION = 8133477


def build_client(exec_shim, **settings):
    """A client trading EURUSD in thousandths of a lot, so a position can close in parts."""
    return build(exec_shim, instruments=[instrument(lot_step="0.001")], **settings)


async def connected(exec_shim, **settings):
    h = build_client(exec_shim, **settings)
    await h.connect()
    return h


async def connect_with_events(h):
    """Connects without discarding the events emitted before the poll starts."""
    await h.client._connect()
    h.client._exec_poll_task.cancel()
    try:
        await h.client._exec_poll_task
    except asyncio.CancelledError:
        pass


def long_position(**fields):
    """Position 8133477, long 0.01 EURUSD, its target at 1.1 and no stop, under `fields`."""
    return ours_position(
        **(
            {
                "ticket": POSITION,
                "identifier": POSITION,
                "type": mirror.POSITION_TYPE_BUY,
                "volume": 0.01,
                "sl": 0.0,
                "tp": 1.1,
            }
            | fields
        )
    )


def holding(*positions):
    """A positions_get answering the positions by ticket, by symbol, or all of them."""

    def positions_get(symbol=None, group=None, ticket=None):
        if ticket is not None:
            return tuple(position for position in positions if position.ticket == ticket)
        elif symbol is not None:
            return tuple(position for position in positions if position.symbol == symbol)
        else:
            return positions

    return positions_get


def history(deals, position_deals=()):
    """A history_deals_get answering `deals` over any window and `position_deals` for a position."""

    def history_deals_get(date_from=None, date_to=None, group=None, ticket=None, position=None):
        if position is not None:
            return tuple(deal for deal in position_deals if deal.position_id == position)
        else:
            return tuple(deals)

    return history_deals_get


def order_history(*orders):
    """A history_orders_get answering `orders` over any window, and the one under a ticket."""

    def history_orders_get(date_from=None, date_to=None, group=None, ticket=None, position=None):
        return tuple(order for order in orders if ticket in (None, order.ticket))

    return history_orders_get


async def submit(h, order, position=POSITION):
    """Submits `order` as NT's engine does, sent against `position` when it names one."""
    position_id = None
    if position is not None:
        position_id = PositionId(str(position))
    h.cache.add_order(order, position_id)
    await h.client._submit_order(
        SubmitOrder(
            trader_id=TRADER_ID,
            strategy_id=order.strategy_id,
            order=order,
            command_id=UUID4(),
            ts_init=0,
            position_id=position_id,
        )
    )


def settle(h):
    """Applies the order events the client emitted to NT's orders, as the execution engine would."""
    for event in h.events:
        order = h.cache.order(event.client_order_id)
        order.apply(event)
        h.cache.update_order(order)
    h.ledger.clear()


def fill_on(h, order, trade_id, quantity):
    """Books a fill of `quantity` under `trade_id` to NT's `order`, as the engine would."""
    order.apply(
        TestEventStubs.order_filled(
            order,
            h.instruments["EURUSD"],
            trade_id=TradeId(trade_id),
            last_qty=Quantity.from_str(quantity),
        )
    )
    h.cache.update_order(order)


def canceled(h, order):
    order.apply(TestEventStubs.order_canceled(order))
    h.cache.update_order(order)


def cancel(h, order):
    return h.client._cancel_order(
        CancelOrder(
            trader_id=TRADER_ID,
            strategy_id=order.strategy_id,
            instrument_id=order.instrument_id,
            client_order_id=order.client_order_id,
            venue_order_id=order.venue_order_id,
            command_id=UUID4(),
            ts_init=0,
        )
    )


def modify(h, order, quantity=None, price=None, trigger_price=None):
    return h.client._modify_order(
        ModifyOrder(
            trader_id=TRADER_ID,
            strategy_id=order.strategy_id,
            instrument_id=order.instrument_id,
            client_order_id=order.client_order_id,
            venue_order_id=order.venue_order_id,
            quantity=quantity,
            price=price,
            trigger_price=trigger_price,
            command_id=UUID4(),
            ts_init=0,
        )
    )


def sent_requests(h) -> list[dict]:
    return [call.args[0] for call in h.venue.order_send.call_args_list]


def stop(h, trigger="1.08000", quantity="0.01"):
    return h.stop_market(OrderSide.SELL, quantity=quantity, trigger=trigger, reduce_only=True)


def target(h, price="1.10000", quantity="0.01"):
    return h.limit(OrderSide.SELL, quantity=quantity, price=price, reduce_only=True)


def close(h, quantity="0.01"):
    return h.market(OrderSide.SELL, quantity=quantity, reduce_only=True)


def bracket_deal(**fields):
    """A deal closing 0.01 of position 8133477 at its stop loss, under `fields`."""
    return ours_deal(
        **(
            {
                "ticket": 7101,
                "order": 9901,
                "type": mirror.DEAL_TYPE_SELL,
                "entry": mirror.DEAL_ENTRY_OUT,
                "reason": mirror.DEAL_REASON_SL,
                "position_id": POSITION,
                "volume": 0.01,
                "price": 1.08,
                "time_msc": 1_760_000_005_000,
            }
            | fields
        )
    )


def opening_deal():
    """The deal that opened position 8133477."""
    return bracket_deal(
        ticket=7100,
        order=POSITION,
        type=mirror.DEAL_TYPE_BUY,
        entry=mirror.DEAL_ENTRY_IN,
        reason=mirror.DEAL_REASON_EXPERT,
        time_msc=1_760_000_000_000,
    )


def execution_order(**fields):
    """The order the venue placed to execute position 8133477's stop loss, as its history holds it
    filled, under `fields`."""
    return ours_order(
        **(
            {
                "ticket": 9901,
                "type": mirror.ORDER_TYPE_SELL,
                "state": mirror.ORDER_STATE_FILLED,
                "reason": mirror.ORDER_REASON_SL,
                "position_id": POSITION,
                "volume_initial": 0.01,
                "volume_current": 0.0,
                "price_open": 1.08,
                "comment": "[sl 1.08]",
                "time_setup_msc": 1_760_000_005_000,
                "time_done_msc": 1_760_000_005_000,
            }
            | fields
        )
    )


def no_changes():
    """The venue's answer to a request that would change nothing it holds."""
    return send_result(retcode=mirror.TRADE_RETCODE_NO_CHANGES, comment="No changes", order=0)


def turn(h, deals=None):
    if deals is not None:
        h.venue.history_deals_get.side_effect = None
        h.venue.history_deals_get.return_value = tuple(deals)
    h.client._poll_turn()


def fills(h) -> list:
    return [event for event in h.events if type(event).__name__ == "OrderFilled"]


async def order_reports(h, open_only=False) -> list:
    return await h.client.generate_order_status_reports(
        GenerateOrderStatusReports(
            instrument_id=None,
            start=None,
            end=None,
            open_only=open_only,
            command_id=UUID4(),
            ts_init=0,
        )
    )


async def fill_reports(h, venue_order_id=None) -> list:
    return await h.client.generate_fill_reports(
        GenerateFillReports(
            instrument_id=None,
            venue_order_id=venue_order_id,
            start=None,
            end=None,
            command_id=UUID4(),
            ts_init=0,
        )
    )


def sltp(request) -> tuple:
    return (request["action"], request["position"], request["sl"], request["tp"])


# ── Stops and targets become the position's brackets ─────────────────────────


async def test_a_stop_then_a_moved_stop_succeed_one_another_in_the_positions_one_bracket(
    exec_shim,
):
    h = await connected(exec_shim)
    h.venue.positions_get.side_effect = holding(long_position())
    first = stop(h, trigger="1.08000")
    await submit(h, first)
    assert h.names() == ["OrderSubmitted", "OrderAccepted", "AccountState"]
    assert h.events[1].venue_order_id == VenueOrderId(f"{POSITION}-SL-1")
    settle(h)

    h.venue.positions_get.side_effect = holding(long_position(sl=1.08))
    second = stop(h, trigger="1.08200")
    await submit(h, second)
    assert [sltp(request) for request in sent_requests(h)] == [
        (mirror.TRADE_ACTION_SLTP, POSITION, 1.08, 1.1),
        (mirror.TRADE_ACTION_SLTP, POSITION, 1.082, 1.1),
    ]
    assert h.names() == ["OrderSubmitted", "OrderAccepted", "OrderCanceled", "AccountState"]
    accepted, canceled = h.events[1:]
    assert (accepted.client_order_id, accepted.venue_order_id) == (
        second.client_order_id,
        VenueOrderId(f"{POSITION}-SL-2"),
    )
    assert (canceled.client_order_id, canceled.venue_order_id) == (
        first.client_order_id,
        VenueOrderId(f"{POSITION}-SL-1"),
    )


async def test_the_bracket_request_names_the_positions_symbol(exec_shim):
    h = await connected(exec_shim)
    h.venue.positions_get.side_effect = holding(long_position())
    await submit(h, stop(h))
    assert sent_requests(h)[0]["symbol"] == "EURUSD"


async def test_a_target_becomes_the_take_profit_carrying_the_standing_stop(exec_shim):
    h = await connected(exec_shim)
    h.venue.positions_get.side_effect = holding(long_position(sl=1.08, tp=0.0))
    await submit(h, target(h, price="1.10000"))
    assert [sltp(request) for request in sent_requests(h)] == [
        (mirror.TRADE_ACTION_SLTP, POSITION, 1.08, 1.1)
    ]
    assert h.names() == ["OrderSubmitted", "OrderAccepted", "AccountState"]
    assert h.events[1].venue_order_id == VenueOrderId(f"{POSITION}-TP-1")


async def test_the_generation_follows_the_highest_nt_holds_for_that_bracket(exec_shim):
    h = build_client(exec_shim)
    h.place(stop(h), ticket=f"{POSITION}-SL-3")
    h.place(target(h), ticket=f"{POSITION}-TP-5")
    h.place(stop(h), ticket="8133478-SL-9")
    await h.connect()
    h.venue.positions_get.side_effect = holding(long_position(sl=1.08))
    await submit(h, stop(h, trigger="1.08200"))
    assert h.events[1].venue_order_id == VenueOrderId(f"{POSITION}-SL-4")


async def test_a_stop_the_venue_refuses_as_invalid_is_rejected_with_its_retcode_and_comment(
    exec_shim,
):
    h = await connected(exec_shim)
    h.venue.positions_get.side_effect = holding(long_position())
    h.venue.order_send.return_value = send_result(
        retcode=mirror.TRADE_RETCODE_INVALID_STOPS, comment="Invalid stops", order=0
    )
    await submit(h, stop(h))
    assert h.names() == ["OrderSubmitted", "OrderRejected", "AccountState"]
    assert "TRADE_RETCODE_INVALID_STOPS" in h.events[1].reason
    assert "Invalid stops" in h.events[1].reason
    assert len(h.venue.order_send.call_args_list) == 1


async def test_a_buy_stop_entry_the_venue_refuses_as_invalid_is_rejected_with_its_retcode(
    exec_shim,
):
    h = await connected(exec_shim)
    h.venue.order_send.return_value = send_result(
        retcode=mirror.TRADE_RETCODE_INVALID_PRICE, comment="Invalid price", order=0
    )
    await submit(h, h.stop_market(OrderSide.BUY, trigger="1.09000"), position=None)
    assert h.names() == ["OrderSubmitted", "OrderRejected", "AccountState"]
    assert "TRADE_RETCODE_INVALID_PRICE" in h.events[1].reason
    assert "Invalid price" in h.events[1].reason


async def test_a_stop_on_a_position_the_venue_does_not_hold_is_rejected_unsent(exec_shim):
    h = await connected(exec_shim)
    h.venue.positions_get.side_effect = holding()
    await submit(h, stop(h))
    assert h.names() == ["OrderSubmitted", "OrderRejected"]
    assert str(POSITION) in h.events[1].reason
    h.venue.order_send.assert_not_called()


# ── Cancel and amend ─────────────────────────────────────────────────────────


async def occupied(exec_shim, *, sl=1.08, tp=1.1):
    """A client with position 8133477 holding NT's stop 8133477-SL-1 and target 8133477-TP-1."""
    h = build_client(exec_shim)
    stop_order = h.place(stop(h), ticket=f"{POSITION}-SL-1")
    target_order = h.place(target(h), ticket=f"{POSITION}-TP-1")
    await h.connect()
    h.venue.positions_get.side_effect = holding(long_position(sl=sl, tp=tp))
    return h, stop_order, target_order


async def test_a_cancel_of_the_stop_clears_the_stop_loss_and_cancels_at_the_acknowledgement(
    exec_shim,
):
    h, stop_order, _ = await occupied(exec_shim)
    await cancel(h, stop_order)
    assert [sltp(request) for request in sent_requests(h)] == [
        (mirror.TRADE_ACTION_SLTP, POSITION, 0.0, 1.1)
    ]
    assert h.names() == ["OrderCanceled", "AccountState"]
    assert h.events[0].venue_order_id == VenueOrderId(f"{POSITION}-SL-1")


async def test_a_cancel_of_the_target_clears_the_take_profit(exec_shim):
    h, _, target_order = await occupied(exec_shim)
    await cancel(h, target_order)
    assert [sltp(request) for request in sent_requests(h)] == [
        (mirror.TRADE_ACTION_SLTP, POSITION, 1.08, 0.0)
    ]
    assert h.names() == ["OrderCanceled", "AccountState"]


async def test_a_cancel_the_venue_refuses_is_rejected_with_the_retcode(exec_shim):
    h, stop_order, _ = await occupied(exec_shim)
    h.venue.order_send.return_value = send_result(
        retcode=mirror.TRADE_RETCODE_INVALID_STOPS, comment="Invalid stops"
    )
    await cancel(h, stop_order)
    assert h.names() == ["OrderCancelRejected", "AccountState"]
    assert "TRADE_RETCODE_INVALID_STOPS" in h.events[0].reason


async def test_a_cancel_of_an_exit_already_closed_is_rejected_naming_its_state(exec_shim):
    h, stop_order, _ = await occupied(exec_shim)
    stop_order.apply(TestEventStubs.order_canceled(stop_order))
    h.cache.update_order(stop_order)
    await cancel(h, stop_order)
    assert h.names() == ["OrderCancelRejected"]
    assert "CANCELED" in h.events[0].reason
    h.venue.order_send.assert_not_called()


async def test_a_cancel_of_a_displaced_stop_is_rejected_and_leaves_the_bracket(exec_shim):
    h, stop_order, _ = await occupied(exec_shim)
    h.place(stop(h, trigger="1.08200"), ticket=f"{POSITION}-SL-2")
    await cancel(h, stop_order)
    assert h.names() == ["OrderCancelRejected"]
    assert f"{POSITION}-SL-2" in h.events[0].reason
    h.venue.order_send.assert_not_called()


async def test_a_cancel_of_an_exit_whose_position_another_bracket_closed_cancels_it(exec_shim):
    h, _, target_order = await occupied(exec_shim)
    h.venue.positions_get.side_effect = holding()
    h.venue.history_deals_get.side_effect = history(
        [],
        position_deals=[opening_deal(), bracket_deal(time_msc=1_760_000_009_000)],
    )
    await cancel(h, target_order)
    assert h.names() == ["OrderCanceled", "AccountState"]
    assert h.events[0].ts_event == 1_760_000_009_000_000_000
    h.venue.order_send.assert_not_called()


async def test_a_cancel_of_a_stop_whose_own_bracket_closed_the_position_is_rejected(exec_shim):
    h, stop_order, _ = await occupied(exec_shim)
    h.venue.positions_get.side_effect = holding()
    h.venue.history_deals_get.side_effect = history(
        [], position_deals=[opening_deal(), bracket_deal()]
    )
    await cancel(h, stop_order)
    assert h.names() == ["OrderCancelRejected", "AccountState"]
    assert str(POSITION) in h.events[0].reason
    h.venue.order_send.assert_not_called()


async def test_a_cancel_of_a_stop_its_bracket_filled_in_part_cancels_the_rest(exec_shim):
    h, stop_order, _ = await occupied(exec_shim)
    stop_order.apply(
        TestEventStubs.order_filled(
            stop_order,
            h.instruments["EURUSD"],
            trade_id=TradeId("7101"),
            last_qty=Quantity.from_str("0.005"),
        )
    )
    h.cache.update_order(stop_order)
    h.venue.positions_get.side_effect = holding()
    h.venue.history_deals_get.side_effect = history(
        [], position_deals=[opening_deal(), bracket_deal(volume=0.005)]
    )
    await cancel(h, stop_order)
    assert h.names() == ["OrderCanceled", "AccountState"]
    h.venue.order_send.assert_not_called()


async def test_a_cancel_after_its_bracket_closed_the_position_cancels_once_every_deal_is_booked(
    exec_shim,
):
    h = build_client(exec_shim)
    first = h.place(stop(h, trigger="1.07900"), ticket=f"{POSITION}-SL-1")
    fill_on(h, first, "7101", "0.005")
    canceled(h, first)
    second = h.place(stop(h, trigger="1.08000"), ticket=f"{POSITION}-SL-2")
    fill_on(h, second, "7102", "0.005")
    await h.connect()
    h.venue.positions_get.side_effect = holding()
    h.venue.history_deals_get.side_effect = history(
        [],
        position_deals=[
            opening_deal(),
            bracket_deal(ticket=7101, volume=0.005),
            bracket_deal(ticket=7102, volume=0.005, time_msc=1_760_000_005_100),
        ],
    )
    await cancel(h, second)
    assert h.names() == ["OrderCanceled", "AccountState"]


async def test_a_price_amend_of_the_target_moves_the_take_profit_and_updates_it(exec_shim):
    h, _, target_order = await occupied(exec_shim)
    await modify(h, target_order, price=Price.from_str("1.10500"))
    assert [sltp(request) for request in sent_requests(h)] == [
        (mirror.TRADE_ACTION_SLTP, POSITION, 1.08, 1.105)
    ]
    assert h.names() == ["OrderUpdated", "AccountState"]
    updated = h.events[0]
    assert (updated.venue_order_id, updated.price, updated.quantity) == (
        VenueOrderId(f"{POSITION}-TP-1"),
        Price.from_str("1.10500"),
        Quantity.from_str("0.01"),
    )


async def test_a_trigger_amend_of_the_stop_moves_the_stop_loss_and_updates_it(exec_shim):
    h, stop_order, _ = await occupied(exec_shim)
    await modify(h, stop_order, trigger_price=Price.from_str("1.08300"))
    assert [sltp(request) for request in sent_requests(h)] == [
        (mirror.TRADE_ACTION_SLTP, POSITION, 1.083, 1.1)
    ]
    assert h.names() == ["OrderUpdated", "AccountState"]
    assert h.events[0].trigger_price == Price.from_str("1.08300")


async def test_an_amend_to_a_quantity_the_position_does_not_hold_is_rejected_naming_it(exec_shim):
    h, stop_order, _ = await occupied(exec_shim)
    await modify(h, stop_order, quantity=Quantity.from_str("0.02"))
    assert h.names() == ["OrderModifyRejected", "AccountState"]
    assert "0.02" in h.events[0].reason
    h.venue.order_send.assert_not_called()


async def test_an_amend_to_the_whole_positions_quantity_updates_it_without_a_request(exec_shim):
    h, stop_order, _ = await occupied(exec_shim)
    h.venue.positions_get.side_effect = holding(long_position(sl=1.08, volume=0.005))
    await modify(h, stop_order, quantity=Quantity.from_str("0.005"))
    assert h.names() == ["OrderUpdated", "AccountState"]
    assert (h.events[0].quantity, h.events[0].trigger_price) == (
        Quantity.from_str("0.005"),
        Price.from_str("1.08000"),
    )
    h.venue.order_send.assert_not_called()


async def test_an_amend_of_a_partly_filled_stop_names_its_whole_quantity_with_its_fills(
    exec_shim,
):
    h, stop_order, _ = await occupied(exec_shim)
    fill_on(h, stop_order, "7101", "0.006")
    h.venue.positions_get.side_effect = holding(long_position(sl=1.08, volume=0.004))
    await modify(h, stop_order, quantity=Quantity.from_str("0.004"))
    assert h.names() == ["OrderModifyRejected", "AccountState"]
    h.ledger.clear()
    await modify(h, stop_order, quantity=Quantity.from_str("0.010"))
    assert h.names() == ["OrderUpdated", "AccountState"]
    assert h.events[0].quantity == Quantity.from_str("0.010")
    h.venue.order_send.assert_not_called()


# ── Closes ───────────────────────────────────────────────────────────────────


async def test_a_flatten_with_a_target_standing_closes_the_position_by_its_ticket(exec_shim):
    h, _, _ = await occupied(exec_shim)
    h.venue.order_send.return_value = send_result(order=9701, deal=7701)
    order = close(h, quantity="0.01")
    await submit(h, order)
    (request,) = sent_requests(h)
    assert (
        request["action"],
        request["position"],
        request["type"],
        request["volume"],
        request["deviation"],
        request["comment"],
    ) == (
        mirror.TRADE_ACTION_DEAL,
        POSITION,
        mirror.ORDER_TYPE_SELL,
        0.01,
        20,
        execution.order_comment(order.client_order_id),
    )
    assert h.names() == ["OrderSubmitted", "OrderAccepted", "AccountState"]
    assert h.events[1].venue_order_id == VenueOrderId("9701")


async def test_a_partial_close_with_a_target_standing_is_rejected_naming_it_unsent(exec_shim):
    h, _, _ = await occupied(exec_shim)
    await submit(h, close(h, quantity="0.005"))
    assert h.names() == ["OrderSubmitted", "OrderRejected"]
    assert f"{POSITION}-TP-1" in h.events[1].reason
    h.venue.order_send.assert_not_called()


async def test_a_partial_close_with_no_target_is_sent(exec_shim):
    h = await connected(exec_shim)
    h.venue.positions_get.side_effect = holding(long_position(tp=0.0))
    await submit(h, close(h, quantity="0.005"))
    (request,) = sent_requests(h)
    assert (request["action"], request["position"], request["volume"]) == (
        mirror.TRADE_ACTION_DEAL,
        POSITION,
        0.005,
    )


async def test_a_closes_deal_fills_it_on_the_position_it_closed(exec_shim):
    h = await connected(exec_shim)
    h.venue.positions_get.side_effect = holding(long_position(tp=0.0))
    h.venue.order_send.return_value = send_result(order=9701, deal=7701)
    order = close(h)
    await submit(h, order)
    settle(h)
    turn(h, deals=[bracket_deal(ticket=7701, order=9701, reason=mirror.DEAL_REASON_EXPERT)])
    (fill,) = fills(h)
    assert (fill.client_order_id, fill.venue_order_id, fill.position_id) == (
        order.client_order_id,
        VenueOrderId("9701"),
        PositionId(str(POSITION)),
    )


# ── The current ticket ───────────────────────────────────────────────────────


async def test_a_position_moved_to_a_new_ticket_is_addressed_by_it(exec_shim):
    h = await connected(exec_shim)
    h.venue.positions_get.side_effect = holding(long_position(ticket=8200001, tp=0.0))
    await submit(h, stop(h))
    await submit(h, close(h))
    assert [(request["action"], request["position"]) for request in sent_requests(h)] == [
        (mirror.TRADE_ACTION_SLTP, 8200001),
        (mirror.TRADE_ACTION_DEAL, 8200001),
    ]


# ── Position-bound pending orders ────────────────────────────────────────────


@pytest.mark.parametrize(
    "make",
    [
        lambda h: h.limit(OrderSide.BUY, price="1.07000"),
        lambda h: h.stop_market(OrderSide.BUY, trigger="1.09000"),
        lambda h: h.stop_limit(OrderSide.SELL, reduce_only=True),
    ],
    ids=["limit", "stop", "reduce-only-stop-limit"],
)
async def test_a_pending_order_bound_to_a_position_is_rejected_unsent(exec_shim, make):
    h = await connected(exec_shim)
    h.venue.positions_get.side_effect = holding(long_position())
    await submit(h, make(h))
    assert h.names() == ["OrderSubmitted", "OrderRejected"]
    h.venue.order_send.assert_not_called()


@pytest.mark.parametrize(
    ("make", "position", "named"),
    [
        (lambda h: close(h), None, "no position"),
        (lambda h: stop(h), "EURUSD.MT5-LONG", "EURUSD.MT5-LONG"),
        (
            lambda h: h.stop_market(OrderSide.SELL, trigger="1.08000", reduce_only=True, **gtd()),
            POSITION,
            "GTD",
        ),
    ],
    ids=["naming-no-position", "naming-no-venue-position", "expiring"],
)
async def test_an_exit_the_venue_cannot_hold_as_stated_is_rejected_unsent(
    exec_shim, make, position, named
):
    h = await connected(exec_shim)
    h.venue.positions_get.side_effect = holding(long_position())
    await submit(h, make(h), position=position)
    assert h.names() == ["OrderSubmitted", "OrderRejected"]
    assert named in h.events[1].reason
    h.venue.order_send.assert_not_called()


# ── Bracket deals ────────────────────────────────────────────────────────────


async def test_a_stop_loss_deal_binds_the_stop_to_the_venues_execution_then_fills_it(exec_shim):
    h, stop_order, _ = await occupied(exec_shim)
    turn(h, deals=[bracket_deal(ticket=7101, order=9901, volume=0.01, price=1.08)])
    assert h.names() == ["OrderUpdated", "OrderFilled", "AccountState"]
    updated, fill = h.events
    assert (
        updated.client_order_id,
        updated.venue_order_id,
        updated.quantity,
        updated.trigger_price,
    ) == (
        stop_order.client_order_id,
        VenueOrderId("9901"),
        Quantity.from_str("0.01"),
        Price.from_str("1.08000"),
    )
    assert (
        fill.client_order_id,
        fill.venue_order_id,
        fill.position_id,
        fill.trade_id,
        fill.order_side,
        fill.order_type,
        fill.last_qty,
        fill.last_px,
        fill.ts_event,
    ) == (
        stop_order.client_order_id,
        VenueOrderId("9901"),
        PositionId(str(POSITION)),
        TradeId("7101"),
        OrderSide.SELL,
        OrderType.STOP_MARKET,
        Quantity.from_str("0.01"),
        Price.from_str("1.08000"),
        1_760_000_005_000_000_000,
    )
    assert h.cache.client_order_id(VenueOrderId("9901")) == stop_order.client_order_id
    h.venue.history_orders_get.assert_not_called()


async def test_the_venues_execution_of_a_bound_stop_reports_as_the_stop(exec_shim):
    h, stop_order, _ = await occupied(exec_shim)
    turn(h, deals=[bracket_deal(ticket=7101, order=9901)])
    settle(h)
    h.venue.positions_get.side_effect = holding()
    h.venue.history_orders_get.return_value = (execution_order(),)
    reports = [
        report for report in await order_reports(h) if report.venue_order_id == VenueOrderId("9901")
    ]
    assert [report_fields(report) for report in reports] == [
        (
            stop_order.client_order_id,
            VenueOrderId("9901"),
            OrderType.STOP_MARKET,
            OrderStatus.FILLED,
            Quantity.from_str("0.01"),
            Quantity.from_str("0.01"),
            Price.from_str("1.08000"),
            Decimal("1.08"),
        )
    ]
    report = await h.client.generate_order_status_report(
        GenerateOrderStatusReport(
            instrument_id=stop_order.instrument_id,
            client_order_id=None,
            venue_order_id=VenueOrderId("9901"),
            command_id=UUID4(),
            ts_init=0,
        )
    )
    assert (report.client_order_id, report.venue_order_id, report.order_status) == (
        stop_order.client_order_id,
        VenueOrderId("9901"),
        OrderStatus.FILLED,
    )


async def test_a_bound_stop_keeps_its_bracket_so_a_moved_stop_succeeds_it(exec_shim):
    h, stop_order, _ = await occupied(exec_shim)
    turn(h, deals=[bracket_deal(ticket=7101, order=9901, volume=0.005)])
    settle(h)
    assert stop_order.status == OrderStatus.PARTIALLY_FILLED
    h.venue.positions_get.side_effect = holding(long_position(sl=1.08, volume=0.005))
    await submit(h, stop(h, trigger="1.08200", quantity="0.005"))
    assert h.names() == ["OrderSubmitted", "OrderAccepted", "OrderCanceled", "AccountState"]
    accepted, canceled = h.events[1:]
    assert accepted.venue_order_id == VenueOrderId(f"{POSITION}-SL-2")
    assert (canceled.client_order_id, canceled.venue_order_id) == (
        stop_order.client_order_id,
        VenueOrderId("9901"),
    )


async def test_a_close_in_two_deals_fills_the_same_stop_twice_summing_to_the_leg(exec_shim):
    h, stop_order, _ = await occupied(exec_shim)
    turn(
        h,
        deals=[
            bracket_deal(ticket=7101, volume=0.005, time_msc=1_760_000_005_000),
            bracket_deal(ticket=7102, volume=0.005, time_msc=1_760_000_005_100),
        ],
    )
    assert [fill.client_order_id for fill in fills(h)] == [stop_order.client_order_id] * 2
    assert sum(fill.last_qty.as_decimal() for fill in fills(h)) == Decimal("0.01")


async def test_the_second_deal_of_a_close_fills_the_stop_once_the_first_is_booked(exec_shim):
    h, stop_order, _ = await occupied(exec_shim)
    turn(h, deals=[bracket_deal(ticket=7101, volume=0.005)])
    settle(h)
    turn(h, deals=[bracket_deal(ticket=7102, volume=0.005, time_msc=1_760_000_005_100)])
    assert [(fill.client_order_id, fill.trade_id) for fill in fills(h)] == [
        (stop_order.client_order_id, TradeId("7102"))
    ]


async def test_a_deal_nt_booked_to_a_displaced_stop_is_not_filled_again_by_the_poll(exec_shim):
    h = build_client(exec_shim)
    first = h.place(stop(h, trigger="1.07900"), ticket=f"{POSITION}-SL-1")
    await h.connect()
    fill_on(h, first, "7101", "0.005")
    canceled(h, first)
    h.place(stop(h, trigger="1.08000", quantity="0.005"), ticket=f"{POSITION}-SL-2")
    turn(h, deals=[bracket_deal(ticket=7101, volume=0.005)])
    assert fills(h) == []
    assert [line for line in h.logged(LogLevel.INFO) if "7101" in line] == []


async def test_a_bound_execution_keeps_filling_its_displaced_stop(exec_shim):
    h, first, _ = await occupied(exec_shim)
    deals = [
        bracket_deal(ticket=7101, order=9901, volume=0.005),
        bracket_deal(ticket=7102, order=9901, volume=0.005, time_msc=1_760_000_005_100),
    ]
    turn(h, deals=deals[:1])
    settle(h)
    h.venue.positions_get.side_effect = holding(long_position(sl=1.08, volume=0.005))
    second = stop(h, trigger="1.08200", quantity="0.005")
    await submit(h, second)
    settle(h)
    assert first.status == OrderStatus.CANCELED
    turn(h, deals=deals)
    assert h.names() == ["OrderFilled", "AccountState"]
    assert [(e.client_order_id, e.venue_order_id, e.trade_id) for e in fills(h)] == [
        (first.client_order_id, VenueOrderId("9901"), TradeId("7102"))
    ]
    settle(h)
    h.venue.positions_get.side_effect = holding()
    h.venue.history_deals_get.side_effect = history(deals, [opening_deal(), *deals])
    report = await one_report(h, first)
    assert (report.order_status, report.venue_order_id, report.filled_qty) == (
        OrderStatus.FILLED,
        VenueOrderId("9901"),
        Quantity.from_str("0.010"),
    )
    assert (await one_report(h, second)).order_status == OrderStatus.CANCELED
    assert second.trade_ids == []


async def test_a_take_profit_deal_fills_the_target(exec_shim):
    h, _, target_order = await occupied(exec_shim)
    turn(h, deals=[bracket_deal(order=9902, reason=mirror.DEAL_REASON_TP, price=1.1)])
    (fill,) = fills(h)
    assert (fill.client_order_id, fill.venue_order_id, fill.order_type) == (
        target_order.client_order_id,
        VenueOrderId("9902"),
        OrderType.LIMIT,
    )


async def test_a_stop_out_fills_the_stop(exec_shim):
    h, stop_order, _ = await occupied(exec_shim)
    turn(h, deals=[bracket_deal(reason=mirror.DEAL_REASON_SO)])
    assert [fill.client_order_id for fill in fills(h)] == [stop_order.client_order_id]


async def test_a_stop_out_with_no_stop_standing_defers_with_one_info(exec_shim):
    h = await connected(exec_shim)
    deal = bracket_deal(reason=mirror.DEAL_REASON_SO)
    turn(h, deals=[deal])
    turn(h, deals=[deal])
    assert h.names() == []
    assert len([line for line in h.logged(LogLevel.INFO) if "7101" in line]) == 1


async def test_a_bracket_deal_fills_the_occupant_whatever_magic_it_carries(exec_shim):
    h, stop_order, _ = await occupied(exec_shim)
    turn(h, deals=[bracket_deal(magic=0)])
    assert [fill.client_order_id for fill in fills(h)] == [stop_order.client_order_id]


@pytest.mark.parametrize(
    "fields",
    [{"entry": 7}, {"reason": 42}],
    ids=["unknown-entry", "unknown-reason"],
)
async def test_a_trade_deal_of_a_kind_the_package_lacks_fails_the_turn_unconsumed(
    exec_shim, fields
):
    h, _, _ = await occupied(exec_shim)
    deal = bracket_deal(**fields)
    turn(h, deals=[deal])
    assert h.names() == []
    assert len(h.client.recorded_log.exception.call_args_list) == 1
    assert deal.ticket not in h.client._seen_deals


async def test_a_deal_of_a_type_the_package_lacks_fails_the_turn_unconsumed(exec_shim):
    h, _, _ = await occupied(exec_shim)
    deal = bracket_deal(type=999)
    turn(h, deals=[deal])
    assert h.names() == []
    assert len(h.client.recorded_log.exception.call_args_list) == 1
    assert deal.ticket not in h.client._seen_deals


@pytest.mark.parametrize("bound", [False, True], ids=["before-the-poll", "after-the-poll"])
@pytest.mark.parametrize(
    "named",
    [None, f"{POSITION}-SL-1", "9901"],
    ids=["any-order", "the-exits-synthetic-id", "the-execution-ticket"],
)
async def test_a_bracket_deals_fill_report_answers_the_exit_under_its_execution(
    exec_shim, named, bound
):
    h, stop_order, _ = await occupied(exec_shim)
    h.venue.history_deals_get.return_value = (bracket_deal(ticket=7101, order=9901),)
    if bound:
        turn(h)
        settle(h)
    named_id = None
    if named is not None:
        named_id = VenueOrderId(named)
    reports = await fill_reports(h, named_id)
    assert [
        (
            report.client_order_id,
            report.venue_order_id,
            report.trade_id,
            report.venue_position_id,
        )
        for report in reports
    ] == [
        (
            stop_order.client_order_id,
            VenueOrderId("9901"),
            TradeId("7101"),
            PositionId(str(POSITION)),
        )
    ]


async def test_a_stop_loss_deal_no_stop_occupies_is_left_to_reconciliation_with_its_execution(
    exec_shim,
):
    h = await connected(exec_shim)
    deal = bracket_deal(ticket=7101, order=9901)
    turn(h, deals=[deal])
    turn(h, deals=[deal])
    assert h.names() == []
    assert len([line for line in h.logged(LogLevel.INFO) if "7101" in line]) == 1
    h.venue.history_orders_get.return_value = (execution_order(),)
    (report,) = await order_reports(h)
    assert (
        report.venue_order_id,
        report.client_order_id,
        report.order_side,
        report.order_type,
        report.order_status,
        report.quantity,
        report.filled_qty,
    ) == (
        VenueOrderId("9901"),
        None,
        OrderSide.SELL,
        OrderType.MARKET,
        OrderStatus.FILLED,
        Quantity.from_str("0.01"),
        Quantity.from_str("0.01"),
    )


# ── Restart: reports from the venue's brackets ───────────────────────────────


async def two_stops(exec_shim, *, positions=(), deals=(), position_deals=()):
    """NT holding 8133477-SL-1 at 1.079 and 8133477-SL-2 at 1.08, both accepted, the venue answering
    `positions`, `deals` over the lookback and `position_deals` for the position."""
    h = build_client(exec_shim)
    first = h.place(stop(h, trigger="1.07900"), ticket=f"{POSITION}-SL-1")
    second = h.place(stop(h, trigger="1.08000"), ticket=f"{POSITION}-SL-2")
    exec_shim.positions_get.side_effect = holding(*positions)
    exec_shim.history_deals_get.side_effect = history(deals, position_deals)
    await connect_with_events(h)
    return h, first, second


async def exit_reports(h, open_only=False):
    return {report.client_order_id: report for report in await order_reports(h, open_only)}


async def one_report(h, order):
    return await h.client.generate_order_status_report(
        GenerateOrderStatusReport(
            instrument_id=order.instrument_id,
            client_order_id=order.client_order_id,
            venue_order_id=order.venue_order_id,
            command_id=UUID4(),
            ts_init=0,
        )
    )


async def report_by_venue_order_id(h, venue_order_id: str):
    return await h.client.generate_order_status_report(
        GenerateOrderStatusReport(
            instrument_id=InstrumentId.from_str("EURUSD.MT5"),
            client_order_id=None,
            venue_order_id=VenueOrderId(venue_order_id),
            command_id=UUID4(),
            ts_init=0,
        )
    )


def report_fields(report) -> tuple:
    """What an exit's report says of it: whose, under which id, its kind, state, size, level."""
    return (
        report.client_order_id,
        report.venue_order_id,
        report.order_type,
        report.order_status,
        report.quantity,
        report.filled_qty,
        report.trigger_price,
        report.avg_px,
    )


async def test_the_latest_stop_reports_accepted_at_the_venues_level_the_earlier_canceled(
    exec_shim,
):
    h, first, second = await two_stops(exec_shim, positions=[long_position(sl=1.08)])
    reports = await exit_reports(h)
    latest = reports[second.client_order_id]
    assert (
        latest.order_status,
        latest.trigger_price,
        latest.venue_order_id,
        latest.venue_position_id,
        latest.reduce_only,
    ) == (
        OrderStatus.ACCEPTED,
        Price.from_str("1.08000"),
        VenueOrderId(f"{POSITION}-SL-2"),
        PositionId(str(POSITION)),
        True,
    )
    assert reports[first.client_order_id].order_status == OrderStatus.CANCELED
    assert (await one_report(h, second)).order_status == OrderStatus.ACCEPTED
    assert (await one_report(h, first)).order_status == OrderStatus.CANCELED


async def test_the_occupant_reports_the_level_the_venue_holds_not_its_own(exec_shim):
    h, _, second = await two_stops(exec_shim, positions=[long_position(sl=1.081)])
    reports = await exit_reports(h)
    assert reports[second.client_order_id].trigger_price == Price.from_str("1.08100")


async def test_a_cleared_stop_loss_reports_every_stop_canceled(exec_shim):
    h, first, second = await two_stops(exec_shim, positions=[long_position(sl=0.0)])
    reports = await exit_reports(h)
    assert [reports[order.client_order_id].order_status for order in (first, second)] == [
        OrderStatus.CANCELED,
        OrderStatus.CANCELED,
    ]


async def test_reports_of_open_orders_only_leave_the_canceled_stop_out(exec_shim):
    h, _, second = await two_stops(exec_shim, positions=[long_position(sl=1.08)])
    reports = await exit_reports(h, open_only=True)
    assert list(reports) == [second.client_order_id]


async def test_a_position_its_stop_closed_while_down_fills_the_stop_once(exec_shim):
    deals = [opening_deal(), bracket_deal(ticket=7101, order=9901)]
    h, first, second = await two_stops(exec_shim, deals=deals, position_deals=deals)
    assert h.names() == ["OrderUpdated", "OrderFilled", "AccountState"]
    fill = h.events[1]
    assert (fill.client_order_id, fill.venue_order_id, fill.trade_id) == (
        second.client_order_id,
        VenueOrderId("9901"),
        TradeId("7101"),
    )
    settle(h)
    turn(h)
    turn(h)
    assert h.events == []
    reports = await exit_reports(h)
    assert (
        reports[second.client_order_id].order_status,
        reports[second.client_order_id].filled_qty,
        reports[second.client_order_id].venue_order_id,
    ) == (OrderStatus.FILLED, Quantity.from_str("0.01"), VenueOrderId("9901"))
    assert reports[first.client_order_id].order_status == OrderStatus.CANCELED


@pytest.mark.parametrize(
    ("second_price", "average"),
    [(1.0796, Decimal("1.0798")), (1.079649999, Decimal("1.079825"))],
    ids=["on-the-grid", "off-the-grid"],
)
async def test_a_stop_its_bracket_filled_in_parts_reports_the_average_price_it_closed_at(
    exec_shim, second_price, average
):
    deals = [
        opening_deal(),
        bracket_deal(ticket=7101, volume=0.005, price=1.08, time_msc=1_760_000_005_000),
        bracket_deal(ticket=7102, volume=0.005, price=second_price, time_msc=1_760_000_005_100),
    ]
    h, _, second = await two_stops(exec_shim, deals=deals, position_deals=deals)
    assert [fill.trade_id for fill in fills(h)] == [TradeId("7101"), TradeId("7102")]
    settle(h)
    turn(h)
    assert h.events == []
    report = await one_report(h, second)
    assert (report.order_status, report.venue_order_id, report.filled_qty, report.avg_px) == (
        OrderStatus.FILLED,
        VenueOrderId("9901"),
        Quantity.from_str("0.010"),
        average,
    )


async def test_a_position_another_deal_closed_while_down_reports_its_stops_canceled(exec_shim):
    deals = [
        opening_deal(),
        bracket_deal(ticket=7101, order=9701, reason=mirror.DEAL_REASON_EXPERT),
    ]
    h, first, second = await two_stops(exec_shim, deals=deals, position_deals=deals)
    assert h.names() == ["AccountState"]
    h.ledger.clear()
    turn(h)
    assert h.names() == []
    reports = await exit_reports(h)
    assert [reports[order.client_order_id].order_status for order in (first, second)] == [
        OrderStatus.CANCELED,
        OrderStatus.CANCELED,
    ]


async def test_a_stop_deal_nt_booked_before_the_restart_is_not_emitted_again(exec_shim):
    h = build_client(exec_shim)
    order = h.place(stop(h), ticket=f"{POSITION}-SL-1")
    order.apply(
        TestEventStubs.order_filled(
            order,
            h.instruments["EURUSD"],
            trade_id=TradeId("7101"),
            last_qty=Quantity.from_str("0.005"),
        )
    )
    h.cache.update_order(order)
    exec_shim.history_deals_get.side_effect = history([bracket_deal(ticket=7101, volume=0.005)])
    await connect_with_events(h)
    turn(h)
    assert fills(h) == []


async def test_a_deal_nt_booked_to_a_displaced_stop_is_not_emitted_again_at_restart(exec_shim):
    h = build_client(exec_shim)
    first = h.place(stop(h, trigger="1.07900"), ticket=f"{POSITION}-SL-1")
    fill_on(h, first, "7101", "0.005")
    canceled(h, first)
    h.place(stop(h, trigger="1.08000", quantity="0.005"), ticket=f"{POSITION}-SL-2")
    exec_shim.history_deals_get.side_effect = history([bracket_deal(ticket=7101, volume=0.005)])
    await connect_with_events(h)
    turn(h)
    assert fills(h) == []


async def test_the_venues_execution_no_exit_explains_still_reports_beside_its_fill(exec_shim):
    h = build_client(exec_shim)
    earlier = h.place(stop(h, trigger="1.07900"), ticket=f"{POSITION}-SL-1")
    canceled(h, earlier)
    deals = [opening_deal(), bracket_deal(ticket=7101, order=9901)]
    exec_shim.history_deals_get.side_effect = history(deals, position_deals=deals)
    await h.connect()
    h.venue.history_orders_get.return_value = (
        ours_order(
            ticket=9901,
            type=mirror.ORDER_TYPE_SELL,
            state=mirror.ORDER_STATE_FILLED,
            reason=mirror.ORDER_REASON_SL,
            position_id=POSITION,
            volume_current=0.0,
        ),
    )
    reports = await h.client.generate_order_status_reports(
        GenerateOrderStatusReports(
            instrument_id=None,
            start=None,
            end=None,
            open_only=False,
            command_id=UUID4(),
            ts_init=0,
        )
    )
    assert [report.venue_order_id for report in reports] == [VenueOrderId("9901")]


async def test_an_exit_executed_under_two_tickets_reports_once_under_its_current_one(exec_shim):
    h = await connected(exec_shim)
    order = h.place(stop(h, quantity="0.010"), ticket=f"{POSITION}-SL-1")
    deals = [
        bracket_deal(ticket=7101, order=9901, volume=0.005),
        bracket_deal(ticket=7102, order=9902, volume=0.005, time_msc=1_760_000_005_100),
    ]
    for deal in deals:
        turn(h, deals=[deal])
        settle(h)
    assert order.venue_order_id == VenueOrderId("9902")
    assert order.venue_order_ids == [VenueOrderId(f"{POSITION}-SL-1"), VenueOrderId("9901")]
    await h.client._disconnect()
    h.cache.clear_index()
    h.cache.build_index()
    h.venue.positions_get.side_effect = holding()
    h.venue.history_deals_get.side_effect = history(deals, [opening_deal(), *deals])
    h.venue.history_orders_get.side_effect = order_history(
        execution_order(ticket=9901, volume_initial=0.005),
        execution_order(ticket=9902, volume_initial=0.005),
    )
    await connect_with_events(h)
    assert h.events == []
    cumulative = (
        order.client_order_id,
        VenueOrderId("9902"),
        OrderType.STOP_MARKET,
        OrderStatus.FILLED,
        Quantity.from_str("0.010"),
        Quantity.from_str("0.010"),
        Price.from_str("1.08000"),
        Decimal("1.08"),
    )
    assert [report_fields(report) for report in await order_reports(h)] == [cumulative]
    assert report_fields(await report_by_venue_order_id(h, "9901")) == cumulative
    assert [
        (report.venue_order_id, report.trade_id, report.client_order_id)
        for report in await fill_reports(h)
    ] == [
        (VenueOrderId("9901"), TradeId("7101"), order.client_order_id),
        (VenueOrderId("9902"), TradeId("7102"), order.client_order_id),
    ]


async def test_connect_emits_an_owed_bracket_fill_before_returning(exec_shim):
    h = build_client(exec_shim)
    order = h.place(stop(h), ticket=f"{POSITION}-SL-1")
    deals = [opening_deal(), bracket_deal()]
    h.venue.history_deals_get.side_effect = history(deals, deals)
    await connect_with_events(h)
    assert h.names() == ["OrderUpdated", "OrderFilled", "AccountState"]
    assert [event.client_order_id for event in h.events] == [order.client_order_id] * 2
    assert [event.venue_order_id for event in h.events] == [VenueOrderId("9901")] * 2
    assert fills(h)[0].trade_id == TradeId("7101")
    settle(h)
    turn(h)
    assert h.events == []
    assert order.trade_ids == [TradeId("7101")]


async def test_connect_emits_an_owed_deal_to_its_bound_canceled_exit(exec_shim):
    h = await connected(exec_shim)
    order = h.place(stop(h), ticket=f"{POSITION}-SL-1")
    deals = [
        bracket_deal(ticket=7101, order=9901, volume=0.005),
        bracket_deal(ticket=7102, order=9901, volume=0.005, time_msc=1_760_000_005_100),
    ]
    turn(h, deals=deals[:1])
    settle(h)
    canceled(h, order)
    await h.client._disconnect()
    h.cache.clear_index()
    h.cache.build_index()
    h.venue.history_deals_get.side_effect = history(deals, [opening_deal(), *deals])
    await connect_with_events(h)
    assert h.names() == ["OrderFilled", "AccountState"]
    assert [(event.client_order_id, event.trade_id) for event in fills(h)] == [
        (order.client_order_id, TradeId("7102"))
    ]
    settle(h)
    turn(h)
    assert h.events == []
    assert order.status == OrderStatus.FILLED


async def test_connect_names_an_owed_deal_whose_fill_cannot_be_built(exec_shim):
    h = build_client(exec_shim)
    order = h.place(stop(h), ticket=f"{POSITION}-SL-1")
    h.venue.history_deals_get.side_effect = history([bracket_deal(symbol="UNLOADED")])
    with pytest.raises(execution.MT5OrderError, match="deal 7101.*UNLOADED is not loaded"):
        await h.client._connect()
    assert h.events == []
    h.venue.history_deals_get.side_effect = history([bracket_deal()])
    await connect_with_events(h)
    assert [(event.client_order_id, event.trade_id) for event in fills(h)] == [
        (order.client_order_id, TradeId("7101"))
    ]


# ── Reports of an exit whose bracket closed its position ─────────────────────


def closed_by_its_stop(h):
    """The venue answering position 8133477 closed by its stop loss: deal 7101 of order 9901."""
    h.venue.positions_get.side_effect = holding()
    deals = [opening_deal(), bracket_deal(ticket=7101, order=9901, price=1.08)]
    h.venue.history_deals_get.side_effect = history(deals, position_deals=deals)


async def test_a_stop_its_bracket_closed_reports_its_own_level_until_the_poll_binds_it(exec_shim):
    h, stop_order, _ = await occupied(exec_shim)
    closed_by_its_stop(h)
    report = await one_report(h, stop_order)
    assert (
        report.order_status,
        report.venue_order_id,
        report.trigger_price,
        report.filled_qty,
    ) == (
        OrderStatus.ACCEPTED,
        VenueOrderId(f"{POSITION}-SL-1"),
        Price.from_str("1.08000"),
        Quantity.from_str("0"),
    )
    turn(h)
    assert [(fill.client_order_id, fill.trade_id) for fill in fills(h)] == [
        (stop_order.client_order_id, TradeId("7101"))
    ]


async def test_a_stop_the_poll_bound_and_filled_reports_filled_under_its_execution(exec_shim):
    h, stop_order, _ = await occupied(exec_shim)
    closed_by_its_stop(h)
    turn(h)
    settle(h)
    report = await one_report(h, stop_order)
    assert (
        report.order_status,
        report.venue_order_id,
        report.filled_qty,
        report.avg_px,
    ) == (
        OrderStatus.FILLED,
        VenueOrderId("9901"),
        Quantity.from_str("0.01"),
        Decimal("1.08"),
    )
    assert (stop_order.status, stop_order.filled_qty) == (
        OrderStatus.FILLED,
        Quantity.from_str("0.01"),
    )


async def test_a_closed_bracket_reports_the_open_stop_it_awaits_not_a_later_canceled_one(
    exec_shim,
):
    h = build_client(exec_shim)
    first = h.place(stop(h, trigger="1.07900"), ticket=f"{POSITION}-SL-1")
    second = h.place(stop(h, trigger="1.08000"), ticket=f"{POSITION}-SL-2")
    canceled(h, second)
    deals = [opening_deal(), bracket_deal(ticket=7101, order=9901)]
    exec_shim.positions_get.side_effect = holding()
    exec_shim.history_deals_get.side_effect = history(deals, position_deals=deals)
    await connect_with_events(h)
    report = await one_report(h, first)
    assert (report.order_status, report.venue_order_id) == (
        OrderStatus.ACCEPTED,
        VenueOrderId(f"{POSITION}-SL-1"),
    )
    assert [(fill.client_order_id, fill.venue_order_id) for fill in fills(h)] == [
        (first.client_order_id, VenueOrderId("9901"))
    ]
    settle(h)
    turn(h)
    assert h.events == []
    assert (await one_report(h, first)).order_status == OrderStatus.FILLED
    assert second.status == OrderStatus.CANCELED


async def test_execution_totals_exclude_a_deal_booked_to_another_generation(exec_shim):
    h = build_client(exec_shim)
    first = h.place(stop(h), ticket=f"{POSITION}-SL-1")
    fill_on(h, first, "7101", "0.005")
    canceled(h, first)
    second = h.place(stop(h, quantity="0.005"), ticket=f"{POSITION}-SL-2")
    await h.connect()
    deals = [
        bracket_deal(ticket=7101, order=9901, volume=0.005),
        bracket_deal(ticket=7102, order=9901, volume=0.005, time_msc=1_760_000_005_100),
    ]
    turn(h, deals=deals)
    settle(h)
    h.venue.positions_get.side_effect = holding()
    h.venue.history_deals_get.side_effect = history(deals, [opening_deal(), *deals])
    report = await one_report(h, second)
    assert (report.order_status, report.filled_qty) == (
        OrderStatus.FILLED,
        Quantity.from_str("0.005"),
    )
    assert first.trade_ids == [TradeId("7101")]
    assert second.trade_ids == [TradeId("7102")]


async def test_a_bound_stop_reports_under_its_synthetic_id_after_nt_rebuilds_its_index(
    exec_shim,
):
    h, stop_order, _ = await occupied(exec_shim)
    closed_by_its_stop(h)
    turn(h)
    settle(h)
    h.cache.clear_index()
    h.cache.build_index()
    report = await h.client.generate_order_status_report(
        GenerateOrderStatusReport(
            instrument_id=stop_order.instrument_id,
            client_order_id=None,
            venue_order_id=VenueOrderId(f"{POSITION}-SL-1"),
            command_id=UUID4(),
            ts_init=0,
        )
    )
    assert (report.client_order_id, report.venue_order_id, report.order_status) == (
        stop_order.client_order_id,
        VenueOrderId("9901"),
        OrderStatus.FILLED,
    )


async def test_a_synthetic_id_no_order_holds_reports_nothing(exec_shim):
    h = await connected(exec_shim)
    report = await h.client.generate_order_status_report(
        GenerateOrderStatusReport(
            instrument_id=InstrumentId.from_str("EURUSD.MT5"),
            client_order_id=None,
            venue_order_id=VenueOrderId(f"{POSITION}-SL-7"),
            command_id=UUID4(),
            ts_init=0,
        )
    )
    assert report is None


@pytest.mark.parametrize("rebuild", [False, True], ids=["as-indexed", "index-rebuilt"])
@pytest.mark.parametrize(
    ("by_client_order_id", "venue_order_id"),
    [(True, None), (False, VenueOrderId(f"{POSITION}-SL-1")), (False, VenueOrderId("9901"))],
    ids=["client-order-id", "synthetic-id", "execution-ticket"],
)
async def test_a_bound_stop_canceled_after_a_partial_execution_reports_canceled_with_it(
    exec_shim, by_client_order_id, venue_order_id, rebuild
):
    h, stop_order, _ = await occupied(exec_shim)
    turn(h, deals=[bracket_deal(ticket=7101, order=9901, volume=0.005)])
    settle(h)
    h.venue.positions_get.side_effect = holding(long_position(sl=1.08, volume=0.005))
    await cancel(h, stop_order)
    assert h.names() == ["OrderCanceled", "AccountState"]
    settle(h)
    h.venue.history_orders_get.return_value = (execution_order(volume_initial=0.005),)
    if rebuild:
        h.cache.clear_index()
        h.cache.build_index()
    client_order_id = None
    if by_client_order_id:
        client_order_id = stop_order.client_order_id
    report = await h.client.generate_order_status_report(
        GenerateOrderStatusReport(
            instrument_id=stop_order.instrument_id,
            client_order_id=client_order_id,
            venue_order_id=venue_order_id,
            command_id=UUID4(),
            ts_init=0,
        )
    )
    assert report_fields(report) == (
        stop_order.client_order_id,
        VenueOrderId("9901"),
        OrderType.STOP_MARKET,
        OrderStatus.CANCELED,
        Quantity.from_str("0.01"),
        Quantity.from_str("0.005"),
        Price.from_str("1.08000"),
        Decimal("1.08"),
    )


@pytest.mark.parametrize("rebuild", [False, True], ids=["as-indexed", "index-rebuilt"])
async def test_a_report_naming_an_earlier_execution_answers_the_exit_under_its_current_one(
    exec_shim, rebuild
):
    h, stop_order, _ = await occupied(exec_shim)
    deals = [
        bracket_deal(ticket=7101, order=9901, volume=0.004),
        bracket_deal(ticket=7102, order=9902, volume=0.003, time_msc=1_760_000_005_100),
    ]
    turn(h, deals=deals[:1])
    settle(h)
    h.venue.positions_get.side_effect = holding(long_position(sl=1.08, volume=0.006))
    turn(h, deals=deals[1:])
    settle(h)
    h.venue.positions_get.side_effect = holding(long_position(sl=1.08, volume=0.003))
    await cancel(h, stop_order)
    settle(h)
    h.venue.history_deals_get.side_effect = history(deals, [opening_deal(), *deals])
    h.venue.history_orders_get.side_effect = order_history(
        execution_order(ticket=9901, volume_initial=0.004),
        execution_order(ticket=9902, volume_initial=0.003),
    )
    if rebuild:
        h.cache.clear_index()
        h.cache.build_index()
    report = await report_by_venue_order_id(h, "9901")
    assert report_fields(report) == (
        stop_order.client_order_id,
        VenueOrderId("9902"),
        OrderType.STOP_MARKET,
        OrderStatus.CANCELED,
        Quantity.from_str("0.01"),
        Quantity.from_str("0.007"),
        Price.from_str("1.08000"),
        Decimal("1.08"),
    )


async def test_a_stop_its_bracket_executed_in_part_reports_once_while_it_stands(exec_shim):
    h, stop_order, _ = await occupied(exec_shim)
    turn(h, deals=[bracket_deal(ticket=7101, order=9901, volume=0.005)])
    settle(h)
    h.venue.positions_get.side_effect = holding(long_position(sl=1.08, volume=0.005))
    h.venue.history_orders_get.side_effect = order_history(execution_order(volume_initial=0.005))
    reports = [
        report
        for report in await order_reports(h)
        if report.client_order_id == stop_order.client_order_id
    ]
    assert [report_fields(report) for report in reports] == [
        (
            stop_order.client_order_id,
            VenueOrderId("9901"),
            OrderType.STOP_MARKET,
            OrderStatus.PARTIALLY_FILLED,
            Quantity.from_str("0.01"),
            Quantity.from_str("0.005"),
            Price.from_str("1.08000"),
            None,
        )
    ]


# ── Requests whose answer was lost ───────────────────────────────────────────


async def test_a_stop_whose_answer_was_lost_is_unknown_to_reports_while_the_venue_holds_its_level(
    exec_shim,
):
    h = await connected(exec_shim)
    h.venue.positions_get.side_effect = holding(long_position())
    h.venue.order_send.return_value = send_result(
        retcode=mirror.TRADE_RETCODE_CONNECTION, comment="No connection", order=0
    )
    order = stop(h, trigger="1.08000")
    await submit(h, order)
    settle(h)
    assert order.status == OrderStatus.SUBMITTED
    h.venue.positions_get.side_effect = holding(long_position(sl=1.08))
    assert await one_report(h, order) is None
    assert order.client_order_id not in await exit_reports(h)


async def test_a_bracket_deal_while_down_of_a_stop_whose_answer_was_lost_is_reconciliations(
    exec_shim,
):
    h = build_client(exec_shim)
    order = stop(h, trigger="1.08000")
    h.cache.add_order(order, PositionId(str(POSITION)))
    order.apply(TestEventStubs.order_submitted(order, account_id=ACCOUNT_ID))
    h.cache.update_order(order)
    deals = [opening_deal(), bracket_deal(ticket=7101, order=9901, volume=0.005)]
    exec_shim.positions_get.side_effect = holding(long_position(sl=1.08, volume=0.005))
    exec_shim.history_deals_get.side_effect = history(deals, position_deals=deals)
    await h.connect()
    turn(h)
    assert h.names() == []
    h.venue.history_orders_get.return_value = (execution_order(volume_initial=0.005),)
    assert [
        (report.venue_order_id, report.client_order_id) for report in await order_reports(h)
    ] == [(VenueOrderId("9901"), None)]
    assert await one_report(h, order) is None


# ── Bracket requests the venue answers with no changes ───────────────────────


async def test_a_stop_set_to_the_level_the_venue_already_holds_is_accepted_on_no_changes(
    exec_shim,
):
    h = build_client(exec_shim)
    earlier = h.place(stop(h, trigger="1.07900"), ticket=f"{POSITION}-SL-1")
    await h.connect()
    h.venue.positions_get.side_effect = holding(long_position(sl=1.08))
    h.venue.order_send.return_value = no_changes()
    resent = stop(h, trigger="1.08000")
    await submit(h, resent)
    assert h.names() == ["OrderSubmitted", "OrderAccepted", "OrderCanceled", "AccountState"]
    accepted, canceled = h.events[1:]
    assert (accepted.client_order_id, accepted.venue_order_id) == (
        resent.client_order_id,
        VenueOrderId(f"{POSITION}-SL-2"),
    )
    assert canceled.client_order_id == earlier.client_order_id
    settle(h)
    report = await one_report(h, resent)
    assert (report.order_status, report.venue_order_id, report.trigger_price) == (
        OrderStatus.ACCEPTED,
        VenueOrderId(f"{POSITION}-SL-2"),
        Price.from_str("1.08000"),
    )


async def test_an_amend_the_venue_answers_with_no_changes_updates_the_exit(exec_shim):
    h, stop_order, _ = await occupied(exec_shim)
    h.venue.order_send.return_value = no_changes()
    await modify(h, stop_order, trigger_price=Price.from_str("1.08000"))
    assert h.names() == ["OrderUpdated", "AccountState"]


async def test_a_cancel_the_venue_answers_with_no_changes_cancels_the_exit(exec_shim):
    h, stop_order, _ = await occupied(exec_shim, sl=0.0)
    h.venue.order_send.return_value = no_changes()
    await cancel(h, stop_order)
    assert h.names() == ["OrderCanceled", "AccountState"]


async def test_a_pending_entry_modify_the_venue_answers_with_no_changes_is_rejected(exec_shim):
    h = build_client(exec_shim)
    order = h.place(h.limit(price="1.08000"), ticket=5002)
    await h.connect()
    resting = ours_order(ticket=5002)

    def orders_get(symbol=None, group=None, ticket=None):
        return (resting,)

    h.venue.orders_get.side_effect = orders_get
    h.venue.order_send.return_value = no_changes()
    await modify(h, order, price=Price.from_str("1.08000"))
    assert h.names() == ["OrderModifyRejected", "AccountState"]
    assert "TRADE_RETCODE_NO_CHANGES" in h.events[0].reason
