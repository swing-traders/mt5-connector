"""The execution client's order identity and its submit, modify and cancel boundary: what each trade
request carries to the venue, and which NT event each answer to it emits."""

from decimal import Decimal

import pytest
from exec_harness import (
    EURUSD,
    FILLING_BOC,
    FILLING_FOK,
    FILLING_IOC,
    MAGIC,
    STRATEGY_ID,
    TRADER_ID,
    build,
    gtd,
    instrument,
    ours_order,
)
from nautilus_trader.common.enums import LogLevel
from nautilus_trader.common.factories import OrderFactory
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.messages import (
    BatchCancelOrders,
    CancelAllOrders,
    CancelOrder,
    ModifyOrder,
    SubmitOrder,
    SubmitOrderList,
)
from nautilus_trader.model.enums import OrderSide, OrderStatus, TimeInForce
from nautilus_trader.model.identifiers import ClientOrderId, StrategyId, VenueOrderId
from nautilus_trader.model.objects import Price, Quantity
from nautilus_trader.test_kit.stubs.data import TestDataStubs
from venue_doubles import send_result, tick, trade_order

from mt5connector.client import errors, execution
from mt5connector.wire import mirror


@pytest.fixture
async def h(exec_shim):
    harness = build(exec_shim)
    await harness.connect()
    return harness


def submit(h, order):
    h.cache.add_order(order)
    return h.client._submit_order(
        SubmitOrder(
            trader_id=TRADER_ID,
            strategy_id=order.strategy_id,
            order=order,
            command_id=UUID4(),
            ts_init=0,
        )
    )


def cancel_command(order):
    return CancelOrder(
        trader_id=TRADER_ID,
        strategy_id=order.strategy_id,
        instrument_id=order.instrument_id,
        client_order_id=order.client_order_id,
        venue_order_id=order.venue_order_id,
        command_id=UUID4(),
        ts_init=0,
    )


def cancel_all_command(order_side=OrderSide.NO_ORDER_SIDE):
    return CancelAllOrders(
        trader_id=TRADER_ID,
        strategy_id=STRATEGY_ID,
        instrument_id=EURUSD,
        order_side=order_side,
        command_id=UUID4(),
        ts_init=0,
    )


def modify_command(order, quantity=None, price=None, trigger_price=None):
    return ModifyOrder(
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


def sent_requests(h) -> list[dict]:
    return [call.args[0] for call in h.venue.order_send.call_args_list]


def calls_after_order_send(h) -> list[str]:
    names = [name for name, _, _ in h.venue.method_calls]
    return names[names.index("order_send") + 1 :]


def resting(*venue_orders):
    """An orders_get answering the venue orders by symbol, or the one a ticket names."""

    def orders_get(symbol=None, group=None, ticket=None):
        if ticket is not None:
            return tuple(order for order in venue_orders if order.ticket == ticket)
        else:
            return venue_orders

    return orders_get


# ── Order identity ───────────────────────────────────────────────────────────


def test_the_order_comment_is_the_client_order_ids_digest_in_29_characters():
    comment = execution.order_comment(ClientOrderId("O-20261002-093000-001-001-1"))
    assert comment == "ed7cc0d404a662b25c7ffe76f3dd4"
    assert len(comment) == 29


async def test_a_long_client_order_id_still_travels_as_a_29_character_digest(h):
    order = h.market(client_order_id=ClientOrderId("O-" + "9" * 40))
    await submit(h, order)
    comment = sent_requests(h)[0]["comment"]
    assert comment == execution.order_comment(order.client_order_id)
    assert len(comment) == 29


# ── Submit: the request ──────────────────────────────────────────────────────


async def test_a_market_buy_is_a_deal_at_the_ask_carrying_the_magic_and_the_digest(h):
    order = h.market(OrderSide.BUY, "0.01")
    await submit(h, order)
    assert sent_requests(h) == [
        {
            "action": mirror.TRADE_ACTION_DEAL,
            "symbol": "EURUSD",
            "volume": 0.01,
            "type": mirror.ORDER_TYPE_BUY,
            "price": 1.0852,
            "deviation": 20,
            "magic": MAGIC,
            "comment": execution.order_comment(order.client_order_id),
            "sl": 0.0,
            "tp": 0.0,
            "type_filling": mirror.ORDER_FILLING_IOC,
        }
    ]


async def test_the_deviation_is_the_configs(exec_shim):
    h = build(exec_shim, deviation_points=7)
    await h.connect()
    await submit(h, h.market())
    assert sent_requests(h)[0]["deviation"] == 7


async def test_a_market_order_is_priced_off_the_cached_quote_without_reading_a_tick(h):
    quote = TestDataStubs.quote_tick(h.instruments["EURUSD"], bid_price=1.08490, ask_price=1.08530)
    h.cache.add_quote_tick(quote)
    await submit(h, h.market(OrderSide.BUY))
    await submit(h, h.market(OrderSide.SELL))
    assert [request["price"] for request in sent_requests(h)] == [1.0853, 1.0849]
    h.venue.symbol_info_tick.assert_not_called()


async def test_without_a_cached_quote_a_market_sell_reads_one_tick_for_its_bid(h):
    h.venue.symbol_info_tick.return_value = tick(bid=1.1, ask=1.2)
    await submit(h, h.market(OrderSide.SELL))
    assert sent_requests(h)[0]["price"] == 1.1
    h.venue.symbol_info_tick.assert_called_once_with("EURUSD")


async def test_a_market_order_without_any_quote_is_rejected_unsent(h):
    h.venue.symbol_info_tick.return_value = tick(bid=0.0, ask=0.0, time=0)
    await submit(h, h.market())
    assert h.names() == ["OrderSubmitted", "OrderRejected"]
    assert h.events[1].reason.startswith("not sent")
    h.venue.order_send.assert_not_called()


@pytest.mark.parametrize(
    ("flags", "filling"),
    [
        (FILLING_FOK | FILLING_IOC, mirror.ORDER_FILLING_IOC),
        (FILLING_IOC, mirror.ORDER_FILLING_IOC),
        (FILLING_FOK, mirror.ORDER_FILLING_FOK),
        (FILLING_FOK | FILLING_BOC, mirror.ORDER_FILLING_FOK),
        (FILLING_BOC, mirror.ORDER_FILLING_RETURN),
        (0, mirror.ORDER_FILLING_RETURN),
    ],
)
async def test_a_market_order_fills_ioc_else_fok_else_return_by_the_symbols_flags(
    exec_shim, flags, filling
):
    h = build(exec_shim, instruments=[instrument(filling_mode=flags)])
    await h.connect()
    await submit(h, h.market())
    assert sent_requests(h)[0]["type_filling"] == filling


async def test_a_pending_order_always_takes_return_whatever_the_symbols_flags(h):
    await submit(h, h.limit())
    assert sent_requests(h)[0]["type_filling"] == mirror.ORDER_FILLING_RETURN


async def test_a_gtd_buy_limit_is_a_pending_order_expiring_at_its_utc_second(h):
    order = h.limit(OrderSide.BUY, "0.02", "1.07500", **gtd("2026-10-04T00:00:00Z"))
    await submit(h, order)
    assert sent_requests(h) == [
        {
            "action": mirror.TRADE_ACTION_PENDING,
            "symbol": "EURUSD",
            "volume": 0.02,
            "type": mirror.ORDER_TYPE_BUY_LIMIT,
            "price": 1.075,
            "magic": MAGIC,
            "comment": execution.order_comment(order.client_order_id),
            "sl": 0.0,
            "tp": 0.0,
            "type_filling": mirror.ORDER_FILLING_RETURN,
            "type_time": mirror.ORDER_TIME_SPECIFIED,
            "expiration": 1_791_072_000,
        }
    ]


async def test_a_stop_limit_carries_its_trigger_as_the_price_and_its_limit_as_the_stoplimit(h):
    order = h.stop_limit(OrderSide.SELL, price="1.07000", trigger="1.07500")
    await submit(h, order)
    request = sent_requests(h)[0]
    assert request["type"] == mirror.ORDER_TYPE_SELL_STOP_LIMIT
    assert request["price"] == 1.075
    assert request["stoplimit"] == 1.07
    assert request["type_time"] == mirror.ORDER_TIME_GTC
    assert "expiration" not in request


async def test_a_stop_market_buy_is_a_buy_stop_at_its_trigger(h):
    await submit(h, h.stop_market(OrderSide.BUY, trigger="1.09000"))
    request = sent_requests(h)[0]
    assert (request["action"], request["type"], request["price"]) == (
        mirror.TRADE_ACTION_PENDING,
        mirror.ORDER_TYPE_BUY_STOP,
        1.09,
    )
    assert "stoplimit" not in request


# ── Submit: what the client refuses before sending ───────────────────────────


@pytest.mark.parametrize(
    ("make", "named"),
    [
        (lambda h: h.limit(post_only=True), "post-only"),
        (lambda h: h.limit(time_in_force=TimeInForce.IOC), "IOC"),
        (lambda h: h.limit(time_in_force=TimeInForce.DAY), "DAY"),
        (lambda h: h.market(time_in_force=TimeInForce.FOK), "FOK"),
        (
            lambda h: h.factory.trailing_stop_market(
                EURUSD,
                OrderSide.SELL,
                Quantity.from_str("0.01"),
                trailing_offset=Decimal("0.001"),
            ),
            "TRAILING_STOP_MARKET",
        ),
        (
            lambda h: h.factory.limit_if_touched(
                EURUSD,
                OrderSide.BUY,
                Quantity.from_str("0.01"),
                Price.from_str("1.08000"),
                Price.from_str("1.08100"),
            ),
            "LIMIT_IF_TOUCHED",
        ),
    ],
)
async def test_an_unsupported_feature_is_rejected_naming_it_before_anything_is_sent(h, make, named):
    await submit(h, make(h))
    assert h.names() == ["OrderSubmitted", "OrderRejected"]
    assert named in h.events[1].reason
    h.venue.order_send.assert_not_called()


def bracket(h):
    return h.factory.bracket(
        EURUSD,
        OrderSide.BUY,
        Quantity.from_str("0.01"),
        sl_trigger_price=Price.from_str("1.07000"),
        tp_price=Price.from_str("1.09000"),
    )


async def test_a_contingent_order_is_rejected_before_anything_is_sent(h):
    await submit(h, bracket(h).first)
    assert h.names() == ["OrderSubmitted", "OrderRejected"]
    assert "contingent" in h.events[1].reason
    h.venue.order_send.assert_not_called()


async def test_every_order_of_a_list_is_rejected_before_anything_is_sent(h):
    order_list = bracket(h)
    for order in order_list.orders:
        h.cache.add_order(order)
    await h.client._submit_order_list(
        SubmitOrderList(
            trader_id=TRADER_ID,
            strategy_id=STRATEGY_ID,
            order_list=order_list,
            command_id=UUID4(),
            ts_init=0,
        )
    )
    assert h.names() == ["OrderSubmitted", "OrderRejected"] * len(order_list.orders)
    rejected = h.events[1::2]
    assert [event.client_order_id for event in rejected] == [
        order.client_order_id for order in order_list.orders
    ]
    assert all("order list" in event.reason for event in rejected)
    h.venue.order_send.assert_not_called()


# ── Submit: what each answer emits ───────────────────────────────────────────


@pytest.mark.parametrize(
    "retcode",
    [mirror.TRADE_RETCODE_DONE, mirror.TRADE_RETCODE_PLACED, mirror.TRADE_RETCODE_DONE_PARTIAL],
)
async def test_a_success_retcode_accepts_the_order_under_its_ticket_and_never_fills_it(h, retcode):
    h.venue.order_send.return_value = send_result(retcode=retcode, order=5001, deal=6001)
    order = h.market()
    await submit(h, order)
    assert h.names() == ["OrderSubmitted", "OrderAccepted", "AccountState"]
    assert h.events[1].venue_order_id == VenueOrderId("5001")
    assert h.events[1].client_order_id == order.client_order_id


async def test_submitted_is_emitted_before_the_request_is_sent(h):
    seen = []
    h.venue.order_send.side_effect = lambda request: seen.append(h.names()) or send_result()
    await submit(h, h.market())
    assert seen == [["OrderSubmitted"]]


async def test_a_venue_refusal_rejects_with_the_retcodes_name_and_the_venues_comment(h):
    h.venue.order_send.return_value = send_result(
        retcode=mirror.TRADE_RETCODE_INVALID_STOPS, comment="Invalid stops", order=0
    )
    await submit(h, h.limit())
    assert h.names() == ["OrderSubmitted", "OrderRejected", "AccountState"]
    assert h.events[1].reason == "TRADE_RETCODE_INVALID_STOPS: Invalid stops"


async def test_a_retcode_the_package_does_not_name_rejects_with_its_number(h):
    h.venue.order_send.return_value = send_result(retcode=10999, comment="odd", order=0)
    await submit(h, h.limit())
    assert h.events[1].reason == "retcode 10999: odd"


@pytest.mark.parametrize(
    "failure",
    [errors.ServerUnreachable("order_send: refused"), errors.ServerBusy("order_send: busy")],
)
async def test_a_request_that_never_left_is_rejected_unsent_at_once(h, failure):
    h.venue.order_send.side_effect = failure
    await submit(h, h.market())
    assert h.names() == ["OrderSubmitted", "OrderRejected"]
    assert h.events[1].reason.startswith("not sent")
    assert calls_after_order_send(h) == []
    h.conn.get_account_info.assert_not_called()


async def test_no_connection_from_the_venue_emits_nothing_but_one_warning(h):
    h.venue.order_send.return_value = send_result(
        retcode=mirror.TRADE_RETCODE_CONNECTION, comment="No connection", order=0
    )
    await submit(h, h.market())
    assert h.names() == ["OrderSubmitted"]
    assert len(h.logged(LogLevel.WARNING)) == 1


async def test_a_lost_response_emits_nothing_but_one_warning(h):
    h.venue.order_send.side_effect = errors.ResponseLost("order_send: read timed out")
    await submit(h, h.market())
    assert h.names() == ["OrderSubmitted"]
    assert len(h.logged(LogLevel.WARNING)) == 1
    assert calls_after_order_send(h) == []


async def test_a_package_failure_to_answer_emits_nothing_but_one_warning(h):
    h.venue.order_send.return_value = None
    h.venue.last_error.return_value = (mirror.RES_E_INTERNAL_FAIL_TIMEOUT, "IPC timeout")
    await submit(h, h.market())
    assert h.names() == ["OrderSubmitted"]
    assert len(h.logged(LogLevel.WARNING)) == 1


async def test_an_accepted_orders_ticket_is_what_a_later_cancel_removes(h):
    h.venue.order_send.return_value = send_result(retcode=mirror.TRADE_RETCODE_PLACED, order=5003)
    order = h.limit()
    await submit(h, order)
    h.venue.orders_get.side_effect = resting(ours_order(ticket=5003))
    h.venue.order_send.return_value = send_result()
    await h.client._cancel_order(cancel_command(order))
    assert sent_requests(h)[-1] == {
        "action": mirror.TRADE_ACTION_REMOVE,
        "symbol": "EURUSD",
        "order": 5003,
    }


# ── Modify ───────────────────────────────────────────────────────────────────


async def test_a_price_modify_of_a_resting_limit_updates_it(h):
    order = h.place(h.limit(price="1.08000"), ticket=5002)
    h.venue.orders_get.side_effect = resting(ours_order(ticket=5002))
    await h.client._modify_order(modify_command(order, price=Price.from_str("1.07000")))
    assert sent_requests(h) == [
        {
            "action": mirror.TRADE_ACTION_MODIFY,
            "symbol": "EURUSD",
            "order": 5002,
            "price": 1.07,
            "sl": 0.0,
            "tp": 0.0,
            "type_time": mirror.ORDER_TIME_GTC,
        }
    ]
    assert h.names() == ["OrderUpdated", "AccountState"]
    updated = h.events[0]
    assert (updated.price, updated.quantity, updated.venue_order_id) == (
        Price.from_str("1.07000"),
        Quantity.from_str("0.01"),
        VenueOrderId("5002"),
    )


async def test_a_stop_limit_modify_carries_its_trigger_its_limit_and_its_expiry(h):
    order = h.place(h.stop_limit(**gtd("2026-10-04T00:00:00Z")), ticket=5004)
    h.venue.orders_get.side_effect = resting(
        ours_order(ticket=5004, type=mirror.ORDER_TYPE_SELL_STOP_LIMIT)
    )
    await h.client._modify_order(
        modify_command(
            order, price=Price.from_str("1.06000"), trigger_price=Price.from_str("1.06500")
        )
    )
    request = sent_requests(h)[0]
    assert (request["price"], request["stoplimit"]) == (1.065, 1.06)
    assert (request["type_time"], request["expiration"]) == (
        mirror.ORDER_TIME_SPECIFIED,
        1_791_072_000,
    )
    assert (h.events[0].price, h.events[0].trigger_price) == (
        Price.from_str("1.06000"),
        Price.from_str("1.06500"),
    )


async def test_a_quantity_modify_is_rejected_and_nothing_is_sent(h):
    order = h.place(h.limit(quantity="0.01"), ticket=5002)
    await h.client._modify_order(modify_command(order, quantity=Quantity.from_str("0.02")))
    assert h.names() == ["OrderModifyRejected"]
    assert "quantity" in h.events[0].reason
    h.venue.order_send.assert_not_called()


async def test_a_modify_of_a_filled_order_is_rejected_naming_its_state(h):
    order = h.place(h.limit(), ticket=5002)
    h.venue.history_orders_get.return_value = (
        ours_order(ticket=5002, state=mirror.ORDER_STATE_FILLED, volume_current=0.0),
    )
    await h.client._modify_order(modify_command(order, price=Price.from_str("1.07000")))
    assert h.names() == ["OrderModifyRejected", "AccountState"]
    assert "FILLED" in h.events[0].reason
    h.venue.history_orders_get.assert_called_once_with(ticket=5002)
    h.venue.order_send.assert_not_called()


async def test_a_modify_the_venue_refuses_is_rejected_with_the_retcode(h):
    order = h.place(h.limit(), ticket=5002)
    h.venue.orders_get.side_effect = resting(ours_order(ticket=5002))
    h.venue.order_send.return_value = send_result(
        retcode=mirror.TRADE_RETCODE_INVALID_PRICE, comment="Invalid price"
    )
    await h.client._modify_order(modify_command(order, price=Price.from_str("1.07000")))
    assert h.names() == ["OrderModifyRejected", "AccountState"]
    assert h.events[0].reason == "TRADE_RETCODE_INVALID_PRICE: Invalid price"


async def test_a_modify_of_a_market_order_is_rejected_unsent(h):
    order = h.place(h.market(), ticket=5007)
    await h.client._modify_order(modify_command(order, price=Price.from_str("1.07000")))
    assert h.names() == ["OrderModifyRejected"]
    assert "MARKET" in h.events[0].reason
    h.venue.order_send.assert_not_called()


async def test_a_modify_of_an_order_without_a_known_ticket_is_rejected_naming_that(h):
    order = h.place(h.limit(), status=OrderStatus.SUBMITTED)
    await h.client._modify_order(modify_command(order, price=Price.from_str("1.07000")))
    assert h.names() == ["OrderModifyRejected"]
    assert "no venue order" in h.events[0].reason
    h.venue.order_send.assert_not_called()


# ── Cancel ───────────────────────────────────────────────────────────────────


async def test_a_cancel_of_a_resting_order_removes_it_and_cancels_at_the_acknowledgement(h):
    order = h.place(h.limit(), ticket=5001)
    h.venue.orders_get.side_effect = resting(ours_order(ticket=5001))
    await h.client._cancel_order(cancel_command(order))
    assert sent_requests(h) == [
        {"action": mirror.TRADE_ACTION_REMOVE, "symbol": "EURUSD", "order": 5001}
    ]
    assert h.names() == ["OrderCanceled", "AccountState"]
    assert h.events[0].venue_order_id == VenueOrderId("5001")
    h.venue.positions_get.assert_not_called()


async def test_a_cancel_of_a_filled_order_is_rejected_naming_it_and_closes_nothing(h):
    order = h.place(h.market(), status=OrderStatus.FILLED, ticket=5005)
    h.venue.history_orders_get.return_value = (
        ours_order(
            ticket=5005,
            type=mirror.ORDER_TYPE_BUY,
            state=mirror.ORDER_STATE_FILLED,
            volume_current=0.0,
        ),
    )
    await h.client._cancel_order(cancel_command(order))
    assert h.names() == ["OrderCancelRejected", "AccountState"]
    assert "FILLED" in h.events[0].reason
    h.venue.positions_get.assert_not_called()
    h.venue.order_send.assert_not_called()


async def test_a_cancel_the_venue_refuses_is_rejected_with_the_retcode(h):
    order = h.place(h.limit(), ticket=5001)
    h.venue.orders_get.side_effect = resting(ours_order(ticket=5001))
    h.venue.order_send.return_value = send_result(
        retcode=mirror.TRADE_RETCODE_REJECT_CANCEL, comment="Rejected"
    )
    await h.client._cancel_order(cancel_command(order))
    assert h.names() == ["OrderCancelRejected", "AccountState"]
    assert h.events[0].reason == "TRADE_RETCODE_REJECT_CANCEL: Rejected"


async def test_a_cancel_whose_venue_read_fails_is_rejected_unsent(h):
    order = h.place(h.limit(), ticket=5001)
    h.venue.orders_get.side_effect = errors.ServerUnreachable("orders_get: refused")
    await h.client._cancel_order(cancel_command(order))
    assert h.names() == ["OrderCancelRejected"]
    assert h.events[0].reason.startswith("not sent")
    h.venue.order_send.assert_not_called()


async def test_a_cancel_whose_response_is_lost_emits_nothing_but_one_warning(h):
    order = h.place(h.limit(), ticket=5001)
    h.venue.orders_get.side_effect = resting(ours_order(ticket=5001))
    h.venue.order_send.side_effect = errors.ResponseLost("order_send: reset")
    await h.client._cancel_order(cancel_command(order))
    assert h.names() == []
    assert len(h.logged(LogLevel.WARNING)) == 1


async def test_a_cancel_of_an_order_without_a_known_ticket_is_rejected_naming_that(h):
    order = h.place(h.limit(), status=OrderStatus.SUBMITTED)
    await h.client._cancel_order(cancel_command(order))
    assert h.names() == ["OrderCancelRejected"]
    assert "no venue order" in h.events[0].reason
    h.venue.order_send.assert_not_called()


@pytest.fixture
async def strategies(exec_shim):
    """S-001 resting a buy (5101) and a sell (5102), S-002 a buy (5103), one unresolvable order of
    this magic (5104) and another magic's (5105) — all on EURUSD."""
    h = build(exec_shim)
    theirs = OrderFactory(
        trader_id=TRADER_ID, strategy_id=StrategyId("S-002"), clock=h.client._clock
    )
    h.place(h.limit(OrderSide.BUY), ticket=5101)
    h.place(h.limit(OrderSide.SELL, price="1.09000"), ticket=5102)
    h.place(
        theirs.limit(EURUSD, OrderSide.BUY, Quantity.from_str("0.01"), Price.from_str("1.08000")),
        ticket=5103,
    )
    await h.connect()
    h.venue.orders_get.side_effect = resting(
        ours_order(ticket=5101),
        ours_order(ticket=5102, type=mirror.ORDER_TYPE_SELL_LIMIT),
        ours_order(ticket=5103),
        ours_order(ticket=5104, comment="manual"),
        trade_order(ticket=5105, magic=MAGIC + 1),
    )
    return h


@pytest.mark.parametrize(
    ("side", "removed"),
    [(OrderSide.NO_ORDER_SIDE, [5101, 5102]), (OrderSide.BUY, [5101]), (OrderSide.SELL, [5102])],
)
async def test_cancel_all_removes_only_the_commanding_strategys_orders_on_its_side(
    strategies, side, removed
):
    await strategies.client._cancel_all_orders(cancel_all_command(side))
    assert [request["order"] for request in sent_requests(strategies)] == removed
    assert [event.venue_order_id for event in strategies.events] == [
        VenueOrderId(str(ticket)) for ticket in removed
    ]
    strategies.venue.orders_get.assert_any_call(symbol="EURUSD")


async def test_a_batch_cancel_removes_each_order_it_names(strategies):
    named = [
        order
        for order in strategies.cache.orders()
        if order.venue_order_id in (VenueOrderId("5101"), VenueOrderId("5103"))
    ]
    await strategies.client._batch_cancel_orders(
        BatchCancelOrders(
            trader_id=TRADER_ID,
            strategy_id=STRATEGY_ID,
            instrument_id=EURUSD,
            cancels=[cancel_command(order) for order in named],
            command_id=UUID4(),
            ts_init=0,
        )
    )
    assert sorted(request["order"] for request in sent_requests(strategies)) == [5101, 5103]
