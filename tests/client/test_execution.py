"""The execution client's order identity and its submit, modify and cancel boundary: what each trade
request carries to the venue, and which NT event its immediate answer and its verdict emit."""

import asyncio
import time
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
from session_double import request_done, verdict_of
from venue_doubles import tick, trade_order, trade_result

from mt5connector.client import errors, execution
from mt5connector.wire import calls, mirror


@pytest.fixture
async def h(exec_venue):
    harness = build(exec_venue)
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


def submitted(h, order):
    """Submits an order NT already holds submitted, as once the engine applied the submit."""
    h.place(order, status=OrderStatus.SUBMITTED)
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


async def test_the_deviation_is_the_configs(exec_venue):
    h = build(exec_venue, deviation_points=7)
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
    h.venue.symbol_info_tick.assert_called_once_with(symbol="EURUSD")


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
    exec_venue, flags, filling
):
    h = build(exec_venue, instruments=[instrument(filling_mode=flags)])
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


# ── Submit: what the answer and the verdict emit ─────────────────────────────


async def test_a_submit_answered_sent_emits_nothing_until_its_verdict_accepts_it(h):
    order = h.market()
    await submit(h, order)
    assert h.names() == ["OrderSubmitted"]
    await h.verdict(order=5001, deal=6001)
    assert h.names() == ["OrderSubmitted", "OrderAccepted", "AccountState"]
    assert (h.events[1].client_order_id, h.events[1].venue_order_id) == (
        order.client_order_id,
        VenueOrderId("5001"),
    )


@pytest.mark.parametrize(
    "retcode",
    [mirror.TRADE_RETCODE_DONE, mirror.TRADE_RETCODE_PLACED, mirror.TRADE_RETCODE_DONE_PARTIAL],
)
async def test_a_verdict_of_success_accepts_the_order_under_its_ticket_and_never_fills_it(
    h, retcode
):
    order = h.market()
    await submit(h, order)
    await h.verdict(retcode, order=5001, deal=6001)
    assert h.names() == ["OrderSubmitted", "OrderAccepted", "AccountState"]
    assert h.events[1].venue_order_id == VenueOrderId("5001")
    assert h.events[1].client_order_id == order.client_order_id


async def test_submitted_is_emitted_before_the_request_is_sent(h):
    seen = []
    h.venue.order_send.side_effect = lambda request: seen.append(h.names()) or trade_result()
    await submit(h, h.market())
    assert seen == [["OrderSubmitted"]]


async def test_a_venue_refusal_in_the_verdict_rejects_with_the_retcodes_name_and_comment(h):
    await submit(h, h.limit())
    await h.verdict(mirror.TRADE_RETCODE_INVALID_STOPS, comment="Invalid stops")
    assert h.names() == ["OrderSubmitted", "OrderRejected", "AccountState"]
    assert h.events[1].reason == "TRADE_RETCODE_INVALID_STOPS: Invalid stops"


async def test_a_verdict_retcode_the_package_does_not_name_rejects_with_its_number(h):
    await submit(h, h.limit())
    await h.verdict(10999, comment="odd")
    assert h.events[1].reason == "retcode 10999: odd"


@pytest.mark.parametrize(
    ("retcode", "comment", "named"),
    [
        (
            mirror.TRADE_RETCODE_CLIENT_DISABLES_AT,
            "AutoTrading disabled by client",
            "TRADE_RETCODE_CLIENT_DISABLES_AT: AutoTrading disabled by client",
        ),
        (
            mirror.TRADE_RETCODE_CONNECTION,
            "No connection",
            "TRADE_RETCODE_CONNECTION: No connection",
        ),
        (10999, "odd", "retcode 10999: odd"),
    ],
)
async def test_a_request_the_terminal_refuses_itself_is_rejected_at_its_answer(
    h, retcode, comment, named
):
    h.venue.order_send.return_value = trade_result(retcode=retcode, comment=comment)
    await submit(h, h.limit())
    assert h.names() == ["OrderSubmitted", "OrderRejected", "AccountState"]
    assert h.events[1].reason == named
    assert h.push.sent == {}


@pytest.mark.parametrize(
    "failure",
    [
        errors.EaError("order_send: the publisher chart of EURUSD failed to open", 4301),
        errors.NotReady("order_send: the broker clock is not verified", 0, 5.0),
        errors.Syncing("order_send: syncing", 0, 5.0),
        errors.Overflow("order_send: the queue is full", 0),
    ],
    ids=["ea-error", "not-ready", "syncing", "overflow"],
)
async def test_a_request_that_never_reached_the_venue_is_rejected_unsent_at_once(h, failure):
    h.venue.order_send.side_effect = failure
    await submit(h, h.market())
    assert h.names() == ["OrderSubmitted", "OrderRejected"]
    assert h.events[1].reason == f"not sent: {failure}"
    assert calls_after_order_send(h) == []
    assert len(h.venue.order_send.call_args_list) == 1
    h.conn.get_account_info.assert_not_called()


def no_publisher() -> errors.NoPublisher:
    return errors.NoPublisher("order_send: no EA serves the publisher of EURUSD", 0, 0.01)


async def test_a_command_waits_out_its_symbols_chart_opening_then_is_sent(h):
    h.venue.order_send.side_effect = [no_publisher(), no_publisher(), trade_result()]
    await submit(h, h.limit())
    assert h.names() == ["OrderSubmitted"]
    assert len(h.venue.order_send.call_args_list) == 3
    await h.verdict(order=5001)
    assert h.names() == ["OrderSubmitted", "OrderAccepted", "AccountState"]


async def test_a_chart_that_does_not_open_within_the_bound_rejects_the_command_unsent(
    h, monkeypatch
):
    monkeypatch.setattr(execution, "_NO_PUBLISHER_BOUND_S", 0.05)
    h.venue.order_send.side_effect = no_publisher()
    await submit(h, h.limit())
    assert h.names() == ["OrderSubmitted", "OrderRejected"]
    assert h.events[1].reason == f"not sent: {no_publisher()}"
    assert 1 < len(h.venue.order_send.call_args_list) <= 6


async def test_a_command_whose_next_ask_would_fall_past_the_bound_is_rejected_unsent(
    h, monkeypatch
):
    monkeypatch.setattr(execution, "_NO_PUBLISHER_BOUND_S", 0.05)
    late = errors.NoPublisher("order_send: no EA serves the publisher of EURUSD", 0, 0.06)
    h.venue.order_send.side_effect = [late, trade_result()]
    await submit(h, h.limit())
    assert h.names() == ["OrderSubmitted", "OrderRejected"]
    assert h.events[1].reason == f"not sent: {late}"
    assert len(h.venue.order_send.call_args_list) == 1


async def test_a_command_waking_past_the_bound_is_rejected_unsent_without_asking_again(
    h, monkeypatch
):
    monkeypatch.setattr(execution, "_NO_PUBLISHER_BOUND_S", 0.05)
    deferred = no_publisher()
    asked = []

    def order_send(request):
        asked.append(request)
        if len(asked) == 1:
            # The loop stalls past the bound while the command waits out the deferral.
            asyncio.get_running_loop().call_soon(time.sleep, 0.08)
            raise deferred
        return trade_result()

    h.venue.order_send.side_effect = order_send
    await submit(h, h.limit())
    assert h.names() == ["OrderSubmitted", "OrderRejected"]
    assert h.events[1].reason == f"not sent: {deferred}"
    assert len(asked) == 1


@pytest.mark.parametrize(
    "failure",
    [
        errors.ResponseLost("hub session: order_send unanswered past the session's deadline"),
        errors.HubTimeout("order_send: the EA did not answer within its deadline", 0),
        errors.EaGone("order_send: the EA left", 0),
        errors.ProtocolError("hub session: order_send: answer frame: truncated"),
    ],
    ids=["response-lost", "hub-timeout", "ea-gone", "protocol-error"],
)
async def test_a_lost_answer_emits_nothing_but_one_warning(h, failure):
    h.venue.order_send.side_effect = failure
    await submit(h, h.market())
    assert h.names() == ["OrderSubmitted"]
    assert len(h.logged(LogLevel.WARNING)) == 1
    assert calls_after_order_send(h) == []


async def test_an_entry_whose_answer_was_lost_is_accepted_by_its_requests_comment(h):
    order = h.limit()
    h.venue.order_send.side_effect = errors.ResponseLost("hub session: order_send: reset")
    await submitted(h, order)
    h.venue.orders_get.side_effect = resting(
        ours_order(ticket=5004, time_setup_msc=1_760_000_002_000)
    )
    await h.delivered(
        request_done(
            5004,
            execution.order_comment(order.client_order_id),
            MAGIC,
            action=mirror.TRADE_ACTION_PENDING,
            retcode=mirror.TRADE_RETCODE_PLACED,
            request_id=41,
        )
    )
    assert h.names() == ["OrderSubmitted", "OrderAccepted"]
    assert (h.events[1].venue_order_id, h.events[1].ts_event) == (
        VenueOrderId("5004"),
        1_760_000_002_000_000_000,
    )


async def test_a_no_connection_verdict_emits_nothing_but_one_warning(h):
    await submit(h, h.market())
    await h.verdict(mirror.TRADE_RETCODE_CONNECTION, comment="No connection")
    assert h.names() == ["OrderSubmitted"]
    assert len(h.logged(LogLevel.WARNING)) == 1


async def test_a_verdict_taken_before_its_command_resumes_settles_it_once_it_does(h):
    order = h.market()

    def sent_and_settled(request):
        # The session hands the REQUEST over before the sender resumes from its answer.
        h.push.deliver(
            verdict_of(
                calls.trade_request(request), 41, mirror.TRADE_RETCODE_DONE, order=5001, deal=6001
            )
        )
        return trade_result(request_id=41)

    h.venue.order_send.side_effect = sent_and_settled
    await submit(h, order)
    assert h.names() == ["OrderSubmitted", "OrderAccepted", "AccountState"]
    assert h.events[1].venue_order_id == VenueOrderId("5001")
    await h.verdict(request_id=41, order=5001, deal=6001)
    assert h.names() == ["OrderSubmitted", "OrderAccepted", "AccountState"]


async def test_a_verdict_held_for_a_command_whose_answer_was_lost_goes_to_the_comment_link(h):
    order = h.limit()
    comment = execution.order_comment(order.client_order_id)

    def settled_then_lost(request):
        h.push.deliver(
            verdict_of(calls.trade_request(request), 41, mirror.TRADE_RETCODE_PLACED, order=5004)
        )
        raise errors.EaGone("order_send: the EA left", 0)

    h.venue.order_send.side_effect = settled_then_lost
    h.venue.orders_get.side_effect = resting(ours_order(ticket=5004, comment=comment))
    await submitted(h, order)
    await h.drained()
    assert h.names() == ["OrderSubmitted", "OrderAccepted"]
    assert h.events[1].venue_order_id == VenueOrderId("5004")
    assert len(h.logged(LogLevel.WARNING)) == 1


async def test_a_held_verdict_no_command_claims_is_released_once_past_the_bound(h, monkeypatch):
    monkeypatch.setattr(execution, "_UNCLAIMED_S", 0.01)
    monkeypatch.setattr(execution, "_NO_PUBLISHER_BOUND_S", 1.0)
    waiting = h.limit()
    lost = h.limit(price="1.07000")
    h.place(lost, status=OrderStatus.SUBMITTED)
    h.venue.orders_get.side_effect = resting(
        ours_order(ticket=5004, comment=execution.order_comment(lost.client_order_id))
    )
    h.venue.order_send.side_effect = no_publisher()
    command = asyncio.get_running_loop().create_task(submit(h, waiting))
    await asyncio.sleep(0.02)
    held = request_done(5004, execution.order_comment(lost.client_order_id), MAGIC, request_id=77)
    await h.delivered(held)
    assert h.names() == ["OrderSubmitted"]
    await asyncio.sleep(0.02)
    await h.delivered(request_done(5005, "manual", MAGIC, request_id=78))
    linked = [event for event in h.events if event.client_order_id == lost.client_order_id]
    assert [type(event).__name__ for event in linked] == ["OrderAccepted"]
    command.cancel()
    await asyncio.gather(command, return_exceptions=True)
    await h.drained()


async def test_a_request_of_another_magic_settles_nothing(h):
    order = h.market()
    await submit(h, order)
    (request_id,) = h.push.sent
    theirs = h.push.sent[request_id] | {"magic": MAGIC + 1}
    await h.delivered(verdict_of(theirs, request_id, mirror.TRADE_RETCODE_DONE, order=5001))
    assert h.names() == ["OrderSubmitted"]
    await h.verdict(order=5001)
    assert h.names() == ["OrderSubmitted", "OrderAccepted", "AccountState"]


async def test_every_command_is_one_order_send_on_the_session_naming_its_symbol_and_magic(h):
    entry = h.market()
    await submit(h, entry)
    resting_limit = h.place(h.limit(), ticket=5002)
    h.venue.orders_get.side_effect = resting(ours_order(ticket=5002))
    await h.client._modify_order(modify_command(resting_limit, price=Price.from_str("1.07000")))
    await h.client._cancel_order(cancel_command(resting_limit))
    assert [request["action"] for request in h.push.sent.values()] == [
        mirror.TRADE_ACTION_DEAL,
        mirror.TRADE_ACTION_MODIFY,
        mirror.TRADE_ACTION_REMOVE,
    ]
    assert [(request["symbol"], request["magic"]) for request in h.push.sent.values()] == [
        ("EURUSD", MAGIC)
    ] * 3
    assert h.push.sent[1]["comment"] == execution.order_comment(entry.client_order_id)
    assert len(h.venue.order_send.call_args_list) == 3


@pytest.mark.parametrize(
    ("command", "rejected"),
    [
        (execution.Command.MODIFY, "OrderModifyRejected"),
        (execution.Command.REMOVE, "OrderCancelRejected"),
    ],
)
async def test_a_modify_or_cancel_the_terminal_refuses_itself_is_rejected_at_its_answer(
    h, command, rejected
):
    order = h.place(h.limit(), ticket=5002)
    h.venue.orders_get.side_effect = resting(ours_order(ticket=5002))
    h.venue.order_send.return_value = trade_result(
        retcode=mirror.TRADE_RETCODE_CLIENT_DISABLES_AT, comment="AutoTrading disabled by client"
    )
    if command == execution.Command.MODIFY:
        await h.client._modify_order(modify_command(order, price=Price.from_str("1.07000")))
    else:
        await h.client._cancel_order(cancel_command(order))
    assert h.names() == [rejected, "AccountState"]
    assert h.events[0].reason == "TRADE_RETCODE_CLIENT_DISABLES_AT: AutoTrading disabled by client"
    assert h.events[0].venue_order_id == VenueOrderId("5002")
    assert h.push.sent == {}


async def test_a_session_not_connected_fails_the_submit_as_a_programming_error(h):
    await h.push.disconnect()
    with pytest.raises(RuntimeError, match="not connected"):
        await submit(h, h.limit())
    assert h.names() == ["OrderSubmitted"]


async def test_an_accepted_orders_ticket_is_what_a_later_cancel_removes(h):
    order = h.limit()
    await submit(h, order)
    await h.verdict(mirror.TRADE_RETCODE_PLACED, order=5003)
    h.venue.orders_get.side_effect = resting(ours_order(ticket=5003))
    await h.client._cancel_order(cancel_command(order))
    assert sent_requests(h)[-1] == {
        "action": mirror.TRADE_ACTION_REMOVE,
        "symbol": "EURUSD",
        "order": 5003,
        "magic": MAGIC,
    }


# ── Modify ───────────────────────────────────────────────────────────────────


async def test_a_price_modify_of_a_resting_limit_updates_it_on_its_verdict(h):
    order = h.place(h.limit(price="1.08000"), ticket=5002)
    h.venue.orders_get.side_effect = resting(ours_order(ticket=5002))
    await h.client._modify_order(modify_command(order, price=Price.from_str("1.07000")))
    assert sent_requests(h) == [
        {
            "action": mirror.TRADE_ACTION_MODIFY,
            "symbol": "EURUSD",
            "order": 5002,
            "magic": MAGIC,
            "price": 1.07,
            "sl": 0.0,
            "tp": 0.0,
            "type_time": mirror.ORDER_TIME_GTC,
        }
    ]
    assert h.names() == []
    await h.verdict(order=5002)
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
    await h.verdict(order=5004)
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
    await h.client._modify_order(modify_command(order, price=Price.from_str("1.07000")))
    await h.verdict(mirror.TRADE_RETCODE_INVALID_PRICE, comment="Invalid price")
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


async def test_a_cancel_of_a_resting_order_removes_it_and_cancels_on_its_verdict(h):
    order = h.place(h.limit(), ticket=5001)
    h.venue.orders_get.side_effect = resting(ours_order(ticket=5001))
    await h.client._cancel_order(cancel_command(order))
    assert sent_requests(h) == [
        {"action": mirror.TRADE_ACTION_REMOVE, "symbol": "EURUSD", "order": 5001, "magic": MAGIC}
    ]
    assert h.names() == []
    await h.verdict(order=5001)
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
    await h.client._cancel_order(cancel_command(order))
    await h.verdict(mirror.TRADE_RETCODE_REJECT_CANCEL, comment="Rejected")
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
async def strategies(exec_venue):
    """S-001 resting a buy (5101) and a sell (5102), S-002 a buy (5103), one unresolvable order of
    this magic (5104) and another magic's (5105) — all on EURUSD."""
    h = build(exec_venue)
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
    for request_id in strategies.push.sent:
        await strategies.verdict(request_id=request_id)
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
