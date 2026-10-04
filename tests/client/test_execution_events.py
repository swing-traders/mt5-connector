"""The execution client's view of the venue: the ticket index it rebuilds at connect, the fills the
deal stream carries, the pending orders the venue ends, the account it reports, and how its poll
loop rides out a server it cannot reach."""

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from exec_harness import ACCOUNT_ID, MAGIC, STRATEGY_ID, TRADER_ID, build, ours_deal, ours_order
from nautilus_trader.common.enums import LogLevel
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.messages import CancelOrder, SubmitOrder
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.enums import LiquiditySide, OrderSide, OrderStatus, OrderType
from nautilus_trader.model.identifiers import PositionId, TradeId, VenueOrderId
from nautilus_trader.model.objects import Money, Price, Quantity
from nautilus_trader.test_kit.stubs.events import TestEventStubs
from venue_doubles import account_info, send_result, trade_deal

from mt5connector.client import errors, execution
from mt5connector.wire import mirror


async def connected(exec_shim, **settings):
    h = build(exec_shim, **settings)
    await h.connect()
    return h


def turn(h, deals=None, open_orders=None):
    """One poll turn of the client over the venue answering `deals` and `open_orders`."""
    if deals is not None:
        h.venue.history_deals_get.return_value = tuple(deals)
    if open_orders is not None:
        h.venue.orders_get.return_value = tuple(open_orders)
    h.client._poll_turn()


async def stop(task):
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


def fills(h) -> list:
    return [event for event in h.events if type(event).__name__ == "OrderFilled"]


# ── The ticket index ─────────────────────────────────────────────────────────


async def test_fills_book_through_the_index_rebuilt_from_nts_orders(exec_shim):
    h = build(exec_shim)
    order = h.place(h.limit(), ticket=9001)
    await h.connect()
    turn(h, deals=[ours_deal(ticket=7001, order=9001)])
    assert [fill.client_order_id for fill in fills(h)] == [order.client_order_id]
    h.venue.history_orders_get.assert_not_called()


async def test_a_venue_order_id_that_is_not_a_ticket_is_left_out_of_the_index(exec_shim):
    h = build(exec_shim)
    h.place(h.limit(), ticket="9001-SL-1")
    await h.connect()
    debug = h.logged(LogLevel.DEBUG)
    assert len([line for line in debug if "9001-SL-1" in line]) == 1


# ── The deal stream ──────────────────────────────────────────────────────────


async def test_deals_already_in_the_history_at_connect_are_never_emitted(exec_shim):
    h = build(exec_shim)
    h.place(h.limit(), ticket=9001)
    windows = []

    def history_deals_get(date_from, date_to):
        windows.append((date_from, date_to))
        return (ours_deal(ticket=7001, order=9001),)

    exec_shim.history_deals_get.side_effect = history_deals_get
    await h.connect()
    h.client._poll_turn()
    assert fills(h) == []
    assert windows[0][1] - windows[0][0] == timedelta(minutes=60)


async def test_two_deals_on_two_positions_fill_with_their_own_venue_position_ids(exec_shim):
    h = build(exec_shim)
    buy = h.place(h.limit(OrderSide.BUY), ticket=9001)
    sell = h.place(h.market(OrderSide.SELL, "0.02"), ticket=9002)
    await h.connect()
    turn(
        h,
        deals=[
            ours_deal(
                ticket=7001,
                order=9001,
                type=mirror.DEAL_TYPE_BUY,
                position_id=8133477,
                time_msc=1_760_000_000_123,
                volume=0.01,
                price=1.08,
                commission=-0.5,
            ),
            ours_deal(
                ticket=7002,
                order=9002,
                type=mirror.DEAL_TYPE_SELL,
                position_id=8133478,
                time_msc=1_760_000_001_456,
                volume=0.02,
                price=1.0851,
                commission=-1.0,
                fee=-0.1,
            ),
        ],
    )
    assert h.names() == ["OrderFilled", "OrderFilled", "AccountState"]
    first, second = fills(h)
    assert (
        first.strategy_id,
        first.client_order_id,
        first.venue_order_id,
        first.position_id,
        first.trade_id,
        first.order_side,
        first.order_type,
        first.last_qty,
        first.last_px,
        first.currency,
        first.commission,
        first.liquidity_side,
        first.ts_event,
    ) == (
        STRATEGY_ID,
        buy.client_order_id,
        VenueOrderId("9001"),
        PositionId("8133477"),
        TradeId("7001"),
        OrderSide.BUY,
        OrderType.LIMIT,
        Quantity.from_str("0.01"),
        Price.from_str("1.08000"),
        USD,
        Money(Decimal("0.50"), USD),
        LiquiditySide.MAKER,
        1_760_000_000_123_000_000,
    )
    assert (
        second.client_order_id,
        second.venue_order_id,
        second.position_id,
        second.trade_id,
        second.order_side,
        second.order_type,
        second.last_qty,
        second.last_px,
        second.commission,
        second.liquidity_side,
        second.ts_event,
    ) == (
        sell.client_order_id,
        VenueOrderId("9002"),
        PositionId("8133478"),
        TradeId("7002"),
        OrderSide.SELL,
        OrderType.MARKET,
        Quantity.from_str("0.02"),
        Price.from_str("1.08510"),
        Money(Decimal("1.10"), USD),
        LiquiditySide.TAKER,
        1_760_000_001_456_000_000,
    )


@pytest.mark.parametrize(
    ("commission", "fee", "charged"),
    [(-0.5, -0.1, "0.60"), (0.3, 0.0, "-0.30"), (0.0, 0.0, "0.00")],
    ids=["charge-and-fee", "rebate", "free"],
)
async def test_a_fills_commission_is_the_charge_the_deal_carries(
    exec_shim, commission, fee, charged
):
    h = build(exec_shim)
    h.place(h.limit(), ticket=9001)
    await h.connect()
    turn(h, deals=[ours_deal(ticket=7001, order=9001, commission=commission, fee=fee)])
    assert fills(h)[0].commission == Money(Decimal(charged), USD)


async def test_a_deal_seen_twice_fills_once(exec_shim):
    h = build(exec_shim)
    h.place(h.limit(), ticket=9001)
    await h.connect()
    deal = ours_deal(ticket=7001, order=9001)
    turn(h, deals=[deal, deal])
    turn(h, deals=[deal])
    assert [fill.trade_id for fill in fills(h)] == [TradeId("7001")]


async def test_the_deal_window_opens_a_second_before_the_last_deal(exec_shim):
    h = build(exec_shim)
    h.place(h.limit(), ticket=9001)
    exec_shim.history_deals_get.return_value = (
        ours_deal(ticket=7000, order=9001, time_msc=1_760_000_000_000),
    )
    await h.connect()
    turn(h, deals=[ours_deal(ticket=7001, order=9001, time_msc=1_760_000_001_456)])
    after_connect, _ = h.venue.history_deals_get.call_args_list[0].args
    turn(h, deals=[])
    after_deal, _ = h.venue.history_deals_get.call_args.args
    assert after_connect == datetime(2025, 10, 9, 8, 53, 19, tzinfo=UTC)
    assert after_deal == datetime(2025, 10, 9, 8, 53, 20, 456_000, tzinfo=UTC)


async def test_a_deal_of_an_unindexed_order_books_to_the_in_flight_order_its_comment_digests(
    exec_shim,
):
    h = build(exec_shim)
    order = h.place(h.market(), status=OrderStatus.SUBMITTED)
    await h.connect()
    h.venue.history_orders_get.return_value = (
        ours_order(
            ticket=9100,
            type=mirror.ORDER_TYPE_BUY,
            state=mirror.ORDER_STATE_FILLED,
            comment=execution.order_comment(order.client_order_id),
        ),
    )
    turn(h, deals=[ours_deal(ticket=7101, order=9100)])
    turn(h, deals=[ours_deal(ticket=7102, order=9100)])
    assert [fill.client_order_id for fill in fills(h)] == [order.client_order_id] * 2
    assert [fill.order_type for fill in fills(h)] == [OrderType.MARKET] * 2
    h.venue.history_orders_get.assert_called_once_with(ticket=9100)


async def test_a_partly_filled_resting_orders_comment_is_read_off_the_open_order(exec_shim):
    h = build(exec_shim)
    order = h.place(h.limit(quantity="0.02"), status=OrderStatus.SUBMITTED)
    await h.connect()
    h.venue.orders_get.side_effect = lambda symbol=None, group=None, ticket=None: (
        (ours_order(ticket=9101, comment=execution.order_comment(order.client_order_id)),)
        if ticket == 9101
        else ()
    )
    turn(h, deals=[ours_deal(ticket=7103, order=9101)])
    assert [fill.client_order_id for fill in fills(h)] == [order.client_order_id]


async def test_a_deal_matching_no_order_emits_nothing_and_logs_one_info_naming_its_ticket(
    exec_shim,
):
    h = build(exec_shim)
    await h.connect()
    h.venue.history_orders_get.return_value = (ours_order(ticket=9200, comment="manual"),)
    deal = ours_deal(ticket=7201, order=9200)
    turn(h, deals=[deal])
    turn(h, deals=[deal])
    assert h.names() == []
    assert len([line for line in h.logged(LogLevel.INFO) if "9200" in line]) == 1


async def test_another_magics_deal_and_a_balance_deal_are_skipped_silently(exec_shim):
    h = build(exec_shim)
    await h.connect()
    turn(
        h,
        deals=[
            trade_deal(ticket=7301, order=9300, magic=MAGIC + 1),
            ours_deal(ticket=7302, order=0, type=mirror.DEAL_TYPE_BALANCE, volume=0.0),
        ],
    )
    assert h.names() == []
    h.venue.history_orders_get.assert_not_called()
    assert [line for line in h.logged(LogLevel.INFO) if "730" in line or "9300" in line] == []


async def test_a_deal_whose_fill_cannot_be_built_yet_fills_once_it_can(exec_shim):
    h = build(exec_shim)
    h.place(h.limit(), ticket=9001)
    await h.connect()
    loaded = h.instruments.pop("EURUSD")
    deal = ours_deal(ticket=7001, order=9001)
    turn(h, deals=[deal])
    assert h.names() == []
    assert len(h.client.recorded_log.exception.call_args_list) == 1
    h.instruments["EURUSD"] = loaded
    turn(h, deals=[deal])
    assert [fill.trade_id for fill in fills(h)] == [TradeId("7001")]


async def test_the_account_is_reported_while_a_fill_cannot_be_built(exec_shim):
    h = build(exec_shim, account_refresh_seconds=1)
    h.place(h.limit(), ticket=9001)
    h.place(h.limit(), ticket=9002)
    await h.connect()
    deals = [
        ours_deal(ticket=7001, order=9001, time_msc=1_760_000_000_000),
        ours_deal(ticket=7002, order=9002, time_msc=1_760_000_001_000, symbol="GBPUSD"),
    ]
    turn(h, deals=deals)
    assert h.names() == ["OrderFilled", "AccountState"]
    await asyncio.sleep(1.05)
    turn(h, deals=deals)
    assert h.names() == ["OrderFilled", "AccountState", "AccountState"]
    assert len(h.client.recorded_log.exception.call_args_list) == 2


async def test_an_account_report_owed_by_an_event_survives_a_turn_that_fails_after_it(exec_shim):
    h = build(exec_shim)
    h.place(h.limit(), ticket=9001)
    await h.connect()
    h.venue.orders_get.side_effect = errors.ServerUnreachable("orders_get: refused")
    h.conn.get_account_info.side_effect = errors.ServerUnreachable("account_info: refused")
    turn(h, deals=[ours_deal(ticket=7001, order=9001)])
    assert h.names() == ["OrderFilled"]
    h.venue.orders_get.side_effect = None
    h.conn.get_account_info.side_effect = None
    turn(h, deals=[])
    assert h.names() == ["OrderFilled", "AccountState"]


# ── Pending orders the venue ends ────────────────────────────────────────────


async def resting_at_connect(exec_shim, ticket=9400):
    h = build(exec_shim)
    order = h.place(h.limit(), ticket=ticket)
    exec_shim.orders_get.return_value = (ours_order(ticket=ticket),)
    await h.connect()
    h.venue.orders_get.return_value = ()
    return h, order


@pytest.mark.parametrize(
    ("state", "event"),
    [
        (mirror.ORDER_STATE_CANCELED, "OrderCanceled"),
        (mirror.ORDER_STATE_EXPIRED, "OrderExpired"),
        (mirror.ORDER_STATE_REJECTED, "OrderRejected"),
    ],
)
async def test_a_pending_order_the_venue_ended_emits_its_end_once(exec_shim, state, event):
    h, order = await resting_at_connect(exec_shim)
    h.venue.history_orders_get.return_value = (
        ours_order(ticket=9400, state=state, time_done_msc=1_760_000_005_000),
    )
    turn(h)
    turn(h)
    assert h.names() == [event, "AccountState"]
    ended = h.events[0]
    assert ended.client_order_id == order.client_order_id
    assert ended.ts_event == 1_760_000_005_000_000_000
    h.venue.history_orders_get.assert_called_once_with(ticket=9400)


@pytest.mark.parametrize(
    ("state", "ends"),
    [
        (mirror.ORDER_STATE_CANCELED, ["OrderAccepted", "OrderCanceled"]),
        (mirror.ORDER_STATE_EXPIRED, ["OrderAccepted", "OrderExpired"]),
        (mirror.ORDER_STATE_REJECTED, ["OrderRejected"]),
    ],
)
async def test_an_unanswered_submits_order_ended_before_the_first_poll_ends_through_its_digest(
    exec_shim, state, ends
):
    h = build(exec_shim)
    order = h.place(h.limit(), status=OrderStatus.SUBMITTED)
    comment = execution.order_comment(order.client_order_id)
    exec_shim.orders_get.return_value = (ours_order(ticket=9600, comment=comment),)
    await h.connect()
    h.venue.history_orders_get.return_value = (
        ours_order(
            ticket=9600,
            comment=comment,
            state=state,
            time_setup_msc=1_760_000_002_000,
            time_done_msc=1_760_000_005_000,
        ),
    )
    turn(h, open_orders=[])
    turn(h, open_orders=[])
    assert h.names() == ends + ["AccountState"]
    assert [event.client_order_id for event in h.events] == [order.client_order_id] * len(ends)
    assert h.events[-1].ts_event == 1_760_000_005_000_000_000
    if len(ends) == 2:
        assert (h.events[0].venue_order_id, h.events[0].ts_event) == (
            VenueOrderId("9600"),
            1_760_000_002_000_000_000,
        )


async def test_an_ended_order_matching_no_nt_order_logs_one_info_naming_its_ticket(exec_shim):
    h = build(exec_shim)
    exec_shim.orders_get.return_value = (ours_order(ticket=9601, comment="manual"),)
    await h.connect()
    h.venue.history_orders_get.return_value = (
        ours_order(ticket=9601, comment="manual", state=mirror.ORDER_STATE_CANCELED),
    )
    turn(h, open_orders=[])
    turn(h, open_orders=[])
    assert h.names() == []
    assert len([line for line in h.logged(LogLevel.INFO) if "9601" in line]) == 1


async def test_a_pending_order_that_filled_emits_nothing_for_its_end(exec_shim):
    h, _ = await resting_at_connect(exec_shim)
    h.venue.history_orders_get.return_value = (
        ours_order(ticket=9400, state=mirror.ORDER_STATE_FILLED, volume_current=0.0),
    )
    turn(h)
    assert h.names() == []


async def test_a_vanished_order_the_history_does_not_hold_yet_is_asked_again(exec_shim):
    h, _ = await resting_at_connect(exec_shim)
    h.venue.history_orders_get.return_value = ()
    turn(h)
    h.venue.history_orders_get.return_value = (
        ours_order(ticket=9400, state=mirror.ORDER_STATE_CANCELED),
    )
    turn(h)
    assert h.names() == ["OrderCanceled", "AccountState"]


async def test_a_pending_order_placed_between_polls_is_watched_for_its_end(exec_shim):
    h = await connected(exec_shim)
    h.venue.order_send.return_value = send_result(retcode=mirror.TRADE_RETCODE_PLACED, order=9401)
    order = h.place(h.limit(), status=OrderStatus.SUBMITTED)
    await h.client._submit_order(
        SubmitOrder(
            trader_id=TRADER_ID,
            strategy_id=STRATEGY_ID,
            order=order,
            command_id=UUID4(),
            ts_init=0,
        )
    )
    # As the engine applies the acceptance the client emitted.
    order.apply(
        TestEventStubs.order_accepted(
            order, account_id=ACCOUNT_ID, venue_order_id=VenueOrderId("9401")
        )
    )
    h.cache.update_order(order)
    h.ledger.clear()
    h.venue.history_orders_get.return_value = (
        ours_order(ticket=9401, state=mirror.ORDER_STATE_CANCELED),
    )
    turn(h, open_orders=[])
    assert h.names() == ["OrderCanceled", "AccountState"]


async def test_an_order_this_client_cancelled_is_not_cancelled_again_when_it_vanishes(exec_shim):
    h, order = await resting_at_connect(exec_shim)
    h.venue.orders_get.side_effect = lambda symbol=None, group=None, ticket=None: (
        (ours_order(ticket=9400),) if ticket == 9400 else ()
    )
    await h.client._cancel_order(
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
    h.venue.orders_get.side_effect = None
    h.venue.history_orders_get.return_value = (
        ours_order(ticket=9400, state=mirror.ORDER_STATE_CANCELED),
    )
    turn(h, open_orders=[])
    assert h.names() == ["OrderCanceled", "AccountState"]


# ── Resting orders the index does not know ───────────────────────────────────


async def test_a_resting_order_of_an_unanswered_submit_is_accepted_under_its_ticket(exec_shim):
    h = build(exec_shim)
    order = h.place(h.limit(), status=OrderStatus.SUBMITTED)
    await h.connect()
    venue_order = ours_order(
        ticket=9500,
        comment=execution.order_comment(order.client_order_id),
        time_setup_msc=1_760_000_002_000,
    )
    turn(h, open_orders=[venue_order])
    turn(h, open_orders=[venue_order])
    assert h.names() == ["OrderAccepted", "AccountState"]
    accepted = h.events[0]
    assert (accepted.client_order_id, accepted.venue_order_id, accepted.ts_event) == (
        order.client_order_id,
        VenueOrderId("9500"),
        1_760_000_002_000_000_000,
    )


async def test_a_resting_order_matching_nothing_is_logged_once(exec_shim):
    h = build(exec_shim)
    await h.connect()
    venue_order = ours_order(ticket=9501, comment="manual")
    turn(h, open_orders=[venue_order])
    turn(h, open_orders=[venue_order])
    assert h.names() == []
    assert len([line for line in h.logged(LogLevel.INFO) if "9501" in line]) == 1


# ── The account ──────────────────────────────────────────────────────────────


async def test_the_account_books_balance_and_credit_with_the_margin_locked(exec_shim):
    h = build(
        exec_shim,
        account=account_info(
            balance=10000.0, credit=50.0, margin=100.0, equity=10075.25, margin_free=9975.25
        ),
    )
    h.place(h.limit(), ticket=9001)
    await h.connect()
    turn(h, deals=[ours_deal(ticket=7001, order=9001)])
    assert h.names() == ["OrderFilled", "AccountState"]
    state = h.ledger[-1]
    assert state.account_id == ACCOUNT_ID
    assert state.is_reported
    (balance,) = state.balances
    assert (balance.total, balance.locked, balance.free) == (
        Money(Decimal("10050.00"), USD),
        Money(Decimal("100.00"), USD),
        Money(Decimal("9950.00"), USD),
    )


async def test_the_account_is_reported_on_the_configs_refresh_period(exec_shim):
    h = await connected(exec_shim, account_refresh_seconds=1, exec_poll_interval_ms=50)
    task = asyncio.get_running_loop().create_task(h.client._exec_poll_loop())
    await asyncio.sleep(0.6)
    assert h.names() == []
    await asyncio.sleep(0.7)
    await stop(task)
    assert h.names() == ["AccountState"]


# ── The poll loop ────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "failure", [errors.ServerBusy, errors.ServerUnreachable, errors.ResponseLost]
)
async def test_successful_venue_reads_do_not_clear_an_account_outage(exec_shim, failure):
    h = build(exec_shim)
    h.place(h.limit(), ticket=9001)
    await h.connect()
    h.conn.get_account_info.side_effect = failure("account_info: unavailable")
    for _ in range(3):
        turn(h, deals=[ours_deal(ticket=7001, order=9001)])
    assert h.names() == ["OrderFilled"]
    assert h.conn.get_account_info.call_count == 3
    assert (
        len(h.logged(LogLevel.WARNING)),
        len([line for line in h.logged(LogLevel.INFO) if "answers again" in line]),
    ) == (1, 0)

    h.conn.get_account_info.side_effect = None
    turn(h, deals=[])
    turn(h)
    assert h.names() == ["OrderFilled", "AccountState"]
    assert len(h.logged(LogLevel.WARNING)) == 1
    assert [line for line in h.logged(LogLevel.INFO) if "answers again" in line] == [
        f"execution poll {execution.PollStep.ACCOUNT_REPORT}: the server answers again"
    ]
    h.conn.reconnect_async.assert_not_called()


@pytest.mark.parametrize(
    "failure", [errors.ServerBusy, errors.ServerUnreachable, errors.ResponseLost]
)
async def test_successful_account_reports_do_not_clear_a_venue_outage(exec_shim, failure):
    h = build(exec_shim)
    h.place(h.limit(quantity="0.03"), ticket=9001)
    await h.connect()
    h.venue.orders_get.side_effect = failure("orders_get: unavailable")
    for ticket in range(7001, 7004):
        turn(h, deals=[ours_deal(ticket=ticket, order=9001)])
    assert h.names() == ["OrderFilled", "AccountState"] * 3
    assert h.conn.get_account_info.call_count == 3
    assert (
        len(h.logged(LogLevel.WARNING)),
        len([line for line in h.logged(LogLevel.INFO) if "answers again" in line]),
    ) == (1, 0)

    h.venue.orders_get.side_effect = None
    turn(h, deals=[])
    turn(h)
    assert h.names() == ["OrderFilled", "AccountState"] * 3
    assert len(h.logged(LogLevel.WARNING)) == 1
    assert [line for line in h.logged(LogLevel.INFO) if "answers again" in line] == [
        f"execution poll {execution.PollStep.VENUE_READ}: the server answers again"
    ]
    h.conn.reconnect_async.assert_not_called()


async def test_a_busy_or_unreachable_server_mid_poll_is_logged_once_and_retried(exec_shim):
    h = await connected(exec_shim, exec_poll_interval_ms=50)
    h.venue.history_deals_get.side_effect = [
        errors.ServerBusy("history_deals_get: busy"),
        errors.ServerUnreachable("history_deals_get: refused"),
        errors.ResponseLost("history_deals_get: read timed out"),
    ] + [()] * 100
    task = asyncio.get_running_loop().create_task(h.client._exec_poll_loop())
    await asyncio.sleep(0.4)
    await stop(task)
    assert len(h.logged(LogLevel.WARNING)) == 1
    assert h.venue.history_deals_get.call_count > 3
    h.conn.reconnect_async.assert_not_called()


async def test_a_lost_terminal_session_is_reconnected_and_polling_resumes(exec_shim):
    h = await connected(exec_shim, exec_poll_interval_ms=50)
    h.conn.ensure_connected.side_effect = [errors.MT5ConnectionError("not connected")] + [
        None
    ] * 100
    task = asyncio.get_running_loop().create_task(h.client._exec_poll_loop())
    await asyncio.sleep(0.3)
    await stop(task)
    h.conn.reconnect_async.assert_awaited_once()
    assert h.venue.history_deals_get.call_count > 1


async def test_a_reconnect_that_gives_up_ends_the_poll_loop(exec_shim):
    h = await connected(exec_shim, exec_poll_interval_ms=50)
    h.conn.ensure_connected.side_effect = errors.MT5ConnectionError("not connected")
    h.conn.reconnect_async.return_value = False
    await asyncio.wait_for(h.client._exec_poll_loop(), timeout=1.0)
    h.conn.reconnect_async.assert_awaited_once()
    assert len(h.logged(LogLevel.ERROR)) == 1
