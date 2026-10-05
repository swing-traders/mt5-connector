"""The execution client's view of the venue: its ticket index, the transactions the venue pushes,
and the account it reports."""

import asyncio
from datetime import timedelta
from decimal import Decimal

import pytest
from exec_harness import ACCOUNT_ID, MAGIC, STRATEGY_ID, TRADER_ID, build, ours_deal, ours_order
from nautilus_trader.accounting.accounts.margin import MarginAccount
from nautilus_trader.common.enums import LogLevel
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.messages import CancelOrder, GenerateFillReports, SubmitOrder
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.enums import LiquiditySide, OrderSide, OrderStatus, OrderType
from nautilus_trader.model.identifiers import PositionId, TradeId, VenueOrderId
from nautilus_trader.model.objects import Money, Price, Quantity
from nautilus_trader.test_kit.stubs.events import TestEventStubs
from push_double import order_left, request_done, transaction_frame
from venue_doubles import account_info, send_result, trade_deal

from mt5connector.client import errors, execution
from mt5connector.client.connection import AccountSnapshot
from mt5connector.wire import mirror
from mt5connector.wire.push_wire import Stream, Subscription, TransactionType


async def connected(exec_shim, **settings):
    h = build(exec_shim, **settings)
    await h.connect()
    return h


def fills(h) -> list:
    return [event for event in h.events if type(event).__name__ == "OrderFilled"]


def resting_by_ticket(*venue_orders):
    """An orders_get answering each resting order under its ticket, and nothing else."""

    def orders_get(symbol=None, group=None, ticket=None):
        return tuple(order for order in venue_orders if order.ticket == ticket)

    return orders_get


async def fill_reports(h):
    return await h.client.generate_fill_reports(
        GenerateFillReports(
            instrument_id=None,
            venue_order_id=None,
            start=None,
            end=None,
            command_id=UUID4(),
            ts_init=0,
        )
    )


# ── The channel ──────────────────────────────────────────────────────────────


async def test_connect_subscribes_the_accounts_trade_transactions_on_the_push_channel(exec_shim):
    h = await connected(exec_shim)

    assert h.push.connected
    assert h.push.wanted == [Subscription(Stream.TRADE_TRANSACTIONS)]
    await h.client._disconnect()
    assert not h.push.connected


async def test_connect_logs_the_push_channel_without_its_credentials(exec_shim):
    h = await connected(exec_shim, ws_url="wss://hub:SYNTHETIC_SECRET@127.0.0.1:9000/push")
    await h.client._disconnect()

    assert not any("SYNTHETIC_SECRET" in str(call) for call in h.client.recorded_log.mock_calls)
    assert "connected, the push channel at wss://127.0.0.1:9000/push" in h.logged(LogLevel.INFO)


# ── The ticket index ─────────────────────────────────────────────────────────


async def test_fills_book_through_the_index_rebuilt_from_nts_orders(exec_shim):
    h = build(exec_shim)
    order = h.place(h.limit(), ticket=9001)
    await h.connect()
    h.deals_added(ours_deal(ticket=7001, order=9001))
    assert [fill.client_order_id for fill in fills(h)] == [order.client_order_id]
    h.venue.history_orders_get.assert_not_called()


async def test_a_venue_order_id_that_is_not_a_ticket_is_left_out_of_the_index(exec_shim):
    h = build(exec_shim)
    h.place(h.limit(), ticket="9001-SL-1")
    await h.connect()
    debug = h.logged(LogLevel.DEBUG)
    assert len([line for line in debug if "9001-SL-1" in line]) == 1


# ── Pushed deals ─────────────────────────────────────────────────────────────


async def test_a_deal_added_fills_at_once_under_the_venues_position_and_the_deals_time(exec_shim):
    h = build(exec_shim)
    order = h.place(h.limit(), ticket=9001)
    await h.connect()

    h.deals_added(
        ours_deal(ticket=7001, order=9001, position_id=8133477, time_msc=1_760_000_000_123)
    )

    (fill,) = fills(h)
    assert (fill.client_order_id, fill.position_id, fill.trade_id, fill.ts_event) == (
        order.client_order_id,
        PositionId("8133477"),
        TradeId("7001"),
        1_760_000_000_123_000_000,
    )
    h.venue.history_deals_get.assert_called_once_with(ticket=7001)


async def test_deals_already_in_the_history_at_connect_are_never_emitted(exec_shim):
    h = build(exec_shim)
    h.place(h.limit(), ticket=9001)
    windows = []

    def history_deals_get(date_from=None, date_to=None, group=None, ticket=None, position=None):
        windows.append((date_from, date_to))
        return (ours_deal(ticket=7001, order=9001),)

    exec_shim.history_deals_get.side_effect = history_deals_get
    await h.connect()
    h.deals_added(ours_deal(ticket=7001, order=9001))
    assert fills(h) == []
    assert windows[0][1] - windows[0][0] == timedelta(minutes=60)


async def test_two_deals_on_two_positions_fill_with_their_own_venue_position_ids(exec_shim):
    h = build(exec_shim)
    buy = h.place(h.limit(OrderSide.BUY), ticket=9001)
    sell = h.place(h.market(OrderSide.SELL, "0.02"), ticket=9002)
    await h.connect()
    h.deals_added(
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
    )
    h.client._account_turn()
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
    h.deals_added(ours_deal(ticket=7001, order=9001, commission=commission, fee=fee))
    assert fills(h)[0].commission == Money(Decimal(charged), USD)


async def test_two_identical_deal_adds_fill_once(exec_shim):
    h = build(exec_shim)
    h.place(h.limit(), ticket=9001)
    await h.connect()
    deal = ours_deal(ticket=7001, order=9001)
    h.deals_added(deal, deal)
    assert [fill.trade_id for fill in fills(h)] == [TradeId("7001")]


async def test_a_deal_the_history_does_not_hold_yet_is_left_to_reconciliation(exec_shim):
    h = build(exec_shim)
    h.place(h.limit(), ticket=9001)
    await h.connect()

    h.push.deliver(transaction_frame(TransactionType.DEAL_ADD, deal=7001, order=9001))

    assert h.names() == []
    assert [line for line in h.logged(LogLevel.INFO) if "7001" in line] == [
        "deal 7001: not in the venue's history, left to reconciliation"
    ]


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
    h.deals_added(ours_deal(ticket=7101, order=9100), ours_deal(ticket=7102, order=9100))
    assert [fill.client_order_id for fill in fills(h)] == [order.client_order_id] * 2
    assert [fill.order_type for fill in fills(h)] == [OrderType.MARKET] * 2
    h.venue.history_orders_get.assert_called_once_with(ticket=9100)


async def test_a_partly_filled_resting_orders_comment_is_read_off_the_open_order(exec_shim):
    h = build(exec_shim)
    order = h.place(h.limit(quantity="0.02"), status=OrderStatus.SUBMITTED)
    await h.connect()
    h.venue.orders_get.side_effect = resting_by_ticket(
        ours_order(ticket=9101, comment=execution.order_comment(order.client_order_id))
    )
    h.deals_added(ours_deal(ticket=7103, order=9101))
    assert [fill.client_order_id for fill in fills(h)] == [order.client_order_id]


async def test_a_deal_matching_no_order_emits_nothing_and_logs_one_info_naming_its_ticket(
    exec_shim,
):
    h = build(exec_shim)
    await h.connect()
    h.venue.history_orders_get.return_value = (ours_order(ticket=9200, comment="manual"),)
    deal = ours_deal(ticket=7201, order=9200)
    h.deals_added(deal, deal)
    assert h.names() == []
    assert len([line for line in h.logged(LogLevel.INFO) if "9200" in line]) == 1


async def test_another_magics_deal_and_a_balance_deal_are_skipped_silently(exec_shim):
    h = build(exec_shim)
    await h.connect()
    h.deals_added(
        trade_deal(ticket=7301, order=9300, magic=MAGIC + 1),
        ours_deal(ticket=7302, order=0, type=mirror.DEAL_TYPE_BALANCE, volume=0.0),
    )
    assert h.names() == []
    h.venue.history_orders_get.assert_not_called()
    assert [line for line in h.logged(LogLevel.INFO) if "730" in line or "9300" in line] == []


async def test_a_deal_whose_fill_cannot_be_built_stays_unseen_so_a_later_delivery_fills_it(
    exec_shim,
):
    h = build(exec_shim)
    h.place(h.limit(), ticket=9001)
    await h.connect()
    loaded = h.instruments.pop("EURUSD")
    deal = ours_deal(ticket=7001, order=9001)
    h.deals_added(deal)
    assert h.names() == []
    (failed,) = h.client.recorded_log.exception.call_args_list
    assert isinstance(failed.args[1], errors.MT5InstrumentError)
    h.instruments["EURUSD"] = loaded
    h.deals_added(deal)
    assert [fill.trade_id for fill in fills(h)] == [TradeId("7001")]


# ── Pushed requests ──────────────────────────────────────────────────────────


async def test_a_deal_of_an_unknown_ticket_books_to_its_order_once_the_request_links_the_ticket(
    exec_shim,
):
    h = build(exec_shim)
    order = h.place(h.market(), status=OrderStatus.SUBMITTED)
    await h.connect()
    deal = ours_deal(ticket=7101, order=9100, position_id=9100, type=mirror.DEAL_TYPE_BUY)

    h.deals_added(deal)
    unlinked = h.names()
    h.push.deliver(request_done(9100, execution.order_comment(order.client_order_id), MAGIC))
    h.venue.history_deals_get.return_value = (deal,)
    (report,) = await fill_reports(h)

    assert unlinked == []
    assert h.names() == []
    assert len([line for line in h.logged(LogLevel.INFO) if "9100" in line]) == 1
    assert (report.client_order_id, report.venue_order_id, report.venue_position_id) == (
        order.client_order_id,
        VenueOrderId("9100"),
        PositionId("9100"),
    )


async def test_a_deal_before_the_accept_fills_and_the_request_then_accepts(exec_shim):
    h = build(exec_shim)
    order = h.place(h.limit(), status=OrderStatus.SUBMITTED)
    await h.connect()
    comment = execution.order_comment(order.client_order_id)
    h.venue.orders_get.side_effect = resting_by_ticket(
        ours_order(ticket=9500, comment=comment, time_setup_msc=1_760_000_002_000)
    )

    h.deals_added(ours_deal(ticket=7501, order=9500, volume=0.005))
    h.push.deliver(request_done(9500, comment, MAGIC, action=mirror.TRADE_ACTION_PENDING))

    assert h.names() == ["OrderFilled", "OrderAccepted"]
    accepted = h.events[1]
    assert (accepted.client_order_id, accepted.venue_order_id, accepted.ts_event) == (
        order.client_order_id,
        VenueOrderId("9500"),
        1_760_000_002_000_000_000,
    )


async def test_a_request_of_an_order_nt_holds_accepted_emits_nothing(exec_shim):
    h = build(exec_shim)
    order = h.place(h.limit(), ticket=9001)
    await h.connect()

    h.push.deliver(request_done(9001, execution.order_comment(order.client_order_id), MAGIC))

    assert h.names() == []
    h.venue.orders_get.assert_not_called()


@pytest.mark.parametrize(
    ("magic", "retcode"),
    [(MAGIC + 1, mirror.TRADE_RETCODE_DONE), (MAGIC, mirror.TRADE_RETCODE_REJECT)],
    ids=["another-trader", "refused"],
)
async def test_another_traders_or_a_refused_request_links_nothing(exec_shim, magic, retcode):
    h = build(exec_shim)
    order = h.place(h.market(), status=OrderStatus.SUBMITTED)
    await h.connect()
    comment = execution.order_comment(order.client_order_id)

    h.push.deliver(request_done(9100, comment, magic, retcode=retcode))
    h.venue.history_deals_get.return_value = (ours_deal(ticket=7101, order=9100),)
    (report,) = await fill_reports(h)

    assert h.names() == []
    assert report.client_order_id is None


# ── Pending orders the venue ends ────────────────────────────────────────────


async def resting_at_connect(exec_shim, ticket=9400):
    h = build(exec_shim)
    order = h.place(h.limit(), ticket=ticket)
    await h.connect()
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
    ended = ours_order(ticket=9400, state=state, time_done_msc=1_760_000_005_000)
    h.venue.history_orders_get.return_value = (ended,)

    h.push.deliver(order_left(TransactionType.ORDER_DELETE, ended))
    h.push.deliver(order_left(TransactionType.HISTORY_ADD, ended))
    h.client._account_turn()

    assert h.names() == [event, "AccountState"]
    assert h.events[0].client_order_id == order.client_order_id
    assert h.events[0].ts_event == 1_760_000_005_000_000_000
    h.venue.history_orders_get.assert_called_once_with(ticket=9400)


async def test_an_order_the_history_does_not_hold_at_its_delete_ends_on_its_history_add(exec_shim):
    h, _ = await resting_at_connect(exec_shim)
    ended = ours_order(ticket=9400, state=mirror.ORDER_STATE_CANCELED)

    h.push.deliver(order_left(TransactionType.ORDER_DELETE, ended))
    deleted = h.names()
    h.venue.history_orders_get.return_value = (ended,)
    h.push.deliver(order_left(TransactionType.HISTORY_ADD, ended))

    assert deleted == []
    assert h.names() == ["OrderCanceled"]


@pytest.mark.parametrize(
    ("state", "ends"),
    [
        (mirror.ORDER_STATE_CANCELED, ["OrderAccepted", "OrderCanceled"]),
        (mirror.ORDER_STATE_EXPIRED, ["OrderAccepted", "OrderExpired"]),
        (mirror.ORDER_STATE_REJECTED, ["OrderRejected"]),
    ],
)
async def test_an_unanswered_submits_order_the_venue_ended_ends_through_its_digest(
    exec_shim, state, ends
):
    h = build(exec_shim)
    order = h.place(h.limit(), status=OrderStatus.SUBMITTED)
    comment = execution.order_comment(order.client_order_id)
    await h.connect()
    ended = ours_order(
        ticket=9600,
        comment=comment,
        state=state,
        time_setup_msc=1_760_000_002_000,
        time_done_msc=1_760_000_005_000,
    )
    h.venue.history_orders_get.return_value = (ended,)

    h.push.deliver(order_left(TransactionType.ORDER_DELETE, ended))
    h.push.deliver(order_left(TransactionType.HISTORY_ADD, ended))

    assert h.names() == ends
    assert [event.client_order_id for event in h.events] == [order.client_order_id] * len(ends)
    assert h.events[-1].ts_event == 1_760_000_005_000_000_000
    if len(ends) == 2:
        assert (h.events[0].venue_order_id, h.events[0].ts_event) == (
            VenueOrderId("9600"),
            1_760_000_002_000_000_000,
        )


async def test_an_ended_order_matching_no_nt_order_logs_one_info_naming_its_ticket(exec_shim):
    h = await connected(exec_shim)
    ended = ours_order(ticket=9601, comment="manual", state=mirror.ORDER_STATE_CANCELED)
    h.venue.history_orders_get.return_value = (ended,)

    h.push.deliver(order_left(TransactionType.ORDER_DELETE, ended))
    h.push.deliver(order_left(TransactionType.HISTORY_ADD, ended))

    assert h.names() == []
    assert len([line for line in h.logged(LogLevel.INFO) if "9601" in line]) == 1


async def test_another_traders_ended_order_is_skipped_silently(exec_shim):
    h = await connected(exec_shim)
    ended = ours_order(ticket=9602, magic=MAGIC + 1, state=mirror.ORDER_STATE_CANCELED)
    h.venue.history_orders_get.return_value = (ended,)

    h.push.deliver(order_left(TransactionType.HISTORY_ADD, ended))

    assert h.names() == []
    assert [line for line in h.logged(LogLevel.INFO) if "9602" in line] == []


async def test_a_pending_order_that_filled_emits_nothing_for_its_end(exec_shim):
    h, _ = await resting_at_connect(exec_shim)
    filled = ours_order(ticket=9400, state=mirror.ORDER_STATE_FILLED, volume_current=0.0)
    h.venue.history_orders_get.return_value = (filled,)

    h.push.deliver(order_left(TransactionType.HISTORY_ADD, filled))

    assert h.names() == []


async def test_a_pending_order_placed_after_connect_is_ended_by_its_transactions(exec_shim):
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
    ended = ours_order(ticket=9401, state=mirror.ORDER_STATE_CANCELED)
    h.venue.history_orders_get.return_value = (ended,)

    h.push.deliver(order_left(TransactionType.HISTORY_ADD, ended))

    assert h.names() == ["OrderCanceled"]


async def test_an_order_this_client_cancelled_is_not_cancelled_again_by_its_transactions(
    exec_shim,
):
    h, order = await resting_at_connect(exec_shim)
    h.venue.orders_get.side_effect = resting_by_ticket(ours_order(ticket=9400))
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
    ended = ours_order(ticket=9400, state=mirror.ORDER_STATE_CANCELED)
    h.venue.history_orders_get.return_value = (ended,)

    h.push.deliver(order_left(TransactionType.ORDER_DELETE, ended))
    h.push.deliver(order_left(TransactionType.HISTORY_ADD, ended))

    assert h.names() == ["OrderCanceled", "AccountState"]


@pytest.mark.parametrize(
    "kind",
    [
        TransactionType.ORDER_ADD,
        TransactionType.ORDER_UPDATE,
        TransactionType.DEAL_UPDATE,
        TransactionType.DEAL_DELETE,
        TransactionType.HISTORY_UPDATE,
        TransactionType.HISTORY_DELETE,
        TransactionType.POSITION,
    ],
)
async def test_a_transaction_no_event_follows_from_reads_nothing_and_emits_nothing(exec_shim, kind):
    h, _ = await resting_at_connect(exec_shim)

    h.push.deliver(transaction_frame(kind, order=9400, deal=7400, position=9400))

    assert h.names() == []
    assert h.venue.mock_calls == []


async def test_a_transaction_of_a_type_mql5_does_not_name_is_refused(exec_shim):
    h = await connected(exec_shim)

    h.push.deliver(transaction_frame(TransactionType.REQUEST) | {"transaction": {"type": 11}})

    assert h.names() == []
    (failed,) = h.client.recorded_log.exception.call_args_list
    assert isinstance(failed.args[1], ValueError)
    assert "11" in str(failed.args[1])


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
    h.deals_added(ours_deal(ticket=7001, order=9001))
    h.client._account_turn()
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


async def test_the_account_reports_its_margin_as_one_account_level_balance(exec_shim):
    h = build(
        exec_shim, account=account_info(margin=100.0, margin_initial=25.5, margin_maintenance=80.0)
    )
    await h.connect()
    h.client._account_owed = True
    h.client._account_turn()
    (margin,) = h.ledger[-1].margins
    assert margin.instrument_id is None
    assert (margin.initial, margin.maintenance) == (
        Money(Decimal("100.00"), USD),
        Money(Decimal("80.00"), USD),
    )


async def test_a_margin_account_reads_the_reported_margin_back_per_currency(exec_shim):
    h = build(exec_shim)
    await h.connect_keeping_events()
    account = MarginAccount(h.ledger[-1])
    h.conn.get_account_info.return_value = AccountSnapshot.from_mt5(
        account_info(margin=200.0, margin_initial=40.0, margin_maintenance=150.0)
    )
    h.client._account_owed = True
    h.client._account_turn()
    account.apply(h.ledger[-1])
    assert account.account_margins_init() == {USD: Money(Decimal("200.00"), USD)}
    assert account.account_margins_maint() == {USD: Money(Decimal("150.00"), USD)}


async def test_a_turn_owing_no_report_and_before_the_refresh_period_reports_nothing(exec_shim):
    h = await connected(exec_shim)

    h.client._account_turn()

    assert h.names() == []


async def test_the_account_is_reported_on_the_configs_refresh_period(exec_shim):
    h = await connected(exec_shim, account_refresh_seconds=1, exec_poll_interval_ms=50)
    task = asyncio.get_running_loop().create_task(h.client._account_loop())
    await asyncio.sleep(0.6)
    assert h.names() == []
    await asyncio.sleep(0.7)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    assert h.names() == ["AccountState"]


@pytest.mark.parametrize(
    "failure",
    [
        errors.ServerBusy("account_info: server busy", 5),
        errors.ServerUnreachable("account_info: unavailable"),
        errors.ResponseLost("account_info: unavailable"),
    ],
)
async def test_an_account_report_the_server_does_not_answer_stays_owed_and_warns_once(
    exec_shim, failure
):
    h = build(exec_shim)
    h.place(h.limit(), ticket=9001)
    await h.connect()
    h.deals_added(ours_deal(ticket=7001, order=9001))
    h.conn.get_account_info.side_effect = failure
    for _ in range(3):
        h.client._account_turn()
    assert h.names() == ["OrderFilled"]
    assert h.conn.get_account_info.call_count == 3
    assert len(h.logged(LogLevel.WARNING)) == 1

    h.conn.get_account_info.side_effect = None
    h.client._account_turn()
    h.client._account_turn()
    assert h.names() == ["OrderFilled", "AccountState"]
    assert len(h.logged(LogLevel.WARNING)) == 1
    assert [line for line in h.logged(LogLevel.INFO) if "answers again" in line] == [
        "account report: the server answers again"
    ]
    h.conn.reconnect_async.assert_not_called()


async def test_a_lost_terminal_session_is_reconnected_and_the_account_loop_resumes(exec_shim):
    h = await connected(exec_shim, account_refresh_seconds=1, exec_poll_interval_ms=50)
    h.conn.ensure_connected.side_effect = [errors.MT5ConnectionError("not connected")] + [
        None
    ] * 100
    h.client._account_owed = True
    task = asyncio.get_running_loop().create_task(h.client._account_loop())
    await asyncio.sleep(0.3)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    h.conn.reconnect_async.assert_awaited_once()
    assert h.names() == ["AccountState"]


async def test_a_reconnect_that_gives_up_ends_the_account_loop(exec_shim):
    h = await connected(exec_shim, exec_poll_interval_ms=50)
    h.conn.ensure_connected.side_effect = errors.MT5ConnectionError("not connected")
    h.conn.reconnect_async.return_value = False
    await asyncio.wait_for(h.client._account_loop(), timeout=1.0)
    h.conn.reconnect_async.assert_awaited_once()
    assert len(h.logged(LogLevel.ERROR)) == 1
