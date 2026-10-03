"""The execution client's reports for NT's reconciliation: each one magic-filtered, its quantities
and prices in the instrument's precisions, its times the venue's own, read over a bounded window."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pandas as pd
import pytest
from exec_harness import ACCOUNT_ID, EURUSD, MAGIC, build, ours_deal, ours_order, ours_position
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.messages import (
    GenerateFillReports,
    GenerateOrderStatusReport,
    GenerateOrderStatusReports,
    GeneratePositionStatusReports,
)
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.enums import (
    LiquiditySide,
    OrderSide,
    OrderStatus,
    OrderType,
    PositionSide,
    TimeInForce,
    TriggerType,
)
from nautilus_trader.model.identifiers import PositionId, TradeId, VenueOrderId
from nautilus_trader.model.objects import Money, Price, Quantity
from venue_doubles import trade_deal, trade_order, trade_position

from mt5connect import errors, execution, mirror


@pytest.fixture
async def h(exec_shim):
    harness = build(exec_shim)
    await harness.connect()
    return harness


async def one_report(h, client_order_id=None, venue_order_id=None):
    return await h.client.generate_order_status_report(
        GenerateOrderStatusReport(
            instrument_id=EURUSD,
            client_order_id=client_order_id,
            venue_order_id=venue_order_id,
            command_id=UUID4(),
            ts_init=0,
        )
    )


async def order_reports(h, instrument_id=None, start=None, end=None, open_only=False):
    return await h.client.generate_order_status_reports(
        GenerateOrderStatusReports(
            instrument_id=instrument_id,
            start=start,
            end=end,
            open_only=open_only,
            command_id=UUID4(),
            ts_init=0,
        )
    )


async def fill_reports(h, venue_order_id=None, start=None, end=None):
    return await h.client.generate_fill_reports(
        GenerateFillReports(
            instrument_id=None,
            venue_order_id=venue_order_id,
            start=start,
            end=end,
            command_id=UUID4(),
            ts_init=0,
        )
    )


async def position_reports(h):
    return await h.client.generate_position_status_reports(
        GeneratePositionStatusReports(
            instrument_id=None, start=None, end=None, command_id=UUID4(), ts_init=0
        )
    )


def by_ticket(*venue_orders):
    def orders_get(symbol=None, group=None, ticket=None):
        if ticket is not None:
            return tuple(order for order in venue_orders if order.ticket == ticket)
        else:
            return venue_orders

    return orders_get


# ── One order ────────────────────────────────────────────────────────────────


async def test_a_resting_order_reports_accepted_in_the_instruments_precisions(exec_shim):
    h = build(exec_shim)
    order = h.place(h.limit(), ticket=9001)
    await h.connect()
    h.venue.orders_get.side_effect = by_ticket(
        ours_order(ticket=9001, price_open=1.08, time_setup_msc=1_760_000_000_000)
    )
    report = await one_report(h, order.client_order_id, VenueOrderId("9001"))
    assert (
        report.account_id,
        report.instrument_id,
        report.venue_order_id,
        report.client_order_id,
        report.order_side,
        report.order_type,
        report.time_in_force,
        report.order_status,
        report.quantity,
        report.filled_qty,
        report.price,
        report.ts_accepted,
        report.ts_last,
        report.reduce_only,
    ) == (
        ACCOUNT_ID,
        EURUSD,
        VenueOrderId("9001"),
        order.client_order_id,
        OrderSide.BUY,
        OrderType.LIMIT,
        TimeInForce.GTC,
        OrderStatus.ACCEPTED,
        Quantity.from_str("0.01"),
        Quantity.from_str("0.00"),
        Price.from_str("1.08000"),
        1_760_000_000_000_000_000,
        1_760_000_000_000_000_000,
        False,
    )


async def test_a_filled_historical_order_reports_filled_with_its_volume(h):
    h.venue.history_orders_get.return_value = (
        ours_order(
            ticket=9002,
            type=mirror.ORDER_TYPE_BUY,
            state=mirror.ORDER_STATE_FILLED,
            volume_initial=0.3,
            volume_current=0.0,
            price_open=1.0852,
            time_setup_msc=1_760_000_000_000,
            time_done_msc=1_760_000_000_250,
        ),
    )
    report = await one_report(h, venue_order_id=VenueOrderId("9002"))
    assert (
        report.order_status,
        report.order_type,
        report.quantity,
        report.filled_qty,
        report.price,
        report.ts_accepted,
        report.ts_last,
    ) == (
        OrderStatus.FILLED,
        OrderType.MARKET,
        Quantity.from_str("0.30"),
        Quantity.from_str("0.30"),
        None,
        1_760_000_000_000_000_000,
        1_760_000_000_250_000_000,
    )
    h.venue.orders_get.assert_called_once_with(ticket=9002)
    h.venue.history_orders_get.assert_called_once_with(ticket=9002)


async def test_a_ticket_the_venue_does_not_know_reports_none(h):
    assert await one_report(h, venue_order_id=VenueOrderId("9003")) is None


@pytest.mark.parametrize("resting", [True, False], ids=["resting", "historical"])
async def test_a_ticket_of_another_magic_is_refused_not_reported(h, resting):
    theirs = trade_order(ticket=9010, magic=MAGIC + 1)
    if resting:
        h.venue.orders_get.side_effect = by_ticket(theirs)
    else:
        h.venue.history_orders_get.return_value = (theirs,)
    with pytest.raises(errors.MT5OrderError, match="9010"):
        await one_report(h, venue_order_id=VenueOrderId("9010"))


async def test_a_failure_to_read_the_venue_propagates(h):
    h.venue.orders_get.side_effect = errors.ServerUnreachable("orders_get: refused")
    with pytest.raises(errors.ServerUnreachable):
        await one_report(h, venue_order_id=VenueOrderId("9003"))


async def test_an_order_without_a_ticket_is_found_resting_by_its_digest(exec_shim):
    h = build(exec_shim)
    order = h.place(h.limit(), status=OrderStatus.SUBMITTED)
    await h.connect()
    h.venue.orders_get.side_effect = by_ticket(
        ours_order(ticket=9004, comment="manual"),
        ours_order(ticket=9005, comment=execution.order_comment(order.client_order_id)),
    )
    report = await one_report(h, client_order_id=order.client_order_id)
    assert (report.venue_order_id, report.client_order_id) == (
        VenueOrderId("9005"),
        order.client_order_id,
    )


async def test_an_order_without_a_ticket_is_found_in_the_lookbacks_history_by_its_digest(
    exec_shim,
):
    h = build(exec_shim, history_lookback_mins=30)
    order = h.place(h.market(), status=OrderStatus.SUBMITTED)
    await h.connect()
    h.venue.history_orders_get.return_value = (
        ours_order(
            ticket=9006,
            type=mirror.ORDER_TYPE_BUY,
            state=mirror.ORDER_STATE_FILLED,
            volume_current=0.0,
            comment=execution.order_comment(order.client_order_id),
        ),
    )
    report = await one_report(h, client_order_id=order.client_order_id)
    assert (report.venue_order_id, report.order_status) == (
        VenueOrderId("9006"),
        OrderStatus.FILLED,
    )
    date_from, date_to = h.venue.history_orders_get.call_args.args
    assert date_to - date_from == timedelta(minutes=30)


async def test_an_order_without_a_ticket_found_nowhere_reports_none(exec_shim):
    h = build(exec_shim)
    order = h.place(h.limit(), status=OrderStatus.SUBMITTED)
    await h.connect()
    assert await one_report(h, client_order_id=order.client_order_id) is None


async def test_a_gtd_stop_limit_reports_its_trigger_its_limit_and_its_expiry(h):
    h.venue.orders_get.side_effect = by_ticket(
        ours_order(
            ticket=9007,
            type=mirror.ORDER_TYPE_SELL_STOP_LIMIT,
            type_time=mirror.ORDER_TIME_SPECIFIED,
            time_expiration=1_791_072_000,
            price_open=1.075,
            price_stoplimit=1.07,
        )
    )
    report = await one_report(h, venue_order_id=VenueOrderId("9007"))
    assert (
        report.order_side,
        report.order_type,
        report.trigger_price,
        report.trigger_type,
        report.price,
        report.time_in_force,
        report.expire_time,
    ) == (
        OrderSide.SELL,
        OrderType.STOP_LIMIT,
        Price.from_str("1.07500"),
        TriggerType.DEFAULT,
        Price.from_str("1.07000"),
        TimeInForce.GTD,
        datetime(2026, 10, 4, tzinfo=UTC),
    )


@pytest.mark.parametrize(
    ("state", "status"),
    [
        (mirror.ORDER_STATE_STARTED, OrderStatus.ACCEPTED),
        (mirror.ORDER_STATE_PLACED, OrderStatus.ACCEPTED),
        (mirror.ORDER_STATE_PARTIAL, OrderStatus.PARTIALLY_FILLED),
        (mirror.ORDER_STATE_FILLED, OrderStatus.FILLED),
        (mirror.ORDER_STATE_CANCELED, OrderStatus.CANCELED),
        (mirror.ORDER_STATE_EXPIRED, OrderStatus.EXPIRED),
        (mirror.ORDER_STATE_REJECTED, OrderStatus.REJECTED),
        (mirror.ORDER_STATE_REQUEST_ADD, OrderStatus.ACCEPTED),
        (mirror.ORDER_STATE_REQUEST_MODIFY, OrderStatus.ACCEPTED),
        (mirror.ORDER_STATE_REQUEST_CANCEL, OrderStatus.ACCEPTED),
    ],
)
async def test_the_venues_order_state_reports_as_nts_status(h, state, status):
    h.venue.history_orders_get.return_value = (
        ours_order(ticket=9008, state=state, volume_initial=0.02, volume_current=0.01),
    )
    report = await one_report(h, venue_order_id=VenueOrderId("9008"))
    assert report.order_status == status


async def test_a_report_on_a_symbol_not_loaded_raises_naming_it(h):
    h.venue.history_orders_get.return_value = (ours_order(ticket=9009, symbol="GBPUSD"),)
    with pytest.raises(errors.MT5InstrumentError, match="GBPUSD"):
        await one_report(h, venue_order_id=VenueOrderId("9009"))


# ── Many orders ──────────────────────────────────────────────────────────────


async def test_order_reports_unite_resting_and_historical_orders_of_this_magic_by_ticket(h):
    h.venue.orders_get.return_value = (
        ours_order(ticket=9101),
        trade_order(ticket=9102, magic=MAGIC + 1),
    )
    h.venue.history_orders_get.return_value = (
        ours_order(ticket=9101),
        ours_order(ticket=9103, state=mirror.ORDER_STATE_CANCELED),
        trade_order(ticket=9104, magic=MAGIC + 1),
    )
    start = pd.Timestamp("2026-10-03T10:00:00Z")
    end = pd.Timestamp("2026-10-03T11:00:00Z")
    reports = await order_reports(h, start=start, end=end)
    assert sorted(report.venue_order_id.value for report in reports) == ["9101", "9103"]
    h.venue.history_orders_get.assert_called_once_with(start, end)


async def test_order_reports_default_to_the_configs_lookback(h):
    await order_reports(h)
    date_from, date_to = h.venue.history_orders_get.call_args.args
    assert date_to - date_from == timedelta(minutes=60)


async def test_open_only_order_reports_read_no_history(h):
    h.venue.orders_get.return_value = (ours_order(ticket=9101),)
    reports = await order_reports(h, instrument_id=EURUSD, open_only=True)
    assert [report.venue_order_id for report in reports] == [VenueOrderId("9101")]
    h.venue.orders_get.assert_called_once_with(symbol="EURUSD")
    h.venue.history_orders_get.assert_not_called()


# ── Fills ────────────────────────────────────────────────────────────────────


async def test_fill_reports_read_a_bounded_window_and_carry_the_venue_position(exec_shim):
    h = build(exec_shim)
    order = h.place(h.limit(), ticket=9001)
    await h.connect()
    h.venue.history_deals_get.return_value = (
        ours_deal(
            ticket=7001,
            order=9001,
            position_id=8133477,
            time_msc=1_760_000_000_123,
            commission=-0.5,
        ),
        trade_deal(ticket=7002, order=9002, magic=MAGIC + 1),
        ours_deal(ticket=7003, order=0, type=mirror.DEAL_TYPE_BALANCE, volume=0.0),
        ours_deal(ticket=7004, order=9001, volume=0.0),
    )
    reports = await fill_reports(h)
    date_from, date_to = h.venue.history_deals_get.call_args.args
    assert date_to - date_from == timedelta(minutes=60)
    (report,) = reports
    assert (
        report.account_id,
        report.instrument_id,
        report.client_order_id,
        report.venue_order_id,
        report.venue_position_id,
        report.trade_id,
        report.order_side,
        report.last_qty,
        report.last_px,
        report.commission,
        report.liquidity_side,
        report.ts_event,
    ) == (
        ACCOUNT_ID,
        EURUSD,
        order.client_order_id,
        VenueOrderId("9001"),
        PositionId("8133477"),
        TradeId("7001"),
        OrderSide.BUY,
        Quantity.from_str("0.01"),
        Price.from_str("1.08000"),
        Money(Decimal("0.50"), USD),
        LiquiditySide.MAKER,
        1_760_000_000_123_000_000,
    )


async def test_fill_reports_pass_the_commands_window_and_filter_by_its_order(h):
    h.venue.history_deals_get.return_value = (
        ours_deal(ticket=7001, order=9001),
        ours_deal(ticket=7002, order=9002),
    )
    start = pd.Timestamp("2026-10-03T10:00:00Z")
    end = pd.Timestamp("2026-10-03T11:00:00Z")
    reports = await fill_reports(h, venue_order_id=VenueOrderId("9002"), start=start, end=end)
    assert [report.trade_id for report in reports] == [TradeId("7002")]
    h.venue.history_deals_get.assert_called_once_with(start, end)


# ── Positions ────────────────────────────────────────────────────────────────


async def test_a_position_of_a_kind_the_package_lacks_is_refused_naming_it(h):
    h.venue.positions_get.return_value = (ours_position(ticket=8133480, type=9876),)
    with pytest.raises(errors.MT5OrderError, match="9876"):
        await position_reports(h)


async def test_each_position_of_this_magic_reports_under_its_identifier(h):
    h.venue.positions_get.return_value = (
        ours_position(
            ticket=8133477,
            identifier=8133477,
            type=mirror.POSITION_TYPE_BUY,
            volume=0.01,
            time_update_msc=1_760_000_003_000,
        ),
        ours_position(
            ticket=8133478,
            identifier=8133478,
            type=mirror.POSITION_TYPE_SELL,
            volume=0.02,
            time_update_msc=1_760_000_004_000,
        ),
        trade_position(ticket=8133479, identifier=8133479, magic=MAGIC + 1),
    )
    reports = await position_reports(h)
    assert [
        (
            report.account_id,
            report.instrument_id,
            report.venue_position_id,
            report.position_side,
            report.quantity,
            report.ts_last,
        )
        for report in reports
    ] == [
        (
            ACCOUNT_ID,
            EURUSD,
            PositionId("8133477"),
            PositionSide.LONG,
            Quantity.from_str("0.01"),
            1_760_000_003_000_000_000,
        ),
        (
            ACCOUNT_ID,
            EURUSD,
            PositionId("8133478"),
            PositionSide.SHORT,
            Quantity.from_str("0.02"),
            1_760_000_004_000_000_000,
        ),
    ]
