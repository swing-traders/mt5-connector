"""The figures of every deal the execution client reads, published on the node's message bus beside
the fill or the fill report it builds from the deal, so a consumer reads what the deal booked
without reading the venue again."""

import asyncio
from decimal import Decimal

from exec_harness import MAGIC, _history_holding, build, ours_deal
from hub_double import eventually
from nautilus_trader.common.enums import LogLevel
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.messages import GenerateFillReports
from nautilus_trader.model.identifiers import Venue
from session_double import deal_added
from venue_doubles import trade_deal

from mt5connector.client import execution
from mt5connector.client.events import (
    DealFigures,
    DealsRecovery,
    deal_figures,
    deals_topic,
    recovery_topic,
)
from mt5connector.wire import calls, mirror


def _forwarded(h) -> list[DealFigures]:
    """What the client publishes on its venue's deals topic, recorded in its emission ledger."""
    h.client._msgbus.subscribe(topic=deals_topic(h.client.venue), handler=h.ledger.append)
    return h.ledger


def _figures(ledger) -> list[DealFigures]:
    return [item for item in ledger if isinstance(item, DealFigures)]


async def test_a_deal_added_publishes_its_figures_right_after_its_fill(exec_venue):
    h = build(exec_venue)
    h.place(h.limit(), ticket=9001)
    await h.connect()
    ledger = _forwarded(h)

    await h.deals_added(
        ours_deal(
            ticket=7001,
            order=9001,
            position_id=777,
            entry=mirror.DEAL_ENTRY_OUT,
            reason=mirror.DEAL_REASON_EXPERT,
            profit=120.0,
            commission=-3.5,
            fee=-0.25,
            swap=-1.2,
            time_msc=1_760_000_000_123,
        )
    )

    assert [type(item).__name__ for item in ledger][-2:] == ["OrderFilled", "DealFigures"]
    (figures,) = _figures(ledger)
    assert (
        figures.ticket,
        figures.order,
        figures.position_id,
        figures.entry,
        figures.reason,
        figures.volume,
        figures.profit,
        figures.commission,
        figures.fee,
        figures.swap,
        figures.time_msc,
    ) == (
        7001,
        9001,
        777,
        mirror.DEAL_ENTRY_OUT,
        mirror.DEAL_REASON_EXPERT,
        Decimal("0.01"),
        Decimal("120.0"),
        Decimal("-3.5"),
        Decimal("-0.25"),
        Decimal("-1.2"),
        1_760_000_000_123,
    )
    # The deal is read once, by its ticket, for the fill and the figures alike.
    h.venue.history_deals_get.assert_called_once_with(ticket=7001)


async def test_a_deal_matching_no_order_publishes_no_figures(exec_venue):
    h = build(exec_venue)
    await h.connect()
    ledger = _forwarded(h)

    await h.deals_added(ours_deal(ticket=7001, order=9999))

    assert _figures(ledger) == []


async def test_each_reported_fill_publishes_its_deals_figures(exec_venue):
    h = build(exec_venue)
    h.place(h.limit(), ticket=9001)
    await h.connect()
    ledger = _forwarded(h)
    h.venue.history_deals_get.return_value = (
        ours_deal(ticket=7001, order=9001, commission=-0.5),
        trade_deal(ticket=7002, order=9002, magic=MAGIC + 1),
        ours_deal(ticket=7003, order=9001, commission=-0.4),
    )

    reports = await h.client.generate_fill_reports(
        GenerateFillReports(
            instrument_id=None,
            venue_order_id=None,
            start=None,
            end=None,
            command_id=UUID4(),
            ts_init=0,
        )
    )

    assert [report.trade_id.value for report in reports] == ["7001", "7003"]
    assert [(f.ticket, f.commission) for f in _figures(ledger)] == [
        (7001, Decimal("-0.5")),
        (7003, Decimal("-0.4")),
    ]


def test_a_deals_raw_row_renders_every_wire_field_as_its_exact_text():
    deal = calls.STRUCT_TYPES[mirror.StructName.TRADE_DEAL](
        **ours_deal(ticket=7331117, profit=-0.15, commission=-0.04, volume=0.01)._asdict()
    )

    raw = deal_figures(deal).raw

    assert set(raw) == set(deal._fields)
    assert (raw["ticket"], raw["profit"], raw["commission"], raw["volume"], raw["symbol"]) == (
        "7331117",
        "-0.15",
        "-0.04",
        "0.01",
        "EURUSD",
    )


def test_the_deals_topic_names_the_account_venue():
    assert deals_topic(Venue("MT5_ICM")) == "mt5.deals.MT5_ICM"


def _held_history(h) -> tuple[list, asyncio.Future]:
    """The client's session with every deals recovery held unanswered on the history EA until the
    returned future is set, recording each call the client asks from here on."""
    asked: list = []
    released = asyncio.get_running_loop().create_future()
    answer = h.push.request

    async def request(call, **arguments):
        asked.append((call, arguments))
        if call is calls.Call.HISTORY_DEALS_RECOVER:
            await released
        return await answer(call, **arguments)

    h.push.request = request
    return asked, released


def _recover(h, position: int) -> None:
    h.client._msgbus.publish(
        topic=recovery_topic(h.client.venue), msg=DealsRecovery(position, "EURUSD")
    )


async def test_two_held_recoveries_delay_no_live_fill_and_a_third_waits_for_its_re_ask(exec_venue):
    h = build(exec_venue)
    order = h.place(h.limit(), ticket=9001)
    await h.connect()
    ledger = _forwarded(h)
    asked, released = _held_history(h)
    for position in (771, 772, 773):
        _recover(h, position)
    live = ours_deal(ticket=7001, order=9001)
    h.venue.history_deals_get.side_effect = _history_holding(live, h.venue)

    h.push.deliver(deal_added(live))
    await eventually(lambda: [f.ticket for f in _figures(ledger)] == [7001])

    # The live fill is emitted and forwarded while both recoveries read are still held; the third,
    # asked while two are in flight, is not read, and the recoveries asked nothing of the spawner.
    assert not released.done()
    assert [fill.client_order_id for fill in h.events if type(fill).__name__ == "OrderFilled"] == [
        order.client_order_id
    ]
    assert asked == [
        (calls.Call.HISTORY_DEALS_RECOVER, {"position": 771, "symbol": "EURUSD"}),
        (calls.Call.HISTORY_DEALS_RECOVER, {"position": 772, "symbol": "EURUSD"}),
        (calls.Call.HISTORY_DEALS_GET, {"ticket": 7001}),
    ]
    assert calls.CALLS[calls.Call.HISTORY_DEALS_RECOVER].role.name == "HISTORY"

    h.venue.history_deals_recover.side_effect = lambda position, symbol: (
        ours_deal(ticket=position + 100, order=position, position_id=position),
    )
    released.set_result(None)
    await h.drained()
    assert sorted(f.ticket for f in _figures(ledger)) == [871, 872, 7001]

    _recover(h, 773)
    await h.drained()
    assert sorted(f.ticket for f in _figures(ledger)) == [871, 872, 873, 7001]


async def test_a_recovery_past_the_read_deadline_publishes_nothing_and_is_logged_once(
    exec_venue, monkeypatch
):
    monkeypatch.setattr(execution, "_RECOVERY_DEADLINE_S", 0.05)
    h = build(exec_venue)
    await h.connect()
    ledger = _forwarded(h)
    _held_history(h)

    _recover(h, 771)
    await h.drained()

    assert _figures(ledger) == []
    assert [line for line in h.logged(LogLevel.WARNING) if "771" in line] == [
        "position 771: its deals were not recovered: TimeoutError()"
    ]


async def test_a_malformed_recovery_publishes_nothing_of_it(exec_venue):
    h = build(exec_venue)
    await h.connect()
    ledger = _forwarded(h)
    h.venue.history_deals_recover.return_value = (
        ours_deal(ticket=601, order=501, commission=-1.4),
        ours_deal(ticket=602, order=501, profit=float("nan")),
    )

    _recover(h, 777)
    await h.drained()

    assert _figures(ledger) == []
    assert [line for line in h.logged(LogLevel.WARNING) if "777" in line] == [
        "position 777: its deals were not recovered: "
        "MT5InstrumentError('profit nan is not finite')"
    ]


async def test_a_disconnected_client_takes_no_recovery(exec_venue):
    h = build(exec_venue)
    await h.connect()
    await h.client._disconnect()

    assert not h.client._msgbus.is_subscribed(
        topic=recovery_topic(h.client.venue), handler=h.client._on_recovery
    )
