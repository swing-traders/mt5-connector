"""The execution client's recovery reads against the hub's pending bound: however many recoveries
are asked, at most two are read at once, so an order and a fill's deal read find room on the
connection beside them. The client runs over a real session on the real hub, its spawner, publisher
and history EA on sockets of their own."""

import asyncio
import sys
from dataclasses import replace
from pathlib import Path

import pytest
from exec_harness import build, ours_deal
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.messages import SubmitOrder
from nautilus_trader.model.enums import OrderStatus
from session_double import deal_added
from venue_doubles import trade_result

from mt5connector.client.events import DealsRecovery, deals_topic, recovery_topic
from mt5connector.client.hub import HubSession
from mt5connector.wire import calls

# The hub's own doubles live beside its tests.
sys.path.insert(0, str(Path(__file__).parents[1] / "server"))

from ea_wire import FakeEa, answer  # noqa: E402
from hub_doubles import run_hub, verified  # noqa: E402

from mt5connector.server.settings import read_hub_settings  # noqa: E402
from mt5connector.server.wire.push_wire import ChartRole  # noqa: E402

Call = calls.Call
# Room for two held recoveries, an order and a fill's deal read, all pending on the connection.
PENDING_BOUND = "4"
HELD = (771, 772)
BURST = tuple(range(801, 821))


async def requests_of(ea: FakeEa, call: Call, *, quiet_s: float = 0.5) -> list:
    """Every request of `call` the hub hands the EA until it hands none for `quiet_s`."""
    asked = []
    while True:
        try:
            request = await ea.request(timeout=quiet_s)
        except TimeoutError:
            return asked
        if request.call == call:
            asked.append(request)


async def next_request(ea: FakeEa, call: Call):
    while True:
        request = await ea.request(timeout=5.0)
        if request.call == call:
            return request


@pytest.fixture
async def hub_with_eas(tmp_path, monkeypatch):
    monkeypatch.setenv("MT5_HISTORY_STORE", str(tmp_path))
    bound = read_hub_settings(
        {"MT5_HUB_PENDING_BOUND": PENDING_BOUND, "MT5_HISTORY_STORE": str(tmp_path)}
    ).pending_bound
    hub, server, port = await run_hub(pending_bound=bound)
    async with verified(port) as spawner:
        history = await FakeEa.connect(port, ChartRole.HISTORY, "EURUSD")
        publisher = await FakeEa.connect(port, ChartRole.PUBLISHER, "EURUSD")
        try:
            yield port, spawner, publisher, history
        finally:
            await publisher.close()
            await history.close()
            await spawner.close()
    server.close()


async def test_two_held_recoveries_and_a_burst_leave_an_order_and_a_fills_deal_read_room(
    exec_venue, hub_with_eas
):
    port, spawner, publisher, history = hub_with_eas
    h = build(exec_venue)
    order = h.place(h.limit(), status=OrderStatus.SUBMITTED)
    resting = h.place(h.limit(price="1.07000"), ticket=9001)
    await h.connect()
    session = HubSession(
        replace(h.client._config, ws_url=f"ws://127.0.0.1:{port}"),
        asyncio.get_running_loop(),
        h.conn._on_frame,
        h.conn._on_reconnect,
        h.client.recorded_log,
    )
    await session.connect()
    h.conn._session = session
    figures: list = []
    h.client._msgbus.subscribe(topic=deals_topic(h.client.venue), handler=figures.append)
    try:
        for position in HELD + BURST:
            h.client._msgbus.publish(
                topic=recovery_topic(h.client.venue), msg=DealsRecovery(position, "EURUSD")
            )
        held = await requests_of(history, Call.HISTORY_DEALS_RECOVER)

        # The burst came while two were read: nothing of it reached the hub.
        assert [
            calls.decode_arguments(calls.CALLS[Call.HISTORY_DEALS_RECOVER], r)["position"]
            for r in held
        ] == list(HELD)

        sending = asyncio.get_running_loop().create_task(
            h.client._submit_order(
                SubmitOrder(
                    trader_id=order.trader_id,
                    strategy_id=order.strategy_id,
                    order=order,
                    command_id=UUID4(),
                    ts_init=0,
                )
            )
        )
        sent = await next_request(publisher, Call.ORDER_SEND)
        deal = ours_deal(ticket=7001, order=9001)
        h.conn._on_frame(deal_added(deal))
        read = await next_request(spawner, Call.HISTORY_DEALS_GET)

        # The order and the deal read are both at their EAs, beside the two held recoveries: four
        # pending on a bound of four, none refused.
        await publisher.send(
            answer(
                sent.request_id, Call.ORDER_SEND, trade_result(request_id=41, order=9401)._asdict()
            )
        )
        await sending
        await spawner.send(answer(read.request_id, Call.HISTORY_DEALS_GET, [deal._asdict()]))
        await asyncio.wait_for(asyncio.gather(*h.client._handlers - h.client._recovering), 5)

        assert "OrderRejected" not in h.names()
        assert 41 in h.client._pending
        assert [
            event.client_order_id for event in h.events if type(event).__name__ == "OrderFilled"
        ] == [resting.client_order_id]
        assert [figure.ticket for figure in figures] == [7001]

        for request in held:
            position = calls.decode_arguments(calls.CALLS[Call.HISTORY_DEALS_RECOVER], request)[
                "position"
            ]
            recovered = ours_deal(ticket=position + 100, order=position, position_id=position)
            await history.send(
                answer(request.request_id, Call.HISTORY_DEALS_RECOVER, [recovered._asdict()])
            )
        await asyncio.wait_for(asyncio.gather(*h.client._recovering), 5)
        assert sorted(figure.ticket for figure in figures) == [871, 872, 7001]
    finally:
        await session.disconnect()
