"""Independent symbols, cancels and deal reports progress while their peers await answers."""

import asyncio

from exec_harness import build, ours_deal, ours_order
from test_execution import cancel_all_command
from test_execution_reports import fill_reports
from test_instrument_definitions import provider_for
from venue_doubles import SymbolVenue, symbol_info

from mt5connector.client.execution import order_comment
from mt5connector.wire import calls


async def test_definitions_are_asked_together_and_assembled_in_input_order():
    venue = SymbolVenue()
    for symbol in ("GBPUSD", "EURUSD"):
        venue.add(symbol_info(name=symbol), bid=1.08, ask=1.09)
    provider = provider_for(venue, "GBPUSD", "EURUSD")
    original = venue.request
    gates = {symbol: asyncio.Event() for symbol in ("GBPUSD", "EURUSD")}
    seen = set()
    both = asyncio.Event()

    async def request(call, **arguments):
        if call is calls.Call.SYMBOL_INFO:
            symbol = arguments["symbol"]
            seen.add(symbol)
            if len(seen) == 2:
                both.set()
            await gates[symbol].wait()
        return await original(call, **arguments)

    venue.request = request
    loading = asyncio.create_task(provider._load(["GBPUSD", "EURUSD"]))
    try:
        await asyncio.wait_for(both.wait(), 0.5)
        gates["EURUSD"].set()
        await asyncio.sleep(0)
        gates["GBPUSD"].set()
        instruments = await loading
        assert [instrument.raw_symbol.value for instrument in instruments] == ["GBPUSD", "EURUSD"]
    finally:
        for gate in gates.values():
            gate.set()
        await loading


async def test_cancel_all_sends_three_commands_before_any_answer(exec_venue):
    h = build(exec_venue)
    await h.connect()
    orders = []
    for ticket in (5101, 5102, 5103):
        order = h.place(h.limit(), ticket=ticket)
        orders.append(ours_order(ticket=ticket, comment=order_comment(order.client_order_id)))
    h.venue.orders_get.side_effect = lambda symbol=None, ticket=None: tuple(
        order for order in orders if ticket is None or order.ticket == ticket
    )
    original = h.push.request
    release = asyncio.Event()
    all_sent = asyncio.Event()
    seen = []

    async def request(call, **arguments):
        if call is calls.Call.ORDER_SEND:
            seen.append(arguments["request"]["order"])
            if len(seen) == 3:
                all_sent.set()
            await release.wait()
        return await original(call, **arguments)

    h.push.request = request
    canceling = asyncio.create_task(h.client._cancel_all_orders(cancel_all_command()))
    try:
        await asyncio.wait_for(all_sent.wait(), 0.5)
        assert seen == [5101, 5102, 5103]
    finally:
        release.set()
        await canceling
    for request_id in h.push.sent:
        await h.verdict(request_id=request_id)
    assert h.names() == ["OrderCanceled", "AccountState"] * 3


async def test_deal_reads_overlap_and_reports_are_sorted_by_ticket(exec_venue):
    h = build(exec_venue)
    await h.connect()
    h.venue.history_deals_get.return_value = tuple(
        ours_deal(ticket=7000 + number, order=9000 + number) for number in (3, 1, 2)
    )
    h.venue.orders_get.side_effect = lambda ticket=None: (ours_order(ticket=ticket),)
    original = h.push.request
    gates = {ticket: asyncio.Event() for ticket in (9001, 9002, 9003)}
    answered = {ticket: asyncio.Event() for ticket in gates}
    seen = set()
    all_seen = asyncio.Event()

    async def request(call, **arguments):
        ticket = arguments.get("ticket")
        if call is calls.Call.ORDERS_GET and ticket in gates:
            seen.add(ticket)
            if len(seen) == 3:
                all_seen.set()
            await gates[ticket].wait()
        result = await original(call, **arguments)
        if call is calls.Call.ORDERS_GET and ticket in gates:
            answered[ticket].set()
        return result

    h.push.request = request
    reading = asyncio.create_task(fill_reports(h))
    try:
        await asyncio.wait_for(all_seen.wait(), 0.5)
        for ticket in (9002, 9001, 9003):
            gates[ticket].set()
            await asyncio.wait_for(answered[ticket].wait(), 0.5)
        reports = await reading
        assert [report.trade_id.value for report in reports] == ["7001", "7002", "7003"]
    finally:
        for gate in gates.values():
            gate.set()
        await reading
