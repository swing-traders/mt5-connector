"""Failed chart reads through the real hub and client session."""

import asyncio
from contextlib import asynccontextmanager
from unittest.mock import MagicMock

import pytest
from hub_doubles import Consumer, eventually, run_hub, verified

from mt5connector.client.config import MT5Config
from mt5connector.client.errors import EaError, MT5SymbolNotFoundError
from mt5connector.client.hub import HubSession
from mt5connector.client.providers import _definition
from mt5connector.server.wire import binary, calls
from mt5connector.server.wire.push_wire import ChartRole
from mt5connector.wire import calls as client_calls


@asynccontextmanager
async def failed_chart(symbol, error):
    hub, server, port = await run_hub(retry_after_s=0.1)
    async with verified(port) as spawner:
        consumer = await Consumer.connect(port)
        session = HubSession(
            MT5Config(account=12345678, server="Broker-Demo", ws_url=f"ws://127.0.0.1:{port}"),
            asyncio.get_running_loop(),
            lambda frame: None,
            None,
            MagicMock(),
        )
        try:
            await consumer.ask(1, calls.Call.SYMBOL_INFO, symbol=symbol)
            await consumer.received()
            assert await spawner.received() == binary.OpenChart(ChartRole.PUBLISHER, symbol)
            await spawner.send(binary.ChartFailed(ChartRole.PUBLISHER, symbol, error))
            await eventually(lambda: (ChartRole.PUBLISHER, symbol) in hub._failed)
            await session.connect()
            yield session, spawner
        finally:
            await session.disconnect()
            await consumer.close()
            await spawner.close()
            server.close()
            await server.wait_closed()


async def test_an_unknown_symbol_chart_fails_the_provider_at_once():
    async with failed_chart("NOSUCH", 4301) as (session, _spawner):
        with pytest.raises(MT5SymbolNotFoundError) as refused:
            await asyncio.wait_for(_definition(session, "NOSUCH"), 0.3)
        assert refused.value.symbol == "NOSUCH"


async def test_a_capacity_refusal_ends_deferring_reads_without_reopening():
    async with failed_chart("EURUSD", 4102) as (session, _spawner):
        with pytest.raises(EaError) as refused:
            await asyncio.wait_for(
                session.deferring(client_calls.Call.SYMBOL_INFO, symbol="EURUSD"), 0.3
            )
        assert refused.value.last_error == 4102
