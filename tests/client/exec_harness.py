"""An execution client wired to real NT components over a double of the shim: it records the events
the client emits and the lines it logs, and places NT orders in the cache at the status a test
needs."""

import asyncio
from dataclasses import dataclass
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pandas as pd
from nautilus_trader.common.component import LiveClock, MessageBus
from nautilus_trader.common.enums import LogLevel
from nautilus_trader.common.factories import OrderFactory
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.enums import OrderSide, OrderStatus, TimeInForce
from nautilus_trader.model.identifiers import (
    AccountId,
    InstrumentId,
    StrategyId,
    Symbol,
    TradeId,
    TraderId,
    VenueOrderId,
)
from nautilus_trader.model.instruments import CurrencyPair
from nautilus_trader.model.objects import Currency, Price, Quantity
from nautilus_trader.test_kit.stubs.component import TestComponentStubs
from nautilus_trader.test_kit.stubs.events import TestEventStubs
from venue_doubles import account_info, trade_deal, trade_order, trade_position

from mt5connector.client import execution
from mt5connector.client.config import MT5Config
from mt5connector.client.connection import AccountSnapshot, MT5Connection
from mt5connector.client.execution import MT5LiveExecutionClient
from mt5connector.client.providers import MT5InstrumentProvider

TRADER_ID = TraderId("TESTER-001")
STRATEGY_ID = StrategyId("S-001")
ACCOUNT_ID = AccountId("MT5-12345678")
MAGIC = execution.magic_for(TRADER_ID)
EURUSD = InstrumentId.from_str("EURUSD.MT5")

# The flags of `symbol_info().filling_mode`, as MQL5 documents them.
FILLING_FOK = 1
FILLING_IOC = 2
FILLING_BOC = 4


def instrument(
    symbol: str = "EURUSD",
    filling_mode: int = FILLING_FOK | FILLING_IOC,
    lot_step: str = "0.01",
):
    """A five-digit pair settling in USD, sized in steps of `lot_step` lots, as the provider types
    it."""
    step = Quantity.from_str(lot_step)
    return CurrencyPair(
        instrument_id=InstrumentId(Symbol(symbol), EURUSD.venue),
        raw_symbol=Symbol(symbol),
        base_currency=Currency.from_str("EUR"),
        quote_currency=USD,
        price_precision=5,
        size_precision=step.precision,
        price_increment=Price.from_str("0.00001"),
        size_increment=step,
        multiplier=Quantity.from_str("100000"),
        min_quantity=step,
        max_quantity=Quantity(500, step.precision),
        maker_fee=Decimal(0),
        taker_fee=Decimal(0),
        ts_event=0,
        ts_init=0,
        info={"filling_mode": filling_mode},
    )


def ours_order(**fields):
    return trade_order(**({"magic": MAGIC} | fields))


def ours_deal(**fields):
    return trade_deal(**({"magic": MAGIC} | fields))


def ours_position(**fields):
    return trade_position(**({"magic": MAGIC} | fields))


class RecordedExecClient(MT5LiveExecutionClient):
    """The execution client with its log calls recorded."""

    def __init__(self, *args, **kwargs):
        self.recorded_log = MagicMock()
        super().__init__(*args, **kwargs)

    @property
    def _log(self):
        return self.recorded_log


def config(**fields) -> MT5Config:
    return MT5Config(
        account=12345678,
        password="p",
        server="Broker-Demo",
        symbols=["EURUSD"],
        server_url="http://127.0.0.1:5000",
        **fields,
    )


@dataclass
class Harness:
    """One client under test, with what it emitted."""

    client: RecordedExecClient
    venue: MagicMock
    conn: MagicMock
    cache: object
    factory: OrderFactory
    ledger: list
    instruments: dict

    @property
    def events(self) -> list:
        """The order events the client emitted, in order."""
        return [item for item in self.ledger if type(item).__name__.startswith("Order")]

    def names(self) -> list[str]:
        """The class names of everything the client emitted, account states included, in order."""
        return [type(item).__name__ for item in self.ledger]

    def logged(self, level: LogLevel) -> list[str]:
        calls = getattr(self.client.recorded_log, level.name.lower()).call_args_list
        return [call.args[0] for call in calls]

    async def connect(self) -> None:
        """Connects the client and stops its poll loop, so a test drives each turn itself."""
        await self.client._connect()
        self.client._exec_poll_task.cancel()
        try:
            await self.client._exec_poll_task
        except asyncio.CancelledError:
            pass
        self.ledger.clear()
        self.venue.reset_mock()
        self.conn.get_account_info.reset_mock()

    def place(self, order, status: OrderStatus = OrderStatus.ACCEPTED, ticket=None):
        """Adds an NT order to the cache, advanced to SUBMITTED, ACCEPTED under `ticket`, or FILLED
        under `ticket`."""
        self.cache.add_order(order)
        if status in (OrderStatus.SUBMITTED, OrderStatus.ACCEPTED, OrderStatus.FILLED):
            order.apply(TestEventStubs.order_submitted(order, account_id=ACCOUNT_ID))
        if status in (OrderStatus.ACCEPTED, OrderStatus.FILLED):
            order.apply(
                TestEventStubs.order_accepted(
                    order, account_id=ACCOUNT_ID, venue_order_id=VenueOrderId(str(ticket))
                )
            )
        if status == OrderStatus.FILLED:
            order.apply(
                TestEventStubs.order_filled(
                    order,
                    self.instruments["EURUSD"],
                    account_id=ACCOUNT_ID,
                    venue_order_id=VenueOrderId(str(ticket)),
                    trade_id=TradeId(f"T-{ticket}"),
                )
            )
        self.cache.update_order(order)
        return order

    def market(self, side=OrderSide.BUY, quantity="0.01", **kwargs):
        return self.factory.market(EURUSD, side, Quantity.from_str(quantity), **kwargs)

    def limit(self, side=OrderSide.BUY, quantity="0.01", price="1.08000", **kwargs):
        return self.factory.limit(
            EURUSD, side, Quantity.from_str(quantity), Price.from_str(price), **kwargs
        )

    def stop_market(self, side=OrderSide.BUY, quantity="0.01", trigger="1.09000", **kwargs):
        return self.factory.stop_market(
            EURUSD, side, Quantity.from_str(quantity), Price.from_str(trigger), **kwargs
        )

    def stop_limit(
        self, side=OrderSide.SELL, quantity="0.01", price="1.07000", trigger="1.07500", **kwargs
    ):
        return self.factory.stop_limit(
            EURUSD,
            side,
            Quantity.from_str(quantity),
            Price.from_str(price),
            Price.from_str(trigger),
            **kwargs,
        )


def gtd(day: str = "2026-10-04T00:00:00Z") -> dict:
    return {"time_in_force": TimeInForce.GTD, "expire_time": pd.Timestamp(day)}


def build(venue, *, instruments=None, account=None, **settings) -> Harness:
    """A client of the trader TRADER_ID over `venue`, with EURUSD loaded unless `instruments` names
    others, its connection reporting `account`."""
    clock = LiveClock()
    msgbus = MessageBus(trader_id=TRADER_ID, clock=clock)
    ledger = []
    msgbus.register(endpoint="ExecEngine.process", handler=ledger.append)
    msgbus.register(endpoint="Portfolio.update_account", handler=ledger.append)
    cache = TestComponentStubs.cache()
    loaded = {item.raw_symbol.value: item for item in (instruments or [instrument()])}

    conn = MagicMock(spec=MT5Connection)
    conn.get_account_info.return_value = AccountSnapshot.from_mt5(account or account_info())
    conn.get_terminal_info.return_value = {"connected": True, "trade_allowed": True}
    conn.reconnect_async = AsyncMock(return_value=True)

    provider = MagicMock(spec=MT5InstrumentProvider)
    provider.load_ids_async = AsyncMock()
    provider.list_all.return_value = list(loaded.values())
    provider.get_instrument.side_effect = loaded.get

    client = RecordedExecClient(
        loop=asyncio.get_running_loop(),
        connection=conn,
        msgbus=msgbus,
        cache=cache,
        clock=clock,
        instrument_provider=provider,
        config=config(**settings),
    )
    factory = OrderFactory(trader_id=TRADER_ID, strategy_id=STRATEGY_ID, clock=clock)
    return Harness(client, venue, conn, cache, factory, ledger, loaded)
