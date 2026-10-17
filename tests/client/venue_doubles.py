"""The terminal's account, symbol and tick structs as the hub session hands them back, every field
the package carries present, the trading sessions a publisher relays, and a hub session serving an
instrument provider's reads, for tests that feed the client a venue."""

import asyncio
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock

from mt5connector.client.errors import EaError
from mt5connector.client.hub import Answered, HubSession
from mt5connector.wire import calls, mirror

Call = calls.Call
# The order_send result the execution client's shim answers, which no hub call carries.
_SHIM_TYPES = {
    name: namedtuple(name.value, mirror.STRUCTS[name].fields)
    for name in (mirror.StructName.TRADE_REQUEST, mirror.StructName.ORDER_SEND_RESULT)
}
# MQL5's ERR_MARKET_UNKNOWN_SYMBOL, which the hub's answer carries for a symbol the broker does not
# list.
UNKNOWN_SYMBOL = 4301

_ACCOUNT_STRINGS = frozenset({"name", "server", "currency", "company"})
_SYMBOL_STRINGS = frozenset(
    {
        "basis",
        "category",
        "currency_base",
        "currency_profit",
        "currency_margin",
        "bank",
        "description",
        "exchange",
        "formula",
        "isin",
        "name",
        "page",
        "path",
    }
)


def account_info(**fields):
    """A hedging account in USD with two currency digits that may trade, under `fields`."""
    struct = calls.STRUCT_TYPES[mirror.StructName.ACCOUNT_INFO]
    values = {name: "" if name in _ACCOUNT_STRINGS else 0 for name in struct._fields}
    values |= {
        "login": 12345678,
        "server": "Broker-Demo",
        "currency": "USD",
        "currency_digits": 2,
        "margin_mode": mirror.ACCOUNT_MARGIN_MODE_RETAIL_HEDGING,
        "trade_allowed": True,
        "leverage": 500,
        "balance": 10000.0,
        "equity": 10050.25,
        "margin": 100.0,
        "margin_free": 9950.25,
        "margin_level": 10050.25,
        "credit": 0.0,
        "profit": 50.25,
    }
    return struct(**(values | fields))


def symbol_info(**fields):
    """EURUSD as a FOREX pair a run may fully trade, under `fields`."""
    struct = calls.STRUCT_TYPES[mirror.StructName.SYMBOL_INFO]
    values = {name: "" if name in _SYMBOL_STRINGS else 0 for name in struct._fields}
    values |= {
        "name": "EURUSD",
        "digits": 5,
        "point": 0.00001,
        "trade_tick_size": 0.00001,
        "volume_min": 0.01,
        "volume_max": 500.0,
        "volume_step": 0.01,
        "trade_contract_size": 100000.0,
        "currency_base": "EUR",
        "currency_profit": "USD",
        "currency_margin": "EUR",
        "trade_calc_mode": mirror.SYMBOL_CALC_MODE_FOREX,
        "trade_mode": mirror.SYMBOL_TRADE_MODE_FULL,
        "chart_mode": mirror.SYMBOL_CHART_MODE_BID,
        "filling_mode": 3,
        "margin_initial": 0.0,
        "margin_maintenance": 0.0,
    }
    return struct(**(values | fields))


def tick(bid: float, ask: float, time: int = 1_760_000_000):
    """A quote the terminal answers for a selected symbol; `time` 0 is the all-zero struct an
    unselected or closed symbol answers."""
    return calls.Tick(
        time=time,
        bid=bid,
        ask=ask,
        last=0.0,
        volume=0,
        time_msc=time * 1000,
        flags=6,
        volume_real=0.0,
    )


def _struct(
    name: mirror.StructName, strings: frozenset[str], fields: dict, types=calls.STRUCT_TYPES
):
    struct = types[name]
    values = {field: "" if field in strings else 0 for field in struct._fields}
    return struct(**(values | fields))


def trade_order(**fields):
    """A pending EURUSD buy limit of 0.01 at 1.08 that the venue placed and still holds, under
    `fields`."""
    return _struct(
        mirror.StructName.TRADE_ORDER,
        frozenset({"symbol", "comment", "external_id"}),
        {
            "ticket": 9001,
            "time_setup": 1_760_000_000,
            "time_setup_msc": 1_760_000_000_000,
            "type": mirror.ORDER_TYPE_BUY_LIMIT,
            "type_time": mirror.ORDER_TIME_GTC,
            "type_filling": mirror.ORDER_FILLING_RETURN,
            "state": mirror.ORDER_STATE_PLACED,
            "volume_initial": 0.01,
            "volume_current": 0.01,
            "price_open": 1.08,
            "symbol": "EURUSD",
        }
        | fields,
    )


def trade_deal(**fields):
    """An EURUSD buy of 0.01 at 1.08 opening a position, under `fields`."""
    return _struct(
        mirror.StructName.TRADE_DEAL,
        frozenset({"symbol", "comment", "external_id"}),
        {
            "ticket": 7001,
            "order": 9001,
            "time": 1_760_000_000,
            "time_msc": 1_760_000_000_000,
            "type": mirror.DEAL_TYPE_BUY,
            "entry": mirror.DEAL_ENTRY_IN,
            "position_id": 9001,
            "volume": 0.01,
            "price": 1.08,
            "symbol": "EURUSD",
        }
        | fields,
    )


def trade_position(**fields):
    """An open EURUSD long of 0.01 at 1.08, under `fields`."""
    return _struct(
        mirror.StructName.TRADE_POSITION,
        frozenset({"symbol", "comment", "external_id"}),
        {
            "ticket": 9001,
            "time": 1_760_000_000,
            "time_msc": 1_760_000_000_000,
            "time_update": 1_760_000_000,
            "time_update_msc": 1_760_000_000_000,
            "type": mirror.POSITION_TYPE_BUY,
            "identifier": 9001,
            "volume": 0.01,
            "price_open": 1.08,
            "price_current": 1.08,
            "symbol": "EURUSD",
        }
        | fields,
    )


def send_result(**fields):
    """The venue's answer to a trade request that completed, under `fields`."""
    request = _struct(
        mirror.StructName.TRADE_REQUEST, frozenset({"symbol", "comment"}), {}, _SHIM_TYPES
    )
    return _struct(
        mirror.StructName.ORDER_SEND_RESULT,
        frozenset({"comment"}),
        {"retcode": mirror.TRADE_RETCODE_DONE, "order": 9001, "request": request} | fields,
        _SHIM_TYPES,
    )


def week() -> list[list[list[int]]]:
    """A week of sessions, Sunday first, as a publisher relays one: open from 18:00 on Sunday to
    16:57 on Friday, with a break from 16:59 to 18:00 every day between."""
    return [
        [[64800, 0]],
        [[0, 61140], [64800, 0]],
        [[0, 61140], [64800, 0]],
        [[0, 61140], [64800, 0]],
        [[0, 61140], [64800, 0]],
        [[0, 61020]],
        [],
    ]


def relayed_sessions() -> dict:
    """The relayed answer for a symbol whose quote and trade sessions are both week()."""
    return {"quote": week(), "trade": week(), "last_error": 0}


def listing(info):
    """A symbol's record in the terminal's listing: its definition without the market state."""
    struct = calls.STRUCT_TYPES[calls.CALLS[Call.SYMBOLS_GET].layout.name]
    return struct(**{field: getattr(info, field) for field in struct._fields})


def run_apart(coroutine):
    """Runs a coroutine to its end on a loop of its own thread, as a connection runs its session's
    coroutines on its own."""
    with ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(asyncio.run, coroutine).result()


class SymbolVenue:
    """The hub session of an instrument provider's connection: the terminal's listing, each listed
    symbol's definition and quote and the commission schedule and trading sessions relayed for it,
    each request asked by its call and symbol, in order. A symbol it does not list fails as the hub
    fails one the broker does not list; a refusal queued for a call and symbol is raised in turn
    before it answers."""

    deferring = HubSession.deferring

    def __init__(self) -> None:
        self.definitions = {}
        self.ticks = {}
        self.schedules = {}
        self.sessions = {}
        self.refusals: dict[tuple[calls.Call, str | None], list[Exception]] = {}
        self.asked: list[tuple[calls.Call, str | None]] = []
        self._log = MagicMock()

    def add(self, info, bid=None, ask=None, schedule=None, sessions=None):
        self.definitions[info.name] = info
        if bid is not None:
            self.ticks[info.name] = tick(bid, ask)
        if schedule is not None:
            self.schedules[info.name] = schedule
        if sessions is not None:
            self.sessions[info.name] = sessions

    def asked_of(self, symbol: str) -> list[calls.Call]:
        return [call for call, asked in self.asked if asked == symbol]

    async def request(self, call: calls.Call, **arguments) -> Answered:
        symbol = arguments.get("symbol")
        self.asked.append((call, symbol))
        refusals = self.refusals.get((call, symbol), [])
        if refusals:
            raise refusals.pop(0)
        elif call is Call.SYMBOLS_GET:
            return Answered(tuple(listing(info) for info in self.definitions.values()), 0)
        elif symbol not in self.definitions:
            raise EaError(
                f"{call.name.lower()}: the publisher chart of {symbol} failed to open: "
                f"MQL5 error {UNKNOWN_SYMBOL}",
                UNKNOWN_SYMBOL,
            )
        elif call is Call.SYMBOL_INFO:
            return Answered(self.definitions[symbol], 0)
        elif call is Call.SYMBOL_INFO_TICK:
            return Answered(self.ticks.get(symbol, tick(0.0, 0.0, time=0)), 0)
        elif call is Call.COMMISSION_SCHEDULE:
            return Answered(self.schedules.get(symbol, {"ret": 0, "last_error": 0, "rules": []}), 0)
        elif call is Call.TRADING_SESSIONS:
            return Answered(self.sessions.get(symbol, relayed_sessions()), 0)
        else:
            raise ValueError(f"{call.name.lower()}: not a call an instrument provider asks")
