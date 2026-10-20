"""MT5InstrumentProvider: the venue's own symbol definitions as NT instruments, and the venue symbol
quoting each currency pair a load's conversions need.

State: the loaded instruments, which each load adds to, replacing the instrument of a symbol it
reloads; and the quoting symbol per currency pair the last load's conversions need, which each load
replaces whole. A failed load changes neither."""

from __future__ import annotations

import logging
from collections import deque
from decimal import Decimal
from operator import attrgetter
from typing import TYPE_CHECKING, Any

from nautilus_trader.common.providers import InstrumentProvider
from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue

from mt5connector.client.commissions import (
    CONVERTED_MODES,
    CommissionRule,
    parse_schedule,
    taker_fee,
)
from mt5connector.client.connection import AccountSnapshot
from mt5connector.client.errors import (
    EaError,
    MT5ConfigError,
    MT5InstrumentError,
    MT5SymbolNotFoundError,
)
from mt5connector.client.parsing import (
    FOREX_MODES,
    InstrumentAny,
    TradeMode,
    calc_mode,
    finite_decimal,
    parse_symbol_info,
    trade_mode,
)
from mt5connector.client.sessions import parse_sessions
from mt5connector.wire import calls
from mt5connector.wire.push_wire import MqlError

if TYPE_CHECKING:
    from collections.abc import Iterable

    from nautilus_trader.common.component import Clock
    from nautilus_trader.config import InstrumentProviderConfig

    from mt5connector.client.connection import MT5Connection
    from mt5connector.client.hub import HubSession

logger = logging.getLogger(__name__)

# The venue symbol quoting each (base, profit) currency pair.
_Pairs = dict[tuple[str, str], str]
# The FOREX symbols listed for each (base, profit) currency pair, alphabetically, by trade mode.
_Listed = dict[tuple[str, str], list[tuple[str, TradeMode]]]
# A symbol's definition as the hub answers it, and its commission rule.
_Definition = tuple[Any, CommissionRule | None]


class MT5InstrumentProvider(InstrumentProvider):
    """Loads MT5 symbol definitions into NautilusTrader's cache."""

    def __init__(
        self,
        connection: MT5Connection,
        venue: Venue,
        clock: Clock,
        config: InstrumentProviderConfig | None = None,
    ) -> None:
        super().__init__(config)
        self._conn = connection
        self._venue = venue
        self._clock = clock
        self._quoted_pairs: dict[tuple[str, str], str] = {}

    # ── NautilusTrader overrides ──────────────────────────────────────────────

    async def initialize(self, reload: bool = False) -> None:
        """NT's initialization, refusing a config that names nothing to load, which NT's would only
        warn of."""
        if not self._config.load_all and not self._config.load_ids:
            raise MT5ConfigError("instrument_provider: load_all is off and load_ids names nothing")
        await super().initialize(reload)

    async def load_all_async(self, filters: dict | None = None) -> None:
        """Loads every symbol the terminal serves; reading a symbol opens its chart, and the
        terminal holds at most CHARTS_MAX charts."""
        self._conn.ensure_connected()
        listing = await _asked(self._conn.session, calls.Call.SYMBOLS_GET)
        await self._load(sorted(info.name for info in listing), listing)

    async def load_ids_async(
        self,
        instrument_ids: list[InstrumentId],
        filters: dict | None = None,
    ) -> None:
        """Loads the symbols the instrument ids name."""
        await self._load([instrument_id.symbol.value for instrument_id in instrument_ids])

    async def load_symbol(self, symbol: str) -> InstrumentAny:
        """Loads one symbol, by its exact broker name, and returns its instrument."""
        instruments = await self._load([symbol])
        return instruments[0]

    # ── Loading ───────────────────────────────────────────────────────────────

    async def _load(self, symbols: list[str], listing: tuple | None = None) -> list[InstrumentAny]:
        """Reads `symbols`, then the venue symbols quoting the pairs their conversions need, adds
        the instruments they build and answers them in that order; `listing` holds the terminal's
        symbols when the caller has read them."""
        self._conn.ensure_connected()
        loaded = {instrument.raw_symbol.value for instrument in self._instruments.values()}
        instruments, pairs = await self._read(symbols, loaded, listing)
        self._quoted_pairs = pairs
        for instrument in instruments:
            self.add(instrument)
        names = [instrument.raw_symbol.value for instrument in instruments]
        logger.info(f"MT5InstrumentProvider: loaded {', '.join(names)}")
        return instruments

    async def _read(
        self, symbols: list[str], loaded: set[str], listing: tuple | None
    ) -> tuple[list[InstrumentAny], _Pairs]:
        """The instrument of each symbol, then of each venue symbol quoting a pair their conversions
        need, each built from its definition, and the quoting symbol per pair, `loaded` the symbols
        already held; the terminal's symbols are listed unless `listing` holds them. Raises naming
        the first symbol the venue does not know or whose definition is refused."""
        session = self._conn.session
        account = AccountSnapshot.from_mt5((await session.account_info()).result)
        ts = self._clock.timestamp_ns()
        definitions = {}
        for symbol in symbols:
            definitions[symbol] = await _definition(session, symbol)
        if listing is None:
            listing = await _asked(session, calls.Call.SYMBOLS_GET)
        pairs, quoting = await _quote_conversions(
            session, definitions, _listed_pairs(listing), loaded | set(symbols), account.currency
        )
        instruments = []
        for info, rule in (definitions | quoting).values():
            try:
                instruments.append(
                    await self._load_definition(session, info, rule, account, pairs, ts)
                )
            except MT5InstrumentError as exc:
                raise MT5InstrumentError(f"{info.name}: {exc}") from exc
        return instruments, pairs

    async def _load_definition(
        self,
        session: HubSession,
        info,
        rule: CommissionRule | None,
        account: AccountSnapshot,
        pairs: _Pairs,
        ts: int,
    ):
        """Builds the instrument a symbol's definition describes, its taker fee from its commission
        rule and its trading sessions as relayed."""
        sessions = parse_sessions(
            await _asked(session, calls.Call.TRADING_SESSIONS, symbol=info.name)
        )
        fee = await _taker_fee(session, info, rule, pairs)
        return parse_symbol_info(info, self._venue, account, fee, sessions, ts)

    # ── Conversion pairs ──────────────────────────────────────────────────────

    def quoted_pairs(self) -> _Pairs:
        """The venue symbol quoting each (base, profit) currency pair the last load's conversions
        need."""
        return dict(self._quoted_pairs)

    async def account_currency(self) -> str:
        """The account's currency code, as the account reports it now."""
        return (await self._conn.account_info()).currency

    # ── Convenience accessors ─────────────────────────────────────────────────

    def get_instrument(self, symbol: str) -> InstrumentAny | None:
        """The loaded instrument of a symbol, by its exact broker name; None while not loaded."""
        return self.find(InstrumentId(Symbol(symbol), self._venue))

    def __repr__(self) -> str:
        return f"MT5InstrumentProvider(loaded={len(self._instruments)})"


async def _asked(session: HubSession, call: calls.Call, **arguments: object) -> object:
    """The result of a call on the hub session, asked again after each deferring answer's delay."""
    return (await session.deferring(call, **arguments)).result


async def _definition(session: HubSession, symbol: str) -> _Definition:
    """A symbol's definition and the commission rule its publisher relays; raises
    MT5SymbolNotFoundError for a symbol the broker does not list, MT5InstrumentError naming it for a
    refused rule, and the hub's error for any other failure."""
    try:
        info = await _asked(session, calls.Call.SYMBOL_INFO, symbol=symbol)
    except EaError as exc:
        if exc.last_error == MqlError.MARKET_UNKNOWN_SYMBOL:
            raise MT5SymbolNotFoundError(symbol) from exc
        else:
            raise
    schedule = await _asked(session, calls.Call.COMMISSION_SCHEDULE, symbol=symbol)
    try:
        rule = parse_schedule(schedule)
    except MT5InstrumentError as exc:
        raise MT5InstrumentError(f"{symbol}: {exc}") from exc
    return info, rule


def _listed_pairs(listing: tuple) -> _Listed:
    """The FOREX symbols of `listing` quoting each (base, profit) pair, alphabetically, with their
    trade modes."""
    listed: _Listed = {}
    for info in sorted(listing, key=attrgetter("name")):
        try:
            if calc_mode(info) in FOREX_MODES:
                pair = (info.currency_base, info.currency_profit)
                listed.setdefault(pair, []).append((info.name, trade_mode(info)))
        except MT5InstrumentError as exc:
            raise MT5InstrumentError(f"{info.name}: {exc}") from exc
    return listed


def _needed_pairs(
    info, rule: CommissionRule | None, account_currency: str, listed: _Listed
) -> set[tuple[str, str]]:
    """The pairs a symbol's conversions need: those on the walk over the listed pairs from its
    settlement currency to the account currency and, for a converted rule, from the rule's currency
    to its settlement currency."""
    needed = set()
    settlement = _walk(info.currency_profit, account_currency, listed)
    if settlement is not None:
        needed |= set(settlement)
    if rule is not None and rule.mode in CONVERTED_MODES:
        fee = _walk(rule.currency, info.currency_profit, listed)
        if fee is not None:
            needed |= set(fee)
    return needed


def _walk(
    source: str, target: str, pairs: Iterable[tuple[str, str]]
) -> tuple[tuple[str, str], ...] | None:
    """The (base, profit) pairs on the shortest walk over `pairs` from `source` to `target`, in walk
    order, each a step either way; None where no walk joins them. Equal choices are settled in the
    pairs' alphabetical order."""
    joins: dict[str, list[tuple[str, tuple[str, str]]]] = {}
    for base, profit in sorted(pairs):
        joins.setdefault(base, []).append((profit, (base, profit)))
        joins.setdefault(profit, []).append((base, (base, profit)))
    walks = {source: ()}
    frontier = deque([source])
    while frontier and target not in walks:
        currency = frontier.popleft()
        for other, pair in joins.get(currency, []):
            if other not in walks:
                walks[other] = (*walks[currency], pair)
                frontier.append(other)
    return walks.get(target)


async def _quote_conversions(
    session: HubSession,
    definitions: dict[str, _Definition],
    listed: _Listed,
    traded: set[str],
    account_currency: str,
) -> tuple[_Pairs, dict[str, _Definition]]:
    """The venue symbol quoting each pair the conversions of `definitions` need, and the definition
    of each quoting symbol beyond them, whose own conversions are needed in turn; `traded` names the
    symbols the run trades."""
    pairs: _Pairs = {}
    quoting: dict[str, _Definition] = {}
    asked: set[tuple[str, str]] = set()
    pending = list(definitions.values())
    while pending:
        needed = set()
        for info, rule in pending:
            needed |= _needed_pairs(info, rule, account_currency, listed)
        found = await _select_quoted_pairs(session, listed, needed - asked, traded)
        asked |= needed
        pairs |= found
        pending = []
        for symbol in sorted(set(found.values()) - definitions.keys()):
            quoting[symbol] = await _definition(session, symbol)
            pending.append(quoting[symbol])
    return pairs, quoting


async def _select_quoted_pairs(
    session: HubSession, listed: _Listed, needed: set[tuple[str, str]], traded: set[str]
) -> _Pairs:
    """The venue symbol quoting each pair of `needed`: one the run trades, else one fully tradable,
    else the first that quotes, each in alphabetical order; a pair none quotes is left out."""
    pairs = {}
    for pair in sorted(needed):
        symbol = await _select_quoting_symbol(session, listed[pair], traded)
        if symbol is not None:
            pairs[pair] = symbol
    return pairs


async def _select_quoting_symbol(
    session: HubSession, candidates: list[tuple[str, TradeMode]], loaded: set[str]
) -> str | None:
    for symbol, _ in candidates:
        if symbol in loaded:
            return symbol
    for symbol, mode in candidates:
        if mode == TradeMode.FULL:
            return symbol
    for symbol, _ in candidates:
        # A closed symbol quotes the all-zero struct.
        quote = await _asked(session, calls.Call.SYMBOL_INFO_TICK, symbol=symbol)
        if quote.time != 0:
            return symbol
    return None


async def _taker_fee(
    session: HubSession, info, rule: CommissionRule | None, pairs: _Pairs
) -> Decimal:
    """The symbol's taker fee under its commission rule, zero without one; the conversion into its
    quote currency is the venue's own quote, never NT's."""
    if rule is None:
        return Decimal(0)
    else:
        return await taker_fee(
            rule,
            price=await _mid(session, info.name),
            contract_size=finite_decimal(info.trade_contract_size, "trade_contract_size"),
            point=finite_decimal(info.point, "point"),
            rate=lambda currency: _rate(session, currency, info.currency_profit, pairs),
        )


async def _rate(session: HubSession, source: str, target: str, pairs: _Pairs) -> Decimal:
    """`target` units per `source` unit along the walk over `pairs`: each pair's quoting symbol at
    its mid, taken inversely where the walk crosses the pair from its profit currency to its base.
    Raises MT5InstrumentError where no walk joins them."""
    walk = _walk(source, target, pairs)
    if walk is None:
        raise MT5InstrumentError(f"no venue symbol converts {source} into {target}")
    else:
        rate = Decimal(1)
        currency = source
        for base, profit in walk:
            if currency == base:
                rate *= await _mid(session, pairs[(base, profit)])
                currency = profit
            else:
                rate /= await _mid(session, pairs[(base, profit)])
                currency = base
        return rate


async def _mid(session: HubSession, symbol: str) -> Decimal:
    """The mid of a symbol's last quote; raises MT5InstrumentError while it has none."""
    quote = await _asked(session, calls.Call.SYMBOL_INFO_TICK, symbol=symbol)
    if quote.time == 0:
        raise MT5InstrumentError(f"{symbol} has no quote")
    bid = finite_decimal(quote.bid, f"{symbol} bid")
    ask = finite_decimal(quote.ask, f"{symbol} ask")
    return (bid + ask) / 2
