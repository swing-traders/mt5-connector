"""
nautilus_mt5/providers.py

MT5InstrumentProvider — loads the symbols a run names into NautilusTrader's instrument cache, each
typed and filled from the venue's own definition, and finds the venue symbol that quotes each
currency pair.

State: the loaded instruments, and the quoting symbol per currency pair the last load found; a load
replaces both together, or neither when it fails.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from operator import attrgetter
from typing import TYPE_CHECKING

from nautilus_trader.common.providers import InstrumentProvider
from nautilus_trader.model.identifiers import InstrumentId, Symbol

from mt5connector.client import remote_mt5 as mt5
from mt5connector.client.commissions import CommissionRule, parse_schedule, taker_fee
from mt5connector.client.constants import MT5_VENUE
from mt5connector.client.errors import (
    MT5ConfigError,
    MT5ConnectionError,
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

if TYPE_CHECKING:
    from nautilus_trader.common.component import Clock
    from nautilus_trader.config import InstrumentProviderConfig

    from mt5connector.client.connection import AccountSnapshot, MT5Connection

logger = logging.getLogger(__name__)

# The venue symbol quoting each (base, profit) currency pair.
_Pairs = dict[tuple[str, str], str]


class MT5InstrumentProvider(InstrumentProvider):
    """Loads MT5 symbol definitions into NautilusTrader's cache."""

    def __init__(
        self,
        connection: MT5Connection,
        clock: Clock,
        config: InstrumentProviderConfig | None = None,
    ) -> None:
        super().__init__(config)
        self._conn = connection
        self._clock = clock
        self._quoted_pairs: dict[tuple[str, str], str] = {}

    # ── Required NautilusTrader overrides ─────────────────────────────────────

    async def load_all_async(self, filters: dict | None = None) -> None:
        """Loads every symbol the provider's config names."""
        if self._config.load_ids is None:
            raise MT5ConfigError("instrument provider: no symbols are configured")
        self._load(sorted(instrument_id.symbol.value for instrument_id in self._config.load_ids))

    async def load_ids_async(
        self,
        instrument_ids: list[InstrumentId],
        filters: dict | None = None,
    ) -> None:
        """Loads the symbols the instrument ids name."""
        self._load([instrument_id.symbol.value for instrument_id in instrument_ids])

    def load_symbol(self, symbol: str) -> InstrumentAny:
        """Loads one symbol, by its exact broker name, and returns its instrument."""
        return self._load([symbol])[0]

    # ── Loading ───────────────────────────────────────────────────────────────

    def _load(self, symbols: list[str]) -> list[InstrumentAny]:
        """Selects and reads each symbol's definition, builds its instrument and adds them all;
        raises naming the first symbol the venue does not know or whose definition is refused."""
        self._conn.ensure_connected()
        account = self._conn.get_account_info()
        ts = self._clock.timestamp_ns()
        definitions = []
        for symbol in symbols:
            definitions.append(self._select_and_read(symbol))
        loaded = {instrument.raw_symbol.value for instrument in self._instruments.values()}
        pairs = self._select_quoted_pairs(loaded | set(symbols))
        instruments = []
        for info in definitions:
            try:
                instruments.append(self._load_definition(info, account, pairs, ts))
            except MT5InstrumentError as exc:
                raise MT5InstrumentError(f"{info.name}: {exc}") from exc
        self._quoted_pairs = pairs
        for instrument in instruments:
            self.add(instrument)
        logger.info(f"MT5InstrumentProvider: loaded {', '.join(symbols)}")
        return instruments

    def _select_and_read(self, symbol: str):
        """Selects a symbol in Market Watch and reads its definition."""
        if not mt5.symbol_select(symbol, True):
            raise MT5SymbolNotFoundError(symbol)
        info = mt5.symbol_info(symbol)
        if info is None:
            raise MT5SymbolNotFoundError(symbol)
        return info

    def _load_definition(self, info, account: AccountSnapshot, pairs: _Pairs, ts: int):
        """Builds the instrument a selected symbol's definition describes, its taker fee from the
        commission rule the server relays, selecting the symbol that converts the rule's currency
        into the quote currency first."""
        rule = parse_schedule(mt5.commission_schedule(info.name))
        if rule is not None and rule.currency != info.currency_profit:
            converter, _ = _conversion(rule.currency, info.currency_profit, pairs)
            mt5.symbol_select(converter, True)
        return parse_symbol_info(info, account, _taker_fee(info, rule, pairs), ts)

    # ── Conversion pairs ──────────────────────────────────────────────────────

    def _select_quoted_pairs(self, loaded: set[str]) -> _Pairs:
        """The venue symbol quoting each (base, profit) pair its FOREX symbols cover: one the run
        loads, else one fully tradable, else the first that prices once selected, each in
        alphabetical order."""
        venue_symbols = mt5.symbols_get()
        if venue_symbols is None:
            code, msg = mt5.last_error()
            raise MT5ConnectionError(f"mt5.symbols_get() returned None — error {code}: {msg}")
        buckets: dict[tuple[str, str], list[tuple[str, TradeMode]]] = {}
        for info in sorted(venue_symbols, key=attrgetter("name")):
            try:
                if calc_mode(info) in FOREX_MODES:
                    pair = (info.currency_base, info.currency_profit)
                    buckets.setdefault(pair, []).append((info.name, trade_mode(info)))
            except MT5InstrumentError as exc:
                raise MT5InstrumentError(f"{info.name}: {exc}") from exc
        pairs = {}
        for pair, candidates in buckets.items():
            symbol = _select_quoting_symbol(candidates, loaded)
            if symbol is not None:
                pairs[pair] = symbol
        return pairs

    def quoted_pairs(self) -> _Pairs:
        """The venue symbol quoting each (base, profit) currency pair, as the last load found it."""
        return dict(self._quoted_pairs)

    def account_currency(self) -> str:
        """The account's currency code."""
        return self._conn.get_account_info().currency

    # ── Convenience accessors ─────────────────────────────────────────────────

    def get_instrument(self, symbol: str) -> InstrumentAny | None:
        """The loaded instrument of a symbol, by its exact broker name; None while not loaded."""
        return self.find(InstrumentId(Symbol(symbol), MT5_VENUE))

    def __repr__(self) -> str:
        return f"MT5InstrumentProvider(loaded={len(self._instruments)})"


def _select_quoting_symbol(candidates: list[tuple[str, TradeMode]], loaded: set[str]) -> str | None:
    for symbol, _ in candidates:
        if symbol in loaded:
            return symbol
    for symbol, mode in candidates:
        if mode == TradeMode.FULL:
            return symbol
    for symbol, _ in candidates:
        # An unselected symbol quotes the all-zero struct.
        if mt5.symbol_select(symbol, True):
            quote = mt5.symbol_info_tick(symbol)
            if quote is not None and quote.time != 0:
                return symbol
    return None


def _taker_fee(info, rule: CommissionRule | None, pairs: _Pairs) -> Decimal:
    """The symbol's taker fee under its commission rule, zero without one; the conversion into its
    quote currency is the venue's own quote, never NT's."""
    if rule is None:
        return Decimal(0)
    else:
        rate = _rate(rule.currency, info.currency_profit, pairs)
        contract_size = finite_decimal(info.trade_contract_size, "trade_contract_size")
        return taker_fee(rule, rate, contract_size, _mid(info.name))


def _rate(source: str, target: str, pairs: _Pairs) -> Decimal:
    """`target` units per `source` unit at the mid of the venue's quote for the pair."""
    if source == target:
        return Decimal(1)
    else:
        symbol, inverse = _conversion(source, target, pairs)
        if inverse:
            return 1 / _mid(symbol)
        else:
            return _mid(symbol)


def _conversion(source: str, target: str, pairs: _Pairs) -> tuple[str, bool]:
    """The venue symbol quoting `source` against `target`, and whether it quotes it inversely;
    raises MT5InstrumentError for a pair the venue does not quote."""
    if (source, target) in pairs:
        return pairs[(source, target)], False
    elif (target, source) in pairs:
        return pairs[(target, source)], True
    else:
        raise MT5InstrumentError(f"no venue symbol converts {source} into {target}")


def _mid(symbol: str) -> Decimal:
    """The mid of a selected symbol's last quote; raises MT5InstrumentError while it has none."""
    quote = mt5.symbol_info_tick(symbol)
    if quote is None or quote.time == 0:
        raise MT5InstrumentError(f"{symbol} has no quote")
    bid = finite_decimal(quote.bid, f"{symbol} bid")
    ask = finite_decimal(quote.ask, f"{symbol} ask")
    return (bid + ask) / 2
