"""MT5InstrumentProvider: loads the symbols a run names into NT's instrument cache, each built from
the venue's own definition, and finds the venue symbol that quotes each currency pair.

State: the loaded instruments, and the quoting symbol per currency pair the last load found; a load
replaces both together, or neither when it fails."""

from __future__ import annotations

import logging
from decimal import Decimal
from operator import attrgetter
from typing import TYPE_CHECKING

from nautilus_trader.common.providers import InstrumentProvider
from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue

from mt5connector.client.commissions import (
    MONEY_MODES,
    CommissionRule,
    parse_schedule,
    taker_fee,
)
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
    from mt5connector.client.remote_mt5 import RemoteMT5

logger = logging.getLogger(__name__)

# The venue symbol quoting each (base, profit) currency pair.
_Pairs = dict[tuple[str, str], str]


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
        """Loads every symbol the terminal serves, selecting each in Market Watch; the commission
        read of a symbol no EA publishes opens its chart, and the terminal holds at most CHARTS_MAX
        charts."""
        self._load(sorted(info.name for info in _venue_symbols(self._conn.mt5)))

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
        if not self._conn.mt5.symbol_select(symbol, True):
            raise MT5SymbolNotFoundError(symbol)
        info = self._conn.mt5.symbol_info(symbol)
        if info is None:
            raise MT5SymbolNotFoundError(symbol)
        return info

    def _load_definition(self, info, account: AccountSnapshot, pairs: _Pairs, ts: int):
        """Builds the instrument a selected symbol's definition describes, its taker fee from the
        commission rule the server relays, selecting first the symbol that converts a money rule's
        currency into the quote currency; raises MT5InstrumentError when the venue refuses that
        selection."""
        rule = parse_schedule(self._conn.mt5.commission_schedule(info.name))
        if rule is not None and rule.mode in MONEY_MODES and rule.currency != info.currency_profit:
            converter, _ = _conversion(rule.currency, info.currency_profit, pairs)
            if not self._conn.mt5.symbol_select(converter, True):
                raise MT5InstrumentError(f"{converter} cannot be selected")
        fee = _taker_fee(self._conn.mt5, info, rule, pairs)
        return parse_symbol_info(info, self._venue, account, fee, ts)

    # ── Conversion pairs ──────────────────────────────────────────────────────

    def _select_quoted_pairs(self, loaded: set[str]) -> _Pairs:
        """The venue symbol quoting each (base, profit) pair its FOREX symbols cover: one the run
        loads, else one fully tradable, else the first that prices once selected, each in
        alphabetical order."""
        buckets: dict[tuple[str, str], list[tuple[str, TradeMode]]] = {}
        for info in sorted(_venue_symbols(self._conn.mt5), key=attrgetter("name")):
            try:
                if calc_mode(info) in FOREX_MODES:
                    pair = (info.currency_base, info.currency_profit)
                    buckets.setdefault(pair, []).append((info.name, trade_mode(info)))
            except MT5InstrumentError as exc:
                raise MT5InstrumentError(f"{info.name}: {exc}") from exc
        pairs = {}
        for pair, candidates in buckets.items():
            symbol = _select_quoting_symbol(self._conn.mt5, candidates, loaded)
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
        return self.find(InstrumentId(Symbol(symbol), self._venue))

    def __repr__(self) -> str:
        return f"MT5InstrumentProvider(loaded={len(self._instruments)})"


def _venue_symbols(mt5: RemoteMT5) -> tuple:
    """Every symbol the terminal serves; raises MT5ConnectionError for a listing that reads None."""
    venue_symbols = mt5.symbols_get()
    if venue_symbols is None:
        code, msg = mt5.last_error()
        raise MT5ConnectionError(f"mt5.symbols_get() returned None — error {code}: {msg}")
    return venue_symbols


def _select_quoting_symbol(
    mt5: RemoteMT5, candidates: list[tuple[str, TradeMode]], loaded: set[str]
) -> str | None:
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


def _taker_fee(mt5: RemoteMT5, info, rule: CommissionRule | None, pairs: _Pairs) -> Decimal:
    """The symbol's taker fee under its commission rule, zero without one; the conversion into its
    quote currency is the venue's own quote, never NT's."""
    if rule is None:
        return Decimal(0)
    else:
        return taker_fee(
            rule,
            price=_mid(mt5, info.name),
            contract_size=finite_decimal(info.trade_contract_size, "trade_contract_size"),
            point=finite_decimal(info.point, "point"),
            rate=lambda currency: _rate(mt5, currency, info.currency_profit, pairs),
        )


def _rate(mt5: RemoteMT5, source: str, target: str, pairs: _Pairs) -> Decimal:
    """`target` units per `source` unit at the mid of the venue's quote for the pair."""
    if source == target:
        return Decimal(1)
    else:
        symbol, inverse = _conversion(source, target, pairs)
        if inverse:
            return 1 / _mid(mt5, symbol)
        else:
            return _mid(mt5, symbol)


def _conversion(source: str, target: str, pairs: _Pairs) -> tuple[str, bool]:
    """The venue symbol quoting `source` against `target`, and whether it quotes it inversely;
    raises MT5InstrumentError for a pair the venue does not quote."""
    if (source, target) in pairs:
        return pairs[(source, target)], False
    elif (target, source) in pairs:
        return pairs[(target, source)], True
    else:
        raise MT5InstrumentError(f"no venue symbol converts {source} into {target}")


def _mid(mt5: RemoteMT5, symbol: str) -> Decimal:
    """The mid of a selected symbol's last quote; raises MT5InstrumentError while it has none."""
    quote = mt5.symbol_info_tick(symbol)
    if quote is None or quote.time == 0:
        raise MT5InstrumentError(f"{symbol} has no quote")
    bid = finite_decimal(quote.bid, f"{symbol} bid")
    ask = finite_decimal(quote.ask, f"{symbol} ask")
    return (bid + ask) / 2
