# mt5-connector

This is the swing-traders organisation's hard fork of [aulekator/mt5-connector](https://github.com/aulekator/mt5-connector). It is maintained independently and does not track upstream.

**Unofficial community MetaTrader 5 adapter for NautilusTrader** — live trading and backtesting on any MT5 broker (Exness, IC Markets, Pepperstone, and more).

> ⚠️ **Disclaimer:** This is an independent community project. It is **not** affiliated with, endorsed by, or supported by [Nautech Systems Pty Ltd](https://nautilustrader.io) or the official [NautilusTrader](https://nautilustrader.io) project.

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Platform: Linux | Windows](https://img.shields.io/badge/platform-Linux%20%7C%20Windows-lightgrey.svg)](#requirements)
[![Unofficial](https://img.shields.io/badge/NautilusTrader-unofficial%20community%20adapter-orange.svg)](https://nautilustrader.io)

---

## What this is

`mt5-connector` is a **data and execution adapter** that connects [NautilusTrader](https://nautilustrader.io) to any MetaTrader 5 broker. Write your strategy once in Python, then run it as a backtest against historical MT5 data  or flip a switch and run it live.

```
MT5 server ←→ mt5-connector-client ←→ NautilusTrader
                              ↑
                 pushed ticks, bars and transactions,
                 order routing, account state, reconciliation
```

**What you get:**

- Live quote ticks, a mark price at the mid of each, and the venue's own closed bars, pushed from the terminal over WebSocket; the ticks aggregate into any bar type NautilusTrader supports
- Order lifecycle: market, limit, stop and stop-limit entries, their fills booked to the venue's hedging positions, and each position's exits — its stop, its target and its closes — as the venue's own stop loss, take profit and closing deals; every fill, cancel and expiry pushed from the terminal as it happens
- Account state and position reconciliation on startup and continuously
- Historical bar data download into a NautilusTrader Parquet catalog for backtesting
- Automatic reconnection with exponential backoff
- Works with any MT5 broker — Exness, IC Markets, Pepperstone, OANDA, and more

> **Platform note:** The adapter talks only to the MT5 server — `mt5-connector-server`, running beside the MT5 terminal (see [The MT5 server](#the-mt5-server)) — over HTTP and WebSocket, so it needs no `MetaTrader5` package and runs on any platform. Backtesting with downloaded data works on any platform once the data has been collected.

---

## Distributions

The repository ships two distributions, versioned together: one release builds both wheels onto one index.

| Distribution | Import packages | Installs into | Carries |
|---|---|---|---|
| `mt5-connector-client` | `mt5connector.client`, `mt5connector.wire` | the Python that runs NautilusTrader | the data and execution clients, the instrument provider, the factories, the remote backend, the history client and the downloader, and the wire vocabulary |
| `mt5-connector-server` | `mt5connector.server` | the terminal's Windows Python, and the Linux Python that runs the hub | the HTTP server, the push hub, the EA's source, and a copy of the wire vocabulary made when its wheel is built |

`mt5connector` is a namespace package, so the two install side by side in one environment. The wire vocabulary — the inventory of the `MetaTrader5` package, the broker clock and the history protocol's names — lives once, in the client's tree.

---

## Table of contents

- [mt5-connector](#mt5-connector)
  - [What this is](#what-this-is)
  - [Distributions](#distributions)
  - [Table of contents](#table-of-contents)
  - [Requirements](#requirements)
  - [Installation](#installation)
  - [Quick start](#quick-start)
  - [Configuration](#configuration)
    - [Symbol naming](#symbol-naming)
  - [Writing a strategy](#writing-a-strategy)
  - [Backtesting](#backtesting)
    - [Step 1 — download historical data](#step-1--download-historical-data)
    - [Step 2 — run the backtest](#step-2--run-the-backtest)
  - [Live trading](#live-trading)
    - [Bar types for live trading](#bar-types-for-live-trading)
  - [The MT5 server](#the-mt5-server)
  - [Releases](#releases)
  - [Running the full test suite](#running-the-full-test-suite)
  - [Project structure](#project-structure)
  - [Broker compatibility](#broker-compatibility)
  - [Troubleshooting](#troubleshooting)
  - [Safety notes](#safety-notes)
  - [License](#license)

---

## Requirements

- Python 3.11+
- An MT5 server (`mt5-connector-server`, see [The MT5 server](#the-mt5-server)) running beside a MetaTrader 5 terminal logged in to your broker account
- An MT5 broker account (demo accounts work perfectly for development)

---

## Installation

Releases are published as wheels on the fork's package index. Install the adapter where NautilusTrader runs:

```bash
pip install --extra-index-url https://swing-traders.github.io/mt5-connector/simple/ "mt5-connector-client==0.4.0+st.N"
```

and the server, at the same release, into the Windows Python beside the terminal (and the Linux Python that runs its hub):

```bash
python -m pip install --extra-index-url https://swing-traders.github.io/mt5-connector/simple/ "mt5-connector-server==0.4.0+st.N"
```

For development, create the environment with mamba and layer the dev tooling on top; `environment.yml` installs the client editable from `packages/client`, and `environment.dev.yml` the server from `packages/server`:

```bash
git clone https://github.com/swing-traders/mt5-connector
cd mt5-connector
mamba env create -f environment.yml
mamba env update -f environment.dev.yml
just lint
just test
```

---

## Quick start

**1. Run an MT5 server.** The adapter talks to the MT5 terminal through `mt5-connector-server` — [its README](packages/server/mt5connector/server/README.md) states what the terminal and the server need. The server must be up, and the terminal's AutoTrading on, before you run any script; without AutoTrading every order is rejected.

**2. Test the connection:**

```python
from mt5connector.client import remote_mt5 as mt5

mt5.configure("http://127.0.0.1:5000")
info = mt5.account_info()
print(info.currency, info.balance)
```

**3. Run the example live strategy.** The examples read `MT5_ACCOUNT`, `MT5_PASSWORD`, `MT5_SERVER`, `MT5_SYMBOLS` and `MT5_SERVER_URL` from the environment. Find your server name in MT5 → File → Open Account → search your broker.

```bash
python examples/live_remote.py
```

---

## Configuration

All configuration goes through `MT5Config`. The required fields are your account credentials, symbols, and the MT5 server's URL; a config without `server_url` is refused when it is built.

```python
from mt5connector.client.config import MT5Config

config = MT5Config(
    account    = 12345678,            # MT5 account number
    password   = "your_password",
    server     = "Exness-MT5Trial9",  # broker server name
    symbols    = ["EURUSDm", "XAUUSDm"],
    server_url = "http://127.0.0.1:5000",  # the MT5 server's HTTP API
)
```

**Full configuration reference:**

```python
config = MT5Config(
    # Required
    account  = 12345678,
    password = "your_password",
    server   = "Exness-MT5Trial9",
    symbols  = ["EURUSDm", "XAUUSDm"],
    server_url = "http://127.0.0.1:5000",

    # The WebSocket push hub (default: derived from server_url, on port 9000)
    ws_url = "ws://127.0.0.1:9000",

    # How often the execution client checks its terminal session and reports an owed account
    exec_poll_interval_ms = 250,   # default: 250ms

    # Execution
    deviation_points        = 20,  # the price deviation a market order accepts, in points
    account_refresh_seconds = 10,  # the longest the account goes unreported between events
    history_lookback_mins   = 60,  # the venue history read when NT names no window, and at connect

    # Reconnection: the terminal session's, and the push channel's backoff (which never gives up)
    reconnect_initial_delay_s = 1.0,
    reconnect_max_delay_s     = 60.0,
    reconnect_max_attempts    = 20,

    # Connection timeout
    timeout_s = 10.0,
)
```

**Loading from the environment** (never hardcode credentials):

```python
import os
from mt5connector.client.config import MT5Config

config = MT5Config(
    account  = int(os.environ["MT5_ACCOUNT"]),
    password = os.environ["MT5_PASSWORD"],
    server   = os.environ["MT5_SERVER"],
    symbols  = os.environ["MT5_SYMBOLS"].split(","),
    server_url = os.environ["MT5_SERVER_URL"],
)
```

### Symbol naming

Different brokers use different symbol names. Always use the **exact name shown in your MT5 Market Watch window**.

| Broker | EURUSD | Gold | Bitcoin |
|--------|--------|------|---------|
| Exness standard | `EURUSDm` | `XAUUSDm` | `BTCUSDm` |
| Exness zero/raw | `EURUSD` | `XAUUSD` | `BTCUSD` |
| IC Markets | `EURUSD` | `XAUUSD` | `BTCUSD` |
| Pepperstone | `EURUSD` | `XAUUSD` | `BTCUSD` |

The instrument provider loads exactly these symbols and builds each one from the venue's own definition — nothing is inferred from a symbol's name:

- **Type** by its calc mode: a FOREX mode is a `CurrencyPair`, a CFD mode (CFD, CFD index, CFD leverage) a `Cfd`; any other mode (futures, exchange stocks, bonds, …) is refused at load, naming the symbol.
- **Grid and limits**: price precision is `digits`, the price increment `trade_tick_size`, the size increment and limits the volume step, minimum and maximum, and the multiplier the contract size.
- **Currencies**: the base is `currency_base` and the quote — the settlement currency — `currency_profit`, each at NT's precision, else the account's currency digits for the account currency, else its ISO 4217 minor units. A settlement code none of those covers is refused at load; a base code is built as NT builds a code it does not know. The account currency and every settlement currency are registered with NT before the first account state; base-only codes never are.
- **Taker fee** from the commission schedule the server relays for the symbol, its first rule's first tier: money per lot in the deposit currency or per unit in a named currency, converted into the quote currency through the venue's own quote; a percentage of the deal's value; or points of the price. It is halved when charged on entry alone. Loading waits for the symbol's EA to relay its schedule — the server has its chart opened when none publishes it; a relayed schedule with no rule gives zero; a refused relay, a chart that fails to open, a conversion symbol the venue refuses to select, and a rule of any other mode or charged on exit alone, fail the load naming the symbol.
- **`info`** carries the venue facts a consumer reads: chart, filling, calc and trade modes, stops and freeze levels, the volume limit, the margin currency, the session calendar (`session_tz`, `session_day_open`, `session_week_open`) and `bar_volume` (`tick_count`).

The provider also answers what a consumer discovers its settlement currencies by:

- `quoted_pairs() -> dict[tuple[str, str], str]` — the venue symbol quoting each `(base, profit)` currency pair the venue's FOREX symbols cover, as the last load found it: a symbol the run loads, else one fully tradable, else the first that prices once selected, each rung in alphabetical order.
- `account_currency() -> str` — the account's currency code, as the account reports it at the call.

---

## Writing a strategy

Strategies are plain NautilusTrader `Strategy` subclasses. The adapter handles all the MT5-specific plumbing — your strategy code is identical for both backtesting and live trading.

```python
from decimal import Decimal
from nautilus_trader.model.data import Bar, BarType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.objects import Quantity
from nautilus_trader.trading.strategy import Strategy
from nautilus_trader.config import StrategyConfig


class SmaCrossConfig(StrategyConfig, frozen=True):
    instrument_id : str
    bar_type      : str
    fast_period   : int     = 10
    slow_period   : int     = 30
    trade_size    : Decimal = Decimal("0.01")


class SmaCrossStrategy(Strategy):

    def __init__(self, config: SmaCrossConfig) -> None:
        super().__init__(config)
        self.instrument_id = InstrumentId.from_str(config.instrument_id)
        self.bar_type      = BarType.from_str(config.bar_type)
        self.fast_period   = config.fast_period
        self.slow_period   = config.slow_period
        self.trade_size    = config.trade_size
        self._fast_prices: list[float] = []
        self._slow_prices: list[float] = []
        self._position_side = None

    def on_start(self) -> None:
        self.instrument = self.cache.instrument(self.instrument_id)
        self.subscribe_bars(self.bar_type)

    def on_bar(self, bar: Bar) -> None:
        close = float(bar.close)
        self._fast_prices.append(close)
        self._slow_prices.append(close)
        if len(self._fast_prices) > self.fast_period:
            self._fast_prices.pop(0)
        if len(self._slow_prices) > self.slow_period:
            self._slow_prices.pop(0)

        if len(self._fast_prices) < self.fast_period:
            return

        fast_sma = sum(self._fast_prices) / self.fast_period
        slow_sma = sum(self._slow_prices) / self.slow_period

        if fast_sma > slow_sma and self._position_side != OrderSide.BUY:
            self._close_position()
            self._open_position(OrderSide.BUY)
        elif fast_sma < slow_sma and self._position_side != OrderSide.SELL:
            self._close_position()
            self._open_position(OrderSide.SELL)

    def _open_position(self, side: OrderSide) -> None:
        quantity = Quantity(float(self.trade_size), self.instrument.size_precision)
        order = self.order_factory.market(
            instrument_id=self.instrument_id,
            order_side=side,
            quantity=quantity,
        )
        self.submit_order(order)
        self._position_side = side

    def _close_position(self) -> None:
        if self._position_side is None:
            return
        for pos in self.cache.positions_open(instrument_id=self.instrument_id):
            close_side = OrderSide.SELL if pos.side.name == "LONG" else OrderSide.BUY
            order = self.order_factory.market(
                instrument_id=self.instrument_id,
                order_side=close_side,
                quantity=pos.quantity,
            )
            self.submit_order(order)
        self._position_side = None

    def on_stop(self) -> None:
        self._close_position()
```

The strategy above is identical whether you run it in a backtest or live — the only difference is which engine you wire it into.

---

## Backtesting

Backtesting requires two steps: download historical bar data from MT5, then run the backtest engine against it.

### Step 1 — download historical data

```bash
python examples/download_historical_data.py
```

This downloads H1 bars for the configured symbol through the MT5 server's [history routes](packages/server/mt5connector/server/README.md#history), so it runs against the MT5 server at `MT5_SERVER_URL` (`http://127.0.0.1:5000` by default), and writes them into a NautilusTrader Parquet catalog at `./catalog`.

The downloader walks back from `end`, one request per window, until `start` or the floor `/history/ranges` advertises for the series — read before the walk, again after a window answers no rows, and once the walk ends — and records that floor in its result when it lies inside the range:

- bars by the windows the terminal answers in one read, `maxbars − 11` periods, each bar stamped at its close — the range names the closes — and typed by the price its symbol's chart mode names: BID or LAST, LAST where the definition states none;
- ticks one UTC day at a time, both sizes of each the instrument's largest order.

You can customise the download by editing the script, or call the downloader directly:

```python
from mt5connector.client.config import MT5Config
from mt5connector.client.connection import MT5Connection
from mt5connector.client.providers import MT5InstrumentProvider
from mt5connector.client.downloader import MT5DataDownloader
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from datetime import datetime, timezone

config = MT5Config(
    account=12345678, password="your_password",
    server="Exness-MT5Trial9", symbols=["EURUSDm"],
    server_url="http://127.0.0.1:5000",
)

conn     = MT5Connection(config)
conn.connect()

provider = MT5InstrumentProvider(conn)
catalog  = ParquetDataCatalog("./catalog")

# Write the instrument definition first (required by the backtest engine)
instrument = provider.load_symbol("EURUSDm")
catalog.write_data([instrument])

# Download bars
downloader = MT5DataDownloader(conn, provider, catalog)
result = downloader.download_bars(
    symbol    = "EURUSDm",
    start     = datetime(2024, 1,  1, tzinfo=timezone.utc),
    end       = datetime(2024, 12, 31, tzinfo=timezone.utc),
    timeframe = 16385,  # MT5 timeframe constant: 16385 = H1
)
print(result)
conn.disconnect()
```

**MT5 timeframe constants:**

| Timeframe | Constant |
|-----------|----------|
| M1  | 1 |
| M5  | 5 |
| M15 | 15 |
| M30 | 30 |
| H1  | 16385 |
| H4  | 16388 |
| D1  | 16408 |
| W1  | 32769 |

### Step 2 — run the backtest

```bash
python examples/backtest_eurusd.py
```

Or wire it up yourself:

```python
from decimal import Decimal
from datetime import datetime, timezone
from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.backtest.models import FillModel
from nautilus_trader.config import BacktestEngineConfig, LoggingConfig
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.enums import AccountType, OmsType
from nautilus_trader.model.identifiers import Venue, TraderId
from nautilus_trader.model.objects import Money
from nautilus_trader.persistence.catalog import ParquetDataCatalog

SYMBOL   = "EURUSDm"
VENUE    = "MT5"
CATALOG  = "./catalog"

catalog     = ParquetDataCatalog(CATALOG)
instruments = catalog.instruments()
instrument  = next(i for i in instruments if i.id.symbol.value == SYMBOL)

# Load bars from catalog
bars = catalog.bars([f"{SYMBOL}.{VENUE}"])

engine = BacktestEngine(
    config=BacktestEngineConfig(
        trader_id=TraderId("BACKTESTER-001"),
        logging=LoggingConfig(log_level="WARNING"),
    )
)

engine.add_venue(
    venue             = Venue(VENUE),
    oms_type          = OmsType.NETTING,
    account_type      = AccountType.MARGIN,
    base_currency     = USD,
    starting_balances = [Money(10_000.0, USD)],
    fill_model        = FillModel(
        prob_fill_on_limit=0.95,
        prob_slippage=0.10,
        random_seed=42,
    ),
)
engine.add_instrument(instrument)
engine.add_data(bars)

strategy = SmaCrossStrategy(
    config=SmaCrossConfig(
        instrument_id = f"{SYMBOL}.{VENUE}",
        bar_type      = f"{SYMBOL}.{VENUE}-1-HOUR-LAST-INTERNAL",
        fast_period   = 10,
        slow_period   = 30,
        trade_size    = Decimal("0.10"),
    )
)
engine.add_strategy(strategy)
engine.run(
    start = datetime(2024, 1,  1, tzinfo=timezone.utc),
    end   = datetime(2024, 12, 31, tzinfo=timezone.utc),
)

# Results
account = engine.trader.generate_account_report(Venue(VENUE))
fills   = engine.trader.generate_order_fills_report()
print(account)
print(f"Total fills: {len(fills)}")
engine.dispose()
```

---

## Live trading

Live trading uses NautilusTrader's `TradingNode` with the MT5 data and execution clients.

```python
import os, signal, sys
from decimal import Decimal
from nautilus_trader.live.node import TradingNode
from mt5connector.client.config import MT5Config
from mt5connector.client.factories import (
    build_mt5_node_config,
    MT5LiveDataClientFactory,
    MT5LiveExecClientFactory,
)

# 1. Configure MT5
mt5_config = MT5Config(
    account  = int(os.environ["MT5_ACCOUNT"]),
    password = os.environ["MT5_PASSWORD"],
    server   = os.environ["MT5_SERVER"],
    symbols  = os.environ["MT5_SYMBOLS"].split(","),
    server_url = os.environ["MT5_SERVER_URL"],
)

# 2. Configure strategy
symbol        = mt5_config.symbols[0]
instrument_id = f"{symbol}.MT5"
bar_type      = f"{instrument_id}-1-MINUTE-LAST-INTERNAL"

strategy_config = SmaCrossConfig(
    instrument_id = instrument_id,
    bar_type      = bar_type,
    fast_period   = 10,
    slow_period   = 30,
    trade_size    = Decimal("0.01"),
)

# 3. Build and run the node
node_config = build_mt5_node_config(mt5_config=mt5_config)
node        = TradingNode(config=node_config)

# 4. Register factories (must be before node.build())
node.add_data_client_factory("MT5", MT5LiveDataClientFactory)
node.add_exec_client_factory("MT5", MT5LiveExecClientFactory)

# 5. Add strategy
node.trader.add_strategy(SmaCrossStrategy(config=strategy_config))

# 6. Graceful shutdown on Ctrl+C
def _shutdown(sig, frame):
    node.stop()
    sys.exit(0)

signal.signal(signal.SIGINT,  _shutdown)
signal.signal(signal.SIGTERM, _shutdown)

# 7. Start
node.build()  # connects to MT5, loads instruments
node.run()    # starts the push channel and the strategy
```

The node lifecycle in order — **sequence matters:**

```
TradingNode(config)                    # 1. init kernel and engines
node.add_data_client_factory(...)      # 2. register MT5 data factory
node.add_exec_client_factory(...)      # 2. register MT5 exec factory
node.trader.add_strategy(instance)     # 3. register strategy instance
node.build()                           # 4. connect to MT5, load instruments
node.run()                             # 5. start the push channel and strategy
```

### Bar types for live trading

A bar type aggregated `EXTERNAL` is the venue's own bar: the terminal's bar of that timeframe — 1, 2, 3, 4, 5, 6, 10, 12, 15, 20 or 30 minutes, 1, 2, 3, 4, 6, 8 or 12 hours, a day or a week — pushed when it closes and stamped at its close, its prices and tick volume the venue's. A step the terminal has no timeframe for is refused at subscription and at a history request. `"EURUSDm.MT5-5-MINUTE-BID-EXTERNAL"` is the venue's 5-minute bar.

Any other bar type NautilusTrader aggregates from the ticks itself, and never carries the venue's bar type. The bar type string format is:

```
{symbol}.{venue}-{step}-{aggregation}-{price_type}-{aggregation_source}
```

Common examples:

```python
"EURUSDm.MT5-1-MINUTE-LAST-INTERNAL"    # 1-minute bars
"EURUSDm.MT5-5-MINUTE-LAST-INTERNAL"    # 5-minute bars
"EURUSDm.MT5-1-HOUR-LAST-INTERNAL"      # 1-hour bars
"EURUSDm.MT5-100-TICK-LAST-INTERNAL"    # 100-tick bars
"EURUSDm.MT5-1000-VOLUME-LAST-INTERNAL" # volume bars
```

---

## The MT5 server

The adapter runs against `mt5-connector-server`: an HTTP server under the terminal's Windows Python that mirrors the `MetaTrader5` package with every epoch in true UTC, a WebSocket hub that carries what the EAs inside the terminal publish — one per symbol on its own chart, which opens when a consumer first asks for the symbol and closes once it is out of use: its ticks, closed bars and trade transactions — and that EA's source. Its HTTP API, its history protocol, its hub and everything the image running it must provide are in [its README](packages/server/mt5connector/server/README.md).

```
┌─ your bot (any OS) ────────────────┐      ┌─ beside the MT5 terminal ──────┐
│  mt5-connector-client              │      │  HTTP API    :5000             │
│   └─ PushClient                    │──────│  WS push hub :9000             │
│      (ticks, bars, transactions)   │      │  MT5 terminal                  │
└────────────────────────────────────┘      └────────────────────────────────┘
```

- `MT5Config` requires the server's `server_url` (HTTP) and derives its `ws_url` (WebSocket) on port 9000 unless one is given — see `packages/client/mt5connector/client/config.py`.
- The adapter calls the server through the shim `mt5connector.client.remote_mt5`, which `MT5Connection.connect()` binds to `server_url`. A connect on a connection already connected or connecting, and a disconnect on one not connected, raise `MT5ConnectionError` naming its state; used as a context manager, a connection connects on entry and disconnects on exit.
- Each client consumes the hub's pushes through `mt5connector.client.push`, on NautilusTrader's own `WebSocketClient`, which reconnects with backoff; on each reconnect the client subscribes everything it wants again.
  - The data client subscribes a symbol's ticks while NautilusTrader subscribes its quotes or its mark prices: each tick is a `QuoteTick`, both sizes the instrument's largest order since the venue publishes no depth, and a `MarkPriceUpdate` at its mid. It subscribes an `EXTERNAL` bar type's series and hands NautilusTrader each closed venue bar. Its history requests answer quote ticks sized the same way, and the venue's bars as the bar type requested. After a reconnect it holds the bars pushed until the first one arrives, then reads back over HTTP, once, the bars that closed between the last one it handed and that one, and hands them in order before the held ones.
  - The execution client subscribes the account's trade transactions: a `DEAL_ADD` is a fill, read from the venue's history by its ticket; an `ORDER_DELETE` or `HISTORY_ADD` ends the order the venue cancelled, expired or rejected; a `TRADE_TRANSACTION_REQUEST` links the ticket of an order whose submit got no answer through its comment's digest, and accepts it. A transaction naming a ticket the client cannot resolve is left to NautilusTrader's reconciliation, never booked as an external order.
  - Each symbol's EA publishes that symbol's transactions, so they are pushed while its chart is open: while a consumer subscribes to the symbol or the account holds open positions or pending orders on it, and for `MT5_CHART_IDLE_SECONDS` after the server last read it. The client names the symbol on every request it sends, cancels and modifies included, so a request's transaction travels through its symbol's EA.
- The execution client polls nothing for events: what the push channel misses — a disconnection, a ticket it could not resolve, a symbol whose chart is closed — NautilusTrader's own reconciliation heals, through its in-flight check and, once a node sets `open_check_interval_secs` and `position_check_interval_secs`, its open-order and position checks. Its one loop checks the terminal session and reports the account.
- There is no authentication: the server trusts its network, so its ports stay on loopback and are never exposed to an untrusted one.

The shim raises `ServerUnreachable` when the server cannot be reached or answers outside its contract, and when it refuses a call while it is not ready — HTTP 503 with no `last_error`, the terminal never asked — naming the function and the server's message. It raises `ServerBusy`, naming the function and carrying the delay the server's `Retry-After` gives as `retry_after_s`, when the server refuses a call with every slot taken: the server is up, and the caller decides whether to ask again. Every call sets the shim's `last_error()` to the pair its answer carries, and a failed call returns the package's failure value, as the package does.

`mt5connector.client.history` calls the server's history routes through the shim's session, with no read timeout:

- `bars()` and `ticks()` answer the rows as the package's arrays, and `ranges()` the advertised floors and `maxbars`.
- On a 503 — the window not proven yet, or every slot taken — each waits its `Retry-After` and asks again, with no deadline of its own, until the server answers otherwise or the `cancel` event it was handed is set.
- They answer `None` for a failure, a 400 among them, and for a cancellation, with `last_error()` set to the pair the server last answered.

---

## Releases

- `VERSION` holds the one version both distributions carry, `0.4.0+st.N`.
- A `v0.4.0+st.N` tag builds both wheels, each `py3-none-any` with a sha256 sidecar, attaches all four files to one GitHub release, and rebuilds the PEP 503 index on this repository's GitHub Pages, one page per project: `/simple/mt5-connector-client/` and `/simple/mt5-connector-server/`.
- Consumers pin a release exactly, by its full local version, from that index; PyPI can never satisfy a `+st` version, so the pin proves provenance.

---

## Running the full test suite

```bash
just test
```

All tests mock the MT5 terminal — no live connection required to run tests.

```
tests/client/              — the adapter: connection, data and execution clients, instruments, factories, the shim, the history client and the downloader
tests/server/              — the server's routes, lifecycle, WS hub and history protocol, and the shim through them
tests/conformance/         — the inventory against the pinned MetaTrader5 wheel
tests/test_distributions.py — the two distributions' shared version and the import boundary between them
```

The conformance suite's static tier downloads the pinned `MetaTrader5` wheel once into pytest's cache and checks it by sha256; set `MT5_WHEEL_PATH` to a local copy to run without network. Its live tier runs on a Windows host with a terminal and the pinned `MetaTrader5` package installed when `MT5_LIVE_CONFORMANCE=1`.

---

## Project structure

```
mt5-connector/
├── VERSION                       # the one version both distributions carry
├── packages/
│   ├── client/                   # mt5-connector-client
│   │   ├── pyproject.toml
│   │   └── mt5connector/
│   │       ├── client/
│   │       │   ├── commissions.py   # the relayed commission rule and the taker fee it implies
│   │       │   ├── config.py        # MT5Config — all user-facing configuration
│   │       │   ├── connection.py    # MT5Connection — server connection lifecycle
│   │       │   ├── constants.py     # venue, account-loop and reconnect defaults
│   │       │   ├── currencies.py    # the precision ladder a venue currency code is built by
│   │       │   ├── data.py          # MT5DataClient — pushed ticks, marks and venue bars, and history requests
│   │       │   ├── downloader.py    # MT5DataDownloader — historical bar download
│   │       │   ├── errors.py        # custom exceptions
│   │       │   ├── execution.py     # MT5LiveExecutionClient — orders, their events, and reconciliation reports
│   │       │   ├── factories.py     # LiveDataClientFactory + LiveExecClientFactory wiring
│   │       │   ├── history.py       # the server's history routes, as a client
│   │       │   ├── parsing.py       # symbol_info → NautilusTrader Instrument conversion
│   │       │   ├── providers.py     # MT5InstrumentProvider
│   │       │   ├── push.py          # the hub's push channel on NT's WebSocketClient
│   │       │   └── remote_mt5.py    # the HTTP shim generated from the inventory
│   │       └── wire/
│   │           ├── mirror.py        # the inventory of the pinned MetaTrader5 package
│   │           ├── broker_clock.py  # broker wall-clock time ↔ true UTC
│   │           ├── history_wire.py  # the history routes' paths, series and answer states
│   │           └── push_wire.py     # the push protocol's version, frames, roles and streams
│   └── server/                   # mt5-connector-server
│       ├── pyproject.toml
│       └── mt5connector/
│           └── server/
│               ├── README.md        # the server's API and what its image must provide
│               ├── app.py           # the HTTP server (mt5-connector-server)
│               ├── ws_server.py     # the push hub (mt5-connector-hub)
│               ├── push_frames.py   # the EA's frames, held to their structs and converted to UTC
│               ├── wire -> ../../../client/mt5connector/wire
│               └── mql5/            # the EA, its startup script and their includes
├── tests/                        # full test suite (no live MT5 required)
└── examples/
    ├── live_remote.py               # stream live ticks through the server
    ├── backtest_eurusd.py           # SMA crossover backtest
    ├── download_historical_data.py  # download bars from MT5
    └── test_place_order.py          # verify execution path end-to-end
```

---

## Broker compatibility

The adapter works with any MT5 broker. The key difference between brokers is the symbol naming convention and the server name format.

| Broker | Server format | Symbol format |
|--------|--------------|---------------|
| Exness standard | `Exness-MT5Trial9` (demo) / `Exness-MT5Real8` (live) | `EURUSDm`, `XAUUSDm` |
| Exness zero/raw | `Exness-MT5Real8` | `EURUSD`, `XAUUSD` |
| IC Markets | `ICMarketsSC-Demo` | `EURUSD`, `XAUUSD` |
| Pepperstone | `Pepperstone-Demo` | `EURUSD`, `XAUUSD` |
| OANDA | `OANDA-OANDATrade-1` | `EUR_USD` |

Find your exact server name in MT5 → File → Open Account → search your broker name.

---

## Troubleshooting

**`mt5.initialize() failed — error -6: Terminal: Authorization failed`**

The server's MT5 terminal is not running, or is not logged in. Check the server's log and its terminal (see [The MT5 server](#the-mt5-server)), wait for the green connection indicator in the terminal's bottom-right corner, then run the script again.

**`mt5.login() failed — error -6: Terminal: Authorization failed`**

Wrong account number, password, or server name. Double-check all three against your broker's welcome email or the MT5 terminal itself (the account number is shown in the top-left of the terminal).

**An order rejected with `TRADE_RETCODE_CLIENT_DISABLES_AT: AutoTrading disabled by client`**

AutoTrading is disabled in the server's MT5 terminal. Click the **AutoTrading** button in the toolbar — it should turn green. This must be enabled for any automated order to be sent.

**`Factory was not of type LiveExecClientFactory`**

You are using an old version of `factories.py` where `MT5LiveExecClientFactory` did not inherit from `LiveExecClientFactory`. Update to the latest version.

**Strategy not placing trades after 30+ minutes**

Check that the bar type string in your strategy config exactly matches the bar type you subscribed to in `on_start`. A mismatch means `on_bar` is never called. Also verify AutoTrading is enabled in the server's MT5 terminal.

---

## Safety notes

- Always use a **demo account** until you have verified your strategy behaves correctly.
- Every order the adapter sends carries a magic derived from the node's trader id (the first 8 bytes of its SHA-256, masked to 63 bits). The execution client tracks only the orders, positions and deals carrying its own magic, and the stop-loss and take-profit deals of the positions it holds exits for, so manual trading on the same account, or a node with another trader id, is left alone.
- The adapter runs on hedging accounts only: it declares NT's `HEDGING` position model and refuses to connect to an account whose margin mode is netting or exchange, or to a read-only (investor) session.
- The execution account id is `MT5-<login>`, the login read from the account at connect.
- An order's comment at the venue is the first 29 hex digits of its client order id's SHA-256. Venue tickets map to NT orders through NT's own order records, and through that digest for an order whose submit got no answer; a deal or order neither explains is logged and left to NT's reconciliation.
- Order lists, post-only and trailing orders, and times in force other than GTC and GTD are rejected before anything is sent.
- A reduce-only order is an exit of the position its submit names (NT's position id, the venue's position identifier), translated into what the venue holds for one, which is one stop loss and one take profit per position:
  - A stop-market order becomes the position's stop loss and a limit order its take profit. The order's venue order id is synthetic, `<identifier>-SL-<n>` or `<identifier>-TP-<n>`; a later order of the same kind moves the bracket and cancels the order it took the place of.
  - A cancel clears the bracket, and a modify moves it; the only quantity a modify takes is the order's fills plus the position's volume, which changes nothing at the venue.
  - The venue answers a request setting a bracket to what it already holds with "no changes" (10025), which confirms the setting: a stop or target sent again to the level the venue holds is accepted like any other.
  - A market order closes the position by its current ticket. A partial close is refused while a target stands, since the venue's take profit covers the whole position.
  - A stop loss or take profit that fires is a fill of the order holding that bracket, and a stop-out a fill of the stop. The order first takes the ticket of the order the venue executed the bracket with as its venue order id, and each fill reports under the ticket of its execution.
  - A bracket can execute in parts, each part an execution of its own: the order keeps every execution ticket it took, across a restart too, and a later deal of one of them fills that order even after another order took the bracket.
  - An exit order reports once, under the ticket it took last, however many executions the venue records for it, and always as the exit: its own type, quantity and level. Once its bracket no longer holds it, the report carries the volume its executions filled at their average price, and reads filled when they filled the order, canceled when they did not.
  - A pending order bound to a position, a reduce-only stop-limit order, and an exit that expires are rejected before anything is sent: the venue ignores a pending order's position, and its fill would open a new one.
- A submit, modify or cancel the venue may have acted on without the client learning it — `order_send` answering 10031, a read timeout, a connection closed after the request arrived — emits no event and logs a warning; the transactions the venue pushes for it or NT's reconciliation settle the order. A request that never left the client is rejected at once as `not sent`.
- Fills come from the venue's deals alone, pushed as they happen, once per deal; deals already in the history when the client connects are never emitted, except a stop loss's or take profit's deal no exit order in NT's cache has booked while an exit holds its execution or occupies that bracket. Connect emits those before it returns, and fails naming the deal when it cannot build one, so the next start tries again.
- Past backtest performance does not guarantee live performance. Spreads, slippage, and execution latency differ between backtest and live environments.

---



Pull requests are welcome. Run the test suite before submitting:

```bash
pytest tests/ -v
```

New features should include tests. The test suite mocks the MT5 terminal so no live account is needed to contribute.

---

## License

MIT — see [LICENSE](LICENSE) for details.

---

*This project is not affiliated with, endorsed by, or supported by Nautech Systems Pty Ltd or the NautilusTrader project.*
