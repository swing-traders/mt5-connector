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
MT5 server (Docker) ←→ mt5-connector ←→ NautilusTrader
                              ↑
                    tick stream, order routing,
                    account state, reconciliation
```

**What you get:**

- Live tick data streamed from the MT5 server over WebSocket, aggregated into any bar type NautilusTrader supports
- Full order lifecycle: market, limit, stop, stop-limit orders with SL/TP
- Account state and position reconciliation on startup and continuously
- Historical bar data download into a NautilusTrader Parquet catalog for backtesting
- Automatic reconnection with exponential backoff
- Works with any MT5 broker — Exness, IC Markets, Pepperstone, OANDA, and more

> **Platform note:** The adapter talks only to the MT5 server — the MT5 terminal in a Docker container (see [Dockerized MT5 server](#dockerized-mt5-server)) — over HTTP and WebSocket, so it needs no `MetaTrader5` package and runs on any platform. Backtesting with downloaded data works on any platform once the data has been collected.

---

## Table of contents

- [mt5-connector](#mt5-connector)
  - [What this is](#what-this-is)
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
  - [Dockerized MT5 server](#dockerized-mt5-server)
    - [What it is](#what-it-is)
    - [Important Security Notice](#important-security-notice)
    - [Requirements](#requirements-1)
      - [Quick start](#quick-start-1)
    - [Persistence](#persistence)
  - [Running the full test suite](#running-the-full-test-suite)
  - [Project structure](#project-structure)
  - [Broker compatibility](#broker-compatibility)
  - [Troubleshooting](#troubleshooting)
  - [Safety notes](#safety-notes)
  - [License](#license)

---

## Requirements

- Python 3.11+
- The MT5 server under `mt5server/` running in Docker on Linux (see [Dockerized MT5 server](#dockerized-mt5-server)); it runs the MetaTrader 5 terminal and logs it in to your broker account
- An MT5 broker account (demo accounts work perfectly for development)

---

## Installation

Releases are published as wheels on the fork's package index:

```bash
pip install --extra-index-url https://swing-traders.github.io/mt5-connector/simple/ "mt5-connector==0.4.0+st.1"
```

For development, create the environment with mamba and layer the dev tooling on top:

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

**1. Create a `.env` file** in your project root with your broker credentials:

```bash
# .env — never commit this file
MT5_ACCOUNT=12345678
MT5_PASSWORD=your_password
MT5_SERVER=Exness-MT5Trial9
MT5_SYMBOLS=EURUSDm,XAUUSDm
MT5_SERVER_URL=http://127.0.0.1:5000
```

Find your server name in MT5 → File → Open Account → search your broker.

**2. Start the MT5 server.** The adapter talks to the MT5 terminal in the server's container — see [Dockerized MT5 server](#dockerized-mt5-server). The server must be up before you run any script.

**3. Enable AutoTrading** in the container's MT5 terminal (the button should show a green dot). Without this, order_send calls will be rejected.

**4. Test the connection:**

```python
from mt5connect import remote_mt5 as mt5

mt5.configure("http://127.0.0.1:5000")
print(mt5.account_info())
```

**5. Run the example live strategy:**

```bash
python examples/live_remote.py
```

---

## Configuration

All configuration goes through `MT5Config`. The required fields are your account credentials, symbols, and the MT5 server's URL; a config without `server_url` is refused when it is built.

```python
from mt5connect.config import MT5Config

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

    # The WebSocket tick hub (default: derived from server_url, on port 9000)
    ws_url = "ws://127.0.0.1:9000",

    # Order/position polling interval
    exec_poll_interval_ms = 250,   # default: 250ms

    # Order tagging — change if running multiple bots simultaneously
    magic_number = 510,

    # Reconnection
    reconnect_initial_delay_s = 1.0,
    reconnect_max_delay_s     = 60.0,
    reconnect_max_attempts    = 20,

    # Connection timeout
    timeout_s = 10.0,
)
```

**Loading from `.env`** (recommended — never hardcode credentials):

```python
import os
from pathlib import Path
from dotenv import load_dotenv
from mt5connect.config import MT5Config

load_dotenv(Path(__file__).parent / ".env")

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

The adapter handles suffix normalisation internally for instrument classification — you just provide the exact broker symbol name.

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

This downloads H1 bars for the configured symbol through the MT5 server's [history routes](#history), so it runs against the MT5 server at `MT5_SERVER_URL` (`http://127.0.0.1:5000` by default), and writes them into a NautilusTrader Parquet catalog at `./catalog`.

The downloader walks back from `end`, one request per window, until `start` or the floor `/history/ranges` advertises for the series — read before the walk, again after a window answers no rows, and once the walk ends — and records that floor in its result when it lies inside the range:

- bars by the windows the terminal answers in one read, `maxbars − 11` periods, each bar stamped at its close — the range names the closes;
- ticks one UTC day at a time.

You can customise the download by editing the script, or call the downloader directly:

```python
from mt5connect.config import MT5Config
from mt5connect.connection import MT5Connection
from mt5connect.providers import MT5InstrumentProvider
from mt5connect.downloader import MT5DataDownloader
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
from pathlib import Path
from dotenv import load_dotenv
from nautilus_trader.live.node import TradingNode
from mt5connect.config import MT5Config
from mt5connect.factories import (
    build_mt5_node_config,
    MT5LiveDataClientFactory,
    MT5LiveExecClientFactory,
)

load_dotenv(Path(__file__).parent / ".env")

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
node.run()    # starts the tick stream, execution polling and strategy
```

The node lifecycle in order — **sequence matters:**

```
TradingNode(config)                    # 1. init kernel and engines
node.add_data_client_factory(...)      # 2. register MT5 data factory
node.add_exec_client_factory(...)      # 2. register MT5 exec factory
node.trader.add_strategy(instance)     # 3. register strategy instance
node.build()                           # 4. connect to MT5, load instruments
node.run()                             # 5. start the tick stream and strategy
```

### Bar types for live trading

NautilusTrader aggregates ticks into bars internally. The bar type string format is:

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

## Dockerized MT5 server

The adapter runs against a **Dockerized MT5 server**. The server container runs MT5 under Wine, exposes an HTTP API mirroring the `MetaTrader5` package (`mt5server/app`) plus a WebSocket tick hub, and runs a small MQL5 EA that publishes live ticks and the trade server's time. The adapter then works on any machine.

```
┌─ your bot (any OS) ────────────────┐      ┌─ MT5 server container ────────┐
│  mt5-connector                     │      │  HTTP API    :5000             │
│   └─ WSStreamClient                │──────│  WS tick hub :9000             │
│      (subscribe/tick messages)     │      │  MT5 terminal (Wine)           │
└────────────────────────────────────┘      └────────────────────────────────┘
```

### What it is

- A required `server_url` (HTTP) on `MT5Config` and a derived `ws_url` (WebSocket) — see `mt5connect/config.py`.
- The Docker image builds MT5 + Wine + the HTTP API + the WS hub in one container (`mt5server/Dockerfile`), with the EA provisioned automatically.
- The adapter calls the server through the shim `mt5connect/remote_mt5.py`, which `MT5Connection.connect()` binds to `server_url`, and streams ticks through `mt5connect/ws_stream.py`.

### HTTP API

The server mirrors the `MetaTrader5` package (5.0.6231), so code written against `import MetaTrader5 as mt5` runs unchanged against `mt5connect.remote_mt5`. Both are generated from one inventory of the package, `mt5connect/mirror.py`.

- `POST /mt5/<function>` for each of the package's 32 functions, its arguments a JSON object keyed by parameter name; datetimes travel as true-UTC epoch seconds.
- A package call answers `{"ok": true, "result": ..., "last_error": [code, message]}` or `{"ok": false, "error": {"code": ..., "message": ...}, "last_error": [code, message]}`. `last_error` is the package's `last_error()` read right after the call; a failure's error is that same pair. Structs answer as objects in the package's field order, arrays as lists of objects keyed by the dtype's fields.
- Every epoch is true UTC in both directions. The terminal keeps time on the broker's clock — a zone's wall time plus a fixed offset, New York plus seven hours by default — and the server converts by the era of each instant:
  - every epoch field of an answer, `0` (no time) left as `0`, with bars still stamped at their open;
  - every time window a client sends, and a trade request's `expiration`.
- An epoch in the zone's repeated autumn hour reads as its first occurrence, and the server logs a warning naming the field. One in its skipped spring hour is a server error (HTTP 500).
- Answers are HTTP 200, except a failing `terminal_info`, which is HTTP 503: the terminal's IPC is down. A missing or unknown parameter, a time that is not an integer epoch, or a history query in none of its documented call forms, is refused with HTTP 400 and code -2 (`RES_E_INVALID_PARAMS`) before the package is called.
- `POST /mt5/shutdown` is the one deliberate departure from the package: it answers `None` without calling the package's `shutdown()`. Every client shares the server's terminal session, and the adapter calls `shutdown()` whenever it disconnects, so passing it on would end the session for every client. `initialize` and `login` pass through unchanged.
- `GET /health` answers the broker clock's latest verification — the relayed sample's chart `symbol`, its `trade_server` and `current` (last quote) times in true UTC, the terminal's own `gmt`, and the `skew_s` from the server's clock and the `offset_s` in effect — beside the server's load: the calls that can reach the terminal `in_flight` now, the `peak_in_flight` and the `refusals` since start, and its `workers`. While the clock is not verified it answers HTTP 503. It never calls the terminal: the server initialized it at start, and the EA's samples say whether it is connected to the trade server.
- `POST /relay/server_time` takes the `server_time` frame the WS hub relays from the EA and answers `{"ok": true, "result": null}`. It accepts a frame from `127.0.0.1` only (HTTP 403 otherwise), and refuses anything but the frame's exact shape with HTTP 400 and code -2.
- `GET /commissions/<symbol>` answers the commission schedule the terminal's EA relayed for the symbol, or code -4 (`RES_E_NOT_FOUND`) while none has been relayed. No package call answers it, so its envelope carries no `last_error`.
- `POST /history/bars` and `POST /history/ticks` answer a history window the server vouches for (see [History](#history)), and `GET /history/ranges?symbol=<symbol>` the floors it has measured.
- While the broker clock is not verified, every route but `/health` and `/relay/server_time` answers HTTP 503 with `{"ok": false, "error": {"code": -1, "message": "the broker clock is not verified"}}` and calls nothing.
- Package calls run one at a time; waitress serves the API with `MT5_API_THREADS` workers, one of them always kept free for `/health` and `/relay/server_time`.
- Every route that can reach the terminal — `/mt5/<function>` and the three history routes — takes one of `MT5_API_THREADS − 1` slots. A call that finds every slot taken is refused at once, never queued on a worker: HTTP 503 with the failure envelope, code -20002 naming the route, and `Retry-After: <MT5_HISTORY_RETRY_SECONDS>`. Each refusal is logged as a warning. The clock check answers first, so while the clock is not verified such a call gets its 503 and takes no slot.

The shim raises `ServerUnreachable` when the server cannot be reached or answers outside this contract, and when it refuses a call while it is not ready — HTTP 503 with no `last_error`, the terminal never asked — naming the function and the server's message. It raises `ServerBusy`, naming the function, when the server refuses a call with every slot taken: the server is up, and the caller decides whether to ask again. Every call sets the shim's `last_error()` to the pair its answer carries, and a failed call returns the package's failure value, as the package does.

The server reads its settings from the environment once at start, initializes the terminal with them, and exits when that fails:

| Variable | Setting | Default |
|---|---|---|
| `MT5_TERMINAL_PATH` | the terminal executable, as Windows names it | set by the image |
| `MT5_LOGIN` | account number | required |
| `MT5_PASSWORD` | account password | required |
| `MT5_SERVER` | trade server name | required |
| `MT5_LOGIN_TIMEOUT_MS` | initialize timeout | `60000` |
| `MT5_API_HOST` | bind address | `0.0.0.0` |
| `MT5_API_PORT` | HTTP port | `5000` |
| `MT5_API_THREADS` | waitress threads, one of them kept free for `/health` and the relay; at least 2 | `5` |
| `MT5_CLOCK_CHECK_SECONDS` | interval between re-verifications of the broker's clock | `300` |
| `MT5_CLOCK_SAMPLE_MAX_AGE_SECONDS` | how long a relayed server-time sample stays fresh | `30` |
| `MT5_CLOCK_BOOTSTRAP_SECONDS` | how long the server waits at start for the first fresh sample | `120` |
| `MT5_BROKER_TZ` | the zone the broker's clock follows, as an IANA name | `America/New_York` |
| `MT5_BROKER_OFFSET_HOURS` | hours the broker's clock runs ahead of that zone | `7` |
| `MT5_HISTORY_RETRY_SECONDS` | the `Retry-After` of a history answer the terminal has not proven yet, and of a call refused with every slot taken: how long the client waits before asking again | `5` |
| `MT5_FLOOR_TTL_SECONDS` | how long a measured history floor is used before it is measured again | `900` |

Once the terminal is initialized, the server verifies the broker's clock against the trade server's time the EA relays through the WS hub, at any hour, market open or closed:

- A sample is fresh while it is younger than `MT5_CLOCK_SAMPLE_MAX_AGE_SECONDS` and says the terminal is connected to the trade server.
- A fresh sample's trade-server time, converted to true UTC, must sit within 120 s of the server's clock, or the server exits with both times and the offset in its log. The terminal extrapolates the trade server's time from the clock it shares with the server, so the comparison checks the offset the terminal learned from the trade server against the broker clock's schedule, whatever the container's clock reads.
- At start the server waits up to `MT5_CLOCK_BOOTSTRAP_SECONDS` for the first fresh sample, and exits when none arrives. It then re-verifies the latest sample every `MT5_CLOCK_CHECK_SECONDS`.
- The moment no sample is fresh the clock is unverified, and stays so until a fresh sample verifies it again. Each change is logged once.

### History

The terminal answers a history request with whatever it has synced, substitutes the nearest data it has, and answers a range it cannot serve with the same emptiness as one that has no data. The history routes answer a window only once the terminal's answers prove it, so a client never receives emptiness it cannot trust.

- `POST /history/bars` takes `{"symbol", "timeframe", "start", "end"}`, the timeframe by its MT5 name from `M1` to `W1` (a month has no fixed period, so `MN1` is refused), and `start` and `end` the bars' opens in true-UTC epoch seconds, inclusive at both ends.
- `POST /history/ticks` takes `{"symbol", "start", "end", "flags"}`: the ticks from the start of second `start` through the end of second `end`, true UTC, and `flags` the selection by its `COPY_TICKS_*` name — `ALL`, `INFO` or `TRADE`, `INFO` when absent.

| HTTP | Answer | When |
|---|---|---|
| 200 | the mirror's envelope, its `result` the window's rows as the mirror answers them, bars stamped at their open | the terminal's answers prove the rows; an empty list is an answer like any other |
| 503 | the failure envelope with code -20001, naming the symbol, the series and the window, and `Retry-After: <MT5_HISTORY_RETRY_SECONDS>` | the terminal's answers do not prove the window yet: ask again after the delay |
| 503 | the failure envelope with code -20002, naming the route, and `Retry-After: <MT5_HISTORY_RETRY_SECONDS>` | every slot for a call that can reach the terminal is taken: ask again after the delay |
| 400 | the failure envelope with code -2 | a body out of shape, or a window that starts after the terminal's current time: the trade-server time the EA last relayed, plus the time since it arrived |

A 200 carries the failure envelope instead when the terminal cannot select the symbol, or when the rows it answers break the checks below.

How the server proves a window, within the request and without waiting:

- It selects the symbol and reads the terminal's `maxbars`. The terminal answers a range spanning more than `maxbars − 11` periods with nothing, so a longer bar window is read in the fewest equal spans within that.
- A read the terminal answers with rows and a success `last_error` is proven: the rows are the answer. A read it fails, with no answer or with an error, is a 503.
- A read that answers no rows is proven empty by any one of these facts, each read from the terminal, and is a 503 without one:
  - **Before the floor**: the window lies wholly before the series' floor. A window that begins before the floor answers its rows from the floor on.
  - **The live edge**: the window lies after the second of the symbol's last quote, read with `symbol_info_tick`. A last quote at time zero dates nothing, and a window holding the last quote's second is a 503: that quote is itself a record.
  - **Bracketed**: the terminal has a row before the window and a record after it. Before it: for bars, the last bar the terminal answers as opened at or before the window's start (`copy_rates_from` for one bar); for ticks, the window's UTC day, then its UTC week. After it: the first bar in the span of one read from the window's end, or the first tick from its last second. An empty span of a long bar window between spans that answered rows is bracketed by them.
- Every read asks the terminal to sync what it lacks, and a client's retry after a 503 reads the window again.
- Every bar must lie on its timeframe's grid, or the answer fails with code -1 naming the row. In an answer that begins at the series' floor, a leading run of an intraday series spaced a whole number of days apart, ahead of its first two rows one period apart, is not that timeframe's data: it is dropped and the floor moves past it. An answer that begins anywhere else drops no such run. Ticks must not go back in time.
- A cold read holds the terminal for as long as the package's own call takes. `/health` never waits on the terminal, so liveness does not see it.

A series' floor is set by the stub — the one bar the terminal answers for a window before any plausible history; for ticks, the first tick from then — or by a coarse prefix at the series' start, at the first row past it. It is used for `MT5_FLOOR_TTL_SECONDS`, then measured again, and sooner when a window read from a kept floor answers no rows, since the terminal's own floor may have moved past it. A stub measured again never moves a floor the coarse prefix set back: it replaces it only with a later row.

`GET /history/ranges?symbol=<symbol>` answers `{"maxbars", "ranges": {"<series>": {"floor", "measured_at", "generation"}}}`: every floor measured for the symbol since the server started, keyed by timeframe name and `ticks`, in true UTC with the time it was measured. A floor's `generation` steps each time its value changes.

`mt5connect.history` calls these routes through the shim's session, with no read timeout:

- `bars()` and `ticks()` answer the rows as the package's arrays, and `ranges()` the advertised floors and `maxbars`.
- On a 503 — the window not proven yet, or every slot taken — each waits its `Retry-After` and asks again, with no deadline of its own, until the server answers otherwise or the `cancel` event it was handed is set.
- They answer `None` for a failure, a 400 among them, and for a cancellation, with `last_error()` set to the pair the server last answered.

### WebSocket hub

The hub (`mt5server/app/ws_server.py`, port `WS_PORT`) carries these frames:

| Frame | From | What the hub does |
|---|---|---|
| `{"type": "hello", "role": "ea"}` or `"role": "adapter"` | EA, adapter | records the connection's role |
| a tick — `symbol`, `bid`, `ask`, `time_msec`, `flags` and more, as strings, with no `type` | EA | broadcasts it to every adapter when its bid or ask changed |
| `{"v": 1, "type": "server_time", "symbol", "trade_server", "current", "gmt", "connected"}`, its epochs and `connected` (0 or 1) as integers | EA | POSTs it to the server's `/relay/server_time` on `127.0.0.1:MT5_API_PORT`, never to adapters, and logs a failed post |
| `{"type": "subscribe", "symbols": [...]}` or `"unsubscribe"` | adapter | records the symbols |

A frame the hub cannot handle closes its connection with a logged warning. The EA sends `server_time` every `RelaySeconds` (5 in the chart template), the terminal's `TimeTradeServer()`, `TimeCurrent()` and `TimeGMT()` with `TERMINAL_CONNECTED`, and reconnects to the hub on the same timer, so both go on while the market is closed. The EA answers the hub's keepalive pings each time it ticks or its timer fires.

### Important Security Notice

There is no authentication in the dockerized server. It is intended to be used on the
same machine only for now. Never expose ports to an insecure network. 

### Requirements

- Docker on the host.

#### Quick start

***1 . Build and start Container***

With docker compose: 

```bash
# 0. create an environment file 
cp .env.example .env   # set MT5_ACCOUNT / MT5_PASSWORD / MT5_SERVER

# 1. Build + start the server (the build context is the repo root; MT5_ACCOUNT becomes MT5_LOGIN)
source .env
# environment variables need to be exported in order to be picked up by docker compose
export MT5_ACCOUNT
export MT5_PASSWORD
export MT5_SERVER
export MT5_SYMBOLS
cd mt5server && docker compose up --build -d
``` 

Without docker compose

Without docker-compose, build/run directly:

```bash
cd mt5server 
docker build -t mt5-server -f Dockerfile ..
source ../.env
docker run -d --name mt5-server \
  -p 127.0.0.1:5000:5000 -p 127.0.0.1:9000:9000 -p 127.0.0.1:3001:3001 \
  -e MT5_SYMBOLS="${MT5_SYMBOLS}" \
  -e MT5_SERVER="${MT5_SERVER}" \
  -e MT5_PASSWORD="${MT5_PASSWORD}" \
  -e MT5_LOGIN="${MT5_ACCOUNT}" \
  mt5-server
```

***2. Wait until container is up***

```bash

# 2. Metatrader 5 ist installed and started in a docker container. Give it 1-2 minutes to start
# if you want to track the installation progress run:
docker exec -it  mt5server-mt5server-1 tail -f /var/log/mt5_setup.log

# if you need to see the metatrader ui open https://localhost:3001 in a browser
```

***3. Run the live tick example***
```bash
# 3. in a new terminal
source .env
source .venv/bin/activate
python examples/live_remote.py
```
If everythng works correctly you shoud see ticks streming in after a few seconds like this:

```
2026-08-19T12:20:07.454036271Z [INFO] TRADER-001.Portfolio: Updated AccountState(account_id=MT5-917132, account_type=MARGIN, base_currency=None, is_reported=True, balances=[AccountBalance(total=9_999.93 USD, locked=0.00 USD, free=9_999.93 USD)], margins=[], event_id=c24fbca4-15bb-4105-9eaa-4ebaa3bfb873)
2026-08-19T12:20:07.613214977Z [INFO] TRADER-001.TickPrintStrategy: XAUUSDp.MT5 bid=4368.34 ask=4368.46 @ 1787152807592000000
2026-08-19T12:20:07.633962622Z [INFO] TRADER-001.TickPrintStrategy: XAUUSDp.MT5 bid=4368.37 ask=4368.49 @ 1787152807612000000
2026-08-19T12:20:07.685732285Z [INFO] TRADER-001.TickPrintStrategy: XAUUSDp.MT5 bid=4368.37 ask=4368.47 @ 1787152807664000000
2026-08-19T12:20:07.697749223Z [INFO] TRADER-001.TickPrintStrategy: XAUUSDp.MT5 bid=4368.25 ask=4368.37 @ 1787152807675000000
2026-08-19T12:20:07.710776389Z [INFO] TRADER-001.TickPrintStrategy: EURUSDp.MT5 bid=1.16074 ask=1.16075 @ 1787152807684000000
2026-08-19T12:20:07.725422727Z [INFO] TRADER-001.TickPrintStrategy: XAUUSDp.MT5 bid=4368.34 ask=4368.46 @ 1787152807704000000
2026-08-19T12:20:07.732231519Z [INFO] TRADER-001.TickPrintStrategy: XAUUSDp.MT5 bid=4368.34 ask=4368.43 @ 1787152807713000000
2026-08-19T12:20:07.746709404Z [INFO] TRADER-001.TickPrintStrategy: XAUUSDp.MT5 bid=4368.31 ask=4368.43 @ 1787152807723000000
2026-08-19T12:20:07.767988612Z [INFO] TRADER-001.TickPrintStrategy: EURUSDp.MT5 bid=1.16075 ask=1.16076 @ 1787152807745000000

```


**Tick streaming setup**

Ticks streamking is set up automatically for every symbol in the environment
variable  `MT5_SYMBOLS`.

**Security note**

For now the server *does not provide any authentication and authorization*. That means it should be only used  locally
and never be exposed over an insecure network, as this will *expose the api and your account* to every one who has access
to the network. On public machines this is the whole internet. 

Account credentials live only in your local gitignored `.env`. They are passed to the container via environment variables, used during setup and by the server's `initialize()` at start, and never baked into the Docker image. The shim's `login()` reaches the server as `POST /mt5/login`. The server's `config/` directory (Wine prefix) is a mounted volume owned by the container.

### Persistence

The dockerized metatrader instance is configured at startup automatically and not intended to be used via the regular matatrader GUI.
Therefor it has no volumes for persisting configuration configured. If you need to persist data between container instances, you need
to mount a volume to the containers `/config` path. For example by additonally passing `-v $PWD/config:/config` to the docker command
line, or change the `docker-compose.yaml` file accordingly. 

---

## Running the full test suite

```bash
just test
```

All tests mock the MT5 terminal — no live connection required to run tests.

```
tests/test_connection.py   — MT5Connection lifecycle, reconnect logic
tests/test_data.py         — MT5DataClient subscriptions and history requests
tests/test_data_ws.py      — MT5DataClient's WebSocket tick stream
tests/test_execution.py    — order submission, fills, reconciliation
tests/test_factories.py    — factory wiring and node config
tests/test_parsing.py      — symbol info → NautilusTrader instrument conversion
tests/test_providers.py    — MT5InstrumentProvider loading
tests/test_remote_mt5.py   — the shim's transport and failure classes
tests/server/              — the server's routes, lifecycle, WS hub and history protocol, and the shim through them
tests/conformance/         — the inventory against the pinned MetaTrader5 wheel
```

The conformance suite's static tier downloads the pinned `MetaTrader5` wheel once into pytest's cache and checks it by sha256; set `MT5_WHEEL_PATH` to a local copy to run without network. Its live tier runs on a Windows host with a terminal and the pinned `MetaTrader5` package installed when `MT5_LIVE_CONFORMANCE=1`.

---

## Project structure

```
mt5-connector/
├── mt5connect/
│   ├── config.py        # MT5Config — all user-facing configuration
│   ├── connection.py    # MT5Connection — server connection lifecycle
│   ├── constants.py     # venue, magic number, symbol sets, normalize_symbol()
│   ├── data.py          # MT5DataClient — WebSocket tick stream and history requests
│   ├── downloader.py    # MT5DataDownloader — historical bar download
│   ├── errors.py        # custom exceptions
│   ├── execution.py     # MT5LiveExecutionClient — order submission and fills
│   ├── factories.py     # LiveDataClientFactory + LiveExecClientFactory wiring
│   ├── history.py       # the server's history routes, as a client
│   ├── history_wire.py  # the history routes' paths, series and answer states
│   ├── parsing.py       # symbol_info → NautilusTrader Instrument conversion
│   └── providers.py     # MT5InstrumentProvider
├── tests/               # full test suite (no live MT5 required)
├── examples/
│   ├── live_simple_strategy.py       # full live trading example
│   ├── backtest_eurusd.py            # SMA crossover backtest
│   ├── download_historical_data.py   # download bars from MT5
│   └── test_place_order.py           # verify execution path end-to-end
├── .env.example         # credential template — copy to .env and fill in
└── pyproject.toml
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

The server's MT5 terminal is not running, or is not logged in. Check the container's log and its terminal UI (see [Dockerized MT5 server](#dockerized-mt5-server)), wait for the green connection indicator in the terminal's bottom-right corner, then run the script again.

**`mt5.login() failed — error -6: Terminal: Authorization failed`**

Wrong account number, password, or server name. Double-check all three against your broker's welcome email or the MT5 terminal itself (the account number is shown in the top-left of the terminal).

**`order_send failed — retcode=10027 comment=AutoTrading disabled by client`**

AutoTrading is disabled in the server's MT5 terminal. Click the **AutoTrading** button in the toolbar — it should turn green. This must be enabled for any automated order to be sent.

**`Factory was not of type LiveExecClientFactory`**

You are using an old version of `factories.py` where `MT5LiveExecClientFactory` did not inherit from `LiveExecClientFactory`. Update to the latest version.

**Strategy not placing trades after 30+ minutes**

Check that the bar type string in your strategy config exactly matches the bar type you subscribed to in `on_start`. A mismatch means `on_bar` is never called. Also verify AutoTrading is enabled in the server's MT5 terminal.

---

## Safety notes

- Always use a **demo account** until you have verified your strategy behaves correctly.
- The `magic_number` in `MT5Config` (default: `510`) tags every order placed by the adapter. Orders without this magic number are ignored — safe to trade the same account manually alongside the bot.
- Change `magic_number` if you run multiple bots simultaneously to avoid one bot managing the other's positions.
- The adapter uses netting mode (one position per symbol) matching how MT5 accounts work by default. Hedging accounts are not currently supported.
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
