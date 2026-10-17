# mt5-connector

This is the swing-traders organisation's hard fork of [aulekator/mt5-connector](https://github.com/aulekator/mt5-connector). It is maintained independently and does not track upstream.

**Unofficial community MetaTrader 5 adapter for NautilusTrader** — live trading and backtesting on any MT5 broker (Exness, IC Markets, Pepperstone, and more).

> ⚠️ **Disclaimer:** This is an independent community project. It is **not** affiliated with, endorsed by, or supported by [Nautech Systems Pty Ltd](https://nautilustrader.io) or the official [NautilusTrader](https://nautilustrader.io) project.

[![Python 3.13](https://img.shields.io/badge/python-3.13-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Platform: Linux](https://img.shields.io/badge/platform-Linux-lightgrey.svg)](#requirements)
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

- Live quote ticks, a mark price at the mid of each, and the venue's own closed bars, pushed from the terminal over WebSocket; the ticks aggregate into any bid, ask or mid bar type NautilusTrader supports
- Order lifecycle: market, limit, stop and stop-limit entries, their fills booked to the venue's hedging positions, and each position's exits — its stop, its target and its closes — as the venue's own stop loss, take profit and closing deals; every fill, cancel and expiry pushed from the terminal as it happens
- Account state and position reconciliation on startup and continuously
- Historical bar data download into a NautilusTrader Parquet catalog for backtesting
- Automatic reconnection with exponential backoff
- Works with any MT5 broker — Exness, IC Markets, Pepperstone, OANDA, and more

> **Platform note:** The adapter talks only to the MT5 server — `mt5-connector-server`, running beside the MT5 terminal (see [The MT5 server](#the-mt5-server)) — over HTTP and WebSocket, so it needs no `MetaTrader5` package. It runs on CPython 3.13 on Linux x86_64, where the NautilusTrader build it pins installs.

---

## Distributions

The repository ships two distributions, versioned together: one release builds both wheels onto one index.

| Distribution | Import packages | Installs into | Carries |
|---|---|---|---|
| `mt5-connector-client` | `mt5connector.client`, `mt5connector.wire` | the Python that runs NautilusTrader | the data and execution clients, the instrument provider, the factories, the remote backend, the history client and the downloader, and the wire vocabulary |
| `mt5-connector-server` | `mt5connector.server` | the Linux Python that runs the hub beside the terminal | the hub, the EA's source, and a copy of the wire vocabulary made when its wheel is built |

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
  - [Downloading history](#downloading-history)
  - [Running in a node](#running-in-a-node)
    - [Bar types](#bar-types)
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

- CPython 3.13 on Linux x86_64 where the client runs: the NautilusTrader build it pins exists for Linux x86_64 alone
- An MT5 server (`mt5-connector-server`, see [The MT5 server](#the-mt5-server)) running beside a MetaTrader 5 terminal logged in to your broker account
- An MT5 broker account (demo accounts work perfectly for development)

---

## Installation

Releases are published as wheels on the fork's package index; `<version>` below is a release's version, the `VERSION` file's value at its tag, the latest listed on the [releases page](https://github.com/swing-traders/mt5-connector/releases). The client pins one exact build of the NautilusTrader fork, the `nautilus_trader` pin in `packages/client/pyproject.toml`, which only that fork's index serves. Install the adapter where NautilusTrader runs, from both indexes:

```bash
pip install \
  --extra-index-url https://swing-traders.github.io/mt5-connector/simple/ \
  --extra-index-url https://swing-traders.github.io/nautilus_trader/simple/ \
  "mt5-connector-client==<version>"
```

and the server, at the same release, into the Linux Python that runs its hub beside the terminal, or run the image each release publishes with it installed ([The MT5 server](#the-mt5-server)):

```bash
python -m pip install --extra-index-url https://swing-traders.github.io/mt5-connector/simple/ "mt5-connector-server==<version>"
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

**1. Run an MT5 server.** The adapter talks to the MT5 terminal through `mt5-connector-server`, which the image each release publishes runs beside the terminal — [its README](packages/server/mt5connector/server/README.md) states what the terminal and the server need, and how to run the image. The server must be up, and the terminal's AutoTrading on, before you run any script; without AutoTrading every order is rejected.

**2. Test the connection:**

```python
from mt5connector.client.config import MT5Config
from mt5connector.client.connection import MT5Connection

config = MT5Config(
    account=12345678, password="your_password", server="YourBroker-Demo",
    server_url="http://127.0.0.1:5000", ws_url="ws://127.0.0.1:9000",
)
with MT5Connection(config) as conn:
    print(conn.get_account_info())
```

---

## Configuration

The adapter's own configuration goes through `MT5Config`; its required fields are your account credentials and the MT5 server's two URLs, its HTTP API's (`server_url`) and its WebSocket hub's (`ws_url`), both supplied by the application that embeds the client. No configuration names the symbols a deployment is for: the server serves every symbol the account has, and each client loads the instruments NT's `instrument_provider` config names on it (see [Running in a node](#running-in-a-node)). A config is refused when it is built:

- without a `server_url`, or with one that is not an `http` or `https` URL naming a host;
- without a `ws_url`, or with one that is not a `ws` or `wss` URL naming a host;
- with either URL malformed, or naming a port outside 1–65535;
- with a `venue` that is not a NautilusTrader `Venue`, or whose name is neither `MT5` nor `MT5_` followed by 1 to 8 capital letters or digits — NautilusTrader reads an account id's issuer as the text before its first hyphen, so a venue cannot carry one.

Its repr and string name none of the credentials: the login, the password and the broker server's name.

The broker behind an account is the venue: a consumer running several brokers in one process gives each its own venue, and every instrument id, client id and account id carries it. Every connection owns its transport; nothing is shared across connections but the process. `venue` defaults to `Venue("MT5")`; an account at another broker beside it takes, say, `Venue("MT5_ALPHA")`, its instruments `XAUUSD.MT5_ALPHA` and its clients registered under `MT5_ALPHA`.

```python
from mt5connector.client.config import MT5Config

config = MT5Config(
    account    = 12345678,            # MT5 account number
    password   = "your_password",
    server     = "Exness-MT5Trial9",  # broker server name
    server_url = "http://127.0.0.1:5000",  # the MT5 server's HTTP API
    ws_url     = "ws://127.0.0.1:9000",    # the MT5 server's WebSocket hub
)
```

**Full configuration reference:**

```python
config = MT5Config(
    # Required
    account  = 12345678,
    password = "your_password",
    server   = "Exness-MT5Trial9",
    server_url = "http://127.0.0.1:5000",
    ws_url     = "ws://127.0.0.1:9000",

    # The NautilusTrader venue the account's instruments, clients and account id carry
    venue = Venue("MT5"),          # default: MT5

    # How often the execution client checks for an account report owed or due
    exec_poll_interval_ms = 250,   # default: 250ms

    # Execution
    deviation_points        = 20,  # the price deviation a market order accepts, in points
    account_refresh_seconds = 10,  # the longest the account goes unreported between events
    history_lookback_mins   = 60,  # the venue history read when NT names no window, and at connect

    # The hub sessions' reconnect backoff, which never gives up
    reconnect_initial_delay_s = 1.0,   # at least 0.001, and at most reconnect_max_delay_s
    reconnect_max_delay_s     = 60.0,  # at least 0.001

    # How long connect() waits for the hub to answer the account, its deferrals included
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
    server_url = os.environ["MT5_SERVER_URL"],
    ws_url     = os.environ["MT5_WS_URL"],
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

The instrument provider loads a symbol by this exact name — one a client's `instrument_provider` config names, or one requested later — and builds it from the venue's own definition, read over its connection's hub session; nothing is inferred from a symbol's name:

- **Type** by its calc mode: a FOREX mode is a `CurrencyPair`, a CFD mode (CFD, CFD index, CFD leverage) a `Cfd`; any other mode (futures, exchange stocks, bonds, …) is refused at load, naming the symbol.
- **Grid and limits**: price precision is `digits`, the price increment `trade_tick_size`, the size increment and limits the volume step, minimum and maximum, and the multiplier the contract size.
- **Currencies**:
  - The base is `currency_base` and the quote — the settlement currency — `currency_profit`.
  - Each takes NT's precision, else the account's currency digits for the account currency, else its ISO 4217 minor units.
  - A settlement code none of those covers is refused at load; a base code is built as NT builds a code it does not know.
  - An instrument's settlement currency is registered with NT before every instrument NT receives.
  - The account currency, with every loaded settlement currency, is registered before the first account state.
  - Base-only codes are never registered.
- **Taker fee** from the commission schedule the symbol's EA relays, its first rule's first tier: money per lot in the deposit currency or per unit in a named currency, converted into the quote currency through the venue's own quote; a percentage of the deal's value; or points of the price. It is halved when charged on entry alone. Loading waits for the symbol's EA to relay its schedule — the hub has its chart opened when none publishes it; a relayed schedule with no rule gives zero; a refused relay, a chart that fails to open, and a rule of any other mode or charged on exit alone, fail the load naming the symbol.
- **`info`** carries the venue facts a consumer reads: chart, filling, calc and trade modes, stops and freeze levels, the volume limit, the margin currency, where the bar day and the bar week open (`bar_tz`, `bar_day_open`, `bar_week_open`), `bar_volume` (`tick_count`), and the symbol's sessions:
  - `quote_sessions` — the broker's quote sessions, as `SymbolInfoSessionQuote` states them: seven days, Sunday first, each a list of `[from, to]` in seconds from the broker day's 00:00, unconverted;
  - `trade_sessions` — the broker's trade sessions, as `SymbolInfoSessionTrade` states them, in the same shape.

  Loading waits for the symbol's EA to relay its sessions as it waits for its commission schedule, and a refused relay, or an answer out of that shape, fails the load naming the symbol.

The provider also answers what a consumer discovers its settlement currencies by:

- `quoted_pairs() -> dict[tuple[str, str], str]` — the venue symbol quoting each `(base, profit)` currency pair the venue's FOREX symbols cover, as the last load found it: a symbol the run loads, else one fully tradable, else the first whose quote is not the all-zero struct, each rung in alphabetical order.
- `account_currency() -> str` — the account's currency code, as the account reports it at the call.

---

## Downloading history

`MT5DataDownloader` writes a symbol's bars and quote ticks into a NautilusTrader Parquet catalog, read through the hub's [history calls](packages/server/mt5connector/server/README.md#history) on its connection's hub session.

The downloader walks back from `end`, one request per window, until `start` or the floor the hub's `ranges` holds for the series — read before the walk, again after a window answers no rows, and once the walk ends — and records that floor in its result when it lies inside the range:

- bars by the windows the terminal answers in one read, `maxbars − 11` periods, each bar stamped at the close the hub answers for it — the range names the closes — and typed by the price its symbol's chart mode names: BID or LAST, LAST where the definition states none;
- ticks one UTC day at a time, both sizes of each the instrument's largest order.

A window the hub answers with an error, or whose answer is lost, is recorded among the result's errors and the walk goes on, and so is a `ranges` read the hub answers with an error; any other error reading it raises.

```python
from mt5connector.client.config import MT5Config
from mt5connector.client.connection import MT5Connection
from mt5connector.client.providers import MT5InstrumentProvider
from mt5connector.client.downloader import MT5DataDownloader
from nautilus_trader.common.component import LiveClock
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from datetime import datetime, timezone

config = MT5Config(
    account=12345678, password="your_password",
    server="Exness-MT5Trial9",
    server_url="http://127.0.0.1:5000",
    ws_url="ws://127.0.0.1:9000",
)

conn     = MT5Connection(config)
conn.connect()

provider = MT5InstrumentProvider(conn, venue=config.venue, clock=LiveClock())
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

---

## Running in a node

The data and execution clients run in NautilusTrader's `TradingNode`, and the node's instrument set is NT's own: each client loads what its `instrument_provider` config names, as NT's adapters do.

- `build_mt5_node_config` makes both clients the node's default route and gives each its `InstrumentProviderConfig(load_ids=...)`: the data client the instruments the node reads, those it trades and those it follows; the execution client those it trades.
- A client loads what its config names when it connects: its `load_ids`, or with `load_all=True` every symbol the terminal serves — reading each opens its chart, of which the terminal holds at most `CHARTS_MAX` (100) at once. A config that names neither is refused at connect with `MT5ConfigError`.
- A symbol no config names is loaded when it is requested (`request_instrument`): the server serves any symbol the account has, so another deployment's instruments need no change to it.
- The factories, registered before `node.build()` under the config's venue, build the clients on one connection per account and server, and one instrument provider per connection, venue and `instrument_provider` config.

```python
import os
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.identifiers import InstrumentId
from mt5connector.client.config import MT5Config
from mt5connector.client.factories import (
    build_mt5_node_config,
    MT5LiveDataClientFactory,
    MT5LiveExecClientFactory,
)

mt5_config = MT5Config(
    account  = int(os.environ["MT5_ACCOUNT"]),
    password = os.environ["MT5_PASSWORD"],
    server   = os.environ["MT5_SERVER"],
    server_url = os.environ["MT5_SERVER_URL"],
    ws_url     = os.environ["MT5_WS_URL"],
)

traded   = frozenset({InstrumentId.from_str("EURUSDm.MT5")})
followed = frozenset({InstrumentId.from_str("XAUUSDm.MT5")})

node = TradingNode(
    config=build_mt5_node_config(
        mt5_config,
        data_instruments=traded | followed,
        exec_instruments=traded,
    )
)
node.add_data_client_factory("MT5", MT5LiveDataClientFactory)
node.add_exec_client_factory("MT5", MT5LiveExecClientFactory)
node.build()  # builds the clients, connecting to the MT5 server
node.run()    # connects the clients: each loads what its config names, and the hub sessions start
```

### Bar types

A bar type aggregated `EXTERNAL` is the venue's own bar: the terminal's bar of that timeframe — 1, 2, 3, 4, 5, 6, 10, 12, 15, 20 or 30 minutes, 1, 2, 3, 4, 6, 8 or 12 hours, a day or a week — pushed when it closes and stamped at its close, its prices and tick volume the venue's. Its price type is the one price the terminal builds the symbol's bars from, as its chart mode names it: `BID` or `LAST`. A step the terminal has no timeframe for is refused at subscription and at a history request. `"EURUSDm.MT5-5-MINUTE-BID-EXTERNAL"` is the venue's 5-minute bar of a symbol charted by bid.

Any other bar type is `INTERNAL`: NautilusTrader aggregates it from the symbol's quote ticks itself, priced `BID`, `ASK` or `MID`, and it never carries the venue's bar type. `LAST` internal bars are not available on this venue: NautilusTrader aggregates them from trade ticks, which MT5 does not provide. The bar type string format is:

```
{symbol}.{venue}-{step}-{aggregation}-{price_type}-{aggregation_source}
```

---

## The MT5 server

The adapter runs against `mt5-connector-server`: a WebSocket hub that carries what the EAs inside the terminal publish — one per symbol on its own chart, which opens when a consumer first asks for the symbol and closes once it is out of use: its ticks, closed bars and trade transactions — and answers the calls they serve, every epoch in true UTC, and that EA's source. Its hub, its history rules and the image that runs it are in [its README](packages/server/mt5connector/server/README.md). Each release publishes that image, built from this repository's `Dockerfile`, as `ghcr.io/swing-traders/mt5-connector-server:<tag>`, the release tag with `+` as `-`.

```
┌─ your bot (Linux) ─────────────────┐      ┌─ beside the MT5 terminal ──────┐
│  mt5-connector-client              │      │  WS hub :9000                  │
│   └─ HubSession                    │──────│  MT5 terminal and its EAs      │
│      (streams and calls)           │      │                                │
└────────────────────────────────────┘      └────────────────────────────────┘
```

- `MT5Config` requires the server's `server_url`, an `http` or `https` URL naming a host, and the hub's `ws_url`, a `ws` or `wss` URL naming a host; a port either URL names must lie in 1–65535 — see `packages/client/mt5connector/client/config.py`.
- Each `MT5Connection` owns a hub session of its own, on an event loop it runs on its own thread while connected, and calls its server through its own transport, `connection.mt5`: a `RemoteMT5` of the shim `mt5connector.client.remote_mt5` on the config's `server_url`, built with the connection and closed by its `disconnect()`.
  - `connect()` starts the thread, connects the session and confirms the terminal's login by reading `account_info`, asked again after each deferring answer's delay. The terminal logs in at the container's boot and the hub answers `not_ready` until it has verified the broker clock, so `timeout_s` bounds that wait: past it, `connect()` raises `MT5ConnectionError` naming the last deferral, such as `not_ready`. A hub it cannot reach raises `MT5ConnectionError` naming the hub's URL, and an account read answered with any other error raises that error. A failed connect leaves the connection disconnected with no thread running.
  - `get_account_info()` and `get_terminal_info()` read through the session and raise the hub's error for a read it fails. `disconnect()` disconnects the session and stops the thread.
  - `run(coroutine)` runs a coroutine on the session's loop and blocks for its result, raising what it raised, and `session` is the session such a coroutine is handed; both raise `RuntimeError` on a connection not connected.
  - A connect on a connection already connected or connecting, and a disconnect on one not connected, raise `MT5ConnectionError` naming its state; used as a context manager, a connection connects on entry and disconnects on exit. A lost hub leaves the connection connected: NautilusTrader's client reconnects its session, as it does a client's.
- Each client holds a hub session, `mt5connector.client.hub.HubSession`, on NautilusTrader's own `WebSocketClient`, which reconnects with backoff; on each reconnect the session says hello and subscribes everything it wants again, and a subscription it changes before that hello waits to ride it. NautilusTrader's client can still send a frame it held through the outage ahead of the hello: the hub refuses it and closes the connection, and the next reconnect's resend is clean — nothing is lost, at the cost of one more reconnect.
- A session also asks the hub's calls: `request(call, **arguments)`, or the method named after each call with its arguments, answers `Answered(result, last_error)` once every part of the answer arrived — a struct as its record type, rows as numpy arrays, transactions as `TradeTransaction` frames.
  - It raises the `HubAnswerError` of the answer's error kind. `Syncing`, `NotReady` and `NoPublisher` carry `retry_after_s`, and `deferring(call, **arguments)` asks again after each such delay until it is answered otherwise or its caller cancels.
  - A request unanswered past the hub's deadline and a margin, or in flight when the connection drops, raises `ResponseLost`.
  - A frame of the hub's the session cannot decode is logged as an error: an answer fails the request it answers with `ProtocolError`, a frame whose header breaks fails every request in flight with it, and a stream frame fails nothing.
  - An `order_send` answered sent stays outstanding, across a disconnect too, until its `REQUEST` transaction reaches the session. A reconnect, and a connect after a disconnect, ask each symbol's publisher for the outstanding ones' `REQUEST` transactions, hand each to the owner as the `TradeTransaction` a live one arrives as, and drop with one warning those the publisher no longer holds.
  - The data client subscribes a symbol's ticks while NautilusTrader subscribes its quotes or its mark prices: each tick is a `QuoteTick`, both sizes the instrument's largest order since the venue publishes no depth, and a `MarkPriceUpdate` at its mid. It subscribes an `EXTERNAL` bar type's series and hands NautilusTrader each closed venue bar. Its history requests answer quote ticks sized the same way, and the venue's bars as the bar type requested. After a reconnect it holds the bars pushed until the first one arrives, then reads back on its session, once, the bars that closed between the last one it handed and that one, and hands them in order before the held ones.
  - The execution client subscribes the account's trade transactions: a `DEAL_ADD` is a fill, read from the venue's history by its ticket; an `ORDER_DELETE` or `HISTORY_ADD` ends the order the venue cancelled, expired or rejected; a `TRADE_TRANSACTION_REQUEST` links the ticket of an order whose submit got no answer through its comment's digest, and accepts it. A transaction naming a ticket the client cannot resolve is left to NautilusTrader's reconciliation, never booked as an external order.
  - Each symbol's publisher publishes that symbol's transactions, so they are pushed while its chart is open: while a consumer subscribes to the symbol or the account holds open positions or pending orders on it, and for `MT5_CHART_IDLE_SECONDS` after the server last read it. The client names the symbol on every request it sends, cancels and modifies included, so a request's transaction travels through its symbol's EA.
- The execution client polls nothing for events: what the hub session misses — a disconnection, a ticket it could not resolve, a symbol whose chart is closed — NautilusTrader's own reconciliation heals, through its in-flight check and, once a node sets `open_check_interval_secs` and `position_check_interval_secs`, its open-order and position checks. Its one loop reports the account.
- The account state carries the login's margin as one account-level margin balance in the account currency: the margin the venue states as used as initial, and the maintenance floor as maintenance.
- There is no authentication: the server trusts its network. Its image publishes no host port, loopback included, so the API and the hub are reachable on the container network alone.

The shim raises `ServerUnreachable` when the server cannot be reached or answers outside its contract, and when it refuses a call while it is not ready — HTTP 503 with no `last_error`, the terminal never asked — naming the function and the server's message. It raises `ServerBusy`, naming the function and carrying the delay the server's `Retry-After` gives as `retry_after_s`, when the server refuses a call with every slot taken: the server is up, and the caller decides whether to ask again. Every call sets its transport's `last_error()` to the pair its answer carries, and a failed call returns the package's failure value, as the package does.

`mt5connector.client.history` reads history as coroutines on the hub session each is handed as its first argument, every epoch true-UTC seconds:

- `bars(session, symbol, series, first_open_s, last_open_s)` answers the bars of a `Series` opened in that window as a numpy array of `MqlRates`' fields and each bar's `time_close`; `ticks(session, symbol, start_s, end_s, flags=TickFlags.INFO)` the ticks of those seconds as the package's array; and `ranges(session, symbol)` the hub's `Ranges`: the terminal's `maxbars` and each series' `SeriesRange` floor.
- Each reads through the session's `deferring`: a `syncing`, `not_ready` or `no_publisher` answer is asked again after its delay, with no deadline of its own, until the hub answers otherwise or the caller cancels. Any other error is the hub's `HubAnswerError` raised — `InvalidWindow` for a window starting after the terminal's time — and an answer lost to the session's deadline or a reconnect raises `ResponseLost`.
- `opens_closing(series, first_close, last_close)` gives the opens a window of closes is read by, and `closing(rows, first_close, last_close)` keeps the bars closing in it.
- A synchronous caller runs one on its connection: `conn.run(history.bars(conn.session, "EURUSDm", Series.H1, first_open_s, last_open_s))`.

---

## Releases

- `VERSION` holds the one version both distributions carry.
- A `vX.Y.Z+st` tag builds both wheels, each `py3-none-any` with a sha256 sidecar, and the server image. The image is pushed to `ghcr.io/swing-traders/mt5-connector-server:vX.Y.Z-st` once the image tier passes on it; only then are all four wheel files attached to one GitHub release and the PEP 503 index rebuilt on this repository's GitHub Pages, one page per project: `/simple/mt5-connector-client/` and `/simple/mt5-connector-server/`.
- The image package is public. A new package's visibility is private by GitHub's default and no API sets it, so the first release's push is followed, once, by making the package public in its settings (Change visibility); later pushes keep it.
- Consumers pin a release exactly, suffix included, from that index: `+st` is a constant local segment marking the fork's own index, never a build counter, and PyPI accepts no local version, so no release elsewhere can satisfy the pin.

---

## Running the full test suite

```bash
just test
```

All tests but the image tier's mock the MT5 terminal — no live connection required to run tests.

```
tests/client/              — the adapter: connection, data and execution clients, instruments, factories, the shim, the history client and the downloader
tests/server/              — the WS hub: its streams, requests, clock gate, epoch conversion and settings, the EA's half of the protocol, and the MQL5 generator
tests/image/               — the server image, started on a made-up account; its GUI driver and log tail
tests/live/                — the hub and its EAs in a running image, through the client's hub session
tests/test_distributions.py — the two distributions' shared version and the import boundary between them
```

The image tier runs against a built image when `MT5_IMAGE_TEST` names it, as the server README's [image tests](packages/server/mt5connector/server/README.md#the-image-tests) state; CI builds the image and runs it on every pull request.

The live hub tier, `tests/live/`, runs from any host when `MT5_LIVE_HUB_URL` names a running image's hub (`ws://host:port`) and `MT5_LIVE_HUB_SYMBOL` a symbol its account trades, weekends included. Every case leaves the account flat:

- every read the spawner and a publisher serve, and a deal read by its ticket;
- the symbol's ticks and M1 bars;
- a pending order whose expiration has passed, which the venue refuses, and a command for a symbol no publisher serves;
- its history: a cold M1 window and the floor it measures, a window before that floor, one starting after the terminal's time, a window one period over the span cap where the terminal's `MaxBars` leaves one, a day of ticks, and a week of ticks, timing a publisher read every second while it downloads.

Run it with:

```bash
MT5_LIVE_HUB_URL=ws://<host>:9000 MT5_LIVE_HUB_SYMBOL=<symbol> pytest tests/live -q
```

---

## Project structure

```
mt5-connector/
├── VERSION                       # the one version both distributions carry
├── Dockerfile                    # the server image
├── image/                        # the image's boot scripts, GUI driver, log tail, startup ini, template and pins
├── packages/
│   ├── client/                   # mt5-connector-client
│   │   ├── pyproject.toml
│   │   └── mt5connector/
│   │       ├── client/
│   │       │   ├── commissions.py   # the relayed commission rule and the taker fee it implies
│   │       │   ├── config.py        # MT5Config — all user-facing configuration
│   │       │   ├── connection.py    # MT5Connection — its lifecycle, hub session and server transport
│   │       │   ├── constants.py     # venue, account-loop and reconnect defaults
│   │       │   ├── currencies.py    # the precision ladder a venue currency code is built by
│   │       │   ├── data.py          # MT5DataClient — pushed ticks, marks and venue bars, and history requests
│   │       │   ├── downloader.py    # MT5DataDownloader — historical bar download
│   │       │   ├── errors.py        # custom exceptions
│   │       │   ├── execution.py     # MT5LiveExecutionClient — orders, their events, and reconciliation reports
│   │       │   ├── factories.py     # LiveDataClientFactory + LiveExecClientFactory wiring
│   │       │   ├── history.py       # the hub's history reads on a session
│   │       │   ├── parsing.py       # symbol_info → NautilusTrader Instrument conversion
│   │       │   ├── providers.py     # MT5InstrumentProvider
│   │       │   ├── sessions.py      # the relayed trading sessions an instrument's info carries
│   │       │   ├── hub.py           # the hub session on NT's WebSocketClient: streams and calls
│   │       │   └── remote_mt5.py    # the HTTP shim generated from the inventory
│   │       └── wire/
│   │           ├── mirror.py        # the inventory of the pinned MetaTrader5 package
│   │           ├── broker_clock.py  # broker wall-clock time ↔ true UTC
│   │           ├── history_wire.py  # the history routes' paths, series and answer states
│   │           ├── push_wire.py     # the hub protocol's roles, streams and MQL5 names
│   │           ├── binary.py        # the hub protocol's binary frames
│   │           └── calls.py         # the hub's call registry and its arguments' and results' codecs
│   └── server/                   # mt5-connector-server
│       ├── pyproject.toml
│       └── mt5connector/
│           └── server/
│               ├── README.md        # the hub protocol and what its image provides
│               ├── ws_server.py     # the hub (mt5-connector-hub)
│               ├── hub_requests.py  # the hub's pending requests and their deadlines
│               ├── hub_gate.py      # the hub's clock gate
│               ├── hub_health.py    # the hub's /health
│               ├── hub_utc.py       # the hub's conversion of every epoch to and from true UTC
│               ├── floors.py        # the history floors measured per series
│               ├── wire -> ../../../client/mt5connector/wire
│               └── mql5/            # the EA, its startup script and their includes
└── tests/                        # full test suite (no live MT5 required; the image tier runs the image)
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

**`hub at ws://…: not ready within 10.0s, the last answer not_ready: account_info: the broker clock is not verified`**

The hub has not verified the broker clock: the terminal is still booting, or it never logged in — a wrong account number, password or server name in the container's environment. Check the container's log and the hub's `/health` (see [its README](packages/server/mt5connector/server/README.md#boot)), then connect again; a longer `timeout_s` waits out a boot still in progress.

**An order rejected with `TRADE_RETCODE_CLIENT_DISABLES_AT: AutoTrading disabled by client`**

AutoTrading is disabled in the server's MT5 terminal. Click the **AutoTrading** button in the toolbar — it should turn green. This must be enabled for any automated order to be sent.

**`Factory was not of type LiveExecClientFactory`**

You are using an old version of `factories.py` where `MT5LiveExecClientFactory` did not inherit from `LiveExecClientFactory`. Update to the latest version.

**Strategy not placing trades after 30+ minutes**

Check that the bar type string in your strategy config exactly matches the bar type you subscribed to in `on_start`. A mismatch means `on_bar` is never called. Also verify AutoTrading is enabled in the server's MT5 terminal.

---

## Safety notes

- Always use a **demo account** until you have verified your strategy behaves correctly.
- Every order the adapter sends carries a magic derived from the node's trader id (the first 8 bytes of its SHA-256, masked to 63 bits). The execution client tracks only the orders, positions and deals carrying its own magic, the stop-loss and take-profit deals of the positions it holds exits for, and the stop-outs of the positions it opened, so manual trading on the same account, or a node with another trader id, is left alone.
- The adapter runs on hedging accounts only: it declares NT's `HEDGING` position model and refuses to connect to an account whose margin mode is netting or exchange, or to a read-only (investor) session.
- The execution account id is `MT5-<hash>-<magic>`: the first 8 hex digits of the SHA-256 of the login's decimal string, the login read from the account at connect, and the node's magic. Two trader ids on one login book under two ids that share the hash, and the login appears in neither. The login, the password and the broker server's name appear in no log, repr, string or exception message the client composes.
- An order's comment at the venue is the first 29 hex digits of its client order id's SHA-256. Venue tickets map to NT orders through NT's own order records, and through that digest for an order whose submit got no answer; a deal or order neither explains is logged and left to NT's reconciliation.
- Order lists, post-only and trailing orders, and times in force other than GTC and GTD are rejected before anything is sent.
- A reduce-only order is an exit of the position its submit names (NT's position id, the venue's position identifier), translated into what the venue holds for one, which is one stop loss and one take profit per position:
  - A stop-market order becomes the position's stop loss and a limit order its take profit. The order's venue order id is synthetic, `<identifier>-SL-<n>` or `<identifier>-TP-<n>`; a later order of the same kind moves the bracket and cancels the order it took the place of.
  - A cancel clears the bracket, and a modify moves it. The bracket has no volume and covers whatever the position holds, so the quantity a modify states changes nothing at the venue and is the order's own figure.
  - The venue answers a request setting a bracket to what it already holds with "no changes" (10025), which confirms the setting: a stop or target sent again to the level the venue holds is accepted like any other.
  - A market order closes the position by its current ticket at the order's quantity, whatever brackets stand: the venue alone judges that volume, and the position's stop loss and take profit stay on whatever remains.
  - A stop loss or take profit that fires is a fill of the order holding that bracket. The order first takes the ticket of the order the venue executed the bracket with as its venue order id, and each fill reports under the ticket of its execution.
  - A stop-out fills no exit. Its deal and the venue order that executed it are this trader's by the position they close, whatever magic the venue stamps on them; no order of the trader explains them, so both are left to NT's reconciliation. A stop-out that closes the position takes its stop loss and take profit with it, so its exits report canceled and a cancel of one cancels it; one that leaves part of the position leaves the surviving brackets standing, as the client reads them from the venue's position.
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
