# mt5-connector-server

The terminal side of mt5-connector: an HTTP server that mirrors the `MetaTrader5` package with every epoch in true UTC, the WebSocket hub that carries the terminal's ticks, and the source of the EA and the startup script that run inside the terminal. `mt5-connector-client`, the NautilusTrader adapter, talks to it.

Releases are wheels on the fork's package index, versioned together with the client's:

```bash
python -m pip install --extra-index-url https://swing-traders.github.io/mt5-connector/simple/ "mt5-connector-server==0.4.0+st.N"
```

| Part | What it is |
|---|---|
| `mt5connector.server.app` | the HTTP server, started as `mt5-connector-server` |
| `mt5connector.server.ws_server` | the tick hub, started as `mt5-connector-hub` |
| `mt5connector/server/mql5/` | the EA (`Expert/ticks.mq5`), its startup script (`Scripts/ticks_setup.mq5`) and the MQL5 libraries they include (`Include/`) |
| `mt5connector.server.wire` | the wire vocabulary, copied from the client's `mt5connector.wire` when the wheel is built |

---

## HTTP API

The server mirrors the `MetaTrader5` package (5.0.6231), so code written against `import MetaTrader5 as mt5` runs unchanged against the client's `mt5connector.client.remote_mt5`. Both are generated from one inventory of the package, `mirror.py` in the wire vocabulary.

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

### The broker clock

Once the terminal is initialized, the server verifies the broker's clock against the trade server's time the EA relays through the WS hub, at any hour, market open or closed:

- A sample is fresh while it is younger than `MT5_CLOCK_SAMPLE_MAX_AGE_SECONDS` and says the terminal is connected to the trade server.
- A fresh sample verifies the clock only once the terminal has been connected without a break for `MT5_CLOCK_SAMPLE_MAX_AGE_SECONDS`, so a terminal back from downtime is not trusted while it re-fetches what it missed. A sample that says the terminal is disconnected, or a gap of `MT5_CLOCK_SAMPLE_MAX_AGE_SECONDS` between two samples, starts the run again.
- A fresh sample's trade-server time, converted to true UTC, must sit within 120 s of the server's clock, or the server exits with both times and the offset in its log. The terminal extrapolates the trade server's time from the clock it shares with the server, so the comparison checks the offset the terminal learned from the trade server against the broker clock's schedule, whatever the host's clock reads.
- At start the server waits up to `MT5_CLOCK_BOOTSTRAP_SECONDS` for the first sample that verifies the clock, and exits when none arrives; the window includes the connected run, so with the defaults `/health` first answers 200 at least 30 s after the first connected sample. It then re-verifies the latest sample every `MT5_CLOCK_CHECK_SECONDS`.
- The moment the latest sample no longer verifies the clock it is unverified, and stays so until a sample verifies it again. Each change is logged once.

## History

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

## WebSocket hub

The hub (`mt5connector.server.ws_server`, port `WS_PORT`) carries these frames:

| Frame | From | What the hub does |
|---|---|---|
| `{"type": "hello", "role": "ea"}` or `"role": "adapter"` | EA, adapter | records the connection's role |
| a tick — `symbol`, `bid`, `ask`, `time_msec`, `flags` and more, as strings, with no `type` | EA | broadcasts it to every adapter when its bid or ask changed |
| `{"v": 1, "type": "server_time", "symbol", "trade_server", "current", "gmt", "connected"}`, its epochs and `connected` (0 or 1) as integers | EA | POSTs it to the server's `/relay/server_time` on `127.0.0.1:MT5_API_PORT`, never to adapters, and logs a failed post |
| `{"type": "subscribe", "symbols": [...]}` or `"unsubscribe"` | adapter | records the symbols |

A frame the hub cannot handle closes its connection with a logged warning. The EA sends `server_time` every `RelaySeconds` (5 in the chart template), the terminal's `TimeTradeServer()`, `TimeCurrent()` and `TimeGMT()` with `TERMINAL_CONNECTED`, and reconnects to the hub on the same timer, so both go on while the market is closed. The EA answers the hub's keepalive pings each time it ticks or its timer fires.

---

## What the image must provide

One image runs the terminal, the HTTP server and the hub. This section is everything they need from it; how the image provides each part is its own.

### Settings

The HTTP server reads its settings from the environment once at start; a missing or malformed one stops it with an error naming the setting, and no error carries a credential's value:

| Variable | Setting | Default |
|---|---|---|
| `MT5_TERMINAL_PATH` | the terminal executable, as Windows names it (`C:\Program Files\MetaTrader 5\terminal64.exe` for a default install) | required |
| `MT5_LOGIN` | account number | required |
| `MT5_PASSWORD` | account password | required |
| `MT5_SERVER` | trade server name | required |
| `MT5_LOGIN_TIMEOUT_MS` | initialize timeout | `60000` |
| `MT5_API_HOST` | bind address | `0.0.0.0` |
| `MT5_API_PORT` | HTTP port | `5000` |
| `MT5_API_THREADS` | waitress threads, one of them kept free for `/health` and the relay; at least 2 | `5` |
| `MT5_CLOCK_CHECK_SECONDS` | interval between re-verifications of the broker's clock | `300` |
| `MT5_CLOCK_SAMPLE_MAX_AGE_SECONDS` | how long a relayed server-time sample stays fresh, and how long the terminal must stay connected before a sample verifies the clock | `30` |
| `MT5_CLOCK_BOOTSTRAP_SECONDS` | how long the server waits at start for the first sample that verifies the clock | `120` |
| `MT5_BROKER_TZ` | the zone the broker's clock follows, as an IANA name | `America/New_York` |
| `MT5_BROKER_OFFSET_HOURS` | hours the broker's clock runs ahead of that zone | `7` |
| `MT5_HISTORY_RETRY_SECONDS` | the `Retry-After` of a history answer the terminal has not proven yet, and of a call refused with every slot taken: how long the client waits before asking again | `5` |
| `MT5_FLOOR_TTL_SECONDS` | how long a measured history floor is used before it is measured again | `900` |

The hub reads two:

| Variable | Setting | Default |
|---|---|---|
| `WS_PORT` | the hub's port, bound on all interfaces | `9000` |
| `MT5_API_PORT` | the HTTP server's port, which the hub posts each relayed server time to on `127.0.0.1` | `5000` |

The credentials reach the processes as environment variables at start; the image never carries them.

### The processes

| Process | Runs under | Started by |
|---|---|---|
| the HTTP server | the Windows Python, under Wine in the terminal's prefix: the `MetaTrader5` package it drives the terminal through is Windows-only | `mt5-connector-server`, or `python -m mt5connector.server.app` |
| the hub | the image's Linux Python | `mt5-connector-hub`, or `python -m mt5connector.server.ws_server` |

- Both Pythons install `mt5-connector-server` from the index. Its dependencies pin `MetaTrader5` on Windows alone, `numpy` at 2.2.1 because later releases crash under Wine, and `tzdata` because the Windows Python has no zone database. The hub imports none of these, but installing the distribution brings them all, and numpy 2.2.1 publishes wheels for CPython 3.10 to 3.13 only.
- The hub posts every relayed server time to `127.0.0.1`, and the server accepts that route from `127.0.0.1` alone: the two share a loopback.
- At start the server initializes the terminal named by `MT5_TERMINAL_PATH` with the account's login, password and server, and exits non-zero when that fails.
- It then answers HTTP 503 on every route but `/health` and the relay until a server-time sample from the EA verifies the broker clock, and exits non-zero when none does within `MT5_CLOCK_BOOTSTRAP_SECONDS`, or when any sample it measures, then or later, puts the trade server more than 120 s from the server's clock. The terminal, its EA and the hub must therefore be up within that window of the server's start; whatever starts the server decides whether to start it again.
- `/health` answers HTTP 200 once the clock is verified, and 503 whenever it is not.

### The terminal

The terminal is started as `terminal64.exe /config:<setup.ini> /portable`, so it keeps its data — the `MQL5` tree, its profiles and templates — under its own install directory. The paths below are relative to that directory.

#### setup.ini

| Section | Key | Value |
|---|---|---|
| `[Common]` | `Login`, `Password`, `Server` | `MT5_LOGIN`, `MT5_PASSWORD` (quoted) and `MT5_SERVER` |
| | `AutoConfiguration` | `true` |
| | `ProxyEnable` | `false` |
| `[Charts]` | `Profile` | `default` |
| `[Experts]` | `AllowDllImport` | `1` |
| | `Enabled` | `1`: algorithmic trading on, without which the terminal refuses every order the adapter sends |
| | `WebRequest` | `1` |
| `[StartUp]` | `Script` | `ticks_setup` |
| | `Symbol`, `Period` | the chart the script runs on, `EURUSD` and `M1`; the script closes it when done |

The terminal must know the trade server `MT5_SERVER` names — its `Config\servers.dat` lists it — before the login can succeed: a server a fresh install does not know is added by searching for its name once (File → Open an Account), or by copying in a `servers.dat` that lists it.

#### The EA and the startup script

The wheel carries their source; `python -c "import mt5connector.server, pathlib; print(pathlib.Path(mt5connector.server.__file__).parent / 'mql5')"` prints where it is installed. Each file goes into the terminal's `MQL5\` tree:

| In the wheel's `mql5/` | In `MQL5\` |
|---|---|
| `Expert/ticks.mq5` | `Experts\ticks.mq5` |
| `Scripts/ticks_setup.mq5` | `Scripts\ticks_setup.mq5` |
| `Include/JAson.mqh` | `Include\JAson.mqh` |
| `Include/MQL5Book/` | `Include\MQL5Book\` |

Both sources are compiled by `MetaEditor64.exe`, beside `terminal64.exe`, into `Experts\ticks.ex5` and `Scripts\ticks_setup.ex5`. Under Wine, MetaEditor has these quirks:

- Its name is `MetaEditor64.exe`, capital M and E, on a volume that is case-sensitive.
- It honours only the last `/compile:` argument of an invocation: each source is compiled by its own.
- It truncates a `/compile:` path that contains a space: each source is compiled from a directory whose path has none — `C:\mt5build\ticks.mq5`, with the `Include\` tree beside it — and the `.ex5` copied into place.
- A clean compile writes `0 errors, 0 warnings` to its `/log:` file; a missing `.ex5` is a failed compile.

On each terminal start `ticks_setup` opens an M1 chart for every symbol in `symbols.txt` and applies the template `ticks.tpl`, which attaches the EA. A symbol whose chart already runs the EA is left as it is, and an open chart without it gets the template again.

#### symbols.txt

`MQL5\Files\symbols.txt` lists the symbols to stream:

- one symbol per line, read as ANSI, its surrounding whitespace trimmed;
- blank lines and lines starting with `#` skipped;
- a symbol the terminal does not know skipped, with a line in the terminal's log.

Only a listed symbol streams ticks: each needs its own chart running the EA, so a symbol the adapter subscribes to that is not listed produces none.

#### The chart template

`ticks.tpl` goes in `MQL5\Profiles\Templates\` and in `Profiles\Templates\`, the two places a template applied by bare name is looked for. It must be in the form the terminal saves templates in:

- UTF-16LE with a byte-order mark (`FF FE`), and CRLF line ends;
- a root `<chart>` section — its symbol and period are ignored, the chart keeps its own;
- the EA in its own `<expert>` block, one key per line:

```
<expert>
name=ticks
path=Experts\ticks.ex5
expertmode=5
<inputs>
Server=ws://127.0.0.1:9000
ReconnectIntervalSec=3
RelaySeconds=5
</inputs>
</expert>
```

| EA input | Meaning |
|---|---|
| `Server` | the hub's URL, `ws://127.0.0.1:<WS_PORT>` |
| `ReconnectIntervalSec` | the fewest seconds between two attempts to reach the hub while the EA is not connected |
| `RelaySeconds` | the EA's timer: the interval between server-time frames, and between checks of the connection. It must stay below `MT5_CLOCK_SAMPLE_MAX_AGE_SECONDS`, or the clock never verifies |

<details>
<summary>A complete template, before its UTF-16LE encoding</summary>

```
<chart>
id=20611538696288
symbol=USDJPY
description=US Dollar vs Japanese Yen
period_type=1
period_size=1
digits=3
tick_size=0.000000
position_time=0
scale_fix=0
scale_fixed_min=158.430000
scale_fixed_max=159.610000
scale_fix11=0
scale_bar=0
scale_bar_val=1.000000
scale=16
mode=1
fore=0
grid=1
volume=1
scroll=1
shift=0
shift_size=19.379845
fixed_pos=0.000000
ticker=1
ohlc=0
one_click=0
one_click_btn=1
bidline=1
askline=0
lastline=0
days=0
descriptions=0
tradelines=1
tradehistory=1
window_left=0
window_top=0
window_right=0
window_bottom=0
window_type=1
floating=0
floating_left=0
floating_top=0
floating_right=0
floating_bottom=0
floating_type=1
floating_toolbar=1
floating_tbstate=
background_color=0
foreground_color=16777215
barup_color=65280
bardown_color=65280
bullcandle_color=0
bearcandle_color=16777215
chartline_color=65280
volumes_color=3329330
grid_color=10061943
bidline_color=10061943
askline_color=255
lastline_color=49152
stops_color=255
windows_total=1

<expert>
name=ticks
path=Experts\ticks.ex5
expertmode=5
<inputs>
Server=ws://127.0.0.1:9000
ReconnectIntervalSec=3
RelaySeconds=5
</inputs>
</expert>

<window>
height=100.000000
objects=0

<indicator>
name=Main
path=
apply=1
show_data=1
scale_inherit=0
scale_line=0
scale_line_percent=50
scale_line_value=0.000000
scale_fix_min=0
scale_fix_min_val=0.000000
scale_fix_max=0
scale_fix_max_val=0.000000
expertmode=0
fixed_height=-1
</indicator>
</window>
</chart>
```

</details>

#### The allowlist

The EA's socket reaches only a host on the terminal's allowlist — Tools → Options → Expert Advisors → "Allow WebRequest for listed URL", checked, with `127.0.0.1` on its list:

- The list lives in memory, and every terminal start empties it: the host is added again after each start, until then the EA cannot reach the hub.
- `setup.ini` cannot set it: MT5 ignores the `WebRequestUrl` key, which is MT4's.
- Confirming the dialog re-initialises every attached EA, which then connects again.

### Network

- The adapter needs two ports: the HTTP server's `MT5_API_PORT` and the hub's `WS_PORT`. It derives the hub's URL from `server_url` on port 9000 unless its config names `ws_url`.
- There is no authentication: the API trusts its network. Both ports are published on loopback alone; exposing either is a design change.
