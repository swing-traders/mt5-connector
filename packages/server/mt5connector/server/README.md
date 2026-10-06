# mt5-connector-server

The terminal side of mt5-connector: an HTTP server that mirrors the `MetaTrader5` package with every epoch in true UTC, the WebSocket hub that pushes the terminal's ticks, closed bars and trade transactions, and the source of the EA and the startup script that run inside the terminal. `mt5-connector-client`, the NautilusTrader adapter, talks to it.

Releases are wheels on the fork's package index, versioned together with the client's; `<version>` is a release's version, the `VERSION` file's value at its tag, the latest listed on the [releases page](https://github.com/swing-traders/mt5-connector/releases):

```bash
python -m pip install --extra-index-url https://swing-traders.github.io/mt5-connector/simple/ "mt5-connector-server==<version>"
```

| Part | What it is |
|---|---|
| `mt5connector.server.app` | the HTTP server, started as `mt5-connector-server` |
| `mt5connector.server.ws_server` | the push hub, started as `mt5-connector-hub` |
| `mt5connector/server/mql5/` | the EA (`Expert/ticks.mq5`), its startup script (`Scripts/ticks_setup.mq5`) and the MQL5 libraries they include (`Include/`) |
| `mt5connector.server.wire` | the wire vocabulary, copied from the client's `mt5connector.wire` when the wheel is built |

---

## HTTP API

The server mirrors the `MetaTrader5` package (5.0.6231): a `RemoteMT5` of the client's `mt5connector.client.remote_mt5`, one per connection, carries the package's functions with their signatures, and the package's constants and struct types live in `mirror.py`, the one inventory of the package in the wire vocabulary that both sides are generated from.

- `POST /mt5/<function>` for each of the package's 32 functions, its arguments a JSON object keyed by parameter name; datetimes travel as true-UTC epoch seconds.
- The server passes the package no keyword mapping at all when it has no named argument to pass, since `order_check` and `order_send` refuse their request with `(-2, 'Unnamed arguments not allowed')` beside any keyword mapping, an empty one included.
- A package call answers `{"ok": true, "result": ..., "last_error": [code, message]}` or `{"ok": false, "error": {"code": ..., "message": ...}, "last_error": [code, message]}`. `last_error` is the package's `last_error()` read right after the call; a failure's error is that same pair. Structs answer as objects in the package's field order, arrays as lists of objects keyed by the dtype's fields.
- Every epoch is true UTC in both directions. The terminal keeps time on the broker's clock — a zone's wall time plus a fixed offset, New York plus seven hours by default — and the server converts by the era of each instant:
  - every epoch field of an answer, `0` (no time) left as `0`, with bars still stamped at their open;
  - every time window a client sends, and a trade request's `expiration`.
- An epoch in the zone's repeated autumn hour reads as its first occurrence, and the server logs one warning per repeated hour, naming the field, however many answers carry it. One in its skipped spring hour is a server error (HTTP 500).
- Answers are HTTP 200, except a failing `terminal_info`, which is HTTP 503: the terminal's IPC is down. A missing or unknown parameter, a time that is not an integer epoch, or a history query in none of its documented call forms, is refused with HTTP 400 and code -2 (`RES_E_INVALID_PARAMS`) before the package is called.
- `POST /mt5/shutdown` is the one deliberate departure from the package: it answers `None` without calling the package's `shutdown()`. Every client shares the server's terminal session, and the adapter calls `shutdown()` whenever it disconnects, so passing it on would end the session for every client. `initialize` and `login` pass through unchanged.
- `GET /health` answers the broker clock's latest verification — the relayed sample's chart `symbol`, its `trade_server` and `current` (last quote) times in true UTC, the terminal's own `gmt`, and the `skew_s` from the server's clock and the `offset_s` in effect — beside the server's load: the calls holding a slot `in_flight` now, the `peak_in_flight` and the `refusals` since start, and its `workers`. While the clock is not verified it answers HTTP 503. It never calls the terminal: the server initialized it at start, and the EAs' samples say whether it is connected to the trade server.
- `POST /relay/server_time` and `POST /relay/commissions/<symbol>` take the `server_time` and `commissions` frames the WS hub relays from the EAs, the commission frame under the symbol of the EA that sent it, and answer `{"ok": true, "result": null}`. Each accepts a frame from `127.0.0.1` only (HTTP 403 otherwise), and refuses anything but the frame's exact shape — for a commission frame, naming the path's symbol, and in each enum field a member of that field's enum — with HTTP 400 and code -2. A refused commission frame is kept against the path's symbol until a later frame for it is accepted.
- `GET /commissions/<symbol>` answers the commission schedule the symbol's EA last relayed — `ret` and `last_error` of its `SymbolInfoCommissions` call and its `rules`, every enum field by the name `EnumToString` gives it, one of the members the MQL5 reference lists for the field's enum, which `push_wire.py` declares:

  | HTTP | When |
  |---|---|
  | 200 | the last relay was accepted; a schedule with no rule answers an empty `rules` list |
  | 503 with code -20001 (`SYNCING`) and `Retry-After: <MT5_HISTORY_RETRY_SECONDS>` | nothing was relayed for the symbol yet |
  | 422 with code -20003 (`RELAY_REFUSED`) | the last relay was refused; the message names the symbol and why |
  | 503 with the failure envelope, code -20002 (`BUSY`) and `Retry-After: <MT5_HISTORY_RETRY_SECONDS>` | every slot is taken |

  No package call answers it, so its envelope carries no `last_error` — except the busy refusal, which is the failure envelope every slotted route answers with.
- `GET /commissions/<symbol>`, `POST /history/bars` and `POST /history/ticks` read a symbol, and first check that an EA publishes it. The server knows the symbols the EAs publish from their server-time samples, each naming its EA's chart symbol, fresh for `MT5_CLOCK_SAMPLE_MAX_AGE_SECONDS`:
  - For a symbol no EA publishes, the server posts the symbol to the hub's chart route (`POST http://127.0.0.1:<MT5_HUB_PORT>/charts/<symbol>`), which has the spawner open its chart, and answers HTTP 503 with code -20001, `Retry-After: <MT5_HISTORY_RETRY_SECONDS>` and the message `no publisher for <symbol>; chart requested` until the chart's EA relays.
  - While the hub's last answer for a symbol is `requested`, the server posts it again at most once per `MT5_HISTORY_RETRY_SECONDS`, so a client's next retry meets a failure to open its chart; the hub sends the spawner one `open_chart` for it however often it is posted.
  - The server posts any other symbol at most once per half `MT5_CHART_IDLE_SECONDS`, the start of an EA's samples counting as a post. For a symbol an EA publishes, the post tells the hub the symbol is in use, which keeps its chart open.
  - For a symbol whose chart the spawner failed to open, the hub answers the post with the spawner's reason, and the server answers HTTP 400 with code -20004 (`CHART_FAILED`) and the message `the chart of <symbol> failed to open: <reason>`. Such a symbol is posted again on its next read, so a read after its chart opens is served.
  - A post the hub does not answer is HTTP 503 with code -1 naming the failure; the next read posts again.
  - The check runs after the clock gate, in the read's slot, so a hub that does not answer holds a slot and never the free worker. `/history/ranges` and the mirror routes check nothing.
- `POST /history/bars` and `POST /history/ticks` answer a history window the server vouches for (see [History](#history)), and `GET /history/ranges?symbol=<symbol>` the floors it has measured.
- While the broker clock is not verified, every route but `/health` and the two relays answers HTTP 503 with `{"ok": false, "error": {"code": -1, "message": "the broker clock is not verified"}}` and calls nothing.
- Package calls run one at a time; waitress serves the API with `MT5_API_THREADS` workers, one of them always kept free for `/health` and the relays.
- Every route that can reach the terminal or the hub — `/mt5/<function>`, the three history routes and `/commissions/<symbol>` — takes one of `MT5_API_THREADS − 1` slots. A call that finds every slot taken is refused at once, never queued on a worker: HTTP 503 with the failure envelope, code -20002 naming the route, and `Retry-After: <MT5_HISTORY_RETRY_SECONDS>`. Each refusal is logged as a warning. The clock check answers first, so while the clock is not verified such a call gets its 503 and takes no slot.

### The broker clock

Once the terminal is initialized, the server verifies the broker's clock against the trade server's time the EAs relay through the WS hub, at any hour, market open or closed:

- A sample is fresh while it is younger than `MT5_CLOCK_SAMPLE_MAX_AGE_SECONDS` and says the terminal is connected to the trade server.
- A fresh sample verifies the clock only once the terminal has been connected without a break for `MT5_CLOCK_SAMPLE_MAX_AGE_SECONDS`, so a terminal back from downtime is not trusted while it re-fetches what it missed. A sample that says the terminal is disconnected, or a gap of `MT5_CLOCK_SAMPLE_MAX_AGE_SECONDS` between two samples, starts the run again.
- A fresh sample's trade-server time, converted to true UTC, must sit within 120 s of the server's clock, or the server exits with both times and the offset in its log. The terminal extrapolates the trade server's time from the clock it shares with the server, so the comparison checks the offset the terminal learned from the trade server against the broker clock's schedule, whatever the host's clock reads.
- A sample reads the terminal's clock live, so its times in the repeated autumn hour read as the occurrence nearer the server's clock: the trade-server time measured, the `trade_server` and `current` `/health` answers, and the terminal's time a history window's start is held to. Every epoch an answer carries keeps its first occurrence.
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
| 503 | the failure envelope with code -20002, naming the route, and `Retry-After: <MT5_HISTORY_RETRY_SECONDS>` | every slot is taken: ask again after the delay |
| 400 | the failure envelope with code -2 | a body out of shape, or a window that starts after the terminal's current time: the trade-server time an EA last relayed, plus the time since it arrived |

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

The hub (`mt5connector.server.ws_server`, port `MT5_HUB_PORT`) carries every push: one EA per symbol, each on its own chart inside the terminal, publishes, and each consumer — the client's data and execution clients — receives the streams it subscribes to. Commands, reconciliation reports, history and account state travel over HTTP; nothing else travels over the hub.

- One endpoint. Every frame is a JSON text frame carrying `"v": 1`; a frame of another version is answered with an `error` frame and the connection closed with code 1002. A frame the hub cannot handle — not JSON, out of its kind's shape, or of a kind the connection's role does not send — is answered the same way and logged as a warning; a frame of an unknown kind is logged and skipped.
- A connection's first frame is its `hello`, declaring its role: `ea` publishes, `adapter` consumes. An EA's hello also names its chart's `symbol` and whether it is the `spawner`.
- The hub keeps no event store and replays nothing: a consumer that reconnects subscribes again and receives what is published from then on.

| Frame | From → to | Fields |
|---|---|---|
| `hello` | EA, adapter → hub | `role`: `ea` or `adapter`; an EA's also `symbol`, its chart's, and `spawner`, a boolean |
| `subscribe`, `unsubscribe` | adapter → hub | `id`, an integer its `ack` carries; `stream`: `ticks` with a `symbol`, `bars` with a `symbol` and a `timeframe` from `M1` to `W1`, or `trade_transactions` |
| `ack` | hub → adapter | the `id` of the op it acknowledges, queued ahead of every frame the op routes to the adapter |
| `error` | hub → EA, adapter | `message`: what the hub could not handle; the connection closes after it |
| `wanted` | hub → the EA of `symbol` | `symbol`; `ticks`, whether some adapter subscribes to its ticks; and `timeframes`, those of its bars some adapter subscribes to, as MQL5's `ENUM_TIMEFRAMES` values. Sent on every change to that symbol's part and in answer to the EA's hello |
| `duplicate` | hub → an EA | `symbol`: another EA publishes the EA's symbol; the connection closes after it |
| `open_chart`, `close_chart` | hub → the spawner | `symbol`: the chart to open, or the chart of a symbol out of use to close |
| `chart_opened` | the spawner → hub | `symbol`, answering its `open_chart`: a chart of the symbol is open |
| `chart_failed` | the spawner → hub | `symbol` and `reason`, answering its `open_chart`: no chart of the symbol opened, and the call that failed with its error |
| `chart_kept` | the spawner → hub | `symbol` and `reason`, answering its `close_chart`: the chart stays open, and the open positions and pending orders the account holds on the symbol |
| `tick` | EA → the adapters subscribed to the symbol's ticks | `symbol` and the `MqlTick` fields verbatim: `time`, `bid`, `ask`, `last`, `volume`, `time_msc`, `flags`, `volume_real` |
| `bar` | EA → the adapters subscribed to the symbol's bars of the timeframe | `symbol`, `timeframe` — the `ENUM_TIMEFRAMES` value from the EA, the series name to adapters — and the `MqlRates` fields verbatim, stamped at the bar's open: `time`, `open`, `high`, `low`, `close`, `tick_volume`, `spread`, `real_volume`; published once the bar has closed |
| `trade_transaction` | EA → the adapters subscribed to `trade_transactions` | `transaction`, `request` and `result`: the three structs `OnTradeTransaction` receives, under their MQL5 field names, every enum as its MQL5 integer |
| `server_time` | EA → the server's `/relay/server_time` | `symbol`, the EA's, `trade_server`, `current`, `gmt` and `connected` (0 or 1), as integers |
| `commissions` | EA → the server's `/relay/commissions/<symbol>`, under the EA's symbol | `symbol`, `ret` and `last_error` of its `SymbolInfoCommissions` call, and `rules`: each rule's `currency`, its enum fields by name and its `tiers`, each with its enum fields by name, its values and its `currency` |

- Every epoch of a `tick`, a `bar` and a `trade_transaction` reaches the adapters in true UTC: the EA sends the broker's clock, and the hub converts through the broker clock `MT5_BROKER_TZ` and `MT5_BROKER_OFFSET_HOURS` set, as the HTTP server does. An epoch in the broker's repeated hour reads as its first occurrence, with one warning per repeated hour.
- An adapter's frames wait in a queue of at most 10,000; the frame that finds it full closes that adapter with code 4008, and the rest are served on.
- The hub pings every connection every 10 s, one ping outstanding at a time. A ping that goes 30 s without its pong fails the connection, which the hub drops within its 10 s close timeout: 50 s after the last pong at most, as long as the hub's writes to the connection drain. A peer that stops reading while the hub has frames queued to it holds back the ping and the close frame, and with them both timeouts.
- The hub hands `server_time` and `commissions` frames to the server's relay routes on `127.0.0.1:MT5_API_PORT` off its event loop, so a slow server never holds back a stream, never sends them to an adapter, and logs a failed post.

One EA publishes each symbol, and nothing is elected:

- A symbol's first EA to say hello publishes it. The hub answers a later EA for that symbol with `duplicate`, closes it, and logs a warning naming the symbol; the publisher is untouched.
- The hub drops, with a warning, a frame an EA may not send: a tick, a bar, a transaction or a server-time sample of a symbol other than its own. A transaction's symbol is its request's for a `TRADE_TRANSACTION_REQUEST`, its own otherwise; the spawner alone may send one that names no symbol. A commission frame is posted under the EA's symbol whatever it names, so the server refuses a mismatch against that symbol.
- The spawner is the EA whose hello says so, the latest one to say hello when two do, with a warning naming both. It publishes its own symbol like any EA.

The hub has the spawner open and close charts:

- **Requests.** A consumer subscribing to the ticks or bars of a symbol no EA publishes, the server posting such a symbol, and a publishing EA going away while its symbol is in use each request the symbol's chart. A request is held until the symbol's EA says hello, and sent to the spawner as `open_chart` once per spawner connection; one made while no spawner is connected is sent on the spawner's hello.
- **Failed charts.** The spawner answers every `open_chart` with `chart_opened` or `chart_failed`. The hub keeps a failure, with its reason and a warning naming the symbol, until the spawner answers `chart_opened` for the symbol or its EA says hello. A chart answer from an EA other than the current spawner is dropped with a warning.
- **The chart route.** `POST /charts/<symbol>` on the hub's port, from `127.0.0.1` alone and with no body, marks the symbol in use and answers `{"ok": true, "result": "published"}` while an EA publishes it, `{"ok": true, "result": "failed", "reason": "<the spawner's reason>"}` while its chart's failure stands, and otherwise requests its chart and answers `{"ok": true, "result": "requested"}`. Another method answers 405, a path naming no symbol 404, another host 403. Any other request goes on to the WebSocket handshake.
- **Idle charts.** A symbol is in use while a consumer subscribes to its ticks or bars, and for `MT5_CHART_IDLE_SECONDS` after its last use: a post naming it, a subscription to it starting or ending, its EA's hello, the spawner's `chart_kept`. The hub tells the spawner to close the chart of a symbol out of use for that long, as `close_chart`, once per spawner connection, and again once the symbol is out of use for another period after a `chart_kept`; the spawner's own chart never closes. A later use requests the chart again.

The EA, `ticks.mq5`, one per symbol on its own chart:

- It publishes one `trade_transaction` frame per `OnTradeTransaction` call whose symbol is its own, in the order the calls arrive, whatever the magic; the spawner also each one that names no symbol, such as a pending order's delete or modify sent without one. Every EA receives the account's whole transaction stream, so the filter publishes each transaction once; a transaction of a symbol no chart publishes, naming one, reaches the hub zero times.
- While the hub wants its ticks it publishes them from its tick database — `COPY_TICKS_INFO`, the ticks that change the bid or the ask — from the moment they are wanted, read at every tick and again on its timer, since the terminal queues no tick event while one is queued or handled. It publishes the bar that closed for each wanted timeframe, found on its timer and its ticks when the forming bar's open moves.
- Every `RelaySeconds` it sends `server_time` and its symbol's commission schedule.
- It reads the hub on its timer and on every tick, so the vendored library answers the hub's pings and acknowledges a close. While disconnected it reconnects every `ReconnectIntervalSec` on `TimeLocal()`, and it publishes no ticks or bars until the reconnected hub wants them again.
- On `duplicate` it closes its own chart, which unloads it. The spawner instead keeps its chart and says hello again on each reconnect, since the publisher may be its own terminal's earlier connection, which the hub drops at most 50 s after its last pong while its writes to it drain; still refused `HubPingTimeoutSec` after its first refusal, it closes its chart like any other duplicate.
- The spawner, on `open_chart`, selects the symbol and opens an M1 chart of it with the template `ChartTemplate`, unless a chart of the symbol already runs this EA or a chart it opened for the symbol is still open — the template's EA loads only once the chart has processed the template. It answers `chart_opened`, or `chart_failed` naming the call that failed — `SymbolSelect`, `ChartOpen` or `ChartApplyTemplate` — and its error.
- The spawner, on `close_chart`, first counts the account's open positions and pending orders on the symbol: while it holds any, the chart stays and the spawner answers `chart_kept`. Otherwise it closes the symbol's chart — the one it opened, or for a publisher that predates it, the chart of the symbol running the EA — then takes the symbol out of Market Watch, and deselects it all the same when it finds no such chart. The terminal refuses that while a chart of the symbol is open or it has open positions, so a refused deselect is tried again on each timer tick until it succeeds or the symbol's chart opens again, and only every 15 minutes, logged once, while positions or orders hold the symbol.

---

## What the image provides

The repository's `Dockerfile` builds one image that runs the terminal, the HTTP server and the hub: a MetaTrader 5 terminal under Wine with the EA compiled in, the HTTP server under a Windows Python and the hub under a Linux Python, headless under Xvfb. Its server and EA are the checkout's, and everything else is downloaded at build time; it carries no credential, login, server name or server list. A release publishes it as `ghcr.io/swing-traders/mt5-connector-server:<tag>`, `<tag>` the release tag `v<version>` with its `+` replaced by `-`.

| Component | Where it is pinned |
|---|---|
| the base, `mambaorg/micromamba` on Debian 13 | by tag in the final stage's `FROM` |
| WineHQ stable, amd64 and i386 | `ARG VERSION_WINE` |
| wine-mono | `image/artifacts.txt` |
| the Windows Python, python.org's x64 installer, at `C:\Python` | `image/artifacts.txt` |
| the Linux Python, the base environment at `/opt/conda` | `environment.yml` |
| `mt5-connector-server`, one wheel built from the checkout into both Pythons | `VERSION` |
| the Windows Python's dependencies, `MetaTrader5` among them | `image/requirements-wine.txt`, by hash |
| the terminal's web installer, `mt5setup.exe` | `image/artifacts.txt` |

### Running it

A run gives the container:

| What | How |
|---|---|
| the account and the spawner's symbol | `MT5_LOGIN`, `MT5_PASSWORD`, `MT5_SERVER` and `MT5_SPAWNER_SYMBOL`, as environment variables |
| the server's other settings, where their defaults do not suit | the optional variables under [Settings](#settings) |
| the terminal's history store | a volume on `/home/mt5/.wine/drive_c/Program Files/MetaTrader 5/Bases`, one per account |
| time to stop | a stop timeout of 190 s |

```bash
podman run -d --name mt5-alpha \
  -e MT5_LOGIN -e MT5_PASSWORD -e MT5_SERVER -e MT5_SPAWNER_SYMBOL=XAUUSD \
  -v mt5-alpha-bases:"/home/mt5/.wine/drive_c/Program Files/MetaTrader 5/Bases" \
  --stop-timeout 190 \
  ghcr.io/swing-traders/mt5-connector-server:<tag>
```

- The HTTP server listens on port 5000 and the hub on 9000, inside the container ([Network](#network)). The image fixes `MT5_API_PORT` and `MT5_HUB_PORT`, since the EA's templates are written against the hub's port.
- `MT5_SPAWNER_SYMBOL` names a liquid symbol the account has, by the broker's own name, exactly as the terminal lists it: some brokers suffix every symbol (`XAUUSD.r`).
- The history volume holds `Bases` and nothing else. The image ships the store empty, so a fresh volume starts with nothing from it, and a new image started on the same volume starts with the history the last one synced.
- The runtime user is `mt5` (UID/GID 65532), with `WINEPREFIX=/home/mt5/.wine`, `WINEARCH=win64` and `WINEDEBUG=-all`. The image sets `MT5_TERMINAL_PATH` to the portable install's `terminal64.exe` and `MT5_PYTHON_DIR` to the Windows Python's directory.

### Settings

The HTTP server reads its settings from the environment once at start; a missing or malformed one stops it with an error naming the setting, and no error carries a credential's value:

| Variable | Setting | Default |
|---|---|---|
| `MT5_TERMINAL_PATH` | the terminal executable, as Windows names it (`C:\Program Files\MetaTrader 5\terminal64.exe` for a default install) | required |
| `MT5_LOGIN` | account number | required |
| `MT5_PASSWORD` | account password | required |
| `MT5_SERVER` | trade server name | required |
| `MT5_SPAWNER_SYMBOL` | the symbol of the spawner's chart, the terminal's `[StartUp]` `Symbol` | required |
| `MT5_LOGIN_TIMEOUT_MS` | initialize timeout | `60000` |
| `MT5_API_HOST` | bind address | `0.0.0.0` |
| `MT5_API_PORT` | HTTP port | `5000` |
| `MT5_API_THREADS` | waitress threads, one of them kept free for `/health` and the relays; at least 2 | `5` |
| `MT5_CLOCK_CHECK_SECONDS` | interval between re-verifications of the broker's clock | `300` |
| `MT5_CLOCK_SAMPLE_MAX_AGE_SECONDS` | how long a relayed server-time sample stays fresh, and how long the terminal must stay connected before a sample verifies the clock | `30` |
| `MT5_CLOCK_BOOTSTRAP_SECONDS` | how long the server waits at start for the first sample that verifies the clock | `120` |
| `MT5_BROKER_TZ` | the zone the broker's clock follows, as an IANA name | `America/New_York` |
| `MT5_BROKER_OFFSET_HOURS` | hours the broker's clock runs ahead of that zone | `7` |
| `MT5_HISTORY_RETRY_SECONDS` | the `Retry-After` of an unproven history answer, a read of a symbol whose chart is requested, a commission schedule not yet relayed, or a call refused with every slot taken: how long the client waits before asking again. The server posts a symbol whose chart is requested to the hub again at most this often | `5` |
| `MT5_FLOOR_TTL_SECONDS` | how long a measured history floor is used before it is measured again | `900` |
| `MT5_HUB_PORT` | the hub's port, which the server posts the symbols it reads to on `127.0.0.1` | `9000` |
| `MT5_CHART_IDLE_SECONDS` | how long a symbol stays in use after its last use; the server posts a symbol it reads at most once per half of it, unless the hub's last answer requested its chart | `900` |

The hub reads five, and stops naming the first that is malformed:

| Variable | Setting | Default |
|---|---|---|
| `MT5_HUB_PORT` | the hub's port, bound on all interfaces, as the HTTP server reads it | `9000` |
| `MT5_API_PORT` | the HTTP server's port, which the hub posts each relayed server time and commission schedule to on `127.0.0.1` | `5000` |
| `MT5_BROKER_TZ` | the zone the broker's clock follows, as the HTTP server reads it | `America/New_York` |
| `MT5_BROKER_OFFSET_HOURS` | hours the broker's clock runs ahead of that zone, as the HTTP server reads it | `7` |
| `MT5_CHART_IDLE_SECONDS` | how long a symbol out of use keeps its chart open, as the HTTP server reads it | `900` |

The credentials reach the processes as environment variables at start; the image never carries them.

### The processes

| Process | Runs under | Started by |
|---|---|---|
| the HTTP server | the Windows Python, under Wine in the terminal's prefix: the `MetaTrader5` package it drives the terminal through is Windows-only | `mt5-connector-server`, or `python -m mt5connector.server.app` |
| the hub | the image's Linux Python | `mt5-connector-hub`, or `python -m mt5connector.server.ws_server` |

- Both Pythons install the one wheel of `mt5-connector-server` the build makes from the checkout. Its dependencies pin `MetaTrader5` on Windows alone, `numpy` at 2.2.1 because later releases crash under Wine, and `tzdata` because the Windows Python has no zone database. The hub imports none of these, but installing the distribution brings them all, and numpy 2.2.1 publishes wheels for CPython 3.10 to 3.13 only.
- The hub posts every relayed server time and commission schedule to `127.0.0.1`, the server posts the symbols it reads to the hub's chart route on `127.0.0.1`, and each accepts those routes from `127.0.0.1` alone: the two share a loopback.
- At start the server initializes the terminal named by `MT5_TERMINAL_PATH` with the account's login, password and server, and exits non-zero when that fails.
- Once the terminal reports itself connected with the broker's symbol list loaded, the server asks it for the definition of `MT5_SPAWNER_SYMBOL`, and exits non-zero with `spawner symbol <symbol> is not listed at the broker` when it has none: the startup script never runs on a chart without a symbol, so no EA would ever relay.
- It then answers HTTP 503 on every route but `/health` and the relays until a server-time sample from an EA verifies the broker clock, and exits non-zero when none does within `MT5_CLOCK_BOOTSTRAP_SECONDS`, or when any sample it measures, then or later, puts the trade server more than 120 s from the server's clock. The terminal, the spawner's EA and the hub must therefore be up within that window of the server's start; whatever starts the server decides whether to start it again.
- `/health` answers HTTP 200 once the clock is verified, and 503 whenever it is not.

### Boot

The entrypoint, `/opt/mt5/entrypoint.sh` under `tini`, prints its own steps, prefixed `mt5:`, and the terminal's logs; never the terminal's window titles, which name the login and the server.

1. Starts Xvfb at 1920x1080, and x11vnc in a debug image.
2. **Restores the baked state,** before any Wine process of the run starts, since Wine reads its registry as it does. `rsync` copies `/opt/mt5-baked` back, comparing files by content: `terminal/` over the terminal directory and `metaquotes/` over the AppData tree, each deleting whatever the baked copy does not hold, `Bases` excluded, and the three registry files over the Wine prefix's. It is bounded at 40 s. Everything an earlier run of the container wrote — `Config\` with its server list and saved accounts, `Profiles\`, the chart profiles, the logs, the AppData tree, the registry — is undone, `Bases` is never written, and the log says so in one line.
3. **Fetches the server list.** The terminal logs in only to a trade server its `Config\servers.dat` lists. The entrypoint launches it on the account without the startup script, searches `MT5_SERVER` on the first page of File → Open an Account — which adds the broker's servers — and closes it, which keeps them.
4. Launches `terminal64.exe /config:<the ini copy> /portable`. `ticks_setup` opens the spawner's chart on the spawner symbol. **Starts the log tail,** `/opt/mt5/tail_logs.py` under the Linux Python, which streams the terminal's journal (`logs\<YYYYMMDD>.log`) and its experts log (`MQL5\logs\<YYYYMMDD>.log`) into the container log from the start of this boot:
   - each line converted from the terminal's UTF-16LE to UTF-8, its byte-order mark and carriage return stripped, and prefixed `terminal:` or `experts:`, its text otherwise written as the terminal wrote it;
   - each log followed onto the next UTC day's file when it appears.
5. **Adds `127.0.0.1` to the WebRequest allowlist,** which no ini key sets: on the Experts tab of Tools → Options it ticks "Allow WebRequest for listed URL" where unticked, adds `127.0.0.1` to the list, and applies the dialog. The list must then hold that one URL.
6. Starts the hub (`mt5-connector-hub`, the Linux Python) and the HTTP server (`mt5-connector-server`, the Windows Python).
7. Supervises Xvfb, the terminal, the hub and the server: the first of them to exit ends the container with its status, and whatever started the container decides whether to start it again. A lost broker session is the terminal's own to recover and ends nothing, and so does a log tail that exits: the log names its status and the tail is restarted, skipping what the logs hold at its first read — so the lines written between the stopped tail's last read and the restart are lost.

**The dialogs are driven by control identity**, by `/opt/mt5/terminal_gui.py` under the Windows Python, through Win32 messages and never a screen position:

- the terminal's main window is found by its window class;
- a dialog is opened by the command id its menu path holds in `terminal64.exe`'s own menu resources;
- a control is found by its id and class, the Options page by its tab's name.

The WebRequest list's text cannot be read, so its row count is what confirms the URL. Each action prints one line and is bounded at 40 s; a window or control it cannot find fails the start, naming it. The server's name reaches the driver through `MT5_SERVER` alone.

A terminal window, dialog or control that never appears fails the start at once, naming the step. What the entrypoint cannot see — a server search that found nothing, a refused login — ends the start through the server's own refusal: it exits non-zero when no EA sample verifies the broker clock within `MT5_CLOCK_BOOTSTRAP_SECONDS`, and the container ends with it. It also exits non-zero on a spawner symbol the broker does not list.

### Stop

A stop closes the terminal as a user would — `taskkill` without `/F`, which posts `WM_CLOSE` — and waits up to 60 s for it, then stops the log tail within 10 s once it has streamed the terminal's last lines, the server and the hub together within 10 s, and Xvfb last within another 10 s. It kills whatever is still running and ends the container with the terminal's own exit status, or 0 when no terminal was running.

Those bounds together, with a call in flight when the stop arrives, stay at least 30 s under the 190 s stop timeout a run gives the container, so a stop ends the terminal by its close, never a kill.

### The terminal

The terminal is started as `terminal64.exe /config:<setup.ini> /portable`, so it keeps its data — the `MQL5` tree, its profiles and templates — under its own install directory. The paths below are relative to that directory.

#### setup.ini

The image bakes the sections without a credential at `/opt/mt5/setup.ini`. At every start the entrypoint writes a copy into `/tmp/mt5`, readable by the runtime user alone, adding `[Common]` `Login`, `Password` and `Server` from the environment and `[StartUp]` `Symbol` from `MT5_SPAWNER_SYMBOL`.

| Section | Key | Value |
|---|---|---|
| `[Common]` | `Login`, `Password`, `Server` | `MT5_LOGIN`, `MT5_PASSWORD` (quoted) and `MT5_SERVER` |
| | `AutoConfiguration` | `true` |
| | `ProxyEnable` | `false` |
| `[Charts]` | `Profile` | `default`, emptied of its charts before every launch, so the terminal starts with none |
| | `MaxBars` | `100000000`, the terminal's ceiling: a larger value is clamped to it. It also bounds the span one history read covers |
| `[Experts]` | `AllowDllImport` | `1` |
| | `Enabled` | `1`: algorithmic trading on, without which the terminal refuses every order the adapter sends |
| | `WebRequest` | `1` |
| `[StartUp]` | `Script` | `ticks_setup` |
| | `Symbol`, `Period` | the chart the script runs on and `M1`: the script opens the spawner's chart on its symbol, then closes its own. The symbol is `MT5_SPAWNER_SYMBOL` |

The terminal must know the trade server `MT5_SERVER` names — its `Config\servers.dat` lists it — before the login can succeed: a server a fresh install does not know is added by searching for its name once (File → Open an Account), which the image does at every boot.

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

The image's build places and compiles them with `image/install-ea.sh`, from the wheel in the Windows Python, under Xvfb. It fails unless both `.ex5` appear and each `/log:` file's `Result:` line reads `0 errors, 0 warnings`, and its log prints both lines.

On each terminal start `ticks_setup` opens an M1 chart of the symbol it runs on, which the server's start checks the broker lists, and applies the template `ticks_spawner.tpl`, which attaches the EA as the spawner, then closes the chart it ran on, which would hold a `CHARTS_MAX` slot for nothing. The terminal starts with no chart, so a chart already running the EA at start is a boot defect, which the script prints, naming the chart's symbol, and opens and closes nothing. Every other chart opens on demand: the spawner opens it with `ticks.tpl`, which attaches the EA for that symbol.

- The terminal keeps at most `CHARTS_MAX` (100) charts open, the spawner's included.
- A chart the spawner opens on demand that does not open — a symbol the venue does not list, or one beyond `CHARTS_MAX` — is reported by the spawner, and a read of that symbol that posts it to the hub fails naming the reason, until a chart of it opens or its EA says hello.
- The spawner refused as a duplicate keeps its chart and says hello again every `ReconnectIntervalSec`; still refused `HubPingTimeoutSec` after its first refusal, it closes its chart like any other duplicate.
- A chart closes once its symbol has been out of use for `MT5_CHART_IDLE_SECONDS`: the spawner closes it and takes the symbol out of Market Watch, where the terminal would keep processing its ticks.
- A symbol with open positions or pending orders never closes idle: the spawner keeps its chart and the hub counts it in use for another `MT5_CHART_IDLE_SECONDS`.
- A deselect the terminal refuses after the close is tried again on the spawner's timer until it succeeds or the chart reopens, less often while positions or orders hold the symbol.

#### The chart template

Two templates attach the EA: `ticks_spawner.tpl`, with `Spawner=true`, and `ticks.tpl`, with `Spawner=false`. Each goes in `MQL5\Profiles\Templates\` and in `Profiles\Templates\`, the two places a template applied by bare name is looked for, and must be in the form the terminal saves templates in:

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
PollMilliseconds=250
Spawner=false
ChartTemplate=ticks.tpl
HubPingTimeoutSec=50
</inputs>
</expert>
```

| EA input | Meaning |
|---|---|
| `Server` | the hub's URL, `ws://127.0.0.1:<MT5_HUB_PORT>` |
| `ReconnectIntervalSec` | the fewest seconds between two attempts to reach the hub while the EA is not connected |
| `RelaySeconds` | the interval between server-time frames and the symbol's commission schedule. It must stay below `MT5_CLOCK_SAMPLE_MAX_AGE_SECONDS`, or the clock never verifies |
| `PollMilliseconds` | the EA's timer: the interval between reads of the hub, of the symbol's ticks and closed bars, and of the connection; the terminal fires it no faster than every 10–16 ms |
| `Spawner` | `true` in `ticks_spawner.tpl`, `false` in `ticks.tpl`: whether the EA opens and closes the charts the hub asks for |
| `ChartTemplate` | the template the spawner applies to a chart it opens, `ticks.tpl` |
| `HubPingTimeoutSec` | how long after its first refusal the spawner, answered `duplicate`, says hello again before it closes its chart: the longest the hub takes to drop a dead connection while its writes to it drain, `50` s — its 10 s ping interval, its 30 s ping timeout and the 10 s close timeout of its WebSocket server |

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
PollMilliseconds=250
Spawner=false
ChartTemplate=ticks.tpl
HubPingTimeoutSec=50
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

The image's build writes both templates from `image/ticks.tpl.in`, filling in the hub's port and `Spawner`.

#### The allowlist

Each EA's socket reaches only a host on the terminal's allowlist — Tools → Options → Expert Advisors → "Allow WebRequest for listed URL", checked, with `127.0.0.1` on its list:

- The list lives in memory, and every terminal start empties it: the host is added again after each start, until then no EA can reach the hub.
- `setup.ini` cannot set it: MT5 ignores the `WebRequestUrl` key, which is MT4's.
- Confirming the dialog re-initialises every attached EA, which then connects again.

### Network

- The adapter needs two ports: the HTTP server's `MT5_API_PORT` and the hub's `MT5_HUB_PORT`. It derives the hub's URL from `server_url` on port 9000 unless its config names `ws_url`.
- There is no authentication: the API and the hub trust their network. The image publishes no host port, loopback included: both are reachable on the container network alone, and publishing either port is a design change.

### Building

```bash
podman build --format=docker -t mt5-connector-server .
podman build --format=docker --build-arg DEBUG=1 -t mt5-connector-server-debug .
```

- The build context is the repository root, and `.dockerignore` admits `environment.yml`, `VERSION`, `packages/` and `image/` alone. Podman builds with `--format=docker`: its default OCI format drops the base image's `SHELL`, through which the build's `RUN` steps run in the activated environment. Docker needs no flag.
- The download stage takes `image/artifacts.txt`, `image/requirements-wine.txt` and `image/first-start.sh` on their own, and the checkout comes in once the terminal is installed, so a change to a package or a boot script rebuilds neither Wine nor the terminal.
- The Linux Python is the checkout's `environment.yml`, installed whole as the base environment — Python, the client editable from the checkout's copy at `/opt/mt5-connector`, and its NautilusTrader pin — and then made root-owned and read-only.
- One wheel of `packages/server`, built with that Python, installs there with its dependencies, for the hub, and into the Windows Python with `--no-deps`, after `image/requirements-wine.txt`'s hashed dependencies. Nothing of this repository's own comes from an index, and `pip check` fails the build in either Python whose installed dependencies no longer satisfy the server's pins.
- `--build-arg DEBUG=1` adds x11vnc, which the entrypoint starts on the display listening on the container's loopback (`127.0.0.1:5900`) and nowhere else; the default build carries no VNC at all, and any other `DEBUG` value fails the build.
- The image is about 6.3 GB.

The build also:

- **Compiles the EA** and **writes its two templates**, as [The EA and the startup script](#the-ea-and-the-startup-script) and [The chart template](#the-chart-template) state.
- **Empties the history store** (`Bases`) after the terminal's first start, so a fresh volume mounted on it starts with nothing from the image.
- **Bakes the terminal's start state** into `/opt/mt5-baked`, owned by root, which every boot restores:
  - `terminal/`, the terminal directory once the EA is installed, with the chart profile `default` emptied in whichever of `Profiles\Charts\` and `MQL5\Profiles\Charts\` exists — the build log names each it empties — its `logs\` and `MQL5\logs\` empty, and without `Bases`;
  - `metaquotes/`, the Wine user's `AppData\Roaming\MetaQuotes`;
  - `registry/`, Wine's `system.reg`, `user.reg` and `userdef.reg`, as the build's last Wine session saved them.

### Pins

- Every apt package the `Dockerfile` pins takes its version from an `ARG` named `VERSION_<PACKAGE>` — `VERSION_WINE` — used at every pin.
- `image/artifacts.txt` lists every file the download stage fetches, one per line as `<name> <url> <sha256>`: its URL carries the version, its hash pins the bytes, and nothing else states that version. One build step fetches them all and checks every hash, so a changed upstream file fails the build.
- A downloaded artifact is bumped by editing its row's URL and `sha256sum` and nothing else. The WineHQ packages are bumped by `VERSION_WINE` alone.
- `image/requirements-wine.txt` is a pip `--require-hashes` file, binary wheels only: the server's dependency closure for the Windows Python, every package at its version with the hash of its `cp313` `win_amd64` wheel or its pure wheel. A change to the server's dependencies, or to the Windows Python's minor version, updates it in the same change.
- numpy is held at `2.2.1` by the server's own pin: later releases crash on import under Wine 10.0, calling `ucrtbase.dll.crealf`, which Wine 10.0 does not implement.

### The terminal build

The build starts the installed terminal once with `/portable`, waits up to 180 s for its log to show the start and the finished first-start MQL5 recompile — so a running container does not pay that recompile — and fails the build if the log never shows both. It writes the build number the terminal logged to `/home/mt5/.terminal-build`:

```bash
podman run --rm --entrypoint cat mt5-connector-server /home/mt5/.terminal-build
```

`mt5setup.exe` is a web installer: its own bytes are pinned, but it installs whatever terminal build MetaQuotes currently serves, so two builds from identical inputs can carry different terminal builds. Nothing stops the terminal's own update check: it never applies an update by itself, so the build an image baked is the one it runs. The installer exits 1 after a successful install, so the build checks the terminal's first start rather than the installer's exit code; without network access it hangs instead of failing, so the build gives it 600 s and fails the step when that runs out.

The files the build downloads itself and the Windows Python's packages are pinned by hash. The base images are pinned by tag, the Debian packages Wine depends on follow trixie's current updates, and the Linux Python's packages, with the build backend both projects build with, resolve at build time as `environment.yml` and the two `pyproject.toml` files declare them.

### The image tests

`tests/image/test_mt5_image.py` starts a built image on an account no broker holds when `MT5_IMAGE_TEST` names it, on docker where it is installed and podman otherwise:

```bash
MT5_IMAGE_TEST=localhost/mt5-connector-server pytest tests/image
```

It pins that:

- the start ends through the server, printing the account's password nowhere;
- the first supervised process to exit — the hub, Xvfb, the terminal during the boot — ends the container with its status, naming it;
- a display that never comes up, a restore that never returns and a driver action that never returns each fail the start naming the step and its bound;
- a stop closes the terminal, stops the log tail after it and ends the container within the stop timeout, killing nothing;
- the main-window action reports the window;
- a boot leaves WebRequest allowed for one URL;
- a restart restores the baked state — a file added under `Config\` or the AppData tree removed, a same-length rewrite of a baked file with its timestamp undone, a value written into the terminal's registry key gone — and leaves `Bases` alone;
- the terminal's journal and experts log reach the container log as UTF-8 under their prefixes, a journal line naming the login and the server arriving as the terminal wrote it;
- the driver fails naming a control a dialog lacks;
- a client's connect to the server, which never comes up, is refused at once, carrying no credential, while the image boots and once it has exited, and building the clients' configs asks no server;
- both ports published on the loopback at host ports the runtime picks read back off the running container, and the HTTP server's is reachable while the server boots.

Its tests of the driver's menu and tab lookups and of the log tail — following the journal onto the next day's file, each line written verbatim as UTF-8 under its prefix, a restarted tail skipping what the logs hold at its first read, a half-written character at a restart or a stop — run without an image.
