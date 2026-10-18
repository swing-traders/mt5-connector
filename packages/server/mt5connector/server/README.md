# mt5-connector-server

The terminal side of mt5-connector: the hub that carries the terminal's ticks, closed bars and trade transactions and answers the calls the EAs serve, every epoch in true UTC, and the source of the EA, its history indicator and the startup script that run inside the terminal. `mt5-connector-client`, the NautilusTrader adapter, talks to it.

Releases are wheels on the fork's package index, versioned together with the client's; `<version>` is a release's version, the `VERSION` file's value at its tag, the latest listed on the [releases page](https://github.com/swing-traders/mt5-connector/releases):

```bash
python -m pip install --extra-index-url https://swing-traders.github.io/mt5-connector/simple/ "mt5-connector-server==<version>"
```

| Part | What it is |
|---|---|
| `mt5connector.server.ws_server` | the hub, started as `mt5-connector-hub` |
| `mt5connector/server/mql5/` | the EA (`Expert/ticks.mq5`), its history indicator (`Indicators/hub_history.mq5`), its startup script (`Scripts/ticks_setup.mq5`) and the MQL5 libraries they include (`Include/`) |
| `mt5connector.server.wire` | the wire vocabulary, copied from the client's `mt5connector.wire` when the wheel is built |

---

## The hub

The hub (`mt5connector.server.ws_server`, port `MT5_HUB_PORT`) is the one endpoint the EAs inside the terminal and the consumers — the client's `HubSession`, one per data and execution client — connect to. Every EA serves one role, set by its chart's template; a consumer subscribes to streams and asks calls. Every frame is binary, encoded and decoded by `mt5connector.server.wire.binary` with the calls of `mt5connector.server.wire.calls`.

| Role | What its EA serves |
|---|---|
| `spawner` | the account: its reads (account, open positions and orders, deal and order history, the symbol list, terminal facts), the charts the hub requests, the server-time relay and the transactions that name no symbol |
| `publisher` | one symbol: its ticks, closed bars and transactions, its reads and bar history, its trade commands and their outcomes, and its commission schedule and trading sessions |
| `history` | one symbol's tick history |

- The hub addresses an EA by its role and symbol, the spawner by its role alone. The first EA to say hello for a role and symbol serves it; the hub answers a later one `duplicate`, closes it, and logs a warning. A refused duplicate's frames are dropped silently.
- The hub keeps no event store and replays nothing: a consumer that reconnects subscribes again and receives what is published from then on.
- A frame the hub cannot handle — text, another version, out of its kind's layout, of a kind its sender's role does not send — is answered with an `error` frame and the connection closed with code 1002, logged as a warning; a frame of an unknown kind is logged and skipped.

### Frames

Every frame is a little-endian header, then a payload of `length` bytes:

| Offset | Field | Type | What it is |
|---|---|---|---|
| 0 | version | u8 | `2`; any other value is refused |
| 1 | kind | u8 | the frame's kind, below |
| 2 | flags | u16 | an answer's: `1` its result rather than an error, `2` its last part; 0 on every other kind |
| 4 | correlation | u64 | a request's id on a request, its answer and a busy; an op's id on a subscribe, unsubscribe and ack; 0 otherwise |
| 12 | count | u32 | the records a tick frame, a request's arguments or an answer part carries; 0 otherwise |
| 16 | length | u32 | the payload's bytes |

Inside a payload a `str` is a u16 byte count, then that many bytes of UTF-8; a StrEnum member travels as its value's `str`. `MqlTick` and `MqlRates` travel as `StructToCharArray` packs them, 60 bytes each with no padding: `MqlTick` as `<qdddQqId` (`time`, `bid`, `ask`, `last`, `volume`, `time_msc`, `flags`, `volume_real`), `MqlRates` as `<qddddQiQ` (`time`, `open`, `high`, `low`, `close`, `tick_volume`, `spread`, `real_volume`). Every other struct travels as its field sequence:

- each member in the package's field order, as the MQL5 function that reads it returns it: an `*InfoInteger`/`*GetInteger` property — a `bool` as 0 or 1, an enum, a `datetime` in epoch seconds, a `long` `*_msc` in epoch milliseconds, a `ulong` ticket — as an int64, a `*InfoDouble` as a float64, a `*InfoString` as a `str`; `MqlTradeRequest`, `MqlTradeResult` and `MqlTradeTransaction` members travel by the same rule;
- `mirror.py` declares each member's MQL5 type beside it, from the MQL5 reference page its struct names;
- a `SymbolInfo` record travels its `name` first, as a `str` key ahead of its 95 typed members;
- a `SymbolListing` record, `symbols_get`'s, travels a symbol's `name` key and its definition members alone, in `SymbolInfo`'s order: the members `mirror.py` marks `selected` — the symbol's market state, which the terminal refuses for a symbol outside Market Watch — are left out;
- `TerminalInfo` travels without `tradeapi_disabled`, which no MQL5 property backs, and without `community_balance`, which the terminal refuses without an MQL5 community login; an `order_send` result travels as `MqlTradeResult` alone, without the request the package appends to it.

| Kind | Code | From → to | Payload |
|---|---|---|---|
| `hello` | 1 | EA, consumer → hub | `role` (`ea` or `adapter`); an EA's also its `chart_role` and `symbol` |
| `subscribe`, `unsubscribe` | 2, 3 | consumer → hub | `stream`; for `ticks` a `symbol`, for `bars` a `symbol` and a `timeframe` from `M1` to `W1`; nothing for `trade_transactions` |
| `ack` | 4 | hub → consumer | nothing: the op it acknowledges is the correlation, queued ahead of every frame the op routes |
| `error` | 5 | hub → EA, consumer | `message`, what the hub could not handle; the connection closes after it |
| `wanted` | 6 | hub → a publisher | `symbol`; `ticks` (u8), whether a consumer subscribes to its ticks; then `count` int32 `ENUM_TIMEFRAMES` values whose bars a consumer subscribes to. Sent on the publisher's hello and on every change |
| `open_chart`, `close_chart` | 7, 8 | hub → spawner | `chart_role` and `symbol` of the chart to open, or of the chart out of use to close |
| `chart_opened` | 9 | spawner → hub | `chart_role`, `symbol`: a chart of it is open |
| `chart_failed` | 10 | spawner → hub | `chart_role`, `symbol`, `error` (int32): no chart opened, and the failing call's MQL5 error |
| `chart_kept` | 11 | spawner → hub | `chart_role`, `symbol`, `positions` and `orders` (int32): the chart stays open for the account's holdings on the symbol |
| `duplicate` | 12 | hub → an EA | `chart_role`, `symbol`: another EA serves them; the connection closes after it |
| `tick` | 13 | publisher → hub → consumers of the symbol's ticks | `symbol`, then `count` `MqlTick` records |
| `bar` | 14 | publisher → hub | `symbol`, `timeframe` (int32 `ENUM_TIMEFRAMES`), one `MqlRates` record, once the bar has closed |
| `bar` | 14 | hub → consumers of the series | `symbol`, `series` (`M1` to `W1`), one `MqlRates` record followed by its close, `time_close` (int64): 68 bytes |
| `trade_transaction` | 15 | publisher, spawner → hub → consumers of `trade_transactions` | the `MqlTradeTransaction`, `MqlTradeRequest` and `MqlTradeResult` records one `OnTradeTransaction` call receives |
| `server_time` | 16 | spawner → hub | `symbol`, the spawner's; `trade_server`, `current`, `gmt` (int64); `connected` (u8) |
| `commissions` | 17 | publisher → hub | `symbol`; `ret` and `last_error` (int32) of its `SymbolInfoCommissions` call; a u16 rule count, each rule its `currency`, its five enum fields by their `EnumToString` names and a u16 tier count, each tier its `mode` and `volume_type` by name, `value`, `min_value`, `max_value`, `range_from`, `range_to` (float64) and `currency` |
| `sessions` | 18 | publisher → hub | `symbol`; then `quote` and `trade`, seven days each, Sunday first, each a u16 session count and each session its `from` and `to` (int32 seconds from the broker day's 00:00); then `last_error` (int32) |
| `request` | 19 | consumer → hub → EA | `call` (u16), then `count` arguments, each its index in the call's arguments (u8, ascending) and its value |
| `answer` | 20 | EA → hub → consumer | `part` (u32), `last_error` (int32); a result's `count` records; or an error's `kind` (u8), `message`, and for a deferring kind `retry_after_ms` (u32) |
| `busy` | 21 | EA → hub → consumer | `bound_s` (u32): the EA is about to wait on the request at most that long |

### Calls

Each call is a row of the registry in `wire/calls.py`, served by the EA of its role, or by the hub:

| Call | Code | Served by | Arguments | Answer | Deadline |
|---|---|---|---|---|---|
| `account_info` | 1 | spawner | — | `AccountInfo` | read |
| `terminal_info` | 2 | spawner | — | `TerminalInfo` | read |
| `symbols_get` | 3 | spawner | `group` | `SymbolListing` rows | read |
| `symbol_info` | 6 | publisher | `symbol` | `SymbolInfo` | read |
| `symbol_info_tick` | 7 | publisher | `symbol` | one `MqlTick` | read |
| `positions_get` | 8 | spawner | `symbol`, `group`, `ticket` | `TradePosition` rows | read |
| `orders_get` | 9 | spawner | `symbol`, `group`, `ticket` | `TradeOrder` rows | read |
| `history_orders_get` | 10 | spawner | `date_from`, `date_to`, `group`, `ticket`, `position` | `TradeOrder` rows | read |
| `history_deals_get` | 11 | spawner | `date_from`, `date_to`, `group`, `ticket`, `position` | `TradeDeal` rows | read |
| `order_send` | 12 | publisher of the request's `symbol` | `request` | `MqlTradeResult` | command |
| `copy_rates_range` | 13 | publisher | `symbol`, `timeframe`, `date_from`, `date_to` | bars | history |
| `copy_ticks_range` | 15 | history | `symbol`, `date_from`, `date_to`, `flags` | `MqlTick` rows | ticks |
| `ranges` | 17 | the hub | `symbol` | the terminal's `maxbars` (int64), then each floor held: `series`, `floor`, `measured_at`, `generation` (int64) | read |
| `outcomes` | 18 | publisher | `symbol`, `request_ids` | per request id its outcome log holds, the `MqlTradeTransaction`, `MqlTradeRequest` and `MqlTradeResult` of its `REQUEST` transaction, back to back | read |
| `commission_schedule` | 19 | the hub | `symbol` | the schedule, laid out as a `commissions` frame's after its symbol | read |
| `trading_sessions` | 20 | the hub | `symbol` | the sessions, laid out as a `sessions` frame's after its symbol | read |

- Arguments travel by the inventory's parameter kinds: integers as int64, strings as `str`, a time as int64 epoch seconds, `request` as an `MqlTradeRequest` record, `request_ids` as a u32 count and each id as a u32. A required argument missing, an argument out of the call's order or of the wrong type, or a history query in none of its call forms is a frame the hub cannot handle; a session refuses it before sending. A bar read of a timeframe no series serves is refused too.
- `last_error` is the MQL5 `GetLastError()` the EA read after the call, folded into every answer; the hub's own answers carry 0, but for a chart the spawner failed to open, which carry the error its `chart_failed` named.
- A result of rows travels in parts, numbered from 0, the last flagged; an EA's part carries at most 1,000,000 result bytes, which keeps its frame within the hub's 1 MiB message limit; an EA's last part of a history read carries a floor fact after its rows — a u8 count, 0 or 1, and for 1 the floor (int64 on the broker's clock, seconds for bars and milliseconds for ticks) and what set it, `stub` or `coarse`. The hub keeps that floor for `ranges` and passes the rows on without it.
- `order_send` is served through `OrderSendAsync`: its answer, the `MqlTradeResult` with its `request_id` and retcode 10008, means sent; the venue's verdict arrives as the `REQUEST` transaction carrying the same `request_id`. A publisher keeps the last 1,000 `REQUEST` transactions of its symbol, and `outcomes` answers those of the request ids it is asked; an id it no longer holds is absent from the answer.
- `ranges` answers the floors the hub holds for the symbol, in true UTC, beside the `maxbars` of a `terminal_info` the hub asks the spawner for it; `commission_schedule` and `trading_sessions` answer the latest relay of the symbol's publisher.

### Every request ends

Every request is answered once, by a result or an error, and nothing comes after it:

| Error | Answered by | `retry_after_ms` | When |
|---|---|---|---|
| `syncing` | EA, hub | yes | a history window the terminal's answers do not prove yet; or a commission schedule or trading sessions not yet relayed |
| `not_ready` | hub | yes | the broker clock is not verified |
| `no_publisher` | hub | yes | no EA serves the call's role and symbol: its chart is requested from the spawner — once per spawner connection — and the request answered at once |
| `invalid_window` | EA | no | a history window starting after the terminal's time |
| `overflow` | hub | no | the consumer has `MT5_HUB_PENDING_BOUND` requests pending, or the EA's outbound queue is full |
| `ea_gone` | hub | no | the EA left — a close, or its pong overdue — with the request pending |
| `timeout` | hub | no | the EA answered nothing by the call's deadline; logged as an error, and a later answer dropped with a warning |
| `ea_error` | EA, hub | no | the EA could not perform the call, its MQL5 error the answer's `last_error`; or the spawner failed to open the chart the call needs, or the last relay a hub-answered call reads was refused |

- The deferring kinds carry `MT5_HISTORY_RETRY_SECONDS` as `retry_after_ms` when the hub answers them; a session's `deferring` asks again after that delay until it is answered otherwise or its caller cancels.
- A call's deadline is its row's `Deadline`, a protocol constant of `calls.py` that binds the hub and the session alike, counted from the moment the request is sent:

  | Deadline | Constant | Calls |
  |---|---|---|
  | `read`, 5 s | `READ_DEADLINE_S` | every read |
  | `command`, 1 s | `COMMAND_DEADLINE_S` | a command's sent answer |
  | `history`, 45 s | `HISTORY_DEADLINE_S` | a bar history read |
  | `ticks`, 900 s | `TICK_BUSY_S` | a tick read, so that one queued at a history EA blocked in another's call outlasts that call until its own `busy` arrives |

- A `busy` moves the request's deadline to its bound plus 10 s (`BUSY_MARGIN_S`) from its arrival, holds off the EA's keepalive as long, and is passed on to the consumer.
- The session waits each request 5 s past the hub's deadline, so the hub's `timeout` reaches it first.
- The hub never waits on an EA inside a consumer's request: a request sent is a pending entry with its deadline, its answer passed on when it arrives, in any order the EA answers.
- A consumer that leaves leaves its requests pending on their EAs, their answers dropped.

### History

The terminal answers a history request with whatever it has synced, substitutes the nearest data it has, and answers a range it cannot serve with the same emptiness as one that has no data. A history read is answered only once the terminal's answers prove it, so a consumer never receives emptiness it cannot trust: a window nothing proves yet is `syncing`, and the consumer's retry reads it again.

- A bar read names its timeframe from `M1` to `W1`: a month has no fixed period, so `MN1` is refused.
- A window about a clock change is read in the broker window holding every stamp that reads into it — a skipped hour's stamps read at the offset before the change, a repeated hour's at either pass — and answered with those stamps alone.
- Every bar an answer carries and the hub pushes carries its close, `time_close`, in true UTC: a frame on the broker day's grid, H2 to W1, closes at the next grid open on the broker's clock, an hour more or less than its period across a clock change; a finer one a period of real time after its open. Of two bars of a series that open at one true-UTC second — about a spring change, one stamped in the hour the broker's clock skips and the one after it — the first is answered and pushed, and the other dropped with one warning per such hour.
- The terminal answers a range spanning more than `maxbars − 11` periods with nothing, so a longer bar window is read in spans within that, `maxbars` read from the terminal.
- A read the terminal answers with rows and a success error is proven: the rows are the answer. A read that answers no rows is proven empty by one of these facts, each read from the terminal, and is `syncing` without one:
  - **Before the floor**: the window lies wholly before the series' floor.
  - **The live edge**: the window lies after the second of the symbol's last quote; a window holding that second is not, since the quote is itself a record.
  - **Bracketed**: the terminal has a row before the window and a record after it. An empty span of a long bar window between spans that answered rows is bracketed by them.
- Every bar must lie on its timeframe's grid, or the read fails naming the row. In an answer that begins at the series' floor, a leading run of an intraday series spaced a whole number of days apart, ahead of its first two rows one period apart, is not that timeframe's data: it is dropped and the floor moves past it. Ticks must not go back in time.
- A series' floor is set by the stub — the one bar the terminal answers for a window before any plausible history; for ticks, the first tick from then — or by a coarse prefix at the series' start, at the first row past it. A stub never moves a floor the coarse prefix set back.

### Clock and true UTC

- Every epoch reaches a consumer in true UTC, and an EA on the broker's clock that `MT5_BROKER_TZ` and `MT5_BROKER_OFFSET_HOURS` set — New York plus seven hours by default — the hub converting by the era of each instant: every epoch field of a published frame and of an answer's records, every time argument of a request and a trade request's `expiration`. A zero epoch stays zero. An epoch in the broker's repeated hour reads as its first occurrence and one in its skipped hour at the offset before the change, with one warning per such hour. Every bar reaches a consumer with its close; of two bars of a series that open in one true-UTC period, a stream and an answer alike pass the first and drop the other, with a warning.
- The hub verifies the broker clock against the spawner's `server_time` samples, at any hour, market open or closed: a sample fresh for `MT5_CLOCK_SAMPLE_MAX_AGE_SECONDS`, from a terminal connected without a break for that long, its trade server within 120 s of the hub's clock, measured again every `MT5_CLOCK_CHECK_SECONDS`. The terminal extrapolates the trade server's time from the clock it shares with the hub, so the comparison checks the offset the terminal learned from the trade server against the broker clock's schedule, whatever the host's clock reads. Until a sample verifies it the hub answers every request `not_ready`. No verification by `MT5_CLOCK_BOOTSTRAP_SECONDS` after start, and a sample the clock contradicts, are logged as critical and leave the gate shut; the hub keeps serving, and a later verifying sample opens it.
- `GET /health` on the hub's port answers HTTP 200 while the spawner is connected, the latest sample is fresh and the clock verified, and HTTP 503 otherwise naming why. Its body carries, either way: `spawner`, whether one is connected; `clock`, the verification's `symbol`, `trade_server`, `current`, `gmt`, `skew_s` and `offset_s`, or null; `sample_age_s`; `bootstrap_missed`; `in_flight`, the requests pending; and `pending`, each EA's `role`, `symbol` and `requests` pending. Another method answers 405.

### Streams, charts and relays

- A publisher's ticks, bars and transactions of its own symbol, and the spawner's transactions that name no symbol, reach the consumers subscribed to them; a frame of another symbol is dropped with a warning. A transaction's symbol is its request's for a `TRADE_TRANSACTION_REQUEST`, its own otherwise.
- A consumer's frames wait in a queue of at most `MT5_HUB_CONSUMER_QUEUE_BOUND`; the frame that finds it full closes that consumer with code 4008, and the rest are served on. An EA's frames wait in one of at most `MT5_HUB_EA_QUEUE_BOUND`: a request that finds it full is answered `overflow`, any other frame closes the EA with code 4008.
- The hub pings every connection every 10 s, one ping outstanding at a time. A pong 30 s overdue — or, while a `busy` of the EA's stands, overdue past its bound — fails the connection: its requests are answered `ea_gone` at once and the hub drops it within its 10 s close timeout. A peer that stops reading while the hub has frames queued to it holds back the ping and the close frame.
- **Charts.** A subscription to the ticks or bars of a symbol no publisher serves, a request for a role and symbol no EA serves, and an EA going away while its chart is in use each request that chart. A request is held until its EA says hello, and sent to the spawner as `open_chart` once per spawner connection; one made while no spawner is connected is sent on the spawner's hello. The spawner answers `chart_opened` or `chart_failed`; the hub keeps a failure, with a warning, until the chart opens or its EA says hello, and answers a request needing that chart `ea_error`.
- **Idle charts.** A chart is in use while a consumer subscribes to its publisher's ticks or bars, a request waits on its EA, and for `MT5_CHART_IDLE_SECONDS` after its last use: a request or a subscription naming it, its EA's hello, the spawner's `chart_kept`. The hub tells the spawner to close a chart out of use for that long, as `close_chart`, once per spawner connection, and again once it is out of use for another period after a `chart_kept`; the spawner's own chart never closes.
- **Relays.** The hub takes every `server_time` sample of the spawner naming its own symbol into its clock gate, and drops one naming another symbol with a warning. It keeps each publisher's latest commission schedule and trading sessions under its own symbol, or why they were refused, until a later one is accepted:
  - a schedule or sessions frame naming another symbol than its publisher's is refused, and so are sessions holding a session with a negative second or one that does not end after it starts, a `to` of 0 ending at midnight;
  - a schedule naming a member its field's enum lacks never decodes: it is a frame the hub cannot handle.

### The EA

The EA, `ticks.mq5`, is one program in three roles, the `Role` its chart's template sets, and every frame it sends and reads is binary. A publisher syncs its bars through the indicator `hub_history.mq5`, which it attaches to its own chart at init and takes off at deinit; a publisher that cannot attach it fails its init. A history EA reads its ticks itself.

- Its half of the protocol is `Include/hub_wire.mqh`, which the image's build writes from the installed wheel's wire inventory with `python -m mt5connector.server.mql5gen <path>`: the header's constants, the frame, error and call codes, the chart roles, the calls each role serves with their arguments, the chart events between a publisher and its indicator, the history's deadline, a tick read's busy bound, the retry delay, span margin, bar series and floor origins, and a writer of every struct an EA sends — and a reader of the trade request — each member read by the MQL5 property `mirror.py` names for it, in the inventory's order.
- A writer reads each property by the overload that answers whether the read succeeded; `AccountInfo*` and `TerminalInfo*` have none, so the error a read of theirs leaves decides. A read that fails writes nothing and the writer reads on, collecting every property that failed, the first `HUB_PROPERTY_FAILURES_MAX` (128) by name, and then fails.
- `Include/hub_codec.mqh` packs and reads the fields: little-endian numbers, a `str` as its u16 byte count and UTF-8 bytes, `MqlTick` and `MqlRates` by `StructToCharArray`, whose 60-byte packing the EA checks at start, failing its init otherwise.

What it does inside the terminal:

- **Every EA** says hello with its role and symbol, and marks its chart with its role in the chart's comment, `ticks publisher` for a publisher. It reads the hub on its timer, every `PollMilliseconds`, and on every tick, so the vendored library answers the hub's pings and acknowledges a close. While disconnected it reconnects every `ReconnectIntervalSec` on `TimeLocal()`, and a publisher publishes no ticks or bars until the reconnected hub wants them again.
- **Requests** are answered once each, under their correlation id: a result's records in parts as [Calls](#calls) lays them out, each part carrying the `GetLastError()` the call left; or `ea_error` with the error the call met. Every call but a history read waits on nothing beyond a local read and is answered at once, in the order the requests arrive; a history read is answered once its windows are synced, as below.
  - A call its role does not serve, and a `group` filter, are answered `ea_error` with `ERR_FUNCTION_NOT_ALLOWED` (4014); arguments out of the call's layout with `ERR_INVALID_PARAMETER` (4003).
  - A property of a record that cannot be read fails the call: `ea_error` naming the record — the symbol, or the position, order or deal by its ticket — and every property of it that failed, each with its MQL5 error, the first one's error the answer's `last_error`. `symbols_get` fails whole on one symbol whose definition it cannot read.
  - The spawner serves `account_info`, `terminal_info` and `symbols_get`, the definition of every symbol the terminal knows, in Market Watch or not; `positions_get` and `orders_get` by `symbol`, by `ticket`, by both, or every one with neither; and `history_orders_get` and `history_deals_get` by `ticket`, else by `position`, else by window. A `ticket` names the order or the deal itself; one the history does not hold answers no record, the selection's error its `last_error`.
  - A publisher serves `symbol_info`, `symbol_info_tick`, `order_send`, `outcomes` and the bar history read of its own symbol; a history EA the tick history read of its own, and nothing else. A `symbol_info` of a symbol `SymbolExist` denies is answered `ea_error` with `ERR_MARKET_UNKNOWN_SYMBOL` (4301).
- **History.** Bars are synced by the indicator, whose reads never wait, and ticks by the history EA's own reads, which do; no read in a publisher waits on a download:
  - The publisher asks the indicator by chart event, `push_wire.HistoryEvent`, its `lparam` the request id and its `sparam` the window of rates on the broker's clock, `"timeframe,from,to"` or `"timeframe,from,count"`. It sends `busy` for `HISTORY_DEADLINE_S` (45 s) with each stage of asks.
  - The indicator reads each window at once — a read in an indicator answers what the terminal holds and starts the download it lacks — and again on every calculation and every second, until it answers rows, none with no error, or an error other than a loading one (4401, 4403); it then signals the window ready, its `dparam` the rows and its `sparam` `"<error>,<time>"`, the time its first row's in broker seconds, or 0 for none. A window still loading 45 s after its ask is signalled with 4403 and forgotten, and so is every window of a request the publisher has given up on. A window of ticks is not the indicator's: in an indicator one without ticks never settles.
  - Once every window of a stage is ready the publisher runs the rules over them, asking a next stage when a rule needs a window not yet synced. It reads a window's rows itself, but takes a floor stub and a probe's one row from the indicator's ready: its own read of a window wholly before a series answers -1 with 4401 even once the indicator's read answers it. Each of those is asked as a stage of its own, since a ready names no window. A ready carrying an error, or a stage not ready 45 s after its ask, answers `syncing` with a 5 s retry, and the client's retry finds more synced.
  - A history EA reads ticks in its own thread, and a tick read waits: one call to the terminal can take about 90 s, and the EA makes it again, at most once a second, while it answers a loading error (4401, 4403), until it answers ticks or none with no error, or another error. Between calls it reads the hub once, so the hub's pings are answered and new requests are taken; the EA is single-threaded, so while a call waits nothing else of it runs.
  - A tick request waits under `TICK_BUSY_S` (900 s) from the moment it is sent, hears `busy` for that bound on its arrival at the EA, before anything waits, and again every 300 s while it is in flight, the hub's keepalive and deadlines following that bound. The requests run from the EA's timer, oldest first, one at a time. A request makes no call once 900 s have passed since its arrival, and answers `syncing` then, whatever a call that returned past that bound read.
  - `copy_rates_range` and `copy_ticks_range` follow the rules of [History](#history) over the broker bounds the hub sends: a window that starts after it ends, or after `TimeTradeServer()`, is `invalid_window`; the bars' floor stub is read from 1970-01-02 on the broker's clock, the bar before a gap as the one bar `CopyRates` reads from its start, the tick after it as the one tick `CopyTicks` reads from its last second, and the ticks before it in its broker day, then its broker week. A window nothing proves is `syncing`.
  - A tick window is read first and answered when it holds ticks. An empty one is proven by the live edge, the tick after it and the ticks before it; only when no tick precedes it is the ticks' floor measured — the first tick of every kind `CopyTicks` answers from 1970-01-02 — and the window is empty when it lies wholly before that floor. The ticks' floor stands once measured, and a window wholly before it is answered empty without a read.
  - The rows go out in parts, more than one part's worth a part per timer pass behind a `busy`; the last part carries the floor fact: the series' floor when the hub has not heard it on this connection, whichever request measured it, or none. A bar series' floor is used for 900 s, then measured again.
- **Commands.** A publisher sends an `order_send`'s `MqlTradeRequest` as it came through `OrderSendAsync` and answers the `MqlTradeResult` at once: retcode 10008 and a `request_id` mean sent, and a request the terminal refuses itself answers with the retcode, comment and error that say why. The venue's verdict is the `REQUEST` transaction carrying that `request_id`, which the publisher publishes and keeps in its outcome log of the last 1,000, connected or not.
- **Transactions.** A publisher publishes one `trade_transaction` frame per `OnTradeTransaction` call whose symbol is its own, in the order the calls arrive, whatever the magic; the spawner each one that names no symbol, such as a pending order's delete or modify sent without one. Every EA receives the account's whole transaction stream, so the filter publishes each transaction once; a transaction of a symbol no chart publishes, naming one, reaches the hub zero times.
- **Ticks and bars.** While the hub wants a publisher's ticks it publishes them from its tick database — `COPY_TICKS_INFO`, the ticks that change the bid or the ask — from the moment they are wanted, read at every tick and again on its timer, since the terminal queues no tick event while one is queued or handled; each read's new ticks travel in one `tick` frame. It publishes the bar that closed for each wanted timeframe, found on its timer and its ticks when the forming bar's open moves.
- **Relays.** Every `RelaySeconds` the spawner sends `server_time`, and a publisher its symbol's commission schedule and trading sessions.
- **Duplicates.** On `duplicate` an EA closes its own chart, which unloads it. The spawner instead keeps its chart and says hello again on each reconnect, since the EA served may be its own terminal's earlier connection, which the hub drops at most 50 s after its last pong while its writes to it drain; still refused `HubPingTimeoutSec` after its first refusal, it closes its chart like any other duplicate.
- **Charts.** The spawner, the publisher of its own symbol and that symbol's history EA are separate charts. The spawner, on `open_chart` for a role and symbol, answers `chart_opened` when a chart of the symbol marked with the role runs the EA, or when a chart it opened for them is still loading its EA — a template's EA loads only once the chart has processed the template. A marked chart whose EA is gone takes its role's template again — `ChartTemplate` for a publisher, `HistoryTemplate` for a history EA. Otherwise it selects the symbol and opens an M1 chart of it with that template. A failure is answered `chart_failed` with the failing call's error — `SymbolSelect`, `ChartOpen` or `ChartApplyTemplate`, which its log names — and a symbol `SymbolExist` denies with `ERR_MARKET_UNKNOWN_SYMBOL` (4301).
- The spawner, on `close_chart`, first counts the account's open positions and pending orders on the symbol: while it holds any, the chart stays and the spawner answers `chart_kept`. Otherwise it closes the chart of the role and symbol — the one it opened, or one its comment marks — then takes the symbol out of Market Watch once no other chart of it is open, and all the same when it finds no such chart. The terminal refuses that while a chart of the symbol is open or it has open positions, so a refused deselect is tried again on each timer tick until it succeeds or a chart of the symbol opens again, and only every 15 minutes, logged once, while positions or orders hold the symbol.

---

## What the image provides

The repository's `Dockerfile` builds one image that runs the terminal and the hub: a MetaTrader 5 terminal under Wine with the EA compiled in, and the hub under a Linux Python, headless under Xvfb. Its hub and EA are the checkout's, and everything else is downloaded at build time; it carries no credential, login, server name or server list. A release publishes it as `ghcr.io/swing-traders/mt5-connector-server:<tag>`, `<tag>` the release tag `v<version>` with its `+` replaced by `-`.

| Component | Where it is pinned |
|---|---|
| the base, `mambaorg/micromamba` on Debian 13 | by tag in the final stage's `FROM` |
| WineHQ devel, amd64 and i386 | `ARG VERSION_WINE` |
| wine-mono | `image/artifacts.txt` |
| the Windows Python, python.org's x64 installer, at `C:\Python`, which runs the dialogs' driver | `image/artifacts.txt` |
| the Linux Python, the base environment at `/opt/conda` | `image/environment.yml` |
| `mt5-connector-server`, one wheel built from the checkout into the Linux Python | `VERSION` |
| the terminal's web installer, `mt5setup.exe` | `image/artifacts.txt` |

### Running it

A run gives the container:

| What | How |
|---|---|
| the account and the spawner's symbol | `MT5_LOGIN`, `MT5_PASSWORD`, `MT5_SERVER` and `MT5_SPAWNER_SYMBOL`, as environment variables |
| the hub's settings, where their defaults do not suit | the optional variables under [Settings](#settings) |
| the terminal's history store | a volume on `/home/mt5/.wine/drive_c/Program Files/MetaTrader 5/Bases`, one per account |
| time to stop | a stop timeout of 190 s |

```bash
podman run -d --name mt5-alpha \
  -e MT5_LOGIN -e MT5_PASSWORD -e MT5_SERVER -e MT5_SPAWNER_SYMBOL=XAUUSD \
  -v mt5-alpha-bases:"/home/mt5/.wine/drive_c/Program Files/MetaTrader 5/Bases" \
  --stop-timeout 190 \
  ghcr.io/swing-traders/mt5-connector-server:<tag>
```

- The hub listens on port 9000 inside the container ([Network](#network)). The image fixes `MT5_HUB_PORT`, since the EA's templates are written against it.
- `MT5_SPAWNER_SYMBOL` names a liquid symbol the account has, by the broker's own name, exactly as the terminal lists it: some brokers suffix every symbol (`XAUUSD.r`).
- The history volume holds `Bases` and nothing else. The image ships the store empty, so a fresh volume starts with nothing from it, and a new image started on the same volume starts with the history the last one synced.
- The runtime user is `mt5` (UID/GID 65532), with `WINEPREFIX=/home/mt5/.wine`, `WINEARCH=win64` and `WINEDEBUG=-all`. The image sets `MT5_TERMINAL_PATH` to the portable install's `terminal64.exe` and `MT5_PYTHON_DIR` to the Windows Python's directory.
- The entrypoint refuses to start without `MT5_LOGIN`, `MT5_PASSWORD`, `MT5_SERVER` and `MT5_SPAWNER_SYMBOL`, naming the first missing, or with an `MT5_MAX_BARS` that is no positive integer, and writes them with `MaxBars` into the terminal's startup ini ([setup.ini](#setupini)).

### Settings

The entrypoint reads `MT5_MAX_BARS` and the hub the rest from the environment once at start, each stopping on the first that is malformed or out of range, naming it:

| Variable | Setting | Default |
|---|---|---|
| `MT5_MAX_BARS` | the terminal's `MaxBars` ([setup.ini](#setupini)) | `100000000` |
| `MT5_HUB_PORT` | the hub's port, bound on all interfaces | `9000` |
| `MT5_BROKER_TZ` | the zone the broker's clock follows, as an IANA name | `America/New_York` |
| `MT5_BROKER_OFFSET_HOURS` | hours the broker's clock runs ahead of that zone | `7` |
| `MT5_CHART_IDLE_SECONDS` | how long a chart out of use stays open | `900` |
| `MT5_CLOCK_SAMPLE_MAX_AGE_SECONDS` | how long a server-time sample stays fresh, and how long the terminal must stay connected before one verifies the clock | `30` |
| `MT5_CLOCK_BOOTSTRAP_SECONDS` | how long after start a verification is due before the hub logs its absence | `120` |
| `MT5_CLOCK_CHECK_SECONDS` | interval between re-verifications of the broker's clock | `300` |
| `MT5_HISTORY_RETRY_SECONDS` | the delay a deferring answer gives | `5` |
| `MT5_HUB_CONSUMER_QUEUE_BOUND` | the most frames queued for a consumer; one more closes it | `10000` |
| `MT5_HUB_EA_QUEUE_BOUND` | the most frames queued for an EA; a request past it is answered `overflow`, any other frame closes it | `10000` |
| `MT5_HUB_PENDING_BOUND` | the most requests of one consumer pending at once; one more is answered `overflow` | `1000` |

The credentials reach the processes as environment variables at start; the image never carries them.

### The processes

| Process | Runs under | Started by |
|---|---|---|
| the hub | the image's Linux Python | `mt5-connector-hub`, or `python -m mt5connector.server.ws_server` |

- The Linux Python installs the one wheel of `mt5-connector-server` the build makes from the checkout. Nothing of this repository's runs under the Windows Python but the dialogs' driver, `image/terminal_gui.py`, on its standard library alone.
- The hub's `/health` answers HTTP 200 while its spawner is connected and its verification of the clock stands, and 503 otherwise; the hub never exits on its clock.
- The image's `HEALTHCHECK` runs `/opt/mt5/healthcheck.py` under the Linux Python every 30 s under a 5 s timeout: it asks `/health`, passes on HTTP 200 alone and otherwise prints what it got. The container is marked unhealthy after 3 failures in a row; failures within its first 360 s, which cover a cold boot of the terminal and the clock's first verification, do not count.

### Boot

The entrypoint, `/opt/mt5/entrypoint.sh` under `tini`, prints its own steps, prefixed `mt5:`, and the terminal's logs; never the terminal's window titles, which name the login and the server.

1. Starts Xvfb at 1920x1080, and x11vnc in a debug image.
2. **Restores the baked state,** before any Wine process of the run starts, since Wine reads its registry as it does. `rsync` copies `/opt/mt5-baked` back, comparing files by content: `terminal/` over the terminal directory and `metaquotes/` over the AppData tree, each deleting whatever the baked copy does not hold, `Bases` excluded, and the three registry files over the Wine prefix's. It is bounded at 40 s. Everything an earlier run of the container wrote — `Config\` with its server list and saved accounts, `Profiles\`, the chart profiles, the logs, the AppData tree, the registry — is undone, `Bases` is never written, and the log says so in one line.
3. **Fetches the server list.** The terminal logs in only to a trade server its `Config\servers.dat` lists. The entrypoint launches it on the account without the startup script, searches `MT5_SERVER` on the first page of File → Open an Account — which adds the broker's servers — and closes it, which keeps them.
4. Launches `terminal64.exe /config:<the ini copy> /portable`. `ticks_setup` opens the spawner's chart on the spawner symbol. **Starts the log tail,** `/opt/mt5/tail_logs.py` under the Linux Python, which streams the terminal's journal (`logs\<YYYYMMDD>.log`) and its experts log (`MQL5\logs\<YYYYMMDD>.log`) into the container log from the start of this boot:
   - each line converted from the terminal's UTF-16LE to UTF-8, its byte-order mark and carriage return stripped, and prefixed `terminal:` or `experts:`, its text otherwise written as the terminal wrote it;
   - each log followed onto the next UTC day's file when it appears.
5. **Adds `127.0.0.1` to the WebRequest allowlist,** which no ini key sets: on the Experts tab of Tools → Options it ticks "Allow WebRequest for listed URL" where unticked, adds `127.0.0.1` to the list, and applies the dialog. The list must then hold that one URL.
6. Starts the hub (`mt5-connector-hub`, the Linux Python).
7. Supervises Xvfb, the terminal and the hub: the first of them to exit ends the container with its status, and whatever started the container decides whether to start it again. A lost broker session is the terminal's own to recover and ends nothing, and so does a log tail that exits: the log names its status and the tail is restarted, skipping what the logs hold at its first read — so the lines written between the stopped tail's last read and the restart are lost.

**The dialogs are driven by control identity**, by `/opt/mt5/terminal_gui.py` under the Windows Python, through Win32 messages and never a screen position:

- the terminal's main window is found by its window class;
- a dialog is opened by the command id its menu path holds in `terminal64.exe`'s own menu resources;
- a control is found by its id and class, the Options page by its tab's name.

The WebRequest list's text cannot be read, so its row count is what confirms the URL. Each action prints one line and is bounded at 40 s; a window or control it cannot find fails the start, naming it. The server's name reaches the driver through `MT5_SERVER` alone.

A terminal window, dialog or control that never appears fails the start at once, naming the step. What the entrypoint cannot see — a server search that found nothing, a refused login, a spawner symbol the broker does not list — ends nothing: no sample verifies the broker clock, so the hub's `/health` stays 503 and it answers every request `not_ready`, logging the missed bootstrap window as critical.

### Stop

A stop closes the terminal as a user would — `taskkill` without `/F`, which posts `WM_CLOSE` — and waits up to 60 s for it, then stops the log tail within 10 s once it has streamed the terminal's last lines, the hub within 10 s, and Xvfb last within another 10 s. It kills whatever is still running and ends the container with the terminal's own exit status, or 0 when no terminal was running.

Those bounds together, with a call in flight when the stop arrives, stay at least 30 s under the 190 s stop timeout a run gives the container, so a stop ends the terminal by its close, never a kill.

### The terminal

The terminal is started as `terminal64.exe /config:<setup.ini> /portable`, so it keeps its data — the `MQL5` tree, its profiles and templates — under its own install directory. The paths below are relative to that directory.

#### setup.ini

The image bakes the sections without a credential at `/opt/mt5/setup.ini`. At every start the entrypoint writes a copy into `/tmp/mt5`, readable by the runtime user alone, adding `[Common]` `Login`, `Password` and `Server` from the environment, `[Charts]` `MaxBars` from `MT5_MAX_BARS` and `[StartUp]` `Symbol` from `MT5_SPAWNER_SYMBOL`.

| Section | Key | Value |
|---|---|---|
| `[Common]` | `Login`, `Password`, `Server` | `MT5_LOGIN`, `MT5_PASSWORD` (quoted) and `MT5_SERVER` |
| | `AutoConfiguration` | `true` |
| | `ProxyEnable` | `false` |
| `[Charts]` | `Profile` | `default`, emptied of its charts before every launch, so the terminal starts with none |
| | `MaxBars` | `MT5_MAX_BARS`, by default `100000000`, the terminal's ceiling: a larger value is clamped to it. It also bounds the span one history read covers |
| `[Experts]` | `AllowDllImport` | `1` |
| | `Enabled` | `1`: algorithmic trading on, without which the terminal refuses every order the adapter sends |
| | `WebRequest` | `1` |
| `[StartUp]` | `Script` | `ticks_setup` |
| | `Symbol`, `Period` | the chart the script runs on and `M1`: the script opens the spawner's chart on its symbol, then closes its own. The symbol is `MT5_SPAWNER_SYMBOL` |

The terminal must know the trade server `MT5_SERVER` names — its `Config\servers.dat` lists it — before the login can succeed: a server a fresh install does not know is added by searching for its name once (File → Open an Account), which the image does at every boot.

#### The EA and the startup script

The wheel carries their source, and the EA's indicator's; `python -c "import mt5connector.server, pathlib; print(pathlib.Path(mt5connector.server.__file__).parent / 'mql5')"` prints where it is installed. Each file goes into the terminal's `MQL5\` tree:

| In the wheel's `mql5/` | In `MQL5\` |
|---|---|
| `Expert/ticks.mq5` | `Experts\ticks.mq5` |
| `Indicators/hub_history.mq5` | `Indicators\hub_history.mq5` |
| `Scripts/ticks_setup.mq5` | `Scripts\ticks_setup.mq5` |
| `Include/hub_codec.mqh` | `Include\hub_codec.mqh` |
| `Include/MQL5Book/` | `Include\MQL5Book\` |
| — | `Include\hub_wire.mqh`, which `python -m mt5connector.server.mql5gen <path>` writes from the wire inventory |

The three sources are compiled by `MetaEditor64.exe`, beside `terminal64.exe`, into `Experts\ticks.ex5`, `Indicators\hub_history.ex5` and `Scripts\ticks_setup.ex5`. Under Wine, MetaEditor has these quirks:

- Its name is `MetaEditor64.exe`, capital M and E, on a volume that is case-sensitive.
- It honours only the last `/compile:` argument of an invocation: each source is compiled by its own.
- It truncates a `/compile:` path that contains a space: each source is compiled from a directory whose path has none — `C:\mt5build\ticks.mq5` — and the `.ex5` copied into place. An `#include <...>` resolves in the terminal's `MQL5\Include\`, so the includes are placed there first.
- A clean compile writes `0 errors, 0 warnings` to its `/log:` file; a missing `.ex5` is a failed compile.

The image's build places and compiles them with `image/install-ea.sh`, from the wheel the Linux Python installed, under Xvfb, after generating `hub_wire.mqh` with it; its log names the generated file. It fails unless each `.ex5` appears and each `/log:` file's `Result:` line reads `0 errors, 0 warnings`, and its log prints each line.

On each terminal start `ticks_setup` opens an M1 chart of the symbol it runs on and applies the template `ticks_spawner.tpl`, which attaches the EA as the spawner, then closes the chart it ran on, which would hold a `CHARTS_MAX` slot for nothing. The terminal starts with no chart, so a chart already running the EA at start is a boot defect, which the script prints, naming the chart's symbol, and opens and closes nothing. Every other chart opens on demand: the spawner opens it with its role's template, `ticks.tpl`, which attaches the EA as that symbol's publisher, or `ticks_history.tpl`, as its history EA.

- The terminal keeps at most `CHARTS_MAX` (100) charts open, the spawner's included.
- A chart the spawner opens on demand that does not open — a symbol the venue does not list, or one beyond `CHARTS_MAX` — is reported by the spawner, and a request that needs it is answered `ea_error` naming the reason, until a chart of it opens or its EA says hello.
- The spawner refused as a duplicate keeps its chart and says hello again every `ReconnectIntervalSec`; still refused `HubPingTimeoutSec` after its first refusal, it closes its chart like any other duplicate.
- A chart closes once its symbol has been out of use for `MT5_CHART_IDLE_SECONDS`: the spawner closes it and takes the symbol out of Market Watch, where the terminal would keep processing its ticks.
- A symbol with open positions or pending orders never closes idle: the spawner keeps its chart and the hub counts it in use for another `MT5_CHART_IDLE_SECONDS`.
- A deselect the terminal refuses after the close is tried again on the spawner's timer until it succeeds or the chart reopens, less often while positions or orders hold the symbol.

#### The chart template

Three templates attach the EA, one per role: `ticks_spawner.tpl` with `Role=0`, `ticks.tpl` with `Role=1` for a publisher, and `ticks_history.tpl` with `Role=2` for a history EA — an enum input takes its value, which the build reads from `ENUM_CHART_ROLE` in the generated `hub_wire.mqh`. Each goes in `MQL5\Profiles\Templates\` and in `Profiles\Templates\`, the two places a template applied by bare name is looked for, and must be in the form the terminal saves templates in:

- UTF-16LE with a byte-order mark (`FF FE`), and CRLF line ends;
- a root `<chart>` section — its symbol and period are ignored, the chart keeps its own;
- the EA in its own `<expert>` block, one key per line:

```
<expert>
name=ticks
path=Experts\ticks.ex5
expertmode=5
<inputs>
Role=1
Server=ws://127.0.0.1:9000
ReconnectIntervalSec=3
RelaySeconds=5
PollMilliseconds=16
ChartTemplate=ticks.tpl
HistoryTemplate=ticks_history.tpl
HubPingTimeoutSec=50
</inputs>
</expert>
```

| EA input | Meaning |
|---|---|
| `Role` | the EA's role: `0` the spawner, which opens and closes the charts the hub asks for, `1` a publisher, `2` a history EA |
| `Server` | the hub's URL, `ws://127.0.0.1:<MT5_HUB_PORT>` |
| `ReconnectIntervalSec` | the fewest seconds between two attempts to reach the hub while the EA is not connected |
| `RelaySeconds` | the interval between the spawner's server-time frames, and a publisher's commission schedules and trading sessions. It must stay below `MT5_CLOCK_SAMPLE_MAX_AGE_SECONDS`, or the clock never verifies |
| `PollMilliseconds` | the EA's timer: the interval between reads of the hub, of the symbol's ticks and closed bars, and of the connection, and so the longest a request waits for its read; the terminal fires it no faster than every 10–16 ms, which `16` matches |
| `ChartTemplate` | the template the spawner applies to a chart it opens for a publisher, `ticks.tpl` |
| `HistoryTemplate` | the template the spawner applies to a chart it opens for a history EA, `ticks_history.tpl` |
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
Role=1
Server=ws://127.0.0.1:9000
ReconnectIntervalSec=3
RelaySeconds=5
PollMilliseconds=16
ChartTemplate=ticks.tpl
HistoryTemplate=ticks_history.tpl
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

The image's build writes the three templates from `image/ticks.tpl.in`, filling in the hub's port and `Role`.

#### The allowlist

Each EA's socket reaches only a host on the terminal's allowlist — Tools → Options → Expert Advisors → "Allow WebRequest for listed URL", checked, with `127.0.0.1` on its list:

- The list lives in memory, and every terminal start empties it: the host is added again after each start, until then no EA can reach the hub.
- `setup.ini` cannot set it: MT5 ignores the `WebRequestUrl` key, which is MT4's.
- Confirming the dialog re-initialises every attached EA, which then connects again.

### Network

- The image serves one port, the hub's `MT5_HUB_PORT`; the adapter's config names it as `ws_url`.
- There is no authentication: the hub trusts its network. The image declares no host port, loopback included, and declaring one is a design change; a runtime may publish the hub's port on its own host.

### Building

```bash
podman build --format=docker -t mt5-connector-server .
podman build --format=docker --build-arg DEBUG=1 -t mt5-connector-server-debug .
```

- The build context is the repository root, and `.dockerignore` admits `VERSION`, `packages/` and `image/` alone. Podman builds with `--format=docker`: its default OCI format drops the base image's `SHELL`, through which the build's `RUN` steps run in the activated environment. Docker needs no flag.
- The download stage takes `image/artifacts.txt` and `image/first-start.sh` on their own, and the checkout comes in once the terminal is installed, so a change to a package or a boot script rebuilds neither Wine nor the terminal.
- The Linux Python is `image/environment.yml` — Python and pip alone — installed as the base environment, then made root-owned and read-only.
- One wheel of `packages/server`, built with that Python from the checkout's copy at `/opt/mt5-connector` with the client's wire vocabulary its `wire` link names, installs there with its dependencies; nothing else of the client enters the image. Nothing of this repository's own comes from an index, and `pip check` fails the build when the installed dependencies no longer satisfy the server's pins.
- `--build-arg DEBUG=1` adds x11vnc, which the entrypoint starts on the display listening on the container's loopback (`127.0.0.1:5900`) and nowhere else; the default build carries no VNC at all, and any other `DEBUG` value fails the build.
- The image is about 5.8 GB.

The build also:

- **Compiles the EA** and **writes its three templates**, as [The EA and the startup script](#the-ea-and-the-startup-script) and [The chart template](#the-chart-template) state.
- **Empties the history store** (`Bases`) after the terminal's first start, so a fresh volume mounted on it starts with nothing from the image.
- **Bakes the terminal's start state** into `/opt/mt5-baked`, owned by root, which every boot restores:
  - `terminal/`, the terminal directory once the EA is installed, with the chart profile `default` emptied in whichever of `Profiles\Charts\` and `MQL5\Profiles\Charts\` exists — the build log names each it empties — its `logs\` and `MQL5\logs\` empty, and without `Bases`;
  - `metaquotes/`, the Wine user's `AppData\Roaming\MetaQuotes`;
  - `registry/`, Wine's `system.reg`, `user.reg` and `userdef.reg`, as the build's last Wine session saved them.

### Pins

- Every apt package the `Dockerfile` pins takes its version from an `ARG` named `VERSION_<PACKAGE>` — `VERSION_WINE` — used at every pin.
- `image/artifacts.txt` lists every file the download stage fetches, one per line as `<name> <url> <sha256>`: its URL carries the version, its hash pins the bytes, and nothing else states that version. One build step fetches them all and checks every hash, so a changed upstream file fails the build.
- A downloaded artifact is bumped by editing its row's URL and `sha256sum` and nothing else. The WineHQ packages are bumped by `VERSION_WINE` alone.

### The terminal build

The build starts the installed terminal once with `/portable`, waits up to 180 s for its log to show the start and the finished first-start MQL5 recompile — so a running container does not pay that recompile — and fails the build if the log never shows both. It writes the build number the terminal logged to `/home/mt5/.terminal-build`:

```bash
podman run --rm --entrypoint cat mt5-connector-server /home/mt5/.terminal-build
```

`mt5setup.exe` is a web installer: its own bytes are pinned, but it installs whatever terminal build MetaQuotes currently serves, so two builds from identical inputs can carry different terminal builds. Nothing stops the terminal's own update check: it never applies an update by itself, so the build an image baked is the one it runs. The installer exits 1 after a successful install, so the build checks the terminal's first start rather than the installer's exit code; without network access, or with a terminal that cannot start, it hangs instead of failing and ignores a termination signal, so the build kills it after 600 s and fails the step.

The files the build downloads itself are pinned by hash. The base images are pinned by tag, the Debian packages Wine depends on follow trixie's current updates, and the Linux Python's packages, with the build backend the server builds with, resolve at build time as `image/environment.yml` and the server's `pyproject.toml` declare them.

### The image tests

`tests/image/test_mt5_image.py` starts a built image on an account no broker holds when `MT5_IMAGE_TEST` names it, on docker where it is installed and podman otherwise:

```bash
MT5_IMAGE_TEST=localhost/mt5-connector-server pytest tests/image
```

It pins that:

- a start on that account boots up to the hub, printing the account's password nowhere;
- the startup ini carries `MaxBars` as `MT5_MAX_BARS` sets it, the terminal's ceiling when it is unset, and an `MT5_MAX_BARS` that is no positive integer fails the start naming it;
- the first supervised process to exit — the hub, Xvfb, the terminal during the boot — ends the container with its status, naming it;
- a display that never comes up, a restore that never returns and a driver action that never returns each fail the start naming the step and its bound;
- a stop closes the terminal, stops the log tail after it and ends the container within the stop timeout, killing nothing;
- the main-window action reports the window;
- a boot leaves WebRequest allowed for one URL;
- a restart restores the baked state — a file added under `Config\` or the AppData tree removed, a same-length rewrite of a baked file with its timestamp undone, a value written into the terminal's registry key gone — and leaves `Bases` alone;
- the terminal's journal and experts log reach the container log as UTF-8 under their prefixes, a journal line naming the login and the server arriving as the terminal wrote it;
- the driver fails naming a control a dialog lacks;
- the hub boots and answers `/health` 503, with no spawner connected and no clock verified, on the loopback port the runtime published it at, and the image's healthcheck reaches it and fails on that 503;
- the image declares its healthcheck as the health probe, every 30 s under a 5 s timeout and unhealthy after 3 failures.

Its tests of the driver's menu and tab lookups, of the log tail — following the journal onto the next day's file, each line written verbatim as UTF-8 under its prefix, a restarted tail skipping what the logs hold at its first read, a half-written character at a restart or a stop — and of the health probe, passing on HTTP 200 alone and naming a 503 or a hub not listening, run without an image.

### The live hub tier

`tests/live/test_hub_live.py` reads and commands through two running images' hubs on one account, and runs only when `MT5_LIVE_HUB_SYMBOL` names a symbol that account trades, weekends included:

| Variable | What it names | Default |
|---|---|---|
| `MT5_LIVE_HUB_SYMBOL` | the symbol every case reads and trades | — |
| `MT5_LIVE_HUB_URL` | the hub of container 1, its `MaxBars` the ceiling, which every case but the span cap's reads | `ws://127.0.0.1:9000` |
| `MT5_LIVE_CAPPED_HUB_URL` | the hub of container 2, its `MaxBars` capped, which the span-cap case reads; a case that finds no hub there fails naming it | `ws://127.0.0.1:9001` |

Nothing starts a container for it, and no CI runs it. By hand, from the repository root:

1. Write a `.env` there, which is never committed: one unquoted `NAME=value` per line, holding nothing the shell interprets, since the container runtime reads each value verbatim and step 4 sources the file. It carries the entrypoint's `MT5_LOGIN`, `MT5_PASSWORD`, `MT5_SERVER` and `MT5_SPAWNER_SYMBOL`, and the tier's `MT5_LIVE_HUB_SYMBOL`.
2. Start both containers from the image under test, each with its own history volume. The two terminals log into the same account at once. Container 2's `MaxBars` of 100,000 caps one read's span at about 70 days of M1 periods:

   ```bash
   image=localhost/mt5-connector-server
   bases="/home/mt5/.wine/drive_c/Program Files/MetaTrader 5/Bases"
   podman run -d --name mt5-live-1 --env-file .env -p 127.0.0.1:9000:9000 \
     -v mt5-live-1-bases:"$bases" --stop-timeout 190 "$image"
   podman run -d --name mt5-live-2 --env-file .env -e MT5_MAX_BARS=100000 \
     -p 127.0.0.1:9001:9000 -v mt5-live-2-bases:"$bases" --stop-timeout 190 "$image"
   ```

3. Wait until each hub's `/health` answers 200, its terminal logged in and its broker clock verified:

   ```bash
   for port in 9000 9001; do
     until curl -fsS "http://127.0.0.1:$port/health" >/dev/null; do sleep 10; done
   done
   ```

4. Run the tier:

   ```bash
   set -a; . ./.env; set +a
   pytest tests/live -q -rA
   ```

5. Stop and remove both containers:

   ```bash
   podman stop mt5-live-1 mt5-live-2
   podman rm mt5-live-1 mt5-live-2
   ```

`docker` takes the same commands.
