# AGENTS.md

Instructions for AI coding assistants working in this repo. Read this first; it captures the load-bearing context that isn't obvious from the code alone.

If something here conflicts with the code, the code wins — flag the drift and update this file in the same PR.

This file holds GUIDELINES and rules of engagement. How a subsystem works belongs in legible code and succinct comments stating its deliberate decisions (see §7) — never here, and never as history.

---

## What this repo is

An owned hard fork of [aulekator/mt5-connector](https://github.com/aulekator/mt5-connector): a [NautilusTrader](https://nautilustrader.io/) (NT) adapter for MetaTrader 5. It does not track upstream.

It ships code, never infrastructure: two distributions in one `mt5connector` namespace package (no `__init__.py` at `mt5connector/`), each its own hatchling project.

- **`mt5-connector-client`** (`packages/client/`) — `mt5connector.client`: NT data and execution clients, the instrument provider, the factories, the remote backend (an HTTP shim generated from the inventory), the history client and the downloader.
- **Wire vocabulary** (`packages/client/mt5connector/wire/`: `mirror.py`, `broker_clock.py`, `history_wire.py`, `push_wire.py`) — the inventory of the pinned `MetaTrader5` package, the broker-clock conversion, and the history and push protocols' names. It lives once, in the client's tree, and ships in both wheels.
- **`mt5-connector-server`** (`packages/server/`) — `mt5connector.server`: the Flask app the remote backend talks to (entry point `mt5-connector-server`), the push hub (`mt5-connector-hub`), the EA and its startup script with their vendored MQL5 libraries as package data (`mql5/`), and its README — the API and the contract the image that runs it builds to.

**This repository is public.** Nothing in it names a private repository, a deployment, an account, a login number, a server name that identifies an account, or a credential — not in code, tests, docs, commit messages, PR text or review files. A commit here stands on its own: it is written for a reader who has never seen the consumer that pins this fork. Credentials reach the server as environment variables, never baked into an image or committed in an `.env`, and a settings error never echoes a credential.

Delivery:

- PRs merge to `main`; CI runs `just lint` and `just test` on every PR and push to `main`.
- `VERSION` is the one version both projects read. A release is a `v0.4.0+st.N` tag; CI builds both `py3-none-any` wheels, each with a sha256 sidecar, publishes them as one GitHub release, and rebuilds the PEP 503 index on this repository's GitHub Pages, a page per project.
- Consumers pin a release exactly, by its full local version, from that index; PyPI can never satisfy a `+st` version, so the pin proves provenance.

---

## Working agreements

### 1. Lint is part of the cycle, not optional

After **any** code change, before claiming done:

```bash
just lint        # ruff check + black --check
just test        # pytest
```

Tests passing ≠ lint passing. Common offenders this repo flags:

- `B905` — `zip()` without `strict=`. Pick `strict=True` if lists must be equal length; `strict=False` for intentional truncation like `zip(xs, xs[1:])`.
- `F841` — unused locals. Delete or use them.
- `B017` — `pytest.raises(Exception)`. Replace with the specific exception type.

`just lint-fix` auto-fixes most of the above; review the diff after.

### 2. Verify before you propose

When you say "the current X does Y", run a tool call to confirm in the current turn. Recall from earlier in the conversation, after summarisation, or from training data is not verification. Re-read the file; re-grep the symbol; re-run the test. Acting on a stale mental model is the most expensive mistake available here.

### 3. Crash-loud, never silent

New code fails loud:

- Config invariants are checked up-front when the config is built, not at the point of use.
- A CLI validates every flag before the job starts — its value, the flags it is combined with, and whatever it names (a symbol, a path, a server) — never at the point of use.
- Parsers and providers crash on bad input from the terminal or the server, not silently emit a default or a NaN.
- Lifecycle methods crash if the state transition is wrong — a connect on an already-connected client, a disconnect on one never connected.

Don't add `if x is None: return` to bury a real problem. Find the real cause; either fix it or crash with a useful message.

### 4. After a revert, sweep for downstream consequences

Reverting a change locally leaks stale state into help text, error messages, comments, conditional branches whose premise was tied to the bad assumption. Grep the codebase for the now-incorrect form.

### 5. Legibility over terseness — never trade clarity to save lines

Code is read far more often than written. Don't collapse a branch-dispatch into a callable-assignment to save a few lines — spell the branches out:

```python
# NO — clever, but the reader has to hold the indirection in their head
action = mt5.ORDER_TYPE_BUY if order.side == OrderSide.BUY else mt5.ORDER_TYPE_SELL
self._send(order, action=action)

# YES — the control flow is right there on the page
if order.side == OrderSide.BUY:
    self._send(order, action=mt5.ORDER_TYPE_BUY)
else:
    self._send(order, action=mt5.ORDER_TYPE_SELL)
```

The same applies to dict-dispatch over a one-shot branch, nested ternaries, and comprehensions doing real work with side effects. A couple of repeated lines that read top-to-bottom beat a compressed form that has to be decoded. Optimize for the next person debugging at 3am, not the line count.

**Settle what always happens first, then branch once.** An act every path performs — releasing a lock, recording a deal as seen — happens once at the top, unconditionally, never repeated inside each branch where one forgotten call is a silent bug. Then the cases are classified and handled in ONE `if`/`elif`/`else`, never a chain of guards each re-testing a fact the previous one already established. A function reads: what always holds, what is always done, then the cases.

```python
# NO — the bookkeeping is a per-branch chore, and the deal is classified twice
if deal.type == mt5.DEAL_TYPE_BUY:
    side = OrderSide.BUY
elif deal.type == mt5.DEAL_TYPE_SELL:
    side = OrderSide.SELL
if side is None:
    self._seen.add(deal.ticket)
    return
self._seen.add(deal.ticket)
self._emit_fill(deal, side)

# YES — done once, then each case classified and handled in one place
self._seen.add(deal.ticket)
if deal.type == mt5.DEAL_TYPE_BUY:
    self._emit_fill(deal, OrderSide.BUY)
elif deal.type == mt5.DEAL_TYPE_SELL:
    self._emit_fill(deal, OrderSide.SELL)
```

**A value computed for one act guards that act.** When something is computed only so that the block after it can use it, that block is written under a condition on the value, never behind an early `return` standing between the two. Guard and consumer then read as one unit, and moving either cannot silently break the other. An early `return` is for a function's own preconditions at its top — not connected, nothing to do — where nothing computed is consumed after it. Nesting is the ordinary shape here: one or two levels read fine, and only a third is the signal to lift a block into its own function, never to flatten it with a return.

```python
# NO — the return hides that `order` exists for the fill below it; reorder the two and it breaks
order = self._cache.order(client_order_id)
if order is None:
    return
self._emit_fill(order, deal)

# YES — the value guards the act it was computed for
order = self._cache.order(client_order_id)
if order is not None:
    self._emit_fill(order, deal)
```

**A wrapper that only returns another attribute is indirection with no value.** Read the field directly. A property or method earns its place only by doing something the field does not — a fallback, a derivation over several fields, a conversion at a boundary. The same goes for a local that only aliases an attribute — `config = self.config` — however many reads follow it: write `self.config.field` at each site. A local holds a computed value or a lookup's result, never a second name for something already in reach.

```python
# NO — two names for one fact; the reader opens the wrapper to learn it adds nothing
@property
def _magic(self) -> int:
    return self.config.magic_number

# YES — the fact, read where it lives
self.config.magic_number
```

### 6. Enums over literals

Any value drawn from a closed set — a reason name, a status, a mode, a selector, a kind — is an enum member, never a string or number literal: declare the `StrEnum` once at the concept's definition site and use its members in every signature, comparison, config field, and log line. A literal is converted to the member at the input boundary (CLI, YAML, environment, the terminal's or server's reply) and never travels further as a raw value; config validation refuses a non-member even under programmatic construction. Literals compared across modules (`return "foo"` here, `x == "foo"` there) are a review finding and a refactor of every site when touched.

When NT exposes an enum for a value, import and use that enum directly — `AccountType.MARGIN`, `OmsType.HEDGING` from `nautilus_trader.model.enums` — never `"MARGIN"` / `"HEDGING"` or an in-repo mirror.

### 7. Comments, docstrings, and error messages — only what the code can't say for itself

This is a hard standard. **A comment earns its place only by saying something the code does NOT already say.** If a reader can see it from the code, omit the comment — `subscribe_quote_ticks(...)  # subscribed unconditionally` is pure noise. Comment the genuinely non-obvious: an NT or MT5 quirk, an ordering constraint, a why-this-not-that. Five failure modes:

- **Restating the mechanism** the code already shows. Say the WHY, never the WHAT.
- **Documenting the callers, not the function** — "ends every poll", "runs before the submit" describe when OTHER code calls this one: that is the call graph, written where it rots the moment a caller changes and teaches nothing at the definition. A docstring states what the function does WHEN called and its own contract; when, whence, and how often it is called belongs to the CALLERS' documentation, at the call sites that decide it.
- **Writing the argument, not the ruling** — a deliberate decision reads as its one-sentence conclusion, never the reasoning that won it. "Deals already in the history at connect are marked seen, never emitted: NT has no order to apply them to" is a ruling; three paragraphs deriving why every alternative is worse is a design review living in a docstring. Truth is not the defense — an essay of accurate, current-state sentences still fails, on size.
- **Over-specifying** — false precision is worse than silence and rots into a lie. Don't write "polls every 100 ms" when the interval is config-driven; the comment is wrong the moment someone sets another value. Say "the config's poll interval".
- **Describing the change, not the present state** — a comment states what the code IS FOR now, never how it got here or how it differs from before: not "now on the config (not a CLI flag)", "no longer polled", "moved from X". The reader sees current code; the diff is in git.

Be terse and human — a glance, not an essay. A block of 5 comment lines over 1 line of code is wrong, **and docstrings and error messages are bound by the same standard**: a function docstring is a FEW lines — the what, plus at most a couple of one-sentence whys — and multiple paragraphs on one function is wrong before its content is even read. **The production codebase IS the validated artifact** — if code is here it's assumed correct; no prose it carries re-argues, re-justifies, or cites the evidence for that, least of all a message shown to someone already stuck.

**Never put the following in a comment — and don't relocate it into this file either; it belongs in project memory or nowhere** (durable *design principles* DO belong here — see below):

- **Plan phases / iteration history** — "Phase 2", "now that we removed X", "first we tried…". The reader sees the current code, not the path to it.
- **Pointers to anything not checked in** — the working spec, plan docs, `scripts/` (throwaway), project-memory files, task IDs. These are ephemeral working state and dangle the moment the repo is read on its own. If a point from them is needed to understand the code, state it INLINE; otherwise omit the pointer. This subsumes studies/results: no "the study showed", no "§3b" / "see the spec". And never reference THIS file from code — AGENTS.md says how to write code; comments are self-contained and carry their why inline. (References to `tests/`, other modules, and the READMEs are fine.)
- **Validation evidence / metrics** — "tested against 5 brokers", "n=880", "zero rejects in a week of demo". It's assumed validated by being here; the numbers live in a writeup or project memory, never the code. (These are forbidden as *validation claims* — the same words as ordinary domain terms are fine: a "robust parser", a "validation error".)
- **Self-justification** — "the cleanest way", "broker-AGNOSTIC", "zero overhead". Give the mechanism, not the argument you won with yourself.
- **Notes to yourself** — an agent's working memory goes in project memory, not a `#`. A `TODO`/`FIXME` must name a concrete code condition or tracked issue, not a scratch note.

**Scope each prose surface to its level:**

- **Class docstring** = the **purpose** of the class (what it is / what it's for), one or two lines. NOT a tour of its methods or fields.
- **Method / function docstring** = **what it does** (and a non-obvious why or two, ONE sentence each), nothing more — and never a restatement of the module's state contract, which has exactly one home (below).
- **Module docstring** = what the module provides, in a line or two — not its origin story. A module that keeps state additionally documents its state contract there.
- **Error / exception message** = what is wrong, and nothing else. Not the rationale for the check, not how the system is wired, not how to use the product. Guidance is the CALLER's job — catch the error and print the explanation there, where the context to give it lives. A message never explains itself.
- **argparse `help=`** — one terse line, SAME SHAPE everywhere: state what setting the flag *changes* (the behaviour difference it causes — explain only the non-obvious, never how the framework works), then end with the default as `Default: X.`. No implementation detail, no provenance, no essay. (Required flags have no default to state.) Don't restate what `choices=`, the type, or the flag name already conveys.

```python
# NO — notebook: history, evidence, self-justification
"""Deal seeding — the ROBUST fix for replayed fills (zero spurious fills over a week of demo).
Phase 2 dropped the per-deal guard; see scripts/replay_audit.py."""

# YES — what the next reader needs
"""Marks the deals already in the terminal's history as seen, so a restart emits none as a fill."""

# NO — explains the design, then the product, to someone already stuck
raise ValueError(
    "no server URL is configured, and the remote backend requires one — the adapter talks to the "
    "containerised terminal over HTTP and WebSocket, so without a URL it has nothing to connect "
    "to. Start the server under packages/server/ and set the URL in your .env."
)

# YES — the fact, and nothing else
raise ValueError("remote backend: no server URL is configured")
```

A durable **design principle** (a real why-it's-built-this-way) belongs in the code it governs, a README, or — if it is a repo-wide rule of engagement — this file. Delete pure history outright; park agent reasoning in project memory.

### 8. Dependencies go through `environment.yml` + mamba, never ad-hoc pip

The env (`mt5-connector`) is declared by `environment.yml` — the client's runtime deps in its `pip:` subsection (conda-forge for `python`/`pip` only) and `packages/client` installed editable — and `environment.dev.yml`, which layers the dev tooling and `packages/server` editable. A distribution's dependencies are declared in its `pyproject.toml`; the client's are also pinned in `environment.yml`, the server's live in its `pyproject.toml` alone. The client pins `nautilus_trader` to one exact fork build (`==1.231.0+st.N`), the build its consumer pins: releasing a new fork build updates both pins in one release, and a client that lags its consumer's pin is a release defect. To add or bump a dependency:

```bash
# 1. edit the distribution's pyproject.toml, and for the client the pin in environment.yml (pip: subsection)
# 2. sync the declared env via mamba — this drives the pip subsection too
mamba env update -n mt5-connector -f environment.yml
mamba env update -n mt5-connector -f environment.dev.yml
```

Never `pip install <pkg>` straight into the env: it leaves the package **undeclared**, so the env can't be reproduced from the file and the next `env create` silently lacks it. Dev-only tooling goes in `environment.dev.yml` the same way. If you find a package installed but missing from the yml, that's drift — declare it and re-sync.

### 9. Ad-hoc tooling belongs in `scripts/`

One-off migrations, audits, studies, and repair utilities belong under the gitignored `scripts/` directory at the repository root, never in a package or a supported CLI. Production code, tests, and READMEs must not import or depend on them. Promote a script into the main codebase only when it becomes a recurring, supported workflow with a maintained contract.

### 10. Tests never shape production code

Production packages carry NO code whose only consumers are tests. A helper, field, constant, parameter, or seam that nothing in production reaches moves into the consuming test file (a shared test utility when several need it) or dies. White-box testing of production code is fine: anything with a real production caller stays, however many tests also exercise it.

The same rule upstream: production code is shaped by PRODUCTION needs alone — never add a hook, widen a signature, split a function, or expose state so a test can observe it. Tests work with the shape production wants, observing behavior through their own records and doubles.

**A test proves a behaviour the code has; it never proves an absence.** That a deleted name is gone, that a string appears nowhere in the tree — the review of the change establishes that, once. A test that reads source files to assert one is a lint wearing a test's clothes: it observes no behaviour, its population is the filesystem, and it breaks for reasons that have nothing to do with the code. Pin what the code does instead.

### 11. Prose line breaks

**Code (comments, docstrings):** fill to the configured width (100) before wrapping, and reflow the whole block after editing so adjacent lines stay balanced. Raw code is read un-rendered in a 100-column file, so keeping every line within the width matters; never break mid-sentence when more words fit.

**Markdown:** don't hard-wrap to a column. The renderer reflows each paragraph, so source line breaks are invisible in the output and a fixed width only forces mid-sentence breaks with no upside. Write one line per paragraph and let the renderer wrap; add an intra-paragraph break only where it is strictly needed (not as a blanket rule), and never mid-sentence. Lists, tables, and code blocks keep their own line structure.

### 12. Break dense prose into segments

When a paragraph packs several parallel facts or rules — "X does Y, A does B, C does D, but E does F when Z" — it reads as a wall. Break it up: a bullet per rule, a short table where there's a repeating shape (field → meaning), or a few short sentences. Keep flowing prose only for a single coherent thread — a narrative or an argument — where segmenting would hurt it. The test: if a sentence is really a list wearing prose, make it a list.

### 13. A getter never mutates

Reading never changes state. A function a caller reaches for to LEARN something — a property, a `get_`/`read_`/`resolve_` function, anything whose return value is the point — must not write files, generate secrets, mutate globals, or create what it failed to find. A side-effecting getter hides the write at its call site: the reader sees a question, the program performs an act.

```python
# NO — a "read" that silently selects the symbol in the terminal's Market Watch
info = get_symbol_info(symbol)

# YES — the caller sees the gap and closes it deliberately
if not is_symbol_selected(symbol):
    select_symbol(symbol)
info = get_symbol_info(symbol)
```

Split such a function into the read and the act, and let the caller sequence them. A constructor, an `ensure_*`, or a `create_*` may of course write — its name says so before it is called.

### 14. Numeric types by kind — `float` only at the MT5 boundary

Every number is typed by what it is, from where it is produced to where it is sent:

| Kind | Type |
|---|---|
| a price, a quantity, a money amount, a fee — anything the venue sees or the account books | NT `Price`, `Quantity`, `Money` |
| a ratio, a percent, leverage, derived money; config fields carrying one | `Decimal` (a `Decimal` NT hands over is passed on unconverted) |
| a raw value as the MT5 terminal or server returns it, before it is parsed, and reporting statistics | `float` |

A value crosses from `float` into accounting exactly once, explicitly, at the call site that decides the level — `instrument.make_price(...)` / `instrument.make_qty(...)` — never by a layer converting internally, and never back. In accounting code, `float(...)` of a `Price`, `Quantity`, `Money`, or `Decimal`, and a signature that accepts `float` for one, are review findings; code that predates this rule is brought to it when touched.

### 15. Maintaining THIS file

This file is rules of engagement: guidelines, hard requirements, and non-obvious framework facts. It is NOT a decision log, NOT a subsystem manual (the code, its §7 comments, and the in-repo READMEs hold that), and NOT a changelog (git holds that). Decision HISTORY lives nowhere in this repo — rationale survives only as current-state §7 comments and README principles, and everything referenced from here must be readable from the repo alone. Rules for editing it:

- **Writing here is a whole-file operation, never an append.** Read the whole file first and place the rule: fold it into the rule it refines, replace what it supersedes, delete what it contradicts. Add a new entry only for a genuinely new rule.
- **Every addition pays for itself.** Check what it makes redundant — rules absorbed elsewhere, behavior the code now enforces mechanically — and remove that in the same edit. A rule earns its place by changing what an agent does, not by recording that something happened.
- **Describe the present, never the transition.** "No longer", "now", "superseded by" are drift — this file is always current state, exactly like a §7 comment.
- **How a subsystem works goes to the code it governs** — legible code, §7 comments for the deliberate decisions, and a README for usage plus high-level principles. How to USE or RUN a subsystem, tool, or test — its commands, env vars, flags, invocations — is usage and lives in its README or module docstring, never here; the repo-wide workflow commands this file itself mandates (the lint/test gate, the env sync) are rules of engagement, not usage. If a subsystem needs presence here, it gets one pointer plus its rules of engagement.
- **Size is a hard budget: the whole file must fit ONE bare Read (~70KB).** An addition that would cross it is not done until the file shrinks elsewhere first.

---

## Architecture (layered, top-down)

```
NT TradingNode ──> mt5connector.client (data + exec clients, provider, factories, history client)
                      │
                      ├── remote_mt5 (the HTTP shim generated from the inventory)
                      └── push (the hub's push channel, on NT's WebSocketClient)
                                 │                       │
                                 ▼                       ▼
                     mt5connector.server.app     mt5connector.server.ws_server (hub, a
                     (Flask under Wine's         Linux Python) <── the EA inside the terminal
                     Windows Python)
                                 │
                                 ▼
                     the MetaTrader5 package ──> the terminal ──> the broker
```

The adapter reaches the terminal through the server alone: every module that calls the package imports the shim, as `mt5`, and the shim reproduces the package's signatures and types, so a call site reads as a package call. A config is refused when built without a server URL. Neither distribution imports anything of the other; `tests/test_distributions.py` pins both import chains.

---

## The server — rules of engagement

The design documents itself in-repo: `packages/server/mt5connector/server/` module docstrings carry the contracts, and its `README.md` the API, the envelope and what the image must provide. What an agent must not get wrong when touching anything nearby:

- **The wire vocabulary lives once.** `packages/server/mt5connector/server/wire` is a symlink to `packages/client/mt5connector/wire`, and the server's wheel build follows it into real copies; the server imports `mt5connector.server.wire` and nothing of `mt5connector.wire` or `mt5connector.client`, and a wire module imports its siblings relatively. A server module that needs more of the client is a design question, never a second copy.
- **The inventory is the single source.** `mirror.py` declares every mirrored function, its parameters, its structs with their epoch fields and units, the dtypes and the constants; the server's routes, the shim's functions and namedtuples, and the conformance samples are all generated from it. Adding or changing a package function is one inventory row — never a hand-written route, shim function or sample. The static conformance tier decodes the pinned wheel (`MetaTrader5==5.0.6231`, verified by sha256) against the inventory; **the package wins** over any recollection of its API.
- **One lock, one read.** Every package call runs through `Terminal.call`, which holds the one lock around the call and its `last_error()` read, so an answer's error is always its own. Nothing calls the package around it, nothing reads a terminal flag for synchronisation, and nothing waits while holding the lock.
- **The envelope is the contract.** `{ok, result | error, last_error}` on every mirror route. A failure is `ok: false` carrying the package's `last_error` pair, never a success with an empty result. A mirror route passes the package's own answer through, so its `[]` is as ambiguous as the package's; the history routes are what prove emptiness. `shutdown` keeps the server's session: every client shares one terminal connection. Unknown, missing or non-epoch parameters, and a history query in none of its call forms, are refused with HTTP 400 before the package is asked.
- **UTC on the wire, broker time only inside the package.** The server converts every epoch field it answers and every window it is asked through `BrokerClock` at the era of the timestamp itself; a consumer never does clock math — it adds a bar's interval for NT's close stamp and subtracts it for a query. A skipped wall-clock hour is a server error; a repeated hour reads as its first occurrence with one warning per hour, except in the relayed server-time sample, a live reading of the terminal's clock, which reads as the occurrence nearer the server's clock. A zero epoch stays zero.
- **The bootstrap gate.** While the clock is not verified against a fresh server-time sample the EA relayed from a terminal connected without a break for one sample max-age, every route but `/health` and the two relays — the server time's and the commission schedules' — answers 503 with the failure envelope, and `/health` answers 503 itself; health reads that verification state and never calls the terminal. The relay routes accept loopback callers only. A missed bootstrap window or a verified mismatch exits the server process.
- **One worker is always free.** Every call that can reach the terminal or the hub — the mirror routes, the three history routes and the commission read — takes a slot from a pool one smaller than the worker count (`MT5_API_THREADS`, at least 2), without waiting, and a read's publisher check runs inside its slot; a call that finds none is refused at once with the busy code and `Retry-After`, never queued on a worker. `/health` and the relays take no slot. The worker count is set from the peak `/health` reports, with a margin, so normal operation never refuses.
- **No authentication.** The API trusts its network. The image publishes its ports on loopback alone; exposing one is a design change.
- **Two Pythons, one distribution.** The Flask app runs under Wine's Windows Python (the terminal's package is Windows-only) and the hub under a Linux Python; both install `mt5-connector-server`, whose `pyproject.toml` declares every server dependency: `MetaTrader5` under the Windows marker, `numpy` held at 2.2.1, `tzdata` pinned because the Windows Python has no system zone database.
- **The image contract lives in the server's README.** A change to a setting, an entry point, the EA's inputs or what the terminal needs updates its "What the image must provide" section in the same change.

---

## The history protocol — rules of engagement

The terminal substitutes nearest-available data, serves only what it has synced locally, caps a request's span, and answers over-span or unsynced requests with an empty result that looks like "no data". The server owns the protocol that turns those answers into honest ones; `mt5connector/server/history.py` documents it. The rules:

- **A 200 is canonical rows, an empty list included** — only rows the terminal's own answers prove: rows read with a success `last_error`, a window wholly before the floor, records on both sides of it, or the part past the symbol's last quote (the live edge). There is no state beside the rows: a window beginning before the floor answers the rows from the floor on, and the floor itself is a fact of `/history/ranges` alone.
- **A window nothing proves yet is a 503 with `Retry-After`** and the syncing code, never a wait: the server makes its reads and answers; the reads it made are what drive the terminal's backfill, and the client's retry is the sync loop, bounded only by its caller. The server never sleeps or retries inside a request.
- **A window starting after the terminal's time is a 400**, never a retry.
- **No calendar.** Nothing in the server knows when a market is open: no weekday, session or holiday logic, only gap arithmetic on the rows the terminal returned. A weekend is a stretch the terminal has no records for.
- **The span cap is `maxbars − SPAN_MARGIN` periods** (`history_wire.py`), `maxbars` read from `terminal_info` at request time, never hard-coded; a longer window is chunked. A cap breach never resolves on retry.
- **Floors are measured, not assumed** — by the stub the terminal answers for a window before any plausible history, or past a coarse prefix at the series' start, sought only in an answer that begins at the floor. A floor the coarse prefix set is never moved back by a later stub.
- **Every bar is held to its timeframe's grid, and ticks to time order**; a row that breaks either fails the answer.
- **Bars leave the server open-stamped**, as MT5 stamps them; the client stamps the close by adding the interval and asks in open-time bounds.
- **The client retries only what the server tells it to**: the syncing and busy 503s, sleeping the advertised `Retry-After`, until the server answers otherwise or its caller cancels; it has no deadline of its own.

---

## The EA and the hub — rules of engagement

The EA (`ticks.mq5`) and its startup script (`ticks_setup.mq5`) are the only code inside the terminal. The server README documents what the EA publishes and the push protocol, and `ws_server.py` the hub. The rules:

- **One EA per symbol, each on its own chart; nothing is elected.**
  - Every EA receives the account's whole transaction stream, so each publishes only its own symbol's — a request transaction's by its request's symbol — and the spawner also those that name none. Nothing downstream deduplicates.
  - A symbol's first EA publishes it; a later one is answered `duplicate`.
  - The spawner, an input its template sets, alone opens and closes charts, on the hub's request; it publishes its own symbol like any EA, and its chart never closes idle. The terminal starts from a chart profile the image empties before every launch, since it would restore the last one's charts with their EAs: the startup script opens the spawner's chart, and a chart already running the EA is a boot defect it reports, never repairs.
  - Every trade request the adapter sends names its symbol, cancels and modifies included, so its transaction travels through that symbol's EA.
- **The EA reads its socket and its tick cursor on its timer and on every tick**, so the vendored library answers the hub's keepalive pings and acknowledges a close, and a tick whose event the terminal never queued is still published. It reconnects on `TimeLocal()`, because the last-quote time stands still while the market is closed.
- **The hub converts every epoch the EA publishes to true UTC** through the one `BrokerClock` rule; a consumer does no clock math.
- **Nothing is stored or replayed.** A consumer's queue is bounded and overflow closes it; a gap heals through NT's reconciliation and the data client's one bar read-back.
- **The protocol's closed sets are `push_wire.py` members on both sides.** A frame the hub cannot handle is answered with an error frame and closes its connection with a warning; a frame of an unknown kind is logged and skipped.
- **An MQL5 enum whose values the reference does not publish travels by its `EnumToString` name**, a member of a `push_wire.py` enum holding exactly the members the reference lists, so a commission schedule naming any other is refused whole at the relay; or, where the protocol carries integers, as the value the image's compiler assigns, pinned in `push_wire.py`; a duplicate `case` label compiled against the terminal's build proves a value.
- The EA and its script ship as source; whoever builds the image compiles them, and the compile holds only under these requirements: `MetaEditor64.exe` by that exact name (the Wine prefix's volume is case-sensitive), one source per invocation (MetaEditor honours only the last `/compile` argument), from a path without spaces (it truncates one that has them). The chart template that attaches the EA must be UTF-16LE with a BOM and CRLF.
- Nothing in the test suite runs the EA. A change to it is proven by the compile (`0 errors, 0 warnings`) and by a running server whose `/health` stays 200, which takes a fresh server time relayed by the EA; a claim beyond those is unverified.

---

## MT5 pitfalls (the long list)

Measured on IC Markets and Bybit MT5 terminals, and none of it in the vendor's documentation, except where a bullet names the MQL5 reference. Internalize them.

### The broker clock

- **Every epoch the package returns is broker wall-clock time**, on every surface — ticks, bars, deals, orders, positions — not UTC as the documentation claims. The rule is `America/New_York + 7 h`, so the offset is +10,800 s in summer and +7,200 s in winter and changes on US DST weekends.
- **Query windows are compared in the broker base**: `copy_rates_range`, `copy_ticks_range` and `history_*` bounds pass through unconverted, so the conversion runs in both directions.
- **`TimeTradeServer()` is computed in the terminal** from the host clock and a learned offset; it moves while the market is closed, where `TimeCurrent()` (the last quote) stands still. That is why it is the verification source and why a weekend start reaches healthy.
- **A pending order's `expiration` is a broker-time field too.**

### History answers

- **Empty is ambiguous.** `[]` with `last_error` success means any of: the series never existed, the symbol is unselected, the span is at or over the cap, or the region is not yet synced locally. It is never a discriminator on its own.
- **The span cap is the span, not the row count**: `MaxBars − 11` periods is accepted, `MaxBars − 10` answers `[]`, at both 100,000 and 1,000,000. `terminal_info.maxbars` carries the configured value.
- **A request into an unsynced region triggers an async backfill**; a retry seconds to a minute later returns data. Coarse-before-fine does nothing for the finer series. A first wide tick read can block for ~90 s and come back `None` with `last_error (-1, "Terminal: Call failed")`; the retry returns the rows.
- **A narrow tick window into an unsynced region never syncs it**: a one-day `copy_ticks_range` answers `[]` with success and no retry of the same window loads it — hence the widening to the UTC day, then the week.
- **There is no sync signal.** `SeriesInfoInteger(SERIES_SYNCHRONIZED)` stays 1 through a backfill; reading it on an unselected symbol adds the symbol to Market Watch.
- **The floor stub**: a request fully before a synced series' floor returns exactly one out-of-window row, the floor bar; a straddling window silently left-clamps.
- **History depth is per broker and per series**: a sliding `MaxBars` window on M1, hourly only from some date on H1 with daily-spaced rows before it. Synced depth survives a terminal restart.
- **Trade ticks do not exist** on this venue class: `copy_ticks_range(flags=COPY_TICKS_TRADE)` returns 0 rows; quote ticks carry real sub-second `time_msc`. The flag values are `INFO=1`, `TRADE=2`, `ALL=-1`.
- **Bars are built from one price per symbol**, declared by `symbol_info.chart_mode` (0 is BID); `MqlRates.spread` is one scalar per bar; no ask-side bar series exists.
- **Pre-1970 epochs** answer HTTP 500 from the package path; the floor probe asks a window from 1970-01-02.

### Symbols, currencies and sessions

- **The venue is the judge of what exists.** `symbol_info` says what a symbol is; no in-repo list of symbols, suffixes or currencies decides a request. Suffixed pairs (`EURUSD.a`) can be the tradeable universe while the base names are close-only.
- **`currency_profit` alone names settlement.** `currency_base` is meaningless for a CFD (a `DE40` listed with base USD settles EUR) and `currency_margin` is not always the base. Account currencies can be non-ISO (`UST`).
- **An unselected or closed symbol quotes zeros**: `symbol_info_tick` returns an all-zero struct with `time=0`; a real tick has `time != 0`.
- **`symbol_info` carries 96 fields and no commission**; deals carry `commission` and `fee`. A commission schedule exists only in MQL5 (`SymbolInfoCommissions`), one rule with one tier per symbol, charged on both legs by one broker and on entry alone by another.
- **Weeks run Monday 00:00 to Friday 23:00 in broker time** with no weekend bars; H2 and coarser bars open on broker-midnight multiples.

### Orders, positions and the transaction stream

- **Hedging is per ticket.** A position's identifier is its opening order's ticket, stable across a partial close, with the average price retained; every deal (`OUT`, `OUT_BY`, stop-loss) carries `position_id`. Comments are venue-mutated and refused outright at 30 characters or more, never truncated.
- **One exit bracket per position.** A pending order carrying `position=ticket` is accepted and the binding silently ignored; its fill opens a new, opposite position. A second `TRADE_ACTION_SLTP` overwrites the first; SLTP on a position confirms as a POSITION transaction alone, and the position's `time_update_msc` does not move on it.
- **`MqlTradeRequest.magic` is `ulong`, `ORDER_MAGIC` reads back `long`**; 63 bits are safely positive in both views.
- **The filling mode is per symbol**, a bitmask on `symbol_info().filling_mode`; a wrong mode is retcode 10030.
- **`order_send` can report "no connection" (10031) for a trade the server executed.** The order exists; the reply does not say so.
- **`account_info.trade_allowed == false` under `terminal_info.trade_allowed == true` marks an investor (read-only) session** — a configuration error to fail at startup, not a refusal to discover on the first order.
- **`OnTradeTransaction` is account-scoped and complete through one EA**: two EAs receive byte-identical streams. `DEAL_ADD` alone carries the fill; `order_state` on it is a meaningless default; fill ordering against `ORDER_DELETE`/`HISTORY_ADD` is not guaranteed; `TRADE_TRANSACTION_REQUEST` trails the lifecycle events with the full request and result, and the MQL5 reference fills only its type — its symbol is the request's, which a delete or modify of a pending order may leave empty.

### Charts

- **At most `CHARTS_MAX` (100) charts are open at once** (MQL5 reference), the spawner's and the startup script's included; `ChartOpen` past it returns 0. The symbols in use at once stay below it.
- **A chart that does not open is reported, never waited on**: the spawner answers every `open_chart` with `chart_opened` or `chart_failed` and the failing call's error, and a read of the symbol the server posts to the hub fails naming it until a chart of it opens or its EA says hello.
- **An EA answered `duplicate` closes its own chart; the spawner first outlasts a stale connection**: it says hello again every `ReconnectIntervalSec` and closes its chart only when still refused `HubPingTimeoutSec`, the longest the hub takes to drop a dead connection while its writes to it drain, after its first refusal.
- **A symbol leaves Market Watch only with no chart of it open and no open position** (MQL5 reference, `SymbolSelect`), so the idle close closes the chart first, then deselects. A chart closes once its symbol is out of use for `MT5_CHART_IDLE_SECONDS`, and the spawner closes it — one it opened, or, for a publisher that predates it, the chart of the symbol running the EA.
- **The deselect right after `ChartClose` may be refused while the close completes**, so the spawner retries it on its timer until it succeeds or the chart reopens, slowly while positions or orders hold the symbol.
- **A symbol with open positions or pending orders never closes idle**: the spawner answers `close_chart` with `chart_kept` and the hub re-arms its idle period, so its transactions keep flowing.
- **`ChartApplyTemplate` only queues the template** (MQL5 reference): the template's EA loads after the call returns, so the spawner remembers the charts it opened to keep an open idempotent.

### The terminal under Wine

- **The WebRequest/socket allowlist lives in memory** and is wiped by any terminal restart; the `setup.ini` `WebRequestUrl` key is MT4-only and MT5 ignores it. The image re-adds `127.0.0.1` after each start, and clicking OK re-initialises attached EAs.
- **`MetaTrader5.initialize()` blocks on an IPC timeout without an account.**
- **The Windows Python's `numpy` is held at 2.2.1**: later releases crash under Wine 10.
- **The terminal never updates itself**; its update dialog is harmless and nothing prevents it.

---

## Test tiers

- **Every test runs without a terminal.** Client tests (`tests/client/`) patch the shim where a module imported it (`tests/client/conftest.py`'s `mock_mt5`); `tests/server/conftest.py` wraps a `MagicMock` package double in a real `Terminal`, serves the app through Flask's test client or a real waitress on a loopback port, and points the shim at it. The server imports the package only inside `main()`.
- **Each side gets its own wire copy.** In one process `mt5connector.wire` and `mt5connector.server.wire` are distinct modules, so an enum identity check or an exception class from one does not match the other: a test hands the server objects from `mt5connector.server.wire`, and the client objects from `mt5connector.wire`.
- **The static conformance tier** (`tests/conformance/test_static_conformance.py`) fetches the pinned wheel by sha256 and checks the inventory and shim against it. **The live tier** needs a Windows host and a terminal and runs only when asked for; `README.md` says how to run both.
- **An `xfail` is `strict=True` and names the defect it pins.** An xfail that starts passing is a fix to record, never a marker to leave.

---

## Things never to do

- **Don't commit a credential, an `.env`, an account number, or a server name that identifies an account** — and don't log one. A settings error names the variable, never a credential's value.
- **Don't patch NT to fit MT5.** The connector works the way NT expects an adapter to work; where the venue's shape differs (per-ticket hedging, venue-minted ids, bid-built bars), the adapter translates, and anything the adapter cannot translate is raised with the consumer, not papered over in NT.
- **Don't hand-write what the inventory generates** — a route, a shim function, a struct, a conformance sample.
- **Don't do clock math outside the server.** The server answers in true UTC, so a consumer that converts broker time converts it twice.
- **Don't trust `[]`.** An empty package answer proves nothing on its own; the protocol's evidence rules decide what is canonical.
- **Don't `pip install` into any of the Pythons.** The env files and the two projects' `pyproject.toml` are the declarations.
- **Don't reach for a second lock, a lock bypass or a wait inside a request** when a terminal call is slow; health does not need the terminal, a call that cannot get a slot is refused, and a window that cannot be proven is a 503 the client retries.
