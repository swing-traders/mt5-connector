# AGENTS.md

Instructions for AI coding assistants working in this repo. Read this first; it captures the load-bearing context that isn't obvious from the code alone.

If something here conflicts with the code, the code wins — flag the drift and update this file in the same PR.

This file holds GUIDELINES and rules of engagement. How a subsystem works belongs in legible code and succinct comments stating its deliberate decisions (see §7) — never here, and never as history.

---

## What this repo is

An owned hard fork of [aulekator/mt5-connector](https://github.com/aulekator/mt5-connector): a [NautilusTrader](https://nautilustrader.io/) (NT) adapter for MetaTrader 5. It does not track upstream.

- **Client package** (`mt5connect/`) — NT data and execution clients, instrument provider, factories, and the remote backend client.
- **Server** (`mt5server/`) — the containerised MT5 terminal with the HTTP and WebSocket API the remote backend talks to.

Delivery:

- PRs merge to `main`.
- A release is a `v0.4.0+st.N` tag; CI builds it into a wheel and publishes it on the fork's GitHub Pages package index.
- Consumers pin a release exactly, by its full version, from that index.

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
    "to. Start the server under mt5server/ and set the URL in your .env."
)

# YES — the fact, and nothing else
raise ValueError("remote backend: no server URL is configured")
```

A durable **design principle** (a real why-it's-built-this-way) belongs in the code it governs, a README, or — if it is a repo-wide rule of engagement — this file. Delete pure history outright; park agent reasoning in project memory.

### 8. Dependencies go through `environment.yml` + mamba, never ad-hoc pip

The env (`mt5-connector`) is declared by `environment.yml`. Runtime deps live in its `pip:` subsection (conda-forge for `python`/`pip` only). To add or bump a dependency:

```bash
# 1. add/edit the pin in environment.yml (pip: subsection)
# 2. sync the declared env via mamba — this drives the pip subsection too
mamba env update -n mt5-connector -f environment.yml
```

Never `pip install <pkg>` straight into the env: it leaves the package **undeclared**, so the env can't be reproduced from the file and the next `env create` silently lacks it. Dev-only tooling goes in `environment.dev.yml` the same way. If you find a package installed but missing from the yml, that's drift — declare it and re-sync.

### 9. Ad-hoc tooling belongs in `scripts/`

One-off migrations, audits, studies, and repair utilities belong under the gitignored `scripts/` directory at the repository root, never in the package or a supported CLI. Production code, tests, and READMEs must not import or depend on them. Promote a script into the main codebase only when it becomes a recurring, supported workflow with a maintained contract.

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
