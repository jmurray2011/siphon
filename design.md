# siphon — design

> Working name. A single-binary, config-driven daemon that mirrors arbitrary HTTP APIs into a local database, scheduling its own polling against a measured rate-limit budget.

**Status:** v0.1 built; see §15 for what is next and why the order changed
**Language:** Rust
**Primary store:** SQLite and Postgres, both first-class. See §9 and open question 9, now decided.

---

## 1. Problem

Pulling data out of a third-party HTTP API and keeping a local copy fresh is a solved problem that everyone re-solves badly. The recurring work is always the same four things:

1. Walking pagination (offset, page number, cursor, `Link` header, `next_url` in body)
2. Not getting rate-limited, and recovering gracefully when you are
3. Persisting where you got to, so a restart doesn't re-fetch everything or silently skip records
4. Absorbing upstream schema changes without the pipeline dying

Existing tools solve this either (a) in Python with a heavy runtime and orchestration stack, or (b) in Go/Rust but with pagination pushed back into user-written imperative code. Nothing occupies the "one static binary, one YAML file, SQLite on disk" corner.

There is also a fifth problem that no existing tool treats as a problem at all:

5. **Deciding what to spend a finite request budget on.** Every tool models the rate limit as a constraint to avoid violating — a static token bucket, plus retry-on-429. None of them treat the remaining budget as a resource to allocate across competing streams. The consequence is a well-documented failure mode: under pressure, a cron-driven pipeline spends its budget uniformly, hits the wall, backs off, hits it again, and falls hours behind on the data that mattered while faithfully refreshing the data that didn't.

And a sixth, which only shows up once you have written (1)-(4) more than twice:

6. **Every project that needs the same upstream keeps its own copy of it.** Not just its own fetch loop - its own store, its own schema, its own watermark, and its own share of the same rate-limit budget. Ten projects pulling from four APIs is forty implementations and ten databases, and the copies drift, because nothing makes them agree on what "new" means. The duplication that hurts is not the storage, which is cheap. It is that the same tickets are fetched twice by two fetchers that disagree.

siphon's thesis is that (5) is the actual differentiator, and that (1)-(4) are table stakes it has to get right in order to earn the right to solve it. (6) is what makes it worth running as shared infrastructure rather than as a library each project vendors: one mirror per upstream, read by many consumers, paced once against one budget.

That last part is a claim about *who reads the store*, and it is load-bearing. A mirror with one owner can put its store anywhere. A mirror with many consumers cannot - which is what forces the storage decision in §9 rather than leaving it to taste.

---

## 2. Goals

- **G1.** Declaratively describe an API endpoint — auth, pagination, incremental cursor, record extraction — in YAML, with no user code.
- **G2.** Let the operator declare *intent* (how fresh each stream needs to be, and what matters most) rather than *mechanism* (a cron expression), and schedule polling against a continuously measured rate-limit budget to satisfy that intent.
- **G2a.** Measure the upstream's actual rate of change, and tell the operator when their declared intent doesn't match reality.
- **G3.** Durable, resumable state. A crash mid-pagination resumes mid-pagination.
- **G4.** Survive upstream schema drift without operator intervention or data loss.
- **G5.** Ship as one binary with no runtime dependency. `siphon run config.yaml` and nothing else.
- **G6.** Make the config cheap to author, since there is no connector catalog to fall back on. Going from a bare URL to a working stream should take under a minute.
- **G7.** Be readable by consumers siphon does not know about, without siphon serving anything. One upstream is mirrored once and read by many projects, each of which keeps its *derived* data - search indexes, embeddings, classifications, resolved names - in its own store, joined to the mirror by upstream ID. The line is: siphon owns what was fetched, the consumer owns what was computed. A consumer that has to re-fetch to compute is a failure of this goal; a consumer that has to re-compute is not.

## 3. Non-goals

- **NG1. Serving a read API.** Explicitly cut. The output is a SQLite file (or Postgres schema); point Datasette, PostgREST, or your own app at it. Building a bespoke read layer is the single fastest way to turn this from a small tool into an unfinishable product. G7 does not reverse this, it depends on it: the way many consumers read one mirror is a database wire protocol that every language already speaks, not an HTTP surface siphon has to design, version, authenticate and keep up. "We will have to serve it" is true, and Postgres is the answer to it.
- **NG2. Writing back to the source API.** Read-only mirror. Bidirectional sync is a different, much harder product.
- **NG3. Transformations.** No DSL, no expression language beyond field extraction. Transform in SQL downstream.
- **NG4. A connector catalog.** No pre-built Stripe/Salesforce/HubSpot connectors. Maintaining those is a full-time treadmill and is the business that other people are already in. Users write ~30 lines of YAML per endpoint.
- **NG5. Streaming, sub-second latency, distributed operation.** Single node, polling, minutes-scale freshness.

## 4. Positioning

Two prior products started at exactly this shape and left it: one pivoted to Postgres CDC and was then acquired with its cloud service shut down; the other became an embedded-integrations platform for B2B SaaS. The lesson taken here is that the generic engine is a **tool, not a company** — so the scope is deliberately set at what one person can finish and maintain, and the parts that create ongoing obligation (connector catalog, hosted service, read API) are cut.

Closest neighbours and why they don't cover this:

| Tool | Lang | Gap |
|---|---|---|
| Redpanda Connect | Go | Pagination via string interpolation into the URL; stream-shaped mental model |
| Conduit HTTP source | Go | Pagination requires an external `.js` file |
| Vector `http_client` | Rust | No pagination or cursor concept; observability-shaped |
| Airbyte low-code CDK | Python | The right config model, wrong runtime weight |
| dlt REST API source | Python | Library, not a daemon; you own the scheduling |
| Steampipe | Go | Zero-ETL — queries live, never persists |

All six share the same blind spot: the rate limit is a static ceiling configured by the operator, and scheduling is a cron expression that ignores it. None of them can answer "I have 800 requests left this hour and four streams that want them — which do I serve?"

Under G7 one of those rows stops being a competitor. Steampipe is listed above for what it lacks - it queries live and never persists - but that is the exact complement of what siphon is: a durable, incrementally maintained, budget-paced store with no query surface of its own. Steampipe over a siphon-maintained Postgres is a better answer than either half, and it is a reason *not* to build the query layer here. The same goes for Datasette, PostgREST and anything else that speaks to a database. siphon's job is to make the store worth pointing them at.

---

## 5. Architecture

```
                    ┌──────────────┐
   config.yaml ────▶│  Config      │  parse, validate, resolve secrets
                    │  loader      │
                    └──────┬───────┘
                           ▼
      ┌────────────────────────────────────────┐
      │  Governor  (one per source)            │
      │                                        │
      │   measured budget ──┐                  │
      │   freshness SLOs ───┼─▶ admission      │
      │   priorities ───────┘   decision       │
      └───────────────┬────────────────────────┘
                      │ grants
                      ▼
         ┌─────────────────────────────────────┐
         │  Sync worker (per stream)           │
         │                                     │
         │  ┌────────┐  ┌────────┐  ┌────────┐ │
         │  │Paginator│─▶│ Fetcher│─▶│Extract │ │
         │  └────┬───┘  └───┬────┘  └───┬────┘ │
         │       │          │           │      │
         │       └──────────┘           │      │
         └───────────────────┬──────────┘      │
                             ▼                 │
                    ┌──────────────┐           │
                    │  Writer      │◀──────────┘
                    └──────┬───────┘  batch upsert + checkpoint, one txn
                           ▼
                    ┌──────────────┐
                    │  SQLite      │
                    └──────┬───────┘
                           │ observed limits, ETags, staleness
                           └──────────▶ back to Governor
```

The loop is the point: the store feeds measured budget and current staleness back to the governor, which decides what runs next. There is no separate scheduler component — **scheduling is what the governor does.**

**Components**

- **Config loader** — parse YAML into typed structs, validate, resolve `${ENV_VAR}` and file-based secrets. Fails fast and loudly; a bad config never starts a run.
- **Governor** — the novel piece. Owns the measured rate-limit budget for a source and decides which stream gets the next request. See §8.
- **Paginator** — a state machine. Given the config strategy and the previous response, produces the next request or signals exhaustion. Pure, trivially testable.
- **Fetcher** — issues the request (with conditional-request headers where available), classifies the response (success / not-modified / retryable / fatal / rate-limited), and reports observed limit headers back to the governor.
- **Extractor** — pulls the record array out of the body via JSONPath, plus the primary key and cursor field per record.
- **Writer** — batches records, upserts, and advances the checkpoint **in the same transaction**. This is the crux of the durability guarantee.

---

## 6. Configuration

The config format is the actual product. Everything else is implementation.

```yaml
version: 1

store:
  type: sqlite
  path: ./data/mirror.db

defaults:
  timeout: 30s
  retry:
    max_attempts: 5
    initial_backoff: 1s
    max_backoff: 5m

sources:
  github:
    base_url: https://api.github.com
    auth:
      type: bearer
      token: ${GITHUB_TOKEN}
    headers:
      Accept: application/vnd.github+json
    budget:
      # fallback ceiling, used only until the API tells us otherwise
      requests_per_hour: 5000
      # trust the API's own accounting over the static guess
      observe:
        remaining_header: X-RateLimit-Remaining
        limit_header: X-RateLimit-Limit
        reset_header: X-RateLimit-Reset
        reset_format: unix_seconds
      # fraction of the measured budget siphon will actually spend.
      # leaves headroom for whatever else uses this token.
      utilization: 0.8
    conditional_requests: true     # send If-None-Match / If-Modified-Since

streams:
  - name: issues
    source: github
    freshness: 15m                 # intent: never staler than this
    priority: high                 # high | normal | low | best_effort
    request:
      path: /repos/{{ owner }}/{{ repo }}/issues
      params:
        state: all
        per_page: 100
        sort: updated
        direction: asc
    vars:
      owner: rust-lang
      repo: rust
    pagination:
      strategy: link_header
      rel: next
    records:
      path: $              # JSONPath to the array in the response body
      primary_key: $.id
      cursor:
        field: $.updated_at
        type: rfc3339
        param: since       # injected into the next full sync's query params
    write:
      mode: upsert         # upsert | append | replace
      soft_delete: false

  # historical walk. no freshness target — consumes surplus budget only,
  # and yields the moment `issues` needs headroom to hold its SLO.
  - name: issues_backfill
    source: github
    priority: best_effort
    direction: backward
    request:
      path: /repos/{{ owner }}/{{ repo }}/issues
      params: { state: all, per_page: 100, sort: updated, direction: desc }
    # ...same pagination/records blocks as `issues`
```

**Why there is no `schedule:` key.** Cron states a mechanism; `freshness` states the requirement the mechanism was trying to satisfy. Given the requirement, the governor can do better than cron when budget is plentiful and degrade sensibly when it isn't — neither of which a fixed schedule can do. `schedule: "*/15 * * * *"` remains supported as an escape hatch for genuinely time-anchored pulls (an endpoint that only refreshes at 03:00), and it bypasses the governor's admission logic entirely apart from hard limits.

**Multi-stream / parent-child** (e.g. fetch comments for each issue) is a `parent` reference. Deferred to v0.3; the config shape is reserved now so it isn't bolted on later.

```yaml
  - name: issue_comments
    source: github
    freshness: 6h
    priority: low
    parent:
      stream: issues
      key: $.number         # each parent record supplies one var
      as: issue_number
    request:
      path: /repos/{{ owner }}/{{ repo }}/issues/{{ issue_number }}/comments
```

---

## 7. Pagination

Named strategies, not code. Each is a small state machine over `(previous_response, previous_state) -> Option<NextRequest>`.

| Strategy | Config | Termination |
|---|---|---|
| `offset` | `limit_param`, `offset_param`, `page_size` | short page, or empty page |
| `page` | `page_param`, `size_param`, `start_page` | empty page, or `total_pages` field |
| `cursor` | `cursor_param`, `cursor_path` (JSONPath into body) | cursor absent or null |
| `link_header` | `rel` (default `next`) | header absent |
| `next_url` | `url_path` (JSONPath to a full URL in body) | field absent or null |
| `none` | — | single request |

Cross-cutting options on every strategy:

- `max_pages` — hard safety valve. Default 10,000. Prevents an infinite loop from a misbehaving API filling the disk.
- `stop_on_empty` — default true.
- `stop_on_seen_key` — for reverse-chronological feeds: stop once a primary key already in the store is encountered. This is how you make an incremental sync cheap on APIs with no cursor.

**Escape hatch:** if none of these fit, `strategy: rhai` with a small embedded script. This exists so the tool never hard-blocks a user, but it is documented as a last resort, and the goal is that the built-in strategies cover the long tail. (This is exactly where Conduit and Benthos start; the whole point is that it should be where *siphon* ends.)

**Mid-pagination resume.** The paginator's state (`next_url`, cursor token, or page number) is serialized to the checkpoint after every successful page write. A crash on page 47 of 200 resumes at page 48.

---

## 8. The governor

The core of the design. Everything else here exists in some form elsewhere; this doesn't.

### 8.1 Measuring the budget

Three sources of truth, in increasing order of authority:

1. **Configured ceiling.** Used only until the API says something. Politeness floor when the API says nothing at all.
2. **Observed headers.** Parse the common families — `X-RateLimit-*`, the IETF `RateLimit-*` draft fields, `Retry-After` in both delta-seconds and HTTP-date forms. This yields `(remaining, limit, reset_at)`, which is the real budget.
3. **Observed rejections.** A 429 means the previous two were wrong. Ratchet the effective ceiling down and don't ratchet it back up within the window.

The governor maintains, per source: `remaining`, `reset_at`, and `spend_rate = remaining × utilization / (reset_at − now)`. That's the request-per-second allowance to distribute.

**The local clock is untrusted.** `reset_at` is a server timestamp; `now` is local. A few seconds of drift is harmless, but a badly-set container clock makes pacing arbitrarily wrong in either direction — sprinting into the wall if local time runs slow, refusing to spend at all if it runs fast. So derive the window from the `Date` response header on the same response that carried the limit headers:

```
skew        = server_date − local_now        # recomputed each response, smoothed
window_left = reset_at − (local_now + skew)
```

Clamp `window_left` to a sane range and fall back to the configured ceiling if it lands outside it. A negative or absurd window means the parse or the clock is wrong, and the correct response is to slow down, not to assume infinite budget.

**Persist it.** The learned ceiling goes in `_siphon_budget`. A process that re-learns its limit by getting 429'd on every restart is a process that punishes you for deploying.

### 8.2 Allocating the budget

Each stream carries a **deficit** — how far past its freshness target it is:

```
deficit(s) = (now − s.last_success_at) / s.freshness
```

`deficit ≥ 1.0` means the SLO is violated. Streams with `priority: best_effort` and no `freshness` have deficit permanently 0.

On each admission decision the governor picks the stream with the highest `deficit × weight(priority)`, subject to:

- `spend_rate` allows a request now (otherwise sleep until it does)
- the stream isn't already running (no overlap)
- for `best_effort` streams, **every** SLO-bearing stream has deficit below a `surplus_threshold` (default 0.5)

That last clause is the whole backfill story. A backfill is not a special mode with its own scheduler; it is a stream that only eats leftovers and yields the moment anything with a deadline starts slipping. The snapshot-then-incremental transition that every other tool handles as an explicit state machine simply doesn't exist here — both run concurrently, and the allocator arbitrates.

**Degradation is graceful and predictable.** When budget tightens, low-priority streams drift toward their bounds while high-priority ones hold. When budget is plentiful, streams beat their SLOs for free. Cron can do neither.

**Starvation guard.** Weight by priority, but never let a low-priority stream be indefinitely preempted: deficit grows without bound, so a `low` stream at deficit 8.0 eventually outranks a `high` stream at deficit 0.3. Priority breaks ties and biases the ordering; it does not create castes.

### 8.3 Conditional requests

`If-None-Match` / `If-Modified-Since` from a stored `ETag` / `Last-Modified` per request URL. On several major APIs a 304 does not decrement the rate limit at all, which makes polling unchanged data close to free and is what makes aggressive freshness targets affordable in the first place.

Sequencing note: 304 handling has to be right before the governor can be trusted, because a 304 must advance `last_success_at` (the data *is* fresh — it's confirmed unchanged) without counting against spend. Get that backwards and every stream looks permanently stale.

### 8.4 Response classification

| Class | Codes | Action |
|---|---|---|
| Success | 2xx | Extract and write; update budget from headers |
| Not modified | 304 | Advance `last_success_at`, write nothing, usually no budget spent |
| Rate limited | 429, or 403 with rate-limit headers | Ratchet ceiling down, honour `Retry-After`, don't count against retry budget |
| Retryable | 500, 502, 503, 504, timeouts, resets | Exponential backoff with full jitter, counts against retry budget |
| Fatal | 400, 401, 403 (no rate headers), 404, 422 | Fail the stream run, log loudly, don't advance checkpoint |

A fatal error on one stream must not stop other streams. Failures are per-stream and surfaced in the run log — and a failing stream keeps accruing deficit, so `siphon status` shows it as an SLO breach rather than only as a log line someone has to notice.

### 8.5 Poison-page quarantine

Fatal errors and mid-pagination resume interact badly if left alone. §7 says a crash on page 47 resumes at page 48; §8.4 says a fatal error doesn't advance the checkpoint. Together, a permanently-broken page 47 — a 422 on one malformed cursor, a 404 on a deleted parent — means the stream retries page 47 forever, never reaches page 200, accrues unbounded deficit, and because deficit drives admission, the governor keeps *preferentially* handing budget to the one stream guaranteed to waste it. Head-of-line blocking plus a scheduler that rewards it.

Rule: after `quarantine_after` consecutive fatal failures on the same paginator state (default 3), record the state and the response in `_siphon_quarantine`, skip forward, and mark the run `partial`. A stream with any quarantined pages never reports `ok` — it reports `partial` with a count, so the gap is visible rather than silently tolerated.

`siphon quarantine <stream>` lists them; `siphon quarantine <stream> --retry` clears them for another attempt after the operator has fixed whatever it was.

This is the second-order failure that only exists because of the governor. Worth calling out as such: giving the scheduler a preference function means every "this stream is behind" signal is now also a resource-allocation signal, and anything that can produce a false one is a budget leak.

### 8.6 Measuring the need

The third term. §8.1 measures what you can afford; §8.2 takes what the operator says they need. But the declared `freshness` was always a guess — nobody knows how often an endpoint actually changes when they first write the config.

Every run already records whether the response was a 304, how many records returned, and how many were genuinely new or modified versus unchanged. Aggregated per stream over a window, that yields an **observed change interval**: the real churn rate of the upstream resource.

Compare it to the declared target and you can tell the operator something no tool in this category tells them:

```
$ siphon status --advise

releases       declared 1h    observed change interval ~26h
               96% of polls returned 304 or zero new records
               → suggest `freshness: 12h`, frees ~1400 req/day

issues         declared 15m   observed change interval ~4m
               31% of polls returned ≥1 new record at the cursor head
               → target is appropriate; consider 10m if budget allows
```

This closes the loop. A wrong guess currently starves the streams that needed the budget, and `siphon status` reports the breach without mentioning that the cause is three streams down polling a changelog that updates monthly.

**Prior art, borrowed from outside the category.** Optimal refresh policy for polled resources is settled work in the web-crawler literature (Cho and Garcia-Molina), and adaptive polling is standard in serious feed readers. It has never crossed into ETL because ETL treats the schedule as an input rather than something to infer. Since siphon already treats it as a requirement rather than a mechanism, inferring it is a natural extension rather than a bolt-on.

**Advisory only.** The tool prints the recommendation; the human edits the YAML. Auto-tuning — where the governor silently widens `freshness` on its own — is a much larger and scarier feature, because it means the tool gets to decide your data doesn't need to be fresh. See §17.

Implementation is a SQL query over `_siphon_runs` plus a print function. Roughly 50 lines, and it makes the otherwise-invisible conditional-request hit rate actionable.

---

## 9. Storage model

**Target choice follows from who reads it.** A single-owner mirror is happiest as a SQLite file: no server, no ceremony, and `sqlite3` on the same box. The moment more than one project reads it, or the daemon runs on a different host from a consumer, that file stops working - not because SQLite is weak, but because a file is reachable from exactly one filesystem and tolerates exactly one writer. Both are true of the setup this is actually for.

So both targets are first-class, and they are for different deployments rather than different maturity levels:

- **SQLite** for a single-owner mirror on the same box as its reader, and for `siphon test`, examples, and anything self-contained.
- **Postgres** for a shared cache: several consumers, possibly several hosts, each holding its own derived tables next to the cached ones and joining locally.

This is what makes G7 work without an HTTP API, and it is why the storage layer is a trait rather than a struct over `rusqlite`. Deciding it late is how the trait gets designed wrong - it would have been shaped entirely by what a single embedded connection happens to make easy, and then bent afterwards around transactions, connection pooling and concurrent writers, all of which SQLite lets you ignore and Postgres does not.

Two-layer: an append-only raw layer, and a typed projection.

```sql
-- raw landing zone; always written, never lossy
CREATE TABLE _siphon_records (
  stream      TEXT NOT NULL,
  pk          TEXT NOT NULL,
  payload     TEXT NOT NULL,        -- verbatim JSON of the record
  hash        BLOB NOT NULL,        -- of payload; distinguishes re-seen from changed
  cursor_val  TEXT,
  fetched_at  INTEGER NOT NULL,     -- unix millis
  PRIMARY KEY (stream, pk)
) STRICT;

-- checkpoint: one row per stream
CREATE TABLE _siphon_state (
  stream          TEXT PRIMARY KEY,
  cursor_val      TEXT,             -- high-water mark for incremental sync
  page_state      TEXT,             -- serialized paginator state, NULL if idle
  last_run_at     INTEGER,
  last_success_at INTEGER,          -- drives deficit; advanced by 304s too
  records_seen    INTEGER NOT NULL DEFAULT 0
) STRICT;

-- learned budget, per source. survives restarts so the limit isn't
-- re-learned by getting 429'd after every deploy.
CREATE TABLE _siphon_budget (
  source          TEXT PRIMARY KEY,
  observed_limit  INTEGER,          -- from headers, or inferred from rejections
  window_secs     INTEGER,
  remaining       INTEGER,
  reset_at        INTEGER,
  ratcheted_at    INTEGER,          -- last time a 429 forced the ceiling down
  updated_at      INTEGER NOT NULL
) STRICT;

-- conditional request validators, keyed by canonical request URL
CREATE TABLE _siphon_validators (
  request_key   TEXT PRIMARY KEY,
  etag          TEXT,
  last_modified TEXT,
  seen_at       INTEGER NOT NULL
) STRICT;

-- observed schema, for drift detection
CREATE TABLE _siphon_schema (
  stream     TEXT NOT NULL,
  field      TEXT NOT NULL,         -- dotted path
  json_type  TEXT NOT NULL,         -- null|bool|number|string|array|object
  first_seen INTEGER NOT NULL,
  last_seen  INTEGER NOT NULL,
  PRIMARY KEY (stream, field, json_type)
) STRICT;

CREATE TABLE _siphon_runs (
  id            INTEGER PRIMARY KEY,
  stream        TEXT NOT NULL,
  started_at    INTEGER NOT NULL,
  ended_at      INTEGER,
  status        TEXT,               -- ok|failed|partial|not_modified
  pages         INTEGER,
  records       INTEGER,            -- records seen
  records_new   INTEGER,            -- inserts
  records_changed INTEGER,          -- updates where the payload hash differed
  not_modified  INTEGER,            -- count of 304 responses
  requests      INTEGER,            -- budget actually spent
  deficit_at_admission REAL,        -- how late it was when the governor picked it
  error         TEXT
);

-- pages that failed fatally and were skipped, so gaps are visible
CREATE TABLE _siphon_quarantine (
  stream       TEXT NOT NULL,
  page_state   TEXT NOT NULL,       -- the paginator state that kept failing
  attempts     INTEGER NOT NULL,
  status_code  INTEGER,
  body_excerpt TEXT,
  first_seen   INTEGER NOT NULL,
  last_seen    INTEGER NOT NULL,
  PRIMARY KEY (stream, page_state)
) STRICT;
```

**The typed projection.** For each stream, siphon maintains a real table (`issues`, not `_siphon_records`) with inferred columns, refreshed from the raw layer. Types are inferred from observed JSON types, widening to `TEXT` on conflict. This is what users actually query.

Rationale for keeping both: the raw layer means a schema-inference bug is never data loss — reproject and move on. Cost is roughly 2x disk, which for this class of workload is acceptable and can be disabled with `store.raw: false`.

**Write transaction.** Per page: insert/upsert records, update `_siphon_schema`, advance `_siphon_state.page_state`, commit. One transaction. This yields **at-least-once with idempotent upserts**, which for a mirror is equivalent to exactly-once.

---

## 10. Schema drift

The failure mode that kills homegrown scripts. Policy:

- **New field appears** → add a nullable column to the projection. No intervention.
- **Field disappears** → keep the column, write NULL. Record in `_siphon_schema` that it stopped being seen.
- **Type changes** (e.g. `number` → `string`) → widen the projection column to `TEXT` and reproject historical rows from the raw layer. Log a warning.
- **Field becomes an object/array** → store as JSON text in that column.
- **Primary key path missing from a record** → fatal for that record; write to a dead-letter table with the raw payload, continue the run. Never silently drop.

`siphon schema <stream>` prints the observed schema and its history. `siphon reproject <stream>` rebuilds the typed table from raw.

---

## 11. Auth

Config-declared, no code:

- `none`
- `bearer` — static token
- `basic` — user/pass
- `api_key` — header or query param, configurable name
- `oauth2_client_credentials` — token endpoint, client id/secret, automatic refresh on 401 with retry-once semantics

OAuth2 authorization-code (three-legged, per-end-user) is **out of scope**. That's the Nango problem, it requires a browser flow and a token store per user, and it's where "small tool" becomes "platform."

Secrets resolve from `${ENV}`, `file:/path`, or the OS keychain. Never inlined in config, and `siphon validate` warns if they are.

---

## 12. CLI

```
siphon infer <url> [--auth ...]     # probe an endpoint, emit starter YAML
siphon run [config.yaml]            # daemon; governor schedules all streams
siphon sync <stream> [--full]       # one-shot, bypasses admission; --full ignores cursor
siphon validate [config.yaml]       # parse + check, no network
siphon test <stream> [--pages 1]    # fetch one page, print records, write nothing
siphon status                       # per-stream SLO attainment, lag, budget
siphon status --advise              # compare declared freshness to observed churn
siphon budget                       # per-source: observed limit, spend rate, allocation
siphon quarantine <stream> [--retry] # list skipped pages; --retry clears them
siphon schema <stream>              # observed schema + drift history
siphon reproject <stream>           # rebuild typed table from raw
siphon reset <stream> [--data]      # clear checkpoint; --data also drops records
```

**`siphon infer` is the substitute for the connector catalog cut in NG4.** Hit the endpoint once and guess the config: `Link` header present → `link_header`; body keys matching `next|next_cursor|nextPageToken|next_url` → `cursor` or `next_url`; `page`/`offset`/`limit` echoed in the response → `offset`/`page`. Find the record array as the longest array of homogeneous objects. Pick the primary key from fields named `id`/`uuid`/`_id` that are unique across the sample. Guess the cursor from any RFC3339-looking `updated_at`/`modified`/`last_modified` field. Emit commented YAML with the guesses flagged, and never write it to disk without the user redirecting it.

Everything it guesses is checkable in seconds with `siphon test`, so wrong guesses are cheap. Without this command, "no connector catalog" is a real adoption tax; with it, the tax is about 40 seconds.

**`siphon status` reports SLO attainment, not just last-run success** — that's the number an operator actually wants, and it's only computable because freshness is declared:

```
STREAM            FRESHNESS  LAG    ATTAIN(7d)  LAST OK   STATE
issues            15m        4m12s  99.2%       4m ago    ok
issue_comments     6h        1h03m  100%        1h ago    ok
stargazers         —         —      —           now       backfilling (surplus)
releases           1h        3h41m   71.4%      3h ago    BREACH: budget starved
```

---

## 13. Observability

- Structured logs (`tracing`), JSON or pretty. Per-request: stream, page, status, latency, remaining budget, deficit at admission.
- `--metrics-addr` exposes Prometheus:
  - `siphon_requests_total{stream,status}` — including a `not_modified` status, so the conditional-request hit rate is directly visible
  - `siphon_records_written_total{stream}`
  - `siphon_budget_remaining{source}` / `siphon_budget_spend_rate{source}`
  - `siphon_stream_deficit{stream}` — the scheduling signal itself; `> 1` is a live SLO breach
  - `siphon_slo_attainment_ratio{stream}`
  - `siphon_observed_change_interval_seconds{stream}` — the inferred churn rate; divergence from declared `freshness` is the advisory signal
  - `siphon_quarantined_pages{stream}` — should be zero; anything else is a known data gap
  - `siphon_backfill_requests_total{stream}` — how much surplus is being scavenged
- `_siphon_runs` is the queryable audit trail. Because it stores `deficit_at_admission` and `requests`, you can answer "why was this stream late" in SQL: starved of budget, or slow to run once admitted.

---

## 14. Guarantees and limits

**Guarantees**
- At-least-once delivery with idempotent upsert on `(stream, pk)`.
- Crash-safe: checkpoint and data commit atomically; resume is exact.
- A stream failure is isolated to that stream.
- No record is ever silently dropped; unparseable records go to dead-letter.

**Explicit limits**
- Single node. No coordination, no HA. Two instances against one SQLite file is unsupported — and two instances against one *API token* would each measure the budget independently and jointly overspend it, which is worse.
- **`freshness` is a target, not a guarantee.** If the budget can't cover the declared targets, siphon degrades by priority and reports the breach; it does not conjure requests. `siphon validate` warns when declared targets are obviously unsatisfiable at the configured ceiling — that check is the difference between a useful abstraction and a lie.
- Polling only. Best achievable freshness is bounded by round-trip time and budget.
- Hard deletes upstream are invisible unless the API exposes them. `mode: replace` for full-refresh streams is the workaround for small datasets.
- SQLite writer is single-threaded by design; concurrency is across streams, not within a write.

---

## 15. Milestones

The governor moves into v0.1. Freshness-based scheduling isn't a feature layered on top of a working poller — it *is* the scheduler, and building cron first means building the wrong component and then deleting it. It also means the first public release has the differentiator in it rather than looking like a worse Benthos.

**v0.1 — the spine and the governor (target: ~3 weeks)**
Config parse/validate; `offset` + `page` + `cursor` + `link_header` pagination; budget measurement from response headers with `Date`-derived skew correction and persisted learned ceiling; deficit-ordered admission with priority weighting and `best_effort` surplus scavenging; conditional requests; poison-page quarantine; bearer/api-key auth; SQLite raw layer with payload hashes; checkpointing; `run` / `sync` / `test` / `validate` / `status`. One worked example in the README that visibly beats its SLO when budget is free and degrades in priority order when it isn't — that demo is the pitch.

Quarantine and skew correction are in v0.1 rather than deferred because both are governor-specific failure modes, not general robustness polish. A scheduler that preferentially funds a stream stuck on a poison page, or that paces against a clock it shouldn't trust, doesn't fail gracefully — it fails in exactly the direction the whole design was meant to prevent.

**v0.2 — the shared cache**
Storage trait; Postgres target; typed projection with drift handling. These three are one piece of work and they come first, ahead of everything previously scheduled here, because they are what G7 needs and because every later feature writes through the storage layer. Adding coverage first means adding it into a store that cannot be shared, and then porting all of it.

The reorder has a cost worth stating: it puts a second store target in front of `siphon infer` and the change-rate advisory, which are the features that make the tool pleasant for a single user. That is the right trade only while the shared-cache case is real. If it stops being real, this ordering should go back.

**v0.2a — usability**
`siphon infer`; `status --advise` change-rate advisory; `next_url` strategy; `schema` / `reproject` / `budget` / `quarantine` commands; full retry classification; dead-letter table.

**v0.3 — breadth**
Parent-child streams (deficit inherits from parent), OAuth2 client credentials, Prometheus metrics, `rhai` escape hatch. The Postgres store target used to be listed here and moved up to v0.2; see open question 9.

**v0.4 — polish**
GraphQL support (`request.graphql` with query + variables, `pagination.strategy: relay_cursor` for `pageInfo.endCursor` / `hasNextPage`), full-refresh mode, Docker image, docs site.

Ship v0.1 before writing a line of v0.2. The temptation will be to build the config format for all four milestones up front; resist it beyond reserving key names.

That instruction no longer applies as written. Shipping v0.1 to anyone else is not a goal; the consumers are local and known. What survives from it is narrower and still true: the storage trait is being extracted against a codebase exactly one machine has ever run, so the SQLite implementation is the only evidence the trait is shaped right. Weight its conformance tests accordingly.

### Riskiest assumption to test first

Before building any of it: take two real APIs with different header conventions and confirm that `(remaining, reset_at)` can actually be read reliably enough to pace against. If the headers are absent, lie, or update lazily, the entire governor degrades to a static token bucket and the project has no differentiator. That's a one-day spike and it gates everything else.

---

## 16. Implementation notes

| Concern | Choice | Why |
|---|---|---|
| Async runtime | `tokio` | Non-negotiable, everything downstream assumes it |
| HTTP | `reqwest` + `rustls` | No OpenSSL, so static musl builds actually work |
| Config | `serde_yaml` + `serde` | Typed structs, real errors |
| SQLite | `rusqlite` (bundled) | Vendored C, one binary, no system libsqlite |
| JSONPath | `serde_json_path` | RFC 9535 compliant; avoid the older ad-hoc crates |
| Scheduling | `tokio-cron-scheduler` or hand-rolled intervals | Evaluate; hand-rolled may be less than the dependency costs |
| Retry/backoff | Hand-rolled | The logic is the product; don't hide it in a crate |
| Logging | `tracing` + `tracing-subscriber` | Standard |
| Scripting escape hatch | `rhai` | Pure Rust, sandboxed, no V8 |
| CLI | `clap` derive | Standard |

Build target: `x86_64-unknown-linux-musl` and `aarch64-apple-darwin`, static, no runtime deps. If the binary needs a `libsqlite3` on the host, the core promise is broken.

---

## 17. Rejected

Recorded so they don't get re-litigated every few months.

- **Change-event log / poll-to-webhook.** Hash records on upsert, write diffs to `_siphon_changes`, `siphon tail <stream>`. "Webhooks for APIs that don't have them" is a great pitch and roughly a day of work. Rejected because the first issue filed is "can it POST these to my endpoint," the second is Kafka, and at that point this is Benthos with a worse config format. If it ever ships, it ships as a table with no delivery mechanism, and the answer to delivery is permanently no.
- **A read API.** See NG1. The single fastest way to never finish.
- **Cron as the primary scheduling interface.** Retained only as an escape hatch. Making it primary would mean the governor can't do its job, since a fixed schedule declares a spend rather than a requirement.
- **Auto-tuned freshness.** The governor infers the observed change rate (§8.6) and could simply rewrite the targets itself. Rejected: it means the tool decides your data doesn't need to be fresh, and it will be wrong exactly when it matters — an endpoint that changes monthly for a year and then changes hourly during an incident. The advisory prints; the human edits. If this is ever revisited it should be `--advise --apply` writing a diff to the config file, never a runtime behaviour.
- **A connector catalog.** See NG4. `siphon infer` is the answer instead: generate the config once, own it in your repo, no upstream to depend on.

---

## 18. Open questions

1. **Do rate-limit headers hold up in practice?** The gating spike above. Everything rests on this.
2. **Is deficit the right ordering function?** `deficit × weight` is the obvious choice but it's untested. An alternative is earliest-deadline-first, which is better understood theoretically and might be simpler to reason about. Prototype both against a simulated budget before committing.
3. **How does the governor handle a stream whose full sync exceeds the entire window's budget?** A 200-page backfill against a 100-request/hour limit spans multiple windows mid-pagination. The checkpoint handles correctness, but the deficit accounting needs to not treat a legitimately long run as a breach.
4. **Should `utilization` be adaptive too?** A fixed 0.8 is a guess. If siphon is the only consumer of the token, 0.95 is fine; if it's sharing with a production app, 0.5 might not be enough. Possibly infer it from the gap between observed consumption and observed decrement.
5. **Does `stop_on_seen_key` generalize?** Cheapest incremental strategy for cursor-less APIs, but wrong if records are updated in place without moving in sort order. Likely opt-in with a loud warning.
6. **Typed projection: eager or lazy?** Eager is simpler; lazy ingests faster. Possibly `store.project: eager | lazy | off`.
7. **How much does the raw layer actually cost?** Benchmark on a 10M-record stream before committing to it as the default.
8. **One YAML file or a `config.d/` directory?** The latter scales past ~10 streams. Probably support both.
9. ~~**Postgres first-class or afterthought?**~~ **Decided: first-class, and it moved to the front of v0.2.** The forcing argument was not "people ask for it" - it was G7. A shared cache read by several projects on more than one host cannot be a file, and the alternative to a database wire protocol is siphon growing the read API that NG1 exists to prevent. The warning in the original question was right and nearly came true: v0.1 shipped with `Store` as a concrete struct over `rusqlite`, so the trait now has to be extracted rather than designed, with the SQLite implementation as the thing to check it against rather than the thing that defines it.
10. **What's the right behaviour when two streams share a source but one has a per-endpoint sub-limit?** Some APIs limit `/search` separately from everything else. The budget model currently assumes one pool per source.
11. **How long a window does change-rate inference need before its advice is trustworthy?** A stream polled hourly for two days has 48 samples, which is thin for estimating a 26-hour interval. The advisory needs a confidence gate and should stay silent rather than recommend from noise.
12. **Does churn inference survive bursty upstreams?** A resource that changes in daily bursts and is otherwise static has a misleading mean interval. Percentiles or a simple burst detector may be needed, or the advisory should decline to speak on high-variance streams.
13. **Should quarantined pages age out?** A 404 from a transiently-deleted parent will never recover on retry, but a 422 from a bad cursor might after an upstream fix. Automatic retry-after-N-days versus purely manual `--retry` is unresolved.
