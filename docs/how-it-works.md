# How siphon works

## Why there is no `schedule:` key

Cron states a mechanism. `freshness` states the requirement the mechanism was
trying to satisfy:

```yaml
- name: issues
  freshness: 15m       # never staler than this
  priority: high       # and served first when the budget is tight
```

Given the requirement rather than the mechanism, the scheduler can do better than
cron when budget is plentiful, and degrade sensibly when it is not. Cron can do
neither.

Here is that difference, from
[`tests/scheduling.rs`](../tests/scheduling.rs) - a deterministic simulation of one
hour with two streams on a 300s target (one `high`, one `low`) and a
`best_effort` backfill. Run it yourself with
`cargo test --test scheduling -- --nocapture`.

**Budget plentiful (600 requests/hour):**

```
STREAM          TARGET   REQUESTS  WORST LAG
issues            300s         23       155s
comments          300s         23       156s
backfill             -        554          -
```

Both streams beat their target by roughly 2x for free, and the backfill
scavenges the 554 requests nobody else needed. Nothing is left on the table.

**Budget scarce (18 requests/hour, where 24 are needed):**

```
STREAM          TARGET   REQUESTS  WORST LAG
issues            300s         13       399s
comments          300s          4       800s
backfill             -          1          -
```

The budget cannot cover both targets, so something has to give - and what gives
is the low-priority stream, not a uniform slide across everything. The backfill
yields entirely. A cron-driven pipeline spends the same budget uniformly and
falls behind on the data that mattered while faithfully refreshing the data that
did not.

Priority biases the ordering; it does not create castes. Deficit grows without
bound, so a `low` stream that is badly late eventually outranks a `high` stream
that is barely late - it still got served four times above.

## Rate-limit signals by API

siphon paces against `(remaining, reset_at)` when an API publishes them, and
falls back to the configured ceiling when it does not. Measured against live
unauthenticated responses on 2026-08-01; the captures are fixtures in
[`tests/real_world_headers.rs`](../tests/real_world_headers.rs):

| API | Fields | Reset | Window | Verdict |
|---|---|---|---|---|
| GitHub | `X-RateLimit-*` | unix seconds | 3600s | Usable; decrements exactly once per request |
| GitLab | `RateLimit-*` (IETF) | unix seconds | **60s**, aligned to the minute | Usable from the same parse, no configuration |
| Bitbucket | `X-RateLimit-*` | **delta** seconds | 3600s, aligned to the hour | Readable, but the counter is **not monotonic** |
| crates.io | none | - | - | No signal at all |
| PyPI | none (but sends `ETag`) | - | - | No signal at all |

How siphon handles each case:

- **Window length is read, not configured.** GitHub's window is an hour and
  GitLab's a minute; both come from the same parse. Pacing ranges from about
  0.013 req/s (GitHub) to 17 req/s (GitLab) with no per-API setting.
- **No signal: the configured ceiling is the budget.** For APIs that publish
  nothing, `requests_per_hour` is the limit, and the politeness floor applies
  when that is unset too.
- **A 304 is not always free.** GitHub does not charge for one; Bitbucket
  charges the same as a 200. Conditional requests always save bandwidth, but on
  Bitbucket a freshness target has to be affordable on request count alone.
- **Disagreeing counters are clamped.** Bitbucket answers from several nodes
  whose counters disagree (55, 57, 59, 54 within two seconds). Within a window
  siphon only ever lowers its estimate of what is left, and restores it only
  when the window has elapsed.

**Limitation:** the budget model is one pool per source. GitHub's `/search` is
a separate bucket on the same token (`X-RateLimit-Resource`), so a source that
mixes `/search` with other endpoints measures one bucket and spends from both.

## No read API

siphon deliberately does not serve a read API. The output is a SQLite file or a
Postgres schema; point Datasette, PostgREST, `psql`, `sqlite3`, or your own app
at it. That is also how several projects share one mirror - a database wire
protocol every language already speaks, rather than an HTTP surface siphon would
have to design, version and keep up.

## What it guarantees

- **At-least-once with idempotent upsert** on `(stream, pk)`, which for a mirror
  is equivalent to exactly-once.
- **Crash-safe.** A page's records and the checkpoint past them commit in one
  transaction. A crash on page 47 of 200 resumes at page 48, exactly.
- **Failure isolation.** A fatal error on one stream never stops another.
- **No silent gaps.** A page that fails fatally three times in a row is recorded
  in `_siphon_quarantine` and skipped, and the stream reports `partial` from then
  on - never `ok`.
- **The learned budget survives restarts.** A daemon that rediscovers its rate
  limit by getting 429'd after every deploy punishes you for deploying.
- **The local clock is untrusted.** Window length is derived from the server's
  own `Date` header, so a badly-set container clock does not make siphon either
  sprint into the wall or idle at a third of the rate it could use.

## What it does not

- **`freshness` is a target, not a guarantee.** If the budget cannot cover the
  declared targets, siphon degrades by priority and reports the breach; it does
  not conjure requests. `siphon validate` warns when the declared targets are
  obviously unsatisfiable at the configured ceiling.
- **Single node.** No coordination, no HA. Two instances against one API token
  would each measure the budget independently and jointly overspend it.
- **Polling only.** Best achievable freshness is bounded by round-trip time and
  budget.
- **Hard deletes upstream are invisible** unless the API exposes them.
- **No read API, no transformations, no connector catalog.** Deliberate. See
  [design.md](../design.md) sections 3 and 17.
