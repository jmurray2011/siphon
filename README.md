# siphon

A single-binary, config-driven daemon that mirrors HTTP APIs into SQLite,
scheduling its own polling against a continuously measured rate-limit budget.

Every tool in this space treats the rate limit as a ceiling to avoid: a static
token bucket plus retry-on-429, with a cron expression deciding what runs.
siphon treats the remaining budget as a **resource to allocate**. You declare how
fresh each stream needs to be and what matters most; siphon measures what the API
will actually give it and decides what to spend it on.

**Status:** v0.1. See [design.md](design.md) for the full design, including what
is deliberately out of scope.

---

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
[`tests/scheduling.rs`](tests/scheduling.rs) - a deterministic simulation of one
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

## Does the premise hold?

The whole design rests on `(remaining, reset_at)` being readable reliably enough
to pace against. If it is not, the governor degrades to a static token bucket and
there is no reason for siphon to exist. Surveyed against live unauthenticated
responses on 2026-08-01, captured as fixtures in
[`tests/real_world_headers.rs`](tests/real_world_headers.rs):

| API | Fields | Reset | Window | Verdict |
|---|---|---|---|---|
| GitHub | `X-RateLimit-*` | unix seconds | 3600s | Usable; decrements exactly once per request |
| GitLab | `RateLimit-*` (IETF) | unix seconds | **60s**, aligned to the minute | Usable from the same parse, no configuration |
| Bitbucket | `X-RateLimit-*` | **delta** seconds | 3600s, aligned to the hour | Readable, but the counter is **not monotonic** |
| crates.io | none | - | - | No signal at all |
| PyPI | none (but sends `ETag`) | - | - | No signal at all |

Three things fell out of this that were not obvious from the design:

**Window length varies by two orders of magnitude.** GitHub's window is an hour;
GitLab's is a minute. Both are read correctly from the same auto-detected fields,
but they produce very different pacing - 0.013 req/s against GitHub versus about
17 req/s against GitLab - and the window estimator has to converge on each
without being told which it is looking at.

**A configured ceiling is not a fallback, it is a primary path.** Two of the five
APIs surveyed publish nothing at all. `requests_per_hour` is the only signal
there, and the politeness floor is what protects an API that has neither.
Conditional requests are independent of this: PyPI publishes no budget but does
publish an `ETag`, so polling it is still cheap. Independent in the other
direction too, and this one is easy to get wrong - a 304 is not automatically
free. GitHub does not charge for one, which is what makes a tight freshness
target affordable there. Bitbucket decrements the same counter it would for a
200, so conditional requests buy bandwidth and not budget, and a target has to
be affordable on request count alone.

**A published counter is not necessarily a counter.** Bitbucket answers from
several nodes whose rate-limit counters disagree. Eight requests inside two
seconds read 55, 57, 59, 54, 59, 59, 58, 58, and a single node reported 57, then
54, then 58 - so the newest value is not the truest one, and taking it verbatim
lets the stalest node hand back an allowance that was already spent. Within a
window siphon now only ever lowers what it believes is left, and restores it
only once that window has actually elapsed. The categories are therefore not
"publishes" and "does not publish" but three: publishes something you can pace
against, publishes nothing, and publishes something that will mislead you if you
believe it. The last is the one worth designing for, because it looks like the
first.

Worth noting for later: GitHub's `X-RateLimit-Resource: core` names the bucket
the limit belongs to, and `/search` is a separate bucket on the same token. The
budget model here assumes one pool per source, so a config that mixes them would
measure one and spend both.

---

## Quick start

```sh
export GITHUB_TOKEN=ghp_...

siphon validate examples/github.yaml     # parse and check; no network calls
siphon test issues --config examples/github.yaml   # fetch one page, print it, write nothing
siphon run examples/github.yaml          # daemon
```

Then query the result with anything that reads SQLite:

```sh
sqlite3 data/mirror.db \
  "SELECT json_extract(payload, '$.title') FROM _siphon_records WHERE stream='issues' LIMIT 5"
```

siphon deliberately does not serve a read API. The output is a SQLite file or a
Postgres schema; point Datasette, PostgREST, `psql`, `sqlite3`, or your own app
at it. That is also how several projects share one mirror - a database wire
protocol every language already speaks, rather than an HTTP surface siphon would
have to design, version and keep up.

## Commands

| Command | What it does |
|---|---|
| `siphon run [config.yaml]` | Daemon. The governor schedules every stream. |
| `siphon sync <stream> [--full]` | One-shot. Bypasses admission, not the budget. `--full` ignores the stored cursor. |
| `siphon test <stream> [--pages N]` | Fetch and print what would be written. Writes nothing. |
| `siphon validate [config.yaml]` | Parse and check. Makes no network calls. |
| `siphon status` | Per-stream SLO attainment, lag and budget. |

`siphon status` reports attainment over seven days, not just whether the last run
succeeded - the number an operator actually wants, and one that is only
computable because freshness is declared:

```
STREAM             FRESHNESS       LAG  ATTAIN(7d)   LAST OK  STATE
issues                   15m     4m12s       99.2%  4m12s ago  ok
issue_comments            6h     1h03m      100.0%  1h03m ago  ok
stargazers                 -         -           -    12s ago  backfilling (surplus)
releases                   1h     3h41m       71.4%  3h41m ago  BREACH: 2h41m past target

SOURCE                REMAINING     WINDOW      SPEND/S     SKEW  STATE
github              4211/5000      41m12s         1.36       2s  ok
```

## Configuration

The config format is the product; everything else is implementation. Three fully
commented files, each covering a different shape:

| File | Pagination | Incremental mark | Credentials |
|---|---|---|---|
| [`github.yaml`](examples/github.yaml) | `link_header` | a `since` parameter | token |
| [`bitbucket.yaml`](examples/bitbucket.yaml) | `page` | in a query expression, no narrowing | none |
| [`jira.yaml`](examples/jira.yaml) | `cursor` | in a query expression, narrowed by `format` | email + token |

Start with `bitbucket.yaml`: it runs as written with no credentials, and it
covers the rate-limit counter that cannot be read verbatim.

### Sources

```yaml
sources:
  github:
    base_url: https://api.github.com
    auth:
      type: bearer            # none | bearer | basic | api_key
      token: ${GITHUB_TOKEN}  # ${ENV}, file:/path, or an inline literal (linted)
    headers:
      Accept: application/vnd.github+json
    budget:
      requests_per_hour: 5000 # fallback ceiling, used until the API says otherwise
      observe:                # or leave it out: X-RateLimit-*, RateLimit-* and
        remaining_header: X-RateLimit-Remaining   # X-Rate-Limit-* are detected
        limit_header: X-RateLimit-Limit
        reset_header: X-RateLimit-Reset
        reset_format: unix_seconds  # unix_seconds | unix_millis | delta_seconds | http_date
      utilization: 0.8        # fraction of the measured budget siphon will spend
    conditional_requests: true
    timeout: 30s
    retry:
      max_attempts: 5
      initial_backoff: 1s
      max_backoff: 5m
```

### Streams

```yaml
streams:
  - name: issues
    source: github
    freshness: 15m            # required, unless priority is best_effort
    priority: high            # high | normal | low | best_effort
    direction: forward        # forward (incremental) | backward (historical walk)
    quarantine_after: 3       # consecutive fatal failures before a page is skipped
    request:
      path: /repos/{{ owner }}/{{ repo }}/issues
      params: { state: all, per_page: 100 }
      headers: {}
    vars:
      owner: rust-lang
      repo: rust
    pagination:
      strategy: link_header
      rel: next
    records:
      path: $                 # JSONPath to the record array
      primary_key: $.id
      cursor:
        field: $.updated_at
        type: rfc3339         # rfc3339 | unix_seconds | unix_millis | integer | string
        param: since          # injected into the next sync's query
    write:
      mode: upsert
```

### Incremental marks on APIs with a query language

Some APIs do not take a `since` parameter; the mark is one clause of an
expression - JQL, SOQL, an OData `$filter`. Interpolate `{{ cursor }}` into the
parameter instead of declaring `cursor.param`:

```yaml
    request:
      path: /rest/api/3/search/jql
      params:
        jql: 'project = {{ project }} AND updated >= "{{ cursor }}" ORDER BY updated ASC'
    records:
      cursor:
        field: $.fields.updated
        type: rfc3339
        format: "%Y-%m-%d %H:%M"    # how to write the mark back
        initial: "1970-01-01 00:00" # the mark before anything is stored
```

`format` exists because APIs routinely accept a narrower timestamp than they
emit, and the failure when they do is silent. Jira returns
`2021-05-04T22:10:32.221+0100`; feeding that straight back into JQL is
**accepted and matches nothing**, so a stream would report `ok` forever while
mirroring no new data. The mark is stored exactly as it arrived and narrowed
only on the way out, in the offset it arrived in - re-rendering in UTC would
silently re-fetch an extra hour of history every run.

`initial` is required whenever `{{ cursor }}` is used: the first run has no
stored mark, and there is nothing sensible to substitute. Declaring both
`{{ cursor }}` and `cursor.param` is refused rather than guessed at.

The `rfc3339` type also accepts the basic-format offset that Java-based APIs
emit (`+0100` rather than `+01:00`). That is not strictly RFC 3339, but
rejecting it means the mark never advances and the mirror re-walks its whole
history forever while reporting success - a much worse failure than a liberal
parser.

### One block, many streams

Mirroring twenty projects from one API means twenty streams differing in a single
value. Writing them out is how a config stops being reviewable, and generating
them from a script means the config nobody can read is the only one that exists.
`for_each` expands one block into one stream per value:

```yaml
streams:
  - name: issues_{{ project }}     # must vary, or the expansions collide
    for_each:
      project: [ABC, DEF, GHI]
    request:
      path: /rest/api/3/search/jql
      params:
        jql: 'project = "{{ project }}" AND updated >= "{{ cursor }}"'
    # ...the rest of the block, written once
```

Each expansion gets the value added to its `vars`, so it reaches the path, the
params and the name. Expansion happens before validation, so every product is
checked exactly as if it had been typed out. The values come from the config,
which is what separates this from the `parent:` key reserved for v0.3 - that one
expands over records the API returns.

`for_each` takes one variable. Two tiers of freshness means two blocks, which is
also the honest way to write it.

### A private base URL

`base_url` accepts `${ENV_VAR}`, so a per-tenant or self-hosted instance URL does
not have to be written into a file that lives in a repo:

```yaml
sources:
  jira:
    base_url: ${JIRA_BASE_URL}
```

Unlike a credential this resolves at load rather than at request time, because
the URL has to parse for the config to validate at all. The cost is that
`siphon validate` needs the variable set - run it under whatever supplies the
rest of the environment. An unset variable is an error naming the variable, never
a silent fallback.

### Pagination strategies

| Strategy | Required | Termination |
|---|---|---|
| `offset` | `page_size`; optional `limit_param`, `offset_param` | short page, or empty page |
| `page` | none; optional `page_param`, `size_param`, `page_size`, `start_page`, `total_pages_path` | short page when `page_size` is set, empty page, or the reported total |
| `cursor` | `cursor_param`, `cursor_path` | cursor absent, null or empty |
| `link_header` | none; optional `rel` (default `next`) | header absent |
| `next_url` | `url_path`; optional `end_path` | URL absent, null or empty; `end_path` true; or the same URL handed back |
| `none` | none | one request |

Cross-cutting on every strategy: `max_pages` (default 10,000), `stop_on_empty`
(default true), `stop_on_seen_key` (default false, and linted - it is wrong for
APIs that update records in place without moving them in sort order).

A next URL from a `Link` header or a `next_url` body field is followed only on
the source's own origin (scheme, host and port of `base_url`). A next URL that
points anywhere else ends the run with an error rather than send the source's
credentials there. `end_path` is for time-based exports that hand out a next
URL on their last page too, so a later run knows where to resume: point it at
the body's end-of-stream flag.

A parameter the strategy sets for itself - or one that `cursor.param` owns - wins
over a `request.params` entry of the same name, and `validate` lints the
duplicate. Sending both would leave the API to choose, and for `offset` the
choice is silently destructive: the API answers with one page size while the
paginator advances by another, so every page skips or repeats records.

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
  design.md sections 3 and 17.

## Storage

Everything siphon owns is prefixed `_siphon_`; the rest of the database is yours.

Two targets, chosen by `store.type`. They hold the same tables; what differs is
who can reach them.

```yaml
store:
  type: sqlite            # the default
  path: ./data/mirror.db
```

```yaml
store:
  type: postgres
  url: ${DATABASE_URL}    # libpq connection string
```

SQLite is right when one project owns the mirror and reads it from the same box:
no server, no ceremony. Postgres is for a mirror several projects read, or a
daemon that does not run on the same host as its readers - a file is reachable
from exactly one filesystem and tolerates exactly one writer.

The two keys are not interchangeable: a `path` under `type: postgres` is refused
rather than ignored, because the likely cause is an author who changed `type`
and left the rest behind, and silently honouring one key would put the mirror
somewhere nobody is looking.

`url` is read as a secret reference, so `${ENV_VAR}` and `file:/path` both work
and a connection string written into the config earns a `validate` finding - it
carries a password. TLS is verified against the host's trust store, so a private
CA works the way everything else on the machine trusts it. Set `sslmode=require`
or stronger in the connection string when the server is not on loopback:
libpq's default is `prefer`, which silently falls back to plaintext, and siphon
warns when it sees anything weaker.

`siphon test` always uses a throwaway in-memory SQLite store regardless of
`store.type`. It writes nothing, so a probe should not need the server to be
reachable.

| Table | What is in it |
|---|---|
| `_siphon_records` | The raw layer: verbatim JSON per record, plus a payload hash |
| `_siphon_state` | One checkpoint per stream: cursor, resume point, last success |
| `_siphon_budget` | The learned rate limit per source |
| `_siphon_validators` | `ETag` / `Last-Modified` per request URL |
| `_siphon_runs` | The audit trail, including deficit at admission |
| `_siphon_quarantine` | Pages that failed fatally and were skipped |

Because `_siphon_runs` records `deficit_at_admission` and `requests`, "why was
this stream late" is answerable in SQL - starved of budget, or slow to run once
admitted:

```sql
SELECT stream, deficit_at_admission, requests, ended_at - started_at AS ms
FROM _siphon_runs ORDER BY id DESC LIMIT 20;
```

## Not in v0.1

Reserved in the config format, rejected with a clear error if used:
`write.mode: append | replace`, `write.soft_delete`,
parent/child streams, OAuth2, and the `schedule:` cron escape hatch. The typed
projection (a real `issues` table alongside the raw layer), schema-drift
handling, `siphon infer`, `siphon status --advise` and the dead-letter table are
v0.2 and v0.3. Query the raw layer with `json_extract` until then.

## Running it for real

`siphon run` is the whole scheduler - it does not exit, and there is nothing to
put in cron. One process drives one config, so a host mirroring three APIs runs
three of them; that is deliberate, because the budget and the governor are
per-source and a shared process would let one API's backlog starve another's.

The binary is static, so the image is the binary plus a CA bundle:

```dockerfile
FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata
COPY siphon /usr/local/bin/siphon
COPY configs/ /etc/siphon/
ENTRYPOINT ["/usr/local/bin/siphon"]
```

```sh
cargo build --release --target x86_64-unknown-linux-musl
docker run -d --name siphon-jira --restart unless-stopped \
  --network siphon-net --env-file ./env.jira \
  siphon:local run /etc/siphon/jira.yaml
```

Put the daemon and its Postgres on the same private network and point
`store.url` at the container name. The hop then never leaves the host, which is
what makes `sslmode=disable` defensible there and nowhere else - anything
reaching the store across a network wants a real certificate.

Credentials come from the environment, so they are whatever your host already
uses for secrets: an `--env-file`, a Docker secret, a systemd `EnvironmentFile`.
siphon resolves every one of them at startup, before the first request, so a
missing token is a container that fails immediately rather than a 401 discovered
three pages into a walk.

The mirror is worth putting on a volume and the daemons are worth
`--restart unless-stopped`. Nothing else is stateful: kill a daemon mid-walk and
the checkpoint it committed with its last page is where the next one resumes.

## Development

```sh
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
cargo deny check
```

The core is deterministic and I/O-free - `clock`, `config`, `pagination`,
`budget`, `governor`, `extract` - with `http` and `store` as the only adapters.
Time is reached only through `clock::Clock`, so scheduling behaviour is tested by
simulation rather than by sleeping.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.
