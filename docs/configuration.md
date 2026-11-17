# Configuration

The config format is the product; everything else is implementation. Three fully
commented files, each covering a different shape:

| File | Pagination | Incremental mark | Credentials |
|---|---|---|---|
| [`github.yaml`](../examples/github.yaml) | `link_header` | a `since` parameter | token |
| [`bitbucket.yaml`](../examples/bitbucket.yaml) | `page` | in a query expression, no narrowing | none |
| [`jira.yaml`](../examples/jira.yaml) | `cursor` | in a query expression, narrowed by `format` | email + token |

Start with `bitbucket.yaml`: it runs as written with no credentials, and it
covers the rate-limit counter that cannot be read verbatim.

## Sources

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

## Streams

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

## Incremental marks on APIs with a query language

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

## One block, many streams

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

## A private base URL

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

## Pagination strategies

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
