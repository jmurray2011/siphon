# siphon

A single-binary, config-driven daemon that mirrors HTTP APIs into SQLite or
Postgres, scheduling its own polling against a continuously measured rate-limit
budget.

Most sync tools treat the rate limit as a ceiling to avoid: a static token
bucket, retry on 429, and a cron expression deciding what runs. siphon treats the
remaining budget as a resource to allocate. You declare how fresh each stream
needs to be and what matters most; siphon measures what the API will actually
give it and decides what to spend it on.

## Features

- **Freshness targets instead of cron.** Each stream declares `freshness: 15m`
  and a priority; when budget is tight, low-priority streams give way first.
- **Measured rate limits.** Reads `X-RateLimit-*`, `RateLimit-*` and similar
  headers automatically, learns the window, and survives restarts.
- **Cheap polling.** Conditional requests (`ETag`, `Last-Modified`) on every
  source.
- **Crash-safe incremental sync.** Records and checkpoints commit together; a
  crash resumes at the next page.
- **No silent gaps.** Pages that keep failing are quarantined and the stream
  reports `partial`, never `ok`.
- **Plain storage.** Raw JSON per record in SQLite or Postgres; query it with
  anything that speaks SQL.
- **One static binary.** No runtime, no C dependencies.

## Install

Requires Rust 1.85 or later.

```sh
git clone https://github.com/jmurray2011/siphon.git
cd siphon
cargo build --release          # binary at target/release/siphon
```

For a fully static Linux binary, build with
`--target x86_64-unknown-linux-musl`.

## Quick start

The Bitbucket example mirrors a public workspace and needs no credentials:

```sh
siphon validate examples/bitbucket.yaml                     # check the config, no network
siphon test repositories --config examples/bitbucket.yaml   # fetch one page, write nothing
siphon run examples/bitbucket.yaml                          # run the daemon
```

Then query the mirror:

```sh
sqlite3 data/bitbucket.db \
  "SELECT json_extract(payload, '$.full_name') FROM _siphon_records WHERE stream = 'repositories' LIMIT 5"
```

## Configuration

A minimal config:

```yaml
version: 1

store:
  type: sqlite
  path: ./data/mirror.db

sources:
  github:
    base_url: https://api.github.com
    auth:
      type: bearer
      token: ${GITHUB_TOKEN}

streams:
  - name: issues
    source: github
    freshness: 15m
    priority: high
    request:
      path: /repos/rust-lang/rust/issues
      params: { state: all, per_page: 100 }
    pagination:
      strategy: link_header
    records:
      path: $
      primary_key: $.id
      cursor:
        field: $.updated_at
        type: rfc3339
        param: since
```

Fully commented examples for GitHub, Bitbucket and Jira are in
[`examples/`](examples). The full reference is in
[docs/configuration.md](docs/configuration.md).

## Commands

| Command | What it does |
|---|---|
| `siphon run [config.yaml]` | Run the daemon; it schedules every stream. |
| `siphon sync <stream> [--full]` | Sync one stream now. `--full` ignores the stored cursor. |
| `siphon test <stream> [--pages N]` | Fetch and print what would be written. Writes nothing. |
| `siphon validate [config.yaml]` | Check the config. Makes no network calls. |
| `siphon status` | Per-stream freshness attainment, lag and budget. |

## Documentation

- [How it works](docs/how-it-works.md): scheduling, rate-limit handling,
  guarantees and limits
- [Configuration](docs/configuration.md): sources, streams, pagination,
  incremental sync
- [Storage](docs/storage.md): SQLite and Postgres, and the tables siphon writes
- [Running in production](docs/deployment.md): containers, credentials,
  monitoring
- [Design](design.md): the full design and what is deliberately out of scope

## Status

v0.1. Not yet supported: `append` and `replace` write modes, soft deletes,
parent/child streams, OAuth2, typed tables alongside the raw JSON, and schema
drift handling. These are reserved in the config format and rejected with a
clear error.

## Development

```sh
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
cargo deny check
```

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.
