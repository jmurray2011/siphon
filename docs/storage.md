# Storage

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
