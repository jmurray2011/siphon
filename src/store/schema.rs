//! The on-disk schema.
//!
//! Everything siphon owns is prefixed `_siphon_`; the rest of the database
//! belongs to the user. There is no read API (NG1) - the output is a SQLite
//! file, and Datasette or your own app points at it.

/// Bumped whenever the DDL below changes in a way that needs a migration.
pub const SCHEMA_VERSION: i32 = 1;

/// The full schema, applied inside one transaction on open.
pub const DDL: &str = r"
-- Raw landing zone. Written verbatim, never lossy: a schema-inference bug is
-- then a reprojection away from being fixed rather than being data loss.
-- `fetched_at` is when siphon first stored *this version* of the record; a poll
-- that re-sees an unchanged record does not rewrite the row.
CREATE TABLE IF NOT EXISTS _siphon_records (
  stream      TEXT NOT NULL,
  pk          TEXT NOT NULL,
  payload     TEXT NOT NULL,
  hash        BLOB NOT NULL,
  cursor_val  TEXT,
  fetched_at  INTEGER NOT NULL,
  PRIMARY KEY (stream, pk)
) STRICT;

CREATE INDEX IF NOT EXISTS _siphon_records_cursor
  ON _siphon_records (stream, cursor_val);

-- One checkpoint per stream. `page_state` is the serialized paginator state and
-- is NULL when no walk is in flight, so a crash on page 47 resumes at page 48.
CREATE TABLE IF NOT EXISTS _siphon_state (
  stream          TEXT PRIMARY KEY,
  cursor_val      TEXT,
  page_state      TEXT,
  last_run_at     INTEGER,
  last_success_at INTEGER,
  records_seen    INTEGER NOT NULL DEFAULT 0
) STRICT;

-- The learned budget. A process that re-learns its limit by getting 429'd on
-- every restart is a process that punishes you for deploying.
CREATE TABLE IF NOT EXISTS _siphon_budget (
  source          TEXT PRIMARY KEY,
  observed_limit  INTEGER,
  window_secs     INTEGER,
  remaining       INTEGER,
  reset_at        INTEGER,
  ratcheted_at    INTEGER,
  updated_at      INTEGER NOT NULL
) STRICT;

-- Conditional-request validators, keyed by canonical request URL.
CREATE TABLE IF NOT EXISTS _siphon_validators (
  request_key   TEXT PRIMARY KEY,
  etag          TEXT,
  last_modified TEXT,
  seen_at       INTEGER NOT NULL
) STRICT;

-- The audit trail. Because it stores `deficit_at_admission` and `requests`,
-- 'why was this stream late' is answerable in SQL: starved of budget, or slow
-- to run once admitted.
CREATE TABLE IF NOT EXISTS _siphon_runs (
  id                   INTEGER PRIMARY KEY,
  stream               TEXT NOT NULL,
  started_at           INTEGER NOT NULL,
  ended_at             INTEGER,
  status               TEXT,
  pages                INTEGER,
  records              INTEGER,
  records_new          INTEGER,
  records_changed      INTEGER,
  not_modified         INTEGER,
  requests             INTEGER,
  deficit_at_admission REAL,
  stop_reason          TEXT,
  error                TEXT
) STRICT;

CREATE INDEX IF NOT EXISTS _siphon_runs_stream_time
  ON _siphon_runs (stream, ended_at);

-- Pages that failed fatally and were skipped, so the gap is visible rather than
-- silently tolerated. A stream with any of these never reports `ok`.
CREATE TABLE IF NOT EXISTS _siphon_quarantine (
  stream       TEXT NOT NULL,
  page_state   TEXT NOT NULL,
  attempts     INTEGER NOT NULL,
  status_code  INTEGER,
  body_excerpt TEXT,
  first_seen   INTEGER NOT NULL,
  last_seen    INTEGER NOT NULL,
  PRIMARY KEY (stream, page_state)
) STRICT;
";
