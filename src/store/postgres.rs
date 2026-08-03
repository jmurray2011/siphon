//! The Postgres store target.
//!
//! This exists for the case SQLite cannot serve: one mirror read by several
//! projects, possibly from more than one host. A file is reachable from exactly
//! one filesystem and tolerates one writer; a server is neither. Nothing here
//! changes what siphon stores - the tables are the same tables - only where they
//! live and who can reach them.
//!
//! The schema is a translation of the SQLite DDL rather than a redesign, so the
//! two targets stay legible as the same thing:
//!
//! | SQLite | Postgres | Why |
//! |---|---|---|
//! | `INTEGER` holding unix millis | `BIGINT` | SQLite's `INTEGER` is up to 64-bit; `INT` here would overflow in 2038 |
//! | `BLOB` | `BYTEA` | The payload hash |
//! | `REAL` | `DOUBLE PRECISION` | `deficit_at_admission` |
//! | `INTEGER PRIMARY KEY` | `BIGSERIAL PRIMARY KEY` | SQLite aliases the rowid and assigns it; Postgres needs to be told |
//! | `STRICT` | - | Postgres has no permissive mode to opt out of |

use tokio_postgres::{Client, NoTls};
use tracing::error;

use super::StoreError;

/// The schema, applied on connect.
///
/// `IF NOT EXISTS` throughout, so connecting to a store that already exists is
/// the same statement as creating one.
pub const DDL: &str = r"
CREATE TABLE IF NOT EXISTS _siphon_records (
  stream      TEXT NOT NULL,
  pk          TEXT NOT NULL,
  payload     TEXT NOT NULL,
  hash        BYTEA NOT NULL,
  cursor_val  TEXT,
  fetched_at  BIGINT NOT NULL,
  PRIMARY KEY (stream, pk)
);

CREATE INDEX IF NOT EXISTS _siphon_records_cursor
  ON _siphon_records (stream, cursor_val);

CREATE TABLE IF NOT EXISTS _siphon_state (
  stream          TEXT PRIMARY KEY,
  cursor_val      TEXT,
  page_state      TEXT,
  last_run_at     BIGINT,
  last_success_at BIGINT,
  records_seen    BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS _siphon_budget (
  source          TEXT PRIMARY KEY,
  observed_limit  BIGINT,
  window_secs     BIGINT,
  remaining       BIGINT,
  reset_at        BIGINT,
  ratcheted_at    BIGINT,
  updated_at      BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS _siphon_validators (
  request_key   TEXT PRIMARY KEY,
  etag          TEXT,
  last_modified TEXT,
  seen_at       BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS _siphon_runs (
  id                   BIGSERIAL PRIMARY KEY,
  stream               TEXT NOT NULL,
  started_at           BIGINT NOT NULL,
  ended_at             BIGINT,
  status               TEXT,
  pages                BIGINT,
  records              BIGINT,
  records_new          BIGINT,
  records_changed      BIGINT,
  not_modified         BIGINT,
  requests             BIGINT,
  deficit_at_admission DOUBLE PRECISION,
  stop_reason          TEXT,
  error                TEXT
);

CREATE INDEX IF NOT EXISTS _siphon_runs_stream_time
  ON _siphon_runs (stream, ended_at);

CREATE TABLE IF NOT EXISTS _siphon_quarantine (
  stream       TEXT NOT NULL,
  page_state   TEXT NOT NULL,
  attempts     BIGINT NOT NULL,
  status_code  BIGINT,
  body_excerpt TEXT,
  first_seen   BIGINT NOT NULL,
  last_seen    BIGINT NOT NULL,
  PRIMARY KEY (stream, page_state)
);
";

/// A mirror held in Postgres.
pub struct PgStore {
    client: Client,
}

impl std::fmt::Debug for PgStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The connection string can carry a password, so it is not held and not
        // printed. There is nothing else here worth showing.
        f.write_str("PgStore")
    }
}

impl PgStore {
    /// Connects, applies the schema, and spawns the connection task.
    ///
    /// The connection speaks **plaintext**. On loopback or a trusted socket that
    /// is fine; across a network it is not, and this is the thing to fix before
    /// the mirror is reachable from another host. Adding TLS means a
    /// `tokio-postgres-rustls` dependency and a decision about certificate
    /// verification, neither of which should be guessed at.
    ///
    /// # Errors
    ///
    /// Fails if the server is unreachable, rejects the credentials, or rejects
    /// the schema.
    pub async fn connect(conn_str: &str) -> Result<Self, StoreError> {
        let (client, connection) = tokio_postgres::connect(conn_str, NoTls).await?;

        // `connection` drives the socket and must be polled for the client to
        // work at all. It resolves when the client is dropped, so this task ends
        // with the store rather than leaking.
        tokio::spawn(async move {
            if let Err(cause) = connection.await {
                error!(%cause, "postgres connection closed");
            }
        });

        client.batch_execute(DDL).await?;
        Ok(Self { client })
    }

    /// How many records are stored for a stream.
    ///
    /// # Errors
    ///
    /// Fails if the query does.
    pub async fn record_count(&self, stream: &str) -> Result<u64, StoreError> {
        let row = self
            .client
            .query_one(
                "SELECT COUNT(*) FROM _siphon_records WHERE stream = $1",
                &[&stream],
            )
            .await?;
        let count: i64 = row.get(0);
        Ok(count.try_into().unwrap_or(0))
    }
}
