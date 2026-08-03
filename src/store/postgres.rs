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

use std::future::Future;
use std::sync::Arc;

use tokio_postgres::Client;
use tokio_postgres_rustls::MakeRustlsConnect;
use tracing::{error, warn};

use super::{Checkpoint, RunSummary, StoreError};
use crate::cli::status::ReportStore;
use crate::clock::Timestamp;

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

/// Advisory-lock key that serializes schema creation across processes.
///
/// Arbitrary but fixed: any two siphon processes must pick the same number for
/// the lock to mean anything, and it only has to avoid colliding with whatever
/// else uses advisory locks in the same database.
const SCHEMA_LOCK_KEY: i64 = 0x5169_686f_6e00_0001;

/// Builds the TLS connector from the host's trust store.
///
/// Pinned to the `ring` provider explicitly rather than relying on a process
/// default: rustls has no default provider unless one is installed, and leaving
/// it to whoever calls `install_default` first is a panic waiting for the first
/// deployment that links things in a different order.
fn tls_connector() -> Result<MakeRustlsConnect, StoreError> {
    let mut roots = rustls::RootCertStore::empty();
    let loaded = rustls_native_certs::load_native_certs();

    // Partial success is normal: a store can hold a certificate this build
    // cannot parse without that making the rest unusable. An empty result is
    // not - it means nothing would ever verify.
    for cause in &loaded.errors {
        warn!(%cause, "skipping an unreadable root certificate");
    }
    let (added, _ignored) = roots.add_parsable_certificates(loaded.certs);
    if added == 0 {
        return Err(StoreError::NoRootCertificates);
    }

    let config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|_| StoreError::NoRootCertificates)?
    .with_root_certificates(roots)
    .with_no_client_auth();

    Ok(MakeRustlsConnect::new(config))
}

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
    /// Connects over TLS, applies the schema, and spawns the connection task.
    ///
    /// Certificates are verified against the host's own trust store, so a
    /// private CA works exactly as it does for everything else on the machine
    /// and nothing here has an option to skip verification.
    ///
    /// Whether TLS is *required* is `sslmode` in the connection string, which
    /// libpq defines and tokio-postgres parses. The default is `prefer`, which
    /// silently falls back to plaintext when the server declines - fine on
    /// loopback, wrong across a network, and impossible to notice after the
    /// fact. Anything weaker than `require` is logged as a warning rather than
    /// quietly accepted.
    ///
    /// # Errors
    ///
    /// Fails if the trust store cannot be read, the server is unreachable,
    /// rejects the credentials, or rejects the schema.
    pub async fn connect(conn_str: &str) -> Result<Self, StoreError> {
        if !conn_str.contains("sslmode=require")
            && !conn_str.contains("sslmode=verify-ca")
            && !conn_str.contains("sslmode=verify-full")
        {
            warn!(
                "postgres sslmode is weaker than `require`; the connection may \
                 silently fall back to plaintext"
            );
        }

        let (client, connection) = tokio_postgres::connect(conn_str, tls_connector()?).await?;

        // `connection` drives the socket and must be polled for the client to
        // work at all. It resolves when the client is dropped, so this task ends
        // with the store rather than leaking.
        tokio::spawn(async move {
            if let Err(cause) = connection.await {
                error!(%cause, "postgres connection closed");
            }
        });

        // `CREATE TABLE IF NOT EXISTS` is not safe to run concurrently. The
        // existence check and the create are not atomic, so two processes
        // starting together can both pass the check and the loser fails on
        // `pg_type_typname_nsp_index`. SQLite never showed this because one
        // embedded connection has nobody to race; a shared server is the whole
        // point here, so serialize on an advisory lock and let the second
        // process wait and find the tables already there.
        //
        // The unlock is not conditional on success: holding a session lock after
        // a failed migration would wedge every later start against a server that
        // is merely unmigrated.
        client
            .query("SELECT pg_advisory_lock($1)", &[&SCHEMA_LOCK_KEY])
            .await?;
        let migrated = client.batch_execute(DDL).await;
        client
            .query("SELECT pg_advisory_unlock($1)", &[&SCHEMA_LOCK_KEY])
            .await?;
        migrated?;

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

impl PgStore {
    /// The stream's checkpoint, defaulted when it has never run.
    async fn read_checkpoint(&self, stream: &str) -> Result<Checkpoint, StoreError> {
        let found = self
            .client
            .query_opt(
                "SELECT cursor_val, page_state, last_run_at, last_success_at, records_seen
                 FROM _siphon_state WHERE stream = $1",
                &[&stream],
            )
            .await?;

        // A stream that has never run has no row, and that is not an error: the
        // default checkpoint is what tells the first walk to start at the top.
        let Some(row) = found else {
            return Ok(Checkpoint::default());
        };

        let records_seen: i64 = row.get(4);
        Ok(Checkpoint {
            cursor_val: row.get(0),
            page_state: row.get(1),
            last_run_at: row.get::<_, Option<i64>>(2).map(Timestamp::from_millis),
            last_success_at: row.get::<_, Option<i64>>(3).map(Timestamp::from_millis),
            records_seen: records_seen.max(0).unsigned_abs(),
        })
    }

    /// When the stream first ran.
    async fn read_first_run_at(&self, stream: &str) -> Result<Option<Timestamp>, StoreError> {
        let row = self
            .client
            .query_one(
                "SELECT MIN(started_at) FROM _siphon_runs WHERE stream = $1",
                &[&stream],
            )
            .await?;
        Ok(row.get::<_, Option<i64>>(0).map(Timestamp::from_millis))
    }

    /// Timestamps of runs that left the stream current, at or after `since`.
    async fn read_successes_since(
        &self,
        stream: &str,
        since: Timestamp,
    ) -> Result<Vec<Timestamp>, StoreError> {
        // `not_modified` counts as a success: a 304 confirms the mirror is
        // current, which is exactly what attainment measures.
        let rows = self
            .client
            .query(
                "SELECT ended_at FROM _siphon_runs
                 WHERE stream = $1 AND ended_at >= $2
                   AND status IN ('ok', 'partial', 'not_modified')
                 ORDER BY ended_at",
                &[&stream, &since.as_millis()],
            )
            .await?;
        Ok(rows
            .iter()
            .map(|row| Timestamp::from_millis(row.get::<_, i64>(0)))
            .collect())
    }

    /// How many pages of this stream are set aside.
    async fn read_quarantined_count(&self, stream: &str) -> Result<u32, StoreError> {
        let row = self
            .client
            .query_one(
                "SELECT COUNT(*) FROM _siphon_quarantine WHERE stream = $1",
                &[&stream],
            )
            .await?;
        let count: i64 = row.get(0);
        Ok(u32::try_from(count.max(0)).unwrap_or(u32::MAX))
    }

    /// The most recent finished run, whatever its outcome.
    async fn read_last_run(&self, stream: &str) -> Result<Option<RunSummary>, StoreError> {
        let found = self
            .client
            .query_opt(
                "SELECT status, ended_at, error FROM _siphon_runs
                 WHERE stream = $1 AND ended_at IS NOT NULL
                 ORDER BY ended_at DESC LIMIT 1",
                &[&stream],
            )
            .await?;
        Ok(found.map(|row| RunSummary {
            status: row.get(0),
            ended_at: row.get::<_, Option<i64>>(1).map(Timestamp::from_millis),
            error: row.get(2),
        }))
    }
}

impl ReportStore for PgStore {
    fn checkpoint(
        &self,
        stream: &str,
    ) -> impl Future<Output = Result<Checkpoint, StoreError>> + Send {
        self.read_checkpoint(stream)
    }

    fn first_run_at(
        &self,
        stream: &str,
    ) -> impl Future<Output = Result<Option<Timestamp>, StoreError>> + Send {
        self.read_first_run_at(stream)
    }

    fn successes_since(
        &self,
        stream: &str,
        since: Timestamp,
    ) -> impl Future<Output = Result<Vec<Timestamp>, StoreError>> + Send {
        self.read_successes_since(stream, since)
    }

    fn quarantined_count(
        &self,
        stream: &str,
    ) -> impl Future<Output = Result<u32, StoreError>> + Send {
        self.read_quarantined_count(stream)
    }

    fn last_run(
        &self,
        stream: &str,
    ) -> impl Future<Output = Result<Option<RunSummary>, StoreError>> + Send {
        self.read_last_run(stream)
    }
}
