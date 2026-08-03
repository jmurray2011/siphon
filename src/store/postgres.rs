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

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;

use tokio::sync::Mutex;
use tokio_postgres::Client;
use tokio_postgres_rustls::MakeRustlsConnect;
use tracing::{error, warn};

use super::{BudgetSnapshot, Checkpoint, RunSummary, StoreError};
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
///
/// The client is behind a `Mutex` because `transaction()` needs `&mut`, and
/// because one connection pipelining concurrent statements cannot also carry an
/// explicit `BEGIN`/`COMMIT` pair safely. That mirrors what the SQLite target
/// already does - it serializes every call onto one connection - so neither
/// target is more concurrent than the other today.
///
/// It is also the obvious next thing to fix. A shared cache read by several
/// consumers wants a connection pool, and swapping this `Mutex` for one is the
/// change that actually makes Postgres worth having over a file.
pub struct PgStore {
    client: Mutex<Client>,
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

        Ok(Self {
            client: Mutex::new(client),
        })
    }

    /// How many records are stored for a stream.
    ///
    /// # Errors
    ///
    /// Fails if the query does.
    pub async fn record_count(&self, stream: &str) -> Result<u64, StoreError> {
        let row = self
            .client
            .lock()
            .await
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
            .lock()
            .await
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
            .lock()
            .await
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
            .lock()
            .await
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
            .lock()
            .await
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
            .lock()
            .await
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

impl PgStore {
    /// Upserts a page's records and moves the checkpoint with them, in one
    /// transaction.
    ///
    /// The two halves must commit together. A page whose records land without
    /// the checkpoint moving is re-fetched and re-counted as new; a checkpoint
    /// that moves without its records is a silent hole in the mirror. SQLite got
    /// this for free from a single writer - here it is explicit.
    async fn write_page_tx(
        &self,
        page: super::PageWrite,
    ) -> Result<super::PageOutcome, StoreError> {
        let mut guard = self.client.lock().await;
        let tx = guard.transaction().await?;

        let mut outcome = super::PageOutcome::default();
        for record in &page.records {
            let hash = record.payload_hash();
            let prior = tx
                .query_opt(
                    "SELECT hash FROM _siphon_records WHERE stream = $1 AND pk = $2",
                    &[&page.stream, &record.pk()],
                )
                .await?;

            // The read-before-write is what separates a record that was re-seen
            // from one that genuinely changed, which is the signal the run log
            // reports and what answers `stop_on_seen_key`.
            if let Some(row) = prior {
                outcome.saw_known_key = true;
                let prior_hash: Vec<u8> = row.get(0);
                if prior_hash == hash.as_slice() {
                    outcome.unchanged += 1;
                    continue;
                }
                outcome.changed += 1;
            } else {
                outcome.new += 1;
            }

            tx.execute(
                "INSERT INTO _siphon_records (stream, pk, payload, hash, cursor_val, fetched_at)
                 VALUES ($1, $2, $3, $4, $5, $6)
                 ON CONFLICT (stream, pk) DO UPDATE SET
                   payload    = EXCLUDED.payload,
                   hash       = EXCLUDED.hash,
                   cursor_val = EXCLUDED.cursor_val,
                   fetched_at = EXCLUDED.fetched_at",
                &[
                    &page.stream,
                    &record.pk(),
                    &record.payload_json(),
                    &hash.as_slice(),
                    &record.cursor().map(crate::extract::CursorValue::as_str),
                    &record.fetched_at().as_millis(),
                ],
            )
            .await?;
        }

        let written = i64::try_from(page.records.len()).unwrap_or(i64::MAX);
        match page.progress {
            super::Progress::Resume(state) => {
                tx.execute(
                    "INSERT INTO _siphon_state (stream, page_state, last_run_at, records_seen)
                     VALUES ($1, $2, $3, $4)
                     ON CONFLICT (stream) DO UPDATE SET
                       page_state   = EXCLUDED.page_state,
                       last_run_at  = EXCLUDED.last_run_at,
                       records_seen = _siphon_state.records_seen + EXCLUDED.records_seen",
                    &[&page.stream, &state, &page.at.as_millis(), &written],
                )
                .await?;
            }
            super::Progress::Complete {
                cursor_val,
                success_at,
            } => {
                tx.execute(
                    "INSERT INTO _siphon_state
                       (stream, cursor_val, page_state, last_run_at, last_success_at, records_seen)
                     VALUES ($1, $2, NULL, $3, $4, $5)
                     ON CONFLICT (stream) DO UPDATE SET
                       page_state      = NULL,
                       cursor_val      = COALESCE(EXCLUDED.cursor_val, _siphon_state.cursor_val),
                       last_run_at     = EXCLUDED.last_run_at,
                       last_success_at = EXCLUDED.last_success_at,
                       records_seen    = _siphon_state.records_seen + EXCLUDED.records_seen",
                    &[
                        &page.stream,
                        &cursor_val,
                        &page.at.as_millis(),
                        &success_at.as_millis(),
                        &written,
                    ],
                )
                .await?;
            }
        }

        tx.commit().await?;
        Ok(outcome)
    }

    /// Clears the resume point and advances the marks a finished walk reached.
    async fn finish_walk_row(
        &self,
        stream: &str,
        cursor_val: Option<String>,
        success_at: Option<Timestamp>,
    ) -> Result<(), StoreError> {
        let at = success_at.map(Timestamp::as_millis);
        self.client
            .lock()
            .await
            .execute(
                "INSERT INTO _siphon_state
                   (stream, cursor_val, page_state, last_run_at, last_success_at)
                 VALUES ($1, $2, NULL, $3, $3)
                 ON CONFLICT (stream) DO UPDATE SET
                   page_state      = NULL,
                   cursor_val      = COALESCE(EXCLUDED.cursor_val, _siphon_state.cursor_val),
                   last_run_at     = COALESCE(EXCLUDED.last_run_at, _siphon_state.last_run_at),
                   last_success_at = COALESCE(EXCLUDED.last_success_at,
                                              _siphon_state.last_success_at)",
                &[&stream, &cursor_val, &at],
            )
            .await?;
        Ok(())
    }

    /// Opens a run row. `RETURNING` because Postgres has no `last_insert_rowid`.
    async fn open_run(
        &self,
        stream: &str,
        started_at: Timestamp,
        deficit: f64,
    ) -> Result<i64, StoreError> {
        let row = self
            .client
            .lock()
            .await
            .query_one(
                "INSERT INTO _siphon_runs (stream, started_at, deficit_at_admission)
                 VALUES ($1, $2, $3) RETURNING id",
                &[&stream, &started_at.as_millis(), &deficit],
            )
            .await?;
        Ok(row.get(0))
    }

    /// Completes a run row.
    async fn close_run(&self, run: super::RunRecord) -> Result<(), StoreError> {
        self.client
            .lock()
            .await
            .execute(
                "UPDATE _siphon_runs SET
                   ended_at        = $2,
                   status          = $3,
                   pages           = $4,
                   records         = $5,
                   records_new     = $6,
                   records_changed = $7,
                   not_modified    = $8,
                   requests        = $9,
                   stop_reason     = $10,
                   error           = $11
                 WHERE id = $1",
                &[
                    &run.id,
                    &run.ended_at.as_millis(),
                    &run.status.as_str(),
                    &i64::from(run.pages),
                    &i64::from(run.records),
                    &i64::from(run.records_new),
                    &i64::from(run.records_changed),
                    &i64::from(run.not_modified),
                    &i64::from(run.requests),
                    &run.stop_reason,
                    &run.error,
                ],
            )
            .await?;
        Ok(())
    }

    /// The stored validator for a request.
    async fn read_validator(
        &self,
        request_key: &str,
    ) -> Result<Option<super::Validator>, StoreError> {
        let found = self
            .client
            .lock()
            .await
            .query_opt(
                "SELECT etag, last_modified FROM _siphon_validators WHERE request_key = $1",
                &[&request_key],
            )
            .await?;
        Ok(found.map(|row| super::Validator {
            etag: row.get(0),
            last_modified: row.get(1),
        }))
    }

    /// Stores the validator a response carried.
    async fn write_validator(
        &self,
        request_key: &str,
        validator: super::Validator,
        at: Timestamp,
    ) -> Result<(), StoreError> {
        self.client
            .lock()
            .await
            .execute(
                "INSERT INTO _siphon_validators (request_key, etag, last_modified, seen_at)
                 VALUES ($1, $2, $3, $4)
                 ON CONFLICT (request_key) DO UPDATE SET
                   etag          = EXCLUDED.etag,
                   last_modified = EXCLUDED.last_modified,
                   seen_at       = EXCLUDED.seen_at",
                &[
                    &request_key,
                    &validator.etag,
                    &validator.last_modified,
                    &at.as_millis(),
                ],
            )
            .await?;
        Ok(())
    }

    /// Sets a page aside, or bumps the attempt count if it is already set aside.
    async fn write_quarantine(&self, page: super::QuarantinedPage) -> Result<(), StoreError> {
        let mut excerpt = page.body_excerpt;
        excerpt.truncate(super::BODY_EXCERPT_LIMIT.min(excerpt.len()));
        let status = page.status_code.map(i64::from);

        self.client
            .lock()
            .await
            .execute(
                "INSERT INTO _siphon_quarantine
                   (stream, page_state, attempts, status_code, body_excerpt, first_seen, last_seen)
                 VALUES ($1, $2, $3, $4, $5, $6, $6)
                 ON CONFLICT (stream, page_state) DO UPDATE SET
                   attempts     = _siphon_quarantine.attempts + EXCLUDED.attempts,
                   status_code  = EXCLUDED.status_code,
                   body_excerpt = EXCLUDED.body_excerpt,
                   last_seen    = EXCLUDED.last_seen",
                &[
                    &page.stream,
                    &page.page_state,
                    &i64::from(page.attempts),
                    &status,
                    &excerpt,
                    &page.at.as_millis(),
                ],
            )
            .await?;
        Ok(())
    }

    /// Whether this exact page state is set aside.
    async fn read_is_quarantined(
        &self,
        stream: &str,
        page_state: &str,
    ) -> Result<bool, StoreError> {
        let found = self
            .client
            .lock()
            .await
            .query_opt(
                "SELECT 1 FROM _siphon_quarantine WHERE stream = $1 AND page_state = $2",
                &[&stream, &page_state],
            )
            .await?;
        Ok(found.is_some())
    }

    /// Whether any of these keys is already stored.
    ///
    /// One statement with `= ANY`, rather than the row-at-a-time loop the SQLite
    /// target uses: there each query is a function call, here each would be a
    /// network round trip.
    async fn read_any_known_key(&self, stream: &str, pks: Vec<String>) -> Result<bool, StoreError> {
        if pks.is_empty() {
            return Ok(false);
        }
        let found = self
            .client
            .lock()
            .await
            .query_opt(
                "SELECT 1 FROM _siphon_records WHERE stream = $1 AND pk = ANY($2) LIMIT 1",
                &[&stream, &pks],
            )
            .await?;
        Ok(found.is_some())
    }
}

impl crate::sync::SyncStore for PgStore {
    fn checkpoint(
        &self,
        stream: &str,
    ) -> impl Future<Output = Result<Checkpoint, StoreError>> + Send {
        self.read_checkpoint(stream)
    }

    fn write_page(
        &self,
        page: super::PageWrite,
    ) -> impl Future<Output = Result<super::PageOutcome, StoreError>> + Send {
        self.write_page_tx(page)
    }

    fn finish_walk(
        &self,
        stream: &str,
        cursor_val: Option<String>,
        success_at: Option<Timestamp>,
    ) -> impl Future<Output = Result<(), StoreError>> + Send {
        self.finish_walk_row(stream, cursor_val, success_at)
    }

    fn begin_run(
        &self,
        stream: &str,
        started_at: Timestamp,
        deficit: f64,
    ) -> impl Future<Output = Result<i64, StoreError>> + Send {
        self.open_run(stream, started_at, deficit)
    }

    fn finish_run(
        &self,
        run: super::RunRecord,
    ) -> impl Future<Output = Result<(), StoreError>> + Send {
        self.close_run(run)
    }

    fn validator(
        &self,
        request_key: &str,
    ) -> impl Future<Output = Result<Option<super::Validator>, StoreError>> + Send {
        self.read_validator(request_key)
    }

    fn save_validator(
        &self,
        request_key: &str,
        validator: super::Validator,
        at: Timestamp,
    ) -> impl Future<Output = Result<(), StoreError>> + Send {
        self.write_validator(request_key, validator, at)
    }

    fn quarantine_page(
        &self,
        page: super::QuarantinedPage,
    ) -> impl Future<Output = Result<(), StoreError>> + Send {
        self.write_quarantine(page)
    }

    fn is_quarantined(
        &self,
        stream: &str,
        page_state: &str,
    ) -> impl Future<Output = Result<bool, StoreError>> + Send {
        self.read_is_quarantined(stream, page_state)
    }

    fn quarantined_count(
        &self,
        stream: &str,
    ) -> impl Future<Output = Result<u32, StoreError>> + Send {
        self.read_quarantined_count(stream)
    }

    fn any_known_key(
        &self,
        stream: &str,
        pks: Vec<String>,
    ) -> impl Future<Output = Result<bool, StoreError>> + Send {
        self.read_any_known_key(stream, pks)
    }
}

impl PgStore {
    /// Every stored checkpoint, keyed by stream.
    ///
    /// # Errors
    ///
    /// Fails if the query does.
    pub async fn checkpoints(&self) -> Result<BTreeMap<String, Checkpoint>, StoreError> {
        let rows = self
            .client
            .lock()
            .await
            .query(
                "SELECT stream, cursor_val, page_state, last_run_at, last_success_at, records_seen
                 FROM _siphon_state",
                &[],
            )
            .await?;

        Ok(rows
            .iter()
            .map(|row| {
                let records_seen: i64 = row.get(5);
                (
                    row.get::<_, String>(0),
                    Checkpoint {
                        cursor_val: row.get(1),
                        page_state: row.get(2),
                        last_run_at: row.get::<_, Option<i64>>(3).map(Timestamp::from_millis),
                        last_success_at: row.get::<_, Option<i64>>(4).map(Timestamp::from_millis),
                        records_seen: records_seen.max(0).unsigned_abs(),
                    },
                )
            })
            .collect())
    }

    /// The learned budget for a source, if one was ever persisted.
    ///
    /// # Errors
    ///
    /// Fails if the query does.
    pub async fn load_budget(&self, source: &str) -> Result<Option<BudgetSnapshot>, StoreError> {
        let found = self
            .client
            .lock()
            .await
            .query_opt(
                "SELECT observed_limit, window_secs, remaining, reset_at, ratcheted_at
                 FROM _siphon_budget WHERE source = $1",
                &[&source],
            )
            .await?;

        Ok(found.map(|row| BudgetSnapshot {
            observed_limit: row.get::<_, Option<i64>>(0).map(super::u64_of),
            window_secs: row.get::<_, Option<i64>>(1).map(super::u64_of),
            remaining: row.get::<_, Option<i64>>(2).map(super::u64_of),
            reset_at: row.get::<_, Option<i64>>(3).map(Timestamp::from_millis),
            ratcheted_at: row.get::<_, Option<i64>>(4).map(Timestamp::from_millis),
        }))
    }

    /// Persists the learned budget for a source.
    ///
    /// # Errors
    ///
    /// Fails if the upsert does.
    pub async fn save_budget(
        &self,
        source: &str,
        snapshot: BudgetSnapshot,
        at: Timestamp,
    ) -> Result<(), StoreError> {
        self.client
            .lock()
            .await
            .execute(
                "INSERT INTO _siphon_budget
                   (source, observed_limit, window_secs, remaining, reset_at, ratcheted_at,
                    updated_at)
                 VALUES ($1, $2, $3, $4, $5, $6, $7)
                 ON CONFLICT (source) DO UPDATE SET
                   observed_limit = EXCLUDED.observed_limit,
                   window_secs    = EXCLUDED.window_secs,
                   remaining      = EXCLUDED.remaining,
                   reset_at       = EXCLUDED.reset_at,
                   ratcheted_at   = EXCLUDED.ratcheted_at,
                   updated_at     = EXCLUDED.updated_at",
                &[
                    &source,
                    &snapshot.observed_limit.map(super::i64_of),
                    &snapshot.window_secs.map(super::i64_of),
                    &snapshot.remaining.map(super::i64_of),
                    &snapshot.reset_at.map(Timestamp::as_millis),
                    &snapshot.ratcheted_at.map(Timestamp::as_millis),
                    &at.as_millis(),
                ],
            )
            .await?;
        Ok(())
    }
}
