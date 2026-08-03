//! The SQLite adapter.
//!
//! One guarantee lives here and shapes the whole module: a page's records and
//! the checkpoint that says the page was handled commit in the *same*
//! transaction. That yields at-least-once delivery with idempotent upserts on
//! `(stream, pk)`, which for a mirror is equivalent to exactly-once.
//!
//! The connection is single-threaded by design; concurrency is across streams,
//! not within a write. Calls are async wrappers over `spawn_blocking` so a slow
//! commit never stalls the executor.

pub mod postgres;
pub mod schema;

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

use rusqlite::{params, Connection, OptionalExtension, Transaction};

use crate::budget::BudgetSnapshot;
use crate::clock::Timestamp;
use crate::extract::Record;

/// How much of a failing response body is kept in the quarantine record.
const BODY_EXCERPT_LIMIT: usize = 2_000;

/// Why a store operation failed.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// The database rejected a statement or could not be opened.
    #[error("database error")]
    Sqlite(#[from] rusqlite::Error),
    /// The Postgres server rejected a statement, or could not be reached.
    #[error("postgres error")]
    Postgres(#[from] tokio_postgres::Error),
    /// The host trust store yielded no usable root certificates.
    ///
    /// Separate from a connection failure because the fix is different: nothing
    /// would ever verify, so this is a broken host rather than a broken server.
    #[error("no usable root certificates in the host trust store")]
    NoRootCertificates,
    /// The database directory could not be created.
    #[error("cannot create database directory `{path}`")]
    Directory {
        /// The directory that was tried.
        path: std::path::PathBuf,
        /// The underlying I/O error.
        #[source]
        source: std::io::Error,
    },
    /// The file was written by a newer siphon.
    #[error("database schema version {found} is newer than this build understands ({expected})")]
    SchemaTooNew {
        /// The version on disk.
        found: i32,
        /// The version this build writes.
        expected: i32,
    },
    /// The blocking task carrying the query panicked or was cancelled.
    #[error("store task failed")]
    Join(#[from] tokio::task::JoinError),
}

/// A handle to the mirror database.
#[derive(Clone)]
pub struct Store {
    conn: Arc<Mutex<Connection>>,
}

impl std::fmt::Debug for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Store")
    }
}

impl Store {
    /// Opens (or creates) the mirror at `path` and applies the schema.
    ///
    /// # Errors
    ///
    /// Fails if the directory cannot be created, the file cannot be opened, or
    /// it was written by a newer siphon.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let path = path.as_ref();
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent).map_err(|source| StoreError::Directory {
                path: parent.to_owned(),
                source,
            })?;
        }
        Self::from_connection(Connection::open(path)?)
    }

    /// Opens a private in-memory mirror, for tests and `siphon test`.
    ///
    /// # Errors
    ///
    /// Fails if SQLite cannot allocate the database.
    pub fn open_in_memory() -> Result<Self, StoreError> {
        Self::from_connection(Connection::open_in_memory()?)
    }

    fn from_connection(conn: Connection) -> Result<Self, StoreError> {
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.busy_timeout(std::time::Duration::from_secs(30))?;

        let found: i32 = conn.pragma_query_value(None, "user_version", |row| row.get(0))?;
        if found > schema::SCHEMA_VERSION {
            return Err(StoreError::SchemaTooNew {
                found,
                expected: schema::SCHEMA_VERSION,
            });
        }
        conn.execute_batch(schema::DDL)?;
        conn.pragma_update(None, "user_version", schema::SCHEMA_VERSION)?;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Runs `f` against the connection on the blocking pool.
    async fn call<T, F>(&self, f: F) -> Result<T, StoreError>
    where
        F: FnOnce(&mut Connection) -> Result<T, StoreError> + Send + 'static,
        T: Send + 'static,
    {
        let conn = Arc::clone(&self.conn);
        tokio::task::spawn_blocking(move || {
            let mut guard = lock(&conn);
            f(&mut guard)
        })
        .await?
    }

    /// The checkpoint for one stream, or a blank one if it has never run.
    ///
    /// # Errors
    ///
    /// Fails if the query fails.
    pub async fn checkpoint(&self, stream: &str) -> Result<Checkpoint, StoreError> {
        let stream = stream.to_owned();
        self.call(move |conn| read_checkpoint(conn, &stream)).await
    }

    /// Checkpoints for every stream that has one, keyed by stream name.
    ///
    /// # Errors
    ///
    /// Fails if the query fails.
    pub async fn checkpoints(&self) -> Result<BTreeMap<String, Checkpoint>, StoreError> {
        self.call(|conn| {
            let mut stmt = conn.prepare(
                "SELECT stream, cursor_val, page_state, last_run_at, last_success_at, records_seen
                 FROM _siphon_state",
            )?;
            let rows = stmt.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    Checkpoint {
                        cursor_val: row.get(1)?,
                        page_state: row.get(2)?,
                        last_run_at: row.get::<_, Option<i64>>(3)?.map(Timestamp::from_millis),
                        last_success_at: row.get::<_, Option<i64>>(4)?.map(Timestamp::from_millis),
                        records_seen: row.get::<_, i64>(5)?.max(0).unsigned_abs(),
                    },
                ))
            })?;
            let mut out = BTreeMap::new();
            for row in rows {
                let (name, checkpoint) = row?;
                out.insert(name, checkpoint);
            }
            Ok(out)
        })
        .await
    }

    /// Writes one page's records and advances the checkpoint, atomically.
    ///
    /// This is the crux of the durability guarantee: either the records and the
    /// paginator's new position both land, or neither does. There is no window
    /// in which a restart re-fetches data already written, or skips data it
    /// believes it wrote.
    ///
    /// # Errors
    ///
    /// Fails if the transaction cannot be committed.
    pub async fn write_page(&self, page: PageWrite) -> Result<PageOutcome, StoreError> {
        self.call(move |conn| {
            let tx = conn.transaction()?;
            let outcome = upsert_records(&tx, &page.stream, &page.records)?;
            let written = i64::try_from(page.records.len()).unwrap_or(i64::MAX);

            match page.progress {
                Progress::Resume(state) => tx.execute(
                    "INSERT INTO _siphon_state (stream, page_state, last_run_at, records_seen)
                     VALUES (?1, ?2, ?3, ?4)
                     ON CONFLICT(stream) DO UPDATE SET
                       page_state   = excluded.page_state,
                       last_run_at  = excluded.last_run_at,
                       records_seen = _siphon_state.records_seen + excluded.records_seen",
                    params![page.stream, state, page.at.as_millis(), written],
                )?,
                // The final page commits the records, the cleared resume point,
                // the advanced high-water mark and the freshness clock together.
                // There is no instant at which the mirror holds the data but not
                // the fact that it holds it.
                Progress::Complete {
                    cursor_val,
                    success_at,
                } => tx.execute(
                    "INSERT INTO _siphon_state
                       (stream, cursor_val, page_state, last_run_at, last_success_at, records_seen)
                     VALUES (?1, ?2, NULL, ?3, ?4, ?5)
                     ON CONFLICT(stream) DO UPDATE SET
                       page_state      = NULL,
                       cursor_val      = COALESCE(excluded.cursor_val, _siphon_state.cursor_val),
                       last_run_at     = excluded.last_run_at,
                       last_success_at = excluded.last_success_at,
                       records_seen    = _siphon_state.records_seen + excluded.records_seen",
                    params![
                        page.stream,
                        cursor_val,
                        page.at.as_millis(),
                        success_at.as_millis(),
                        written
                    ],
                )?,
            };

            tx.commit()?;
            Ok(outcome)
        })
        .await
    }

    /// Completes a walk: clears the resume point and advances the high-water
    /// mark and the freshness clock.
    ///
    /// `success_at` is also supplied for a 304, because a confirmed-unchanged
    /// response means the mirror is current.
    ///
    /// # Errors
    ///
    /// Fails if the update fails.
    pub async fn finish_walk(
        &self,
        stream: &str,
        cursor_val: Option<String>,
        success_at: Option<Timestamp>,
    ) -> Result<(), StoreError> {
        let stream = stream.to_owned();
        self.call(move |conn| {
            conn.execute(
                "INSERT INTO _siphon_state (stream, cursor_val, page_state, last_run_at, last_success_at)
                 VALUES (?1, ?2, NULL, ?3, ?3)
                 ON CONFLICT(stream) DO UPDATE SET
                   page_state      = NULL,
                   cursor_val      = COALESCE(excluded.cursor_val, _siphon_state.cursor_val),
                   last_run_at     = COALESCE(excluded.last_run_at, _siphon_state.last_run_at),
                   last_success_at = COALESCE(excluded.last_success_at, _siphon_state.last_success_at)",
                params![stream, cursor_val, success_at.map(Timestamp::as_millis)],
            )?;
            Ok(())
        })
        .await
    }

    /// Records that a run started, returning its id.
    ///
    /// # Errors
    ///
    /// Fails if the insert fails.
    pub async fn begin_run(
        &self,
        stream: &str,
        started_at: Timestamp,
        deficit: f64,
    ) -> Result<i64, StoreError> {
        let stream = stream.to_owned();
        self.call(move |conn| {
            conn.execute(
                "INSERT INTO _siphon_runs (stream, started_at, deficit_at_admission)
                 VALUES (?1, ?2, ?3)",
                params![stream, started_at.as_millis(), deficit],
            )?;
            Ok(conn.last_insert_rowid())
        })
        .await
    }

    /// Records how a run ended.
    ///
    /// # Errors
    ///
    /// Fails if the update fails.
    pub async fn finish_run(&self, run: RunRecord) -> Result<(), StoreError> {
        self.call(move |conn| {
            conn.execute(
                "UPDATE _siphon_runs SET
                   ended_at        = ?2,
                   status          = ?3,
                   pages           = ?4,
                   records         = ?5,
                   records_new     = ?6,
                   records_changed = ?7,
                   not_modified    = ?8,
                   requests        = ?9,
                   stop_reason     = ?10,
                   error           = ?11
                 WHERE id = ?1",
                params![
                    run.id,
                    run.ended_at.as_millis(),
                    run.status.as_str(),
                    run.pages,
                    run.records,
                    run.records_new,
                    run.records_changed,
                    run.not_modified,
                    run.requests,
                    run.stop_reason,
                    run.error,
                ],
            )?;
            Ok(())
        })
        .await
    }

    /// The learned budget for a source, if one was ever persisted.
    ///
    /// # Errors
    ///
    /// Fails if the query fails.
    pub async fn load_budget(&self, source: &str) -> Result<Option<BudgetSnapshot>, StoreError> {
        let source = source.to_owned();
        self.call(move |conn| {
            let snapshot = conn
                .query_row(
                    "SELECT observed_limit, window_secs, remaining, reset_at, ratcheted_at
                     FROM _siphon_budget WHERE source = ?1",
                    params![source],
                    |row| {
                        Ok(BudgetSnapshot {
                            observed_limit: row.get::<_, Option<i64>>(0)?.map(u64_of),
                            window_secs: row.get::<_, Option<i64>>(1)?.map(u64_of),
                            remaining: row.get::<_, Option<i64>>(2)?.map(u64_of),
                            reset_at: row.get::<_, Option<i64>>(3)?.map(Timestamp::from_millis),
                            ratcheted_at: row.get::<_, Option<i64>>(4)?.map(Timestamp::from_millis),
                        })
                    },
                )
                .optional()?;
            Ok(snapshot)
        })
        .await
    }

    /// Persists the learned budget for a source.
    ///
    /// # Errors
    ///
    /// Fails if the upsert fails.
    pub async fn save_budget(
        &self,
        source: &str,
        snapshot: BudgetSnapshot,
        at: Timestamp,
    ) -> Result<(), StoreError> {
        let source = source.to_owned();
        self.call(move |conn| {
            conn.execute(
                "INSERT INTO _siphon_budget
                   (source, observed_limit, window_secs, remaining, reset_at, ratcheted_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT(source) DO UPDATE SET
                   observed_limit = excluded.observed_limit,
                   window_secs    = excluded.window_secs,
                   remaining      = excluded.remaining,
                   reset_at       = excluded.reset_at,
                   ratcheted_at   = excluded.ratcheted_at,
                   updated_at     = excluded.updated_at",
                params![
                    source,
                    snapshot.observed_limit.map(i64_of),
                    snapshot.window_secs.map(i64_of),
                    snapshot.remaining.map(i64_of),
                    snapshot.reset_at.map(Timestamp::as_millis),
                    snapshot.ratcheted_at.map(Timestamp::as_millis),
                    at.as_millis(),
                ],
            )?;
            Ok(())
        })
        .await
    }

    /// The stored `ETag` / `Last-Modified` for a request URL.
    ///
    /// # Errors
    ///
    /// Fails if the query fails.
    pub async fn validator(&self, request_key: &str) -> Result<Option<Validator>, StoreError> {
        let request_key = request_key.to_owned();
        self.call(move |conn| {
            let found = conn
                .query_row(
                    "SELECT etag, last_modified FROM _siphon_validators WHERE request_key = ?1",
                    params![request_key],
                    |row| {
                        Ok(Validator {
                            etag: row.get(0)?,
                            last_modified: row.get(1)?,
                        })
                    },
                )
                .optional()?;
            Ok(found)
        })
        .await
    }

    /// Stores the validators a response carried.
    ///
    /// # Errors
    ///
    /// Fails if the upsert fails.
    pub async fn save_validator(
        &self,
        request_key: &str,
        validator: Validator,
        at: Timestamp,
    ) -> Result<(), StoreError> {
        let request_key = request_key.to_owned();
        self.call(move |conn| {
            conn.execute(
                "INSERT INTO _siphon_validators (request_key, etag, last_modified, seen_at)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(request_key) DO UPDATE SET
                   etag          = excluded.etag,
                   last_modified = excluded.last_modified,
                   seen_at       = excluded.seen_at",
                params![
                    request_key,
                    validator.etag,
                    validator.last_modified,
                    at.as_millis()
                ],
            )?;
            Ok(())
        })
        .await
    }

    /// Records a page that failed fatally and was skipped.
    ///
    /// # Errors
    ///
    /// Fails if the upsert fails.
    pub async fn quarantine_page(&self, page: QuarantinedPage) -> Result<(), StoreError> {
        self.call(move |conn| {
            let mut excerpt = page.body_excerpt;
            if excerpt.len() > BODY_EXCERPT_LIMIT {
                excerpt.truncate(BODY_EXCERPT_LIMIT);
            }
            conn.execute(
                "INSERT INTO _siphon_quarantine
                   (stream, page_state, attempts, status_code, body_excerpt, first_seen, last_seen)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6)
                 ON CONFLICT(stream, page_state) DO UPDATE SET
                   attempts     = _siphon_quarantine.attempts + excluded.attempts,
                   status_code  = excluded.status_code,
                   body_excerpt = excluded.body_excerpt,
                   last_seen    = excluded.last_seen",
                params![
                    page.stream,
                    page.page_state,
                    page.attempts,
                    page.status_code,
                    excerpt,
                    page.at.as_millis(),
                ],
            )?;
            Ok(())
        })
        .await
    }

    /// Whether this exact paginator state has already been quarantined.
    ///
    /// A walk that reaches a known-poisoned page stops rather than spending the
    /// budget to fail again.
    ///
    /// # Errors
    ///
    /// Fails if the query fails.
    pub async fn is_quarantined(&self, stream: &str, page_state: &str) -> Result<bool, StoreError> {
        let stream = stream.to_owned();
        let page_state = page_state.to_owned();
        self.call(move |conn| {
            let found: Option<i64> = conn
                .query_row(
                    "SELECT 1 FROM _siphon_quarantine WHERE stream = ?1 AND page_state = ?2",
                    params![stream, page_state],
                    |row| row.get(0),
                )
                .optional()?;
            Ok(found.is_some())
        })
        .await
    }

    /// How many pages of a stream are quarantined. Anything above zero is a
    /// known gap in the mirror.
    ///
    /// # Errors
    ///
    /// Fails if the query fails.
    pub async fn quarantined_count(&self, stream: &str) -> Result<u32, StoreError> {
        let stream = stream.to_owned();
        self.call(move |conn| {
            let count: i64 = conn.query_row(
                "SELECT COUNT(*) FROM _siphon_quarantine WHERE stream = ?1",
                params![stream],
                |row| row.get(0),
            )?;
            Ok(u32::try_from(count).unwrap_or(u32::MAX))
        })
        .await
    }

    /// Timestamps of every successful run for a stream at or after `since`.
    ///
    /// Used to compute SLO attainment. A 304 counts: the data was confirmed
    /// current, which is what the target is about.
    ///
    /// # Errors
    ///
    /// Fails if the query fails.
    pub async fn successes_since(
        &self,
        stream: &str,
        since: Timestamp,
    ) -> Result<Vec<Timestamp>, StoreError> {
        let stream = stream.to_owned();
        self.call(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT ended_at FROM _siphon_runs
                 WHERE stream = ?1 AND ended_at >= ?2
                   AND status IN ('ok', 'partial', 'not_modified')
                 ORDER BY ended_at",
            )?;
            let rows = stmt.query_map(params![stream, since.as_millis()], |row| {
                Ok(Timestamp::from_millis(row.get::<_, i64>(0)?))
            })?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(StoreError::from)
        })
        .await
    }

    /// When the stream first ran at all.
    ///
    /// Attainment is measured from here rather than from the start of the
    /// reporting window: scoring a stream over days during which it did not
    /// exist reports a healthy new stream as a total failure.
    ///
    /// # Errors
    ///
    /// Fails if the query fails.
    pub async fn first_run_at(&self, stream: &str) -> Result<Option<Timestamp>, StoreError> {
        let stream = stream.to_owned();
        self.call(move |conn| {
            let found: Option<i64> = conn.query_row(
                "SELECT MIN(started_at) FROM _siphon_runs WHERE stream = ?1",
                params![stream],
                |row| row.get(0),
            )?;
            Ok(found.map(Timestamp::from_millis))
        })
        .await
    }

    /// The most recent run of a stream, whatever its outcome.
    ///
    /// # Errors
    ///
    /// Fails if the query fails.
    pub async fn last_run(&self, stream: &str) -> Result<Option<RunSummary>, StoreError> {
        let stream = stream.to_owned();
        self.call(move |conn| {
            let found = conn
                .query_row(
                    "SELECT status, ended_at, error FROM _siphon_runs
                     WHERE stream = ?1 AND ended_at IS NOT NULL
                     ORDER BY ended_at DESC LIMIT 1",
                    params![stream],
                    |row| {
                        Ok(RunSummary {
                            status: row.get::<_, Option<String>>(0)?,
                            ended_at: row.get::<_, Option<i64>>(1)?.map(Timestamp::from_millis),
                            error: row.get(2)?,
                        })
                    },
                )
                .optional()?;
            Ok(found)
        })
        .await
    }

    /// Whether any of these primary keys is already stored for the stream.
    ///
    /// This is what answers `stop_on_seen_key`: on a reverse-chronological feed
    /// with no cursor, meeting a record you already have means everything after
    /// it is older and already mirrored.
    ///
    /// # Errors
    ///
    /// Fails if the query fails.
    pub async fn any_known_key(&self, stream: &str, pks: Vec<String>) -> Result<bool, StoreError> {
        let stream = stream.to_owned();
        self.call(move |conn| {
            let mut stmt =
                conn.prepare_cached("SELECT 1 FROM _siphon_records WHERE stream = ?1 AND pk = ?2")?;
            for pk in pks {
                let found: Option<i64> = stmt
                    .query_row(params![stream, pk], |row| row.get(0))
                    .optional()?;
                if found.is_some() {
                    return Ok(true);
                }
            }
            Ok(false)
        })
        .await
    }

    /// How many records a stream has stored.
    ///
    /// # Errors
    ///
    /// Fails if the query fails.
    pub async fn record_count(&self, stream: &str) -> Result<u64, StoreError> {
        let stream = stream.to_owned();
        self.call(move |conn| {
            let count: i64 = conn.query_row(
                "SELECT COUNT(*) FROM _siphon_records WHERE stream = ?1",
                params![stream],
                |row| row.get(0),
            )?;
            Ok(u64_of(count))
        })
        .await
    }
}

fn lock(conn: &Mutex<Connection>) -> MutexGuard<'_, Connection> {
    conn.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn read_checkpoint(conn: &Connection, stream: &str) -> Result<Checkpoint, StoreError> {
    let found = conn
        .query_row(
            "SELECT cursor_val, page_state, last_run_at, last_success_at, records_seen
             FROM _siphon_state WHERE stream = ?1",
            params![stream],
            |row| {
                Ok(Checkpoint {
                    cursor_val: row.get(0)?,
                    page_state: row.get(1)?,
                    last_run_at: row.get::<_, Option<i64>>(2)?.map(Timestamp::from_millis),
                    last_success_at: row.get::<_, Option<i64>>(3)?.map(Timestamp::from_millis),
                    records_seen: row.get::<_, i64>(4)?.max(0).unsigned_abs(),
                })
            },
        )
        .optional()?;
    Ok(found.unwrap_or_default())
}

/// Upserts a page of records, counting what was new, changed and unchanged.
///
/// The read-before-write is deliberate: it is what distinguishes a record that
/// was re-seen from one that genuinely changed, which is the signal the run log
/// needs, and it is also what answers `stop_on_seen_key`.
fn upsert_records(
    tx: &Transaction<'_>,
    stream: &str,
    records: &[Record],
) -> Result<PageOutcome, StoreError> {
    let mut existing =
        tx.prepare_cached("SELECT hash FROM _siphon_records WHERE stream = ?1 AND pk = ?2")?;
    let mut upsert = tx.prepare_cached(
        "INSERT INTO _siphon_records (stream, pk, payload, hash, cursor_val, fetched_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)
         ON CONFLICT(stream, pk) DO UPDATE SET
           payload    = excluded.payload,
           hash       = excluded.hash,
           cursor_val = excluded.cursor_val,
           fetched_at = excluded.fetched_at",
    )?;

    let mut outcome = PageOutcome::default();
    for record in records {
        let hash = record.payload_hash();
        let prior: Option<Vec<u8>> = existing
            .query_row(params![stream, record.pk()], |row| row.get(0))
            .optional()?;

        match prior {
            Some(prior_hash) => {
                outcome.saw_known_key = true;
                if prior_hash == hash {
                    outcome.unchanged += 1;
                    continue;
                }
                outcome.changed += 1;
            }
            None => outcome.new += 1,
        }

        upsert.execute(params![
            stream,
            record.pk(),
            record.payload_json(),
            hash.as_slice(),
            record.cursor().map(crate::extract::CursorValue::as_str),
            record.fetched_at().as_millis(),
        ])?;
    }
    Ok(outcome)
}

fn u64_of(value: i64) -> u64 {
    value.max(0).unsigned_abs()
}

fn i64_of(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

/// Where a stream got to.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Checkpoint {
    /// High-water mark for incremental sync.
    pub cursor_val: Option<String>,
    /// Serialized paginator state, or `None` when no walk is in flight.
    pub page_state: Option<String>,
    /// When the last run started.
    pub last_run_at: Option<Timestamp>,
    /// When the stream was last confirmed current.
    pub last_success_at: Option<Timestamp>,
    /// Records written across all runs.
    pub records_seen: u64,
}

/// One page's worth of work to commit.
#[derive(Debug)]
pub struct PageWrite {
    /// Which stream.
    pub stream: String,
    /// The records extracted from the page.
    pub records: Vec<Record>,
    /// Where the walk stands after this page.
    pub progress: Progress,
    /// When the page was handled.
    pub at: Timestamp,
}

/// Where a walk stands after a page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Progress {
    /// More pages to come; this is the state to resume from.
    Resume(String),
    /// The walk is finished.
    Complete {
        /// The new high-water mark, if the stream advanced one.
        cursor_val: Option<String>,
        /// When the stream was confirmed current.
        success_at: Timestamp,
    },
}

/// What a page write changed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PageOutcome {
    /// Records that were not in the store.
    pub new: u32,
    /// Records whose payload hash differed from the stored one.
    pub changed: u32,
    /// Records that were byte-identical to what was stored.
    pub unchanged: u32,
    /// Whether any record on the page was already known.
    pub saw_known_key: bool,
}

impl PageOutcome {
    /// How many records the page held.
    #[must_use]
    pub const fn total(&self) -> u32 {
        self.new + self.changed + self.unchanged
    }
}

/// Conditional-request validators for one URL.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Validator {
    /// The `ETag` the server last sent.
    pub etag: Option<String>,
    /// The `Last-Modified` the server last sent.
    pub last_modified: Option<String>,
}

impl Validator {
    /// True when there is something to send back.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.etag.is_none() && self.last_modified.is_none()
    }
}

/// How a run ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunStatus {
    /// Completed, mirror is current.
    Ok,
    /// Completed, but pages are quarantined or the walk was truncated.
    Partial,
    /// The upstream confirmed nothing had changed.
    NotModified,
    /// The run did not complete.
    Failed,
}

impl RunStatus {
    /// The label written to `_siphon_runs.status`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Partial => "partial",
            Self::NotModified => "not_modified",
            Self::Failed => "failed",
        }
    }

    /// True when the mirror can be considered current as of this run.
    #[must_use]
    pub const fn is_success(self) -> bool {
        matches!(self, Self::Ok | Self::Partial | Self::NotModified)
    }
}

/// The outcome of a run, written when it ends.
#[derive(Debug, Clone)]
pub struct RunRecord {
    /// The id returned by [`Store::begin_run`].
    pub id: i64,
    /// When it ended.
    pub ended_at: Timestamp,
    /// How it ended.
    pub status: RunStatus,
    /// Pages walked.
    pub pages: u32,
    /// Records seen.
    pub records: u32,
    /// Records inserted.
    pub records_new: u32,
    /// Records whose payload changed.
    pub records_changed: u32,
    /// How many 304 responses were received.
    pub not_modified: u32,
    /// Requests actually spent.
    pub requests: u32,
    /// Why the walk stopped.
    pub stop_reason: Option<String>,
    /// The failure, if it failed.
    pub error: Option<String>,
}

/// A summary of the most recent run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunSummary {
    /// The run's status label.
    pub status: Option<String>,
    /// When it ended.
    pub ended_at: Option<Timestamp>,
    /// The failure, if it failed.
    pub error: Option<String>,
}

/// A page that failed fatally and was skipped.
#[derive(Debug, Clone)]
pub struct QuarantinedPage {
    /// Which stream.
    pub stream: String,
    /// The serialized paginator state that kept failing.
    pub page_state: String,
    /// How many consecutive attempts failed.
    pub attempts: u32,
    /// The status code the upstream returned.
    pub status_code: Option<u16>,
    /// The start of the failing response body.
    pub body_excerpt: String,
    /// When it was quarantined.
    pub at: Timestamp,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extract::{RecordSpec, RecordsConfig};
    use serde_json::json;

    fn spec() -> RecordSpec {
        RecordSpec::from_config(&RecordsConfig {
            path: "$".to_owned(),
            primary_key: "$.id".to_owned(),
            cursor: None,
        })
        .expect("valid spec")
    }

    fn records(body: &serde_json::Value, at: i64) -> Vec<Record> {
        spec()
            .extract(body, Timestamp::from_secs(at))
            .expect("records extract")
    }

    fn t(secs: i64) -> Timestamp {
        Timestamp::from_secs(secs)
    }

    async fn store() -> Store {
        Store::open_in_memory().expect("in-memory store opens")
    }

    #[tokio::test]
    async fn a_fresh_store_has_a_blank_checkpoint() {
        let store = store().await;
        let checkpoint = store.checkpoint("issues").await.expect("query works");
        assert_eq!(checkpoint, Checkpoint::default());
    }

    #[tokio::test]
    async fn writing_a_page_stores_records_and_the_resume_point_together() {
        let store = store().await;
        let outcome = store
            .write_page(PageWrite {
                stream: "issues".to_owned(),
                records: records(&json!([{ "id": 1 }, { "id": 2 }]), 100),
                progress: Progress::Resume(r#"{"kind":"page","page":2}"#.to_owned()),
                at: t(100),
            })
            .await
            .expect("page writes");

        assert_eq!(outcome.new, 2);
        assert_eq!(outcome.changed, 0);
        assert!(!outcome.saw_known_key);

        let checkpoint = store.checkpoint("issues").await.expect("query works");
        assert_eq!(
            checkpoint.page_state.as_deref(),
            Some(r#"{"kind":"page","page":2}"#)
        );
        assert_eq!(checkpoint.records_seen, 2);
        assert_eq!(store.record_count("issues").await.expect("count"), 2);
    }

    #[tokio::test]
    async fn re_seeing_a_record_is_distinguished_from_it_changing() {
        let store = store().await;
        let page = |body: serde_json::Value| PageWrite {
            stream: "issues".to_owned(),
            records: records(&body, 100),
            progress: Progress::Resume("mid-walk".to_owned()),
            at: t(100),
        };

        store
            .write_page(page(json!([{ "id": 1, "title": "a" }])))
            .await
            .expect("first write");

        let unchanged = store
            .write_page(page(json!([{ "id": 1, "title": "a" }])))
            .await
            .expect("second write");
        assert_eq!(unchanged.unchanged, 1);
        assert_eq!(unchanged.changed, 0);
        assert!(unchanged.saw_known_key);

        let changed = store
            .write_page(page(json!([{ "id": 1, "title": "b" }])))
            .await
            .expect("third write");
        assert_eq!(changed.changed, 1);
        assert_eq!(changed.unchanged, 0);

        // Still one row: the mirror upserts, it does not accumulate versions.
        assert_eq!(store.record_count("issues").await.expect("count"), 1);
    }

    #[tokio::test]
    async fn a_replayed_page_is_idempotent() {
        let store = store().await;
        let page = || PageWrite {
            stream: "issues".to_owned(),
            records: records(&json!([{ "id": 1 }, { "id": 2 }]), 100),
            progress: Progress::Resume("state".to_owned()),
            at: t(100),
        };
        store.write_page(page()).await.expect("first write");
        store.write_page(page()).await.expect("replay");
        assert_eq!(store.record_count("issues").await.expect("count"), 2);
    }

    #[tokio::test]
    async fn the_final_page_commits_data_and_completion_together() {
        let store = store().await;
        store
            .write_page(PageWrite {
                stream: "issues".to_owned(),
                records: records(&json!([{ "id": 1 }]), 100),
                progress: Progress::Complete {
                    cursor_val: Some("2024-01-01T00:00:00Z".to_owned()),
                    success_at: t(150),
                },
                at: t(150),
            })
            .await
            .expect("final page writes");

        let checkpoint = store.checkpoint("issues").await.expect("query works");
        assert_eq!(checkpoint.page_state, None);
        assert_eq!(
            checkpoint.cursor_val.as_deref(),
            Some("2024-01-01T00:00:00Z")
        );
        assert_eq!(checkpoint.last_success_at, Some(t(150)));
        assert_eq!(store.record_count("issues").await.expect("count"), 1);
    }

    #[tokio::test]
    async fn a_completing_page_never_walks_the_high_water_mark_backwards() {
        let store = store().await;
        store
            .write_page(PageWrite {
                stream: "issues".to_owned(),
                records: Vec::new(),
                progress: Progress::Complete {
                    cursor_val: Some("2024-06-01T00:00:00Z".to_owned()),
                    success_at: t(100),
                },
                at: t(100),
            })
            .await
            .expect("first walk");

        // A later walk that advanced no cursor leaves the mark where it was.
        store
            .write_page(PageWrite {
                stream: "issues".to_owned(),
                records: Vec::new(),
                progress: Progress::Complete {
                    cursor_val: None,
                    success_at: t(200),
                },
                at: t(200),
            })
            .await
            .expect("second walk");

        let checkpoint = store.checkpoint("issues").await.expect("query works");
        assert_eq!(
            checkpoint.cursor_val.as_deref(),
            Some("2024-06-01T00:00:00Z")
        );
        assert_eq!(checkpoint.last_success_at, Some(t(200)));
    }

    #[tokio::test]
    async fn finishing_a_walk_clears_the_resume_point_and_advances_freshness() {
        let store = store().await;
        store
            .write_page(PageWrite {
                stream: "issues".to_owned(),
                records: records(&json!([{ "id": 1 }]), 100),
                progress: Progress::Resume("mid-walk".to_owned()),
                at: t(100),
            })
            .await
            .expect("page writes");

        store
            .finish_walk(
                "issues",
                Some("2024-01-01T00:00:00Z".to_owned()),
                Some(t(200)),
            )
            .await
            .expect("walk finishes");

        let checkpoint = store.checkpoint("issues").await.expect("query works");
        assert_eq!(checkpoint.page_state, None);
        assert_eq!(
            checkpoint.cursor_val.as_deref(),
            Some("2024-01-01T00:00:00Z")
        );
        assert_eq!(checkpoint.last_success_at, Some(t(200)));
    }

    #[tokio::test]
    async fn a_not_modified_run_advances_freshness_without_touching_the_cursor() {
        let store = store().await;
        store
            .finish_walk("releases", Some("cursor-1".to_owned()), Some(t(100)))
            .await
            .expect("first walk");
        store
            .finish_walk("releases", None, Some(t(200)))
            .await
            .expect("304 run");

        let checkpoint = store.checkpoint("releases").await.expect("query works");
        assert_eq!(checkpoint.cursor_val.as_deref(), Some("cursor-1"));
        assert_eq!(checkpoint.last_success_at, Some(t(200)));
    }

    #[tokio::test]
    async fn the_learned_budget_survives_a_restart() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("mirror.db");

        let snapshot = BudgetSnapshot {
            observed_limit: Some(5_000),
            window_secs: Some(3_600),
            remaining: Some(4_211),
            reset_at: Some(t(9_000)),
            ratcheted_at: None,
        };
        {
            let store = Store::open(&path).expect("store opens");
            store
                .save_budget("github", snapshot, t(100))
                .await
                .expect("budget saves");
        }

        let reopened = Store::open(&path).expect("store reopens");
        let loaded = reopened
            .load_budget("github")
            .await
            .expect("budget loads")
            .expect("budget exists");
        assert_eq!(loaded, snapshot);
    }

    #[tokio::test]
    async fn validators_round_trip() {
        let store = store().await;
        assert_eq!(store.validator("key").await.expect("query"), None);

        store
            .save_validator(
                "key",
                Validator {
                    etag: Some("\"abc\"".to_owned()),
                    last_modified: None,
                },
                t(10),
            )
            .await
            .expect("validator saves");

        let loaded = store
            .validator("key")
            .await
            .expect("query")
            .expect("exists");
        assert_eq!(loaded.etag.as_deref(), Some("\"abc\""));
        assert!(!loaded.is_empty());
    }

    #[tokio::test]
    async fn quarantined_pages_are_counted_and_recognised() {
        let store = store().await;
        let page = QuarantinedPage {
            stream: "issues".to_owned(),
            page_state: r#"{"kind":"page","page":47}"#.to_owned(),
            attempts: 3,
            status_code: Some(422),
            body_excerpt: "bad cursor".to_owned(),
            at: t(500),
        };
        store
            .quarantine_page(page.clone())
            .await
            .expect("quarantine");

        assert!(store
            .is_quarantined("issues", &page.page_state)
            .await
            .expect("query"));
        assert!(!store
            .is_quarantined("issues", "{\"kind\":\"start\"}")
            .await
            .expect("query"));
        assert_eq!(store.quarantined_count("issues").await.expect("count"), 1);

        // Quarantining the same page again accumulates attempts rather than
        // creating a second row.
        store.quarantine_page(page).await.expect("re-quarantine");
        assert_eq!(store.quarantined_count("issues").await.expect("count"), 1);
    }

    #[tokio::test]
    async fn runs_record_how_they_ended() {
        let store = store().await;
        let id = store
            .begin_run("issues", t(100), 1.5)
            .await
            .expect("run begins");
        store
            .finish_run(RunRecord {
                id,
                ended_at: t(160),
                status: RunStatus::Ok,
                pages: 3,
                records: 250,
                records_new: 12,
                records_changed: 4,
                not_modified: 0,
                requests: 3,
                stop_reason: Some("short_page".to_owned()),
                error: None,
            })
            .await
            .expect("run finishes");

        let last = store
            .last_run("issues")
            .await
            .expect("query")
            .expect("run exists");
        assert_eq!(last.status.as_deref(), Some("ok"));
        assert_eq!(last.ended_at, Some(t(160)));
        assert_eq!(last.error, None);
    }

    #[tokio::test]
    async fn first_run_at_marks_when_a_stream_started_existing() {
        let store = store().await;
        assert_eq!(store.first_run_at("issues").await.expect("query"), None);

        store.begin_run("issues", t(500), 0.0).await.expect("run 1");
        store.begin_run("issues", t(900), 0.0).await.expect("run 2");
        assert_eq!(
            store.first_run_at("issues").await.expect("query"),
            Some(t(500))
        );
    }

    #[tokio::test]
    async fn successes_exclude_failures() {
        let store = store().await;
        for (at, status) in [
            (100, RunStatus::Ok),
            (200, RunStatus::Failed),
            (300, RunStatus::NotModified),
            (400, RunStatus::Partial),
        ] {
            let id = store
                .begin_run("issues", t(at), 0.0)
                .await
                .expect("run begins");
            store
                .finish_run(RunRecord {
                    id,
                    ended_at: t(at),
                    status,
                    pages: 1,
                    records: 0,
                    records_new: 0,
                    records_changed: 0,
                    not_modified: 0,
                    requests: 1,
                    stop_reason: None,
                    error: None,
                })
                .await
                .expect("run finishes");
        }

        let successes = store
            .successes_since("issues", t(0))
            .await
            .expect("query works");
        assert_eq!(successes, vec![t(100), t(300), t(400)]);
    }

    #[tokio::test]
    async fn checkpoints_returns_every_stream() {
        let store = store().await;
        store
            .finish_walk("a", None, Some(t(10)))
            .await
            .expect("walk a");
        store
            .finish_walk("b", None, Some(t(20)))
            .await
            .expect("walk b");
        let all = store.checkpoints().await.expect("query works");
        assert_eq!(all.len(), 2);
        assert_eq!(all.get("b").and_then(|c| c.last_success_at), Some(t(20)));
    }

    #[tokio::test]
    async fn a_newer_schema_is_refused_rather_than_corrupted() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("mirror.db");
        {
            let conn = Connection::open(&path).expect("opens");
            conn.pragma_update(None, "user_version", schema::SCHEMA_VERSION + 1)
                .expect("bump version");
        }
        let err = Store::open(&path).expect_err("should refuse");
        assert!(matches!(err, StoreError::SchemaTooNew { .. }));
    }
}

/// The mirror, whichever target holds it.
///
/// The store target is chosen once at startup from a config file, so it is a
/// runtime value rather than a type parameter: making `App` generic would push
/// `S` through every command signature for a choice that is made exactly once
/// and never varies within a process.
#[derive(Debug, Clone)]
pub enum Mirror {
    /// A local SQLite file.
    Sqlite(Store),
    /// A Postgres server.
    ///
    /// Behind an `Arc` so `Mirror` stays cheap to clone the way `Store` is:
    /// `PgStore` owns a connection behind a `Mutex` and cannot be duplicated,
    /// and the worker takes its store by value.
    Postgres(Arc<postgres::PgStore>),
}

impl Mirror {
    /// Every stored checkpoint, keyed by stream.
    ///
    /// # Errors
    ///
    /// Fails if the query does.
    pub async fn checkpoints(&self) -> Result<BTreeMap<String, Checkpoint>, StoreError> {
        match self {
            Self::Sqlite(store) => store.checkpoints().await,
            Self::Postgres(store) => store.checkpoints().await,
        }
    }

    /// The checkpoint for one stream, or a blank one if it has never run.
    ///
    /// Inherent rather than only a trait method so callers that hold a concrete
    /// `Mirror` do not have to disambiguate between the two traits that both
    /// declare it.
    ///
    /// # Errors
    ///
    /// Fails if the query does.
    pub async fn checkpoint(&self, stream: &str) -> Result<Checkpoint, StoreError> {
        crate::sync::SyncStore::checkpoint(self, stream).await
    }

    /// How many pages of this stream are quarantined.
    ///
    /// # Errors
    ///
    /// Fails if the query does.
    pub async fn quarantined_count(&self, stream: &str) -> Result<u32, StoreError> {
        crate::sync::SyncStore::quarantined_count(self, stream).await
    }

    /// How many records are stored for a stream.
    ///
    /// # Errors
    ///
    /// Fails if the query does.
    pub async fn record_count(&self, stream: &str) -> Result<u64, StoreError> {
        match self {
            Self::Sqlite(store) => store.record_count(stream).await,
            Self::Postgres(store) => store.record_count(stream).await,
        }
    }

    /// The learned budget for a source, if one was ever persisted.
    ///
    /// # Errors
    ///
    /// Fails if the query does.
    pub async fn load_budget(&self, source: &str) -> Result<Option<BudgetSnapshot>, StoreError> {
        match self {
            Self::Sqlite(store) => store.load_budget(source).await,
            Self::Postgres(store) => store.load_budget(source).await,
        }
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
        match self {
            Self::Sqlite(store) => store.save_budget(source, snapshot, at).await,
            Self::Postgres(store) => store.save_budget(source, snapshot, at).await,
        }
    }
}

impl crate::sync::SyncStore for Mirror {
    async fn checkpoint(&self, stream: &str) -> Result<Checkpoint, StoreError> {
        match self {
            Self::Sqlite(s) => crate::sync::SyncStore::checkpoint(s, stream).await,
            Self::Postgres(s) => crate::sync::SyncStore::checkpoint(&**s, stream).await,
        }
    }

    async fn write_page(&self, page: PageWrite) -> Result<PageOutcome, StoreError> {
        match self {
            Self::Sqlite(s) => crate::sync::SyncStore::write_page(s, page).await,
            Self::Postgres(s) => crate::sync::SyncStore::write_page(&**s, page).await,
        }
    }

    async fn finish_walk(
        &self,
        stream: &str,
        cursor_val: Option<String>,
        success_at: Option<Timestamp>,
    ) -> Result<(), StoreError> {
        match self {
            Self::Sqlite(s) => {
                crate::sync::SyncStore::finish_walk(s, stream, cursor_val, success_at).await
            }
            Self::Postgres(s) => {
                crate::sync::SyncStore::finish_walk(&**s, stream, cursor_val, success_at).await
            }
        }
    }

    async fn begin_run(
        &self,
        stream: &str,
        started_at: Timestamp,
        deficit: f64,
    ) -> Result<i64, StoreError> {
        match self {
            Self::Sqlite(s) => {
                crate::sync::SyncStore::begin_run(s, stream, started_at, deficit).await
            }
            Self::Postgres(s) => {
                crate::sync::SyncStore::begin_run(&**s, stream, started_at, deficit).await
            }
        }
    }

    async fn finish_run(&self, run: RunRecord) -> Result<(), StoreError> {
        match self {
            Self::Sqlite(s) => crate::sync::SyncStore::finish_run(s, run).await,
            Self::Postgres(s) => crate::sync::SyncStore::finish_run(&**s, run).await,
        }
    }

    async fn validator(&self, request_key: &str) -> Result<Option<Validator>, StoreError> {
        match self {
            Self::Sqlite(s) => crate::sync::SyncStore::validator(s, request_key).await,
            Self::Postgres(s) => crate::sync::SyncStore::validator(&**s, request_key).await,
        }
    }

    async fn save_validator(
        &self,
        request_key: &str,
        validator: Validator,
        at: Timestamp,
    ) -> Result<(), StoreError> {
        match self {
            Self::Sqlite(s) => {
                crate::sync::SyncStore::save_validator(s, request_key, validator, at).await
            }
            Self::Postgres(s) => {
                crate::sync::SyncStore::save_validator(&**s, request_key, validator, at).await
            }
        }
    }

    async fn quarantine_page(&self, page: QuarantinedPage) -> Result<(), StoreError> {
        match self {
            Self::Sqlite(s) => crate::sync::SyncStore::quarantine_page(s, page).await,
            Self::Postgres(s) => crate::sync::SyncStore::quarantine_page(&**s, page).await,
        }
    }

    async fn is_quarantined(&self, stream: &str, page_state: &str) -> Result<bool, StoreError> {
        match self {
            Self::Sqlite(s) => crate::sync::SyncStore::is_quarantined(s, stream, page_state).await,
            Self::Postgres(s) => {
                crate::sync::SyncStore::is_quarantined(&**s, stream, page_state).await
            }
        }
    }

    async fn quarantined_count(&self, stream: &str) -> Result<u32, StoreError> {
        match self {
            Self::Sqlite(s) => crate::sync::SyncStore::quarantined_count(s, stream).await,
            Self::Postgres(s) => crate::sync::SyncStore::quarantined_count(&**s, stream).await,
        }
    }

    async fn any_known_key(&self, stream: &str, pks: Vec<String>) -> Result<bool, StoreError> {
        match self {
            Self::Sqlite(s) => crate::sync::SyncStore::any_known_key(s, stream, pks).await,
            Self::Postgres(s) => crate::sync::SyncStore::any_known_key(&**s, stream, pks).await,
        }
    }
}

impl crate::cli::status::ReportStore for Mirror {
    async fn checkpoint(&self, stream: &str) -> Result<Checkpoint, StoreError> {
        crate::sync::SyncStore::checkpoint(self, stream).await
    }

    async fn first_run_at(&self, stream: &str) -> Result<Option<Timestamp>, StoreError> {
        match self {
            Self::Sqlite(s) => crate::cli::status::ReportStore::first_run_at(s, stream).await,
            Self::Postgres(s) => crate::cli::status::ReportStore::first_run_at(&**s, stream).await,
        }
    }

    async fn successes_since(
        &self,
        stream: &str,
        since: Timestamp,
    ) -> Result<Vec<Timestamp>, StoreError> {
        match self {
            Self::Sqlite(s) => {
                crate::cli::status::ReportStore::successes_since(s, stream, since).await
            }
            Self::Postgres(s) => {
                crate::cli::status::ReportStore::successes_since(&**s, stream, since).await
            }
        }
    }

    async fn quarantined_count(&self, stream: &str) -> Result<u32, StoreError> {
        crate::sync::SyncStore::quarantined_count(self, stream).await
    }

    async fn last_run(&self, stream: &str) -> Result<Option<RunSummary>, StoreError> {
        match self {
            Self::Sqlite(s) => crate::cli::status::ReportStore::last_run(s, stream).await,
            Self::Postgres(s) => crate::cli::status::ReportStore::last_run(&**s, stream).await,
        }
    }
}
