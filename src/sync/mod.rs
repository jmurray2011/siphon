//! The worker that drives one stream: paginate, fetch, extract, write.
//!
//! Everything here is per-stream and failure-isolated. A fatal error on one
//! stream must not stop the others, so upstream failures come back inside the
//! [`RunReport`] rather than as an `Err`; only a broken database is an error,
//! because at that point nothing can proceed.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, info, warn};

use crate::clock::{Clock, Timestamp};
use crate::config::Stream;
use crate::extract::{CursorValue, Record};
use crate::governor::Governor;
use crate::http::{HttpError, Response, ResponseClass, SourceClient};
use crate::pagination::{Advance, PageOutcome, PageState, StopReason};
use crate::store::{PageWrite, Progress, QuarantinedPage, RunRecord, RunStatus, Store, StoreError};

/// How many times one page will re-wait on a rate limit before the run gives up.
///
/// Rate limiting does not count against the retry budget, but it cannot be
/// unbounded either: an API that 429s every request would otherwise hold a
/// worker forever while its deficit tells the governor to keep funding it.
const MAX_RATE_LIMIT_WAITS: u32 = 10;

/// Pause between attempts at a page that failed fatally, before quarantine.
const FATAL_RETRY_PAUSE: Duration = Duration::from_millis(250);

/// How many pages in a row may be quarantined before the run gives up.
///
/// Stepping over one poisoned page is the point of quarantine. Stepping over
/// every page is not: a positional paginator can always compute a next offset,
/// so an endpoint that fails wholesale would otherwise be quarantined and
/// skipped forever, and `max_pages` would never see it because a skipped page
/// is not a walked one. Three consecutive failures says the endpoint is broken,
/// not the page.
const MAX_CONSECUTIVE_QUARANTINES: u32 = 3;

/// Why a run could not even be attempted.
#[derive(Debug, thiserror::Error)]
pub enum SyncError {
    /// The database failed.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// A request could not be built.
    #[error(transparent)]
    Http(#[from] HttpError),
    /// The stream names a source with no client.
    #[error("no client for source `{0}`")]
    UnknownSource(String),
    /// A paginator state could not be serialized for the checkpoint.
    #[error("cannot serialize paginator state")]
    PageState(#[from] serde_json::Error),
}

/// How a run was started.
#[derive(Debug, Clone, Copy, Default)]
pub struct RunOptions {
    /// The stream's deficit when it was admitted, recorded in the run log.
    pub deficit: f64,
    /// Ignore the stored cursor and resume point, and walk from the start.
    pub full: bool,
}

/// What a run did.
#[derive(Debug, Clone)]
pub struct RunReport {
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
    /// Requests spent.
    pub requests: u32,
    /// Pages quarantined during this run.
    pub quarantined: u32,
    /// Why the walk stopped.
    pub stop_reason: Option<String>,
    /// The failure, if it failed.
    pub error: Option<String>,
    /// When the stream was confirmed current, if it was.
    pub success_at: Option<Timestamp>,
}

impl RunReport {
    fn new() -> Self {
        Self {
            status: RunStatus::Failed,
            pages: 0,
            records: 0,
            records_new: 0,
            records_changed: 0,
            not_modified: 0,
            requests: 0,
            quarantined: 0,
            stop_reason: None,
            error: None,
            success_at: None,
        }
    }
}

/// What `siphon test` found without writing anything.
#[derive(Debug)]
pub struct ProbeReport {
    /// The records that would have been written.
    pub records: Vec<Record>,
    /// Pages fetched.
    pub pages: u32,
    /// The URLs that were requested.
    pub urls: Vec<String>,
    /// Why the walk stopped, if it did.
    pub stop_reason: Option<StopReason>,
    /// The failure, if there was one.
    pub error: Option<String>,
}

/// Drives streams against their sources.
pub struct SyncWorker {
    store: Store,
    governor: Arc<Governor>,
    clock: Arc<dyn Clock>,
    clients: BTreeMap<String, Arc<SourceClient>>,
}

impl SyncWorker {
    /// Builds a worker over the given source clients.
    #[must_use]
    pub fn new(
        store: Store,
        governor: Arc<Governor>,
        clock: Arc<dyn Clock>,
        clients: BTreeMap<String, Arc<SourceClient>>,
    ) -> Self {
        Self {
            store,
            governor,
            clock,
            clients,
        }
    }

    fn client_for(&self, stream: &Stream) -> Result<&Arc<SourceClient>, SyncError> {
        self.clients
            .get(&stream.source)
            .ok_or_else(|| SyncError::UnknownSource(stream.source.clone()))
    }

    /// Runs one stream to completion, resuming mid-pagination if it was
    /// interrupted.
    ///
    /// # Errors
    ///
    /// Fails only when the database or the request builder fails. Everything the
    /// upstream does is reported in the [`RunReport`].
    pub async fn run(
        &self,
        stream: &Arc<Stream>,
        options: RunOptions,
    ) -> Result<RunReport, SyncError> {
        let client = Arc::clone(self.client_for(stream)?);
        let started_at = self.clock.now();
        let run_id = self
            .store
            .begin_run(&stream.name, started_at, options.deficit)
            .await?;

        let report = self.walk(stream, &client, options).await;
        let report = match report {
            Ok(report) => report,
            Err(SyncError::Store(e)) => return Err(SyncError::Store(e)),
            Err(other) => {
                let mut report = RunReport::new();
                report.error = Some(other.to_string());
                report
            }
        };

        self.store
            .finish_run(RunRecord {
                id: run_id,
                ended_at: self.clock.now(),
                status: report.status,
                pages: report.pages,
                records: report.records,
                records_new: report.records_new,
                records_changed: report.records_changed,
                not_modified: report.not_modified,
                requests: report.requests,
                stop_reason: report.stop_reason.clone(),
                error: report.error.clone(),
            })
            .await?;

        Ok(report)
    }

    #[allow(clippy::too_many_lines)] // The walk is one story; splitting it hides the control flow.
    async fn walk(
        &self,
        stream: &Arc<Stream>,
        client: &Arc<SourceClient>,
        options: RunOptions,
    ) -> Result<RunReport, SyncError> {
        let mut report = RunReport::new();
        let source = client.source();
        let checkpoint = self.store.checkpoint(&stream.name).await?;

        // A resumed walk continues from the stored paginator state; `--full`
        // deliberately throws it away and starts over.
        let resumed = if options.full {
            None
        } else {
            checkpoint
                .page_state
                .as_deref()
                .and_then(|raw| serde_json::from_str::<PageState>(raw).ok())
        };
        if resumed.is_some() {
            debug!(stream = %stream.name, "resuming mid-pagination");
        }
        let mut state = resumed
            .clone()
            .unwrap_or_else(|| stream.paginator.initial());

        // The incremental cursor is fixed for the whole walk: it is the mark the
        // *previous* walk finished on, and it is what makes this walk cheap. It
        // is rendered in whatever shape the upstream accepts, which is often
        // narrower than the shape it emits.
        let cursor_param_value = if options.full || !stream.is_incremental() {
            None
        } else {
            outgoing_cursor(stream, checkpoint.cursor_val.as_deref())
        };
        let mut high_water = stream
            .records
            .cursor()
            .zip(checkpoint.cursor_val.as_deref())
            .and_then(|(spec, raw)| CursorValue::parse(raw, spec.kind()));

        let mut conditional = resumed.is_none();
        let mut truncated = false;
        let mut consecutive_quarantines = 0u32;

        loop {
            let state_key = state.to_key()?;
            if self.store.is_quarantined(&stream.name, &state_key).await? {
                warn!(
                    stream = %stream.name,
                    state = %state_key,
                    "stopping at a quarantined page; the mirror has a known gap"
                );
                report.stop_reason = Some("quarantined".to_owned());
                truncated = true;
                break;
            }

            let page_request = stream.paginator.request_for(&state);
            let url = client.url_for(stream, &page_request, cursor_param_value.as_deref())?;

            let fetched = self
                .fetch_page(stream, client, &url, conditional, &mut report)
                .await?;
            conditional = false;

            let response = match fetched {
                Fetched::Response(response) => response,
                Fetched::GiveUp(message) => {
                    report.status = RunStatus::Failed;
                    report.error = Some(message);
                    return Ok(report);
                }
                Fetched::Poisoned {
                    status_code,
                    attempts,
                    body_excerpt,
                    detail,
                } => {
                    self.store
                        .quarantine_page(QuarantinedPage {
                            stream: stream.name.clone(),
                            page_state: state_key.clone(),
                            attempts,
                            status_code,
                            body_excerpt,
                            at: self.clock.now(),
                        })
                        .await?;
                    report.quarantined += 1;
                    truncated = true;
                    consecutive_quarantines += 1;
                    warn!(
                        stream = %stream.name,
                        state = %state_key,
                        detail = %detail,
                        "quarantined a page after repeated fatal failures"
                    );

                    match stream.paginator.skip(&state) {
                        Some(next) if consecutive_quarantines < MAX_CONSECUTIVE_QUARANTINES => {
                            state = next;
                            continue;
                        }
                        Some(_) => {
                            warn!(
                                stream = %stream.name,
                                quarantined = consecutive_quarantines,
                                "consecutive pages all failed; the endpoint looks broken"
                            );
                            report.stop_reason = Some("endpoint_failing".to_owned());
                            break;
                        }
                        None => {
                            report.stop_reason = Some("quarantined".to_owned());
                            break;
                        }
                    }
                }
            };

            if response.class == ResponseClass::NotModified {
                report.not_modified += 1;
                let at = self.clock.now();
                self.store.finish_walk(&stream.name, None, Some(at)).await?;
                report.status = RunStatus::NotModified;
                report.success_at = Some(at);
                report.stop_reason = Some("not_modified".to_owned());
                info!(stream = %stream.name, "confirmed unchanged");
                return Ok(report);
            }

            if source.conditional_requests && !response.validator.is_empty() {
                self.store
                    .save_validator(url.as_str(), response.validator.clone(), self.clock.now())
                    .await?;
            }

            let body = response.body_or_null();
            let records = match stream.records.extract(&body, self.clock.now()) {
                Ok(records) => records,
                Err(error) => {
                    // A record siphon cannot key is a page-level fault, handled
                    // by the same bounded machinery as a 422: quarantine it, so
                    // the gap is visible and the stream does not block forever
                    // on a page it will never parse.
                    self.store
                        .quarantine_page(QuarantinedPage {
                            stream: stream.name.clone(),
                            page_state: state_key.clone(),
                            attempts: 1,
                            status_code: response.status,
                            body_excerpt: error.to_string(),
                            at: self.clock.now(),
                        })
                        .await?;
                    report.quarantined += 1;
                    truncated = true;
                    consecutive_quarantines += 1;
                    warn!(stream = %stream.name, %error, "quarantined an unextractable page");
                    match stream.paginator.skip(&state) {
                        Some(next) if consecutive_quarantines < MAX_CONSECUTIVE_QUARANTINES => {
                            state = next;
                            continue;
                        }
                        _ => {
                            report.stop_reason = Some("unextractable".to_owned());
                            break;
                        }
                    }
                }
            };

            if stream.is_incremental() {
                for record in &records {
                    if let Some(value) = record.cursor() {
                        high_water = Some(match high_water {
                            Some(current) => current.max(value.clone()),
                            None => value.clone(),
                        });
                    }
                }
            }

            // Where the walk goes next has to be decided before the write, so
            // that the records and the resume point past them land in one
            // transaction. Everything the decision needs is in the response -
            // except `stop_on_seen_key`, which by definition needs the store.
            let saw_known_key = if stream.paginator.stops_on_seen_key() {
                let pks = records.iter().map(|r| r.pk().to_owned()).collect();
                self.store.any_known_key(&stream.name, pks).await?
            } else {
                false
            };

            let outcome = PageOutcome {
                records: records.len(),
                body: &body,
                link_header: response.link_header.as_deref(),
                saw_known_key,
            };
            let advance = stream.paginator.advance(&state, &outcome, report.pages + 1);

            let at = self.clock.now();
            let progress = match &advance {
                Advance::Next(next) => Progress::Resume(next.to_key()?),
                Advance::Done(_) => Progress::Complete {
                    cursor_val: high_water.as_ref().map(|c| c.as_str().to_owned()),
                    success_at: at,
                },
            };

            let written = self
                .store
                .write_page(PageWrite {
                    stream: stream.name.clone(),
                    records,
                    progress,
                    at,
                })
                .await?;

            report.pages += 1;
            report.records += written.total();
            report.records_new += written.new;
            report.records_changed += written.changed;
            consecutive_quarantines = 0;

            match advance {
                Advance::Next(next) => state = next,
                Advance::Done(reason) => {
                    report.success_at = Some(at);
                    report.stop_reason = Some(reason.as_str().to_owned());
                    truncated |= reason.is_truncation();
                    break;
                }
            }
        }

        // A walk that stopped early still leaves the mirror current as of now
        // for the pages it did cover. Not advancing the clock here would grow
        // the deficit without bound, and the governor would then preferentially
        // fund the one stream guaranteed to waste the budget.
        if report.success_at.is_none() {
            let at = self.clock.now();
            self.store
                .finish_walk(
                    &stream.name,
                    high_water.as_ref().map(|c| c.as_str().to_owned()),
                    Some(at),
                )
                .await?;
            report.success_at = Some(at);
        }

        let quarantined_total = self.store.quarantined_count(&stream.name).await?;
        report.status = if truncated || quarantined_total > 0 {
            RunStatus::Partial
        } else {
            RunStatus::Ok
        };
        Ok(report)
    }

    /// Fetches one page, absorbing rate limits and retryable failures.
    async fn fetch_page(
        &self,
        stream: &Arc<Stream>,
        client: &Arc<SourceClient>,
        url: &url::Url,
        conditional: bool,
        report: &mut RunReport,
    ) -> Result<Fetched, SyncError> {
        let source = client.source();
        let retry = source.retry;
        let validator = if conditional && source.conditional_requests {
            self.store.validator(url.as_str()).await?
        } else {
            None
        };

        let mut attempt = 1u32;
        let mut rate_limit_waits = 0u32;
        let mut fatal_attempts = 0u32;

        loop {
            self.governor
                .acquire(&stream.source, stream.priority)
                .await
                .map_err(|e| SyncError::UnknownSource(e.to_string()))?;

            let response = client
                .fetch(
                    url,
                    &stream.request.headers,
                    validator.as_ref(),
                    self.clock.now(),
                )
                .await?;
            report.requests += 1;

            match response.class {
                ResponseClass::Success | ResponseClass::NotModified => {
                    self.governor.observe(&stream.source, &response.limits);
                    return Ok(Fetched::Response(response));
                }

                ResponseClass::RateLimited => {
                    // Not counted against the retry budget: being rate limited
                    // is the budget working, not the request failing.
                    self.governor
                        .observe_rate_limited(&stream.source, &response.limits);
                    rate_limit_waits += 1;
                    warn!(
                        stream = %stream.name,
                        status = response.status.unwrap_or_default(),
                        waits = rate_limit_waits,
                        "rate limited; ratcheting the budget down"
                    );
                    if rate_limit_waits >= MAX_RATE_LIMIT_WAITS {
                        return Ok(Fetched::GiveUp(format!(
                            "rate limited {rate_limit_waits} times in a row"
                        )));
                    }
                }

                ResponseClass::Retryable => {
                    // No status at all means nothing was served: a reset, a
                    // timeout or a DNS blip. The slot reserved for it was never
                    // spent, and a transport failure carries no headers to
                    // correct the tally later, so hand it back here.
                    if response.status.is_none() {
                        self.governor.refund(&stream.source);
                    }
                    self.governor.observe(&stream.source, &response.limits);
                    if attempt >= retry.max_attempts {
                        return Ok(Fetched::GiveUp(
                            response
                                .detail
                                .unwrap_or_else(|| "retries exhausted".to_owned()),
                        ));
                    }
                    let backoff = retry.backoff(attempt, jitter_unit());
                    debug!(
                        stream = %stream.name,
                        attempt,
                        backoff_ms = backoff.as_millis(),
                        "retrying after a transient failure"
                    );
                    attempt += 1;
                    tokio::time::sleep(backoff).await;
                }

                ResponseClass::Fatal => {
                    self.governor.observe(&stream.source, &response.limits);
                    fatal_attempts += 1;
                    if fatal_attempts >= stream.quarantine_after {
                        return Ok(Fetched::Poisoned {
                            status_code: response.status,
                            attempts: fatal_attempts,
                            body_excerpt: response.body_excerpt,
                            detail: response
                                .detail
                                .unwrap_or_else(|| "fatal response".to_owned()),
                        });
                    }
                    tokio::time::sleep(FATAL_RETRY_PAUSE).await;
                }
            }
        }
    }

    /// Fetches up to `max_pages` pages and returns what would have been written.
    ///
    /// Writes nothing: this is what `siphon test` uses to check a config against
    /// a real endpoint before trusting it with the database.
    ///
    /// # Errors
    ///
    /// Fails if a request cannot be built or the governor does not know the source.
    pub async fn probe(
        &self,
        stream: &Arc<Stream>,
        max_pages: u32,
    ) -> Result<ProbeReport, SyncError> {
        let client = Arc::clone(self.client_for(stream)?);
        let mut state = stream.paginator.initial();
        let mut report = ProbeReport {
            records: Vec::new(),
            pages: 0,
            urls: Vec::new(),
            stop_reason: None,
            error: None,
        };

        // A probe that skipped the cursor would issue a request the real sync
        // never makes - and for a `{{ cursor }}` stream it would send the
        // placeholder verbatim, which is exactly the mistake `siphon test`
        // exists to catch before it reaches the database.
        let cursor = outgoing_cursor(stream, None);

        while report.pages < max_pages {
            let page_request = stream.paginator.request_for(&state);
            let url = client.url_for(stream, &page_request, cursor.as_deref())?;
            report.urls.push(url.to_string());

            self.governor
                .acquire(&stream.source, stream.priority)
                .await
                .map_err(|e| SyncError::UnknownSource(e.to_string()))?;
            let response = client
                .fetch(&url, &stream.request.headers, None, self.clock.now())
                .await?;
            self.governor.observe(&stream.source, &response.limits);
            report.pages += 1;

            if response.class != ResponseClass::Success {
                report.error = Some(
                    response
                        .detail
                        .unwrap_or_else(|| format!("{:?} response", response.class)),
                );
                return Ok(report);
            }

            let body = response.body_or_null();
            let records = match stream.records.extract(&body, self.clock.now()) {
                Ok(records) => records,
                Err(error) => {
                    report.error = Some(error.to_string());
                    return Ok(report);
                }
            };

            let outcome = PageOutcome {
                records: records.len(),
                body: &body,
                link_header: response.link_header.as_deref(),
                saw_known_key: false,
            };
            report.records.extend(records);

            match stream.paginator.advance(&state, &outcome, report.pages) {
                Advance::Next(next) => state = next,
                Advance::Done(reason) => {
                    report.stop_reason = Some(reason);
                    break;
                }
            }
        }
        Ok(report)
    }
}

/// What one page's fetch loop concluded.
enum Fetched {
    /// A response worth handling.
    Response(Response),
    /// The page failed in a way that ends the run without advancing anything.
    GiveUp(String),
    /// The page failed fatally often enough to be quarantined.
    Poisoned {
        status_code: Option<u16>,
        attempts: u32,
        body_excerpt: String,
        detail: String,
    },
}

/// The mark to send upstream, rendered as the API wants it.
///
/// Falls back to the declared starting mark when nothing is stored yet, which
/// is what makes the first request of a `{{ cursor }}` stream well-formed
/// rather than carrying an unsubstituted placeholder.
fn outgoing_cursor(stream: &Stream, stored: Option<&str>) -> Option<String> {
    let spec = stream.records.cursor()?;
    stored
        .map(|raw| {
            CursorValue::parse(raw, spec.kind())
                .map_or_else(|| raw.to_owned(), |value| value.render(spec))
        })
        .or_else(|| spec.initial().map(str::to_owned))
}

/// A value in `[0, 1)` for full-jitter backoff.
///
/// Backoff jitter needs decorrelation, not unpredictability, so the process's
/// own hasher seed and the current nanosecond are enough - and a dependency on
/// a random number generator is not worth taking for it.
fn jitter_unit() -> f64 {
    use std::hash::{BuildHasher, Hasher};
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.subsec_nanos());
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    hasher.write_u32(nanos);
    let bits = hasher.finish() >> 11;
    bits as f64 / (1u64 << 53) as f64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jitter_stays_in_range_and_moves() {
        let mut seen = std::collections::BTreeSet::new();
        for _ in 0..64 {
            let value = jitter_unit();
            assert!((0.0..1.0).contains(&value), "jitter out of range: {value}");
            seen.insert(value.to_bits());
        }
        assert!(seen.len() > 1, "jitter never varied");
    }
}
