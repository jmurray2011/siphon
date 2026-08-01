//! Admission and pacing: what runs next, and when its next request may go out.
//!
//! There is no separate scheduler component. Scheduling is what the governor
//! does, and it does it in two places:
//!
//! - [`Governor::next_admission`] decides *which* stream should run, from
//!   deficits and priorities ([`policy`]).
//! - [`Governor::acquire`] decides *when* each individual request may go out,
//!   from the measured budget ([`crate::budget`]).
//!
//! Splitting it that way is what lets a `best_effort` backfill yield mid-run:
//! it holds no lock on the source, it just waits longer for its next permit.

pub mod policy;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use tokio::sync::Notify;

use crate::budget::{Budget, BudgetSnapshot, LimitHeaders};
use crate::clock::{Clock, Timestamp};

pub use policy::{attainment, AdmissionPolicy, Decision, Priority, StreamState};

/// Longest a yielding `best_effort` stream waits before re-checking for surplus.
///
/// Surplus normally reappears when an SLO-bearing stream finishes, which wakes
/// waiters directly. The cap is a backstop, not the mechanism.
const SURPLUS_RECHECK: Duration = Duration::from_secs(5);

/// Longest a paced waiter sleeps in one step, so budget changes are picked up.
const MAX_PACING_SLEEP: Duration = Duration::from_secs(30);

/// Why the governor could not answer.
#[derive(Debug, thiserror::Error)]
pub enum GovernorError {
    /// A stream referenced a source that was not registered.
    #[error("unknown source `{0}`")]
    UnknownSource(String),
}

struct Inner {
    budgets: BTreeMap<String, Budget>,
    streams: Vec<StreamState>,
}

/// Owns the budget for every source and the deficit for every stream.
pub struct Governor {
    clock: Arc<dyn Clock>,
    policy: AdmissionPolicy,
    inner: Mutex<Inner>,
    wake: Notify,
}

impl Governor {
    /// Builds a governor over the given sources and streams.
    #[must_use]
    pub fn new(
        clock: Arc<dyn Clock>,
        policy: AdmissionPolicy,
        budgets: BTreeMap<String, Budget>,
        streams: Vec<StreamState>,
    ) -> Self {
        Self {
            clock,
            policy,
            inner: Mutex::new(Inner { budgets, streams }),
            wake: Notify::new(),
        }
    }

    /// The admission policy in force.
    #[must_use]
    pub const fn policy(&self) -> AdmissionPolicy {
        self.policy
    }

    /// Which stream should run next, if any.
    #[must_use]
    pub fn next_admission(&self) -> Decision {
        let inner = self.lock();
        policy::select(&inner.streams, self.clock.now(), self.policy)
    }

    /// Waits until the source's budget allows another request, then charges it.
    ///
    /// A `best_effort` caller additionally waits for surplus: it will not spend
    /// a request while any SLO-bearing stream is at or past the surplus
    /// threshold. This is what makes a backfill yield partway through its walk
    /// rather than only between runs.
    ///
    /// Cancel-safe: dropping the future before it resolves charges nothing.
    ///
    /// # Errors
    ///
    /// Fails if `source` was never registered.
    pub async fn acquire(&self, source: &str, priority: Priority) -> Result<(), GovernorError> {
        loop {
            let wait = {
                let mut inner = self.lock();
                let now = self.clock.now();

                if matches!(priority, Priority::BestEffort)
                    && !self.policy.has_surplus(&inner.streams, now)
                {
                    Some(SURPLUS_RECHECK)
                } else {
                    let budget = inner
                        .budgets
                        .get_mut(source)
                        .ok_or_else(|| GovernorError::UnknownSource(source.to_owned()))?;
                    let slot = budget.next_slot(now);
                    if slot <= now {
                        budget.record_request(now);
                        return Ok(());
                    }
                    Some(slot.saturating_since(now).min(MAX_PACING_SLEEP))
                }
            };

            if let Some(delay) = wait {
                tokio::select! {
                    () = tokio::time::sleep(delay) => {}
                    () = self.wake.notified() => {}
                }
            }
        }
    }

    /// Folds a successful response's limit headers into the source's budget.
    pub fn observe(&self, source: &str, observed: &LimitHeaders) {
        let now = self.clock.now();
        if let Some(budget) = self.lock().budgets.get_mut(source) {
            budget.observe(observed, now);
        }
        self.wake.notify_waiters();
    }

    /// Folds a rate limiting response into the source's budget.
    pub fn observe_rate_limited(&self, source: &str, observed: &LimitHeaders) {
        let now = self.clock.now();
        if let Some(budget) = self.lock().budgets.get_mut(source) {
            budget.on_rate_limited(observed, now);
        }
        self.wake.notify_waiters();
    }

    /// Marks a stream as in flight so it is not admitted twice.
    pub fn mark_running(&self, stream: &str) {
        self.with_stream(stream, |s| s.running = true);
    }

    /// Marks a stream as finished.
    ///
    /// `success_at` advances the deficit clock and must be supplied for a 304 as
    /// well as for a run that wrote records: a confirmed-unchanged response
    /// means the mirror *is* fresh. Get that backwards and every stream looks
    /// permanently stale, and the governor spends the whole budget chasing it.
    pub fn mark_finished(&self, stream: &str, success_at: Option<Timestamp>) {
        self.with_stream(stream, |s| {
            s.running = false;
            if let Some(at) = success_at {
                s.last_success_at = Some(at);
            }
        });
        self.wake.notify_waiters();
    }

    /// A copy of every stream's current state, for `siphon status` and logging.
    #[must_use]
    pub fn stream_states(&self) -> Vec<StreamState> {
        self.lock().streams.clone()
    }

    /// The current deficit of one stream.
    #[must_use]
    pub fn deficit(&self, stream: &str) -> Option<f64> {
        let now = self.clock.now();
        self.lock()
            .streams
            .iter()
            .find(|s| s.name == stream)
            .map(|s| s.deficit(now))
    }

    /// The persistable budget for every source.
    #[must_use]
    pub fn budget_snapshots(&self) -> Vec<(String, BudgetSnapshot)> {
        self.lock()
            .budgets
            .iter()
            .map(|(name, budget)| (name.clone(), budget.snapshot()))
            .collect()
    }

    /// A readable summary of one source's budget, for `siphon status`.
    #[must_use]
    pub fn budget_view(&self, source: &str) -> Option<BudgetView> {
        let now = self.clock.now();
        let inner = self.lock();
        let budget = inner.budgets.get(source)?;
        Some(BudgetView {
            remaining: budget.effective_remaining(),
            limit: budget.observed_limit(),
            window_left: budget.window_left(now),
            spend_rate: budget.spend_rate(now),
            ratcheted: budget.is_ratcheted(now),
            skew: budget.skew(),
        })
    }

    fn with_stream(&self, stream: &str, apply: impl FnOnce(&mut StreamState)) {
        let mut inner = self.lock();
        if let Some(state) = inner.streams.iter_mut().find(|s| s.name == stream) {
            apply(state);
        }
    }

    /// The lock is only ever held across straight-line arithmetic, never across
    /// an await, so a poisoned lock means a panic in that arithmetic. Recovering
    /// the guard keeps a single bad decision from wedging the whole daemon.
    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// A snapshot of a source's budget for display.
#[derive(Debug, Clone, Copy)]
pub struct BudgetView {
    /// Requests left in the window.
    pub remaining: Option<u64>,
    /// The window's total allowance.
    pub limit: Option<u64>,
    /// How much of the window is left.
    pub window_left: Option<Duration>,
    /// Requests per second currently allowed.
    pub spend_rate: f64,
    /// Whether a rate limiting response is still suppressing the rate.
    pub ratcheted: bool,
    /// Estimated offset between the server's clock and the local one.
    pub skew: Duration,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::budget::{BudgetConfig, BudgetPolicy};
    use crate::clock::TestClock;

    fn budget(per_hour: u64) -> Budget {
        Budget::new(
            BudgetPolicy::from_config(Some(&BudgetConfig {
                requests_per_hour: Some(per_hour),
                observe: None,
                utilization: Some(1.0),
            }))
            .expect("valid policy"),
        )
    }

    fn stream(name: &str, freshness: Option<u64>, priority: Priority) -> StreamState {
        StreamState {
            name: name.to_owned(),
            source: "api".to_owned(),
            freshness: freshness.map(Duration::from_secs),
            priority,
            last_success_at: Some(Timestamp::from_secs(0)),
            running: false,
        }
    }

    fn governor(clock: &Arc<TestClock>, streams: Vec<StreamState>, per_hour: u64) -> Governor {
        let mut budgets = BTreeMap::new();
        budgets.insert("api".to_owned(), budget(per_hour));
        Governor::new(
            Arc::clone(clock) as Arc<dyn Clock>,
            AdmissionPolicy::default(),
            budgets,
            streams,
        )
    }

    #[tokio::test]
    async fn first_request_is_granted_immediately() {
        let clock = Arc::new(TestClock::new(Timestamp::from_secs(0)));
        let gov = governor(
            &clock,
            vec![stream("issues", Some(900), Priority::High)],
            3_600,
        );
        gov.acquire("api", Priority::High)
            .await
            .expect("source is registered");
    }

    #[tokio::test]
    async fn unknown_source_is_an_error_not_a_hang() {
        let clock = Arc::new(TestClock::new(Timestamp::from_secs(0)));
        let gov = governor(&clock, vec![], 3_600);
        let err = gov.acquire("nope", Priority::High).await.unwrap_err();
        assert!(matches!(err, GovernorError::UnknownSource(_)));
    }

    #[tokio::test(start_paused = true)]
    async fn pacing_delays_the_second_request() {
        let clock = Arc::new(TestClock::new(Timestamp::from_secs(0)));
        // 360 requests/hour is one every 10 seconds.
        let gov = governor(
            &clock,
            vec![stream("issues", Some(900), Priority::High)],
            360,
        );

        gov.acquire("api", Priority::High).await.expect("granted");

        // The second acquire must not resolve while the clock stands still.
        let pending = tokio::time::timeout(
            Duration::from_millis(50),
            gov.acquire("api", Priority::High),
        )
        .await;
        assert!(pending.is_err(), "second request should still be paced");

        clock.advance(Duration::from_secs(10));
        tokio::time::timeout(Duration::from_secs(60), gov.acquire("api", Priority::High))
            .await
            .expect("granted once the interval elapsed")
            .expect("source is registered");
    }

    #[tokio::test(start_paused = true)]
    async fn best_effort_yields_while_an_slo_stream_is_slipping() {
        let clock = Arc::new(TestClock::new(Timestamp::from_secs(0)));
        let gov = governor(
            &clock,
            vec![
                stream("issues", Some(100), Priority::High),
                stream("backfill", None, Priority::BestEffort),
            ],
            36_000,
        );

        // 60s into a 100s window: deficit 0.6, past the surplus threshold.
        clock.advance(Duration::from_secs(60));
        let pending = tokio::time::timeout(
            Duration::from_millis(50),
            gov.acquire("api", Priority::BestEffort),
        )
        .await;
        assert!(pending.is_err(), "backfill should be yielding");

        // A successful run on `issues` restores surplus and wakes the backfill.
        gov.mark_finished("issues", Some(clock.now()));
        tokio::time::timeout(
            Duration::from_secs(60),
            gov.acquire("api", Priority::BestEffort),
        )
        .await
        .expect("backfill resumes once there is surplus")
        .expect("source is registered");
    }

    #[test]
    fn a_running_stream_is_not_admitted_again() {
        let clock = Arc::new(TestClock::new(Timestamp::from_secs(1_000)));
        let gov = governor(
            &clock,
            vec![stream("issues", Some(100), Priority::High)],
            3_600,
        );

        assert!(matches!(gov.next_admission(), Decision::Admit { .. }));
        gov.mark_running("issues");
        assert!(matches!(gov.next_admission(), Decision::Idle { .. }));
    }

    #[test]
    fn a_finished_run_resets_the_deficit() {
        let clock = Arc::new(TestClock::new(Timestamp::from_secs(1_000)));
        let gov = governor(
            &clock,
            vec![stream("issues", Some(100), Priority::High)],
            3_600,
        );
        assert!(gov.deficit("issues").unwrap_or_default() > 1.0);

        gov.mark_running("issues");
        gov.mark_finished("issues", Some(clock.now()));
        assert_eq!(gov.deficit("issues"), Some(0.0));
    }

    #[test]
    fn a_not_modified_response_counts_as_success_for_freshness() {
        let clock = Arc::new(TestClock::new(Timestamp::from_secs(1_000)));
        let gov = governor(
            &clock,
            vec![stream("releases", Some(100), Priority::Normal)],
            3_600,
        );
        // Nothing was written, but the mirror is confirmed current.
        gov.mark_finished("releases", Some(clock.now()));
        assert_eq!(gov.deficit("releases"), Some(0.0));
    }

    #[test]
    fn budget_snapshots_cover_every_source() {
        let clock = Arc::new(TestClock::new(Timestamp::from_secs(0)));
        let gov = governor(&clock, vec![], 3_600);
        let snapshots = gov.budget_snapshots();
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].0, "api");
    }

    #[test]
    fn budget_view_reports_the_measured_state() {
        let clock = Arc::new(TestClock::new(Timestamp::from_secs(0)));
        let gov = governor(&clock, vec![], 3_600);
        gov.observe(
            "api",
            &LimitHeaders {
                limit: Some(5_000),
                remaining: Some(4_000),
                reset_at: Some(Timestamp::from_secs(3_600)),
                server_date: Some(Timestamp::from_secs(0)),
                retry_after: None,
            },
        );
        let view = gov.budget_view("api").expect("source is registered");
        assert_eq!(view.remaining, Some(4_000));
        assert_eq!(view.limit, Some(5_000));
        assert!(!view.ratcheted);
        assert!(view.spend_rate > 1.0);
    }
}
