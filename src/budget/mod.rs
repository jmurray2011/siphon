//! The measured rate-limit budget for one source.
//!
//! Three sources of truth, in increasing order of authority: the configured
//! ceiling, the headers the API sends back, and the 429s it sends when the first
//! two were wrong. The budget is a resource to allocate, so what this module
//! produces is not a yes/no verdict but a *rate*: how many requests per second
//! siphon may spend right now.
//!
//! The local clock is untrusted. `reset_at` is a server timestamp and `now` is
//! local, so every observation also re-estimates the skew between them from the
//! `Date` header on the same response.

pub mod headers;

use std::time::Duration;

use serde::Deserialize;

use crate::clock::Timestamp;
pub use headers::{HeaderSource, LimitHeaders, ObserveConfig, ObserveSpec, ResetFormat};

/// Politeness floor when neither the config nor the API says anything: 1 req/s.
const POLITENESS_PER_HOUR: u64 = 3_600;

/// A measured window shorter than this is treated as a bad parse or a bad clock.
const MIN_WINDOW: Duration = Duration::from_secs(1);

/// A measured window longer than this is treated as a bad parse or a bad clock.
const MAX_WINDOW: Duration = Duration::from_secs(24 * 3_600);

/// How far a rate limiting response cuts the spend rate, multiplicatively.
const RATCHET_STEP: f64 = 0.5;

/// The floor the ratchet cannot cut below, so a source is never wedged shut.
const RATCHET_FLOOR: f64 = 0.05;

/// How long to hold back when a 429 arrives with no `Retry-After` and no reset.
const BLIND_RATCHET_PAUSE: Duration = Duration::from_secs(60);

/// Weight of each new skew sample. Low enough that one odd `Date` header does
/// not move the pacing much, high enough to follow a real clock correction.
const SKEW_ALPHA: f64 = 0.25;

/// The budget block of a source, as written in YAML.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BudgetConfig {
    /// Fallback ceiling, used only until the API says otherwise.
    #[serde(default)]
    pub requests_per_hour: Option<u64>,
    /// How to read the API's own accounting out of response headers.
    #[serde(default)]
    pub observe: Option<ObserveConfig>,
    /// Fraction of the measured budget siphon will actually spend.
    #[serde(default)]
    pub utilization: Option<f64>,
}

/// A validated budget block.
#[derive(Debug, Clone)]
pub struct BudgetPolicy {
    fallback_per_hour: Option<u64>,
    observe: Option<ObserveSpec>,
    utilization: f64,
}

impl Default for BudgetPolicy {
    fn default() -> Self {
        Self {
            fallback_per_hour: None,
            observe: None,
            utilization: 0.8,
        }
    }
}

/// Why a budget block was rejected.
#[derive(Debug, thiserror::Error)]
pub enum BudgetConfigError {
    /// `utilization` was outside `(0, 1]`.
    #[error("budget.utilization must be greater than 0 and at most 1, got {0}")]
    Utilization(f64),
    /// `requests_per_hour` was zero.
    #[error("budget.requests_per_hour must be greater than zero")]
    ZeroCeiling,
}

impl BudgetPolicy {
    /// Validates a YAML budget block.
    ///
    /// # Errors
    ///
    /// Fails when `utilization` is outside `(0, 1]` or the ceiling is zero.
    pub fn from_config(cfg: Option<&BudgetConfig>) -> Result<Self, BudgetConfigError> {
        let Some(cfg) = cfg else {
            return Ok(Self::default());
        };
        let utilization = cfg.utilization.unwrap_or(0.8);
        if !(utilization > 0.0 && utilization <= 1.0) {
            return Err(BudgetConfigError::Utilization(utilization));
        }
        if cfg.requests_per_hour == Some(0) {
            return Err(BudgetConfigError::ZeroCeiling);
        }
        Ok(Self {
            fallback_per_hour: cfg.requests_per_hour,
            observe: cfg.observe.as_ref().map(ObserveSpec::from),
            utilization,
        })
    }

    /// How to read limit headers, if the operator configured it explicitly.
    #[must_use]
    pub const fn observe(&self) -> Option<&ObserveSpec> {
        self.observe.as_ref()
    }

    /// The configured or default utilization fraction.
    #[must_use]
    pub const fn utilization(&self) -> f64 {
        self.utilization
    }

    /// The configured ceiling, if any.
    #[must_use]
    pub const fn ceiling_per_hour(&self) -> Option<u64> {
        self.fallback_per_hour
    }

    /// Requests per second siphon may spend when nothing has been measured yet.
    #[must_use]
    pub fn fallback_rate(&self) -> f64 {
        let per_hour = self.fallback_per_hour.unwrap_or(POLITENESS_PER_HOUR);
        (per_hour as f64) * self.utilization / 3_600.0
    }
}

/// The part of a budget that outlives the process.
///
/// A daemon that re-learns its rate limit by getting 429'd after every deploy
/// punishes the operator for deploying, so this round-trips through
/// `_siphon_budget`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BudgetSnapshot {
    /// The window's total allowance, as last observed.
    pub observed_limit: Option<u64>,
    /// The estimated window length in seconds.
    pub window_secs: Option<u64>,
    /// Requests left when the window was last observed.
    pub remaining: Option<u64>,
    /// When the window resets, in server time.
    pub reset_at: Option<Timestamp>,
    /// When a rate limiting response last forced the ceiling down.
    pub ratcheted_at: Option<Timestamp>,
}

#[derive(Debug, Clone, Copy)]
struct Ratchet {
    factor: f64,
    until: Timestamp,
    at: Timestamp,
}

/// A pause before the next request.
#[derive(Debug, Clone, Copy)]
struct Hold {
    until: Timestamp,
    /// True when the server named the time itself via `Retry-After`.
    ///
    /// An explicit instruction supersedes the wait that an exhausted window
    /// would otherwise imply: an API that says "come back in 30s" while its
    /// primary window has 16 minutes left is describing a secondary limit, and
    /// idling for the longer of the two would throw away budget it just offered.
    explicit: bool,
}

/// The live budget for one source.
#[derive(Debug)]
pub struct Budget {
    policy: BudgetPolicy,
    limit: Option<u64>,
    remaining: Option<u64>,
    reset_at: Option<Timestamp>,
    window_secs: Option<u64>,
    skew_millis: i64,
    have_skew: bool,
    ratchet: Option<Ratchet>,
    spent_since_observation: u64,
    last_grant: Option<Timestamp>,
    hold: Option<Hold>,
}

impl Budget {
    /// A budget that has measured nothing yet.
    #[must_use]
    pub fn new(policy: BudgetPolicy) -> Self {
        Self {
            policy,
            limit: None,
            remaining: None,
            reset_at: None,
            window_secs: None,
            skew_millis: 0,
            have_skew: false,
            ratchet: None,
            spent_since_observation: 0,
            last_grant: None,
            hold: None,
        }
    }

    /// Restores what was learned before the last restart.
    ///
    /// A window that has already elapsed is assumed to have rolled over: the
    /// allowance is restored and the window re-anchored to now. The first real
    /// response corrects both, and until then the learned ceiling is a far
    /// better guess than starting blind.
    #[must_use]
    pub fn restore(policy: BudgetPolicy, snapshot: BudgetSnapshot, local_now: Timestamp) -> Self {
        let mut budget = Self::new(policy);
        budget.limit = snapshot.observed_limit;
        budget.window_secs = snapshot.window_secs;
        budget.ratchet = None;

        match snapshot.reset_at {
            Some(reset_at) if reset_at > local_now => {
                budget.reset_at = Some(reset_at);
                budget.remaining = snapshot.remaining;
            }
            _ => {
                budget.remaining = snapshot.observed_limit;
                budget.reset_at = snapshot
                    .window_secs
                    .map(|secs| local_now.saturating_add(Duration::from_secs(secs)));
            }
        }
        budget
    }

    /// The persistable part of the budget.
    #[must_use]
    pub fn snapshot(&self) -> BudgetSnapshot {
        BudgetSnapshot {
            observed_limit: self.limit,
            window_secs: self.window_secs,
            remaining: self.effective_remaining(),
            reset_at: self.reset_at,
            ratcheted_at: self.ratchet.map(|r| r.at),
        }
    }

    /// Folds a successful response's headers into the budget.
    pub fn observe(&mut self, observed: &LimitHeaders, local_now: Timestamp) {
        if let Some(server_date) = observed.server_date {
            self.update_skew(server_date.diff_millis(local_now));
        }
        if let Some(limit) = observed.limit {
            self.limit = Some(limit);
        }
        if observed.reset_at.is_some() {
            self.reset_at = observed.reset_at;
        }
        if let Some(remaining) = observed.remaining {
            self.remaining = Some(remaining);
            self.spent_since_observation = 0;
        }
        self.update_window_estimate(local_now);

        // A window that has rolled since the ratchet was applied clears it; the
        // rule is only that it does not ratchet back up *within* the window.
        if let Some(ratchet) = self.ratchet {
            if local_now >= ratchet.until {
                self.ratchet = None;
            }
        }
        if let Some(hold) = self.hold {
            if local_now >= hold.until {
                self.hold = None;
            }
        }
    }

    /// Folds a rate limiting response into the budget and cuts the spend rate.
    ///
    /// Called for a 429, or a 403 that carried rate-limit headers. Both mean the
    /// previous two sources of truth were wrong.
    pub fn on_rate_limited(&mut self, observed: &LimitHeaders, local_now: Timestamp) {
        self.observe(observed, local_now);
        self.remaining = Some(observed.remaining.unwrap_or(0));
        self.spent_since_observation = 0;

        let explicit = observed.retry_after.is_some();
        let resume_at = observed
            .retry_after
            .map(|after| local_now.saturating_add(after))
            .or_else(|| self.local_reset_at())
            .unwrap_or_else(|| local_now.saturating_add(BLIND_RATCHET_PAUSE));

        let factor = self.ratchet.map_or(RATCHET_STEP, |r| {
            (r.factor * RATCHET_STEP).max(RATCHET_FLOOR)
        });
        self.ratchet = Some(Ratchet {
            factor,
            until: resume_at.max(local_now.saturating_add(MIN_WINDOW)),
            at: local_now,
        });
        self.hold = Some(Hold {
            until: resume_at,
            explicit,
        });
    }

    /// Records that a request was issued, so pacing accounts for it before the
    /// response arrives.
    pub fn record_request(&mut self, local_now: Timestamp) {
        self.spent_since_observation = self.spent_since_observation.saturating_add(1);
        self.last_grant = Some(local_now);
    }

    /// Requests per second siphon may spend right now.
    #[must_use]
    pub fn spend_rate(&self, local_now: Timestamp) -> f64 {
        let measured = match (self.effective_remaining(), self.window_left(local_now)) {
            (Some(remaining), Some(window)) => {
                Some((remaining as f64) * self.policy.utilization / window.as_secs_f64())
            }
            _ => None,
        };
        let base = measured.unwrap_or_else(|| self.policy.fallback_rate());
        let factor = self.active_ratchet(local_now).map_or(1.0, |r| r.factor);
        (base * factor).max(f64::MIN_POSITIVE)
    }

    /// The earliest local instant at which the next request may go out.
    #[must_use]
    pub fn next_slot(&self, local_now: Timestamp) -> Timestamp {
        let mut at = local_now;

        if let Some(hold) = self.hold {
            at = at.max(hold.until);
        }
        // An exhausted window means waiting for the reset - unless the server
        // named a time itself, in which case that instruction is the one to
        // follow, including after it has elapsed and the probe is due.
        if !self.hold.is_some_and(|h| h.explicit) && self.effective_remaining() == Some(0) {
            let resume = self
                .local_reset_at()
                .unwrap_or_else(|| local_now.saturating_add(BLIND_RATCHET_PAUSE));
            at = at.max(resume);
        }
        if let Some(last) = self.last_grant {
            let rate = self.spend_rate(local_now);
            let interval = Duration::from_secs_f64((1.0 / rate).min(MAX_WINDOW.as_secs_f64()));
            at = at.max(last.saturating_add(interval));
        }
        at
    }

    /// Requests left in the window, accounting for requests already in flight.
    #[must_use]
    pub fn effective_remaining(&self) -> Option<u64> {
        self.remaining
            .map(|r| r.saturating_sub(self.spent_since_observation))
    }

    /// The window's total allowance, as last observed.
    #[must_use]
    pub const fn observed_limit(&self) -> Option<u64> {
        self.limit
    }

    /// How long the current window has left, in local time, or `None` when the
    /// measurement is missing or implausible.
    #[must_use]
    pub fn window_left(&self, local_now: Timestamp) -> Option<Duration> {
        let reset_at = self.local_reset_at()?;
        let left = Duration::from_millis(u64::try_from(reset_at.diff_millis(local_now)).ok()?);
        (left >= MIN_WINDOW && left <= MAX_WINDOW).then_some(left)
    }

    /// The estimated offset between the server's clock and the local one.
    #[must_use]
    pub fn skew(&self) -> Duration {
        Duration::from_millis(self.skew_millis.unsigned_abs())
    }

    /// True while a rate limiting response is still suppressing the spend rate.
    #[must_use]
    pub fn is_ratcheted(&self, local_now: Timestamp) -> bool {
        self.active_ratchet(local_now).is_some()
    }

    fn active_ratchet(&self, local_now: Timestamp) -> Option<Ratchet> {
        self.ratchet.filter(|r| local_now < r.until)
    }

    /// The reset instant translated from server time into local time.
    fn local_reset_at(&self) -> Option<Timestamp> {
        self.reset_at.map(|at| at.shifted(-self.skew_millis))
    }

    fn update_skew(&mut self, sample_millis: i64) {
        if self.have_skew {
            let blended = (sample_millis as f64) * SKEW_ALPHA
                + (self.skew_millis as f64) * (1.0 - SKEW_ALPHA);
            self.skew_millis = blended as i64;
        } else {
            self.skew_millis = sample_millis;
            self.have_skew = true;
        }
    }

    /// Estimates the window length as the largest gap ever seen between the
    /// server's clock and its own reset instant. Mid-window that gap is short;
    /// at the start of a window it is the whole window, so the maximum is the
    /// estimator that converges on the truth.
    fn update_window_estimate(&mut self, local_now: Timestamp) {
        let Some(left) = self.window_left(local_now) else {
            return;
        };
        let secs = left.as_secs().max(1);
        self.window_secs = Some(self.window_secs.map_or(secs, |prev| prev.max(secs)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(per_hour: Option<u64>, utilization: f64) -> BudgetPolicy {
        BudgetPolicy::from_config(Some(&BudgetConfig {
            requests_per_hour: per_hour,
            observe: None,
            utilization: Some(utilization),
        }))
        .unwrap()
    }

    fn t(secs: i64) -> Timestamp {
        Timestamp::from_secs(secs)
    }

    fn observed(remaining: u64, limit: u64, reset_at: i64, server_date: i64) -> LimitHeaders {
        LimitHeaders {
            limit: Some(limit),
            remaining: Some(remaining),
            reset_at: Some(t(reset_at)),
            server_date: Some(t(server_date)),
            retry_after: None,
        }
    }

    #[test]
    fn rejects_out_of_range_utilization() {
        for bad in [0.0, -0.5, 1.5] {
            let err = BudgetPolicy::from_config(Some(&BudgetConfig {
                requests_per_hour: None,
                observe: None,
                utilization: Some(bad),
            }))
            .unwrap_err();
            assert!(matches!(err, BudgetConfigError::Utilization(_)));
        }
    }

    #[test]
    fn falls_back_to_the_configured_ceiling_before_anything_is_measured() {
        let budget = Budget::new(policy(Some(3_600), 0.5));
        // 3600/hour is 1/s; at 50% utilization that is one request every 2s.
        assert!((budget.spend_rate(t(0)) - 0.5).abs() < 1e-9);
    }

    #[test]
    fn falls_back_to_a_politeness_floor_when_nothing_is_configured() {
        let budget = Budget::new(policy(None, 1.0));
        assert!((budget.spend_rate(t(0)) - 1.0).abs() < 1e-9);
    }

    #[test]
    fn measured_headers_override_the_configured_ceiling() {
        let mut budget = Budget::new(policy(Some(36_000), 1.0));
        // 1000 requests left, window closes in 1000s: exactly 1 req/s.
        budget.observe(&observed(1_000, 5_000, 1_000, 0), t(0));
        assert!((budget.spend_rate(t(0)) - 1.0).abs() < 1e-6);
    }

    #[test]
    fn utilization_leaves_headroom() {
        let mut budget = Budget::new(policy(None, 0.8));
        budget.observe(&observed(1_000, 5_000, 1_000, 0), t(0));
        assert!((budget.spend_rate(t(0)) - 0.8).abs() < 1e-6);
    }

    #[test]
    fn spend_rate_rises_as_the_window_closes_with_budget_left() {
        let mut budget = Budget::new(policy(None, 1.0));
        budget.observe(&observed(600, 5_000, 600, 0), t(0));
        let early = budget.spend_rate(t(0));
        let late = budget.spend_rate(t(300));
        assert!((early - 1.0).abs() < 1e-6);
        assert!((late - 2.0).abs() < 1e-6, "late rate was {late}");
    }

    #[test]
    fn window_is_measured_in_server_time_not_local_time() {
        // In both cases the server has 600 requests and 600 seconds of window
        // left, so the honest answer is 1 req/s. Only the local clock differs.

        // Local clock runs 300s slow. Uncorrected, `reset_at - local_now` reads
        // 900s and siphon would dawdle at 0.67 req/s, missing freshness targets
        // it could have met.
        let mut slow = Budget::new(policy(None, 1.0));
        slow.observe(&observed(600, 5_000, 900, 300), t(0));
        assert!((slow.spend_rate(t(0)) - 1.0).abs() < 1e-6);
        assert_eq!(slow.skew(), Duration::from_secs(300));

        // Local clock runs 300s fast. Uncorrected, the window reads 300s and
        // siphon would spend at 2 req/s - sprinting into the wall.
        let mut fast = Budget::new(policy(None, 1.0));
        fast.observe(&observed(600, 5_000, 300, -300), t(0));
        assert!((fast.spend_rate(t(0)) - 1.0).abs() < 1e-6);
        assert_eq!(fast.skew(), Duration::from_secs(300));
    }

    #[test]
    fn an_absurd_window_falls_back_rather_than_assuming_infinite_budget() {
        let mut budget = Budget::new(policy(Some(360), 1.0));
        // Reset claimed 40 hours out: beyond any plausible window.
        budget.observe(&observed(10_000, 10_000, 40 * 3_600, 0), t(0));
        assert!((budget.spend_rate(t(0)) - 0.1).abs() < 1e-9);

        // Reset already in the past: also implausible, same conservative answer.
        let mut stale = Budget::new(policy(Some(360), 1.0));
        stale.observe(&observed(10_000, 10_000, -100, 0), t(0));
        assert!((stale.spend_rate(t(0)) - 0.1).abs() < 1e-9);
    }

    #[test]
    fn in_flight_requests_are_counted_before_the_response_lands() {
        let mut budget = Budget::new(policy(None, 1.0));
        budget.observe(&observed(10, 100, 10, 0), t(0));
        assert_eq!(budget.effective_remaining(), Some(10));
        budget.record_request(t(0));
        budget.record_request(t(0));
        assert_eq!(budget.effective_remaining(), Some(8));
        budget.observe(&observed(7, 100, 10, 0), t(1));
        assert_eq!(budget.effective_remaining(), Some(7));
    }

    #[test]
    fn a_counter_that_moves_backwards_never_restores_the_allowance() {
        // Some APIs answer from several nodes whose counters disagree: eight
        // requests inside two seconds read 55, 57, 59, 54, 59, 59, 58, 58, and a
        // single node reported 57, 54, 58 in that order. Taking the newest value
        // verbatim lets the stalest node hand the whole allowance back, and the
        // in-flight count is zeroed with it, so siphon forgets what it spent.
        let mut budget = Budget::new(policy(Some(60), 1.0));
        budget.observe(&observed(59, 60, 3_600, 0), t(0));

        for _ in 0..40 {
            budget.record_request(t(0));
        }
        assert_eq!(budget.effective_remaining(), Some(19));

        budget.observe(&observed(59, 60, 3_600, 0), t(1));
        assert_eq!(
            budget.effective_remaining(),
            Some(19),
            "a stale counter restored budget that had already been spent"
        );
    }

    #[test]
    fn the_allowance_returns_when_the_window_actually_rolls() {
        // The guard above must not wedge the source shut: once the window it was
        // measured against has elapsed, a larger count is the real thing.
        let mut budget = Budget::new(policy(Some(60), 1.0));
        budget.observe(&observed(59, 60, 3_600, 0), t(0));
        for _ in 0..40 {
            budget.record_request(t(0));
        }
        assert_eq!(budget.effective_remaining(), Some(19));

        budget.observe(&observed(60, 60, 7_200, 3_600), t(3_600));
        assert_eq!(budget.effective_remaining(), Some(60));
    }

    #[test]
    fn pacing_spreads_the_remaining_budget_over_the_remaining_window() {
        let mut budget = Budget::new(policy(None, 1.0));
        // 10 requests over 100 seconds. The first may go immediately; after it,
        // 9 are left for the same 100 seconds, so the gap is 11.1s rather than
        // 10s. Recomputing from what is actually left is what makes pacing
        // self-correcting: it never front-loads and never has to catch up.
        budget.observe(&observed(10, 100, 100, 0), t(0));
        assert_eq!(budget.next_slot(t(0)), t(0));

        budget.record_request(t(0));
        let gap = budget.next_slot(t(0)).saturating_since(t(0));
        assert!(
            gap >= Duration::from_secs(10) && gap <= Duration::from_secs(12),
            "gap was {gap:?}"
        );

        // Ten requests must not all fit inside the window's first instant.
        budget.record_request(t(0));
        assert!(budget.next_slot(t(0)) > t(0));
    }

    #[test]
    fn exhausted_budget_waits_for_the_window_to_reset() {
        let mut budget = Budget::new(policy(None, 1.0));
        budget.observe(&observed(0, 100, 300, 0), t(0));
        assert_eq!(budget.next_slot(t(0)), t(300));
    }

    #[test]
    fn exhausted_budget_uses_the_skew_corrected_reset() {
        let mut budget = Budget::new(policy(None, 1.0));
        // Server clock reads 300s ahead of local; reset is at server T+600,
        // which is local T+300.
        budget.observe(&observed(0, 100, 600, 300), t(0));
        assert_eq!(budget.next_slot(t(0)), t(300));
    }

    #[test]
    fn rate_limiting_ratchets_the_spend_rate_down() {
        let mut budget = Budget::new(policy(None, 1.0));
        budget.observe(&observed(500, 1_000, 1_000, 0), t(0));
        let before = budget.spend_rate(t(0));

        budget.on_rate_limited(
            &LimitHeaders {
                remaining: Some(0),
                retry_after: Some(Duration::from_secs(30)),
                ..LimitHeaders::default()
            },
            t(10),
        );
        assert!(budget.is_ratcheted(t(11)));
        // Retry-After is the server's own instruction and outranks the window
        // reset it would otherwise wait for at t(1000).
        assert_eq!(budget.next_slot(t(10)), t(40));
        assert_eq!(budget.next_slot(t(41)), t(41));

        // The rate stays cut even once the pause has elapsed.
        let after = budget.spend_rate(t(20));
        assert!(after < before, "{after} should be below {before}");
    }

    #[test]
    fn an_exhausted_window_still_waits_for_the_reset_when_no_retry_after_is_given() {
        let mut budget = Budget::new(policy(None, 1.0));
        budget.observe(&observed(500, 1_000, 1_000, 0), t(0));
        budget.on_rate_limited(
            &LimitHeaders {
                remaining: Some(0),
                reset_at: Some(t(1_000)),
                ..LimitHeaders::default()
            },
            t(10),
        );
        assert_eq!(budget.next_slot(t(10)), t(1_000));
    }

    #[test]
    fn repeated_rate_limiting_does_not_wedge_the_source_shut() {
        let mut budget = Budget::new(policy(Some(3_600), 1.0));
        for step in 0..20 {
            budget.on_rate_limited(
                &LimitHeaders {
                    retry_after: Some(Duration::from_secs(1)),
                    ..LimitHeaders::default()
                },
                t(step),
            );
        }
        let rate = budget.spend_rate(t(100));
        assert!(rate > 0.0, "rate collapsed to {rate}");
        assert!(rate >= 1.0 / 3_600.0 * RATCHET_FLOOR);
    }

    #[test]
    fn the_ratchet_does_not_lift_within_the_window() {
        let mut budget = Budget::new(policy(None, 1.0));
        budget.on_rate_limited(
            &LimitHeaders {
                retry_after: Some(Duration::from_secs(600)),
                ..LimitHeaders::default()
            },
            t(0),
        );
        // A successful response mid-window must not clear the ratchet.
        budget.observe(&observed(900, 1_000, 1_000, 0), t(60));
        assert!(budget.is_ratcheted(t(60)));
        // Once the window has passed, the next observation clears it.
        budget.observe(&observed(1_000, 1_000, 2_000, 0), t(601));
        assert!(!budget.is_ratcheted(t(601)));
    }

    #[test]
    fn a_learned_ceiling_survives_a_restart() {
        let mut budget = Budget::new(policy(None, 1.0));
        budget.observe(&observed(400, 1_000, 3_600, 0), t(0));
        let snapshot = budget.snapshot();
        assert_eq!(snapshot.observed_limit, Some(1_000));
        assert_eq!(snapshot.remaining, Some(400));

        let restored = Budget::restore(policy(None, 1.0), snapshot, t(10));
        assert_eq!(restored.observed_limit(), Some(1_000));
        assert_eq!(restored.effective_remaining(), Some(400));
        assert!((restored.spend_rate(t(10)) - 400.0 / 3_590.0).abs() < 1e-6);
    }

    #[test]
    fn a_restart_after_the_window_rolled_assumes_a_fresh_allowance() {
        let mut budget = Budget::new(policy(None, 1.0));
        budget.observe(&observed(3, 1_000, 3_600, 0), t(0));
        let snapshot = budget.snapshot();

        // Restarting an hour later: the window is long gone.
        let restored = Budget::restore(policy(None, 1.0), snapshot, t(7_200));
        assert_eq!(restored.effective_remaining(), Some(1_000));
        assert!(restored.window_left(t(7_200)).is_some());
    }

    #[test]
    fn window_estimate_converges_on_the_largest_gap_seen() {
        let mut budget = Budget::new(policy(None, 1.0));
        budget.observe(&observed(500, 1_000, 3_600, 0), t(0));
        assert_eq!(budget.snapshot().window_secs, Some(3_600));
        // Mid-window observations are shorter and must not shrink the estimate.
        budget.observe(&observed(400, 1_000, 3_600, 1_800), t(1_800));
        assert_eq!(budget.snapshot().window_secs, Some(3_600));
    }
}
