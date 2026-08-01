//! The admission decision, as a pure function.
//!
//! Each stream carries a *deficit*: how far past its freshness target it is.
//! The governor serves the stream with the highest `deficit x weight(priority)`.
//! Priority biases the ordering; because deficit grows without bound it cannot
//! create castes, so a `low` stream that is badly late eventually outranks a
//! `high` stream that is barely late.

use std::time::Duration;

use serde::Deserialize;

use crate::clock::Timestamp;

/// How much of its freshness window a stream must have burned before the
/// governor will spend a request on it.
///
/// Without a floor, plentiful budget is spent re-polling whichever stream is
/// marginally least fresh, forever. At 0.5 a stream is polled at twice the rate
/// its target demands - it beats its SLO when budget allows, with margin for
/// the poll to fail and be retried - and no faster.
const DEFAULT_ADMIT_FLOOR: f64 = 0.5;

/// How late any SLO-bearing stream may be before surplus scavenging stops.
const DEFAULT_SURPLUS_THRESHOLD: f64 = 0.5;

/// Deficit assigned to a stream that has never successfully synced.
///
/// Such a stream is infinitely stale, so it outranks anything with a real
/// measurement. A large finite value rather than infinity keeps the ordering
/// arithmetic total: among several never-synced streams, priority still decides.
const NEVER_SYNCED_DEFICIT: f64 = 1e9;

/// What the operator said this stream is worth relative to the others.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Priority {
    /// Consumes only surplus budget, and yields the moment anything with a
    /// deadline starts slipping.
    BestEffort,
    /// Served last among streams with deadlines.
    Low,
    /// The default.
    #[default]
    Normal,
    /// Served first.
    High,
}

impl Priority {
    /// The multiplier applied to a stream's deficit when ordering.
    #[must_use]
    pub const fn weight(self) -> f64 {
        match self {
            Self::BestEffort => 0.0,
            Self::Low => 1.0,
            Self::Normal => 2.0,
            Self::High => 4.0,
        }
    }

    /// A label for logs and `siphon status`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BestEffort => "best_effort",
            Self::Low => "low",
            Self::Normal => "normal",
            Self::High => "high",
        }
    }
}

/// What the governor knows about one stream.
#[derive(Debug, Clone, PartialEq)]
pub struct StreamState {
    /// The stream's name.
    pub name: String,
    /// Which source's budget it spends.
    pub source: String,
    /// How stale it is allowed to get, if it declared a target.
    pub freshness: Option<Duration>,
    /// Its priority.
    pub priority: Priority,
    /// When it last completed a successful run, including a confirmed 304.
    pub last_success_at: Option<Timestamp>,
    /// Whether a run is in flight.
    pub running: bool,
}

impl StreamState {
    /// True when this stream has a deadline the governor is accountable for.
    ///
    /// A `best_effort` stream never is, even if it declared a target: that is
    /// what makes surplus scavenging safe to reason about.
    #[must_use]
    pub const fn is_slo_bearing(&self) -> bool {
        self.freshness.is_some() && !matches!(self.priority, Priority::BestEffort)
    }

    /// How far past its freshness target this stream is.
    ///
    /// `1.0` means the target is exactly met; above `1.0` is a live breach.
    #[must_use]
    pub fn deficit(&self, now: Timestamp) -> f64 {
        if !self.is_slo_bearing() {
            return 0.0;
        }
        let Some(freshness) = self.freshness.filter(|f| !f.is_zero()) else {
            return 0.0;
        };
        match self.last_success_at {
            None => NEVER_SYNCED_DEFICIT,
            Some(last) => now.saturating_since(last).as_secs_f64() / freshness.as_secs_f64(),
        }
    }

    /// When this stream's deficit will reach `target`, or `None` if it already has.
    fn reaches(&self, target: f64, now: Timestamp) -> Option<Duration> {
        let freshness = self.freshness?;
        let last = self.last_success_at?;
        let due = last.saturating_add(freshness.mul_f64(target));
        (due > now).then(|| due.saturating_since(now))
    }
}

/// The tunables behind the admission decision.
#[derive(Debug, Clone, Copy)]
pub struct AdmissionPolicy {
    admit_floor: f64,
    surplus_threshold: f64,
}

impl Default for AdmissionPolicy {
    fn default() -> Self {
        Self {
            admit_floor: DEFAULT_ADMIT_FLOOR,
            surplus_threshold: DEFAULT_SURPLUS_THRESHOLD,
        }
    }
}

impl AdmissionPolicy {
    /// True when no SLO-bearing stream is close enough to its bound to need the
    /// budget that a `best_effort` stream would otherwise scavenge.
    #[must_use]
    pub fn has_surplus(&self, streams: &[StreamState], now: Timestamp) -> bool {
        !streams
            .iter()
            .filter(|s| s.is_slo_bearing())
            .any(|s| s.deficit(now) >= self.surplus_threshold)
    }
}

/// What the governor decided to do next.
#[derive(Debug, Clone, PartialEq)]
pub enum Decision {
    /// Run this stream now.
    Admit {
        /// The chosen stream.
        stream: String,
        /// Its deficit at the moment of admission, recorded in the run log so
        /// "why was this stream late" is answerable in SQL afterwards.
        deficit: f64,
    },
    /// Nothing is due. Sleep for this long, or until a run finishes if `None`.
    Idle {
        /// How long until the earliest stream becomes eligible.
        retry_in: Option<Duration>,
    },
}

/// Picks the stream that should get the next request.
///
/// Streams already running are skipped: a stream never overlaps itself, because
/// two concurrent walks of the same paginator would fight over one checkpoint.
#[must_use]
pub fn select(streams: &[StreamState], now: Timestamp, policy: AdmissionPolicy) -> Decision {
    let mut best: Option<(f64, &StreamState)> = None;

    for stream in streams.iter().filter(|s| !s.running) {
        if !stream.is_slo_bearing() {
            continue;
        }
        let deficit = stream.deficit(now);
        if deficit < policy.admit_floor {
            continue;
        }
        let score = deficit * stream.priority.weight();
        // Ties break on name so a given fleet of streams always admits in the
        // same order, which keeps the daemon's behaviour reproducible in tests.
        let better = match best {
            None => true,
            Some((best_score, best_stream)) => {
                score > best_score || (score == best_score && stream.name < best_stream.name)
            }
        };
        if better {
            best = Some((score, stream));
        }
    }

    if let Some((_, stream)) = best {
        return Decision::Admit {
            stream: stream.name.clone(),
            deficit: stream.deficit(now),
        };
    }

    // Nothing with a deadline is due. Surplus, if there is any, goes to the
    // backfills - that is the whole backfill story: not a mode, just a stream
    // that eats leftovers.
    if policy.has_surplus(streams, now) {
        if let Some(stream) = streams
            .iter()
            .filter(|s| !s.running && !s.is_slo_bearing())
            .min_by(|a, b| a.name.cmp(&b.name))
        {
            return Decision::Admit {
                stream: stream.name.clone(),
                deficit: 0.0,
            };
        }
    }

    Decision::Idle {
        retry_in: streams
            .iter()
            .filter(|s| !s.running && s.is_slo_bearing())
            .filter_map(|s| s.reaches(policy.admit_floor, now))
            .min(),
    }
}

/// The fraction of a window during which a stream was inside its freshness target.
///
/// This is the number `siphon status` reports, and it is only computable because
/// freshness is declared rather than implied by a cron expression. `successes`
/// must be sorted ascending and should include the last success *before*
/// `window_start`, since the coverage it bought can extend into the window.
#[must_use]
pub fn attainment(
    successes: &[Timestamp],
    window_start: Timestamp,
    window_end: Timestamp,
    freshness: Duration,
) -> Option<f64> {
    let total = window_end.diff_millis(window_start);
    if total <= 0 || freshness.is_zero() {
        return None;
    }

    let mut covered_millis: i64 = 0;
    let mut cursor = window_start;

    for success in successes {
        let covered_until = success.saturating_add(freshness).min(window_end);
        if covered_until <= cursor {
            continue;
        }
        let from = (*success).max(cursor);
        if from >= window_end {
            break;
        }
        covered_millis += covered_until.diff_millis(from);
        cursor = covered_until;
    }

    Some((covered_millis as f64 / total as f64).clamp(0.0, 1.0))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(secs: i64) -> Timestamp {
        Timestamp::from_secs(secs)
    }

    fn stream(
        name: &str,
        freshness_secs: u64,
        priority: Priority,
        last: Option<i64>,
    ) -> StreamState {
        StreamState {
            name: name.to_owned(),
            source: "api".to_owned(),
            freshness: (freshness_secs > 0).then(|| Duration::from_secs(freshness_secs)),
            priority,
            last_success_at: last.map(t),
            running: false,
        }
    }

    fn admitted(decision: &Decision) -> Option<&str> {
        match decision {
            Decision::Admit { stream, .. } => Some(stream),
            Decision::Idle { .. } => None,
        }
    }

    #[test]
    fn deficit_is_elapsed_over_target() {
        let s = stream("issues", 900, Priority::High, Some(0));
        assert!((s.deficit(t(450)) - 0.5).abs() < 1e-9);
        assert!((s.deficit(t(900)) - 1.0).abs() < 1e-9);
        assert!((s.deficit(t(1_800)) - 2.0).abs() < 1e-9);
    }

    #[test]
    fn best_effort_has_no_deficit_ever() {
        let mut s = stream("backfill", 0, Priority::BestEffort, Some(0));
        assert_eq!(s.deficit(t(100_000)), 0.0);
        // Even if it declares a target, it is not SLO-bearing.
        s.freshness = Some(Duration::from_secs(60));
        assert_eq!(s.deficit(t(100_000)), 0.0);
        assert!(!s.is_slo_bearing());
    }

    #[test]
    fn a_never_synced_stream_outranks_a_merely_late_one() {
        let streams = vec![
            stream("fresh_never_run", 3_600, Priority::Low, None),
            stream("late", 900, Priority::High, Some(0)),
        ];
        let decision = select(&streams, t(3_000), AdmissionPolicy::default());
        assert_eq!(admitted(&decision), Some("fresh_never_run"));
    }

    #[test]
    fn admission_orders_by_deficit_times_weight() {
        // low at deficit 1.0 (weight 1) scores 1.0; normal at 0.6 scores 1.2.
        let streams = vec![
            stream("low_stream", 100, Priority::Low, Some(0)),
            stream("normal_stream", 100, Priority::Normal, Some(40)),
        ];
        let decision = select(&streams, t(100), AdmissionPolicy::default());
        assert_eq!(admitted(&decision), Some("normal_stream"));
    }

    #[test]
    fn priority_biases_but_does_not_create_castes() {
        // A low-priority stream 8 windows late outranks a high-priority stream
        // that is barely late: 8.0 x 1 beats 0.6 x 4.
        let streams = vec![
            stream("low_stream", 100, Priority::Low, Some(200)),
            stream("high_stream", 100, Priority::High, Some(940)),
        ];
        let decision = select(&streams, t(1_000), AdmissionPolicy::default());
        assert_eq!(admitted(&decision), Some("low_stream"));
    }

    #[test]
    fn a_stream_well_inside_its_window_is_not_polled_just_because_budget_exists() {
        let streams = vec![stream("issues", 900, Priority::High, Some(1_000))];
        let decision = select(&streams, t(1_100), AdmissionPolicy::default());
        assert!(matches!(decision, Decision::Idle { .. }));
    }

    #[test]
    fn idle_reports_when_the_next_stream_becomes_due() {
        let streams = vec![stream("issues", 900, Priority::High, Some(1_000))];
        match select(&streams, t(1_100), AdmissionPolicy::default()) {
            // Due at half its window: 1000 + 450 = 1450, so 350s away.
            Decision::Idle { retry_in } => assert_eq!(retry_in, Some(Duration::from_secs(350))),
            other => panic!("expected idle, got {other:?}"),
        }
    }

    #[test]
    fn a_running_stream_is_never_admitted_twice() {
        let mut streams = vec![stream("issues", 900, Priority::High, Some(0))];
        streams[0].running = true;
        let decision = select(&streams, t(10_000), AdmissionPolicy::default());
        assert_eq!(decision, Decision::Idle { retry_in: None });
    }

    #[test]
    fn backfill_runs_only_on_surplus() {
        let backfill = stream("issues_backfill", 0, Priority::BestEffort, Some(0));
        let mut issues = stream("issues", 900, Priority::High, Some(1_000));

        // Everything is comfortably fresh: the backfill eats the leftovers.
        let streams = vec![issues.clone(), backfill.clone()];
        assert_eq!(
            admitted(&select(&streams, t(1_100), AdmissionPolicy::default())),
            Some("issues_backfill")
        );

        // `issues` slips past the surplus threshold: the backfill yields and the
        // budget it was scavenging goes to `issues` instead. The surplus
        // threshold and the admit floor are the same number, so the handover is
        // seamless - there is no window in which the backfill has yielded but
        // nothing has taken its place.
        issues.last_success_at = Some(t(0));
        let streams = vec![issues.clone(), backfill.clone()];
        assert_eq!(
            admitted(&select(&streams, t(500), AdmissionPolicy::default())),
            Some("issues")
        );
        assert_eq!(
            admitted(&select(&streams, t(600), AdmissionPolicy::default())),
            Some("issues")
        );
    }

    #[test]
    fn surplus_handover_leaves_no_idle_gap() {
        // Walk a stream's whole freshness window one step at a time. At every
        // point either the SLO stream or the backfill is runnable, so a
        // configured backfill means measured budget is never left on the table.
        let streams = vec![
            stream("issues", 900, Priority::High, Some(0)),
            stream("backfill", 0, Priority::BestEffort, Some(0)),
        ];
        for secs in 0..1_800 {
            let decision = select(&streams, t(secs), AdmissionPolicy::default());
            assert!(
                matches!(decision, Decision::Admit { .. }),
                "nothing runnable at t={secs}: {decision:?}"
            );
        }
    }

    #[test]
    fn backfill_yields_to_a_stream_that_is_already_running_and_behind() {
        let mut issues = stream("issues", 900, Priority::High, Some(0));
        issues.running = true;
        let streams = vec![issues, stream("backfill", 0, Priority::BestEffort, Some(0))];
        // `issues` is mid-run and behind; the surplus it needs is not surplus.
        let decision = select(&streams, t(900), AdmissionPolicy::default());
        assert!(matches!(decision, Decision::Idle { .. }));
    }

    #[test]
    fn admission_is_deterministic_under_ties() {
        let streams = vec![
            stream("b_stream", 100, Priority::Normal, Some(0)),
            stream("a_stream", 100, Priority::Normal, Some(0)),
        ];
        for _ in 0..8 {
            assert_eq!(
                admitted(&select(&streams, t(100), AdmissionPolicy::default())),
                Some("a_stream")
            );
        }
    }

    #[test]
    fn degradation_under_pressure_follows_priority_order() {
        // Four streams, all one window late. Serving them one at a time, the
        // order is high, normal, low - and the backfill never gets a look in.
        let mut streams = vec![
            stream("low_stream", 100, Priority::Low, Some(0)),
            stream("high_stream", 100, Priority::High, Some(0)),
            stream("normal_stream", 100, Priority::Normal, Some(0)),
            stream("backfill", 0, Priority::BestEffort, Some(0)),
        ];

        let mut served = Vec::new();
        for _ in 0..3 {
            let decision = select(&streams, t(100), AdmissionPolicy::default());
            let name = admitted(&decision).expect("a stream is due").to_owned();
            for s in &mut streams {
                if s.name == name {
                    s.running = true;
                }
            }
            served.push(name);
        }
        assert_eq!(served, ["high_stream", "normal_stream", "low_stream"]);
    }

    #[test]
    fn attainment_is_full_when_polls_keep_pace() {
        let successes: Vec<Timestamp> = (0..=10).map(|i| t(i * 50)).collect();
        let got = attainment(&successes, t(0), t(500), Duration::from_secs(100));
        assert_eq!(got, Some(1.0));
    }

    #[test]
    fn attainment_falls_when_polls_are_missed() {
        // One success at the start of a 400s window with a 100s target: covered
        // for 100s of 400.
        let got = attainment(&[t(0)], t(0), t(400), Duration::from_secs(100));
        assert_eq!(got, Some(0.25));
    }

    #[test]
    fn attainment_counts_coverage_bought_before_the_window() {
        // A success 50s before the window still covers its first 50 seconds.
        let got = attainment(&[t(-50)], t(0), t(400), Duration::from_secs(100));
        assert_eq!(got, Some(0.125));
    }

    #[test]
    fn attainment_does_not_double_count_overlapping_polls() {
        let got = attainment(
            &[t(0), t(10), t(20)],
            t(0),
            t(400),
            Duration::from_secs(100),
        );
        assert_eq!(got, Some(0.3));
    }

    #[test]
    fn attainment_is_none_for_a_degenerate_window() {
        assert_eq!(
            attainment(&[t(0)], t(0), t(0), Duration::from_secs(100)),
            None
        );
        assert_eq!(attainment(&[t(0)], t(0), t(100), Duration::ZERO), None);
    }
}
