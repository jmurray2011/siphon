//! A deterministic simulation of the governor over a whole rate-limit window.
//!
//! This is the claim the whole design rests on, so it is worth stating as a test
//! rather than only as prose: given the same streams and the same intent, siphon
//! beats its freshness targets when budget is plentiful, and when it is not, it
//! degrades in priority order rather than uniformly.
//!
//! No clocks, no sockets. Time advances one second per tick and the budget is a
//! fixed number of requests per hour, so the results are exact and repeatable.

// Integration tests are their own crates, so `allow-unwrap-in-tests` in
// clippy.toml does not reach helpers that sit outside a `#[test]` function.
// Panicking on a broken fixture is the correct behaviour here.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::time::Duration;

use siphon::clock::Timestamp;
use siphon::governor::policy::{select, AdmissionPolicy, Decision, Priority, StreamState};

const WINDOW_SECS: i64 = 3_600;

struct Sim {
    streams: Vec<StreamState>,
    /// Seconds a run takes from admission to completion.
    run_secs: i64,
    /// In-flight runs: stream name -> tick it completes on.
    running: BTreeMap<String, i64>,
    /// Requests spent per stream.
    spent: BTreeMap<String, u32>,
    /// The worst lag each stream reached, in seconds.
    worst_lag: BTreeMap<String, i64>,
}

impl Sim {
    fn new(streams: Vec<StreamState>, run_secs: i64) -> Self {
        Self {
            streams,
            run_secs,
            running: BTreeMap::new(),
            spent: BTreeMap::new(),
            worst_lag: BTreeMap::new(),
        }
    }

    /// Runs one full window at `requests_per_hour`, evenly paced.
    fn run(&mut self, requests_per_hour: u32) {
        let interval = (WINDOW_SECS as f64 / f64::from(requests_per_hour)).max(1.0) as i64;
        let policy = AdmissionPolicy::default();
        let mut next_slot = 0i64;

        for tick in 0..WINDOW_SECS {
            let now = Timestamp::from_secs(tick);

            // Complete anything that has finished.
            let finished: Vec<String> = self
                .running
                .iter()
                .filter(|(_, done)| **done <= tick)
                .map(|(name, _)| name.clone())
                .collect();
            for name in finished {
                self.running.remove(&name);
                for stream in &mut self.streams {
                    if stream.name == name {
                        stream.running = false;
                        stream.last_success_at = Some(now);
                    }
                }
            }

            self.record_lag(tick);

            if tick < next_slot {
                continue;
            }
            if let Decision::Admit { stream, .. } = select(&self.streams, now, policy) {
                next_slot = tick + interval;
                *self.spent.entry(stream.clone()).or_default() += 1;
                self.running.insert(stream.clone(), tick + self.run_secs);
                for state in &mut self.streams {
                    if state.name == stream {
                        state.running = true;
                    }
                }
            }
        }
    }

    fn record_lag(&mut self, tick: i64) {
        for stream in &self.streams {
            if stream.freshness.is_none() {
                continue;
            }
            let lag = stream
                .last_success_at
                .map_or(tick, |at| tick - at.as_secs());
            let worst = self.worst_lag.entry(stream.name.clone()).or_default();
            *worst = (*worst).max(lag);
        }
    }

    fn spent(&self, stream: &str) -> u32 {
        self.spent.get(stream).copied().unwrap_or_default()
    }

    /// Prints the window's outcome. Run with `--nocapture` to see the demo.
    fn report(&self, title: &str) {
        println!("\n{title}");
        println!(
            "{:<12} {:>9} {:>10} {:>10}",
            "STREAM", "TARGET", "REQUESTS", "WORST LAG"
        );
        for stream in &self.streams {
            let target = stream
                .freshness
                .map_or_else(|| "-".to_owned(), |f| format!("{}s", f.as_secs()));
            let lag = if stream.freshness.is_some() {
                format!("{}s", self.worst_lag(&stream.name))
            } else {
                "-".to_owned()
            };
            println!(
                "{:<12} {:>9} {:>10} {:>10}",
                stream.name,
                target,
                self.spent(&stream.name),
                lag
            );
        }
    }

    fn worst_lag(&self, stream: &str) -> i64 {
        self.worst_lag.get(stream).copied().unwrap_or_default()
    }
}

fn stream(name: &str, freshness_secs: Option<u64>, priority: Priority) -> StreamState {
    StreamState {
        name: name.to_owned(),
        source: "api".to_owned(),
        freshness: freshness_secs.map(Duration::from_secs),
        priority,
        last_success_at: Some(Timestamp::from_secs(0)),
        running: false,
    }
}

/// The fleet used by both halves of the demo: two streams with deadlines at
/// different priorities, and a backfill with none.
fn fleet() -> Vec<StreamState> {
    vec![
        stream("issues", Some(300), Priority::High),
        stream("comments", Some(300), Priority::Low),
        stream("backfill", None, Priority::BestEffort),
    ]
}

#[test]
fn with_plentiful_budget_every_stream_beats_its_target() {
    // 300s targets, so each SLO stream needs 12 polls an hour to just meet them.
    // 600 requests an hour is far more than the two of them need.
    let mut sim = Sim::new(fleet(), 1);
    sim.run(600);
    sim.report("budget plentiful: 600 requests/hour");

    assert!(
        sim.worst_lag("issues") < 300,
        "issues peaked at {}s against a 300s target",
        sim.worst_lag("issues")
    );
    assert!(
        sim.worst_lag("comments") < 300,
        "comments peaked at {}s against a 300s target",
        sim.worst_lag("comments")
    );

    // Beating the target, not merely meeting it: polls land at about half the
    // window, which is the margin the admit floor is there to buy.
    assert!(
        sim.worst_lag("issues") <= 180,
        "issues should be polled well inside its window, peaked at {}s",
        sim.worst_lag("issues")
    );

    // And the surplus is scavenged rather than left on the table.
    assert!(
        sim.spent("backfill") > sim.spent("issues"),
        "the backfill should absorb the leftovers: backfill={} issues={}",
        sim.spent("backfill"),
        sim.spent("issues")
    );
}

#[test]
fn under_pressure_degradation_follows_priority_order() {
    // 18 requests an hour against two streams that between them need 24 just to
    // hold their targets. Something has to give.
    let mut sim = Sim::new(fleet(), 1);
    sim.run(18);
    sim.report("budget scarce: 18 requests/hour against 24 needed");

    assert!(
        sim.spent("issues") > sim.spent("comments"),
        "the high-priority stream should be served more often: issues={} comments={}",
        sim.spent("issues"),
        sim.spent("comments")
    );
    assert!(
        sim.worst_lag("issues") < sim.worst_lag("comments"),
        "the high-priority stream should hold better: issues={}s comments={}s",
        sim.worst_lag("issues"),
        sim.worst_lag("comments")
    );

    // The backfill yields for the whole window bar the opening tick, where
    // every stream really was fresh and the surplus really was surplus. After
    // that it never takes a request a stream with a deadline could have used.
    assert!(
        sim.spent("backfill") <= 1,
        "the backfill took {} requests from a budget that cannot cover the deadlines",
        sim.spent("backfill")
    );
    let slo_spend = sim.spent("issues") + sim.spent("comments");
    assert!(
        slo_spend >= 15,
        "streams with deadlines should get nearly the whole budget, got {slo_spend}"
    );
}

#[test]
fn a_starved_low_priority_stream_is_still_served_eventually() {
    // Priority biases the ordering; it does not create castes. Deficit grows
    // without bound, so `comments` must outrank `issues` sooner or later.
    let mut sim = Sim::new(fleet(), 1);
    sim.run(18);

    assert!(
        sim.spent("comments") > 0,
        "the low-priority stream was never served at all"
    );
}

#[test]
fn a_slow_stream_does_not_block_the_others() {
    // Runs take 30 seconds, so a stream is in flight for a long time. Nothing
    // else should stall behind it: admission skips running streams.
    let mut sim = Sim::new(fleet(), 30);
    sim.run(600);

    assert!(sim.spent("issues") > 0);
    assert!(sim.spent("comments") > 0);
    assert!(
        sim.spent("backfill") > 0,
        "the backfill should still get surplus while others are in flight"
    );
}

#[test]
fn the_same_inputs_always_produce_the_same_schedule() {
    let mut first = Sim::new(fleet(), 1);
    first.run(60);
    let mut second = Sim::new(fleet(), 1);
    second.run(60);

    for name in ["issues", "comments", "backfill"] {
        assert_eq!(
            first.spent(name),
            second.spent(name),
            "scheduling should be reproducible for {name}"
        );
    }
}
