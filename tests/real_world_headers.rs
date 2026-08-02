//! The design's riskiest assumption, checked against real responses.
//!
//! Everything in the governor rests on `(remaining, reset_at)` being readable
//! reliably enough to pace against. If the headers are absent, lie, or update
//! lazily, the governor degrades to a static token bucket and siphon has no
//! differentiator - so it is worth pinning down rather than assuming.
//!
//! The header sets below were captured from live, unauthenticated responses on
//! 2026-08-01. They are fixtures, not requests: this test makes no network
//! calls, so it neither flakes nor rate-limits anyone. Re-capture with:
//!
//! ```sh
//! curl -s -o /dev/null -D - https://api.github.com/repos/rust-lang/rust/issues?per_page=1
//! curl -s -o /dev/null -D - https://gitlab.com/api/v4/projects/278964/issues?per_page=1
//! curl -s -o /dev/null -D - https://api.bitbucket.org/2.0/repositories/atlassian?pagelen=1
//! ```
//!
//! What the survey found:
//!
//! - GitHub publishes `X-RateLimit-*` with a unix-seconds reset and a one-hour
//!   window, and decrements exactly once per request.
//! - GitLab publishes the IETF `RateLimit-*` fields with a **sixty-second**
//!   window aligned to the minute - two orders of magnitude shorter than
//!   GitHub's, from the same parse.
//! - Bitbucket publishes the same field names as GitHub but means different
//!   things by them: a compound limit carrying its own window length, a reset
//!   expressed as a delta, and - the finding that mattered - a counter that is
//!   **not monotonic**. It is served by several nodes whose counts disagree and
//!   move in both directions, so the newest value is not the truest one.
//! - crates.io and PyPI publish no rate-limit headers at all. The configured
//!   ceiling is not a fallback for these; it is the only signal there is.

// Integration tests are their own crates, so `allow-unwrap-in-tests` in
// clippy.toml does not reach helpers that sit outside a `#[test]` function.
// Panicking on a broken fixture is the correct behaviour here.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::time::Duration;

use siphon::budget::{headers, Budget, BudgetConfig, BudgetPolicy, LimitHeaders};
use siphon::clock::Timestamp;

/// Captured from `api.github.com`, unauthenticated.
const GITHUB: &[(&str, &str)] = &[
    ("date", "Sat, 01 Aug 2026 22:49:34 GMT"),
    ("x-ratelimit-limit", "60"),
    ("x-ratelimit-remaining", "59"),
    ("x-ratelimit-used", "1"),
    ("x-ratelimit-resource", "core"),
    ("x-ratelimit-reset", "1785628174"),
];

/// The same request a second later, to check the counter actually moves.
const GITHUB_NEXT: &[(&str, &str)] = &[
    ("date", "Sat, 01 Aug 2026 22:49:34 GMT"),
    ("x-ratelimit-limit", "60"),
    ("x-ratelimit-remaining", "58"),
    ("x-ratelimit-used", "2"),
    ("x-ratelimit-resource", "core"),
    ("x-ratelimit-reset", "1785628174"),
];

/// Captured from `gitlab.com/api/v4`, unauthenticated.
const GITLAB: &[(&str, &str)] = &[
    ("date", "Sat, 01 Aug 2026 22:49:37 GMT"),
    ("ratelimit-limit", "500"),
    ("ratelimit-name", "throttle_unauthenticated_api"),
    ("ratelimit-observed", "1"),
    ("ratelimit-remaining", "499"),
    ("ratelimit-reset", "1785624600"),
];

/// Captured from `crates.io/api/v1`: no rate-limit headers of any kind.
const CRATES_IO: &[(&str, &str)] = &[
    ("date", "Sat, 01 Aug 2026 22:50:11 GMT"),
    ("content-type", "application/json"),
];

/// Captured from `pypi.org`: no budget signal, but a validator to poll against.
const PYPI: &[(&str, &str)] = &[
    ("date", "Sat, 01 Aug 2026 22:50:11 GMT"),
    ("etag", "\"PIda7csZtWyaGzro3RbDQA\""),
];

/// Captured from `api.bitbucket.org/2.0`, unauthenticated: eight requests
/// inside one two-second burst, as `(date, remaining, reset, served-by)`.
///
/// Five distinct nodes answered, and the count moves in both directions. Even a
/// single node - `449d70f80336`, at positions 2, 4 and 7 - reported 57, then 54,
/// then 58. Nothing here is a decrementing counter.
/// `(second, remaining, reset, served-by)`. The whole burst lands inside one
/// minute, so only the seconds differ.
const BITBUCKET_BURST: &[(&str, &str, &str, &str)] = &[
    ("34", "55", "3506", "95f53111acaf"),
    ("35", "57", "3506", "449d70f80336"),
    ("35", "59", "3505", "7121e3271ceb"),
    ("35", "54", "3505", "449d70f80336"),
    ("36", "59", "3505", "7121e3271ceb"),
    ("36", "59", "3504", "34438306376b"),
    ("36", "58", "3504", "449d70f80336"),
    ("36", "58", "3504", "bcf6d5697d77"),
];

/// The minute the burst was captured in.
const BITBUCKET_MINUTE: &str = "Sun, 02 Aug 2026 00:01:";

/// Bitbucket sends the current quota and the policy that produced it in one
/// field: 60 per window, window 3600s.
const BITBUCKET_LIMIT: &str = "60, 60;w=3600";

fn bitbucket(index: usize) -> Vec<(String, String)> {
    let (second, remaining, reset, served_by) = BITBUCKET_BURST[index];
    let date = format!("{BITBUCKET_MINUTE}{second} GMT");
    owned(&[
        ("date", date.as_str()),
        ("x-ratelimit-limit", BITBUCKET_LIMIT),
        ("x-ratelimit-remaining", remaining),
        ("x-ratelimit-reset", reset),
        ("x-served-by", served_by),
    ])
}

fn owned(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect()
}

/// Reads a fixture with no `observe:` block, so auto-detection is what is tested.
fn measured(pairs: &[(&str, &str)]) -> LimitHeaders {
    let captured = owned(pairs);
    let local_now = headers::read(&captured, None, Timestamp::EPOCH)
        .server_date
        .expect("every fixture carries a Date header");
    headers::read(&captured, None, local_now)
}

fn budget(ceiling: Option<u64>) -> Budget {
    Budget::new(
        BudgetPolicy::from_config(Some(&BudgetConfig {
            requests_per_hour: ceiling,
            observe: None,
            utilization: Some(0.8),
        }))
        .expect("valid policy"),
    )
}

#[test]
fn github_headers_yield_a_usable_budget_without_configuration() {
    let read = measured(GITHUB);
    assert_eq!(read.limit, Some(60));
    assert_eq!(read.remaining, Some(59));
    assert_eq!(read.reset_at, Some(Timestamp::from_secs(1_785_628_174)));
    assert_eq!(read.server_date, Some(Timestamp::from_secs(1_785_624_574)));

    // Exactly one hour of window, which is what GitHub documents.
    let window = read
        .reset_at
        .unwrap()
        .diff_millis(read.server_date.unwrap());
    assert_eq!(window, 3_600_000);
}

#[test]
fn the_github_counter_decrements_once_per_request() {
    // A budget you cannot watch move is a budget you cannot pace against.
    assert_eq!(measured(GITHUB).remaining, Some(59));
    assert_eq!(measured(GITHUB_NEXT).remaining, Some(58));
}

#[test]
fn gitlab_headers_yield_a_usable_budget_from_the_same_parse() {
    let read = measured(GITLAB);
    assert_eq!(read.limit, Some(500));
    assert_eq!(read.remaining, Some(499));
    assert_eq!(read.reset_at, Some(Timestamp::from_secs(1_785_624_600)));
    assert_eq!(read.server_date, Some(Timestamp::from_secs(1_785_624_577)));
}

#[test]
fn window_length_varies_by_two_orders_of_magnitude_between_apis() {
    // GitHub: 3600s. GitLab: a 60s window aligned to the minute, so mid-window
    // only 23 seconds were left. Both are correct readings of the same fields,
    // and pacing has to hold up across both without being told which is which.
    let github = measured(GITHUB);
    let gitlab = measured(GITLAB);

    let github_window = github
        .reset_at
        .unwrap()
        .diff_millis(github.server_date.unwrap());
    let gitlab_window = gitlab
        .reset_at
        .unwrap()
        .diff_millis(gitlab.server_date.unwrap());

    assert_eq!(github_window, 3_600_000);
    assert_eq!(gitlab_window, 23_000);
    assert_eq!(
        1_785_624_600 % 60,
        0,
        "GitLab's reset lands on a minute boundary"
    );
}

#[test]
fn both_apis_pace_at_a_sane_rate() {
    let mut github = budget(Some(5_000));
    let read = measured(GITHUB);
    let now = read.server_date.unwrap();
    github.observe(&read, now);
    // 59 requests over 3600s at 80% utilisation is roughly one every 76s.
    let rate = github.spend_rate(now);
    assert!(
        (0.01..0.02).contains(&rate),
        "github paced at {rate} req/s, which is not plausible for 59 requests an hour"
    );

    let mut gitlab = budget(Some(5_000));
    let read = measured(GITLAB);
    let now = read.server_date.unwrap();
    gitlab.observe(&read, now);
    // 499 requests over the 23s left in a minute-long window is fast, and
    // correct: the window really does refill every minute.
    let rate = gitlab.spend_rate(now);
    assert!(
        (10.0..30.0).contains(&rate),
        "gitlab paced at {rate} req/s against a 60s window"
    );
}

#[test]
fn an_api_that_publishes_nothing_falls_back_rather_than_assuming_plenty() {
    for fixture in [CRATES_IO, PYPI] {
        let read = measured(fixture);
        assert!(
            !read.is_informative(),
            "fixture should carry no budget signal"
        );
        assert_eq!(read.remaining, None);
        assert_eq!(read.reset_at, None);

        // The configured ceiling is the whole signal here, not a fallback.
        let mut budget = budget(Some(3_600));
        let now = read.server_date.unwrap();
        budget.observe(&read, now);
        assert!((budget.spend_rate(now) - 0.8).abs() < 1e-9);
    }
}

#[test]
fn an_unmeasurable_api_with_no_ceiling_still_gets_a_politeness_floor() {
    let read = measured(CRATES_IO);
    let now = read.server_date.unwrap();
    let mut budget = budget(None);
    budget.observe(&read, now);

    let rate = budget.spend_rate(now);
    assert!(
        rate > 0.0 && rate <= 1.0,
        "an unmeasurable API should be polled politely, got {rate} req/s"
    );
}

#[test]
fn the_window_estimator_converges_on_each_apis_real_window() {
    let mut github = budget(None);
    let read = measured(GITHUB);
    github.observe(&read, read.server_date.unwrap());
    assert_eq!(github.snapshot().window_secs, Some(3_600));

    let mut gitlab = budget(None);
    let read = measured(GITLAB);
    gitlab.observe(&read, read.server_date.unwrap());
    // Observed mid-window, so this is a lower bound that rises toward 60 as
    // more responses land. It must never exceed the truth.
    let estimate = gitlab.snapshot().window_secs.expect("an estimate");
    assert!(
        (1..=60).contains(&estimate),
        "estimated a {estimate}s window for a 60s limit"
    );
}

#[test]
fn a_validator_is_usable_even_when_the_budget_is_not() {
    // PyPI publishes no budget but does publish an ETag, so conditional
    // requests still make polling cheap there. The two features are
    // independent, and a source that supports only one still benefits.
    let captured = owned(PYPI);
    let etag = captured
        .iter()
        .find(|(name, _)| name == "etag")
        .map(|(_, value)| value.clone());
    assert_eq!(etag.as_deref(), Some("\"PIda7csZtWyaGzro3RbDQA\""));
    assert!(!measured(PYPI).is_informative());
}

#[test]
fn bitbucket_headers_are_read_despite_a_compound_limit_and_a_delta_reset() {
    let captured = bitbucket(0);
    let server_date = Timestamp::from_secs(1_785_628_894);
    let read = headers::read(&captured, None, server_date);

    // `60, 60;w=3600` is the current quota followed by the policy that set it.
    // The leading integer is the number to pace against.
    assert_eq!(read.limit, Some(60));
    assert_eq!(read.remaining, Some(55));
    assert_eq!(read.server_date, Some(server_date));

    // 3506 is seconds-until-reset, not a unix instant. Read as an instant it
    // would land in 1970 and the window would be thrown out as implausible,
    // dropping siphon back to the configured ceiling on an API that does in
    // fact publish a usable window.
    let reset_at = read.reset_at.expect("a reset");
    assert_eq!(reset_at.diff_millis(server_date), 3_506_000);

    // And it lands on the hour, so the window is wall-clock aligned like
    // GitLab's minute rather than rolling from first use.
    assert_eq!(reset_at.as_secs() % 3_600, 0);
}

#[test]
fn the_bitbucket_counter_moves_in_both_directions() {
    // GitHub's counter is worth pacing against because it decrements exactly
    // once per request. This one does not decrement at all reliably.
    let counts: Vec<u64> = (0..BITBUCKET_BURST.len())
        .map(|index| {
            headers::read(&bitbucket(index), None, Timestamp::EPOCH)
                .remaining
                .expect("a remaining count")
        })
        .collect();

    assert_eq!(counts, vec![55, 57, 59, 54, 59, 59, 58, 58]);
    assert!(
        counts.windows(2).any(|pair| pair[1] > pair[0]),
        "the finding this fixture exists to pin is that the count rises"
    );
}

#[test]
fn a_non_monotonic_counter_never_restores_spent_budget() {
    // The burst replayed through the governor, one request issued per response
    // exactly as a real walk would. Taking each newest count verbatim lets the
    // stalest node hand the allowance back, and because an observation also
    // clears the in-flight tally, siphon's belief climbs while it is spending -
    // then sprints into a 429 at the end of the window.
    let mut budget = budget(Some(60));
    let mut believed = u64::MAX;

    for index in 0..BITBUCKET_BURST.len() {
        let captured = bitbucket(index);
        let server_date = headers::read(&captured, None, Timestamp::EPOCH)
            .server_date
            .expect("every fixture carries a Date header");
        let read = headers::read(&captured, None, server_date);

        budget.record_request(server_date);
        budget.observe(&read, server_date);

        let now = budget.effective_remaining().expect("a measured budget");
        assert!(
            now <= believed,
            "belief rose from {believed} to {now} at response {index} while spending"
        );
        believed = now;
    }

    // Eight requests went out against a first reading of 55.
    assert_eq!(
        believed, 48,
        "eight requests should leave the belief eight below the first reading"
    );
}

#[test]
fn skew_correction_survives_a_badly_wrong_local_clock() {
    // The same GitHub response read on a machine whose clock is an hour fast.
    // Uncorrected, the window would read as zero and siphon would stop dead.
    let captured = owned(GITHUB);
    let server_now = Timestamp::from_secs(1_785_624_574);
    let local_now = server_now.saturating_add(Duration::from_secs(3_600));

    let read = headers::read(&captured, None, local_now);
    let mut budget = budget(Some(5_000));
    budget.observe(&read, local_now);

    assert_eq!(budget.skew(), Duration::from_secs(3_600));
    assert_eq!(
        budget.window_left(local_now),
        Some(Duration::from_secs(3_600)),
        "the window is an hour long regardless of what the local clock believes"
    );
}
