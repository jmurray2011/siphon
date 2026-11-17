//! `siphon status` and `siphon validate`.
//!
//! Status reports SLO attainment, not just last-run success. That is the number
//! an operator actually wants, and it is only computable because freshness is
//! declared as a requirement rather than implied by a cron expression.

use std::future::Future;
use std::path::Path;
use std::time::Duration;

use anyhow::Context;

use crate::cli::App;
use crate::clock::Timestamp;
use crate::config::duration::format_duration;
use crate::config::Config;
use crate::governor::{attainment, Priority};
use crate::store::{Checkpoint, RunSummary, Store, StoreError};

/// The window `ATTAIN` is computed over.
const ATTAINMENT_WINDOW: Duration = Duration::from_secs(7 * 24 * 3_600);

/// What reporting needs from a store, which is strictly less than a store does.
///
/// Reporting reads; it never writes, never checkpoints and never quarantines.
/// Naming that separately is what lets a second store target be checked against
/// the five questions `status` actually asks, rather than against the whole
/// surface of whichever implementation happened to exist first.
///
/// The methods spell out `impl Future + Send` rather than using `async fn`,
/// because an `async fn` in a public trait leaves the returned future without a
/// `Send` bound and the caller no way to add one - which the governor's spawned
/// tasks need.
pub trait ReportStore {
    /// The stream's stored checkpoint, defaulted if it has never run.
    fn checkpoint(
        &self,
        stream: &str,
    ) -> impl Future<Output = Result<Checkpoint, StoreError>> + Send;

    /// When the stream first ran, so a young stream is not scored over a window
    /// that predates it.
    fn first_run_at(
        &self,
        stream: &str,
    ) -> impl Future<Output = Result<Option<Timestamp>, StoreError>> + Send;

    /// Timestamps of successful runs at or after `since`.
    fn successes_since(
        &self,
        stream: &str,
        since: Timestamp,
    ) -> impl Future<Output = Result<Vec<Timestamp>, StoreError>> + Send;

    /// How many pages of this stream are quarantined.
    fn quarantined_count(
        &self,
        stream: &str,
    ) -> impl Future<Output = Result<u32, StoreError>> + Send;

    /// The most recent run, if there has been one.
    fn last_run(
        &self,
        stream: &str,
    ) -> impl Future<Output = Result<Option<RunSummary>, StoreError>> + Send;
}

impl ReportStore for Store {
    fn checkpoint(
        &self,
        stream: &str,
    ) -> impl Future<Output = Result<Checkpoint, StoreError>> + Send {
        Self::checkpoint(self, stream)
    }

    fn first_run_at(
        &self,
        stream: &str,
    ) -> impl Future<Output = Result<Option<Timestamp>, StoreError>> + Send {
        Self::first_run_at(self, stream)
    }

    fn successes_since(
        &self,
        stream: &str,
        since: Timestamp,
    ) -> impl Future<Output = Result<Vec<Timestamp>, StoreError>> + Send {
        Self::successes_since(self, stream, since)
    }

    fn quarantined_count(
        &self,
        stream: &str,
    ) -> impl Future<Output = Result<u32, StoreError>> + Send {
        Self::quarantined_count(self, stream)
    }

    fn last_run(
        &self,
        stream: &str,
    ) -> impl Future<Output = Result<Option<RunSummary>, StoreError>> + Send {
        Self::last_run(self, stream)
    }
}

/// Parses and checks a config without touching the network.
///
/// # Errors
///
/// Fails if the config does not load or does not validate.
pub fn validate(path: &Path) -> anyhow::Result<()> {
    let config =
        Config::load(path).with_context(|| format!("loading config from {}", path.display()))?;

    println!(
        "{}: {} source(s), {} stream(s)",
        path.display(),
        config.sources.len(),
        config.streams.len()
    );

    let lints = config.lint();
    if lints.is_empty() {
        println!("no findings");
    } else {
        println!();
        for lint in &lints {
            println!("warning [{}]: {}", lint.subject, lint.message);
        }
    }

    println!();
    println!("credentials are resolved at run time, not here; this check made no network calls");
    Ok(())
}

/// Prints per-stream SLO attainment, lag and budget.
///
/// # Errors
///
/// Fails if the store cannot be queried.
pub async fn report(app: &App) -> anyhow::Result<()> {
    let now = app.clock.now();
    let window_start = now.saturating_sub(ATTAINMENT_WINDOW);

    println!(
        "{}",
        stream_row(
            "STREAM",
            "FRESHNESS",
            "LAG",
            "ATTAIN(7d)",
            "LAST OK",
            "STATE"
        )
    );

    for stream in &app.config.streams {
        let read = read_stream(&app.store, stream, now, window_start).await?;
        let last_ok = read.last_ok;
        let lag = last_ok.map(|at| now.saturating_since(at));
        let attain = read.attain;

        let state = describe_state(
            stream.priority,
            stream.freshness,
            lag,
            read.quarantined,
            read.ran,
            read.last_status.as_deref(),
        );

        println!(
            "{}",
            stream_row(
                &truncate(&stream.name, 18),
                &stream
                    .freshness
                    .map_or_else(|| "-".to_owned(), format_duration),
                &lag.map_or_else(|| "-".to_owned(), format_duration),
                &attain.unwrap_or_else(|| "-".to_owned()),
                &last_ok.map_or_else(
                    || "never".to_owned(),
                    |at| format!("{} ago", format_duration(now.saturating_since(at)))
                ),
                &state,
            )
        );
    }

    println!();
    print_budgets(app);
    Ok(())
}

/// What `status` reads from the store for one stream.
struct StreamRead {
    last_ok: Option<Timestamp>,
    attain: Option<String>,
    quarantined: u32,
    ran: bool,
    last_status: Option<String>,
}

/// Reads one stream's status. Generic over the store so a second target is
/// checked against these five questions and nothing else.
async fn read_stream(
    store: &impl ReportStore,
    stream: &crate::config::Stream,
    now: Timestamp,
    window_start: Timestamp,
) -> Result<StreamRead, StoreError> {
    let checkpoint = store.checkpoint(&stream.name).await?;
    let last_ok = checkpoint.last_success_at;
    // Counts runs that never finished, unlike `last_run`, so an interrupted
    // first run is not mistaken for one that never started.
    let first_run = store.first_run_at(&stream.name).await?;

    let attain = match stream.freshness {
        Some(freshness) => {
            // A stream is only accountable for the time it has existed:
            // scoring one that started an hour ago against seven days would
            // report a perfectly healthy stream as a near-total failure.
            let from = first_run.map_or(window_start, |at| at.max(window_start));

            // Coverage bought just before the window still counts inside it,
            // so the query reaches back one target beyond the window edge.
            let successes = store
                .successes_since(&stream.name, from.saturating_sub(freshness))
                .await?;
            attainment(&successes, from, now, freshness)
                .map(|ratio| format!("{:.1}%", ratio * 100.0))
        }
        None => None,
    };

    let last_run = store.last_run(&stream.name).await?;
    Ok(StreamRead {
        last_ok,
        attain,
        quarantined: store.quarantined_count(&stream.name).await?,
        ran: first_run.is_some(),
        last_status: last_run.and_then(|run| run.status),
    })
}

fn stream_row(
    name: &str,
    freshness: &str,
    lag: &str,
    attain: &str,
    last_ok: &str,
    state: &str,
) -> String {
    format!("{name:<18} {freshness:>9} {lag:>9} {attain:>11} {last_ok:>9}  {state}")
}

fn budget_row(
    source: &str,
    remaining: &str,
    window: &str,
    rate: &str,
    skew: &str,
    state: &str,
) -> String {
    format!("{source:<18} {remaining:>12} {window:>10} {rate:>12} {skew:>8}  {state}")
}

fn print_budgets(app: &App) {
    println!(
        "{}",
        budget_row("SOURCE", "REMAINING", "WINDOW", "SPEND/S", "SKEW", "STATE")
    );
    for name in app.config.sources.keys() {
        let Some(view) = app.governor.budget_view(name) else {
            continue;
        };
        let remaining = match (view.remaining, view.limit) {
            (Some(remaining), Some(limit)) => format!("{remaining}/{limit}"),
            (Some(remaining), None) => remaining.to_string(),
            _ => "unmeasured".to_owned(),
        };
        println!(
            "{}",
            budget_row(
                &truncate(name, 18),
                &remaining,
                &view
                    .window_left
                    .map_or_else(|| "-".to_owned(), format_duration),
                &format!("{:.2}", view.spend_rate),
                &format_duration(view.skew),
                if view.ratcheted {
                    "RATCHETED after rate limiting"
                } else {
                    "ok"
                }
            )
        );
    }
}

fn describe_state(
    priority: Priority,
    freshness: Option<Duration>,
    lag: Option<Duration>,
    quarantined: u32,
    ran: bool,
    last_status: Option<&str>,
) -> String {
    if quarantined > 0 {
        return format!("PARTIAL: {quarantined} page(s) quarantined");
    }
    if last_status == Some("failed") {
        return "FAILED: see the run log".to_owned();
    }
    match (freshness, lag) {
        (None, _) if priority == Priority::BestEffort => "backfilling (surplus)".to_owned(),
        (None, _) => "no target".to_owned(),
        // No success yet: say whether anything ran at all. A run with no
        // status is still going, or was killed before it could record one.
        (Some(_), None) => match (ran, last_status) {
            (false, _) => "never run",
            (true, None) => "in progress or interrupted",
            (true, Some(_)) => "no successful run yet",
        }
        .to_owned(),
        (Some(target), Some(lag)) if lag > target => {
            format!("BREACH: {} past target", format_duration(lag - target))
        }
        (Some(_), Some(_)) => "ok".to_owned(),
    }
}

fn truncate(text: &str, width: usize) -> String {
    if text.chars().count() <= width {
        return text.to_owned();
    }
    text.chars()
        .take(width.saturating_sub(1))
        .collect::<String>()
        + "\u{2026}"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_reports_a_breach_with_how_far_past() {
        let state = describe_state(
            Priority::High,
            Some(Duration::from_secs(900)),
            Some(Duration::from_secs(1_200)),
            0,
            true,
            Some("ok"),
        );
        assert_eq!(state, "BREACH: 5m00s past target");
    }

    #[test]
    fn state_reports_ok_inside_the_target() {
        let state = describe_state(
            Priority::High,
            Some(Duration::from_secs(900)),
            Some(Duration::from_secs(120)),
            0,
            true,
            Some("ok"),
        );
        assert_eq!(state, "ok");
    }

    #[test]
    fn a_quarantined_page_outranks_everything_else_in_the_report() {
        let state = describe_state(
            Priority::High,
            Some(Duration::from_secs(900)),
            Some(Duration::from_secs(10)),
            2,
            true,
            Some("partial"),
        );
        assert_eq!(state, "PARTIAL: 2 page(s) quarantined");
    }

    #[test]
    fn a_backfill_is_labelled_rather_than_shown_as_breaching() {
        let state = describe_state(
            Priority::BestEffort,
            None,
            Some(Duration::from_secs(9_999)),
            0,
            true,
            Some("ok"),
        );
        assert_eq!(state, "backfilling (surplus)");
    }

    #[test]
    fn a_stream_that_never_ran_says_so() {
        let state = describe_state(
            Priority::Normal,
            Some(Duration::from_secs(60)),
            None,
            0,
            false,
            None,
        );
        assert_eq!(state, "never run");
    }

    #[test]
    fn an_unfinished_run_is_not_reported_as_never_run() {
        let state = describe_state(
            Priority::Normal,
            Some(Duration::from_secs(60)),
            None,
            0,
            true,
            None,
        );
        assert_eq!(state, "in progress or interrupted");
    }

    #[test]
    fn finished_runs_without_a_success_say_so() {
        let state = describe_state(
            Priority::Normal,
            Some(Duration::from_secs(60)),
            None,
            0,
            true,
            Some("partial"),
        );
        assert_eq!(state, "no successful run yet");
    }

    #[test]
    fn long_names_are_truncated_not_wrapped() {
        assert_eq!(truncate("short", 18), "short");
        assert_eq!(
            truncate("a_very_long_stream_name_here", 10),
            "a_very_lo\u{2026}"
        );
    }
}
