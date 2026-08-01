//! The daemon loop, and the one-shot commands.
//!
//! The loop is deliberately thin. It asks the governor what should run, starts
//! it, and reports what happened; every decision worth arguing about lives in
//! [`crate::governor`], not here.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use tokio::task::JoinSet;
use tracing::{error, info, warn};

use crate::cli::{scheduled, App};
use crate::governor::Decision;
use crate::store::RunStatus;
use crate::sync::{RunOptions, RunReport};

/// How long the loop sleeps when nothing is due and nothing is running.
const IDLE_POLL: Duration = Duration::from_secs(30);

/// How long in-flight runs get to finish after a shutdown signal.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(30);

/// Runs every stream under the governor until interrupted.
///
/// # Errors
///
/// Fails if the store becomes unusable; upstream failures are per-stream and
/// only logged.
pub async fn daemon(app: App) -> anyhow::Result<()> {
    let stream_count = app.config.streams.len();
    info!(
        streams = stream_count,
        sources = app.config.sources.len(),
        store = %app.config.store.path.display(),
        "siphon starting"
    );
    if stream_count == 0 {
        warn!("no streams configured; nothing to do");
        return Ok(());
    }

    let mut tasks: JoinSet<(String, Result<RunReport, crate::sync::SyncError>)> = JoinSet::new();
    let mut shutting_down = false;

    loop {
        if !shutting_down {
            while let Decision::Admit { stream, deficit } = app.governor.next_admission() {
                let Ok(handle) = app.stream(&stream) else {
                    // The governor only knows streams that came from the config,
                    // so this cannot happen; skip rather than panic if it does.
                    warn!(stream = %stream, "admitted a stream that is no longer configured");
                    app.governor.mark_finished(&stream, None);
                    break;
                };

                // Marked running here, before the task is spawned, so the next
                // admission decision cannot pick the same stream twice.
                app.governor.mark_running(&stream);
                info!(stream = %stream, deficit = format!("{deficit:.2}"), "admitted");

                let worker = Arc::clone(&app.worker);
                tasks.spawn(async move {
                    let report = worker.run(&handle, scheduled(deficit)).await;
                    (handle.name.clone(), report)
                });
            }
        }

        if shutting_down && tasks.is_empty() {
            break;
        }

        let delay = match app.governor.next_admission() {
            Decision::Idle { retry_in } => retry_in.unwrap_or(IDLE_POLL),
            Decision::Admit { .. } => Duration::ZERO,
        };

        tokio::select! {
            biased;

            finished = tasks.join_next(), if !tasks.is_empty() => {
                if let Some(result) = finished {
                    settle(&app, result).await?;
                }
            }

            signal = tokio::signal::ctrl_c(), if !shutting_down => {
                if signal.is_ok() {
                    info!(
                        in_flight = tasks.len(),
                        "shutdown requested; letting in-flight runs finish"
                    );
                    shutting_down = true;
                    if tasks.is_empty() {
                        break;
                    }
                }
            }

            () = tokio::time::sleep(delay), if !shutting_down && !delay.is_zero() => {}
        }

        if shutting_down {
            // Anything still running gets a bounded grace period; page writes
            // are transactional, so an abort loses at most the current page.
            let deadline = tokio::time::sleep(SHUTDOWN_GRACE);
            tokio::pin!(deadline);
            while !tasks.is_empty() {
                tokio::select! {
                    finished = tasks.join_next() => {
                        if let Some(result) = finished {
                            settle(&app, result).await?;
                        }
                    }
                    () = &mut deadline => {
                        warn!(in_flight = tasks.len(), "grace period elapsed; aborting runs");
                        tasks.abort_all();
                        break;
                    }
                }
            }
            break;
        }
    }

    app.persist_budgets().await?;
    info!("siphon stopped");
    Ok(())
}

/// Records a finished run: freshness clock, budget, and the log line.
async fn settle(
    app: &App,
    result: Result<(String, Result<RunReport, crate::sync::SyncError>), tokio::task::JoinError>,
) -> anyhow::Result<()> {
    let (stream, outcome) = match result {
        Ok(pair) => pair,
        Err(error) if error.is_cancelled() => return Ok(()),
        Err(error) => {
            error!(%error, "a stream task panicked");
            return Ok(());
        }
    };

    match outcome {
        Ok(report) => {
            app.governor.mark_finished(&stream, report.success_at);
            log_report(&stream, &report);
        }
        Err(error) => {
            // The stream stays marked finished so it keeps accruing deficit and
            // shows up in `siphon status` as a breach, not just a log line.
            app.governor.mark_finished(&stream, None);
            error!(stream = %stream, %error, "run failed");
        }
    }

    app.persist_budgets()
        .await
        .context("persisting the measured budget")
}

fn log_report(stream: &str, report: &RunReport) {
    match report.status {
        RunStatus::Ok | RunStatus::NotModified => info!(
            stream = %stream,
            status = report.status.as_str(),
            pages = report.pages,
            records = report.records,
            new = report.records_new,
            changed = report.records_changed,
            requests = report.requests,
            reason = report.stop_reason.as_deref().unwrap_or("-"),
            "run complete"
        ),
        RunStatus::Partial => warn!(
            stream = %stream,
            pages = report.pages,
            records = report.records,
            quarantined = report.quarantined,
            requests = report.requests,
            reason = report.stop_reason.as_deref().unwrap_or("-"),
            "run partial; the mirror has a known gap"
        ),
        RunStatus::Failed => error!(
            stream = %stream,
            requests = report.requests,
            error = report.error.as_deref().unwrap_or("unknown"),
            "run failed"
        ),
    }
}

/// Syncs one stream once, bypassing admission but not the rate-limit budget.
///
/// # Errors
///
/// Fails if the stream is unknown or the store is unusable.
pub async fn once(app: &App, stream: &str, full: bool) -> anyhow::Result<()> {
    let handle = app.stream(stream)?;
    let deficit = app.governor.deficit(stream).unwrap_or_default();

    app.governor.mark_running(stream);
    let report = app
        .worker
        .run(&handle, RunOptions { deficit, full })
        .await
        .with_context(|| format!("syncing stream `{stream}`"))?;
    app.governor.mark_finished(stream, report.success_at);
    app.persist_budgets().await?;

    log_report(stream, &report);
    println!(
        "{stream}: {} - {} pages, {} records ({} new, {} changed), {} requests",
        report.status.as_str(),
        report.pages,
        report.records,
        report.records_new,
        report.records_changed,
        report.requests
    );
    if let Some(error) = &report.error {
        println!("  error: {error}");
        anyhow::bail!("stream `{stream}` did not complete");
    }
    if report.quarantined > 0 {
        println!(
            "  {} page(s) quarantined; the mirror has a gap",
            report.quarantined
        );
    }
    Ok(())
}

/// Fetches a page and prints what would be written, writing nothing.
///
/// # Errors
///
/// Fails if the stream is unknown or a request cannot be built.
pub async fn probe(app: &App, stream: &str, pages: u32) -> anyhow::Result<()> {
    let handle = app.stream(stream)?;
    let report = app
        .worker
        .probe(&handle, pages.max(1))
        .await
        .with_context(|| format!("testing stream `{stream}`"))?;

    for url in &report.urls {
        println!("GET {url}");
    }
    println!();

    for record in &report.records {
        let cursor = record
            .cursor()
            .map_or_else(|| "-".to_owned(), |c| c.as_str().to_owned());
        println!(
            "pk={} cursor={} {}",
            record.pk(),
            cursor,
            record.payload_json()
        );
    }

    println!();
    println!(
        "{} record(s) over {} page(s); stopped: {}",
        report.records.len(),
        report.pages,
        report.stop_reason.map_or_else(
            || "page limit reached".to_owned(),
            |r| r.as_str().to_owned()
        )
    );
    println!("nothing was written");

    if let Some(error) = report.error {
        println!("error: {error}");
        anyhow::bail!("stream `{stream}` did not fetch cleanly");
    }
    Ok(())
}
