//! End-to-end tests: real HTTP client, real pagination, real SQLite writes.
//!
//! These are the tests that would catch a design claim being false rather than
//! a function being wrong - resume is exact, a 304 is free freshness, a poison
//! page does not block a stream forever.

// Integration tests are their own crates, so `allow-unwrap-in-tests` in
// clippy.toml does not reach helpers that sit outside a `#[test]` function.
// Panicking on a broken fixture is the correct behaviour here.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use common::{MockApi, Reply};
use siphon::cli::{App, Storage};
use siphon::store::{PageWrite, Progress, RunStatus, Store};
use siphon::sync::RunOptions;

struct Fixture {
    _dir: tempfile::TempDir,
    config_path: PathBuf,
    db_path: PathBuf,
}

/// Writes a config pointing at the mock server into a temp directory.
fn fixture(base_url: &str, stream_yaml: &str) -> Fixture {
    let dir = tempfile::tempdir().expect("temp dir");
    let db_path = dir.path().join("mirror.db");
    let config_path = dir.path().join("siphon.yaml");

    let config = format!(
        r"version: 1
store:
  type: sqlite
  path: {db}
sources:
  api:
    base_url: {base_url}
    budget:
      # High enough that pacing is not what these tests are measuring.
      requests_per_hour: 3600000
streams:
{stream_yaml}
",
        db = db_path.display(),
    );
    std::fs::write(&config_path, config).expect("config writes");

    Fixture {
        _dir: dir,
        config_path,
        db_path,
    }
}

async fn app(fixture: &Fixture) -> App {
    App::build(&fixture.config_path, Storage::OnDisk)
        .await
        .expect("app builds")
}

async fn sync(app: &App, stream: &str, full: bool) -> siphon::sync::RunReport {
    let handle = app.stream(stream).expect("stream is configured");
    app.worker
        .run(&handle, RunOptions { deficit: 0.0, full })
        .await
        .expect("run completes")
}

const LINK_STREAM: &str = r"  - name: issues
    source: api
    freshness: 15m
    priority: high
    request:
      path: /issues
      params:
        per_page: 2
    pagination:
      strategy: link_header
      rel: next
    records:
      path: $
      primary_key: $.id
      cursor:
        field: $.updated_at
        type: rfc3339
        param: since
";

const OFFSET_STREAM: &str = r"  - name: items
    source: api
    freshness: 15m
    request:
      path: /items
    pagination:
      strategy: offset
      page_size: 2
    records:
      path: $.data
      primary_key: $.id
";

#[tokio::test]
async fn walks_link_header_pagination_and_lands_every_record() {
    let api = MockApi::spawn(|request| {
        let page: u32 = request
            .param("page")
            .and_then(|p| p.parse().ok())
            .unwrap_or(1);
        let base = request
            .header("host")
            .map_or_else(|| "127.0.0.1".to_owned(), str::to_owned);
        match page {
            1 => Reply::json(
                r#"[{"id":1,"updated_at":"2024-01-01T00:00:00Z"},
                    {"id":2,"updated_at":"2024-01-02T00:00:00Z"}]"#,
            )
            .with_header(
                "Link",
                format!("<http://{base}/issues?page=2>; rel=\"next\""),
            ),
            2 => Reply::json(
                r#"[{"id":3,"updated_at":"2024-01-03T00:00:00Z"},
                    {"id":4,"updated_at":"2024-01-04T00:00:00Z"}]"#,
            )
            .with_header(
                "Link",
                format!("<http://{base}/issues?page=3>; rel=\"next\""),
            ),
            _ => Reply::json(r#"[{"id":5,"updated_at":"2024-01-05T00:00:00Z"}]"#),
        }
    })
    .await;

    let fixture = fixture(&api.base_url(), LINK_STREAM);
    let app = app(&fixture).await;
    let report = sync(&app, "issues", false).await;

    assert_eq!(report.status, RunStatus::Ok, "{report:?}");
    assert_eq!(report.pages, 3);
    assert_eq!(report.records, 5);
    assert_eq!(report.records_new, 5);
    assert_eq!(api.request_count(), 3);

    let checkpoint = app.store.checkpoint("issues").await.expect("checkpoint");
    assert_eq!(checkpoint.page_state, None, "the walk should be complete");
    assert_eq!(
        checkpoint.cursor_val.as_deref(),
        Some("2024-01-05T00:00:00Z"),
        "the high-water mark should be the newest cursor seen"
    );
    assert_eq!(app.store.record_count("issues").await.expect("count"), 5);
}

#[tokio::test]
async fn a_second_sync_sends_the_stored_cursor_and_upserts_rather_than_duplicating() {
    let api =
        MockApi::spawn(|_| Reply::json(r#"[{"id":1,"updated_at":"2024-01-01T00:00:00Z"}]"#)).await;

    let fixture = fixture(&api.base_url(), LINK_STREAM);
    let app = app(&fixture).await;

    sync(&app, "issues", false).await;
    let second = sync(&app, "issues", false).await;

    assert_eq!(second.records_new, 0, "the record was already stored");
    assert_eq!(second.records_changed, 0, "and it had not changed");
    assert_eq!(app.store.record_count("issues").await.expect("count"), 1);

    let requests = api.requests();
    assert_eq!(
        requests[0].param("since"),
        None,
        "the first sync has no mark"
    );
    assert_eq!(
        requests[1].param("since"),
        Some("2024-01-01T00:00:00Z"),
        "the second sync resumes from the high-water mark"
    );
}

/// A stream shaped like an API whose filter is a query language: the mark is a
/// clause inside one parameter, and the API accepts a narrower timestamp than
/// it emits.
const QUERY_LANGUAGE_STREAM: &str = r#"  - name: issues
    source: api
    freshness: 15m
    request:
      path: /search
      params:
        query: 'project = {{ project }} AND updated >= "{{ cursor }}" ORDER BY updated ASC'
    vars:
      project: ABC
    pagination:
      strategy: cursor
      cursor_param: nextPageToken
      cursor_path: $.nextPageToken
    records:
      path: $.issues
      primary_key: $.key
      cursor:
        field: $.fields.updated
        type: rfc3339
        format: "%Y-%m-%d %H:%M"
        initial: "1970-01-01 00:00"
"#;

#[tokio::test]
async fn a_query_language_stream_carries_its_mark_inside_the_query() {
    let api = MockApi::spawn(|request| {
        // Only ever answer with issues the query actually asks for. An API that
        // silently matches nothing on a malformed mark is exactly the failure
        // this test exists to catch, so the mock is strict about it.
        let query = request.param("query").unwrap_or_default().to_owned();
        if !query.contains("project = ABC") {
            return Reply::status(400);
        }
        if query.contains("1970-01-01 00:00") {
            return Reply::json(
                r#"{"issues":[{"key":"ABC-1","fields":{"updated":"2021-05-04T22:10:32.221+0100"}}]}"#,
            );
        }
        if query.contains("2021-05-04 22:10") {
            return Reply::json(r#"{"issues":[]}"#);
        }
        // Any other mark means the watermark was rendered wrong.
        Reply::json(r#"{"issues":[],"unexpected_mark":true}"#)
    })
    .await;

    let fixture = fixture(&api.base_url(), QUERY_LANGUAGE_STREAM);
    let app = app(&fixture).await;

    let first = sync(&app, "issues", false).await;
    assert_eq!(first.status, RunStatus::Ok, "{first:?}");
    assert_eq!(first.records_new, 1);

    let second = sync(&app, "issues", false).await;
    assert_eq!(second.status, RunStatus::Ok, "{second:?}");

    let queries: Vec<String> = api
        .requests()
        .iter()
        .filter_map(|r| r.param("query").map(str::to_owned))
        .collect();

    assert!(
        queries[0].contains(r#"updated >= "1970-01-01 00:00""#),
        "the first run should use the initial mark, got: {}",
        queries[0]
    );
    assert!(
        queries
            .last()
            .is_some_and(|q| q.contains(r#"updated >= "2021-05-04 22:10""#)),
        "the second run should use the stored mark, narrowed to what the API accepts, got: {:?}",
        queries.last()
    );

    // The mark is stored verbatim as the upstream emitted it, and only narrowed
    // on the way back out - so nothing is lost to the format.
    let checkpoint = app.store.checkpoint("issues").await.expect("checkpoint");
    assert_eq!(
        checkpoint.cursor_val.as_deref(),
        Some("2021-05-04T22:10:32.221+0100")
    );
}

#[tokio::test]
async fn a_basic_format_offset_still_advances_the_mark() {
    // The regression this guards: a `+0100` offset that fails to parse leaves
    // the high-water mark null, so every run re-walks the whole history while
    // reporting success.
    let api = MockApi::spawn(|request| {
        if request.param("query").unwrap_or_default().contains("1970-01-01") {
            return Reply::json(
                r#"{"issues":[{"key":"ABC-1","fields":{"updated":"2021-05-04T22:10:32.221+0100"}}]}"#,
            );
        }
        Reply::json(r#"{"issues":[]}"#)
    })
    .await;

    let fixture = fixture(&api.base_url(), QUERY_LANGUAGE_STREAM);
    let app = app(&fixture).await;
    sync(&app, "issues", false).await;

    let checkpoint = app.store.checkpoint("issues").await.expect("checkpoint");
    assert!(
        checkpoint.cursor_val.is_some(),
        "the mark must advance even when the offset is basic-format"
    );
}

#[tokio::test]
async fn a_crash_mid_pagination_resumes_at_the_next_page() {
    let api = MockApi::spawn(|request| {
        let offset: u64 = request
            .param("offset")
            .and_then(|o| o.parse().ok())
            .unwrap_or(0);
        if offset >= 6 {
            return Reply::json(r#"{"data":[]}"#);
        }
        Reply::json(format!(
            r#"{{"data":[{{"id":{}}},{{"id":{}}}]}}"#,
            offset + 1,
            offset + 2
        ))
    })
    .await;

    let fixture = fixture(&api.base_url(), OFFSET_STREAM);

    // Stand in for a process that died after committing pages 1 and 2: the
    // records are stored and the checkpoint says the next page is at offset 4.
    {
        let store = Store::open(&fixture.db_path).expect("store opens");
        store
            .write_page(PageWrite {
                stream: "items".to_owned(),
                records: Vec::new(),
                progress: Progress::Resume(r#"{"kind":"offset","offset":4}"#.to_owned()),
                at: siphon::clock::Timestamp::from_secs(1),
            })
            .await
            .expect("checkpoint writes");
    }

    let app = app(&fixture).await;
    let report = sync(&app, "items", false).await;
    assert_eq!(report.status, RunStatus::Ok, "{report:?}");

    let offsets: Vec<Option<String>> = api
        .requests()
        .iter()
        .map(|r| r.param("offset").map(str::to_owned))
        .collect();
    assert_eq!(
        offsets,
        vec![Some("4".to_owned()), Some("6".to_owned())],
        "the resumed walk must not re-fetch pages 1 and 2"
    );
}

#[tokio::test]
async fn full_ignores_the_resume_point_and_walks_from_the_start() {
    let api = MockApi::spawn(|request| {
        let offset: u64 = request
            .param("offset")
            .and_then(|o| o.parse().ok())
            .unwrap_or(0);
        if offset >= 2 {
            return Reply::json(r#"{"data":[]}"#);
        }
        Reply::json(r#"{"data":[{"id":1},{"id":2}]}"#)
    })
    .await;

    let fixture = fixture(&api.base_url(), OFFSET_STREAM);
    {
        let store = Store::open(&fixture.db_path).expect("store opens");
        store
            .write_page(PageWrite {
                stream: "items".to_owned(),
                records: Vec::new(),
                progress: Progress::Resume(r#"{"kind":"offset","offset":400}"#.to_owned()),
                at: siphon::clock::Timestamp::from_secs(1),
            })
            .await
            .expect("checkpoint writes");
    }

    let app = app(&fixture).await;
    sync(&app, "items", true).await;

    assert_eq!(
        api.requests()[0].param("offset"),
        Some("0"),
        "--full should start over"
    );
}

#[tokio::test]
async fn a_not_modified_response_is_free_freshness() {
    let api = MockApi::spawn(|request| {
        if request.header("if-none-match") == Some("\"v1\"") {
            return Reply::status(304).with_header("ETag", "\"v1\"");
        }
        Reply::json(r#"[{"id":1,"updated_at":"2024-01-01T00:00:00Z"}]"#)
            .with_header("ETag", "\"v1\"")
    })
    .await;

    let fixture = fixture(&api.base_url(), LINK_STREAM);
    let app = app(&fixture).await;

    // Validators are keyed by request URL, and the first sync moves the cursor
    // from nothing to a real mark - which changes the URL. It is the *steady
    // state*, once the stream is caught up and its cursor stops moving, where
    // polling becomes conditional and therefore close to free.
    assert_eq!(sync(&app, "issues", false).await.status, RunStatus::Ok);
    assert_eq!(sync(&app, "issues", false).await.status, RunStatus::Ok);

    let before = app
        .store
        .checkpoint("issues")
        .await
        .expect("checkpoint")
        .last_success_at;

    let steady = sync(&app, "issues", false).await;
    assert_eq!(steady.status, RunStatus::NotModified, "{steady:?}");
    assert_eq!(steady.not_modified, 1);
    assert_eq!(steady.records, 0, "a 304 writes nothing");
    assert_eq!(steady.requests, 1, "and costs one request");

    assert_eq!(
        api.requests()
            .last()
            .and_then(|r| r.header("if-none-match")),
        Some("\"v1\""),
        "the steady-state poll should be conditional"
    );

    // The data is confirmed current, so freshness advances even though nothing
    // was written. Getting this backwards makes every stream look permanently
    // stale and the governor spends the whole budget chasing it.
    let after = app
        .store
        .checkpoint("issues")
        .await
        .expect("checkpoint")
        .last_success_at;
    assert!(
        after > before,
        "a 304 must advance the freshness clock: {before:?} -> {after:?}"
    );
    assert_eq!(app.store.record_count("issues").await.expect("count"), 1);
}

#[tokio::test]
async fn a_poison_page_is_quarantined_and_the_walk_steps_over_it() {
    let api = MockApi::spawn(|request| {
        let offset: u64 = request
            .param("offset")
            .and_then(|o| o.parse().ok())
            .unwrap_or(0);
        match offset {
            0 => Reply::json(r#"{"data":[{"id":1},{"id":2}]}"#),
            2 => Reply::status(422).with_header("Content-Type", "application/json"),
            4 => Reply::json(r#"{"data":[{"id":5},{"id":6}]}"#),
            _ => Reply::json(r#"{"data":[]}"#),
        }
    })
    .await;

    let fixture = fixture(&api.base_url(), OFFSET_STREAM);
    let app = app(&fixture).await;
    let report = sync(&app, "items", false).await;

    assert_eq!(
        report.status,
        RunStatus::Partial,
        "a stream with a gap never reports ok: {report:?}"
    );
    assert_eq!(report.quarantined, 1);
    assert_eq!(
        app.store.quarantined_count("items").await.expect("count"),
        1
    );

    // Pages either side of the poisoned one still landed.
    assert_eq!(app.store.record_count("items").await.expect("count"), 4);

    // And the deficit clock advanced, so the governor does not keep
    // preferentially funding the one stream guaranteed to waste the budget.
    let checkpoint = app.store.checkpoint("items").await.expect("checkpoint");
    assert!(checkpoint.last_success_at.is_some());
}

#[tokio::test]
async fn a_quarantined_page_is_not_paid_for_twice() {
    let api = MockApi::spawn(|request| {
        let offset: u64 = request
            .param("offset")
            .and_then(|o| o.parse().ok())
            .unwrap_or(0);
        match offset {
            0 => Reply::json(r#"{"data":[{"id":1},{"id":2}]}"#),
            2 => Reply::status(422),
            _ => Reply::json(r#"{"data":[]}"#),
        }
    })
    .await;

    let fixture = fixture(&api.base_url(), OFFSET_STREAM);
    let app = app(&fixture).await;

    let first = sync(&app, "items", false).await;
    let first_requests = api.request_count();
    assert_eq!(first.quarantined, 1);

    let second = sync(&app, "items", false).await;
    assert_eq!(second.status, RunStatus::Partial);
    assert_eq!(
        second.quarantined, 0,
        "the page is already quarantined, not re-quarantined"
    );

    let spent_on_the_poison_page = api.request_count() - first_requests;
    assert!(
        spent_on_the_poison_page <= 2,
        "the second run spent {spent_on_the_poison_page} requests; it should stop at the known gap"
    );
}

#[tokio::test]
async fn a_transient_failure_is_retried_and_then_succeeds() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&attempts);
    let api = MockApi::spawn(move |_| {
        if counter.fetch_add(1, Ordering::SeqCst) == 0 {
            return Reply::status(503);
        }
        Reply::json(r#"{"data":[]}"#)
    })
    .await;

    let fixture = fixture(&api.base_url(), OFFSET_STREAM);
    let app = app(&fixture).await;
    let report = sync(&app, "items", false).await;

    assert_eq!(report.status, RunStatus::Ok, "{report:?}");
    assert_eq!(report.requests, 2, "one failure, one success");
}

#[tokio::test]
async fn rate_limiting_ratchets_the_budget_and_the_run_still_completes() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&attempts);
    let api = MockApi::spawn(move |_| {
        if counter.fetch_add(1, Ordering::SeqCst) == 0 {
            return Reply::status(429)
                .with_header("Retry-After", "0")
                .with_header("X-RateLimit-Remaining", "0")
                .with_header("X-RateLimit-Limit", "5000");
        }
        Reply::json(r#"{"data":[]}"#)
            .with_header("X-RateLimit-Remaining", "4999")
            .with_header("X-RateLimit-Limit", "5000")
    })
    .await;

    let fixture = fixture(&api.base_url(), OFFSET_STREAM);
    let app = app(&fixture).await;
    let report = sync(&app, "items", false).await;

    assert_eq!(report.status, RunStatus::Ok, "{report:?}");
    let view = app
        .governor
        .budget_view("api")
        .expect("source is registered");
    assert_eq!(view.limit, Some(5_000));
    assert!(
        view.ratcheted,
        "a 429 should still be suppressing the spend rate"
    );
}

#[tokio::test]
async fn the_measured_budget_is_persisted_for_the_next_start() {
    let api = MockApi::spawn(|_| {
        Reply::json(r#"{"data":[]}"#)
            .with_header("X-RateLimit-Limit", "5000")
            .with_header("X-RateLimit-Remaining", "4321")
            .with_header("X-RateLimit-Reset", "4102444800")
    })
    .await;

    let fixture = fixture(&api.base_url(), OFFSET_STREAM);
    {
        let app = app(&fixture).await;
        sync(&app, "items", false).await;
        app.persist_budgets().await.expect("budgets persist");
    }

    let store = Store::open(&fixture.db_path).expect("store reopens");
    let snapshot = store
        .load_budget("api")
        .await
        .expect("query works")
        .expect("a budget was learned");
    assert_eq!(snapshot.observed_limit, Some(5_000));
    assert_eq!(
        snapshot.remaining,
        Some(4_321),
        "the learned ceiling should survive a restart"
    );
}

#[tokio::test]
async fn a_fatal_error_on_one_stream_does_not_stop_another() {
    let api = MockApi::spawn(|request| {
        if request.path.starts_with("/broken") {
            return Reply::status(404);
        }
        Reply::json(r#"{"data":[{"id":1}]}"#)
    })
    .await;

    let streams = format!(
        "{OFFSET_STREAM}{}",
        r"  - name: broken
    source: api
    freshness: 15m
    request:
      path: /broken
    pagination:
      strategy: offset
      page_size: 2
    records:
      path: $.data
      primary_key: $.id
"
    );

    let fixture = fixture(&api.base_url(), &streams);
    let app = app(&fixture).await;

    let broken = sync(&app, "broken", false).await;
    assert_eq!(broken.status, RunStatus::Partial, "{broken:?}");
    assert!(broken.quarantined >= 1);

    let healthy = sync(&app, "items", false).await;
    assert_eq!(healthy.status, RunStatus::Ok, "{healthy:?}");
    assert_eq!(healthy.records, 1);
}

#[tokio::test]
async fn a_wholly_broken_endpoint_stops_instead_of_quarantining_forever() {
    // Every page 404s. A positional paginator can always compute a next offset,
    // so without a bound the walk would quarantine and skip until the offset
    // overflowed - spending the budget the whole way.
    let api = MockApi::spawn(|_| Reply::status(404)).await;

    let fixture = fixture(&api.base_url(), OFFSET_STREAM);
    let app = app(&fixture).await;
    let report = sync(&app, "items", false).await;

    assert_eq!(report.status, RunStatus::Partial, "{report:?}");
    assert_eq!(report.stop_reason.as_deref(), Some("endpoint_failing"));
    assert!(
        report.requests <= 9,
        "a broken endpoint cost {} requests; it should give up quickly",
        report.requests
    );

    // It still advanced the freshness clock, so the governor does not keep
    // funding a stream that can only fail.
    let checkpoint = app.store.checkpoint("items").await.expect("checkpoint");
    assert!(checkpoint.last_success_at.is_some());
}

#[tokio::test]
async fn test_command_fetches_without_writing() {
    let api = MockApi::spawn(|_| Reply::json(r#"{"data":[{"id":1},{"id":2}]}"#)).await;
    let fixture = fixture(&api.base_url(), OFFSET_STREAM);

    let app = App::build(&fixture.config_path, Storage::Ephemeral)
        .await
        .expect("app builds");
    let handle = app.stream("items").expect("stream is configured");
    let probe = app.worker.probe(&handle, 1).await.expect("probe runs");

    assert_eq!(probe.records.len(), 2);
    assert_eq!(probe.pages, 1);

    // The real mirror was never even created.
    assert!(
        !fixture.db_path.exists(),
        "`siphon test` must not touch the store"
    );
}
