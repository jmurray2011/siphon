//! The Postgres target, checked against a real server.
//!
//! Skipped unless `SIPHON_TEST_POSTGRES` is set to a connection string, because
//! the rest of the suite must stay runnable with nothing installed - that is the
//! same promise the single static binary makes. Run it with, for example:
//!
//! ```sh
//! docker run --rm -d --name siphon-pg -e POSTGRES_PASSWORD=siphon \
//!   -e POSTGRES_DB=siphon -p 55432:5432 postgres:16
//! SIPHON_TEST_POSTGRES='host=localhost port=55432 user=postgres \
//!   password=siphon dbname=siphon' cargo test --test postgres_store
//! ```

use siphon::cli::status::ReportStore;
use siphon::clock::Timestamp;
use siphon::extract::{Record, RecordSpec, RecordsConfig};
use siphon::store::postgres::PgStore;
use siphon::store::{PageWrite, Progress};
use siphon::sync::SyncStore;

/// The connection string, or `None` when the test should be skipped.
fn conn_str() -> Option<String> {
    std::env::var("SIPHON_TEST_POSTGRES").ok()
}

/// A server presenting a certificate the host does not trust, or `None`.
fn untrusted_tls_conn_str() -> Option<String> {
    std::env::var("SIPHON_TEST_POSTGRES_UNTRUSTED_TLS").ok()
}

#[tokio::test]
async fn an_untrusted_certificate_is_refused_rather_than_accepted() {
    let Some(conn) = untrusted_tls_conn_str() else {
        eprintln!("skipped: SIPHON_TEST_POSTGRES_UNTRUSTED_TLS is not set");
        return;
    };

    // The server offers TLS with a certificate signed by nobody the host trusts.
    // Succeeding here would mean verification is switched off somewhere, which
    // is the failure worth a test: it looks identical to working, and the whole
    // reason for TLS is the hop to another host.
    let refused = PgStore::connect(&conn).await;
    let Err(cause) = refused else {
        panic!("connected to a server whose certificate should not verify");
    };

    let text = format!("{cause:?}").to_lowercase();
    assert!(
        text.contains("certificate") || text.contains("tls") || text.contains("unknownissuer"),
        "expected a certificate failure, got: {cause:?}"
    );
}

#[tokio::test]
async fn connecting_applies_the_schema_and_is_repeatable() {
    let Some(conn) = conn_str() else {
        eprintln!("skipped: SIPHON_TEST_POSTGRES is not set");
        return;
    };

    let store = PgStore::connect(&conn)
        .await
        .expect("connects and migrates");

    // An unknown stream has no records rather than erroring, which is what every
    // caller assumes on a first run.
    let count = store
        .record_count("never_synced")
        .await
        .expect("counting an unknown stream is not an error");
    assert_eq!(count, 0);

    // Connecting again must be a no-op, not a failure: every process start
    // re-applies the DDL, so `IF NOT EXISTS` has to hold across the whole
    // schema, indexes included.
    let again = PgStore::connect(&conn).await.expect("reconnects cleanly");
    assert_eq!(
        again
            .record_count("never_synced")
            .await
            .expect("still queryable"),
        0
    );
}

#[tokio::test]
async fn an_unknown_stream_reads_as_a_default_rather_than_an_error() {
    let Some(conn) = conn_str() else {
        eprintln!("skipped: SIPHON_TEST_POSTGRES is not set");
        return;
    };
    let store = PgStore::connect(&conn).await.expect("connects");

    // Every one of these is asked about a stream that has never run, which is
    // the state of every stream on a first start. The SQLite target answers with
    // defaults rather than errors, and a second target that raised here instead
    // would break `status` on a fresh mirror - the exact case nobody tests by
    // hand because it only happens once.
    let checkpoint = ReportStore::checkpoint(&store, "never_synced")
        .await
        .expect("checkpoint defaults");
    assert_eq!(checkpoint.cursor_val, None);
    assert_eq!(checkpoint.page_state, None);
    assert_eq!(checkpoint.records_seen, 0);

    assert_eq!(
        ReportStore::first_run_at(&store, "never_synced")
            .await
            .expect("first_run_at is queryable"),
        None
    );
    assert!(
        ReportStore::successes_since(&store, "never_synced", Timestamp::from_millis(0))
            .await
            .expect("successes are queryable")
            .is_empty()
    );
    assert_eq!(
        ReportStore::quarantined_count(&store, "never_synced")
            .await
            .expect("quarantine is queryable"),
        0
    );
    assert!(ReportStore::last_run(&store, "never_synced")
        .await
        .expect("last_run is queryable")
        .is_none());
}

/// Builds records the only way anything else does: by extracting them.
///
/// `Record` has no public constructor, which is deliberate - a record that did
/// not come out of a response is not a thing the store should be able to hold.
// `allow-expect-in-tests` covers `#[test]` functions, and this is a helper
// beside them rather than one of them. Panicking here is still the right
// behaviour: the inputs are literals in this file, so a failure is a broken test
// rather than a condition worth handling.
#[allow(clippy::expect_used)]
fn records_from(json: &str, at: Timestamp) -> Vec<Record> {
    let spec = RecordSpec::from_config(&RecordsConfig {
        path: "$.items".to_owned(),
        primary_key: "$.id".to_owned(),
        cursor: None,
    })
    .expect("the record spec is valid");
    let body: serde_json::Value = serde_json::from_str(json).expect("valid json");
    spec.extract(&body, at).expect("records extract")
}

#[tokio::test]
async fn a_page_and_its_checkpoint_land_together_and_re_seeing_it_is_not_a_change() {
    let Some(conn) = conn_str() else {
        eprintln!("skipped: SIPHON_TEST_POSTGRES is not set");
        return;
    };
    let store = PgStore::connect(&conn).await.expect("connects");
    let at = Timestamp::from_millis(1_700_000_000_000);

    // Unique per run, not just per test. The database outlives the process, so a
    // fixed name would make the second `cargo test` see the first one's records
    // and report a false failure on the new-record count.
    let stream = &format!(
        "pg_write_path_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos())
    );
    let page = PageWrite {
        stream: stream.clone(),
        records: records_from(r#"{"items":[{"id":"a","v":1},{"id":"b","v":1}]}"#, at),
        progress: Progress::Complete {
            cursor_val: Some("mark-1".to_owned()),
            success_at: at,
        },
        at,
    };

    let first = SyncStore::write_page(&store, page).await.expect("writes");
    assert_eq!((first.new, first.changed, first.unchanged), (2, 0, 0));
    assert!(!first.saw_known_key, "nothing was stored before this");

    // The checkpoint must reflect the same write: records without the mark means
    // the next run re-fetches, and the mark without records means a hole.
    let checkpoint = SyncStore::checkpoint(&store, stream).await.expect("reads");
    assert_eq!(checkpoint.cursor_val.as_deref(), Some("mark-1"));
    assert_eq!(
        checkpoint.page_state, None,
        "a finished walk has no resume point"
    );
    assert_eq!(checkpoint.last_success_at, Some(at));

    // The same page again is re-seen, not changed. This is the distinction the
    // run log reports and that `stop_on_seen_key` depends on.
    let again = PageWrite {
        stream: stream.clone(),
        records: records_from(r#"{"items":[{"id":"a","v":1},{"id":"b","v":1}]}"#, at),
        progress: Progress::Complete {
            cursor_val: Some("mark-1".to_owned()),
            success_at: at,
        },
        at,
    };
    let second = SyncStore::write_page(&store, again)
        .await
        .expect("rewrites");
    assert_eq!((second.new, second.changed, second.unchanged), (0, 0, 2));
    assert!(second.saw_known_key);

    // A changed payload under a known key is a change, not a new record.
    let edited = PageWrite {
        stream: stream.clone(),
        records: records_from(r#"{"items":[{"id":"a","v":2}]}"#, at),
        progress: Progress::Complete {
            cursor_val: Some("mark-2".to_owned()),
            success_at: at,
        },
        at,
    };
    let third = SyncStore::write_page(&store, edited)
        .await
        .expect("updates");
    assert_eq!((third.new, third.changed, third.unchanged), (0, 1, 0));

    assert!(
        SyncStore::any_known_key(&store, stream, vec!["a".to_owned()])
            .await
            .expect("queries")
    );
    assert!(
        !SyncStore::any_known_key(&store, stream, vec!["nope".to_owned()])
            .await
            .expect("queries"),
        "an unseen key must not report as known"
    );
}
