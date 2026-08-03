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

use siphon::store::postgres::PgStore;

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
