//! Query fuel floor + touch-rescale integration tests.
//!
//! Companion to the unit tests in `fluree-db-core/src/tracking.rs`, which pin
//! the schedule values (floor = 1.000, I/O touch = 0.010). These tests verify
//! the floor is wired into the query entry paths: every fuel-tracked query
//! reports at least the floor, a parse error still reports it, and a `max-fuel`
//! below the floor is rejected up front.

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::json;

/// Seed a tiny in-memory ledger with a single named subject.
async fn seed_one() -> (support::MemoryFluree, support::MemoryLedger) {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = support::genesis_ledger(&fluree, "fuel/floor:main");
    let seed = json!({
        "@context": { "a": "http://a.co/" },
        "@graph": [{ "@id": "http://a.co/x", "a:name": "X" }]
    });
    let ledger = fluree.insert(ledger0, &seed).await.expect("seed").ledger;
    (fluree, ledger)
}

#[tokio::test]
async fn tracked_query_reports_at_least_floor() {
    let (fluree, ledger) = seed_one().await;

    let query = json!({
        "@context": { "a": "http://a.co/" },
        "select": ["?name"],
        "where": { "@id": "?s", "a:name": "?name" },
        "opts": { "meta": true }
    });

    let tracked = support::query_jsonld_tracked(&fluree, &ledger, &query)
        .await
        .expect("query should succeed");
    let fuel = tracked.fuel.expect("fuel should be present when meta=true");

    // Floor is always charged; this tiny overlay scan adds only a few 0.001
    // rows on top, so the floor dominates.
    assert!(fuel >= 1.0, "fuel ({fuel}) should include the 1.0 floor");
    assert!(
        fuel < 2.0,
        "tiny query fuel ({fuel}) should be dominated by the floor"
    );
}

#[tokio::test]
async fn parse_error_with_tracking_still_reports_floor() {
    let (fluree, ledger) = seed_one().await;

    // `where` must be an object/array; a scalar fails parsing. The floor is
    // charged before parsing, so the error response still reports it.
    let bad = json!({
        "@context": { "a": "http://a.co/" },
        "select": ["?name"],
        "where": 42,
        "opts": { "meta": true }
    });

    let err = support::query_jsonld_tracked(&fluree, &ledger, &bad)
        .await
        .expect_err("malformed query should fail to parse");

    assert_eq!(err.status, 400);
    assert_eq!(
        err.fuel,
        Some(1.0),
        "a parse error with tracking should still report the floor"
    );
}

#[tokio::test]
async fn max_fuel_below_floor_is_rejected() {
    let (fluree, ledger) = seed_one().await;

    // max-fuel: 0.5 leaves no room for the 1.0 floor → rejected up front.
    let query = json!({
        "@context": { "a": "http://a.co/" },
        "select": ["?name"],
        "where": { "@id": "?s", "a:name": "?name" },
        "opts": { "max-fuel": 0.5, "meta": true }
    });

    let err = support::query_jsonld_tracked(&fluree, &ledger, &query)
        .await
        .expect_err("sub-floor max-fuel should fail");

    assert!(
        err.error.to_lowercase().contains("fuel"),
        "error should mention fuel, got: {}",
        err.error
    );
    // The floor was added before the limit check, so the reported usage is 1.0.
    assert_eq!(err.fuel, Some(1.0));
}

#[tokio::test]
async fn max_fuel_just_above_floor_allows_query() {
    let (fluree, ledger) = seed_one().await;

    // 1.5 fuel = floor (1.0) + room for ~50 touches / 500 rows; the tiny scan
    // fits comfortably.
    let query = json!({
        "@context": { "a": "http://a.co/" },
        "select": ["?name"],
        "where": { "@id": "?s", "a:name": "?name" },
        "opts": { "max-fuel": 1.5, "meta": true }
    });

    let tracked = support::query_jsonld_tracked(&fluree, &ledger, &query)
        .await
        .expect("query within budget should succeed");
    let fuel = tracked.fuel.expect("fuel should be present");
    assert!(
        (1.0..=1.5).contains(&fuel),
        "fuel ({fuel}) should fit budget"
    );
}

/// Untracked (`max-fuel` only, no `meta`) path: the floor is charged before
/// parsing, so a sub-floor budget is rejected as a *fuel* error even when the
/// query is also malformed — the parse never runs.
#[tokio::test]
async fn untracked_max_fuel_below_floor_rejected_before_parse() {
    let (fluree, ledger) = seed_one().await;

    let q = json!({
        "@context": { "a": "http://a.co/" },
        "select": ["?name"],
        "where": 42, // malformed: would fail parsing if we got that far
        "opts": { "max-fuel": 0.5 }
    });

    let err = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .expect_err("sub-floor max-fuel should fail");
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("fuel"),
        "should fail on the floor before parsing, got: {msg}"
    );
}

/// Connection-level tracked JSON-LD error (missing ledger spec) is raised
/// before delegating to the per-view tracked query, yet still reports the floor.
#[tokio::test]
async fn connection_tracked_missing_spec_reports_floor() {
    let fluree = FlureeBuilder::memory().build_memory();

    // No `from`/ledger spec → rejected at the connection layer.
    let q = json!({
        "@context": { "a": "http://a.co/" },
        "select": ["?name"],
        "where": { "@id": "?s", "a:name": "?name" }
    });

    let err = fluree
        .query_connection_tracked(&q)
        .await
        .expect_err("missing ledger spec should error");
    assert_eq!(err.status, 400);
    assert_eq!(
        err.fuel,
        Some(1.0),
        "connection-level spec error should still report the floor"
    );
}

/// Connection-level tracked SPARQL parse error reports the floor.
#[tokio::test]
async fn connection_sparql_tracked_parse_error_reports_floor() {
    let fluree = FlureeBuilder::memory().build_memory();

    let err = fluree
        .query_connection_sparql_tracked("SELECT ?x WHERE {", None, None)
        .await
        .expect_err("malformed SPARQL should error");
    assert_eq!(err.status, 400);
    assert_eq!(
        err.fuel,
        Some(1.0),
        "SPARQL connection parse error should report the floor"
    );
}

/// Connection JSON-LD: a sub-floor `max-fuel` (via `opts`) is enforced at the
/// connection layer *before* the dataset spec is parsed — so even a query that
/// would otherwise fail spec validation fails as a fuel error first.
#[tokio::test]
async fn connection_tracked_sub_floor_max_fuel_rejected_before_spec() {
    let fluree = FlureeBuilder::memory().build_memory();

    // No `from` (would be a "missing ledger spec" error) AND sub-floor budget.
    let q = json!({
        "@context": { "a": "http://a.co/" },
        "select": ["?name"],
        "where": { "@id": "?s", "a:name": "?name" },
        "opts": { "max-fuel": 0.5 }
    });

    let err = fluree
        .query_connection_tracked(&q)
        .await
        .expect_err("sub-floor max-fuel should fail");
    assert_eq!(err.status, 400);
    assert!(
        err.error.to_lowercase().contains("fuel"),
        "should be rejected on the floor before spec parsing, got: {}",
        err.error
    );
    assert_eq!(err.fuel, Some(1.0));
}

/// Connection SPARQL: a sub-floor `max-fuel` supplied via the tracking override
/// (the header path) is enforced before the SPARQL is parsed.
#[tokio::test]
async fn connection_sparql_tracked_sub_floor_override_rejected_before_parse() {
    use fluree_db_api::TrackingOptions;
    let fluree = FlureeBuilder::memory().build_memory();

    // 500 micro-fuel = 0.5 fuel, below the 1000 micro-fuel (1.0) floor.
    let override_opts = TrackingOptions {
        track_time: false,
        track_fuel: true,
        track_policy: false,
        max_fuel: Some(500),
    };

    // Malformed SPARQL: would fail parsing, but the floor is enforced first.
    let err = fluree
        .query_connection_sparql_tracked("SELECT ?x WHERE {", None, Some(override_opts))
        .await
        .expect_err("sub-floor override should fail");
    assert_eq!(err.status, 400);
    assert!(
        err.error.to_lowercase().contains("fuel"),
        "should be rejected on the floor before parsing, got: {}",
        err.error
    );
    assert_eq!(err.fuel, Some(1.0));
}

/// BM25/vector graph-source path: a sub-floor `max-fuel` is enforced before the
/// query is parsed, so a malformed query fails as a fuel error.
#[tokio::test]
async fn bm25_sub_floor_max_fuel_rejected_before_parse() {
    use fluree_db_api::{DataSetDb, GraphDb};
    let (fluree, ledger) = seed_one().await;
    let dataset = DataSetDb::new().with_default(GraphDb::from_ledger_state(&ledger));

    let q = json!({
        "@context": { "a": "http://a.co/" },
        "select": ["?name"],
        "where": 42, // malformed: would fail parsing if we got that far
        "opts": { "max-fuel": 0.5 }
    });

    let err = fluree
        .query_dataset_with_bm25(&dataset, &q)
        .await
        .expect_err("sub-floor max-fuel should fail");
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("fuel"),
        "should reject on the floor before parsing, got: {msg}"
    );
}
