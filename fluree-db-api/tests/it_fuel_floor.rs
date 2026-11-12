//! Query fuel floor + touch-rescale integration tests.
//!
//! Companion to the unit tests in `fluree-db-core/src/tracking.rs`, which pin
//! the schedule values (floor = 1.000, I/O touch = 0.010). These tests verify
//! the floor is wired into the query entry paths: every fuel-tracked query
//! reports at least the floor, a parse error still reports it, and a `max-fuel`
//! below the floor is rejected up front.

use crate::support;
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

/// SPARQL twin of `untracked_max_fuel_below_floor_rejected_before_parse`:
/// `# PRAGMA max-fuel` caps an untracked query on the view path, and above the
/// floor the same query runs.
#[tokio::test]
async fn untracked_sparql_max_fuel_pragma_below_floor_rejected() {
    let (fluree, ledger) = seed_one().await;
    const QUERY: &str = "SELECT ?name WHERE { ?s <http://a.co/name> ?name }";

    let err = support::query_sparql(
        &fluree,
        &ledger,
        &format!("# PRAGMA max-fuel: 0.5\n{QUERY}"),
    )
    .await
    .expect_err("sub-floor max-fuel pragma should fail");
    assert!(err.to_string().to_lowercase().contains("fuel"), "{err}");

    support::query_sparql(
        &fluree,
        &ledger,
        &format!("# PRAGMA max-fuel: 100\n{QUERY}"),
    )
    .await
    .expect("a budget above the floor runs");
}

/// On the tracked view path the AST's `# PRAGMA meta` chooses the tracking,
/// as `opts.meta` does for JSON-LD: `time` alone reports no fuel.
#[tokio::test]
async fn tracked_sparql_meta_pragma_selects_tracking() {
    let (fluree, ledger) = seed_one().await;
    const QUERY: &str = "SELECT ?name WHERE { ?s <http://a.co/name> ?name }";
    let db = support::graphdb_from_ledger(&ledger);

    let all = db
        .query(&fluree)
        .sparql(QUERY)
        .execute_tracked()
        .await
        .expect("tracked query");
    assert!(all.fuel.is_some(), "control: all tracking by default");

    let time_only = db
        .query(&fluree)
        .sparql(&format!("# PRAGMA meta: time\n{QUERY}"))
        .execute_tracked()
        .await
        .expect("tracked query");
    assert!(time_only.time.is_some());
    assert!(time_only.fuel.is_none(), "{:?}", time_only.fuel);
}

/// A `FROM` naming a graph inside the ledger runs on the dataset path, which
/// charges the floor against the pragma's `max-fuel` itself.
#[tokio::test]
async fn untracked_sparql_max_fuel_pragma_caps_a_within_ledger_dataset_query() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = support::genesis_ledger(&fluree, "fuel/floor-dataset:main");
    let ledger = fluree
        .stage_owned(ledger)
        .upsert_turtle("GRAPH <urn:fuel:g> { <http://a.co/x> <http://a.co/name> \"X\" . }")
        .execute()
        .await
        .expect("seed a named graph")
        .ledger;
    const QUERY: &str = "SELECT ?name FROM <urn:fuel:g> WHERE { ?s <http://a.co/name> ?name }";

    let rows = support::query_sparql(&fluree, &ledger, QUERY)
        .await
        .expect("control");
    assert_eq!(rows.row_count(), 1, "control: the FROM graph resolves");

    let err = support::query_sparql(
        &fluree,
        &ledger,
        &format!("# PRAGMA max-fuel: 0.5\n{QUERY}"),
    )
    .await
    .expect_err("sub-floor max-fuel pragma should fail");
    assert!(err.to_string().to_lowercase().contains("fuel"), "{err}");
}

/// Index reads on the range path charge the leaflets they touch, as the
/// scan's index lane does: a dataset spanning ledgers reads every member
/// through it, and a property path reads every hop's edges through it.
/// The same 300 facts are indexed in 3-row leaflets in one ledger and in
/// default leaflets in another, so the scan's answers and rows match and its
/// fuel differs by the leaflets alone (~100 against 1, 0.01 each).
#[tokio::test]
async fn range_path_reads_charge_the_leaflets_they_touch() {
    use fluree_db_api::admin::ReindexOptions;
    use fluree_db_indexer::IndexerConfig;
    const SMALL: &str = "fuel/range-leaflets-small:main";
    const LARGE: &str = "fuel/range-leaflets-large:main";
    const OTHER: &str = "fuel/range-leaflets-other:main";
    const N: usize = 300;
    let fluree = FlureeBuilder::memory().without_indexing().build_memory();
    let chain: String = (0..N)
        .map(|i| format!("ex:n{i} ex:next ex:n{} .\n", i + 1))
        .collect();
    for (ledger, leaflet_rows) in [(SMALL, Some(3)), (LARGE, None), (OTHER, None)] {
        fluree.create_ledger(ledger).await.expect("create");
        let handle = fluree.ledger_cached(ledger).await.expect("cache");
        let turtle = if ledger == OTHER {
            "ex:o ex:other 1 .\n".to_string()
        } else {
            chain.clone()
        };
        fluree
            .stage(&handle)
            .upsert_turtle(&format!("@prefix ex: <http://example.org/> .\n{turtle}"))
            .execute()
            .await
            .expect("seed");
        let mut opts = ReindexOptions::default();
        if let Some(rows) = leaflet_rows {
            opts = opts.with_indexer_config(IndexerConfig::default().with_leaflet_rows(rows));
        }
        fluree.reindex(ledger, opts).await.expect("index");
        fluree.disconnect_ledger(ledger).await;
    }
    let tracked = |query: String| {
        let fluree = fluree.clone();
        async move {
            let response = fluree
                .query_from()
                .sparql(&query)
                .execute_tracked()
                .await
                .unwrap_or_else(|e| panic!("{query}: {e:?}"));
            let rows = response.result["results"]["bindings"]
                .as_array()
                .map_or(0, Vec::len);
            (rows, response.fuel.expect("fuel"))
        }
    };

    let scan = |ledger: &str| {
        format!(
            "PREFIX ex: <http://example.org/> \
             SELECT ?s ?o FROM <{ledger}> FROM <{OTHER}> WHERE {{ ?s ex:next ?o }}"
        )
    };
    let (small_rows, small_fuel) = tracked(scan(SMALL)).await;
    let (large_rows, large_fuel) = tracked(scan(LARGE)).await;
    assert_eq!((small_rows, large_rows), (N, N));
    assert!(
        small_fuel - large_fuel > 0.5,
        "~100 leaflets cost {small_fuel} fuel, one leaflet {large_fuel}"
    );

    // Over a default graph of two graphs a property path reads every hop's
    // edges through the range path: at least one leaflet each.
    const GRAPHS: &str = "fuel/range-leaflets-graphs:main";
    fluree.create_ledger(GRAPHS).await.expect("create");
    let handle = fluree.ledger_cached(GRAPHS).await.expect("cache");
    fluree
        .stage(&handle)
        .upsert_turtle(&format!(
            "@prefix ex: <http://example.org/> .\n\
             GRAPH <urn:g1> {{\n{chain}}}\nGRAPH <urn:g2> {{ ex:o ex:other 1 . }}\n"
        ))
        .execute()
        .await
        .expect("seed graphs");
    fluree
        .reindex(GRAPHS, ReindexOptions::default())
        .await
        .expect("index");
    fluree.disconnect_ledger(GRAPHS).await;
    let handle = fluree.ledger_cached(GRAPHS).await.expect("reload");
    let view =
        fluree_db_api::GraphDb::from_ledger_state(&handle.snapshot().await.to_ledger_state());
    let path_fuel = view
        .query(&fluree)
        .sparql(
            "PREFIX ex: <http://example.org/> SELECT (COUNT(?o) AS ?n) \
             FROM <urn:g1> FROM <urn:g2> WHERE { ex:n0 ex:next+ ?o }",
        )
        .execute_tracked()
        .await
        .expect("path")
        .fuel
        .expect("fuel");
    assert!(
        path_fuel >= 1.0 + N as f64 * 0.01,
        "{N} hops cost {path_fuel} fuel"
    );
}
