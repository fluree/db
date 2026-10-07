//! Integration tests for geo proximity search via geof:distance patterns.
//!
//! These tests use the unified Triple + Bind(geof:distance) + Filter pattern
//! that works identically in both JSON-LD and SPARQL. The `geo_rewrite` pass in
//! `prepare_execution` rewrites this pattern into `Pattern::GeoSearch` when a
//! binary index covers the query's `t`, so each test indexes a file-backed
//! ledger and checks the index is loaded before it queries.
//!
//! Tests cover:
//! - Time-travel: a query as of the index's base `t` and one at head, with
//!   novelty past it
//! - Retraction: a location retracted in novelty over the index
//! - One row per matching point, as the patterns evaluated as written give
//! - Distances, ORDER BY + LIMIT, and SPARQL
//! - Datasets spanning two ledgers, which run without a binary index

#![cfg(feature = "native")]

use crate::support;
use crate::support::{start_background_indexer_local, trigger_index_and_wait_outcome};
use fluree_db_api::{
    policy_builder, Fluree, FlureeBuilder, GovernanceOptions, GraphDb, IndexConfig, LedgerState,
};
use fluree_db_indexer::IndexerHandle;
use fluree_db_transact::{CommitOpts, TxnOpts};
use serde_json::{json, Value as JsonValue};
use tokio::task::LocalSet;

fn geo_search_context() -> JsonValue {
    json!({
        "ex": "http://example.org/",
        "geo": "http://www.opengis.net/ont/geosparql#",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    })
}

const PARIS: (f64, f64) = (2.3522, 48.8566);

/// A file-backed connection that indexes on request; an in-memory one with
/// no indexing mode never loads the index it builds, so its queries never
/// reach `GeoSearch`. Run the test body inside `local.run_until`.
fn indexed_fluree(tmp: &tempfile::TempDir) -> (Fluree, LocalSet, IndexerHandle) {
    let mut fluree = FlureeBuilder::file(tmp.path().to_string_lossy().to_string())
        .build()
        .expect("build");
    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        fluree
            .nameservice_mode()
            .publisher_arc()
            .expect("test setup requires ReadWrite nameservice mode"),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));
    (fluree, local, handle)
}

/// Index `alias` through its head and reload it, with the index loaded.
async fn index_and_load(fluree: &Fluree, handle: &IndexerHandle, alias: &str) -> LedgerState {
    let t = fluree.ledger(alias).await.expect("ledger").t();
    trigger_index_and_wait_outcome(handle, alias, t).await;
    let loaded = fluree.ledger(alias).await.expect("load ledger");
    assert!(
        loaded.snapshot.range_provider.is_some(),
        "the index is loaded"
    );
    loaded
}

/// Helper to insert a city and return the resulting ledger state.
async fn insert_city(
    fluree: &Fluree,
    ledger: LedgerState,
    id: &str,
    name: &str,
    lng: f64,
    lat: f64,
) -> LedgerState {
    let tx = json!({
        "@context": geo_search_context(),
        "@id": id,
        "@type": "ex:City",
        "ex:name": name,
        "ex:location": {
            "@value": format!("POINT({} {})", lng, lat),
            "@type": "geo:wktLiteral"
        }
    });

    fluree
        .insert(ledger, &tx)
        .await
        .expect("insert city")
        .ledger
}

/// Retract a city's location. The location is bound by a variable: a WKT
/// constant in a pattern does not match a stored point.
async fn retract_location(fluree: &Fluree, ledger: LedgerState, id: &str) -> LedgerState {
    let tx = json!({
        "@context": geo_search_context(),
        "where": { "@id": id, "ex:location": "?loc" },
        "delete": { "@id": id, "ex:location": "?loc" }
    });
    let result = fluree.update(ledger, &tx).await.expect("retract");
    assert_eq!(result.receipt.retract_count, 1, "the location is retracted");
    result.ledger
}

/// The places within `radius_meters` of `center` with their distances,
/// nearest first. The Triple + Bind(geof:distance) + Filter shape is what
/// `geo_rewrite` turns into `Pattern::GeoSearch`.
async fn nearby(
    fluree: &Fluree,
    db: &GraphDb,
    (center_lng, center_lat): (f64, f64),
    radius_meters: f64,
) -> Vec<(String, f64)> {
    let query = json!({
        "@context": geo_search_context(),
        "select": ["?name", "?dist"],
        "where": [
            { "@id": "?place", "ex:location": "?loc" },
            ["bind", "?dist", format!("(geof:distance ?loc \"POINT({center_lng} {center_lat})\")")],
            ["filter", format!("(<= ?dist {radius_meters})")],
            { "@id": "?place", "ex:name": "?name" }
        ],
        "orderBy": "?dist"
    });
    let rows = fluree
        .query(db, &query)
        .await
        .expect("geo query")
        .to_jsonld(&db.snapshot)
        .expect("jsonld");
    rows.as_array()
        .expect("rows")
        .iter()
        .map(|row| {
            (
                row[0].as_str().expect("name").to_string(),
                row[1].as_f64().expect("distance"),
            )
        })
        .collect()
}

fn names(rows: &[(String, f64)]) -> Vec<&str> {
    rows.iter().map(|(name, _)| name.as_str()).collect()
}

// =============================================================================
// Time-travel and novelty tests
// =============================================================================

/// GeoSearch reads the index as of the query's `t` and overlays the novelty
/// past the index's base: a city inserted after the index is found at head
/// and not as of the base.
#[tokio::test]
async fn geo_search_time_travel_different_results_at_different_t() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let (fluree, local, handle) = indexed_fluree(&tmp);
    let alias = "it/geo-search-time-travel:main";

    local
        .run_until(async move {
            let ledger = fluree.create_ledger(alias).await.expect("create");
            insert_city(&fluree, ledger, "ex:paris", "Paris", 2.3522, 48.8566).await;
            let indexed = index_and_load(&fluree, &handle, alias).await;
            let base_t = indexed.t();

            // London (~343km from Paris) only in novelty.
            insert_city(&fluree, indexed, "ex:london", "London", -0.1278, 51.5074).await;

            let head = fluree.db(alias).await.expect("db");
            assert_eq!(
                names(&nearby(&fluree, &head, PARIS, 500_000.0).await),
                ["Paris", "London"]
            );
            let at_base = fluree.db(alias).await.expect("db").as_of(base_t);
            assert_eq!(
                names(&nearby(&fluree, &at_base, PARIS, 500_000.0).await),
                ["Paris"]
            );
        })
        .await;
}

/// A location retracted after the index was built is gone from the results,
/// though the index still holds it.
#[tokio::test]
async fn geo_search_retraction_removes_point_from_results() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let (fluree, local, handle) = indexed_fluree(&tmp);
    let alias = "it/geo-search-retraction:main";

    local
        .run_until(async move {
            let ledger = fluree.create_ledger(alias).await.expect("create");
            let ledger = insert_city(&fluree, ledger, "ex:paris", "Paris", 2.3522, 48.8566).await;
            insert_city(&fluree, ledger, "ex:london", "London", -0.1278, 51.5074).await;
            let indexed = index_and_load(&fluree, &handle, alias).await;
            let before = GraphDb::from_ledger_state(&indexed);
            assert_eq!(
                names(&nearby(&fluree, &before, PARIS, 500_000.0).await),
                ["Paris", "London"]
            );

            let ledger = retract_location(&fluree, indexed, "ex:london").await;
            let after = GraphDb::from_ledger_state(&ledger);
            assert_eq!(
                names(&nearby(&fluree, &after, PARIS, 500_000.0).await),
                ["Paris"]
            );

            // And once the retraction is indexed too.
            let reindexed = index_and_load(&fluree, &handle, alias).await;
            let reindexed = GraphDb::from_ledger_state(&reindexed);
            assert_eq!(
                names(&nearby(&fluree, &reindexed, PARIS, 500_000.0).await),
                ["Paris"]
            );
        })
        .await;
}

// =============================================================================
// Deduplication tests
// =============================================================================

/// A subject with several points within the radius matches once per point,
/// each with that point's own distance and the point bound — exactly what the
/// triple + bind + filter evaluated as written gives, before the ledger is
/// indexed. The rewrite into GeoSearch must not change the answer.
#[tokio::test]
async fn geo_search_matches_once_per_point_like_the_patterns_it_replaces() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let mut fluree = FlureeBuilder::file(tmp.path().to_string_lossy().to_string())
        .build()
        .expect("build");
    let alias = "it/geo-search-points:main";
    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        fluree
            .nameservice_mode()
            .publisher_arc()
            .expect("test setup requires ReadWrite nameservice mode"),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));

    local
        .run_until(async move {
            let ledger = fluree.create_ledger(alias).await.expect("create");
            // Paris with two points ~100km apart, and Rome, out of range.
            let tx = json!({
                "@context": geo_search_context(),
                "@graph": [
                    {"@id": "ex:paris", "ex:name": "Paris", "ex:location": [
                        {"@value": "POINT(2.3522 48.8566)", "@type": "geo:wktLiteral"},
                        {"@value": "POINT(2.3522 49.7566)", "@type": "geo:wktLiteral"}
                    ]},
                    {"@id": "ex:rome", "ex:name": "Rome",
                     "ex:location": {"@value": "POINT(12.4964 41.9028)", "@type": "geo:wktLiteral"}}
                ]
            });
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 1_000_000,
            };
            let ledger = fluree
                .insert_with_opts(
                    ledger,
                    &tx,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("insert")
                .ledger;
            let query = json!({
                "@context": geo_search_context(),
                "select": ["?name", "?loc", "?dist"],
                "where": [
                    { "@id": "?place", "ex:location": "?loc" },
                    ["bind", "?dist", "(geof:distance ?loc \"POINT(2.3522 48.8566)\")"],
                    ["filter", "(<= ?dist 200000)"],
                    { "@id": "?place", "ex:name": "?name" }
                ],
                "orderBy": "?dist"
            });
            let rows = |ledger: LedgerState| {
                let (fluree, query) = (&fluree, &query);
                async move {
                    support::query_jsonld(fluree, &ledger, query)
                        .await
                        .expect("query")
                        .to_jsonld(&ledger.snapshot)
                        .expect("format")
                }
            };

            let as_written = rows(ledger.clone()).await;
            let found = as_written.as_array().expect("rows");
            assert_eq!(found.len(), 2, "one row per point: {as_written}");
            assert!(found
                .iter()
                .all(|row| row[0] == "Paris" && row[1].is_object()));
            assert!(found[0][2].as_f64().unwrap() < 1.0);
            assert!(found[1][2].as_f64().unwrap() > 90_000.0);

            trigger_index_and_wait_outcome(&handle, alias, ledger.t()).await;
            let indexed = fluree.ledger(alias).await.expect("load ledger");
            assert!(
                indexed.snapshot.range_provider.is_some(),
                "the index is loaded"
            );
            assert_eq!(rows(indexed).await, as_written);
        })
        .await;
}

// =============================================================================
// Distance calculation tests
// =============================================================================

#[tokio::test]
async fn geo_search_returns_correct_distances() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let (fluree, local, handle) = indexed_fluree(&tmp);
    let alias = "it/geo-search-distance:main";

    local
        .run_until(async move {
            let ledger = fluree.create_ledger(alias).await.expect("create");
            let ledger = insert_city(&fluree, ledger, "ex:paris", "Paris", 2.3522, 48.8566).await;
            let ledger =
                insert_city(&fluree, ledger, "ex:london", "London", -0.1278, 51.5074).await;
            insert_city(&fluree, ledger, "ex:berlin", "Berlin", 13.4050, 52.5200).await;
            let indexed = index_and_load(&fluree, &handle, alias).await;

            let rows = nearby(
                &fluree,
                &GraphDb::from_ledger_state(&indexed),
                PARIS,
                1_000_000.0,
            )
            .await;
            assert_eq!(names(&rows), ["Paris", "London", "Berlin"]);
            assert!(rows[0].1 < 1.0, "Paris is ~0m away: {rows:?}");
            assert!(
                (330_000.0..360_000.0).contains(&rows[1].1),
                "London is ~343km away: {rows:?}"
            );
            assert!(
                (860_000.0..900_000.0).contains(&rows[2].1),
                "Berlin is ~878km away: {rows:?}"
            );
        })
        .await;
}

// =============================================================================
// Limit tests
// =============================================================================

#[tokio::test]
async fn geo_search_respects_limit_returns_nearest() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let (fluree, local, handle) = indexed_fluree(&tmp);
    let alias = "it/geo-search-limit:main";

    local
        .run_until(async move {
            let ledger = fluree.create_ledger(alias).await.expect("create");
            // Inserted farthest first, so insertion order cannot pass for
            // nearest-first.
            let ledger = insert_city(&fluree, ledger, "ex:tokyo", "Tokyo", 139.6917, 35.6895).await;
            let ledger =
                insert_city(&fluree, ledger, "ex:berlin", "Berlin", 13.4050, 52.5200).await;
            let ledger =
                insert_city(&fluree, ledger, "ex:london", "London", -0.1278, 51.5074).await;
            insert_city(&fluree, ledger, "ex:paris", "Paris", 2.3522, 48.8566).await;
            let indexed = index_and_load(&fluree, &handle, alias).await;

            let query = json!({
                "@context": geo_search_context(),
                "select": ["?name", "?dist"],
                "where": [
                    { "@id": "?place", "ex:location": "?loc" },
                    ["bind", "?dist", "(geof:distance ?loc \"POINT(2.3522 48.8566)\")"],
                    ["filter", "(<= ?dist 20000000)"],
                    { "@id": "?place", "ex:name": "?name" }
                ],
                "orderBy": "?dist",
                "limit": 2
            });
            let rows = support::query_jsonld(&fluree, &indexed, &query)
                .await
                .expect("geo query")
                .to_jsonld(&indexed.snapshot)
                .expect("jsonld");
            let found: Vec<&str> = rows
                .as_array()
                .expect("rows")
                .iter()
                .map(|row| row[0].as_str().expect("name"))
                .collect();
            assert_eq!(found, ["Paris", "London"]);
        })
        .await;
}

// =============================================================================
// Named Graph Tests
// =============================================================================

/// Geo search stays within the graph it reads: the default graph's cities
/// and each named graph's, though every one is within reach of the others.
/// Two named graphs, so graph routing must tell them apart, not only named
/// from default.
#[tokio::test]
async fn geo_search_respects_named_graph_boundaries() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let (fluree, local, handle) = indexed_fluree(&tmp);
    let alias = "it/geo-named-graph:main";

    local
        .run_until(async move {
            let ledger = fluree.create_ledger(alias).await.expect("create");
            // Default graph: France.
            let ledger = insert_city(&fluree, ledger, "ex:paris", "Paris", 2.3522, 48.8566).await;
            let ledger = insert_city(&fluree, ledger, "ex:lyon", "Lyon", 4.8357, 45.7640).await;
            let trig = r#"
                @prefix ex: <http://example.org/> .
                @prefix geo: <http://www.opengis.net/ont/geosparql#> .

                GRAPH <http://example.org/graphs/germany> {
                    ex:berlin a ex:City ;
                        ex:name "Berlin" ;
                        ex:location "POINT(13.4050 52.5200)"^^geo:wktLiteral .
                    ex:munich a ex:City ;
                        ex:name "Munich" ;
                        ex:location "POINT(11.5820 48.1351)"^^geo:wktLiteral .
                }
                GRAPH <http://example.org/graphs/italy> {
                    ex:rome a ex:City ;
                        ex:name "Rome" ;
                        ex:location "POINT(12.4964 41.9028)"^^geo:wktLiteral .
                    ex:milan a ex:City ;
                        ex:name "Milan" ;
                        ex:location "POINT(9.1900 45.4642)"^^geo:wktLiteral .
                }
            "#;
            fluree
                .stage_owned(ledger)
                .upsert_turtle(trig)
                .execute()
                .await
                .expect("named graphs");
            let indexed = index_and_load(&fluree, &handle, alias).await;

            // Every city is within 1,500 km of Paris, and of Munich.
            let names_in = |graph: Option<&'static str>, center: &'static str| {
                let (fluree, indexed) = (&fluree, &indexed);
                async move {
                    let shape = format!(
                        r#"?place <http://example.org/location> ?loc ;
                                  <http://example.org/name> ?name .
                           BIND(geof:distance(?loc, "{center}"^^geo:wktLiteral) AS ?dist)
                           FILTER(?dist <= 1500000)"#
                    );
                    let pattern = match graph {
                        Some(iri) => format!("GRAPH <{iri}> {{ {shape} }}"),
                        None => shape,
                    };
                    let sparql = format!(
                        "PREFIX geo: <http://www.opengis.net/ont/geosparql#>
                         PREFIX geof: <http://www.opengis.net/def/function/geosparql/>
                         SELECT ?name WHERE {{ {pattern} }} ORDER BY ?name"
                    );
                    let rows = support::query_sparql(fluree, indexed, &sparql)
                        .await
                        .expect("geo query")
                        .to_jsonld(&indexed.snapshot)
                        .expect("jsonld");
                    rows.as_array()
                        .expect("rows")
                        .iter()
                        .map(|row| {
                            row.as_str()
                                .or_else(|| row[0].as_str())
                                .unwrap_or_else(|| panic!("a name: {row}"))
                                .to_string()
                        })
                        .collect::<Vec<_>>()
                }
            };
            assert_eq!(
                names_in(None, "POINT(2.3522 48.8566)").await,
                ["Lyon", "Paris"]
            );
            assert_eq!(
                names_in(
                    Some("http://example.org/graphs/germany"),
                    "POINT(11.5820 48.1351)"
                )
                .await,
                ["Berlin", "Munich"]
            );
            assert_eq!(
                names_in(
                    Some("http://example.org/graphs/italy"),
                    "POINT(11.5820 48.1351)"
                )
                .await,
                ["Milan", "Rome"]
            );
        })
        .await;
}

// =============================================================================
// SPARQL geof:distance rewrite tests
// =============================================================================

/// The same shape in SPARQL:
/// ```sparql
/// ?place ex:location ?loc .
/// BIND(geof:distance(?loc, "POINT(...)"^^geo:wktLiteral) AS ?dist)
/// FILTER(?dist < 500000)
/// ```
#[tokio::test]
async fn sparql_geof_distance_uses_geo_index() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let (fluree, local, handle) = indexed_fluree(&tmp);
    let alias = "it/geo-search-sparql:main";

    local
        .run_until(async move {
            let ledger = fluree.create_ledger(alias).await.expect("create");
            let ledger = insert_city(&fluree, ledger, "ex:paris", "Paris", 2.3522, 48.8566).await;
            let ledger =
                insert_city(&fluree, ledger, "ex:london", "London", -0.1278, 51.5074).await;
            let ledger =
                insert_city(&fluree, ledger, "ex:berlin", "Berlin", 13.4050, 52.5200).await;
            insert_city(&fluree, ledger, "ex:tokyo", "Tokyo", 139.6917, 35.6895).await;
            let indexed = index_and_load(&fluree, &handle, alias).await;

            let sparql = r#"
                PREFIX ex: <http://example.org/>
                PREFIX geo: <http://www.opengis.net/ont/geosparql#>
                PREFIX geof: <http://www.opengis.net/def/function/geosparql/>

                SELECT ?name ?dist
                WHERE {
                    ?place a ex:City .
                    ?place ex:name ?name .
                    ?place ex:location ?loc .
                    BIND(geof:distance(?loc, "POINT(2.3522 48.8566)"^^geo:wktLiteral) AS ?dist)
                    FILTER(?dist < 500000)
                }
                ORDER BY ?dist
            "#;
            let rows = support::query_sparql(&fluree, &indexed, sparql)
                .await
                .expect("SPARQL geo query")
                .to_jsonld(&indexed.snapshot)
                .expect("jsonld");
            let rows: Vec<(String, f64)> = rows
                .as_array()
                .expect("rows")
                .iter()
                .map(|row| {
                    (
                        row[0].as_str().expect("name").to_string(),
                        row[1].as_f64().expect("distance"),
                    )
                })
                .collect();
            assert_eq!(names(&rows), ["Paris", "London"]);
            assert!(rows[0].1 < 1.0, "Paris is ~0m away: {rows:?}");
            assert!(
                (330_000.0..360_000.0).contains(&rows[1].1),
                "London is ~343km away: {rows:?}"
            );
        })
        .await;
}

// =============================================================================
// Multi-ledger datasets
// =============================================================================

/// A dataset spanning two ledgers runs without a binary index, so the shape
/// is left as written rather than rewritten into a `GeoSearch` that cannot
/// run, whether the second ledger joins the default graph or is named.
#[tokio::test]
async fn geof_distance_across_two_indexed_ledgers() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let (fluree, local, handle) = indexed_fluree(&tmp);
    let (a, b) = ("it/geo-multi-a:main", "it/geo-multi-b:main");

    local
        .run_until(async move {
            let ledger = fluree.create_ledger(a).await.expect("create a");
            let ledger = insert_city(&fluree, ledger, "ex:paris", "Paris", 2.3522, 48.8566).await;
            insert_city(&fluree, ledger, "ex:lille", "Lille", 3.0573, 50.6292).await;
            index_and_load(&fluree, &handle, a).await;
            let ledger = fluree.create_ledger(b).await.expect("create b");
            insert_city(&fluree, ledger, "ex:brussels", "Brussels", 4.3517, 50.8503).await;
            index_and_load(&fluree, &handle, b).await;

            let run = |sparql: String| {
                let fluree = &fluree;
                async move {
                    let rows = fluree
                        .query_from()
                        .sparql(&sparql)
                        .format(fluree_db_api::FormatterConfig::jsonld())
                        .execute_formatted()
                        .await
                        .unwrap_or_else(|e| panic!("{e}: {sparql}"));
                    let mut found: Vec<String> = rows
                        .as_array()
                        .expect("rows")
                        .iter()
                        .map(|row| {
                            row.as_str()
                                .or_else(|| row[0].as_str())
                                .unwrap_or_else(|| panic!("a name: {row}"))
                                .to_string()
                        })
                        .collect();
                    found.sort();
                    found
                }
            };
            let shape = r#"?place <http://example.org/location> ?loc ;
                               <http://example.org/name> ?name .
                        BIND(geof:distance(?loc, "POINT(2.3522 48.8566)"^^geo:wktLiteral) AS ?dist)
                        FILTER(?dist < 300000)"#;
            let prefixes = r"PREFIX geo: <http://www.opengis.net/ont/geosparql#>
                PREFIX geof: <http://www.opengis.net/def/function/geosparql/>";

            for (first, second) in [(a, b), (b, a)] {
                assert_eq!(
                    run(format!(
                        "{prefixes} SELECT ?name FROM <{first}> FROM <{second}> WHERE {{ {shape} }}"
                    ))
                    .await,
                    ["Brussels", "Lille", "Paris"]
                );
            }
            assert_eq!(
                run(format!(
                    "{prefixes} SELECT ?name FROM <{a}> FROM NAMED <{b}> WHERE {{ GRAPH <{b}> {{ {shape} }} }}"
                ))
                .await,
                ["Brussels"]
            );
        })
        .await;
}

/// View policy must drop geo search hits whose location flake the identity
/// cannot view. Regression for the geo leak: `GeoSearchOperator` reads location
/// flakes directly from the index with no per-flake policy filtering.
#[tokio::test]
async fn geo_search_enforces_view_policy_on_location_flake() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();
    let index_cfg = IndexConfig {
        reindex_min_bytes: 0,
        reindex_max_bytes: 1_000_000,
    };
    let mut fluree = FlureeBuilder::file(path).build().expect("build");
    let alias = "it/geo-policy:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        fluree
            .nameservice_mode()
            .publisher_arc()
            .expect("test setup requires ReadWrite nameservice mode"),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));

    local
        .run_until(async move {
            let ledger = fluree.create_ledger(alias).await.unwrap();

            // Paris (shareable) + London, both within 500km of the search center.
            let setup = json!({
                "@context": {"ex": "http://example.org/", "geo": "http://www.opengis.net/ont/geosparql#"},
                "@graph": [
                    {"@id": "ex:paris", "@type": "ex:City", "ex:canSee": true,
                     "ex:location": {"@value": "POINT(2.3522 48.8566)", "@type": "geo:wktLiteral"}},
                    {"@id": "ex:london", "@type": "ex:City",
                     "ex:location": {"@value": "POINT(-0.1278 51.5074)", "@type": "geo:wktLiteral"}}
                ]
            });
            let r1 = fluree
                .upsert_with_opts(ledger, &setup, TxnOpts::default(), CommitOpts::default(), &index_cfg)
                .await
                .unwrap();
            trigger_index_and_wait_outcome(&handle, alias, r1.receipt.t).await;
            let loaded = fluree.ledger(alias).await.expect("load ledger");
            assert!(
                loaded.snapshot.range_provider.is_some(),
                "geo search needs the binary index"
            );

            let geo_where = json!([
                {"@id": "?place", "ex:location": "?loc"},
                ["bind", "?dist", "(geof:distance ?loc \"POINT(2.3522 48.8566)\")"],
                ["filter", "(<= ?dist 500000)"]
            ]);

            // The loaded indexed ledger carries the binary store geo needs, so
            // query through that GraphDb (query_connection's path doesn't wire it
            // for geo). Same query body for control + policy.
            let query = json!({
                "@context": {"ex": "http://example.org/", "geo": "http://www.opengis.net/ont/geosparql#"},
                "select": ["?place"],
                "where": geo_where
            });

            // Control: no policy => both cities are within range.
            let control_json = support::query_jsonld(&fluree, &loaded, &query)
                .await
                .expect("control query")
                .to_jsonld(&loaded.snapshot)
                .expect("jsonld")
                .to_string();
            assert!(
                control_json.contains("paris") && control_json.contains("london"),
                "control: both cities should be in range; got {control_json}"
            );

            // Policy: ex:location is viewable only for shareable cities (Paris).
            let policy = json!([{
                "@id": "ex:locPolicy",
                "@type": "f:AccessPolicy",
                "f:action": "f:view",
                "f:onProperty": [{"@id": "http://example.org/location"}],
                "f:query": {
                    "@type": "@json",
                    "@value": {
                        "@context": {"ex": "http://example.org/"},
                        "where": [{"@id": "?$this", "ex:canSee": true}]
                    }
                }
            }]);
            let opts = GovernanceOptions {
                policy: Some(policy),
                default_allow: Some(false),
                ..Default::default()
            };
            let policy_ctx = policy_builder::build_policy_context_from_opts(
                &loaded.snapshot,
                loaded.novelty.as_ref(),
                None,
                loaded.t(),
                &opts,
                &[0],
            )
            .await
            .expect("build policy");

            let jsonld =
                support::query_jsonld_with_policy(&fluree, &loaded, &query, &policy_ctx)
                    .await
                    .expect("policy query")
                    .to_jsonld(&loaded.snapshot)
                    .expect("jsonld");
            let rendered = jsonld.to_string();
            assert!(
                rendered.contains("paris"),
                "Paris (viewable location) must remain; got {jsonld:#?}"
            );
            assert!(
                !rendered.contains("london"),
                "London (hidden location) must not leak through geo search; got {jsonld:#?}"
            );
        })
        .await;
}
