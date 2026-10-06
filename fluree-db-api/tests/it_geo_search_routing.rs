//! The geo rewrite turns `Triple + BIND(geof:distance) + FILTER(< r)` into an
//! index-accelerated GeoSearch only when a binary index covers the query's
//! `t`; otherwise the patterns run as written. Asserted through the
//! `geo_search` routing stamp, which this binary captures with a
//! thread-local subscriber — its own process, so concurrently running tests
//! cannot disturb the capture.

#![cfg(feature = "native")]

mod support;

use fluree_db_api::{FlureeBuilder, IndexConfig, LedgerState};
use fluree_db_transact::{CommitOpts, TxnOpts};
use serde_json::{json, Value as JsonValue};
use support::{start_background_indexer_local, trigger_index_and_wait_outcome};

fn geo_search_context() -> JsonValue {
    json!({
        "ex": "http://example.org/",
        "geo": "http://www.opengis.net/ont/geosparql#"
    })
}

/// A distance search over a ledger with no binary index yet — a fresh
/// ledger, or a time before the index's base — is answered by evaluating the
/// patterns as written, not rewritten into a GeoSearch that needs the index.
/// Once indexed, the rewrite takes over and gives the same answers.
#[tokio::test]
async fn geof_distance_answers_before_and_after_indexing() {
    let (stamps, _tracing) = support::span_capture::init_test_tracing();
    let geo_stamps = move |from: usize| -> (usize, Vec<String>) {
        let events = stamps.find_events("fast-path outcome");
        let outcomes = events[from..]
            .iter()
            .filter(|e| e.fields.get("site").map(String::as_str) == Some("geo_search"))
            .filter_map(|e| e.fields.get("outcome").cloned())
            .collect();
        (events.len(), outcomes)
    };
    // File-backed, with the indexer as the connection's indexing mode, so the
    // index it builds reaches the ledger loaded afterwards.
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let mut fluree = FlureeBuilder::file(tmp.path().to_string_lossy().to_string())
        .build()
        .expect("build");
    let alias = "it/geo-search-unindexed:main";
    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        fluree
            .nameservice_mode()
            .publisher_arc()
            .expect("test setup requires ReadWrite nameservice mode"),
        fluree_db_indexer::IndexerConfig::small(),
    );
    // Indexes the indexer publishes reach the ledgers this connection loads.
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));
    local
        .run_until(async move {
            let ledger = fluree.create_ledger(alias).await.expect("create");
            let cities = json!({
                "@context": geo_search_context(),
                "@graph": [
                    {"@id": "ex:paris", "ex:name": "Paris",
                     "ex:location": {"@value": "POINT(2.3522 48.8566)", "@type": "geo:wktLiteral"}},
                    {"@id": "ex:london", "ex:name": "London",
                     "ex:location": {"@value": "POINT(-0.1278 51.5074)", "@type": "geo:wktLiteral"}},
                    {"@id": "ex:tokyo", "ex:name": "Tokyo",
                     "ex:location": {"@value": "POINT(139.6917 35.6895)", "@type": "geo:wktLiteral"}}
                ]
            });
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 1_000_000,
            };
            let ledger = fluree
                .insert_with_opts(ledger, &cities, TxnOpts::default(), CommitOpts::default(), &index_cfg)
                .await
                .expect("insert")
                .ledger;

            let sparql = r#"
                PREFIX ex: <http://example.org/>
                PREFIX geof: <http://www.opengis.net/def/function/geosparql/>
                SELECT ?name WHERE {
                    ?place ex:location ?loc ; ex:name ?name .
                    BIND(geof:distance(?loc, "POINT(2.3522 48.8566)") AS ?dist)
                    FILTER(?dist < 500000)
                }
                ORDER BY ?dist
            "#;
            let jsonld = json!({
                "@context": geo_search_context(),
                "select": "?name",
                "where": [
                    { "@id": "?place", "ex:location": "?loc", "ex:name": "?name" },
                    ["bind", "?dist", "(geof:distance ?loc \"POINT(2.3522 48.8566)\")"],
                    ["filter", "(< ?dist 500000)"]
                ],
                "orderBy": "?dist"
            });
            let both = |ledger: LedgerState| {
                let (fluree, sparql, jsonld) = (&fluree, sparql, &jsonld);
                async move {
                    let by_sparql = support::query_sparql(fluree, &ledger, sparql)
                        .await
                        .expect("sparql")
                        .to_jsonld(&ledger.snapshot)
                        .expect("format");
                    let by_jsonld = support::query_jsonld(fluree, &ledger, jsonld)
                        .await
                        .expect("jsonld")
                        .to_jsonld(&ledger.snapshot)
                        .expect("format");
                    (by_sparql, by_jsonld)
                }
            };
            let expected = (json!([["Paris"], ["London"]]), json!(["Paris", "London"]));

            let (seen, _) = geo_stamps(0);
            assert_eq!(both(ledger.clone()).await, expected);
            let (seen, unindexed) = geo_stamps(seen);
            assert_eq!(unindexed, ["fallback:gate_declined"; 2]);

            trigger_index_and_wait_outcome(&handle, alias, ledger.t()).await;
            let indexed = fluree.ledger(alias).await.expect("load ledger");
            assert!(indexed.snapshot.range_provider.is_some(), "the index is loaded");
            assert_eq!(both(indexed).await, expected);
            let (_, indexed) = geo_stamps(seen);
            assert_eq!(indexed, ["proceed"; 2]);
        })
        .await;
}
