//! Differential tests for the CyclicBgpOperator cascading bounded-probe path.
//!
//! Each cyclic query runs under three configurations against the same indexed
//! ledger and must produce identical bindings:
//!   1. `FLUREE_CYCLIC_BGP=0` — fallback nested-loop join tree (ground truth)
//!   2. probing forced on (`FLUREE_CYCLIC_BGP_PROBE_SCAN_RATIO=1`)
//!   3. probing forced off (`FLUREE_CYCLIC_BGP_MAX_BOUNDED_SUBJECTS=0`) — the
//!      full-scan semi-join path
//!
//! All env mutation lives in ONE test fn so parallel test threads in this
//! binary can't race on process-global state.

#![cfg(feature = "native")]

mod support;

use fluree_db_api::{FlureeBuilder, IndexConfig, LedgerManagerConfig, QueryInput};
use fluree_db_transact::{CommitOpts, TxnOpts};
use serde_json::json;
use support::{
    genesis_ledger_for_fluree, normalize_rows, span_capture, start_background_indexer_local,
    trigger_index_and_wait_outcome,
};

fn clear_cyclic_env() {
    for key in [
        "FLUREE_CYCLIC_BGP",
        "FLUREE_CYCLIC_BGP_PROBE_SCAN_RATIO",
        "FLUREE_CYCLIC_BGP_MAX_BOUNDED_SUBJECTS",
    ] {
        std::env::remove_var(key);
    }
}

#[tokio::test]
async fn cyclic_bgp_bounded_probe_matches_fallback() {
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/cyclic-bgp-probe:main";

    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        fluree.nameservice_mode().as_arc_indexing_nameservice().expect("test fluree has writable nameservice"),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };
            let ledger = genesis_ledger_for_fluree(&fluree, ledger_id);

            // Edge data covering: two directed triangles sharing node n1/n3
            // (multiplicity through shared vertices), a "shortcut" triangle,
            // a mixed-direction square, and dangling edges on every predicate
            // that must not join.
            let insert = json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [
                    // Directed triangle 1: n1 -p1-> n2 -p2-> n3 -p3-> n1
                    {"@id": "ex:n1", "ex:p1": {"@id": "ex:n2"}},
                    {"@id": "ex:n2", "ex:p2": {"@id": "ex:n3"}},
                    {"@id": "ex:n3", "ex:p3": {"@id": "ex:n1"}},
                    // Directed triangle 2: n4 -p1-> n5 -p2-> n6 -p3-> n4
                    {"@id": "ex:n4", "ex:p1": {"@id": "ex:n5"}},
                    {"@id": "ex:n5", "ex:p2": {"@id": "ex:n6"}},
                    {"@id": "ex:n6", "ex:p3": {"@id": "ex:n4"}},
                    // Second path through triangle 1's corners:
                    // n1 -p1-> n20 -p2-> n3 (closes via existing n3 -p3-> n1)
                    {"@id": "ex:n1", "ex:p1": {"@id": "ex:n20"}},
                    {"@id": "ex:n20", "ex:p2": {"@id": "ex:n3"}},
                    // Shortcut triangle: n1 -p4-> n3 (closes n1 -p1-> n2 -p2-> n3)
                    {"@id": "ex:n1", "ex:p4": {"@id": "ex:n3"}},
                    // Mixed square: n1 -p1-> n2 -p2-> n3, n40 -p4-> n3, n1 -p5-> n40
                    {"@id": "ex:n40", "ex:p4": {"@id": "ex:n3"}},
                    {"@id": "ex:n1", "ex:p5": {"@id": "ex:n40"}},
                    // Dangling edges that must never appear in results.
                    {"@id": "ex:n10", "ex:p1": {"@id": "ex:n11"}},
                    {"@id": "ex:n12", "ex:p2": {"@id": "ex:n13"}},
                    {"@id": "ex:n14", "ex:p3": {"@id": "ex:n15"}},
                    {"@id": "ex:n16", "ex:p4": {"@id": "ex:n17"}},
                    {"@id": "ex:n18", "ex:p5": {"@id": "ex:n19"}}
                ]
            });
            let result = fluree
                .insert_with_opts(
                    ledger,
                    &insert,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("insert");
            let ledger = result.ledger;

            trigger_index_and_wait_outcome(&handle, ledger_id, ledger.t()).await;
            let view = fluree.db(ledger_id).await.expect("load view");

            // (name, query) pairs covering both join modes and both shapes:
            // - directed 3-cycle: RefOnly mode, previously never probe-eligible
            // - shortcut triangle: EncodedObject mode (object-only ?c)
            // - mixed 4-cycle: EncodedObject square
            let queries = [
                (
                    "directed-triangle",
                    r"PREFIX ex: <http://example.org/ns/>
                      SELECT ?a ?b ?c
                      WHERE { ?a ex:p1 ?b . ?b ex:p2 ?c . ?c ex:p3 ?a }
                      ORDER BY ?a ?b ?c",
                ),
                (
                    "shortcut-triangle",
                    r"PREFIX ex: <http://example.org/ns/>
                      SELECT ?a ?b ?c
                      WHERE { ?a ex:p1 ?b . ?b ex:p2 ?c . ?a ex:p4 ?c }
                      ORDER BY ?a ?b ?c",
                ),
                (
                    "mixed-square",
                    r"PREFIX ex: <http://example.org/ns/>
                      SELECT ?a ?b ?c ?d
                      WHERE { ?a ex:p1 ?b . ?b ex:p2 ?c . ?d ex:p4 ?c . ?a ex:p5 ?d }
                      ORDER BY ?a ?b ?c ?d",
                ),
            ];

            for (name, query) in queries {
                // 1. Fallback operator tree = ground truth.
                clear_cyclic_env();
                std::env::set_var("FLUREE_CYCLIC_BGP", "0");
                let expected = run_query(&fluree, &view, query).await;
                assert!(!expected.is_empty(), "{name}: fallback should produce rows");

                // 2. Cascade with probing forced on (every gate-passing edge probes).
                clear_cyclic_env();
                std::env::set_var("FLUREE_CYCLIC_BGP_PROBE_SCAN_RATIO", "1");
                let probed = run_query(&fluree, &view, query).await;
                assert_eq!(probed, expected, "{name}: probed cascade != fallback");

                // 3. Cascade with probing forced off (full-scan semi-join path).
                clear_cyclic_env();
                std::env::set_var("FLUREE_CYCLIC_BGP_MAX_BOUNDED_SUBJECTS", "0");
                let scanned = run_query(&fluree, &view, query).await;
                assert_eq!(scanned, expected, "{name}: full-scan cascade != fallback");
            }

            // Phase 2: novelty tail — retract triangle 2's closing edge and
            // assert a triangle that exists only in novelty. Probes must merge
            // the per-edge ops (suppressing the retracted closure, injecting
            // the novelty-only edges) and still match the fallback.
            let receipt = fluree
                .update(
                    ledger,
                    &json!({
                        "@context": {"ex": "http://example.org/ns/"},
                        "where":  {"@id": "ex:n6", "ex:p3": "?o"},
                        "delete": {"@id": "ex:n6", "ex:p3": "?o"}
                    }),
                )
                .await
                .expect("retract n6 closing edge");
            let _receipt = fluree
                .insert(
                    receipt.ledger,
                    &json!({
                        "@context": {"ex": "http://example.org/ns/"},
                        "@graph": [
                            {"@id": "ex:n7", "ex:p1": {"@id": "ex:n8"}},
                            {"@id": "ex:n8", "ex:p2": {"@id": "ex:n9"}},
                            {"@id": "ex:n9", "ex:p3": {"@id": "ex:n7"}}
                        ]
                    }),
                )
                .await
                .expect("novelty triangle");
            let novelty_t = _receipt.ledger.t();
            // `db()` can serve a cached pre-commit view in this manager
            // configuration; pin the post-novelty `t` explicitly.
            let view = fluree
                .db_at_t(ledger_id, novelty_t)
                .await
                .expect("novelty view");

            for (name, query) in queries {
                clear_cyclic_env();
                std::env::set_var("FLUREE_CYCLIC_BGP", "0");
                let expected = run_query(&fluree, &view, query).await;

                clear_cyclic_env();
                std::env::set_var("FLUREE_CYCLIC_BGP_PROBE_SCAN_RATIO", "1");
                let (spans, guard) = span_capture::init_test_tracing();
                let probed = run_query(&fluree, &view, query).await;
                drop(guard);
                assert_eq!(
                    probed, expected,
                    "{name}: probed cascade under novelty != fallback"
                );
                if name == "directed-triangle" {
                    assert!(
                        spans.has_event("cyclic cascade: probing edge per-subject"),
                        "{name}: bounded probes should engage under novelty"
                    );
                    assert!(
                        expected
                            .iter()
                            .any(|row| format!("{row:?}").contains("ex:n7")),
                        "{name}: novelty-only triangle should appear; rows: {expected:?}"
                    );
                    assert!(
                        !expected
                            .iter()
                            .any(|row| format!("{row:?}").contains("ex:n4")),
                        "{name}: retracted closure should disappear; rows: {expected:?}"
                    );
                }

                clear_cyclic_env();
                std::env::set_var("FLUREE_CYCLIC_BGP_MAX_BOUNDED_SUBJECTS", "0");
                let scanned = run_query(&fluree, &view, query).await;
                assert_eq!(
                    scanned, expected,
                    "{name}: full-scan cascade under novelty != fallback"
                );
            }
            clear_cyclic_env();
        })
        .await;
}

async fn run_query(
    fluree: &fluree_db_api::Fluree,
    view: &fluree_db_api::GraphDb,
    query: &str,
) -> Vec<serde_json::Value> {
    let result = fluree
        .query(view, QueryInput::Sparql(query))
        .await
        .expect("query");
    let jsonld = result.to_jsonld(&view.snapshot).expect("to_jsonld");
    normalize_rows(&jsonld)
}
