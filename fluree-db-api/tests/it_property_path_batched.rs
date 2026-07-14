//! Differential tests for the property-path batched raw-id lane.
//!
//! `PropertyPathOperator` traversals (`+`/`*`, forward/backward/inverse,
//! alternation, bound-bound reachability, bounded wildcard) run a raw-id lane
//! when the view allows: each BFS level expands with batched galloping index
//! sweeps over base rows, and overlay correctness is per-node (dirty or
//! novelty-only nodes take the per-node Sid fallback, which merges novelty).
//!
//! Each query runs against (a) the base index + novelty tail and (b) the same
//! ledger fully reindexed, asserting identical rows — plus explicit expected
//! values so a bug shared by both paths can't pass. Engagement is proven via
//! event capture: the lane logs `property path raw-id expand` with
//! `batched`/`fallback` split per level.

#![cfg(feature = "native")]

use crate::support::{genesis_ledger_for_fluree, normalize_rows, span_capture};
use fluree_db_api::{FlureeBuilder, QueryInput, ReindexOptions};
use serde_json::json;

fn ctx() -> serde_json::Value {
    json!({"ex": "http://example.org/ns/"})
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

/// Typed paths (`+`/`*`, backward, inverse, alternation, reachability) over
/// an indexed base: the raw-id lane must expand with zero fallback on a clean
/// HEAD, and stay correct — via per-node fallback — under a novelty tail.
#[tokio::test]
async fn property_path_batched_lane_clean_and_novelty() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/property-path-batched:main";
    let ledger = genesis_ledger_for_fluree(&fluree, ledger_id);

    // t=1 (indexed): a 4-cycle of `knows` plus a `likes` branch for the
    // alternation path. `c` has exactly one knows edge so the novelty phase
    // can retract it with a variable pattern.
    let base = json!({
        "@context": ctx(),
        "@graph": [
            {"@id": "ex:a", "ex:knows": {"@id": "ex:b"}},
            {"@id": "ex:b", "ex:knows": {"@id": "ex:c"}},
            {"@id": "ex:c", "ex:knows": {"@id": "ex:d"}},
            {"@id": "ex:d", "ex:knows": {"@id": "ex:a"}},
            {"@id": "ex:b", "ex:likes": {"@id": "ex:p"}},
            {"@id": "ex:p", "ex:likes": {"@id": "ex:q"}}
        ]
    });
    let receipt = fluree.insert(ledger, &base).await.expect("base insert");
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex base");

    // Phase 1: clean HEAD. Every traversal level must be fully batched
    // (fallback = 0 — the overlay is empty, so no node is dirty).
    let clean_queries: &[(&str, &str, usize)] = &[
        (
            "forward-plus",
            r"PREFIX ex: <http://example.org/ns/>
              SELECT ?o WHERE { ex:a ex:knows+ ?o } ORDER BY ?o",
            4, // b, c, d, and a itself via the 4-cycle
        ),
        (
            "forward-star",
            r"PREFIX ex: <http://example.org/ns/>
              SELECT ?o WHERE { ex:a ex:knows* ?o } ORDER BY ?o",
            4, // a (zero-length), b, c, d
        ),
        (
            "backward-plus",
            r"PREFIX ex: <http://example.org/ns/>
              SELECT ?s WHERE { ?s ex:knows+ ex:d } ORDER BY ?s",
            4, // c, b, a, and d itself via the cycle
        ),
        (
            "inverse-plus",
            r"PREFIX ex: <http://example.org/ns/>
              SELECT ?s WHERE { ex:d (^ex:knows)+ ?s } ORDER BY ?s",
            4, // same set as backward-plus, driven from the inverse side
        ),
        (
            "alternation-plus",
            r"PREFIX ex: <http://example.org/ns/>
              SELECT ?o WHERE { ex:a (ex:knows|ex:likes)+ ?o } ORDER BY ?o",
            6, // b, c, d, a via knows-cycle; p, q via likes
        ),
        (
            "exists-true",
            r"PREFIX ex: <http://example.org/ns/>
              SELECT (1 AS ?x) WHERE { ex:a ex:knows+ ex:d }",
            1,
        ),
        (
            "exists-false",
            r"PREFIX ex: <http://example.org/ns/>
              SELECT (1 AS ?x) WHERE { ex:d ex:knows+ ex:q }",
            0, // q is reachable only via likes
        ),
    ];

    let view = fluree.db(ledger_id).await.expect("clean view");
    for (name, query, expected_len) in clean_queries {
        let (spans, guard) = span_capture::init_test_tracing();
        let rows = run_query(&fluree, &view, query).await;
        drop(guard);
        assert_eq!(
            rows.len(),
            *expected_len,
            "{name}: row count on clean HEAD; got {rows:?}"
        );
        let expands = spans.find_events("property path raw-id expand");
        assert!(
            !expands.is_empty(),
            "{name}: raw-id lane should engage on a clean indexed HEAD"
        );
        assert!(
            expands
                .iter()
                .all(|e| e.fields.get("fallback").map(String::as_str) == Some("0")),
            "{name}: clean HEAD expansion must not fall back per-node; events: {:?}",
            expands.iter().map(|e| &e.fields).collect::<Vec<_>>()
        );
        assert!(
            expands
                .iter()
                .any(|e| e.fields.get("batched").is_some_and(|b| b != "0")),
            "{name}: at least one level should batch persisted ids; events: {:?}",
            expands.iter().map(|e| &e.fields).collect::<Vec<_>>()
        );
    }

    // Novelty tail (never indexed until the ground-truth phase): break the
    // cycle at c, graft a novelty-only branch b→e→f, and churn a's edge
    // (retract + re-assert nets to the same fact but dirties `a`).
    let receipt = fluree
        .update(
            receipt.ledger,
            &json!({
                "@context": ctx(),
                "where":  {"@id": "ex:c", "ex:knows": "?o"},
                "delete": {"@id": "ex:c", "ex:knows": "?o"}
            }),
        )
        .await
        .expect("retract c knows");
    let receipt = fluree
        .update(
            receipt.ledger,
            &json!({
                "@context": ctx(),
                "where":  {"@id": "ex:a", "ex:knows": "?o"},
                "delete": {"@id": "ex:a", "ex:knows": "?o"}
            }),
        )
        .await
        .expect("retract a knows");
    let _receipt = fluree
        .insert(
            receipt.ledger,
            &json!({
                "@context": ctx(),
                "@graph": [
                    {"@id": "ex:a", "ex:knows": {"@id": "ex:b"}},
                    {"@id": "ex:b", "ex:knows": {"@id": "ex:e"}},
                    {"@id": "ex:e", "ex:knows": {"@id": "ex:f"}},
                    {"@id": "ex:a", "ex:newpred": {"@id": "ex:b"}}
                ]
            }),
        )
        .await
        .expect("novelty asserts");

    let novelty_queries: &[(&str, &str, usize)] = &[
        (
            "novelty-forward-plus",
            r"PREFIX ex: <http://example.org/ns/>
              SELECT ?o WHERE { ex:a ex:knows+ ?o } ORDER BY ?o",
            4, // b, c (dead end now), e, f — the cycle back to a is broken
        ),
        (
            "novelty-backward-plus",
            r"PREFIX ex: <http://example.org/ns/>
              SELECT ?s WHERE { ?s ex:knows+ ex:f } ORDER BY ?s",
            4, // e, b, a, and d (d→a survives)
        ),
        (
            "novelty-exists-broken",
            r"PREFIX ex: <http://example.org/ns/>
              SELECT (1 AS ?x) WHERE { ex:a ex:knows+ ex:d }",
            0, // c→d was retracted
        ),
        (
            "novelty-exists-new",
            r"PREFIX ex: <http://example.org/ns/>
              SELECT (1 AS ?x) WHERE { ex:a ex:knows+ ex:f }",
            1,
        ),
        (
            // The predicate exists only in novelty: the raw-id lane must
            // decline (no base p_id) and the Sid lane must still answer.
            "novelty-only-predicate",
            r"PREFIX ex: <http://example.org/ns/>
              SELECT ?o WHERE { ex:a ex:newpred+ ?o } ORDER BY ?o",
            1, // b
        ),
    ];

    let view = fluree.db(ledger_id).await.expect("novelty view");
    let mut novelty_results = Vec::new();
    for (name, query, expected_len) in novelty_queries {
        let rows = run_query(&fluree, &view, query).await;
        assert_eq!(
            rows.len(),
            *expected_len,
            "{name}: row count under novelty; got {rows:?}"
        );
        novelty_results.push(rows);
    }

    // Ground truth: fully reindexed, same queries, identical rows.
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex ground truth");
    let view = fluree.db(ledger_id).await.expect("indexed view");
    for ((name, query, _), novelty_rows) in novelty_queries.iter().zip(&novelty_results) {
        let indexed_rows = run_query(&fluree, &view, query).await;
        assert_eq!(
            &indexed_rows, novelty_rows,
            "{name}: novelty-merged raw-id lane != reindexed ground truth"
        );
    }
}

/// Wildcard paths through the Cypher surface: a fused anonymous-hop run
/// (exact-depth wildcard path) and an untyped bounded range, both against an
/// indexed base and then under a novelty tail with ground-truth comparison.
#[tokio::test]
async fn property_path_batched_wildcard_cypher() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/property-path-batched-cypher:main";
    let ledger = genesis_ledger_for_fluree(&fluree, ledger_id);

    // Bare names (namespace 0): a→b→c→d chain over `rel`, with a `name`
    // data property that wildcard hops must ignore.
    let base = json!({
        "@graph": [
            {"@id": "a", "name": "a", "rel": {"@id": "b"}},
            {"@id": "b", "name": "b", "rel": {"@id": "c"}},
            {"@id": "c", "name": "c", "rel": {"@id": "d"}},
            {"@id": "d", "name": "d"}
        ]
    });
    let receipt = fluree.insert(ledger, &base).await.expect("base insert");
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex base");

    let fused = "MATCH (s {name: 'a'})-->()-->(n) RETURN DISTINCT n.name AS nm ORDER BY nm";
    let bounded = "MATCH (s {name: 'a'})-[*1..3]->(n) RETURN DISTINCT n.name AS nm ORDER BY nm";

    let run_cypher = |view: fluree_db_api::GraphDb, q: &'static str| {
        let fluree = &fluree;
        async move {
            let result = fluree.query_cypher(&view, q).await.expect("cypher query");
            let jsonld = result.to_jsonld(&view.snapshot).expect("to_jsonld");
            normalize_rows(&jsonld)
        }
    };

    // Clean HEAD: fused exact-depth-2 reaches only c; 1..3 reaches b, c, d.
    let view = fluree.db(ledger_id).await.expect("clean view");
    let (spans, guard) = span_capture::init_test_tracing();
    let fused_rows = run_cypher(view.clone(), fused).await;
    drop(guard);
    assert_eq!(fused_rows.len(), 1, "fused depth-2 on clean HEAD: only c");
    assert!(
        spans.has_event("property path raw-id expand"),
        "fused wildcard run should engage the raw-id lane on a clean HEAD"
    );
    let bounded_rows = run_cypher(view.clone(), bounded).await;
    assert_eq!(bounded_rows.len(), 3, "bounded 1..3 on clean HEAD: b, c, d");

    // Novelty tail: cut c→d, graft b→e (novelty-only node).
    let receipt = fluree
        .update(
            receipt.ledger,
            &json!({
                "where":  {"@id": "c", "rel": "?o"},
                "delete": {"@id": "c", "rel": "?o"}
            }),
        )
        .await
        .expect("retract c rel");
    let _receipt = fluree
        .insert(
            receipt.ledger,
            &json!({"@graph": [
                {"@id": "b", "rel": {"@id": "e"}},
                {"@id": "e", "name": "e"}
            ]}),
        )
        .await
        .expect("novelty asserts");

    let view = fluree.db(ledger_id).await.expect("novelty view");
    let fused_novelty = run_cypher(view.clone(), fused).await;
    assert_eq!(fused_novelty.len(), 2, "fused depth-2 under novelty: c, e");
    let bounded_novelty = run_cypher(view.clone(), bounded).await;
    assert_eq!(
        bounded_novelty.len(),
        3,
        "bounded 1..3 under novelty: b, c, e (d unreachable)"
    );

    // Ground truth after reindex.
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex ground truth");
    let view = fluree.db(ledger_id).await.expect("indexed view");
    assert_eq!(
        run_cypher(view.clone(), fused).await,
        fused_novelty,
        "fused: novelty-merged != reindexed ground truth"
    );
    assert_eq!(
        run_cypher(view.clone(), bounded).await,
        bounded_novelty,
        "bounded: novelty-merged != reindexed ground truth"
    );
}
