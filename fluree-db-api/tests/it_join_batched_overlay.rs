//! Differential tests for the batched subject-probe join lane under a
//! novelty overlay.
//!
//! The NestedLoopJoin batched lane (`scan_matches`) reads base leaflets
//! directly; it used to bail to per-row scans whenever the graph carried any
//! novelty. It now merges the right predicate's resolved overlay ops per
//! probed subject: novelty retracts suppress matched base rows, novelty
//! asserts inject new matches (including for subjects that exist only in
//! novelty), and cross-commit retract+re-assert chains net out via
//! `resolve_overlay_ops`.
//!
//! Each query runs against (a) the base index + novelty tail and (b) the same
//! ledger fully reindexed, asserting identical rows — plus explicit expected
//! values so a bug shared by both paths can't pass. Engagement is proven via
//! span capture: under an active overlay, the `join_flush_batched_binary`
//! span only exists when the merge mode engaged (the bail would route to
//! per-row scans, which never enter the flush).

#![cfg(feature = "native")]

mod support;

use fluree_db_api::{FlureeBuilder, QueryInput, ReindexOptions};
use serde_json::json;
use support::{genesis_ledger_for_fluree, normalize_rows, span_capture};

fn ctx() -> serde_json::Value {
    json!({"ex": "http://example.org/ns/"})
}

#[tokio::test]
async fn batched_subject_join_merges_novelty() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/join-batched-overlay:main";
    let ledger = genesis_ledger_for_fluree(&fluree, ledger_id);

    // t=1 (indexed): knows edges + ages + names. `lonely` must never join.
    let base = json!({
        "@context": ctx(),
        "@graph": [
            {"@id": "ex:alice", "ex:knows": {"@id": "ex:bob"}},
            {"@id": "ex:carol", "ex:knows": {"@id": "ex:dave"}},
            {"@id": "ex:eve",   "ex:knows": {"@id": "ex:frank"}},
            {"@id": "ex:bob",   "ex:age": 25, "ex:name": "Bob"},
            {"@id": "ex:dave",  "ex:age": 30},
            {"@id": "ex:frank", "ex:age": 40},
            {"@id": "ex:lonely", "ex:age": 99}
        ]
    });
    let receipt = fluree.insert(ledger, &base).await.expect("base insert");
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex base");

    // Novelty tail (never indexed until the ground-truth phase):
    // retract dave's only match — the carol row must vanish.
    let receipt = fluree
        .update(
            receipt.ledger,
            &json!({
                "@context": ctx(),
                "where":  {"@id": "ex:dave", "ex:age": "?a"},
                "delete": {"@id": "ex:dave", "ex:age": "?a"}
            }),
        )
        .await
        .expect("retract dave age");
    // Cross-commit retract + re-assert of the same fact — must net to exactly
    // one frank row (resolve_overlay_ops keeps the latest op).
    let receipt = fluree
        .update(
            receipt.ledger,
            &json!({
                "@context": ctx(),
                "where":  {"@id": "ex:frank", "ex:age": "?a"},
                "delete": {"@id": "ex:frank", "ex:age": "?a"}
            }),
        )
        .await
        .expect("retract frank age");
    let receipt = fluree
        .insert(
            receipt.ledger,
            &json!({"@context": ctx(), "@id": "ex:frank", "ex:age": 40}),
        )
        .await
        .expect("re-assert frank age");
    // Extra assert beside a surviving base row, plus a novelty-only subject
    // (grace: reached through a novelty knows edge, with novelty-only age and
    // a dict-novelty string value).
    let _receipt = fluree
        .insert(
            receipt.ledger,
            &json!({
                "@context": ctx(),
                "@graph": [
                    {"@id": "ex:bob", "ex:age": 26},
                    {"@id": "ex:alice", "ex:knows": {"@id": "ex:grace"}},
                    {"@id": "ex:grace", "ex:age": 35, "ex:name": "Grace"}
                ]
            }),
        )
        .await
        .expect("novelty asserts");

    // Bound-subject left patterns keep the planner driving from the knows
    // side, so the right age/name probe is the batched subject lane.
    let queries: &[(&str, &str, usize)] = &[
        (
            "alice-ages",
            r"PREFIX ex: <http://example.org/ns/>
              SELECT ?b ?v WHERE { ex:alice ex:knows ?b . ?b ex:age ?v }
              ORDER BY ?b ?v",
            3, // bob 25, bob 26 (injected), grace 35 (novelty-only subject)
        ),
        (
            "carol-ages",
            r"PREFIX ex: <http://example.org/ns/>
              SELECT ?b ?v WHERE { ex:carol ex:knows ?b . ?b ex:age ?v }
              ORDER BY ?b ?v",
            0, // dave's only age was novelty-retracted
        ),
        (
            "eve-ages",
            r"PREFIX ex: <http://example.org/ns/>
              SELECT ?b ?v WHERE { ex:eve ex:knows ?b . ?b ex:age ?v }
              ORDER BY ?b ?v",
            1, // retract + re-assert nets to one frank row
        ),
        (
            "alice-names",
            r"PREFIX ex: <http://example.org/ns/>
              SELECT ?b ?n WHERE { ex:alice ex:knows ?b . ?b ex:name ?n }
              ORDER BY ?b ?n",
            2, // "Bob" (base), "Grace" (dict-novelty string)
        ),
        (
            "alice-ages-filtered",
            r"PREFIX ex: <http://example.org/ns/>
              SELECT ?b ?v WHERE { ex:alice ex:knows ?b . ?b ex:age ?v . FILTER(?v > 30) }
              ORDER BY ?b ?v",
            1, // bounds applied to injected asserts too: only grace 35
        ),
    ];

    // Phase 1: base index + novelty tail, with span capture for engagement.
    let view = fluree.db(ledger_id).await.expect("novelty view");
    let mut novelty_results = Vec::new();
    {
        let (spans, _guard) = span_capture::init_test_tracing();
        for (name, query, expected_len) in queries {
            let rows = run_query(&fluree, &view, query).await;
            assert_eq!(
                rows.len(),
                *expected_len,
                "{name}: row count under novelty; got {rows:?}"
            );
            novelty_results.push(rows);
        }
        assert!(
            spans.has_span("join_flush_batched_binary"),
            "batched subject lane should engage under novelty (merge mode); \
             captured spans: {:?}",
            spans.span_names()
        );
    }

    // Phase 2: ground truth — fully reindexed, same queries, identical rows.
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex ground truth");
    let view = fluree.db(ledger_id).await.expect("indexed view");
    for ((name, query, _), novelty_rows) in queries.iter().zip(&novelty_results) {
        let indexed_rows = run_query(&fluree, &view, query).await;
        assert_eq!(
            &indexed_rows, novelty_rows,
            "{name}: novelty-merged batched join != reindexed ground truth"
        );
    }
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
