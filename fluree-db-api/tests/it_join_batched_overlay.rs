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
        (
            "values-novelty-key",
            r"PREFIX ex: <http://example.org/ns/>
              SELECT ?b ?v WHERE { VALUES ?b { ex:grace ex:bob } ?b ex:age ?v }
              ORDER BY ?b ?v",
            3, // bob 25 + 26, grace 35 — grace's key resolves via DictNovelty
        ),
    ];

    // Phase 1: base index + novelty tail. Each query gets its own span
    // capture so engagement is proven per query: under an active overlay the
    // flush span only exists in merge mode (the bail routes to per-row scans,
    // which never enter the flush).
    let view = fluree.db(ledger_id).await.expect("novelty view");
    let mut novelty_results = Vec::new();
    for (name, query, expected_len) in queries {
        let (spans, guard) = span_capture::init_test_tracing();
        let rows = run_query(&fluree, &view, query).await;
        drop(guard);
        assert_eq!(
            rows.len(),
            *expected_len,
            "{name}: row count under novelty; got {rows:?}"
        );
        let flushes = spans.find_spans("join_flush_batched_binary");
        assert!(
            !flushes.is_empty(),
            "{name}: batched subject lane should engage under novelty (merge \
             mode); captured spans: {:?}",
            spans.span_names()
        );
        if *name == "values-novelty-key" {
            // The novelty-only subject (ex:grace, Sid-form from VALUES) must
            // resolve through DictNovelty and accumulate alongside ex:bob —
            // accum_len 2 proves neither key fell back to a per-row scan.
            assert!(
                flushes
                    .iter()
                    .any(|s| s.fields.get("accum_len").map(String::as_str) == Some("2")),
                "{name}: both VALUES keys (incl. the novelty-only subject) \
                 should enter the batched accumulator; flush fields: {:?}",
                flushes.iter().map(|s| &s.fields).collect::<Vec<_>>()
            );
        }
        novelty_results.push(rows);
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

/// Differential test for the shared probe helpers under novelty: the EXISTS
/// lane, single and grouped OPTIONAL, and the property-join SPOT star walk —
/// all converted from the overlay-free bail to the per-subject ops merge.
#[tokio::test]
async fn probe_helpers_merge_novelty() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/probe-helpers-overlay:main";
    let ledger = genesis_ledger_for_fluree(&fluree, ledger_id);

    let base = json!({
        "@context": ctx(),
        "@graph": [
            {"@id": "ex:alice", "ex:knows": {"@id": "ex:bob"}},
            {"@id": "ex:carol", "ex:knows": {"@id": "ex:dave"}},
            {"@id": "ex:eve",   "ex:knows": {"@id": "ex:frank"}},
            {"@id": "ex:bob",   "ex:age": 25, "ex:name": "Bob", "ex:city": "Berlin"},
            {"@id": "ex:dave",  "ex:age": 30, "ex:name": "Dave", "ex:city": "Lyon"},
            {"@id": "ex:frank", "ex:age": 40, "ex:name": "Frank", "ex:city": "Oslo"},
            {"@id": "ex:lonely", "ex:age": 99}
        ]
    });
    let receipt = fluree.insert(ledger, &base).await.expect("base insert");
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex base");

    // Novelty tail: dave loses his age (name survives), frank's age is
    // retracted + re-asserted, bob gains a second age, grace exists only in
    // novelty (knows edge + age + name).
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
    let _receipt = fluree
        .insert(
            receipt.ledger,
            &json!({
                "@context": ctx(),
                "@graph": [
                    {"@id": "ex:bob", "ex:age": 26},
                    {"@id": "ex:alice", "ex:knows": {"@id": "ex:grace"}},
                    {"@id": "ex:grace", "ex:age": 35, "ex:name": "Grace", "ex:city": "Quito"}
                ]
            }),
        )
        .await
        .expect("novelty asserts");

    // (name, query, expected row count, engagement)
    enum Engage {
        ExistsFlush,
        NoGroupedFallback,
        PropertyJoinFast,
        DifferentialOnly,
    }
    let queries: &[(&str, &str, usize, Engage)] = &[
        (
            "exists-injected-age",
            r"PREFIX ex: <http://example.org/ns/>
              SELECT ?b WHERE { ex:alice ex:knows ?b . ?b ex:age 26 } ORDER BY ?b",
            1, // bob: his 26 exists only in novelty
            Engage::ExistsFlush,
        ),
        (
            "exists-retracted-age",
            r"PREFIX ex: <http://example.org/ns/>
              SELECT ?b WHERE { ex:carol ex:knows ?b . ?b ex:age 30 } ORDER BY ?b",
            0, // dave's 30 was novelty-retracted
            Engage::ExistsFlush,
        ),
        (
            "optional-retracted-row-survives",
            r"PREFIX ex: <http://example.org/ns/>
              SELECT ?b ?v WHERE { ex:carol ex:knows ?b . OPTIONAL { ?b ex:age ?v } }
              ORDER BY ?b ?v",
            1, // dave row stays with ?v unbound — never dropped
            Engage::DifferentialOnly,
        ),
        (
            "optional-injected",
            r"PREFIX ex: <http://example.org/ns/>
              SELECT ?b ?v WHERE { ex:alice ex:knows ?b . OPTIONAL { ?b ex:age ?v } }
              ORDER BY ?b ?v",
            3, // bob 25 + 26 (injected), grace 35 (novelty-only subject)
            Engage::DifferentialOnly,
        ),
        (
            "grouped-optional",
            r"PREFIX ex: <http://example.org/ns/>
              SELECT ?b ?v ?n
              WHERE { ex:alice ex:knows ?b . OPTIONAL { ?b ex:age ?v . ?b ex:name ?n } }
              ORDER BY ?b ?v ?n",
            3, // bob (25,Bob) + (26,Bob), grace (35,Grace)
            Engage::NoGroupedFallback,
        ),
        (
            // The bound object anchors the star on PropertyJoinOperator
            // (pure variable-object stars stay on the sequential join chain).
            "property-join-star",
            r#"PREFIX ex: <http://example.org/ns/>
              SELECT ?s ?v ?n
              WHERE { ?s ex:city "Quito" . ?s ex:age ?v . ?s ex:name ?n }
              ORDER BY ?s ?v ?n"#,
            1, // grace — city/age/name are all novelty-only
            Engage::PropertyJoinFast,
        ),
    ];

    let view = fluree.db(ledger_id).await.expect("novelty view");
    let mut novelty_results = Vec::new();
    for (name, query, expected_len, engage) in queries {
        let (spans, guard) = span_capture::init_test_tracing();
        let rows = run_query(&fluree, &view, query).await;
        drop(guard);
        assert_eq!(
            rows.len(),
            *expected_len,
            "{name}: row count under novelty; got {rows:?}"
        );
        match engage {
            Engage::ExistsFlush => {
                assert!(
                    spans.has_span("join_flush_batched_exists_binary"),
                    "{name}: exists lane should engage under novelty; spans: {:?}",
                    spans.span_names()
                );
            }
            Engage::NoGroupedFallback => {
                assert!(
                    !spans.has_event("grouped optional builder fallback"),
                    "{name}: grouped optional should stay batched under novelty"
                );
            }
            Engage::PropertyJoinFast => {
                let opens = spans.find_events("property_join: open complete");
                assert!(
                    opens.iter().any(|e| {
                        e.fields.get("used_spot_star_walk").map(String::as_str) == Some("true")
                            || e.fields.get("used_batched_probe").map(String::as_str)
                                == Some("true")
                    }),
                    "{name}: property join should use a batched walk/probe under \
                     novelty; events: {opens:?}"
                );
            }
            Engage::DifferentialOnly => {}
        }
        novelty_results.push(rows);
    }

    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex ground truth");
    let view = fluree.db(ledger_id).await.expect("indexed view");
    for ((name, query, _, _), novelty_rows) in queries.iter().zip(&novelty_results) {
        let indexed_rows = run_query(&fluree, &view, query).await;
        assert_eq!(
            &indexed_rows, novelty_rows,
            "{name}: novelty-merged probe helpers != reindexed ground truth"
        );
    }
}
