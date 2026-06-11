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
                    {"@id": "ex:grace", "ex:age": 35, "ex:name": "Grace", "ex:city": "Quito",
                     "ex:nick": "G"}
                ]
            }),
        )
        .await
        .expect("novelty asserts");

    // (name, query, expected row count, engagement)
    enum Engage {
        ExistsFlush,
        OptionalBatched,
        GroupedOptionalBatched,
        SpotStarWalk,
        BatchedProbe,
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
            Engage::OptionalBatched,
        ),
        (
            "optional-injected",
            r"PREFIX ex: <http://example.org/ns/>
              SELECT ?b ?v WHERE { ex:alice ex:knows ?b . OPTIONAL { ?b ex:age ?v } }
              ORDER BY ?b ?v",
            3, // bob 25 + 26 (injected), grace 35 (novelty-only subject)
            Engage::OptionalBatched,
        ),
        (
            // Two consecutive single-triple OPTIONALs take the grouped
            // builder (a multi-triple OPTIONAL block goes through the
            // correlated per-row chain, whose inner joins merge via the
            // batched subject lane instead).
            "grouped-optional",
            r"PREFIX ex: <http://example.org/ns/>
              SELECT ?b ?v ?n
              WHERE { ex:alice ex:knows ?b .
                      OPTIONAL { ?b ex:age ?v } OPTIONAL { ?b ex:name ?n } }
              ORDER BY ?b ?v ?n",
            3, // bob (25,Bob) + (26,Bob), grace (35,Grace)
            Engage::GroupedOptionalBatched,
        ),
        (
            // The bound object anchors the star on PropertyJoinOperator
            // (pure variable-object stars stay on the sequential join chain).
            "property-join-spot-walk",
            r#"PREFIX ex: <http://example.org/ns/>
              SELECT ?s ?v ?n
              WHERE { ?s ex:city "Quito" . ?s ex:age ?v . ?s ex:name ?n }
              ORDER BY ?s ?v ?n"#,
            1, // grace — city/age/name are all novelty-only
            Engage::SpotStarWalk,
        ),
        (
            // ex:nick exists only in novelty, so the whole-star SPOT plan
            // declines and the remaining base predicates take the chunked
            // batched probes instead (the novelty-only predicate full-scans).
            "property-join-chunked-probe",
            r#"PREFIX ex: <http://example.org/ns/>
              SELECT ?s ?k
              WHERE { ?s ex:city "Quito" . ?s ex:age 35 . ?s ex:nick ?k }
              ORDER BY ?s ?k"#,
            1, // grace ("G"); both base predicates are bound-object so one of
            // them probes right after the driver (the novelty-only ex:nick
            // declines the SPOT walk and full-scans)
            Engage::BatchedProbe,
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
            Engage::OptionalBatched => {
                assert!(
                    spans.has_event("optional batched probe complete"),
                    "{name}: single OPTIONAL should stay batched under novelty"
                );
            }
            Engage::GroupedOptionalBatched => {
                assert!(
                    spans.has_event("grouped optional batched probe complete"),
                    "{name}: grouped OPTIONAL should stay batched under novelty"
                );
            }
            Engage::SpotStarWalk => {
                let opens = spans.find_events("property_join: open complete");
                assert!(
                    opens
                        .iter()
                        .any(|e| e.fields.get("used_spot_star_walk").map(String::as_str)
                            == Some("true")),
                    "{name}: property join should use the SPOT star walk under \
                     novelty; events: {opens:?}"
                );
            }
            Engage::BatchedProbe => {
                let opens = spans.find_events("property_join: open complete");
                assert!(
                    opens
                        .iter()
                        .any(|e| e.fields.get("used_batched_probe").map(String::as_str)
                            == Some("true")),
                    "{name}: property join should use the chunked batched probes \
                     under novelty; events: {opens:?}"
                );
            }
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

/// Differential test for the bound-object (OPST) lane under novelty: retracts
/// suppress matched base rows (including multi-entry `@list` refs, whose
/// retract identity lives in `o_i` — the column the lane's narrow projection
/// previously never decoded), asserts inject new matches, and novelty-only
/// subjects emit as encoded ids.
#[tokio::test]
async fn batched_object_join_merges_novelty() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/join-batched-object-overlay:main";
    let ledger = genesis_ledger_for_fluree(&fluree, ledger_id);

    let base = json!({
        "@context": ctx(),
        "@graph": [
            {"@id": "ex:alice", "ex:knows": {"@id": "ex:bob"}},
            {"@id": "ex:b1", "ex:author": {"@id": "ex:alice"}},
            {"@id": "ex:b2", "ex:author": {"@id": "ex:bob"}},
            {"@id": "ex:b5", "ex:authors": {"@list": [{"@id": "ex:bob"}, {"@id": "ex:bob"}]}},
            {"@id": "ex:d1", "ex:author": {"@id": "ex:dave"}}
        ]
    });
    let receipt = fluree.insert(ledger, &base).await.expect("base insert");
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex base");

    // Novelty tail: b2's author retracted, b3 asserted, grace + b4 exist only
    // in novelty, and b5's two-entry author @list is fully retracted (both
    // flakes share (s, p, o) and differ only in o_i).
    let receipt = fluree
        .update(
            receipt.ledger,
            &json!({
                "@context": ctx(),
                "where":  {"@id": "ex:b2", "ex:author": "?a"},
                "delete": {"@id": "ex:b2", "ex:author": "?a"}
            }),
        )
        .await
        .expect("retract b2 author");
    let receipt = fluree
        .update(
            receipt.ledger,
            &json!({
                "@context": ctx(),
                "where":  {"@id": "ex:b5", "ex:authors": "?a"},
                "delete": {"@id": "ex:b5", "ex:authors": "?a"}
            }),
        )
        .await
        .expect("retract b5 author list");
    let _receipt = fluree
        .insert(
            receipt.ledger,
            &json!({
                "@context": ctx(),
                "@graph": [
                    {"@id": "ex:b3", "ex:author": {"@id": "ex:bob"}},
                    {"@id": "ex:alice", "ex:knows": {"@id": "ex:grace"}},
                    {"@id": "ex:b4", "ex:author": {"@id": "ex:grace"}}
                ]
            }),
        )
        .await
        .expect("novelty asserts");

    let queries: &[(&str, &str, usize)] = &[
        (
            "object-probe-mixed",
            r"PREFIX ex: <http://example.org/ns/>
              SELECT ?b ?x WHERE { ex:alice ex:knows ?b . ?x ex:author ?b }
              ORDER BY ?b ?x",
            2, // (bob, b3 injected) + (grace novelty-only, b4 injected);
               // b2 retracted, b1/d1 authors not known by alice
        ),
        (
            "object-probe-list-retract",
            r"PREFIX ex: <http://example.org/ns/>
              SELECT ?x WHERE { VALUES ?b { ex:bob } ?x ex:authors ?b }
              ORDER BY ?x",
            1, // the delete's WHERE dedupes the repeated @list value to one
               // binding, so exactly ONE of the two o_i entries is retracted:
               // the fate check must discriminate the entries by o_i (a
               // default-read o_i would either resurrect both or drop both)
        ),
    ];

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
        assert!(
            spans.has_span("join_flush_batched_object_binary"),
            "{name}: bound-object lane should engage under novelty; spans: {:?}",
            spans.span_names()
        );
        assert!(
            spans.has_event("join batched object flush merged novelty overlay"),
            "{name}: object flush should merge the overlay"
        );
        novelty_results.push(rows);
    }

    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex ground truth");
    let view = fluree.db(ledger_id).await.expect("indexed view");
    for ((name, query, _), novelty_rows) in queries.iter().zip(&novelty_results) {
        let indexed_rows = run_query(&fluree, &view, query).await;
        assert_eq!(
            &indexed_rows, novelty_rows,
            "{name}: novelty-merged object join != reindexed ground truth"
        );
    }
}

/// Decimal (NumBig arena) overlay translation: retracts and re-asserts of
/// already-indexed `xsd:decimal` values resolve to their arena handles, so
/// the batched lane stays live (previously ANY decimal in novelty declined
/// the whole predicate lane and, on the range path, warned per flake). A
/// value the index has never seen has no handle — the lane declines to the
/// overlay-correct fallback.
#[tokio::test]
async fn batched_join_decimal_novelty() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/join-batched-decimal:main";
    let ledger = genesis_ledger_for_fluree(&fluree, ledger_id);

    fn dec(v: &str) -> serde_json::Value {
        json!({"@value": v, "@type": "http://www.w3.org/2001/XMLSchema#decimal"})
    }

    let base = json!({
        "@context": ctx(),
        "@graph": [
            {"@id": "ex:alice", "ex:knows": {"@id": "ex:bob"}},
            {"@id": "ex:carol", "ex:knows": {"@id": "ex:dave"}},
            {"@id": "ex:eve",   "ex:knows": {"@id": "ex:frank"}},
            {"@id": "ex:bob",   "ex:budget": dec("19.99")},
            {"@id": "ex:dave",  "ex:budget": dec("30.50")},
            {"@id": "ex:frank", "ex:budget": dec("40.25")}
        ]
    });
    let receipt = fluree.insert(ledger, &base).await.expect("base insert");
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex base");

    // Novelty phase A: only arena-resolvable decimal ops — dave's budget
    // retracted, frank's retracted and re-asserted with the same value.
    let receipt = fluree
        .update(
            receipt.ledger,
            &json!({
                "@context": ctx(),
                "where":  {"@id": "ex:dave", "ex:budget": "?v"},
                "delete": {"@id": "ex:dave", "ex:budget": "?v"}
            }),
        )
        .await
        .expect("retract dave budget");
    let receipt = fluree
        .update(
            receipt.ledger,
            &json!({
                "@context": ctx(),
                "where":  {"@id": "ex:frank", "ex:budget": "?v"},
                "delete": {"@id": "ex:frank", "ex:budget": "?v"}
            }),
        )
        .await
        .expect("retract frank budget");
    let receipt = fluree
        .insert(
            receipt.ledger,
            &json!({"@context": ctx(), "@id": "ex:frank", "ex:budget": dec("40.25")}),
        )
        .await
        .expect("re-assert frank budget");

    let phase_a: &[(&str, &str, usize)] = &[
        (
            "carol-budget-retracted",
            r"PREFIX ex: <http://example.org/ns/>
              SELECT ?v WHERE { ex:carol ex:knows ?b . ?b ex:budget ?v } ORDER BY ?v",
            0,
        ),
        (
            "eve-budget-reasserted",
            r"PREFIX ex: <http://example.org/ns/>
              SELECT ?v WHERE { ex:eve ex:knows ?b . ?b ex:budget ?v } ORDER BY ?v",
            1,
        ),
        (
            "alice-budget-base",
            r"PREFIX ex: <http://example.org/ns/>
              SELECT ?v WHERE { ex:alice ex:knows ?b . ?b ex:budget ?v } ORDER BY ?v",
            1,
        ),
    ];

    let view = fluree
        .db_at_t(ledger_id, receipt.ledger.t())
        .await
        .expect("phase A view");
    let mut results_a = Vec::new();
    for (name, query, expected_len) in phase_a {
        let (spans, guard) = span_capture::init_test_tracing();
        let rows = run_query(&fluree, &view, query).await;
        drop(guard);
        assert_eq!(rows.len(), *expected_len, "{name}: rows; got {rows:?}");
        // Arena-resolved decimal ops keep the lane live — engagement proves
        // translation succeeded (a single untranslatable op would decline
        // the predicate's lane entirely).
        assert!(
            spans.has_span("join_flush_batched_binary"),
            "{name}: decimal ops should translate and keep the batched lane \
             live; spans: {:?}",
            spans.span_names()
        );
        results_a.push(rows);
    }

    // Novelty phase B: a decimal the index has never seen — no arena handle,
    // so the budget lane declines and the fallback serves (still correct).
    let _receipt = fluree
        .insert(
            receipt.ledger,
            &json!({
                "@context": ctx(),
                "@graph": [
                    {"@id": "ex:alice", "ex:knows": {"@id": "ex:grace"}},
                    {"@id": "ex:grace", "ex:budget": dec("55.55")}
                ]
            }),
        )
        .await
        .expect("novelty-new decimal");

    // Second view fetch after more commits: pin the post-commit t (a cached
    // pre-commit view can otherwise serve).
    let view = fluree
        .db_at_t(ledger_id, _receipt.ledger.t())
        .await
        .expect("phase B view");
    let alice_q = r"PREFIX ex: <http://example.org/ns/>
        SELECT ?v WHERE { ex:alice ex:knows ?b . ?b ex:budget ?v } ORDER BY ?v";
    let rows_b = run_query(&fluree, &view, alice_q).await;
    assert_eq!(rows_b.len(), 2, "phase B: bob + grace; got {rows_b:?}");

    // Ground truth for both phases' final state.
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex ground truth");
    let view = fluree
        .db_at_t(ledger_id, _receipt.ledger.t())
        .await
        .expect("indexed view");
    // Decimal rendering differs between raw-flake-served values (plain
    // string) and arena-decoded ones (`{"@value": …}`) — a pre-existing
    // formatter discrepancy — so compare extracted decimal values rather
    // than exact JSON shapes.
    let indexed_b = run_query(&fluree, &view, alice_q).await;
    assert_eq!(
        decimal_values(&indexed_b),
        decimal_values(&rows_b),
        "phase B != reindexed ground truth"
    );
    for ((name, query, _), novelty_rows) in phase_a.iter().zip(&results_a) {
        // dave/frank state is unchanged by phase B; their queries must match.
        if *name == "alice-budget-base" {
            continue;
        }
        let indexed_rows = run_query(&fluree, &view, query).await;
        assert_eq!(
            decimal_values(&indexed_rows),
            decimal_values(novelty_rows),
            "{name} != reindexed ground truth"
        );
    }
}

/// Extract decimal lexical values from result rows regardless of whether the
/// formatter rendered them as plain strings or `{"@value": …}` objects.
fn decimal_values(rows: &[serde_json::Value]) -> Vec<String> {
    let mut out: Vec<String> = rows
        .iter()
        .flat_map(|row| row.as_array().into_iter().flatten())
        .map(|cell| match cell {
            serde_json::Value::String(s) => s.clone(),
            serde_json::Value::Object(o) => o
                .get("@value")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            other => other.to_string(),
        })
        .collect();
    out.sort();
    out
}
