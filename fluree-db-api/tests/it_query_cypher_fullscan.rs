// Cypher whole-graph-scan opt-in tests. These live in their own test binary
// because `FLUREE_CYPHER_ALLOW_FULL_SCAN` is read once per process — setting
// it here must not leak into the main Cypher tests (which assert the
// default rejection of bare `MATCH (n)`).
#![allow(clippy::needless_raw_string_hashes)]

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::json;
use support::{genesis_ledger, graphdb_from_ledger, rebuild_and_publish_index};

#[tokio::test]
async fn cypher_bare_match_full_scan_opt_in() {
    std::env::set_var("FLUREE_CYPHER_ALLOW_FULL_SCAN", "1");

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/cypher:full-scan");
    let committed = fluree
        .insert(
            ledger0,
            &json!({
                                "@graph": [
                    {"@id": "alice", "@type": "Person", "age": 25},
                    {"@id": "bob",   "@type": "Person"},
                    {"@id": "acme",  "@type": "Company", "age": 99},
                ]
            }),
        )
        .await
        .expect("seed");
    let db = graphdb_from_ledger(&committed.ledger);

    // count(n) counts distinct nodes (subjects), not triples; count(n.age)
    // counts nodes carrying the property (whole-graph count shape).
    let cj = fluree
        .query_cypher(&db, "MATCH (n) RETURN count(n) AS c, count(n.age) AS ca")
        .await
        .expect("bare MATCH scan")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json");
    let row = &cj["results"][0]["data"][0]["row"];
    // 3 user nodes + 2 class nodes (ex:Person / ex:Company are subjects of
    // nothing — they appear only as objects, so they are not counted).
    assert_eq!(row[0], json!(3), "count(n): {cj}");
    assert_eq!(row[1], json!(2), "count(n.age): {cj}");

    // min/max/avg over a property through the scan
    // (whole-graph min/max/avg shape).
    let cj = fluree
        .query_cypher(
            &db,
            "MATCH (n) RETURN min(n.age) AS mn, max(n.age) AS mx, avg(n.age) AS av",
        )
        .await
        .expect("min/max/avg scan")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json");
    let row = &cj["results"][0]["data"][0]["row"];
    assert_eq!(row[0], json!(25), "min: {cj}");
    assert_eq!(row[1], json!(99), "max: {cj}");
    // avg yields xsd:decimal, which cypher-json renders as a string to
    // preserve precision.
    assert_eq!(row[2], json!("62"), "avg: {cj}");
}

/// Run one Cypher query against both views and return the two cypher-json rows.
async fn row_on_both(
    fluree: &fluree_db_api::Fluree,
    novelty_db: &fluree_db_api::GraphDb,
    indexed_db: &fluree_db_api::GraphDb,
    cypher: &str,
) -> (serde_json::Value, serde_json::Value) {
    let mut rows = Vec::new();
    for db in [novelty_db, indexed_db] {
        let cj = fluree
            .query_cypher(db, cypher)
            .await
            .expect("query")
            .to_cypher_json_async(db.as_graph_db_ref())
            .await
            .expect("cypher json");
        rows.push(cj["results"][0]["data"][0]["row"].clone());
    }
    let indexed = rows.pop().unwrap();
    let novelty = rows.pop().unwrap();
    (novelty, indexed)
}

/// Whole-graph aggregates on an indexed ledger take the directory-fold fast
/// path (`fast_whole_graph_agg`); the same queries on the novelty-only view
/// run the general pipeline. Both must agree — including the row-multiplying
/// left-join semantics of a multi-valued property.
#[tokio::test]
async fn cypher_indexed_whole_graph_aggregates_match_pipeline() {
    std::env::set_var("FLUREE_CYPHER_ALLOW_FULL_SCAN", "1");

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/cypher:full-scan-indexed";
    let ledger0 = genesis_ledger(&fluree, ledger_id);
    let committed = fluree
        .insert(
            ledger0,
            &json!({
                                "@graph": [
                    // alice has TWO ages: the accessor left-join gives her two
                    // rows, so count(n) = 5 over 4 subjects while
                    // count(n.age) = 4 values.
                    {"@id": "alice", "@type": "Person", "age": [25, 30]},
                    {"@id": "bob",   "@type": "Person"},
                    {"@id": "carol", "@type": "Person", "age": 40},
                    {"@id": "dave",  "@type": "Person", "age": 40},
                ]
            }),
        )
        .await
        .expect("seed");
    let novelty_db = graphdb_from_ledger(&committed.ledger);

    rebuild_and_publish_index(&fluree, ledger_id).await;
    let indexed_db = fluree.db(ledger_id).await.expect("indexed view");

    // count(n) / count(n.prop) / count(DISTINCT n) — exact integer folds.
    let (novelty, indexed) = row_on_both(
        &fluree,
        &novelty_db,
        &indexed_db,
        "MATCH (n) RETURN count(n) AS c, count(n.age) AS ca, count(DISTINCT n) AS cd",
    )
    .await;
    assert_eq!(novelty, json!([5, 4, 4]), "pipeline row");
    assert_eq!(indexed, json!([5, 4, 4]), "fold row");

    // min/max fold from POST boundary keys; avg from the predicate-scoped
    // scan. avg = (25+30+40+40)/4 = 33.75. The general pipeline renders the
    // decimal as a string while the fold (like the existing SPARQL AVG fast
    // path) yields a double — compare numerically.
    let (novelty, indexed) = row_on_both(
        &fluree,
        &novelty_db,
        &indexed_db,
        "MATCH (n) RETURN min(n.age) AS mn, max(n.age) AS mx, avg(n.age) AS av",
    )
    .await;
    for (label, row) in [("pipeline", &novelty), ("fold", &indexed)] {
        assert_eq!(row[0], json!(25), "{label} min: {row}");
        assert_eq!(row[1], json!(40), "{label} max: {row}");
        let avg = row[2]
            .as_f64()
            .or_else(|| row[2].as_str().and_then(|s| s.parse().ok()))
            .expect("numeric avg");
        assert!((avg - 33.75).abs() < 1e-9, "{label} avg: {row}");
    }

    // sum through the same fold.
    let (novelty, indexed) = row_on_both(
        &fluree,
        &novelty_db,
        &indexed_db,
        "MATCH (n) RETURN sum(n.age) AS s",
    )
    .await;
    assert_eq!(novelty, json!([135]), "pipeline sum");
    assert_eq!(indexed, json!([135]), "fold sum");

    // count(n) with no accessor = distinct subjects.
    let (novelty, indexed) = row_on_both(
        &fluree,
        &novelty_db,
        &indexed_db,
        "MATCH (n) RETURN count(n) AS c",
    )
    .await;
    assert_eq!(novelty, json!([4]), "pipeline count");
    assert_eq!(indexed, json!([4]), "fold count");
}

/// Run a whole-graph aggregate through the overlay fold and again behind a
/// `WITH n` barrier (which blocks the fold, forcing the general pipeline), both
/// on the same view. The two rows must agree.
async fn fold_vs_barrier(
    fluree: &fluree_db_api::Fluree,
    db: &fluree_db_api::GraphDb,
    ret: &str,
) -> (serde_json::Value, serde_json::Value) {
    let fold = format!("MATCH (n) RETURN {ret}");
    let truth = format!("MATCH (n) WITH n RETURN {ret}");
    let mut rows = Vec::new();
    for q in [fold, truth] {
        let cj = fluree
            .query_cypher(db, &q)
            .await
            .expect("query")
            .to_cypher_json_async(db.as_graph_db_ref())
            .await
            .expect("cypher json");
        rows.push(cj["results"][0]["data"][0]["row"].clone());
    }
    let truth = rows.pop().unwrap();
    let fold = rows.pop().unwrap();
    (fold, truth)
}

/// The overlay lane: whole-graph aggregates on an INDEXED ledger that has
/// accumulated post-index novelty (new nodes + a property update) must stay
/// exact. Without the overlay reconciliation these declined to a full-graph
/// flake scan; the fold now folds directory base counts against the novelty.
#[tokio::test]
async fn cypher_whole_graph_aggregates_overlay_matches_pipeline() {
    std::env::set_var("FLUREE_CYPHER_ALLOW_FULL_SCAN", "1");

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/cypher:full-scan-overlay";
    let ledger0 = genesis_ledger(&fluree, ledger_id);
    fluree
        .insert(
            ledger0,
            &json!({
                "@graph": [
                    {"@id": "alice", "@type": "Person", "age": 30},
                    {"@id": "bob",   "@type": "Person", "age": 40},
                    {"@id": "carol", "@type": "Person", "age": 40},
                ]
            }),
        )
        .await
        .expect("seed");
    rebuild_and_publish_index(&fluree, ledger_id).await;

    // Post-index novelty: a brand-new node (with age), and a property update
    // on an existing node (retract 30 + assert 31 — a surviving assertion, so
    // the subject universe is unchanged but avg/distinct shift).
    let indexed = fluree.ledger(ledger_id).await.expect("indexed ledger");
    let after_insert = fluree
        .insert(
            indexed,
            &json!({"@graph": [{"@id": "dave", "@type": "Person", "age": 50}]}),
        )
        .await
        .expect("novelty insert")
        .ledger;
    fluree
        .update(
            after_insert,
            &json!({
                "delete": [{"@id": "alice", "age": 30}],
                "insert": [{"@id": "alice", "age": 31}]
            }),
        )
        .await
        .expect("novelty update");
    let db = fluree.db(ledger_id).await.expect("dirty view");

    // Final state: alice(31), bob(40), carol(40), dave(50) — 4 subjects, 4 ages.
    for ret in [
        "count(n) AS c",
        "count(n.age) AS ca",
        "count(n) AS c, count(n.age) AS ca, count(DISTINCT n) AS cd",
        "count(DISTINCT n.age) AS cda",
        "min(n.age) AS mn, max(n.age) AS mx",
        "sum(n.age) AS s",
    ] {
        let (fold, truth) = fold_vs_barrier(&fluree, &db, ret).await;
        assert_eq!(fold, truth, "overlay fold vs pipeline for `{ret}`");
    }

    // avg renders as a double from the fold and a decimal-string from the
    // pipeline; compare numerically. avg = (31+40+40+50)/4 = 40.25.
    let (fold, truth) = fold_vs_barrier(&fluree, &db, "avg(n.age) AS av").await;
    let num = |v: &serde_json::Value| -> f64 {
        v[0].as_f64()
            .or_else(|| v[0].as_str().and_then(|s| s.parse().ok()))
            .expect("numeric avg")
    };
    assert!((num(&fold) - 40.25).abs() < 1e-9, "fold avg: {fold}");
    assert!((num(&truth) - 40.25).abs() < 1e-9, "pipeline avg: {truth}");

    // Spot-check the absolute values against hand-computed truth.
    let (count, _) = fold_vs_barrier(&fluree, &db, "count(n) AS c, count(n.age) AS ca").await;
    assert_eq!(count, json!([4, 4]), "count(n), count(n.age)");
    let (sum, _) = fold_vs_barrier(&fluree, &db, "sum(n.age) AS s").await;
    assert_eq!(sum, json!([161]), "sum = 31+40+40+50");
    let (dd, _) = fold_vs_barrier(&fluree, &db, "count(DISTINCT n.age) AS d").await;
    assert_eq!(dd, json!([3]), "distinct ages 31/40/50");
}

/// Deleting a node in post-index novelty leaves the subject universe ambiguous
/// to the arithmetic reconciliation (a retraction-only, base-present subject),
/// so the overlay lane declines to the exact pipeline — the count must still be
/// correct.
#[tokio::test]
async fn cypher_whole_graph_count_overlay_deletion_declines_but_correct() {
    std::env::set_var("FLUREE_CYPHER_ALLOW_FULL_SCAN", "1");

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/cypher:full-scan-overlay-del";
    let ledger0 = genesis_ledger(&fluree, ledger_id);
    fluree
        .insert(
            ledger0,
            &json!({
                "@graph": [
                    {"@id": "alice", "@type": "Person", "age": 30},
                    {"@id": "bob",   "@type": "Person", "age": 40},
                    {"@id": "carol", "@type": "Person", "age": 50},
                ]
            }),
        )
        .await
        .expect("seed");
    rebuild_and_publish_index(&fluree, ledger_id).await;

    // Delete carol entirely (retract all her flakes, no assertion).
    let indexed = fluree.ledger(ledger_id).await.expect("indexed ledger");
    fluree
        .update(
            indexed,
            &json!({"delete": [{"@id": "carol", "@type": "Person", "age": 50}]}),
        )
        .await
        .expect("delete node");
    let db = fluree.db(ledger_id).await.expect("dirty view");

    let (fold, truth) = fold_vs_barrier(&fluree, &db, "count(n) AS c").await;
    assert_eq!(
        fold, truth,
        "deletion: fold declines but agrees with pipeline"
    );
    assert_eq!(fold, json!([2]), "alice + bob remain");
}

/// `f:reifies*` facts are hidden from the `?n ?p ?o` pipeline but present in
/// the SPOT directories, so their presence must make the fold decline to the
/// fallback — results stay identical to the novelty view either way.
#[tokio::test]
async fn cypher_indexed_whole_graph_count_declines_on_edge_annotations() {
    std::env::set_var("FLUREE_CYPHER_ALLOW_FULL_SCAN", "1");

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/cypher:full-scan-annotated";
    let ledger0 = genesis_ledger(&fluree, ledger_id);
    let committed = fluree
        .insert(
            ledger0,
            &json!({
                                "@graph": [
                    {
                        "@id": "alice",
                        "@type": "Person",
                        "knows": {
                            "@id": "bob",
                            "@annotation": {"since": 2020}
                        }
                    },
                    {"@id": "bob", "@type": "Person"},
                ]
            }),
        )
        .await
        .expect("seed");
    let novelty_db = graphdb_from_ledger(&committed.ledger);

    rebuild_and_publish_index(&fluree, ledger_id).await;
    let indexed_db = fluree.db(ledger_id).await.expect("indexed view");

    let (novelty, indexed) = row_on_both(
        &fluree,
        &novelty_db,
        &indexed_db,
        "MATCH (n) RETURN count(n) AS c",
    )
    .await;
    assert_eq!(
        novelty, indexed,
        "annotated graph: fold must defer to the pipeline"
    );
}

/// Incrementally-built indexes currently persist delta-only per-graph
/// property stats (base entries lost; net-zero churn kept at count 0). The
/// folds must stay correct anyway: COUNT(DISTINCT ?p) always walks PSOT
/// directories, and the whole-graph fold's hidden-predicate gate checks the
/// predicate dictionary rather than the stats. This drives the corruption
/// trigger — full index, then writes (new predicate, net-zero churn, a
/// reified edge), then an incremental index — and pins fold == pipeline.
#[tokio::test]
async fn folds_stay_correct_on_incremental_index() {
    std::env::set_var("FLUREE_CYPHER_ALLOW_FULL_SCAN", "1");

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/cypher:incremental-stats";
    let ledger0 = genesis_ledger(&fluree, ledger_id);
    let ctx = json!({"ex": ""});
    let ledger1 = fluree
        .insert(
            ledger0,
            &json!({
                "@context": ctx,
                "@graph": [
                    {"@id": "alice", "@type": "Person", "age": 30, "score": 1},
                    {"@id": "bob",   "@type": "Person", "age": 40},
                ]
            }),
        )
        .await
        .expect("seed")
        .ledger;
    rebuild_and_publish_index(&fluree, ledger_id).await;

    // Post-index novelty: a brand-new predicate + class...
    let ledger2 = fluree
        .insert(
            ledger1,
            &json!({
                "@context": ctx,
                "@id": "w1", "@type": "Widget", "brandnew": 7
            }),
        )
        .await
        .expect("new predicate")
        .ledger;
    // ...net-zero churn on ex:score (retract 1, assert 2 — count unchanged)...
    let ledger3 = fluree
        .update(
            ledger2,
            &json!({
                "@context": ctx,
                "delete": [{"@id": "alice", "score": 1}],
                "insert": [{"@id": "alice", "score": 2}]
            }),
        )
        .await
        .expect("churn")
        .ledger;
    // ...and a reified edge (f:reifies* enters the predicate dictionary).
    let _ledger4 = fluree
        .insert(
            ledger3,
            &json!({
                "@context": ctx,
                "@id": "alice",
                "knows": {"@id": "bob", "@annotation": {"since": 2020}}
            }),
        )
        .await
        .expect("annotated edge")
        .ledger;

    // Incremental index (base exists, small commit gap → incremental path).
    support::build_and_publish_index(&fluree, ledger_id).await;
    let db = fluree.db(ledger_id).await.expect("incremental view");

    // The incremental root must carry the BASE per-graph property stats
    // forward, not just the novelty deltas: every property entry sums to the
    // graph's flake total. (The regression dropped base entries and kept
    // net-zero churn at count 0 — the sums diverged immediately.)
    let stats = db.snapshot.stats.as_ref().expect("index stats");
    let g0 = stats
        .graphs
        .as_ref()
        .expect("per-graph stats")
        .iter()
        .find(|g| g.g_id == 0)
        .expect("default graph stats");
    let prop_sum: u64 = g0.properties.iter().map(|p| p.count).sum();
    assert_eq!(
        prop_sum, g0.flakes,
        "per-graph property counts must cover all flakes (delta-only stats?)"
    );
    assert!(
        g0.properties.iter().all(|p| p.count > 0),
        "net-zero churn must keep the base count, not zero: {:?}",
        g0.properties
            .iter()
            .map(|p| (p.p_id, p.count))
            .collect::<Vec<_>>()
    );

    // COUNT(DISTINCT ?p): the fold's PSOT directory walk vs the general
    // pipeline (GROUP BY blocks the distinct-count fold).
    let folded = fluree
        .query(
            &db,
            fluree_db_api::QueryInput::Sparql(
                "SELECT (COUNT(DISTINCT ?p) AS ?c) WHERE { ?s ?p ?o }",
            ),
        )
        .await
        .expect("distinct preds")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    let truth = fluree
        .query(
            &db,
            fluree_db_api::QueryInput::Sparql("SELECT ?p WHERE { ?s ?p ?o } GROUP BY ?p"),
        )
        .await
        .expect("preds truth")
        .row_count();
    assert_eq!(
        folded[0][0].as_i64(),
        Some(truth as i64),
        "COUNT(DISTINCT ?p) fold vs pipeline: {folded} vs {truth}"
    );

    // Whole-graph count: reifies facts are in the dictionary, so the fold
    // must decline (dict-based gate) — and agree with the WITH-blocked
    // pipeline either way.
    let count_fold = fluree
        .query_cypher(&db, "MATCH (n) RETURN count(n) AS c")
        .await
        .expect("count")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cj");
    let count_truth = fluree
        .query_cypher(&db, "MATCH (n) WITH n RETURN count(n) AS c")
        .await
        .expect("count truth")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cj");
    assert_eq!(
        count_fold["results"][0]["data"][0]["row"], count_truth["results"][0]["data"][0]["row"],
        "whole-graph count on incremental+reified store"
    );

    // Class-anchored histogram still folds correctly on the incremental
    // index (class stats are maintained incrementally).
    let hist = fluree
        .query_cypher(&db, "MATCH (n:Person) RETURN n.age, COUNT(*)")
        .await
        .expect("hist")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cj");
    let rows = hist["results"][0]["data"].as_array().expect("rows");
    assert_eq!(rows.len(), 2, "{hist}");
}

/// Companion to `folds_stay_correct_on_incremental_index` that exercises the
/// COUNT(DISTINCT ?p) *stats shortcut* rather than the PSOT scan. The sibling
/// test adds a reified edge, which trips the hidden-predicate gate and makes the
/// operator decline to the pipeline — so it never reaches the predicate arm.
/// Here there is no reifier, so the gate passes and the arm reads
/// `distinct_predicates_from_graph_stats`. It drives the same delta-only
/// corruption trigger (import base, then a new predicate + net-zero churn, then
/// an incremental index) and pins the stats count to the general pipeline: if
/// the incremental build ever drops base property entries again, the shortcut's
/// count diverges from the pipeline and this fails.
#[tokio::test]
async fn distinct_predicate_stats_shortcut_matches_pipeline_after_incremental() {
    std::env::set_var("FLUREE_CYPHER_ALLOW_FULL_SCAN", "1");

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/cypher:incremental-distinct-preds";
    let ledger0 = genesis_ledger(&fluree, ledger_id);
    let ctx = json!({"ex": ""});
    let ledger1 = fluree
        .insert(
            ledger0,
            &json!({
                "@context": ctx,
                "@graph": [
                    {"@id": "alice", "@type": "Person", "age": 30, "score": 1},
                    {"@id": "bob",   "@type": "Person", "age": 40},
                ]
            }),
        )
        .await
        .expect("seed")
        .ledger;
    rebuild_and_publish_index(&fluree, ledger_id).await;

    // Post-index novelty: a brand-new predicate...
    let ledger2 = fluree
        .insert(
            ledger1,
            &json!({
                "@context": ctx,
                "@id": "w1", "@type": "Widget", "brandnew": 7
            }),
        )
        .await
        .expect("new predicate")
        .ledger;
    // ...and net-zero churn on ex:score (retract 1, assert 2 — count unchanged).
    // No reified edge: the hidden-predicate gate stays clear so the fold runs.
    let _ledger3 = fluree
        .update(
            ledger2,
            &json!({
                "@context": ctx,
                "delete": [{"@id": "alice", "score": 1}],
                "insert": [{"@id": "alice", "score": 2}]
            }),
        )
        .await
        .expect("churn")
        .ledger;

    support::build_and_publish_index(&fluree, ledger_id).await;
    let db = fluree.db(ledger_id).await.expect("incremental view");

    // Stats present → the predicate arm takes the in-memory shortcut, not the
    // PSOT-scan fallback. (Fallback would still be correct, but then this test
    // would not be guarding the shortcut.)
    assert!(
        db.snapshot
            .stats
            .as_ref()
            .and_then(|s| s.graphs.as_ref())
            .is_some_and(|g| !g.is_empty()),
        "incremental index must publish per-graph stats for the shortcut"
    );

    let folded = fluree
        .query(
            &db,
            fluree_db_api::QueryInput::Sparql(
                "SELECT (COUNT(DISTINCT ?p) AS ?c) WHERE { ?s ?p ?o }",
            ),
        )
        .await
        .expect("distinct preds")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("jsonld");
    let truth = fluree
        .query(
            &db,
            fluree_db_api::QueryInput::Sparql("SELECT ?p WHERE { ?s ?p ?o } GROUP BY ?p"),
        )
        .await
        .expect("preds truth")
        .row_count();
    // rdf:type, ex:age, ex:score, ex:brandnew.
    assert_eq!(truth, 4, "expected 4 distinct predicates, got {truth}");
    assert_eq!(
        folded[0][0].as_i64(),
        Some(truth as i64),
        "COUNT(DISTINCT ?p) stats shortcut vs pipeline: {folded} vs {truth}"
    );
}
