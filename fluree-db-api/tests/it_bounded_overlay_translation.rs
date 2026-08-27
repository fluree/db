//! Correctness cover for the bound-term overlay translation (fluree/db#1722).
//!
//! Opening a bound-subject (or bound-predicate) scan no longer translates the
//! whole graph's novelty before applying the bound term — it seeks a
//! subject-/predicate-bracketed window instead. The window is built BEFORE
//! lifecycle resolution, so the hazard is a window that is too narrow: a novelty
//! retraction left out of it would stop suppressing an indexed base row and the
//! scan would emit a stale value.
//!
//! Every test here therefore pins RESULTS across the index/novelty boundary
//! rather than timings — reindexing so a subject lives in the persisted
//! dictionary (the path that takes the bounded translation; a novelty-only
//! subject diverts to `open_overlay_only_fallback` and always did seek), then
//! mutating it so its live state is a base row plus a novelty retraction.

#![cfg(feature = "native")]

use fluree_db_api::{Fluree, FlureeBuilder, QueryInput, ReindexOptions};
use serde_json::{json, Value};

const LEDGER: &str = "bounded-overlay:main";

fn ctx() -> Value {
    json!({
        "ex": "http://example.org/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    })
}

async fn new_fluree() -> (Fluree, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .build()
        .expect("build");
    (fluree, dir)
}

/// `n` subjects, each with a colour, a rank and a shared tag — enough breadth
/// that a whole-novelty walk and a one-subject window differ observably.
fn seed_graph(n: usize) -> Value {
    let nodes: Vec<Value> = (0..n)
        .map(|i| {
            json!({
                "@id": format!("ex:item{i}"),
                "@type": "ex:Item",
                "ex:colour": format!("colour{i}"),
                "ex:rank": i as i64,
                "ex:tag": "shared"
            })
        })
        .collect();
    json!({ "@context": ctx(), "@graph": nodes })
}

/// All (predicate, object) pairs bound for `subject`, as sorted `p=o` strings.
async fn props(fluree: &Fluree, ledger_id: &str, subject: &str) -> Vec<String> {
    let view = fluree.db(ledger_id).await.expect("db view");
    let q = json!({
        "@context": ctx(),
        "select": ["?p", "?o"],
        "where": {"@id": subject, "?p": "?o"}
    });
    let out = fluree
        .query(&view, QueryInput::JsonLd(&q))
        .await
        .expect("query");
    rows_to_pairs(&out.to_jsonld(&view.snapshot).expect("jsonld"))
}

fn rows_to_pairs(jsonld: &Value) -> Vec<String> {
    let mut v: Vec<String> = jsonld
        .as_array()
        .map(|rows| {
            rows.iter()
                .map(|r| match r {
                    Value::Array(cols) if cols.len() == 2 => {
                        format!("{}={}", cols[0], cols[1])
                    }
                    other => other.to_string(),
                })
                .collect()
        })
        .unwrap_or_default();
    v.sort();
    v
}

/// Base rows + a novelty retraction for the SAME subject: the exact shape the
/// bounded window must not truncate. A too-narrow window would leave the
/// retraction out and the retracted value would still be returned.
#[tokio::test]
async fn retraction_in_novelty_suppresses_indexed_base_row() {
    let (fluree, _dir) = new_fluree().await;
    let mut ledger = fluree.create_ledger(LEDGER).await.expect("create");

    ledger = fluree
        .insert(ledger, &seed_graph(60))
        .await
        .expect("seed")
        .ledger;
    fluree
        .reindex(LEDGER, ReindexOptions::default())
        .await
        .expect("reindex");

    // Everything above is now in the persisted index. Retract ONE property of
    // ONE subject; that retraction lives only in novelty.
    let update = json!({
        "@context": ctx(),
        "where":  {"@id": "ex:item7", "ex:colour": "?c"},
        "delete": {"@id": "ex:item7", "ex:colour": "?c"}
    });
    fluree.update(ledger, &update).await.expect("retract");

    let after = props(&fluree, LEDGER, "ex:item7").await;
    assert!(
        !after.iter().any(|s| s.contains("colour7")),
        "retracted value still visible — the novelty retraction was not applied \
         to the base row: {after:?}"
    );
    assert!(
        after.iter().any(|s| s.contains("shared")),
        "unretracted properties of the same subject disappeared: {after:?}"
    );

    // A neighbouring subject must be entirely unaffected.
    let neighbour = props(&fluree, LEDGER, "ex:item8").await;
    assert!(
        neighbour.iter().any(|s| s.contains("colour8")),
        "neighbouring subject lost a property: {neighbour:?}"
    );
}

/// Retract-then-reassert within novelty, on top of an indexed base row: the
/// window must carry the whole lifecycle so the newest op wins.
#[tokio::test]
async fn assert_retract_reassert_lifecycle_resolves_to_newest() {
    let (fluree, _dir) = new_fluree().await;
    let mut ledger = fluree.create_ledger(LEDGER).await.expect("create");

    ledger = fluree
        .insert(ledger, &seed_graph(40))
        .await
        .expect("seed")
        .ledger;
    fluree
        .reindex(LEDGER, ReindexOptions::default())
        .await
        .expect("reindex");

    for value in ["v1", "v2", "v3"] {
        let update = json!({
            "@context": ctx(),
            "where":  {"@id": "ex:item3", "ex:colour": "?c"},
            "delete": {"@id": "ex:item3", "ex:colour": "?c"},
            "insert": {"@id": "ex:item3", "ex:colour": value}
        });
        ledger = fluree.update(ledger, &update).await.expect("update").ledger;
    }

    let after = props(&fluree, LEDGER, "ex:item3").await;
    let colours: Vec<&String> = after.iter().filter(|s| s.contains("colour")).collect();
    assert_eq!(
        colours.len(),
        1,
        "exactly one colour must survive the lifecycle, got {colours:?}"
    );
    assert!(
        colours[0].contains("v3"),
        "newest assertion must win, got {colours:?}"
    );
}

/// A subject that exists ONLY in novelty (never indexed) must still return its
/// properties — this is the `open_overlay_only_fallback` lane, which the
/// bounded path must not have disturbed.
#[tokio::test]
async fn novelty_only_subject_is_fully_visible() {
    let (fluree, _dir) = new_fluree().await;
    let mut ledger = fluree.create_ledger(LEDGER).await.expect("create");

    ledger = fluree
        .insert(ledger, &seed_graph(30))
        .await
        .expect("seed")
        .ledger;
    fluree
        .reindex(LEDGER, ReindexOptions::default())
        .await
        .expect("reindex");

    let fresh = json!({
        "@context": ctx(),
        "@id": "ex:brand-new",
        "@type": "ex:Item",
        "ex:colour": "novel",
        "ex:rank": 999
    });
    fluree.insert(ledger, &fresh).await.expect("insert fresh");

    let after = props(&fluree, LEDGER, "ex:brand-new").await;
    assert!(
        after.iter().any(|s| s.contains("novel")),
        "novelty-only subject lost its properties: {after:?}"
    );
    assert!(
        after.iter().any(|s| s.contains("999")),
        "novelty-only subject lost a property: {after:?}"
    );
}

/// A bound-PREDICATE scan (the PSOT bracket) across the index/novelty boundary:
/// the predicate window must hold every subject's ops for that predicate.
#[tokio::test]
async fn bound_predicate_scan_spans_index_and_novelty() {
    let (fluree, _dir) = new_fluree().await;
    let mut ledger = fluree.create_ledger(LEDGER).await.expect("create");

    ledger = fluree
        .insert(ledger, &seed_graph(25))
        .await
        .expect("seed")
        .ledger;
    fluree
        .reindex(LEDGER, ReindexOptions::default())
        .await
        .expect("reindex");

    // One retraction and one fresh assertion under the SAME predicate, both in
    // novelty, over an indexed base.
    let update = json!({
        "@context": ctx(),
        "where":  {"@id": "ex:item2", "ex:tag": "?t"},
        "delete": {"@id": "ex:item2", "ex:tag": "?t"}
    });
    let ledger = fluree
        .update(ledger, &update)
        .await
        .expect("retract")
        .ledger;
    let add = json!({
        "@context": ctx(),
        "@id": "ex:item99",
        "ex:tag": "shared"
    });
    fluree.insert(ledger, &add).await.expect("add");

    let view = fluree.db(LEDGER).await.expect("view");
    let q = json!({
        "@context": ctx(),
        "select": ["?s"],
        "where": {"@id": "?s", "ex:tag": "shared"}
    });
    let out = fluree
        .query(&view, QueryInput::JsonLd(&q))
        .await
        .expect("query");
    let rows = rows_to_pairs(&out.to_jsonld(&view.snapshot).expect("jsonld"));

    assert!(
        !rows.iter().any(|s| s.contains("item2\"")),
        "retracted subject still matched the bound-predicate scan: {rows:?}"
    );
    assert!(
        rows.iter().any(|s| s.contains("item99")),
        "novelty-only subject missing from the bound-predicate scan: {rows:?}"
    );
    assert!(
        rows.iter().any(|s| s.contains("item1\"")),
        "indexed subject missing from the bound-predicate scan: {rows:?}"
    );
}

/// Named graphs: the window is built per `g_id`, so a retraction in one graph
/// must not leak across to the same subject IRI in another.
#[tokio::test]
async fn named_graphs_stay_isolated() {
    const G1: &str = "http://example.org/g1";
    const G2: &str = "http://example.org/g2";

    let (fluree, _dir) = new_fluree().await;
    let ledger = fluree.create_ledger(LEDGER).await.expect("create");

    // `["graph", <iri>, {..}]` template sugar is what actually routes triples
    // into a NAMED graph; a top-level "graph" key on a plain insert does not.
    let seed = json!({
        "@context": ctx(),
        "insert": [
            ["graph", G1, {"@id": "ex:shared-subject", "ex:colour": "original", "ex:tag": "keep"}],
            ["graph", G2, {"@id": "ex:shared-subject", "ex:colour": "original", "ex:tag": "keep"}]
        ]
    });
    let ledger = fluree
        .update(ledger, &seed)
        .await
        .expect("seed graphs")
        .ledger;
    fluree
        .reindex(LEDGER, ReindexOptions::default())
        .await
        .expect("reindex");

    // Retract the colour in G1 only. `from` scopes the WHERE to G1 so the
    // matching binding comes from that graph alone.
    let update = json!({
        "@context": ctx(),
        "from": G1,
        "where": {"@id": "ex:shared-subject", "ex:colour": "?c"},
        "delete": [["graph", G1, {"@id": "ex:shared-subject", "ex:colour": "?c"}]]
    });
    fluree.update(ledger, &update).await.expect("retract in g1");

    let read = |g: &'static str| {
        let fluree = &fluree;
        async move {
            let q = json!({
                "@context": ctx(),
                "from": format!("{LEDGER}#{g}"),
                "select": ["?p", "?o"],
                "where": {"@id": "ex:shared-subject", "?p": "?o"}
            });
            let out = fluree.query_connection(&q).await.expect("graph query");
            let view = fluree.ledger(LEDGER).await.expect("ledger");
            rows_to_pairs(&out.to_jsonld(&view.snapshot).expect("jsonld"))
        }
    };

    let g1 = read(G1).await;
    let g2 = read(G2).await;

    assert!(
        !g1.iter().any(|s| s.contains("original")),
        "g1 retraction did not apply: {g1:?}"
    );
    assert!(
        g1.iter().any(|s| s.contains("keep")),
        "g1 lost an unrelated property: {g1:?}"
    );
    assert!(
        g2.iter().any(|s| s.contains("original")),
        "g1 retraction leaked into g2: {g2:?}"
    );
}

/// Time travel: a query pinned to `t` before the retraction must still see the
/// retracted value. The window is built under the query's `to_t`, so an
/// off-by-one there would surface here.
#[tokio::test]
async fn time_travel_sees_pre_retraction_state() {
    let (fluree, _dir) = new_fluree().await;
    let mut ledger = fluree.create_ledger(LEDGER).await.expect("create");

    ledger = fluree
        .insert(ledger, &seed_graph(20))
        .await
        .expect("seed")
        .ledger;
    fluree
        .reindex(LEDGER, ReindexOptions::default())
        .await
        .expect("reindex");

    let view_before = fluree.db(LEDGER).await.expect("view");
    let t_before = view_before.t;

    let update = json!({
        "@context": ctx(),
        "where":  {"@id": "ex:item5", "ex:colour": "?c"},
        "delete": {"@id": "ex:item5", "ex:colour": "?c"}
    });
    fluree.update(ledger, &update).await.expect("retract");

    // A `from: {"t": N}` pin is resolved by the CONNECTION-level query; passing
    // a concrete head view to `query()` would silently read at the view's own t.
    let at_t = |t: i64| {
        let fluree = &fluree;
        async move {
            let q = json!({
                "@context": ctx(),
                "from": {"@id": LEDGER, "t": t},
                "select": ["?p", "?o"],
                "where": {"@id": "ex:item5", "?p": "?o"}
            });
            let out = fluree
                .query_connection(&q)
                .await
                .expect("time-travel query");
            let view = fluree.ledger(LEDGER).await.expect("ledger");
            rows_to_pairs(&out.to_jsonld(&view.snapshot).expect("jsonld"))
        }
    };

    let before = at_t(t_before).await;
    assert!(
        before.iter().any(|s| s.contains("colour5")),
        "time travel to t={t_before} lost the pre-retraction value: {before:?}"
    );

    let now = props(&fluree, LEDGER, "ex:item5").await;
    assert!(
        !now.iter().any(|s| s.contains("colour5")),
        "value still visible at head after retraction: {now:?}"
    );
}

/// Many retractions across many subjects, then a full read-back of every
/// subject: catches a window whose bracket is subtly off for some subjects but
/// not others (the failure a single-subject test would miss).
#[tokio::test]
async fn every_subject_reads_back_correctly_after_scattered_retractions() {
    let (fluree, _dir) = new_fluree().await;
    let mut ledger = fluree.create_ledger(LEDGER).await.expect("create");

    const N: usize = 24;
    ledger = fluree
        .insert(ledger, &seed_graph(N))
        .await
        .expect("seed")
        .ledger;
    fluree
        .reindex(LEDGER, ReindexOptions::default())
        .await
        .expect("reindex");

    // Retract `ex:colour` from every third subject.
    for i in (0..N).step_by(3) {
        let update = json!({
            "@context": ctx(),
            "where":  {"@id": format!("ex:item{i}"), "ex:colour": "?c"},
            "delete": {"@id": format!("ex:item{i}"), "ex:colour": "?c"}
        });
        ledger = fluree
            .update(ledger, &update)
            .await
            .expect("retract")
            .ledger;
    }

    for i in 0..N {
        let p = props(&fluree, LEDGER, &format!("ex:item{i}")).await;
        let has_colour = p.iter().any(|s| s.contains(&format!("colour{i}")));
        if i % 3 == 0 {
            assert!(
                !has_colour,
                "item{i}: retracted colour still visible: {p:?}"
            );
        } else {
            assert!(has_colour, "item{i}: untouched colour went missing: {p:?}");
        }
        assert!(
            p.iter().any(|s| s.contains("shared")),
            "item{i}: unrelated property lost: {p:?}"
        );
    }
}
