//! `RangeSemiJoinOperator`: a probe whose object feeds a correlated range
//! FILTER and is otherwise dead is answered as an index-walk semi-join
//! (`fluree-db-query/src/range_semijoin.rs`). The shape is BSBM Explore Q5.
//!
//! Expected rows are computed independently in Rust over the generated data,
//! so the assertions do not depend on the engine agreeing with itself.

#![cfg(feature = "native")]

mod support;

use fluree_db_api::{FlureeBuilder, QueryInput};
use serde_json::{json, Value as JsonValue};
use support::{
    genesis_ledger, normalize_rows, rebuild_and_publish_index, span_capture, MemoryFluree,
};

const LEDGER: &str = "range-semijoin:main";
const ANCHOR_N1: i64 = 100;
const ANCHOR_N2: i64 = 500;

/// One product: shared features, numeric values (a string stands in for a
/// non-numeric value), labels.
struct Product {
    id: String,
    features: Vec<&'static str>,
    n1: Vec<JsonValue>,
    n2: Vec<JsonValue>,
    labels: Vec<String>,
}

fn products() -> Vec<Product> {
    let p = |id: &str,
             features: Vec<&'static str>,
             n1: Vec<JsonValue>,
             n2: Vec<JsonValue>,
             labels: Vec<&str>| Product {
        id: id.to_string(),
        features,
        n1,
        n2,
        labels: labels.into_iter().map(str::to_string).collect(),
    };
    let mut out = vec![
        // in / in
        p(
            "A",
            vec!["f1"],
            vec![json!(150)],
            vec![json!(600)],
            vec!["a"],
        ),
        // n1 out
        p(
            "B",
            vec!["f1"],
            vec![json!(300)],
            vec![json!(600)],
            vec!["b"],
        ),
        // both just inside the strict bounds
        p(
            "C",
            vec!["f2"],
            vec![json!(219)],
            vec![json!(331)],
            vec!["c"],
        ),
        // n1 exactly on the (exclusive) upper bound
        p(
            "D",
            vec!["f2"],
            vec![json!(220)],
            vec![json!(400)],
            vec!["d"],
        ),
        // multi-valued n1: one out, one in
        p(
            "E",
            vec!["f1"],
            vec![json!(-100), json!(190)],
            vec![json!(400)],
            vec!["e"],
        ),
        // no n1 at all
        p("F", vec!["f1"], vec![], vec![json!(400)], vec!["f"]),
        // shares both features (duplicate candidate) and has two labels
        p(
            "G",
            vec!["f1", "f2"],
            vec![json!(120)],
            vec![json!(340)],
            vec!["g1", "g2"],
        ),
        // shares no feature with the anchor
        p(
            "H",
            vec!["f3"],
            vec![json!(100)],
            vec![json!(500)],
            vec!["h"],
        ),
        // non-numeric n1
        p(
            "I",
            vec!["f1"],
            vec![json!("abc")],
            vec![json!(400)],
            vec!["i"],
        ),
        // n2 exactly on the (exclusive) lower bound
        p(
            "J",
            vec!["f2"],
            vec![json!(100)],
            vec![json!(330)],
            vec!["j"],
        ),
        // two in-range n1 values (semi-join keeps the row once)
        p(
            "K",
            vec!["f1"],
            vec![json!(110), json!(130)],
            vec![json!(450)],
            vec!["k"],
        ),
        // a rare feature shared by three products only: the capped-walk case
        p(
            "R",
            vec!["f_rare"],
            vec![json!(100)],
            vec![json!(500)],
            vec!["r"],
        ),
        p(
            "R1",
            vec!["f_rare"],
            vec![json!(7)],
            vec![json!(-40)],
            vec!["r1"],
        ),
        p(
            "R2",
            vec!["f_rare"],
            vec![json!(1999)],
            vec![json!(1999)],
            vec!["r2"],
        ),
    ];
    // Bulk products so the planner's driving-row estimate clears the lane's
    // minimum on the indexed ledger; deterministic LCG values, ~12% / ~17%
    // of them inside each range as in BSBM. Each carries a few features so
    // `feature` averages several flakes per subject and the anchor's single-
    // valued numerics win the seed race ahead of its feature list, as at scale.
    const EXTRA_FEATURES: [&str; 6] = ["f4", "f5", "f6", "f7", "f8", "f9"];
    let mut seed: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut next = || {
        seed = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (seed >> 33) as i64
    };
    for i in 0..1500 {
        let n1 = next() % 2000;
        let n2 = next() % 2000;
        let extra_a = EXTRA_FEATURES[(next() as usize) % EXTRA_FEATURES.len()];
        let mut extra_b = EXTRA_FEATURES[(next() as usize) % EXTRA_FEATURES.len()];
        if extra_b == extra_a {
            extra_b = EXTRA_FEATURES[(EXTRA_FEATURES.iter().position(|f| *f == extra_a).unwrap()
                + 1)
                % EXTRA_FEATURES.len()];
        }
        out.push(p(
            &format!("bulk{i}"),
            vec!["f1", extra_a, extra_b],
            vec![json!(n1)],
            vec![json!(n2)],
            vec![],
        ));
        out.last_mut().unwrap().labels = vec![format!("bulk-{i:04}")];
    }
    out
}

/// A product's `ex:d` date, derived from its first integer `n1` so the
/// expected rows need no second fixture: months and days spread over 2024.
fn date_of(p: &Product) -> Option<String> {
    let n = p.n1.iter().find_map(JsonValue::as_i64)?;
    Some(format!(
        "2024-{:02}-{:02}T00:00:00Z",
        1 + n.rem_euclid(12),
        1 + (n / 12).rem_euclid(28)
    ))
}

const DATE_LO: &str = "2024-03-15T00:00:00Z";
const DATE_HI: &str = "2024-09-15T00:00:00Z";

/// (label) rows the date-window query must return, in label order.
fn expected_date_labels(products: &[Product]) -> Vec<String> {
    let anchor_features = ["f1", "f2"];
    let mut labels: Vec<String> = products
        .iter()
        .filter(|p| p.features.iter().any(|f| anchor_features.contains(f)))
        .filter(|p| date_of(p).is_some_and(|d| d.as_str() > DATE_LO && d.as_str() < DATE_HI))
        .flat_map(|p| p.labels.iter().cloned())
        .collect();
    labels.sort();
    labels
}

fn in_range(values: &[JsonValue], center: i64, half: i64) -> bool {
    values.iter().any(|v| {
        v.as_i64()
            .is_some_and(|n| n < center + half && n > center - half)
    })
}

/// (label) rows the DISTINCT query must return, in label order.
fn expected_labels(products: &[Product]) -> Vec<String> {
    let anchor_features = ["f1", "f2"];
    let mut labels: Vec<String> = products
        .iter()
        .filter(|p| p.features.iter().any(|f| anchor_features.contains(f)))
        .filter(|p| in_range(&p.n1, ANCHOR_N1, 120) && in_range(&p.n2, ANCHOR_N2, 170))
        .flat_map(|p| p.labels.iter().cloned())
        .collect();
    labels.sort();
    labels
}

fn graph(products: &[Product]) -> JsonValue {
    let mut nodes = vec![json!({
        "@id": "ex:P",
        "ex:feature": [{"@id": "ex:f1"}, {"@id": "ex:f2"}],
        "ex:n1": ANCHOR_N1,
        "ex:n2": ANCHOR_N2,
        "ex:dlo": {"@value": DATE_LO, "@type": "http://www.w3.org/2001/XMLSchema#dateTime"},
        "ex:dhi": {"@value": DATE_HI, "@type": "http://www.w3.org/2001/XMLSchema#dateTime"},
        "ex:label": "anchor"
    })];
    for p in products {
        let mut node = serde_json::Map::new();
        node.insert("@id".into(), json!(format!("ex:{}", p.id)));
        node.insert(
            "ex:feature".into(),
            JsonValue::Array(
                p.features
                    .iter()
                    .map(|f| json!({"@id": format!("ex:{f}")}))
                    .collect(),
            ),
        );
        if !p.n1.is_empty() {
            node.insert("ex:n1".into(), JsonValue::Array(p.n1.clone()));
        }
        if !p.n2.is_empty() {
            node.insert("ex:n2".into(), JsonValue::Array(p.n2.clone()));
        }
        if let Some(d) = date_of(p) {
            node.insert(
                "ex:d".into(),
                json!({"@value": d, "@type": "http://www.w3.org/2001/XMLSchema#dateTime"}),
            );
        }
        node.insert(
            "ex:label".into(),
            JsonValue::Array(p.labels.iter().map(|l| json!(l)).collect()),
        );
        nodes.push(JsonValue::Object(node));
    }
    json!({"@context": {"ex": "http://example.org/ns/"}, "@graph": nodes})
}

const Q5_SHAPE: &str = "PREFIX ex: <http://example.org/ns/>\n\
    SELECT DISTINCT ?product ?label WHERE {\n\
      ?product ex:label ?label .\n\
      FILTER (ex:P != ?product)\n\
      ex:P ex:feature ?f .\n\
      ?product ex:feature ?f .\n\
      ex:P ex:n1 ?o1 .\n\
      ?product ex:n1 ?s1 .\n\
      FILTER (?s1 < (?o1 + 120) && ?s1 > (?o1 - 120))\n\
      ex:P ex:n2 ?o2 .\n\
      ?product ex:n2 ?s2 .\n\
      FILTER (?s2 < (?o2 + 170) && ?s2 > (?o2 - 170))\n\
    } ORDER BY ?label";

/// Same body, but `?s1` is projected: the value is live, so the probe must
/// stay a real join (one row per in-range value) and no fold may apply.
const S1_LIVE: &str = "PREFIX ex: <http://example.org/ns/>\n\
    SELECT DISTINCT ?label ?s1 WHERE {\n\
      ?product ex:label ?label .\n\
      FILTER (ex:P != ?product)\n\
      ex:P ex:feature ?f .\n\
      ?product ex:feature ?f .\n\
      ex:P ex:n1 ?o1 .\n\
      ?product ex:n1 ?s1 .\n\
      FILTER (?s1 < (?o1 + 120) && ?s1 > (?o1 - 120))\n\
      ex:P ex:n2 ?o2 .\n\
      ?product ex:n2 ?s2 .\n\
      FILTER (?s2 < (?o2 + 170) && ?s2 > (?o2 - 170))\n\
      FILTER (?label = \"k\" || ?label = \"e\")\n\
    } ORDER BY ?label ?s1";

/// Q5 shape anchored on the rare-feature product with a range wide enough
/// to cover every value: three driving rows against a whole-predicate
/// envelope, so the walk must cap out and the batch must be answered by probes.
const WIDE_RANGE_RARE_ANCHOR: &str = "PREFIX ex: <http://example.org/ns/>\n\
    SELECT DISTINCT ?product ?label WHERE {\n\
      ?product ex:label ?label .\n\
      FILTER (ex:R != ?product)\n\
      ex:R ex:feature ?f .\n\
      ?product ex:feature ?f .\n\
      ex:R ex:n1 ?o1 .\n\
      ?product ex:n1 ?s1 .\n\
      FILTER (?s1 < (?o1 + 100000) && ?s1 > (?o1 - 100000))\n\
      ex:R ex:n2 ?o2 .\n\
      ?product ex:n2 ?s2 .\n\
      FILTER (?s2 < (?o2 + 100000) && ?s2 > (?o2 - 100000))\n\
    } ORDER BY ?label";

/// Q5 shape over a date window whose bounds come from the anchor (a constant
/// bound would be pushed into the probe instead of folded): the fold applies
/// but the walk keys inline numerics only, so every batch must be answered by
/// probes with the filter re-evaluated on the decoded dates.
const DATE_WINDOW: &str = "PREFIX ex: <http://example.org/ns/>\n\
    SELECT DISTINCT ?product ?label WHERE {\n\
      ?product ex:label ?label .\n\
      FILTER (ex:P != ?product)\n\
      ex:P ex:feature ?f .\n\
      ?product ex:feature ?f .\n\
      ex:P ex:dlo ?lo .\n\
      ex:P ex:dhi ?hi .\n\
      ?product ex:d ?sd .\n\
      FILTER (?sd > ?lo && ?sd < ?hi)\n\
    } ORDER BY ?label";

/// Routing stamps for the semi-join site since `before`, as outcome labels.
fn semijoin_outcomes(store: &span_capture::SpanStore, before: usize) -> Vec<String> {
    store.find_events("fast-path outcome")[before..]
        .iter()
        .filter(|e| e.fields.get("site").map(String::as_str) == Some("range-semijoin"))
        .filter_map(|e| e.fields.get("outcome").cloned())
        .collect()
}

async fn rows_of(
    fluree: &MemoryFluree,
    view: &fluree_db_api::GraphDb,
    sparql: &str,
) -> Vec<JsonValue> {
    let res = fluree
        .query(view, QueryInput::Sparql(sparql))
        .await
        .expect("sparql query");
    normalize_rows(&res.to_jsonld(&view.snapshot).expect("to_jsonld"))
}

/// The label column of the Q5-shaped query's rows, sorted.
async fn labels_of(
    fluree: &MemoryFluree,
    view: &fluree_db_api::GraphDb,
    sparql: &str,
) -> Vec<String> {
    let mut labels: Vec<String> = rows_of(fluree, view, sparql)
        .await
        .iter()
        .map(|row| {
            row.as_array()
                .and_then(|r| r.last())
                .and_then(JsonValue::as_str)
                .map(str::to_string)
                .unwrap_or_else(|| row.to_string())
        })
        .collect();
    labels.sort();
    labels
}

fn ops(node: &JsonValue, out: &mut Vec<String>) {
    if let Some(op) = node["op"].as_str() {
        out.push(match node["details"]["right"].as_str() {
            Some(right) => format!("{op} {right}"),
            None => op.to_string(),
        });
    }
    if let Some(children) = node["children"].as_array() {
        for c in children {
            ops(&c["node"], out);
        }
    }
}

#[tokio::test]
async fn correlated_range_probes_fold_into_a_semijoin_with_exact_rows() {
    // This is the only test in the binary, so the process-wide walk floor is
    // ours to set before the first walk reads it: a tiny floor makes the
    // wide-range case cap out (three driving rows against ~1500 in-range
    // rows) while the Q5 shape stays within 16 rows per driving row.
    std::env::set_var("FLUREE_RANGE_SEMIJOIN_WALK_FLOOR", "100");
    let (events, _tracing_guard) = span_capture::init_test_tracing();
    let products = products();
    let expected = expected_labels(&products);
    assert!(
        expected.iter().any(|l| l == "e") && expected.iter().any(|l| l == "g2"),
        "fixture sanity: multi-valued and duplicate-candidate cases are in range"
    );

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, LEDGER);
    let ledger = fluree
        .insert(ledger0, &graph(&products))
        .await
        .expect("insert")
        .ledger;

    // Pure novelty (no statistics): the fold fires on the estimate-free gate.
    let novelty_view = support::graphdb_from_ledger(&ledger);
    assert_eq!(labels_of(&fluree, &novelty_view, Q5_SHAPE).await, expected);

    // Binary index: EncodedSid keys, the POST walk, planner statistics.
    rebuild_and_publish_index(&fluree, LEDGER).await;
    let view = fluree.db(LEDGER).await.expect("indexed view");
    let before = events.find_events("fast-path outcome").len();
    assert_eq!(labels_of(&fluree, &view, Q5_SHAPE).await, expected);
    let outcomes = semijoin_outcomes(&events, before);
    assert!(
        !outcomes.is_empty() && outcomes.iter().all(|o| o == "proceed"),
        "every batch of the Q5 shape is answered from a built index: {outcomes:?}"
    );

    // Wide range on a rare anchor: the walk caps out and the batch is
    // answered by subject probes, with exactly the same rows.
    let before = events.find_events("fast-path outcome").len();
    let expected_rare = {
        let mut labels: Vec<String> = products
            .iter()
            .filter(|p| p.features.contains(&"f_rare") && p.id != "R")
            .flat_map(|p| p.labels.iter().cloned())
            .collect();
        labels.sort();
        labels
    };
    assert_eq!(
        labels_of(&fluree, &view, WIDE_RANGE_RARE_ANCHOR).await,
        expected_rare,
        "capped walk falls back to probes with exact rows"
    );
    let outcomes = semijoin_outcomes(&events, before);
    assert!(
        outcomes.iter().any(|o| o == "fallback:gate_declined"),
        "the wide range must cap the walk and take the probe fallback: {outcomes:?}"
    );

    // Date window: non-numeric bounds never build a walked index (which
    // holds inline numerics only); the batch takes the probe path and the
    // filter is re-evaluated on the decoded dates.
    let before = events.find_events("fast-path outcome").len();
    let expected_dates = expected_date_labels(&products);
    assert!(
        expected_dates.len() > 100 && expected_dates.len() < products.len(),
        "fixture sanity: the date window keeps some but not all products"
    );
    assert_eq!(
        labels_of(&fluree, &view, DATE_WINDOW).await,
        expected_dates,
        "date bounds are answered by probes with exact rows"
    );
    let outcomes = semijoin_outcomes(&events, before);
    assert!(
        !outcomes.is_empty() && outcomes.iter().all(|o| o == "fallback:gate_declined"),
        "non-numeric bounds must take the probe fallback on every batch: {outcomes:?}"
    );
    let plan = fluree
        .explain_sparql(&view, DATE_WINDOW)
        .await
        .expect("explain");
    let mut names = Vec::new();
    ops(&plan["plan"]["physical"], &mut names);
    assert!(
        names.iter().any(|n| n == "RangeSemiJoinOperator"),
        "the date probe folds like a numeric one: {names:?}"
    );

    let plan = fluree
        .explain_sparql(&view, Q5_SHAPE)
        .await
        .expect("explain");
    let mut names = Vec::new();
    ops(&plan["plan"]["physical"], &mut names);
    assert!(
        names.iter().any(|n| n == "RangeSemiJoinOperator"),
        "both numeric probes fold into one RangeSemiJoinOperator: {names:?}"
    );
    assert!(
        !names
            .iter()
            .any(|n| n.contains("ex:n1>") || n.contains("ns/n1>") || n.contains(":n1> ?")),
        "the folded probes must not remain as joins: {names:?}"
    );

    // Novelty on top of the index: a product that exists only in the overlay
    // (subject id minted after the index) must be found by the overlay-merged
    // probes the walk declines to, keyed consistently with the driving rows.
    let ledger2 = fluree
        .insert(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [{
                    "@id": "ex:N",
                    "ex:feature": [{"@id": "ex:f1"}],
                    "ex:n1": 105,
                    "ex:n2": 505,
                    "ex:label": "n"
                }]
            }),
        )
        .await
        .expect("overlay insert")
        .ledger;
    assert!(ledger2.t() > 1, "overlay commit landed");
    let view2 = fluree.db(LEDGER).await.expect("view with novelty");
    let mut expected_with_novelty = expected.clone();
    expected_with_novelty.push("n".to_string());
    expected_with_novelty.sort();
    assert_eq!(
        labels_of(&fluree, &view2, Q5_SHAPE).await,
        expected_with_novelty,
        "novelty-only product with in-range values is kept"
    );

    // A live value var keeps the real join for THAT probe (the still-dead n2
    // probe may fold on its own): K has two in-range values and yields two
    // rows, E's out-of-range value yields nothing.
    let live = rows_of(&fluree, &view, S1_LIVE).await;
    assert_eq!(
        live,
        normalize_rows(&json!([["e", 190], ["k", 110], ["k", 130]])),
        "one row per in-range value: {live:?}"
    );
    let plan = fluree
        .explain_sparql(&view, S1_LIVE)
        .await
        .expect("explain");
    let mut names = Vec::new();
    ops(&plan["plan"]["physical"], &mut names);
    assert!(
        names
            .iter()
            .any(|n| n.starts_with("NestedLoopJoinOperator") && n.contains(":n1> ?")),
        "the probe whose value is projected must stay a join: {names:?}"
    );
}
