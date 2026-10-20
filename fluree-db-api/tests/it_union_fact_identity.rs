//! A default graph of several graphs deduplicates on fact identity (AJ-66):
//! storage's identity, one fact per value and list position (#2109's
//! `FactIdentity`). Only a fact two members both hold collapses, so a union
//! with a graph that adds nothing answers as the graph alone.
//!
//! The cases are the probe table of round 6's lane C report ("Union dedup
//! identity"), where the dedup keyed on the emitted row and lost the term a
//! constant object matched and the list position. Each runs before the
//! ledger is indexed (the scan reads decoded flakes) and after (it reads
//! index rows), through SPARQL, with a JSON-LD twin.

use fluree_db_api::{Fluree, FlureeBuilder, GraphDb, ReindexOptions};
use serde_json::{json, Value as JsonValue};

const LEDGER: &str = "it/union-fact-identity:main";

/// g1: a bare number under two datatypes for one subject, and a list that
/// repeats a value. gEmpty: nothing `ex:p` or `ex:q` can match. g2: the
/// list's first item and one of g1's numbers, the facts g1 holds too.
const TRIG: &str = r#"
@prefix ex: <http://example.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
GRAPH <urn:g1> {
  ex:a ex:p 1 , "1.0e0"^^xsd:double .
  ex:b ex:p 1 .
  ex:d ex:q ( 5 5 7 ) .
}
GRAPH <urn:gEmpty> { ex:z ex:unrelated "x" . }
GRAPH <urn:g2> {
  ex:a ex:p 1 .
  ex:d ex:q ( 5 ) .
}
"#;

const G1: &str = "FROM <urn:g1>";
const G1_EMPTY: &str = "FROM <urn:g1> FROM <urn:gEmpty>";
const G1_G2: &str = "FROM <urn:g1> FROM <urn:g2>";

/// The query's rows, each `var=value^^datatype` joined by `;`, sorted.
async fn sparql_rows(fluree: &Fluree, body: &str, dataset: &str) -> Vec<String> {
    let handle = fluree.ledger_cached(LEDGER).await.expect("ledger");
    let view = GraphDb::from_ledger_state(&handle.snapshot().await.to_ledger_state());
    let query = format!(
        "PREFIX ex: <http://example.org/>\n{}",
        body.replace("{ds}", dataset)
    );
    let json = view
        .query(fluree)
        .sparql(&query)
        .execute_formatted()
        .await
        .unwrap_or_else(|e| panic!("{query}: {e}"));
    let mut rows: Vec<String> = json["results"]["bindings"]
        .as_array()
        .unwrap_or_else(|| panic!("{query}: {json}"))
        .iter()
        .map(|b| {
            let mut cols: Vec<String> = b
                .as_object()
                .expect("binding object")
                .iter()
                .map(|(var, v)| {
                    let value = v["value"].as_str().unwrap_or_default();
                    let value = value.rsplit('/').next().unwrap_or(value);
                    match v["datatype"].as_str() {
                        Some(dt) => format!("{var}={value}^^{}", dt.rsplit('#').next().unwrap()),
                        None => format!("{var}={value}"),
                    }
                })
                .collect();
            cols.sort();
            cols.join(";")
        })
        .collect();
    rows.sort();
    rows
}

fn want(rows: &[&str]) -> Vec<String> {
    let mut rows: Vec<String> = rows.iter().map(|r| (*r).to_string()).collect();
    rows.sort();
    rows
}

/// Every case of the probe table, plus the facts two members both hold.
async fn check_sparql(fluree: &Fluree, state: &str) {
    let int = "integer";
    let one_a = format!("o=1^^{int};s=a");
    let one_b = format!("o=1^^{int};s=b");
    let cases: Vec<(&str, &str, Vec<String>)> = vec![
        // A bare number matches 1 and 1.0e0: two facts of a.
        (
            "SELECT ?s {ds} WHERE { ?s ex:p 1 }",
            G1,
            want(&["s=a", "s=a", "s=b"]),
        ),
        (
            "SELECT ?s {ds} WHERE { ?s ex:p 1 }",
            G1_EMPTY,
            want(&["s=a", "s=a", "s=b"]),
        ),
        // g2's `a p 1` is g1's.
        (
            "SELECT ?s {ds} WHERE { ?s ex:p 1 }",
            G1_G2,
            want(&["s=a", "s=a", "s=b"]),
        ),
        (
            "SELECT (COUNT(?s) AS ?c) {ds} WHERE { ?s ex:p 1 }",
            G1_EMPTY,
            want(&["c=3^^integer"]),
        ),
        (
            "SELECT ?s ?o {ds} WHERE { ?s ex:p ?o }",
            G1_EMPTY,
            want(&[&one_a, "o=1.0E0^^double;s=a", &one_b]),
        ),
        (
            "SELECT ?s ?o {ds} WHERE { ?s ex:p ?o }",
            G1_G2,
            want(&[&one_a, "o=1.0E0^^double;s=a", &one_b]),
        ),
        // A list that repeats 5: two facts, one per list position.
        (
            "SELECT ?s {ds} WHERE { ?s ex:q 5 }",
            G1,
            want(&["s=d", "s=d"]),
        ),
        (
            "SELECT ?s {ds} WHERE { ?s ex:q 5 }",
            G1_EMPTY,
            want(&["s=d", "s=d"]),
        ),
        // g2's first item is g1's first item.
        (
            "SELECT ?s {ds} WHERE { ?s ex:q 5 }",
            G1_G2,
            want(&["s=d", "s=d"]),
        ),
        (
            "SELECT (COUNT(?s) AS ?c) {ds} WHERE { ?s ex:q 5 }",
            G1_EMPTY,
            want(&["c=2^^integer"]),
        ),
        (
            "SELECT ?s ?o {ds} WHERE { ?s ex:q ?o }",
            G1_EMPTY,
            want(&["o=5^^integer;s=d", "o=5^^integer;s=d", "o=7^^integer;s=d"]),
        ),
        (
            "SELECT ?s ?o {ds} WHERE { ?s ex:q ?o }",
            G1_G2,
            want(&["o=5^^integer;s=d", "o=5^^integer;s=d", "o=7^^integer;s=d"]),
        ),
        // A ground pattern: one empty solution per fact it matches.
        (
            "SELECT (COUNT(*) AS ?c) {ds} WHERE { ex:a ex:p 1 }",
            G1_EMPTY,
            want(&["c=2^^integer"]),
        ),
        (
            "SELECT (COUNT(*) AS ?c) {ds} WHERE { ex:a ex:p 1 }",
            G1_G2,
            want(&["c=2^^integer"]),
        ),
    ];
    let mut failures = Vec::new();
    for (body, dataset, expected) in cases {
        let got = sparql_rows(fluree, body, dataset).await;
        if got != expected {
            failures.push(format!(
                "{state}: {dataset} {body}: {got:?}, want {expected:?}"
            ));
        }
        // The union with a graph that adds nothing answers as g1 alone.
        if dataset == G1_EMPTY {
            let alone = sparql_rows(fluree, body, G1).await;
            if got != alone {
                failures.push(format!(
                    "{state}: {body}: {got:?} with gEmpty, {alone:?} alone"
                ));
            }
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

/// The JSON-LD twin of the list case, through the connection.
async fn check_jsonld(fluree: &Fluree, state: &str) {
    let count = |graphs: &[&str]| {
        let from: Vec<String> = graphs.iter().map(|g| format!("{LEDGER}#{g}")).collect();
        json!({
            "@context": {"ex": "http://example.org/"},
            "from": from,
            "select": ["?s"],
            "where": [{"@id": "?s", "ex:q": 5}]
        })
    };
    for graphs in [
        &["urn:g1"][..],
        &["urn:g1", "urn:gEmpty"],
        &["urn:g1", "urn:g2"],
    ] {
        let result = fluree
            .query_connection(&count(graphs))
            .await
            .unwrap_or_else(|e| panic!("{graphs:?}: {e}"));
        let ledger = fluree.ledger(LEDGER).await.expect("ledger");
        let json: JsonValue = result
            .to_jsonld_async(ledger.as_graph_db_ref(0))
            .await
            .expect("format");
        let rows = json.as_array().map_or(0, Vec::len);
        assert_eq!(rows, 2, "{state}: JSON-LD from {graphs:?}: {json}");
    }
}

#[tokio::test]
async fn union_dedups_on_fact_identity() {
    let fluree = FlureeBuilder::memory().without_indexing().build_memory();
    fluree.create_ledger(LEDGER).await.expect("create ledger");
    let handle = fluree.ledger_cached(LEDGER).await.expect("cache");
    fluree
        .stage(&handle)
        .upsert_turtle(TRIG)
        .execute()
        .await
        .expect("seed");

    // Novelty only: the scans read decoded flakes.
    check_sparql(&fluree, "novelty").await;
    check_jsonld(&fluree, "novelty").await;

    // Indexed: the scans read index rows.
    fluree
        .reindex(LEDGER, ReindexOptions::default())
        .await
        .expect("index");
    fluree.disconnect_ledger(LEDGER).await;
    let handle = fluree.ledger_cached(LEDGER).await.expect("reload");
    assert!(handle.snapshot().await.binary_store.is_some(), "indexed");
    check_sparql(&fluree, "indexed").await;
    check_jsonld(&fluree, "indexed").await;
}
