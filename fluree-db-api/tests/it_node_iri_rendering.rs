//! A node reads the same in a result however the query produced it: a stored
//! node, a `GRAPH` variable's graph and an IRI an expression built (#1496).
//! JSON-LD, typed JSON and the display profiles of SPARQL JSON and CSV compact
//! each by the query's prefixes; the W3C SPARQL JSON profile writes each as
//! its absolute IRI.

use crate::support;
use fluree_db_api::{FlureeBuilder, FormatterConfig, LedgerState, QueryResult};
use serde_json::{json, Value as JsonValue};

const LEDGER: &str = "it/node-iri-rendering:main";

/// `?g` is the graph `ex:g1`, `?o` the same IRI as a stored object, and
/// `?made` an IRI in a namespace the ledger has never stored.
const SPARQL: &str = "PREFIX ex: <http://example.org/ns/> PREFIX o: <http://other.example/> \
                      SELECT ?g ?o ?made WHERE { \
                      GRAPH ?g { ex:s ex:p ?o } BIND(IRI(\"http://other.example/x\") AS ?made) }";

async fn seed() -> (fluree_db_api::Fluree, LedgerState) {
    let fluree = FlureeBuilder::memory().build_memory();
    fluree.create_ledger(LEDGER).await.expect("create");
    fluree
        .graph(LEDGER)
        .transact()
        .insert_turtle(
            "@prefix ex: <http://example.org/ns/> .\nGRAPH ex:g1 { ex:s ex:p ex:g1 . }\n",
        )
        .commit()
        .await
        .expect("insert");
    let ledger = fluree.ledger(LEDGER).await.expect("load");
    (fluree, ledger)
}

/// The `value` of each binding in the one row of a SPARQL JSON result.
fn sparql_json_row(json: &JsonValue) -> Vec<JsonValue> {
    ["g", "o", "made"]
        .iter()
        .map(|v| json["results"]["bindings"][0][*v]["value"].clone())
        .collect()
}

fn check(result: &QueryResult, ledger: &LedgerState, surface: &str) {
    let snapshot = &ledger.snapshot;
    assert_eq!(
        result.to_jsonld(snapshot).expect("jsonld"),
        json!([["ex:g1", "ex:g1", "o:x"]]),
        "{surface}: JSON-LD"
    );
    assert_eq!(
        result.to_typed_json(snapshot).expect("typed"),
        json!([{
            "?g": {"@id": "ex:g1"},
            "?o": {"@id": "ex:g1"},
            "?made": {"@id": "o:x"}
        }]),
        "{surface}: typed JSON"
    );
    let display = result
        .format(
            snapshot,
            &FormatterConfig::sparql_json().with_compact_iris(),
        )
        .expect("display SPARQL JSON");
    assert_eq!(
        sparql_json_row(&display),
        ["ex:g1", "ex:g1", "o:x"],
        "{surface}: SPARQL JSON display profile"
    );
    let w3c = result.to_sparql_json(snapshot).expect("SPARQL JSON");
    assert_eq!(
        sparql_json_row(&w3c),
        [
            "http://example.org/ns/g1",
            "http://example.org/ns/g1",
            "http://other.example/x"
        ],
        "{surface}: SPARQL JSON"
    );
    let csv = result
        .to_delimited_bytes(snapshot, &FormatterConfig::csv().with_compact_iris())
        .expect("display CSV");
    assert_eq!(
        String::from_utf8(csv).expect("utf-8").lines().nth(1),
        Some("ex:g1,ex:g1,o:x"),
        "{surface}: CSV display profile"
    );
}

#[tokio::test]
async fn a_node_reads_alike_however_the_query_produced_it() {
    let (fluree, ledger) = seed().await;
    let sparql = support::query_sparql(&fluree, &ledger, SPARQL)
        .await
        .expect("SPARQL");
    check(&sparql, &ledger, "SPARQL");

    let jsonld = json!({
        "@context": {"ex": "http://example.org/ns/", "o": "http://other.example/"},
        "select": ["?g", "?o", "?made"],
        "where": [
            ["graph", "?g", {"@id": "ex:s", "ex:p": "?o"}],
            ["bind", "?made", "(iri \"http://other.example/x\")"]
        ]
    });
    let jsonld = support::query_jsonld(&fluree, &ledger, &jsonld)
        .await
        .expect("JSON-LD");
    check(&jsonld, &ledger, "JSON-LD");
}

/// A JSON-LD `@vocab` shortens a predicate, not a node: a node a query
/// returns keeps its full IRI in every compacting result format (#1280).
#[tokio::test]
async fn vocab_does_not_shorten_a_node_in_a_result() {
    let (fluree, ledger) = seed().await;
    let query = json!({
        "@context": {"@vocab": "http://example.org/ns/"},
        "select": ["?o"],
        "where": [["graph", "?g", {"@id": "http://example.org/ns/s", "p": "?o"}]]
    });
    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("JSON-LD");
    let snapshot = &ledger.snapshot;
    let node = "http://example.org/ns/g1";
    assert_eq!(result.to_jsonld(snapshot).expect("jsonld"), json!([[node]]));
    let csv = result
        .to_delimited_bytes(snapshot, &FormatterConfig::csv().with_compact_iris())
        .expect("display CSV");
    assert_eq!(
        String::from_utf8(csv).expect("utf-8").lines().nth(1),
        Some(node)
    );
}
