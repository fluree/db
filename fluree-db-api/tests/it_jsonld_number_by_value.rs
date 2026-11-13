//! A JSON-LD number is typed by its value, as JSON-LD 1.1 API § 8.2.2
//! converts it to RDF: `1.0` and `1e3` are `xsd:integer`, and only a number
//! with a fractional part (or of 10^21 and more) is an `xsd:double`. A
//! document that wants the double `1.0` gives the value its `@type`.
//!
//! Documents are parsed from text: `json!` would not keep `1.0` apart from
//! what a client sends.

use crate::support;
use crate::support::normalize_rows;
use fluree_db_api::FlureeBuilder;
use serde_json::{json, Value};

const XSD: &str = "http://www.w3.org/2001/XMLSchema#";

fn parsed(text: &str) -> Value {
    serde_json::from_str(text).expect("JSON")
}

/// Each property's value datatype, as `[property, datatype]` rows.
async fn datatypes(fluree: &support::MemoryFluree, ledger: &support::MemoryLedger) -> Vec<Value> {
    let query = r"PREFIX ex: <http://example.org/>
        SELECT (STR(?p) AS ?property) (STR(DATATYPE(?v)) AS ?dt)
        WHERE { ex:s ?p ?v } ORDER BY ?property";
    let out = support::query_sparql(fluree, ledger, query)
        .await
        .expect("sparql query")
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");
    normalize_rows(&out)
}

#[tokio::test]
async fn a_whole_number_is_stored_as_an_integer_however_it_is_spelled() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree
        .create_ledger("tx/number-by-value:main")
        .await
        .expect("genesis");
    let doc = parsed(
        r#"{
            "@context": {"ex": "http://example.org/", "xsd": "http://www.w3.org/2001/XMLSchema#"},
            "@id": "ex:s",
            "ex:a": 1,
            "ex:b": 1.0,
            "ex:c": 1e3,
            "ex:d": 2.5,
            "ex:e": {"@value": 3.0},
            "ex:f": {"@value": 1.0, "@type": "xsd:double"},
            "ex:g": {"@list": [4.0]},
            "ex:h": 18446744073709551615
        }"#,
    );
    let ledger = fluree.insert(ledger, &doc).await.expect("insert").ledger;
    let (integer, double) = (format!("{XSD}integer"), format!("{XSD}double"));
    let ex = |name: &str| format!("http://example.org/{name}");
    assert_eq!(
        datatypes(&fluree, &ledger).await,
        vec![
            json!([ex("a"), integer]),
            json!([ex("b"), integer]),
            json!([ex("c"), integer]),
            json!([ex("d"), double]),
            json!([ex("e"), integer]),
            json!([ex("f"), double]),
            json!([ex("g"), integer]),
            json!([ex("h"), integer]),
        ]
    );

    // A `values` cell and a delete read the number the same way, so `1.0`
    // names the stored integer.
    let query = parsed(
        r#"{
            "@context": {"ex": "http://example.org/"},
            "select": "?s",
            "where": [{"@id": "?s", "ex:b": "?v"}],
            "values": ["?v", [1.0]]
        }"#,
    );
    let rows = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("jsonld query")
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");
    assert_eq!(rows, json!(["ex:s"]));

    let delete = parsed(
        r#"{
            "@context": {"ex": "http://example.org/"},
            "delete": {"@id": "ex:s", "ex:b": 1.0, "ex:c": 1000}
        }"#,
    );
    let ledger = fluree.update(ledger, &delete).await.expect("delete").ledger;
    let left: Vec<Value> = datatypes(&fluree, &ledger)
        .await
        .into_iter()
        .map(|row| row[0].clone())
        .collect();
    assert_eq!(
        left,
        ["a", "d", "e", "f", "g", "h"].map(|name| json!(ex(name)))
    );
}

/// A typed integer past i64 is kept exact, as the untyped one is.
#[tokio::test]
async fn a_typed_integer_past_i64_is_kept_exact() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree
        .create_ledger("tx/number-typed-big:main")
        .await
        .expect("genesis");
    let doc = parsed(
        r#"{
            "@context": {"ex": "http://example.org/", "xsd": "http://www.w3.org/2001/XMLSchema#"},
            "@id": "ex:s",
            "ex:a": {"@value": 18446744073709551615, "@type": "xsd:integer"},
            "ex:b": {"@value": 18446744073709551615, "@type": "xsd:unsignedLong"}
        }"#,
    );
    let ledger = fluree.insert(ledger, &doc).await.expect("insert").ledger;
    let query = r"PREFIX ex: <http://example.org/>
        SELECT (STR(?v) AS ?text) WHERE { ex:s ?p ?v } ORDER BY ?p";
    let out = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("sparql query")
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");
    assert_eq!(
        out,
        json!([["18446744073709551615"], ["18446744073709551615"]])
    );

    let too_big = parsed(
        r#"{
            "@context": {"ex": "http://example.org/", "xsd": "http://www.w3.org/2001/XMLSchema#"},
            "@id": "ex:s",
            "ex:c": {"@value": 18446744073709551615, "@type": "xsd:long"}
        }"#,
    );
    assert!(fluree.insert(ledger, &too_big).await.is_err());
}

/// A removal's selector reads a bare number as a transaction does, so an
/// integer past i64 names the integer stored, not the double it rounds to.
#[tokio::test]
async fn a_removal_selector_reads_a_big_integer_exactly() {
    use fluree_db_api::retention::{FactSelector, RemovalRequest};
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "tx/number-selector:main";
    let ledger = fluree.create_ledger(id).await.expect("genesis");
    let doc = parsed(
        r#"{
            "@context": {"ex": "http://example.org/"},
            "@id": "ex:s",
            "ex:b": [18446744073709551615, 2.5]
        }"#,
    );
    fluree.insert(ledger, &doc).await.expect("insert");
    let receipt = fluree
        .remove_occurrences(
            id,
            RemovalRequest {
                occurrences: vec![],
                matching: vec![FactSelector {
                    subject: "http://example.org/s".into(),
                    predicate: "http://example.org/b".into(),
                    graph: None,
                    object: Some(parsed("18446744073709551615")),
                }],
            },
        )
        .await
        .expect("remove");
    assert!(receipt.retracted_t.is_some(), "{receipt:?}");
    let ledger = fluree.ledger(id).await.expect("load");
    let query = r"PREFIX ex: <http://example.org/>
        SELECT ?v WHERE { ex:s ex:b ?v }";
    let out = support::query_sparql(&fluree, &ledger, query)
        .await
        .expect("sparql query")
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");
    assert_eq!(out, json!([[2.5]]));
}
