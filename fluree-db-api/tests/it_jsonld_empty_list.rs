//! JSON-LD empty-`@list` ingest — the twin of the Turtle `()` fix (issue
//! #1694).
//!
//! Per JSON-LD 1.1 → RDF deserialization ("List to RDF Conversion"), an
//! empty `@list` denotes the IRI `rdf:nil`. The transact JSON-LD parser used
//! to produce ZERO triple templates for `{"@list": []}` — the same
//! silent-loss class as the Turtle bug: the statement vanished with no
//! diagnostic. These tests pin the fix end-to-end through the public
//! transact surface and prove the two ingest surfaces agree: an empty
//! `@list` stores the one `rdf:nil` triple, identically to Turtle's `()`,
//! while non-empty `@list`s keep their `list_index` storage shape.

use crate::support;
use crate::support::normalize_rows;
use fluree_db_api::FlureeBuilder;
use serde_json::json;

const RDF_NIL: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#nil";

async fn rows(
    fluree: &support::MemoryFluree,
    ledger: &support::MemoryLedger,
    sparql: &str,
) -> Vec<serde_json::Value> {
    let out = support::query_sparql(fluree, ledger, sparql)
        .await
        .expect("sparql query")
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");
    normalize_rows(&out)
}

/// The issue-#1694 repro shape, spoken in JSON-LD: three list members under
/// `ex:s` and an empty list under `ex:s2`. As RDF this denotes
/// `ex:s2 ex:items rdf:nil` — before the fix `ex:s2` never reached the
/// database at all.
#[tokio::test]
async fn empty_jsonld_list_object_stores_rdf_nil() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = fluree
        .create_ledger("tx/jsonld-empty-list:main")
        .await
        .expect("genesis");
    let txn = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [
            {"@id": "ex:s", "ex:items": {"@list": [
                {"@id": "ex:a"}, {"@id": "ex:b"}, {"@id": "ex:c"}
            ]}},
            {"@id": "ex:s2", "ex:items": {"@list": []}}
        ]
    });
    let ledger = fluree.insert(ledger0, &txn).await.expect("insert").ledger;

    let nil_rows = rows(
        &fluree,
        &ledger,
        r"PREFIX ex: <http://example.org/>
          SELECT ?o WHERE { ex:s2 ex:items ?o }",
    )
    .await;
    assert_eq!(nil_rows.len(), 1, "ex:s2 must exist: {nil_rows:?}");
    let obj = nil_rows[0]
        .as_array()
        .and_then(|r| r.first())
        .and_then(|v| v.as_str())
        .unwrap_or_else(|| panic!("expected an IRI object row, got {nil_rows:?}"));
    assert!(
        obj == RDF_NIL || obj == "rdf:nil",
        "object must be rdf:nil, got {obj}"
    );

    // Non-empty lists keep the list_index storage shape: members stay
    // direct objects and no rdf:first/rest spine is materialized.
    let items = rows(
        &fluree,
        &ledger,
        r"PREFIX ex: <http://example.org/>
          SELECT ?o WHERE { ex:s ex:items ?o }",
    )
    .await;
    assert_eq!(items.len(), 3, "list members stay direct: {items:?}");

    // The whole graph: 3 indexed list items + the nil edge.
    let all = rows(&fluree, &ledger, "SELECT ?s ?p ?o WHERE { ?s ?p ?o }").await;
    assert_eq!(all.len(), 4, "expected 4 stored statements: {all:?}");
}

/// The two ingest surfaces must agree: the same statements through Turtle
/// (`()`) and through JSON-LD (`{"@list": []}`) store identical graphs.
#[tokio::test]
async fn empty_jsonld_list_matches_turtle_empty_collection() {
    let fluree = FlureeBuilder::memory().build_memory();

    let ttl_ledger0 = fluree
        .create_ledger("tx/jsonld-empty-list:ttl")
        .await
        .expect("genesis");
    let ttl_ledger = fluree
        .insert_turtle(
            ttl_ledger0,
            r"@prefix ex: <http://example.org/> .
ex:s  ex:items ( ex:a ex:b ex:c ) .
ex:s2 ex:items () .
",
        )
        .await
        .expect("insert turtle")
        .ledger;

    let json_ledger0 = fluree
        .create_ledger("tx/jsonld-empty-list:json")
        .await
        .expect("genesis");
    let txn = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [
            {"@id": "ex:s", "ex:items": {"@list": [
                {"@id": "ex:a"}, {"@id": "ex:b"}, {"@id": "ex:c"}
            ]}},
            {"@id": "ex:s2", "ex:items": {"@list": []}}
        ]
    });
    let json_ledger = fluree
        .insert(json_ledger0, &txn)
        .await
        .expect("insert jsonld")
        .ledger;

    let q = "SELECT ?s ?p ?o WHERE { ?s ?p ?o }";
    assert_eq!(
        rows(&fluree, &ttl_ledger, q).await,
        rows(&fluree, &json_ledger, q).await,
        "Turtle `()` and JSON-LD `{{\"@list\": []}}` must store identically"
    );
}

/// An empty `@list` in explicit array position (`[{"@list": []}]`) is the
/// same value and must store the same `rdf:nil` triple. This spelling had a
/// DIFFERENT defect from the bare-object one: JSON-LD expansion dropped the
/// `@list` key (any key whose values expand to nothing was dropped), turning
/// the item into `{}` — which then stored a spurious BLANK NODE object
/// instead of `rdf:nil`.
#[tokio::test]
async fn empty_jsonld_list_in_array_position_stores_rdf_nil() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = fluree
        .create_ledger("tx/jsonld-empty-list:arr")
        .await
        .expect("genesis");
    let txn = json!({
        "@context": {"ex": "http://example.org/"},
        "@id": "ex:s2",
        "ex:items": [{"@list": []}]
    });
    let ledger = fluree.insert(ledger0, &txn).await.expect("insert").ledger;

    let all = rows(&fluree, &ledger, "SELECT ?s ?p ?o WHERE { ?s ?p ?o }").await;
    assert_eq!(
        all.len(),
        1,
        "exactly the rdf:nil triple, nothing else: {all:?}"
    );
    let obj = all[0]
        .as_array()
        .and_then(|r| r.get(2))
        .and_then(|v| v.as_str())
        .unwrap_or_else(|| panic!("expected an IRI object, got {all:?}"));
    assert_eq!(
        obj, RDF_NIL,
        "array-position empty @list must store rdf:nil, not a blank node"
    );
}
