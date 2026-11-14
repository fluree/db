//! An explicit `@vocab` is taken as written.
//!
//! JSON-LD 1.1 joins a term to the vocabulary mapping as written. The
//! expander used to add a `/` to a `@vocab` ending in neither `/` nor `#`, so
//! `"@vocab": "urn:ex:"` wrote `urn:ex:/name`. Fluree's own fallbacks keep the
//! `/`: a string `@context`, and a `@base` standing in for a missing `@vocab`.

use crate::support;
use crate::support::{genesis_ledger, graphdb_from_ledger, normalize_rows};
use fluree_db_api::FlureeBuilder;
use serde_json::{json, Value};

const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";

/// Every statement about `<http://example.org/s>`, as plain strings.
async fn statements(fluree: &support::MemoryFluree, ledger: &support::MemoryLedger) -> Vec<Value> {
    let out = support::query_sparql(
        fluree,
        ledger,
        "SELECT (STR(?p) AS ?ps) (STR(?o) AS ?os) WHERE { <http://example.org/s> ?p ?o }",
    )
    .await
    .expect("sparql query")
    .to_jsonld(&ledger.snapshot)
    .expect("to_jsonld");
    normalize_rows(&out)
}

#[tokio::test]
async fn an_explicit_vocab_is_taken_as_written() {
    let fluree = FlureeBuilder::memory().build_memory();
    for (i, vocab) in ["urn:ex:", "http://example.org/ns"].into_iter().enumerate() {
        let context = json!({"@vocab": vocab});
        let ledger0 = genesis_ledger(&fluree, &format!("tx/vocab-as-written-{i}:main")).await;
        let ledger = fluree
            .insert(
                ledger0,
                &json!({
                    "@context": context,
                    "@id": "http://example.org/s",
                    "@type": "Person",
                    "name": "Ann"
                }),
            )
            .await
            .expect("insert")
            .ledger;
        assert_eq!(
            statements(&fluree, &ledger).await,
            normalize_rows(&json!([
                [RDF_TYPE, format!("{vocab}Person")],
                [format!("{vocab}name"), "Ann"],
            ])),
            "insert under {vocab}"
        );

        // A query under the same context reads the same IRIs, and its result
        // compacts them back to the terms.
        let query = json!({
            "@context": context,
            "select": {"?s": ["*"]},
            "where": {"@id": "?s", "@type": "Person", "name": "Ann"}
        });
        let rows = support::query_jsonld_formatted(&fluree, &ledger, &query)
            .await
            .expect("jsonld query");
        assert_eq!(
            rows,
            json!([{"@id": "http://example.org/s", "@type": "Person", "name": "Ann"}]),
            "query under {vocab}"
        );
    }
}

/// Cypher joins a default context's `@vocab` to a name as written, so a
/// Cypher read finds what a JSON-LD write under the same `@vocab` stored.
#[tokio::test]
async fn cypher_and_jsonld_agree_on_the_vocab() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "tx/vocab-as-written-cypher:main").await;
    let ledger = fluree
        .insert(
            ledger0,
            &json!({
                "@context": {"@vocab": "urn:ex:"},
                "@id": "urn:ex:ann",
                "@type": "Person",
                "name": "Ann"
            }),
        )
        .await
        .expect("insert")
        .ledger;
    let db = graphdb_from_ledger(&ledger).with_default_context(Some(json!({"@vocab": "urn:ex:"})));
    let cj = fluree
        .query_cypher(&db, "MATCH (n:Person) RETURN n.name AS name")
        .await
        .expect("cypher query")
        .to_cypher_json_async(db.as_graph_db_ref())
        .await
        .expect("cypher json");
    assert_eq!(
        cj["results"][0]["data"][0]["row"][0],
        json!("Ann"),
        "cypher result: {cj}"
    );
}

#[tokio::test]
async fn a_string_context_and_a_base_as_vocab_keep_their_slash() {
    let fluree = FlureeBuilder::memory().build_memory();
    let cases = [
        (
            json!("http://example.org/terms"),
            "http://example.org/terms/name",
        ),
        (
            json!({"@base": "http://example.org/ns"}),
            "http://example.org/ns/name",
        ),
    ];
    for (i, (context, predicate)) in cases.into_iter().enumerate() {
        let ledger0 = genesis_ledger(&fluree, &format!("tx/vocab-fallback-{i}:main")).await;
        let ledger = fluree
            .insert(
                ledger0,
                &json!({"@context": context, "@id": "http://example.org/s", "name": "Ann"}),
            )
            .await
            .expect("insert")
            .ledger;
        assert_eq!(
            statements(&fluree, &ledger).await,
            vec![json!([predicate, "Ann"])],
            "insert under {context}"
        );
    }
}
