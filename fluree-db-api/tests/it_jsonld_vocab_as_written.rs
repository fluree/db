//! An explicit `@vocab` is taken as written.
//!
//! JSON-LD 1.1 joins a term to the vocabulary mapping as written. The
//! expander used to add a `/` to a `@vocab` ending in neither `/` nor `#`, so
//! `"@vocab": "urn:ex:"` wrote `urn:ex:/name`. A string `@context`, Fluree's
//! fallback for a remote context, keeps the `/`.

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

/// Cypher names IRIs by a default context's vocabulary as JSON-LD resolves
/// it, so it reads what a JSON-LD write under that context stored.
#[tokio::test]
async fn cypher_reads_the_vocab_a_jsonld_write_used() {
    let fluree = FlureeBuilder::memory().build_memory();
    let contexts = [
        json!({"@vocab": "urn:ex:"}),
        // A relative @vocab resolves against the base.
        json!({"@base": "http://example.org/doc/", "@vocab": ""}),
        // A compact-IRI @vocab expands against the context before it.
        json!([{"ex": "http://example.org/"}, {"@vocab": "ex:v/"}]),
    ];
    for (i, context) in contexts.into_iter().enumerate() {
        let ledger0 = genesis_ledger(&fluree, &format!("tx/vocab-cypher-{i}:main")).await;
        let ledger = fluree
            .insert(
                ledger0,
                &json!({
                    "@context": context,
                    "@id": "http://example.org/ann",
                    "@type": "Person",
                    "name": "Ann"
                }),
            )
            .await
            .expect("insert")
            .ledger;
        let db = graphdb_from_ledger(&ledger).with_default_context(Some(context.clone()));
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
            "cypher under {context}: {cj}"
        );
    }
}

#[tokio::test]
async fn a_string_context_keeps_its_slash() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "tx/vocab-string-context:main").await;
    let ledger = fluree
        .insert(
            ledger0,
            &json!({
                "@context": "http://example.org/terms",
                "@id": "http://example.org/s",
                "name": "Ann"
            }),
        )
        .await
        .expect("insert")
        .ledger;
    assert_eq!(
        statements(&fluree, &ledger).await,
        vec![json!(["http://example.org/terms/name", "Ann"])]
    );
}

/// A `@vocab` equal to the `@base` names the properties and types in a
/// result, and the `@base` the ids, so each reads back under the context.
#[tokio::test]
async fn an_equal_base_and_vocab_read_back() {
    let fluree = FlureeBuilder::memory().build_memory();
    let context = json!({"@base": "http://example.org/ns#", "@vocab": "http://example.org/ns#"});
    let ledger0 = genesis_ledger(&fluree, "tx/vocab-equal-base:main").await;
    let ledger = fluree
        .insert(
            ledger0,
            &json!({"@context": context, "@id": "#alice", "@type": "Person", "name": "Ann"}),
        )
        .await
        .expect("insert")
        .ledger;
    let query = json!({
        "@context": context,
        "select": {"?s": ["*"]},
        "where": {"@id": "?s", "name": "Ann"}
    });
    let rows = support::query_jsonld_formatted(&fluree, &ledger, &query)
        .await
        .expect("jsonld query");
    assert_eq!(
        rows,
        json!([{"@id": "#alice", "@type": "Person", "name": "Ann"}])
    );
    let read_back = json!({
        "@context": context,
        "select": "?n",
        "where": {"@id": "#alice", "@type": "Person", "name": "?n"}
    });
    assert_eq!(
        support::query_jsonld_formatted(&fluree, &ledger, &read_back)
            .await
            .expect("read back"),
        json!(["Ann"])
    );
}

/// A name holding a `:` would read back as a compact or absolute IRI, so a
/// result keeps it whole rather than shortening it by the vocabulary.
#[tokio::test]
async fn a_vocab_name_holding_a_colon_reads_back() {
    let fluree = FlureeBuilder::memory().build_memory();
    let context = json!({"@vocab": "urn:ex:"});
    let ledger0 = genesis_ledger(&fluree, "tx/vocab-colon-name:main").await;
    let ledger = fluree
        .insert(
            ledger0,
            &json!({"@context": context, "@id": "urn:ex:s", "urn:ex:a:b": "x"}),
        )
        .await
        .expect("insert")
        .ledger;
    let query = json!({
        "@context": context,
        "select": {"?s": ["*"]},
        "where": {"@id": "?s", "urn:ex:a:b": "x"}
    });
    let rows = support::query_jsonld_formatted(&fluree, &ledger, &query)
        .await
        .expect("jsonld query");
    assert_eq!(rows, json!([{"@id": "urn:ex:s", "urn:ex:a:b": "x"}]));
}

/// An IRI equal to the `@base` or the `@vocab` comes back as an IRI that
/// reads back under the same context, in JSON-LD and in SPARQL results.
#[tokio::test]
async fn an_iri_equal_to_the_base_or_vocab_reads_back() {
    let fluree = FlureeBuilder::memory().build_memory();

    // A node named by the document's own base, and a reference to it.
    let context = json!({"@base": "http://example.org/doc"});
    let ledger0 = genesis_ledger(&fluree, "tx/equal-base:main").await;
    let ledger = fluree
        .insert(
            ledger0,
            &json!({"@context": context, "@graph": [
                {"@id": "", "http://example.org/title": "The doc"},
                {"@id": "http://example.org/other", "http://example.org/cites": {"@id": ""}}
            ]}),
        )
        .await
        .expect("insert")
        .ledger;
    let query = json!({
        "@context": context,
        "select": {"?s": ["*"]},
        "where": {"@id": "?s", "http://example.org/cites": "?o"}
    });
    assert_eq!(
        support::query_jsonld_formatted(&fluree, &ledger, &query)
            .await
            .expect("jsonld query"),
        json!([{
            "@id": "http://example.org/other",
            "http://example.org/cites": {"@id": "http://example.org/doc"}
        }])
    );
    let sparql = support::query_sparql_formatted(
        &fluree,
        &ledger,
        "BASE <http://example.org/doc> SELECT ?o WHERE { <http://example.org/other> ?p ?o }",
    )
    .await
    .expect("sparql query");
    assert_eq!(sparql, json!([["http://example.org/doc"]]));

    // A type equal to the vocabulary.
    let context = json!({"@vocab": "urn:ex:"});
    let ledger0 = genesis_ledger(&fluree, "tx/equal-vocab:main").await;
    let ledger = fluree
        .insert(
            ledger0,
            &json!({"@context": context, "@id": "urn:ex:s", "@type": "urn:ex:", "name": "x"}),
        )
        .await
        .expect("insert")
        .ledger;
    let query = json!({
        "@context": context,
        "select": {"?s": ["*"]},
        "where": {"@id": "?s", "name": "x"}
    });
    assert_eq!(
        support::query_jsonld_formatted(&fluree, &ledger, &query)
            .await
            .expect("jsonld query"),
        json!([{"@id": "urn:ex:s", "@type": "urn:ex:", "name": "x"}])
    );
}
