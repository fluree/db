//! A scoped context's term takes the vocabulary in effect, and a keyword-form
//! string is refused as a node reference.
//!
//! A term a property- or type-scoped context defines with no `@id` used to
//! become a plain name, as if the enclosing `@vocab` did not exist. A string
//! such as `"@ignoreMe"` given as an `@id` used to be stored as that IRI.

use crate::support;
use crate::support::{genesis_ledger, normalize_rows};
use fluree_db_api::FlureeBuilder;
use serde_json::json;

fn context() -> serde_json::Value {
    json!({
        "@vocab": "http://example.org/",
        "foo": {"@context": {"bar": {"@type": "@id"}}}
    })
}

#[tokio::test]
async fn a_scoped_term_with_no_id_takes_the_vocabulary() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "tx/scoped-term-vocab:main").await;
    let ledger = fluree
        .insert(
            ledger0,
            &json!({
                "@context": context(),
                "@id": "http://example.org/s",
                "foo": {"@id": "http://example.org/n", "bar": "http://example.org/o"}
            }),
        )
        .await
        .expect("insert")
        .ledger;

    let out = support::query_sparql(
        &fluree,
        &ledger,
        "SELECT (STR(?p) AS ?ps) (STR(?o) AS ?os) (isIRI(?o) AS ?ref) WHERE { <http://example.org/n> ?p ?o }",
    )
    .await
    .expect("sparql query")
    .to_jsonld(&ledger.snapshot)
    .expect("to_jsonld");
    assert_eq!(
        normalize_rows(&out),
        vec![json!([
            "http://example.org/bar",
            "http://example.org/o",
            true
        ])]
    );

    // A query under the same context reads `bar` through the outer
    // `@vocab`, so it finds what the insert wrote.
    let query = json!({
        "@context": context(),
        "select": "?o",
        "where": {"@id": "?s", "foo": {"bar": "?o"}}
    });
    let rows = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("jsonld query")
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");
    assert_eq!(rows, json!(["http://example.org/o"]));
}

#[tokio::test]
async fn a_keyword_form_node_reference_is_refused() {
    let fluree = FlureeBuilder::memory().build_memory();
    let documents = [
        json!({"@context": {"ex": "http://example.org/"}, "@id": "ex:s", "ex:p": {"@id": "@ignoreMe"}}),
        json!({"@context": {"ex": "http://example.org/"}, "@id": "@ignoreMe", "ex:p": 1}),
    ];
    for (i, doc) in documents.into_iter().enumerate() {
        let ledger0 = genesis_ledger(&fluree, &format!("tx/keyword-reference-{i}:main")).await;
        let err = fluree
            .insert(ledger0, &doc)
            .await
            .expect_err(&doc.to_string())
            .to_string();
        assert!(
            err.contains("\"@ignoreMe\" is not supported as a node reference"),
            "{doc}: {err}"
        );
    }
}
