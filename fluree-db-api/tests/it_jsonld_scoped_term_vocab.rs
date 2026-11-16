//! A scoped context's term takes the vocabulary in effect, and a keyword-form
//! string is refused as a node reference.
//!
//! A term a property- or type-scoped context defines with no `@id` used to
//! become a plain name, as if the enclosing `@vocab` did not exist. A string
//! such as `"@ignoreMe"` given as an `@id` used to be stored as that IRI.

use crate::support;
use crate::support::{genesis_ledger, normalize_rows};
use fluree_db_api::FlureeBuilder;
use serde_json::{json, Value};

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
        json!({"@context": {"ex": "http://example.org/"}, "@id": "ex:s", "@type": "@ignoreMe"}),
    ];
    for (i, doc) in documents.into_iter().enumerate() {
        let ledger0 = genesis_ledger(&fluree, &format!("tx/keyword-reference-{i}:main")).await;
        let err = fluree
            .insert(ledger0, &doc)
            .await
            .expect_err(&doc.to_string())
            .to_string();
        assert!(
            err.contains("\"@ignoreMe\" has the form of a JSON-LD keyword, so it names no node"),
            "{doc}: {err}"
        );
    }
}

/// Every statement, as plain strings.
async fn all_statements(
    fluree: &support::MemoryFluree,
    ledger: &support::MemoryLedger,
) -> Vec<Value> {
    let out = support::query_sparql(
        fluree,
        ledger,
        "SELECT (STR(?s) AS ?ss) (STR(?p) AS ?ps) (STR(?o) AS ?os) WHERE { ?s ?p ?o }",
    )
    .await
    .expect("sparql query")
    .to_jsonld(&ledger.snapshot)
    .expect("to_jsonld");
    normalize_rows(&out)
}

/// The values of `foo`'s `bar`, read under `context()`.
async fn bar_values(fluree: &support::MemoryFluree, ledger: &support::MemoryLedger) -> Value {
    let query = json!({
        "@context": context(),
        "select": "?o",
        "where": {"@id": "?s", "foo": {"bar": "?o"}}
    });
    support::query_jsonld(fluree, ledger, &query)
        .await
        .expect("jsonld query")
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld")
}

/// An earlier release wrote such a term as a plain name, which a query under
/// the context never found; an update with no context renames it to the IRI
/// the term now names.
#[tokio::test]
async fn a_plain_name_an_earlier_release_wrote_renames() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "tx/scoped-term-rename:main").await;
    // The insert above, as an earlier release stored it.
    let ledger = fluree
        .insert(
            ledger0,
            &json!({
                "@id": "http://example.org/s",
                "http://example.org/foo": {
                    "@id": "http://example.org/n",
                    "bar": {"@id": "http://example.org/o"}
                }
            }),
        )
        .await
        .expect("insert")
        .ledger;
    assert_eq!(bar_values(&fluree, &ledger).await, json!([]));

    let ledger = fluree
        .update(
            ledger,
            &json!({
                "where": {"@id": "?n", "bar": "?o"},
                "delete": {"@id": "?n", "bar": "?o"},
                "insert": {"@id": "?n", "http://example.org/bar": "?o"}
            }),
        )
        .await
        .expect("rename")
        .ledger;
    assert_eq!(
        bar_values(&fluree, &ledger).await,
        json!(["http://example.org/o"])
    );
    assert_eq!(
        all_statements(&fluree, &ledger).await,
        normalize_rows(&json!([
            [
                "http://example.org/n",
                "http://example.org/bar",
                "http://example.org/o"
            ],
            [
                "http://example.org/s",
                "http://example.org/foo",
                "http://example.org/n"
            ]
        ]))
    );
}

/// An earlier release stored a keyword-form `@id` as the IRI. A query that
/// names it still reads it, a delete that names it is refused, and two
/// updates that match it by a filter move its statements, and the
/// references to it, to an IRI.
#[tokio::test]
async fn a_keyword_form_iri_an_earlier_release_wrote_moves() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ctx = json!({"ex": "http://example.org/"});
    let ledger0 = genesis_ledger(&fluree, "tx/keyword-iri-move:main").await;
    // `{"@id": "@alice", ...}` as an earlier release stored it.
    let ledger = fluree
        .update(
            ledger0,
            &json!({
                "@context": ctx,
                "where": [["bind", "?a", "(iri \"@alice\")"]],
                "insert": [
                    {"@id": "?a", "@type": "ex:Person", "ex:name": "Alice"},
                    {"@id": "ex:bob", "ex:knows": {"@id": "?a"}}
                ]
            }),
        )
        .await
        .expect("insert")
        .ledger;

    let names = support::query_jsonld(
        &fluree,
        &ledger,
        &json!({"@context": ctx, "select": "?n", "where": {"@id": "@alice", "ex:name": "?n"}}),
    )
    .await
    .expect("query")
    .to_jsonld(&ledger.snapshot)
    .expect("to_jsonld");
    assert_eq!(names, json!(["Alice"]));
    let err = fluree
        .update(
            ledger.clone(),
            &json!({"@context": ctx, "delete": {"@id": "@alice", "ex:name": "Alice"}}),
        )
        .await
        .expect_err("delete naming @alice")
        .to_string();
    assert!(err.contains("has the form of a JSON-LD keyword"), "{err}");

    let ledger = fluree
        .update(
            ledger,
            &json!({
                "@context": ctx,
                "where": [{"@id": "?s", "?p": "?o"}, ["filter", "(= (str ?s) \"@alice\")"]],
                "delete": {"@id": "?s", "?p": "?o"},
                "insert": {"@id": "ex:alice", "?p": "?o"}
            }),
        )
        .await
        .expect("move statements")
        .ledger;
    let ledger = fluree
        .update(
            ledger,
            &json!({
                "@context": ctx,
                "where": [{"@id": "?x", "?p": "?s"}, ["filter", "(= (str ?s) \"@alice\")"]],
                "delete": {"@id": "?x", "?p": "?s"},
                "insert": {"@id": "?x", "?p": {"@id": "ex:alice"}}
            }),
        )
        .await
        .expect("move references")
        .ledger;
    assert_eq!(
        all_statements(&fluree, &ledger).await,
        normalize_rows(&json!([
            [
                "http://example.org/alice",
                "http://example.org/name",
                "Alice"
            ],
            [
                "http://example.org/alice",
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                "http://example.org/Person"
            ],
            [
                "http://example.org/bob",
                "http://example.org/knows",
                "http://example.org/alice"
            ]
        ]))
    );
}
