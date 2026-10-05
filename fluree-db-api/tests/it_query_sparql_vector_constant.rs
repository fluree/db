//! Vector similarity against a query vector written as a constant: an
//! `f:embeddingVector` literal inside the function call scores the same as
//! the same vector bound through `VALUES`, in SPARQL and JSON-LD alike.

use crate::support::{genesis_ledger, graphdb_from_ledger};
use fluree_db_api::FlureeBuilder;
use serde_json::{json, Value as JsonValue};

const PREFIX: &str = "PREFIX ex: <http://example.org/> PREFIX f: <https://ns.flur.ee/db#> ";

fn rows(value: JsonValue) -> Vec<(String, f64)> {
    value
        .as_array()
        .expect("rows")
        .iter()
        .map(|row| {
            (
                row[0].as_str().expect("id").to_string(),
                row[1].as_f64().expect("score"),
            )
        })
        .collect()
}

#[tokio::test]
async fn a_constant_query_vector_scores_as_a_values_binding() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, "it/sparql-vector-constant:main");
    let ledger = fluree
        .insert(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/"},
                "@graph": [
                    {"@id": "ex:a", "ex:embedding": {"@value": [0.8, 0.14, 0.9], "@type": "@vector"}},
                    {"@id": "ex:b", "ex:embedding": {"@value": [0.1, 0.9, 0.3], "@type": "@vector"}}
                ]
            }),
        )
        .await
        .expect("insert")
        .ledger;
    let db = graphdb_from_ledger(&ledger);
    let sparql =
        |body: &str| format!("{PREFIX}SELECT ?s ?score WHERE {{ {body} }} ORDER BY DESC(?score)");
    let run = |query: String| {
        let (fluree, db, snapshot) = (&fluree, &db, &ledger.snapshot);
        async move {
            let result = fluree.query(db, query.as_str()).await.expect("query");
            rows(result.to_jsonld(snapshot).expect("jsonld"))
        }
    };

    let bound = run(sparql(
        "VALUES ?q { \"[0.8, 0.14, 0.9]\"^^f:embeddingVector } \
         ?s ex:embedding ?v BIND(cosineSimilarity(?v, ?q) AS ?score)",
    ))
    .await;
    assert_eq!(bound.len(), 2, "{bound:?}");
    assert!((bound[0].1 - 1.0).abs() < 1e-6, "{bound:?}");

    for function in ["cosineSimilarity", "dotProduct", "euclideanDistance"] {
        let with_values = run(sparql(&format!(
            "VALUES ?q {{ \"[0.8, 0.14, 0.9]\"^^f:embeddingVector }} \
             ?s ex:embedding ?v BIND({function}(?v, ?q) AS ?score)"
        )))
        .await;
        let constant = run(sparql(&format!(
            "?s ex:embedding ?v \
             BIND({function}(?v, \"[0.8, 0.14, 0.9]\"^^f:embeddingVector) AS ?score)"
        )))
        .await;
        assert_eq!(constant, with_values, "{function}");
    }

    let jsonld = fluree
        .query(
            &db,
            &json!({
                "@context": {"ex": "http://example.org/"},
                "select": ["?s", "?score"],
                "values": [["?q"], [{"@value": [0.8, 0.14, 0.9], "@type": "https://ns.flur.ee/db#embeddingVector"}]],
                "where": [
                    {"@id": "?s", "ex:embedding": "?v"},
                    ["bind", "?score", "(cosineSimilarity ?v ?q)"]
                ],
                "orderBy": [["desc", "?score"]]
            }),
        )
        .await
        .expect("jsonld query");
    assert_eq!(
        rows(jsonld.to_jsonld(&ledger.snapshot).expect("jsonld")),
        bound
    );
}
