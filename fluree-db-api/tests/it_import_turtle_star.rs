//! Bulk import of Turtle-star: end-to-end from a directory of `.ttl` files
//! through `ImportSink` to a queryable ledger.
//!
//! `ImportSink` opted in to reified triples when the Turtle parser gained the
//! RDF 1.2 asserting forms, but until now no test drove the whole import
//! path (splitter → parser → sink → index) with star input and read the
//! claims back through the query surfaces. This does, with the RDF 1.2
//! `VERSION` directive in the header the way a conformant producer writes it.

#![cfg(feature = "native")]

use crate::support;
use fluree_db_api::{FlureeBuilder, LedgerState};
use serde_json::Value as JsonValue;

const CLAIMS: &str = r#"VERSION "1.2"
@prefix ex: <http://example.org/> .

ex:alice ex:knows ex:bob ~ ex:claim1 {| ex:confidence 0.9 ; ex:source ex:hr |} .
ex:alice ex:knows ex:carol {| ex:source ex:linkedin |} .
<< ex:bob ex:knows ex:dave >> ex:source ex:crm .
ex:carol ex:age 42 ~ ex:claim2 .
"#;

const PLAIN: &str = r#"@prefix ex: <http://example.org/> .
ex:dave ex:name "Dave" .
"#;

async fn import_dir(files: &[(&str, &str)], alias: &str) -> (fluree_db_api::Fluree, LedgerState) {
    let db_dir = tempfile::tempdir().expect("db tmpdir");
    let data_dir = tempfile::tempdir().expect("data tmpdir");
    for (name, content) in files {
        std::fs::write(data_dir.path().join(name), content).expect("write fixture");
    }
    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");
    fluree
        .create(alias)
        .import(data_dir.path())
        .threads(1)
        .memory_budget_mb(256)
        .cleanup(false)
        .execute()
        .await
        .expect("import of Turtle-star must succeed");
    let ledger = fluree.ledger(alias).await.expect("load ledger");
    // Keep the temp dirs alive for the ledger's lifetime.
    std::mem::forget(db_dir);
    std::mem::forget(data_dir);
    (fluree, ledger)
}

fn rows(result: &JsonValue) -> Vec<Vec<String>> {
    result
        .as_array()
        .expect("row array")
        .iter()
        .map(|row| {
            row.as_array()
                .expect("row")
                .iter()
                .map(|cell| match cell {
                    JsonValue::String(s) => s.clone(),
                    JsonValue::Number(n) => n.to_string(),
                    other => other
                        .get("@id")
                        .or_else(|| other.get("@value"))
                        .map(|v| match v {
                            JsonValue::String(s) => s.clone(),
                            other => other.to_string(),
                        })
                        .unwrap_or_else(|| other.to_string()),
                })
                .collect()
        })
        .collect()
}

#[tokio::test]
async fn imported_turtle_star_claims_are_queryable() {
    let (fluree, ledger) = import_dir(
        &[("claims.ttl", CLAIMS), ("plain.ttl", PLAIN)],
        "it/import-turtle-star:claims",
    )
    .await;

    // Claim-first: every reifier and the edge it reifies.
    let sparql = "PREFIX ex: <http://example.org/>\n\
                  PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n\
                  SELECT ?s ?p ?o ?src WHERE {\n\
                    ?r rdf:reifies <<( ?s ?p ?o )>> .\n\
                    OPTIONAL { ?r ex:source ?src }\n\
                  } ORDER BY ?s ?p ?o";
    let result = support::query_sparql_formatted(&fluree, &ledger, sparql)
        .await
        .expect("claim-first query over an imported ledger");
    let got = rows(&result);
    assert_eq!(got.len(), 4, "{got:#?}");
    let edges: Vec<(String, String)> = got.iter().map(|r| (r[0].clone(), r[2].clone())).collect();
    assert!(
        edges
            .iter()
            .any(|(s, o)| s.ends_with("alice") && o.ends_with("bob")),
        "{edges:?}"
    );
    assert!(
        edges
            .iter()
            .any(|(s, o)| s.ends_with("bob") && o.ends_with("dave")),
        "`<< s p o >>` in subject position reifies the (asserted) base edge: {edges:?}"
    );
    assert!(
        edges.iter().any(|(s, o)| s.ends_with("carol") && o == "42"),
        "literal-object edge: {edges:?}"
    );

    // Inline: the named claim's body.
    let sparql = "PREFIX ex: <http://example.org/>\n\
                  SELECT ?conf WHERE { ex:alice ex:knows ex:bob ~ ex:claim1 {| ex:confidence ?conf |} }";
    let result = support::query_sparql_formatted(&fluree, &ledger, sparql)
        .await
        .expect("inline annotation query");
    assert_eq!(rows(&result), vec![vec!["0.9".to_string()]]);

    // The base edges are ordinary data: one row per edge, never duplicated
    // by their claims.
    let sparql = "PREFIX ex: <http://example.org/>\n\
                  SELECT ?o WHERE { ex:alice ex:knows ?o } ORDER BY ?o";
    let result = support::query_sparql_formatted(&fluree, &ledger, sparql)
        .await
        .expect("plain edge query");
    assert_eq!(rows(&result).len(), 2, "{result:#}");
}
