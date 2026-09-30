//! The RDF 1.2 link form on the bulk-import path.
//!
//! Alongside the `f:reifies*` bundle, import writes one `rdf:reifies` flake
//! per reifier whose object is a triple-term handle, and persists the term
//! dictionary that gives the handle its meaning. Reading the link back
//! through a query exercises the whole chain: sink → chunk term table →
//! global remap and interning → dictionary upload → root section → store
//! load → handle decode.

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

async fn import(files: &[(&str, &str)], alias: &str) -> (fluree_db_api::Fluree, LedgerState) {
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
                            v => v.to_string(),
                        })
                        .unwrap_or_else(|| other.to_string()),
                })
                .collect()
        })
        .collect()
}

#[tokio::test]
async fn imported_reifiers_carry_a_decodable_triple_term_link() {
    let (fluree, ledger) = import(&[("claims.ttl", CLAIMS)], "it/triple-term-links:claims").await;

    // One link per reifier, and each link's object decodes through the term
    // dictionary to the base edge it reifies.
    let sparql = "PREFIX ex: <http://example.org/>\n\
                  PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n\
                  SELECT ?r ?t WHERE { ?r rdf:reifies ?t } ORDER BY ?r";
    let result = support::query_sparql_formatted(&fluree, &ledger, sparql)
        .await
        .expect("link query over an imported ledger");
    let got = rows(&result);
    assert_eq!(got.len(), 4, "{got:#?}");

    let term_of = |reifier: &str| -> String {
        got.iter()
            .find(|r| r[0].ends_with(reifier))
            .unwrap_or_else(|| panic!("no link for {reifier}: {got:#?}"))[1]
            .clone()
    };
    let t1 = term_of("claim1");
    assert!(
        t1.contains("alice") && t1.contains("knows") && t1.contains("bob"),
        "claim1 must reify alice knows bob: {t1}"
    );
    let t2 = term_of("claim2");
    assert!(
        t2.contains("carol") && t2.contains("age") && t2.contains("42"),
        "claim2 must reify carol age 42 (literal object): {t2}"
    );

    // While the link form is index-internal, wildcard scans keep hiding it
    // exactly as they hide the `f:reifies*` bundle.
    let sparql = "PREFIX ex: <http://example.org/>\n\
                  SELECT ?p WHERE { ex:claim1 ?p ?o } ORDER BY ?p";
    let result = support::query_sparql_formatted(&fluree, &ledger, sparql)
        .await
        .expect("wildcard predicate scan");
    let preds: Vec<String> = rows(&result).into_iter().map(|r| r[0].clone()).collect();
    assert!(
        !preds.iter().any(|p| p.contains("reifies")),
        "wildcard scan must hide rdf:reifies and f:reifies*: {preds:?}"
    );
    assert!(
        preds.iter().any(|p| p.ends_with("confidence")),
        "the claim body stays visible: {preds:?}"
    );
}
