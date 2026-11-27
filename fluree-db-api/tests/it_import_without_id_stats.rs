//! An import that skips ID statistics leaves the ledger without them, and a
//! later incremental build keeps them missing rather than counting only the
//! facts after the import: counts read every fact.

use crate::support;
use fluree_db_api::FlureeBuilder;
use serde_json::json;
use tempfile::TempDir;

const LEDGER: &str = "it/import-without-id-stats:main";

async fn count(
    fluree: &fluree_db_api::Fluree,
    ledger: &fluree_db_api::LedgerState,
    body: &str,
) -> serde_json::Value {
    let sparql =
        format!("PREFIX ex: <http://example.org/ns/> SELECT (COUNT(*) AS ?n) WHERE {{ {body} }}");
    let rows = support::query_sparql(fluree, ledger, &sparql)
        .await
        .expect("query")
        .to_sparql_json(&ledger.snapshot)
        .expect("format");
    rows["results"]["bindings"][0]["n"]["value"].clone()
}

#[tokio::test]
async fn statistics_an_import_skipped_stay_missing_after_later_writes() {
    let db_dir = TempDir::new().unwrap();
    let data_dir = TempDir::new().unwrap();
    let facts: String = (0..100)
        .map(|i| format!("ex:s{i} a ex:T ; ex:p {i} .\n"))
        .collect();
    std::fs::write(
        data_dir.path().join("data.ttl"),
        format!("@prefix ex: <http://example.org/ns/> .\n{facts}"),
    )
    .unwrap();
    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .without_indexing()
        .build()
        .unwrap();
    fluree
        .create(LEDGER)
        .import(data_dir.path())
        .collect_id_stats(false)
        .execute()
        .await
        .expect("import");

    let ledger = fluree.ledger(LEDGER).await.unwrap();
    fluree
        .insert(
            ledger,
            &json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@id": "ex:s100", "@type": "ex:T", "ex:p": 100
            }),
        )
        .await
        .unwrap();
    support::build_and_publish_index(&fluree, LEDGER).await;

    let ledger = fluree.ledger(LEDGER).await.unwrap();
    assert_eq!(ledger.snapshot.t, ledger.t(), "the write is indexed");
    assert!(
        ledger.snapshot.stats.is_none(),
        "the build has no statistics of the import to add to"
    );
    assert_eq!(count(&fluree, &ledger, "?s ex:p ?o").await, json!("101"));
    assert_eq!(count(&fluree, &ledger, "?s a ex:T").await, json!("101"));
}
