//! Bulk import applies no lifetimes: its commits are not marked as carrying
//! retention input, so a lifetime in imported data would never expire
//! anything. An import holding one is refused, in every source form, and
//! leaves no ledger behind.

use fluree_db_api::{FlureeBuilder, ImportError};

const TURTLE: &str = r#"@prefix ex: <http://example.org/> .
@prefix f: <https://ns.flur.ee/db#> .
ex:device1 ex:status "online" {| f:ttl 60 |} .
"#;

const TRIG_GRAPH: &str = r#"@prefix ex: <http://example.org/> .
@prefix f: <https://ns.flur.ee/db#> .
GRAPH ex:devices { ex:device1 ex:status "online" {| f:expiresAt "2030-01-01T00:00:00Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> |} . }
"#;

const TRIG_TXN_META: &str = r#"@prefix ex: <http://example.org/> .
@prefix f: <https://ns.flur.ee/db#> .
ex:device1 ex:status "online" .
GRAPH <#txn-meta> { <fluree:commit:this> f:ttl 60 . }
"#;

const JSONLD: &str = r#"{"@context": {"ex": "http://example.org/"},
 "@id": "ex:device1", "ex:status": {"@value": "online", "@ttl": 60}}"#;

#[tokio::test]
async fn an_import_holding_a_lifetime_is_refused() {
    let cases = [
        ("an annotation", "data.ttl", TURTLE),
        ("an annotation in a named graph", "data.trig", TRIG_GRAPH),
        ("a transaction default", "data.trig", TRIG_TXN_META),
        ("JSON-LD shorthand", "data.jsonld", JSONLD),
    ];
    let mut failures = Vec::new();
    for (i, (what, file, body)) in cases.into_iter().enumerate() {
        let dir = tempfile::tempdir().unwrap();
        let source = tempfile::tempdir().unwrap();
        std::fs::write(source.path().join(file), body).unwrap();
        let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
            .without_indexing()
            .build()
            .unwrap();
        let ledger = format!("it/import-ttl-{i}:main");
        let outcome = fluree
            .create(&ledger)
            .import(source.path())
            .threads(1)
            .memory_budget_mb(256)
            .execute()
            .await;
        match outcome {
            Err(ImportError::Refused(why)) if why.contains("does not apply lifetimes") => {}
            other => failures.push(format!("{what}: {:?}", other.map(|r| r.t))),
        }
        if fluree.ledger(&ledger).await.is_ok() {
            failures.push(format!("{what}: the refused import left a ledger"));
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}
