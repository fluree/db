//! Verifies the MIN/MAX string and COUNT(DISTINCT) fast paths actually serve
//! (not just agree with) aggregates over a multi-language predicate on a
//! bulk-imported (lex-sorted) index.
//!
//! Kept as the only test in this binary: the assertion relies on a thread-local
//! tracing subscriber (`set_default`), and concurrent tests in the same process
//! push parts of query execution onto threads the subscriber can't see.
#![cfg(feature = "native")]

mod support;

use fluree_db_api::FlureeBuilder;
use std::io::Write;
use tempfile::TempDir;

#[tokio::test(flavor = "current_thread")]
async fn multilang_min_served_by_fast_path() {
    let db_dir = TempDir::new().expect("db tmpdir");
    let data_dir = TempDir::new().expect("data tmpdir");

    let ttl = r#"
@prefix ex: <http://example.org/ns/> .

ex:s1 ex:desc "banana"@en .
ex:s2 ex:desc "cherry"@en .
ex:s3 ex:desc "apfel"@de .
ex:s4 ex:desc "zwiebel"@de .
ex:s5 ex:desc "abricot"@fr .
ex:s6 ex:desc "tomate"@fr .
"#;
    let path = data_dir.path().join("00-multilang.ttl");
    let mut f = std::fs::File::create(&path).expect("create ttl");
    f.write_all(ttl.as_bytes()).expect("write ttl");

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");
    let ledger_id = "test/minmax-fired:main";
    fluree
        .create(ledger_id)
        .import(data_dir.path())
        .threads(1)
        .memory_budget_mb(256)
        .cleanup(false)
        .execute()
        .await
        .expect("import");
    let ledger = fluree.ledger(ledger_id).await.expect("load");

    let (store, _guard) = support::span_capture::init_test_tracing();

    let result = support::query_sparql(
        &fluree,
        &ledger,
        r"PREFIX ex: <http://example.org/ns/>
          SELECT (MIN(?o) AS ?min) WHERE { ?s ex:desc ?o }",
    )
    .await
    .expect("query")
    .to_sparql_json(&ledger.snapshot)
    .expect("to_sparql_json");
    assert_eq!(result["results"]["bindings"][0]["min"]["value"], "abricot");

    let distinct = support::query_sparql(
        &fluree,
        &ledger,
        r"PREFIX ex: <http://example.org/ns/>
          SELECT (COUNT(DISTINCT ?o) AS ?count) WHERE { ?s ex:desc ?o }",
    )
    .await
    .expect("distinct query")
    .to_sparql_json(&ledger.snapshot)
    .expect("to_sparql_json");
    assert_eq!(distinct["results"]["bindings"][0]["count"]["value"], "6");

    let served: Vec<String> = store
        .find_events("fast path produced result")
        .iter()
        .filter_map(|e| e.fields.get("label").cloned())
        .collect();
    assert!(
        served.iter().any(|l| l.contains("MIN/MAX")),
        "expected the MIN/MAX string fast path to serve; served: {served:?}"
    );
    assert!(
        served.iter().any(|l| l.contains("COUNT(DISTINCT)")),
        "expected the COUNT(DISTINCT) lead-group walk to serve; served: {served:?}"
    );
    assert!(
        !store.has_event("fast path declined; running fallback"),
        "no fast path should decline on a lex-sorted multi-language predicate"
    );
}
