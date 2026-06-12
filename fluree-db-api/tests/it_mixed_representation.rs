//! Mixed binding-representation equality across operators.
//!
//! On a fully-indexed ledger, scans emit late-materialized encoded bindings
//! (`EncodedSid`/`EncodedLit`) while VALUES/UNION branches emit decoded ones
//! (`Sid`/`Lit`) — and `Binding` equality/hashing is structural, so the same
//! value in two representations failed to dedup, group, join, or eliminate.
//! Equality surfaces now normalize decoded bindings to their encoded form
//! (`object_binding::encoded_equivalent`) before keying.
#![cfg(feature = "native")]

mod support;

use fluree_db_api::FlureeBuilder;
use std::io::Write;
use tempfile::TempDir;

fn write_ttl(dir: &std::path::Path, name: &str, content: &str) -> std::path::PathBuf {
    let path = dir.join(name);
    let mut f = std::fs::File::create(&path).expect("create ttl file");
    f.write_all(content.as_bytes()).expect("write ttl");
    path
}

async fn bulk_import() -> (TempDir, TempDir, fluree_db_api::Fluree, String) {
    let db_dir = TempDir::new().expect("db tmpdir");
    let data_dir = TempDir::new().expect("data tmpdir");

    write_ttl(
        data_dir.path(),
        "00-data.ttl",
        r#"
@prefix ex: <http://example.org/ns/> .
ex:a a ex:T ; ex:name "alice" ; ex:n 5 .
ex:b a ex:T .
"#,
    );

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");

    let ledger_id = "test/mixed-repr:main".to_string();
    let result = fluree
        .create(&ledger_id)
        .import(data_dir.path())
        .threads(1)
        .memory_budget_mb(128)
        .cleanup(false)
        .execute()
        .await
        .expect("import should succeed");
    assert!(result.t > 0);

    (db_dir, data_dir, fluree, ledger_id)
}

async fn rows(
    fluree: &fluree_db_api::Fluree,
    ledger: &fluree_db_api::LedgerState,
    sparql: &str,
) -> Vec<serde_json::Value> {
    let r = support::query_sparql(fluree, ledger, sparql)
        .await
        .expect("query")
        .to_sparql_json(&ledger.snapshot)
        .expect("to_sparql_json");
    r["results"]["bindings"].as_array().expect("array").clone()
}

const P: &str = "PREFIX ex: <http://example.org/ns/>\n";

/// Each case unions an indexed scan (encoded bindings) with a VALUES branch
/// (decoded bindings) carrying the SAME value, then exercises one equality
/// surface. `(name, expected_row_count, query)`.
#[tokio::test]
async fn mixed_representation_equality_surfaces() {
    let (_db_dir, _data_dir, fluree, ledger_id) = bulk_import().await;
    let ledger = fluree.ledger(&ledger_id).await.expect("load");

    let cases: Vec<(&str, usize, String)> = vec![
        (
            "DISTINCT dedups IRIs across representations",
            2,
            format!(
                r"{P}SELECT DISTINCT ?x {{ {{ ?x a ex:T }} UNION {{ VALUES ?x {{ ex:a }} }} }}"
            ),
        ),
        (
            "DISTINCT dedups strings across representations",
            1,
            format!(
                r#"{P}SELECT DISTINCT ?n {{ {{ ex:a ex:name ?n }} UNION {{ VALUES ?n {{ "alice" }} }} }}"#
            ),
        ),
        // NOTE: integers are excluded — `VALUES ?n { 5 }` lowers to
        // xsd:long while the stored literal materializes as xsd:integer, so
        // they are distinct terms regardless of representation (a separate
        // datatype-canonicalization question, reproducible decoded-vs-decoded).
        (
            "GROUP BY merges representations into one group",
            2,
            format!(
                r"{P}SELECT ?x (COUNT(*) AS ?c) {{ {{ ?x a ex:T }} UNION {{ VALUES ?x {{ ex:a }} }} }} GROUP BY ?x"
            ),
        ),
        (
            "MINUS eliminates a decoded value from encoded input",
            1,
            format!(r"{P}SELECT ?x {{ ?x a ex:T MINUS {{ VALUES ?x {{ ex:a }} }} }}"),
        ),
        (
            "MINUS eliminates an encoded value from decoded input",
            0,
            format!(r"{P}SELECT ?x {{ VALUES ?x {{ ex:a }} MINUS {{ ?x a ex:T }} }}"),
        ),
        (
            "EXISTS matches across representations",
            1,
            format!(
                r"{P}SELECT DISTINCT ?x {{ {{ ?x a ex:T }} UNION {{ VALUES ?x {{ ex:a }} }} FILTER EXISTS {{ ?x ex:name ?n }} }}"
            ),
        ),
        (
            "NOT EXISTS eliminates across representations",
            1,
            format!(
                r"{P}SELECT DISTINCT ?x {{ {{ ?x a ex:T }} UNION {{ VALUES ?x {{ ex:a }} }} FILTER NOT EXISTS {{ ?x ex:name ?n }} }}"
            ),
        ),
    ];

    for (name, expected, q) in cases {
        let got = rows(&fluree, &ledger, &q).await;
        assert_eq!(got.len(), expected, "{name}: rows {got:?}");
    }
}

/// COUNT(DISTINCT) over a mixed-representation stream must count values, not
/// representations, and the per-group counts must merge.
#[tokio::test]
async fn mixed_representation_count_distinct_and_group_counts() {
    let (_db_dir, _data_dir, fluree, ledger_id) = bulk_import().await;
    let ledger = fluree.ledger(&ledger_id).await.expect("load");

    let cd = rows(
        &fluree,
        &ledger,
        &format!(
            r"{P}SELECT (COUNT(DISTINCT ?x) AS ?c) {{ {{ ?x a ex:T }} UNION {{ VALUES ?x {{ ex:a }} }} }}"
        ),
    )
    .await;
    assert_eq!(cd[0]["c"]["value"], "2", "COUNT(DISTINCT) counts values");

    let groups = rows(
        &fluree,
        &ledger,
        &format!(
            r"{P}SELECT ?x (COUNT(*) AS ?c) {{ {{ ?x a ex:T }} UNION {{ VALUES ?x {{ ex:a }} }} }} GROUP BY ?x ORDER BY ?x"
        ),
    )
    .await;
    let a_count = groups
        .iter()
        .find(|g| g["x"]["value"] == "ex:a")
        .expect("ex:a group");
    assert_eq!(a_count["c"]["value"], "2", "ex:a rows merge into one group");
}
