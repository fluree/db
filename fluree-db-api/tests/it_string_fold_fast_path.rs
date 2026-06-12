//! Per-distinct-string fold fast path (`fluree-db-query/src/fast_string_fold.rs`).
//!
//! Covers the six Sparqloscope timeout shapes: COUNT(*) with REGEX/CONTAINS
//! filters and SUM of STRLEN / STRLEN∘STRBEFORE / STRLEN∘STRAFTER /
//! xsd:integer∘STRENDS over a single predicate. Values include duplicates
//! (exercising the per-distinct cache), multiple language tags, and non-ASCII
//! strings (STRLEN counts codepoints, not bytes).
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

/// `ex:label` rows: "common"@en ×3, "camp"@en, "dotcom"@de, "zebra", "äcom".
/// `ex:mixed`: one string that matches plus one number (the number row is
/// excluded by expression-error semantics, not by declining the fast path).
fn fold_ttl() -> &'static str {
    r#"
@prefix ex: <http://example.org/ns/> .

ex:s1 ex:label "common"@en .
ex:s2 ex:label "common"@en .
ex:s3 ex:label "common"@en .
ex:s4 ex:label "camp"@en .
ex:s5 ex:label "dotcom"@de .
ex:s6 ex:label "zebra" .
ex:s7 ex:label "äcom" .
ex:m1 ex:mixed "com" .
ex:m2 ex:mixed 5 .
"#
}

async fn bulk_import_fold() -> (TempDir, TempDir, fluree_db_api::Fluree, String) {
    let db_dir = TempDir::new().expect("db tmpdir");
    let data_dir = TempDir::new().expect("data tmpdir");

    write_ttl(data_dir.path(), "00-fold.ttl", fold_ttl());

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");

    let ledger_id = "test/string-fold:main".to_string();
    let result = fluree
        .create(&ledger_id)
        .import(data_dir.path())
        .threads(2)
        .memory_budget_mb(256)
        .cleanup(false)
        .execute()
        .await
        .expect("import should succeed");
    assert!(result.t > 0);

    (db_dir, data_dir, fluree, ledger_id)
}

async fn scalar(
    fluree: &fluree_db_api::Fluree,
    ledger: &fluree_db_api::LedgerState,
    sparql: &str,
) -> serde_json::Value {
    let bindings = support::query_sparql(fluree, ledger, sparql)
        .await
        .expect("query")
        .to_sparql_json(&ledger.snapshot)
        .expect("to_sparql_json");
    let arr = bindings["results"]["bindings"]
        .as_array()
        .expect("bindings array");
    assert_eq!(arr.len(), 1, "scalar aggregate should yield one row");
    arr[0]["v"]["value"].clone()
}

/// The six fold shapes with hand-computed expectations. Per value:
/// REGEX "c.m": common✓ camp✓ dotcom✓ zebra✗ äcom✓ → 3+1+1+0+1 = 6 rows
/// CONTAINS "com": common✓ camp✗ dotcom✓ zebra✗ äcom✓ → 3+0+1+0+1 = 5 rows
/// STRLEN: 6·3 + 4 + 6 + 5 + 4 = 37 (äcom = 4 codepoints, not 5 bytes)
/// STRLEN(STRBEFORE(·,"m")): "co"·3 + "ca" + "dotco" + "" + "äco" = 6+2+5+0+3 = 16
/// STRLEN(STRAFTER(·,"m")): "mon"·3 + "p" + "" + "" + "" = 9+1 = 10
/// xsd:integer(STRENDS(·,"m")): dotcom + äcom = 2
fn fold_cases() -> Vec<(&'static str, &'static str)> {
    vec![
        (
            r#"SELECT (COUNT(*) AS ?v) WHERE { ?s ex:label ?o FILTER REGEX(?o, "c.m") }"#,
            "6",
        ),
        (
            r#"SELECT (COUNT(*) AS ?v) WHERE { ?s ex:label ?o FILTER CONTAINS(?o, "com") }"#,
            "5",
        ),
        (
            r"SELECT (SUM(STRLEN(?o)) AS ?v) WHERE { ?s ex:label ?o }",
            "37",
        ),
        (
            r#"SELECT (SUM(STRLEN(STRBEFORE(?o, "m"))) AS ?v) WHERE { ?s ex:label ?o }"#,
            "16",
        ),
        (
            r#"SELECT (SUM(STRLEN(STRAFTER(?o, "m"))) AS ?v) WHERE { ?s ex:label ?o }"#,
            "10",
        ),
        (
            r#"SELECT (SUM(xsd:integer(STRENDS(?o, "m"))) AS ?v) WHERE { ?s ex:label ?o }"#,
            "2",
        ),
    ]
}

const PREFIXES: &str = r"PREFIX ex: <http://example.org/ns/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
";

#[tokio::test]
async fn bulk_import_string_folds() {
    let (_db_dir, _data_dir, fluree, ledger_id) = bulk_import_fold().await;
    let ledger = fluree.ledger(&ledger_id).await.expect("load");

    for (body, expected) in fold_cases() {
        let got = scalar(&fluree, &ledger, &format!("{PREFIXES}{body}")).await;
        assert_eq!(got, expected, "query: {body}");
    }
}

/// Appending a tautological `FILTER(BOUND(?o))` makes the pattern list too
/// long for detection, so the identical aggregate runs through the planned
/// pipeline on the same index — fast path and fallback must agree.
#[tokio::test]
async fn string_folds_agree_with_fallback_pipeline() {
    let (_db_dir, _data_dir, fluree, ledger_id) = bulk_import_fold().await;
    let ledger = fluree.ledger(&ledger_id).await.expect("load");

    for (body, _) in fold_cases() {
        let fast = scalar(&fluree, &ledger, &format!("{PREFIXES}{body}")).await;
        let fallback_body = body.replacen(" }", " FILTER(BOUND(?o)) }", 1);
        assert_ne!(body, fallback_body, "rewrite must change the query");
        let slow = scalar(&fluree, &ledger, &format!("{PREFIXES}{fallback_body}")).await;
        assert_eq!(fast, slow, "fast path must agree with pipeline: {body}");
    }
}

/// Mixed string/numeric predicate: the numeric row errors in the expression
/// (FILTER excludes it; SUM skips it), so it contributes 0 — the fast path
/// skips it the same way rather than declining.
#[tokio::test]
async fn mixed_type_rows_contribute_zero() {
    let (_db_dir, _data_dir, fluree, ledger_id) = bulk_import_fold().await;
    let ledger = fluree.ledger(&ledger_id).await.expect("load");

    let count = scalar(
        &fluree,
        &ledger,
        &format!(
            r#"{PREFIXES}SELECT (COUNT(*) AS ?v) WHERE {{ ?s ex:mixed ?o FILTER CONTAINS(?o, "com") }}"#
        ),
    )
    .await;
    assert_eq!(count, "1", "number row is excluded, not declined");

    let total = scalar(
        &fluree,
        &ledger,
        &format!(r"{PREFIXES}SELECT (SUM(STRLEN(?o)) AS ?v) WHERE {{ ?s ex:mixed ?o }}"),
    )
    .await;
    assert_eq!(total, "3", "STRLEN sums only the string row");
}

/// The fold needs only value equality/adjacency (POST groups equal o_keys),
/// not lex-sorted IDs — it must stay exact on incrementally built indexes.
#[tokio::test]
async fn incremental_index_string_folds_exact() {
    use fluree_db_api::{IndexConfig, LedgerManagerConfig};
    use fluree_db_transact::{CommitOpts, TxnOpts};
    use serde_json::json;
    use std::sync::Arc;

    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/string-fold-incremental:main";

    let (local, handle) = support::start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };
            let ledger = support::genesis_ledger_for_fluree(&fluree, ledger_id);
            let first = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    {"@id": "ex:a", "ex:word": "zebra"},
                    {"@id": "ex:b", "ex:word": "zebra"}
                ]
            });
            let result = fluree
                .insert_with_opts(
                    ledger,
                    &first,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("first insert");
            let second = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    {"@id": "ex:c", "ex:word": "apple"}
                ]
            });
            let result = fluree
                .insert_with_opts(
                    result.ledger,
                    &second,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("second insert");
            let ledger = result.ledger;

            support::trigger_index_and_wait_outcome(&handle, ledger_id, ledger.t()).await;
            let ledger_state = fluree.ledger(ledger_id).await.expect("ledger state");

            // zebra(5)·2 + apple(5) = 15
            let total = scalar(
                &fluree,
                &ledger_state,
                &format!(r"{PREFIXES}SELECT (SUM(STRLEN(?o)) AS ?v) WHERE {{ ?s ex:word ?o }}"),
            )
            .await;
            assert_eq!(total, "15");

            // contains "ebr": zebra ×2
            let count = scalar(
                &fluree,
                &ledger_state,
                &format!(
                    r#"{PREFIXES}SELECT (COUNT(*) AS ?v) WHERE {{ ?s ex:word ?o FILTER CONTAINS(?o, "ebr") }}"#
                ),
            )
            .await;
            assert_eq!(count, "2");
        })
        .await;
}
