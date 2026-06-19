//! MIN/MAX string fast path (`fluree-db-query/src/fast_min_max_string.rs`).
//!
//! Bulk imports assign string dictionary IDs in lexicographic order
//! (`lex_sorted_string_ids`), which the fast path relies on to answer
//! `SELECT (MIN(?o) AS ?min) { ?s <p> ?o }` from POST leaflet metadata —
//! including predicates whose objects span many language tags (the
//! Sparqloscope `group-by-implicit-string-min` shape).
//!
//! Incrementally built indexes do NOT have lex-sorted string IDs; the fast
//! path must decline there and the planned pipeline must still produce
//! value-ordered results.
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

/// `ex:desc` carries langStrings across several languages (mixed o_types in
/// one leaflet exercises the column-scan branch); `ex:name` carries plain
/// xsd:strings (homogeneous leaflet exercises the boundary-key branch);
/// `ex:mixed` carries a string and a number (fast path must decline).
fn multilang_ttl() -> &'static str {
    r#"
@prefix ex: <http://example.org/ns/> .

ex:s1 ex:desc "banana"@en ; ex:name "Mango" ; ex:mixed "apple" .
ex:s2 ex:desc "cherry"@en ; ex:name "Apricot" .
ex:s3 ex:desc "apfel"@de ; ex:name "Zucchini" .
ex:s4 ex:desc "zwiebel"@de ; ex:name "Banana" .
ex:s5 ex:desc "abricot"@fr ; ex:mixed 5 .
ex:s6 ex:desc "tomate"@fr .
"#
}

async fn bulk_import_multilang() -> (TempDir, TempDir, fluree_db_api::Fluree, String) {
    let db_dir = TempDir::new().expect("db tmpdir");
    let data_dir = TempDir::new().expect("data tmpdir");

    write_ttl(data_dir.path(), "00-multilang.ttl", multilang_ttl());

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");

    let ledger_id = "test/minmax-multilang:main".to_string();
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

async fn run_scalar(
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
    arr[0].clone()
}

#[tokio::test]
async fn bulk_import_multilang_min_max() {
    let (_db_dir, _data_dir, fluree, ledger_id) = bulk_import_multilang().await;
    let ledger = fluree.ledger(&ledger_id).await.expect("load");

    let row = run_scalar(
        &fluree,
        &ledger,
        r"PREFIX ex: <http://example.org/ns/>
          SELECT (MIN(?o) AS ?min) WHERE { ?s ex:desc ?o }",
    )
    .await;
    assert_eq!(row["min"]["value"], "abricot", "MIN spans language groups");
    assert_eq!(row["min"]["xml:lang"], "fr");

    let row = run_scalar(
        &fluree,
        &ledger,
        r"PREFIX ex: <http://example.org/ns/>
          SELECT (MAX(?o) AS ?max) WHERE { ?s ex:desc ?o }",
    )
    .await;
    assert_eq!(row["max"]["value"], "zwiebel", "MAX spans language groups");
    assert_eq!(row["max"]["xml:lang"], "de");
}

#[tokio::test]
async fn bulk_import_plain_string_min_max() {
    let (_db_dir, _data_dir, fluree, ledger_id) = bulk_import_multilang().await;
    let ledger = fluree.ledger(&ledger_id).await.expect("load");

    let row = run_scalar(
        &fluree,
        &ledger,
        r"PREFIX ex: <http://example.org/ns/>
          SELECT (MIN(?o) AS ?min) WHERE { ?s ex:name ?o }",
    )
    .await;
    assert_eq!(row["min"]["value"], "Apricot");

    let row = run_scalar(
        &fluree,
        &ledger,
        r"PREFIX ex: <http://example.org/ns/>
          SELECT (MAX(?o) AS ?max) WHERE { ?s ex:name ?o }",
    )
    .await;
    assert_eq!(row["max"]["value"], "Zucchini");
}

/// The fast path and the planned pipeline must agree. `FILTER(BOUND(?o))` is a
/// tautology that disqualifies the query shape from the fused fast path, so
/// this runs the same aggregate through the fallback on the same index.
#[tokio::test]
async fn fast_path_agrees_with_fallback_pipeline() {
    let (_db_dir, _data_dir, fluree, ledger_id) = bulk_import_multilang().await;
    let ledger = fluree.ledger(&ledger_id).await.expect("load");

    for (agg, var) in [("MIN", "min"), ("MAX", "max")] {
        let fast = run_scalar(
            &fluree,
            &ledger,
            &format!(
                r"PREFIX ex: <http://example.org/ns/>
                  SELECT ({agg}(?o) AS ?{var}) WHERE {{ ?s ex:desc ?o }}"
            ),
        )
        .await;
        let fallback = run_scalar(
            &fluree,
            &ledger,
            &format!(
                r"PREFIX ex: <http://example.org/ns/>
                  SELECT ({agg}(?o) AS ?{var}) WHERE {{ ?s ex:desc ?o . FILTER(BOUND(?o)) }}"
            ),
        )
        .await;
        assert_eq!(
            fast[var]["value"], fallback[var]["value"],
            "{agg} fast path must agree with the planned pipeline"
        );
    }
}

/// Incrementally built indexes assign string dictionary IDs in insertion
/// order, not lex order (`lex_sorted_string_ids = false`), so the fast path
/// must decline. Inserting values in reverse lex order makes ID order disagree
/// with value order: any path that trusts raw IDs returns "zebra" for MIN.
#[tokio::test]
async fn incremental_index_min_max_is_value_ordered() {
    use fluree_db_api::{IndexConfig, LedgerManagerConfig};
    use fluree_db_transact::{CommitOpts, TxnOpts};
    use serde_json::json;

    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/minmax-incremental:main";

    let (local, handle) = support::start_background_indexer_local(
        fluree.backend().clone(),
        fluree.nameservice_mode().as_arc_indexing_nameservice().expect("test fluree has writable nameservice"),
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
                    {"@id": "ex:b", "ex:word": "mango"}
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

            let row = run_scalar(
                &fluree,
                &ledger_state,
                r"PREFIX ex: <http://example.org/ns/>
                  SELECT (MIN(?o) AS ?min) WHERE { ?s ex:word ?o }",
            )
            .await;
            assert_eq!(
                row["min"]["value"], "apple",
                "MIN on an incrementally indexed ledger must be value-ordered"
            );

            let row = run_scalar(
                &fluree,
                &ledger_state,
                r"PREFIX ex: <http://example.org/ns/>
                  SELECT (MAX(?o) AS ?max) WHERE { ?s ex:word ?o }",
            )
            .await;
            assert_eq!(
                row["max"]["value"], "zebra",
                "MAX on an incrementally indexed ledger must be value-ordered"
            );
        })
        .await;
}

/// MIN/MAX grouped alongside a non-streamable aggregate (GROUP_CONCAT) route
/// to the traditional GroupBy+Aggregate path, whose comparator ordered encoded
/// bindings by raw dictionary ID — insertion order, not value order. With
/// values inserted in reverse lex order this returned min="zebra"/max="apple"
/// for strings and min=ex:zzz/max=ex:aaa for IRIs.
#[tokio::test]
async fn min_max_value_ordered_alongside_group_concat() {
    use fluree_db_api::{IndexConfig, LedgerManagerConfig};
    use fluree_db_transact::{CommitOpts, TxnOpts};
    use serde_json::json;

    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/minmax-traditional-groupby:main";

    let (local, handle) = support::start_background_indexer_local(
        fluree.backend().clone(),
        fluree.nameservice_mode().as_arc_indexing_nameservice().expect("test fluree has writable nameservice"),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };
            let ledger = support::genesis_ledger_for_fluree(&fluree, ledger_id);
            // Reverse lex order: zebra/zzz get LOWER dictionary ids.
            let first = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    {"@id": "ex:s", "ex:word": "zebra", "ex:link": {"@id": "ex:zzz"}}
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
                    {"@id": "ex:s", "ex:word": "apple", "ex:link": {"@id": "ex:aaa"}}
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
            support::trigger_index_and_wait_outcome(&handle, ledger_id, result.ledger.t()).await;
            let ledger = fluree.ledger(ledger_id).await.expect("ledger state");

            for (pred, expect_min, expect_max) in [
                ("ex:word", json!("apple"), json!("zebra")),
                ("ex:link", json!("ex:aaa"), json!("ex:zzz")),
            ] {
                let q = format!(
                    r#"PREFIX ex: <http://example.org/ns/>
                       SELECT ?s (MIN(?o) AS ?min) (MAX(?o) AS ?max)
                              (GROUP_CONCAT(?o; SEPARATOR=",") AS ?cat)
                       WHERE {{ ?s {pred} ?o }} GROUP BY ?s"#
                );
                let r = support::query_sparql(&fluree, &ledger, &q)
                    .await
                    .expect("query")
                    .to_sparql_json(&ledger.snapshot)
                    .expect("json");
                let b = &r["results"]["bindings"][0];
                assert_eq!(b["min"]["value"], expect_min, "{pred} MIN");
                assert_eq!(b["max"]["value"], expect_max, "{pred} MAX");
            }
        })
        .await;
}

/// Commits above the index head (novelty) must be reflected: `to_t > max_t`
/// fails the fast-path gates, so MIN/MAX, COUNT(DISTINCT), and the string
/// folds all fall back to the overlay-aware pipeline.
#[tokio::test]
async fn novelty_above_index_head_is_visible() {
    use fluree_db_api::{IndexConfig, LedgerManagerConfig};
    use fluree_db_transact::{CommitOpts, TxnOpts};
    use serde_json::json;

    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/minmax-novelty:main";

    let (local, handle) = support::start_background_indexer_local(
        fluree.backend().clone(),
        fluree.nameservice_mode().as_arc_indexing_nameservice().expect("test fluree has writable nameservice"),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let index_cfg = IndexConfig {
                reindex_min_bytes: 0,
                reindex_max_bytes: 10_000_000,
            };
            let ledger = support::genesis_ledger_for_fluree(&fluree, ledger_id);
            let indexed = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    {"@id": "ex:a", "ex:word": "zebra"},
                    {"@id": "ex:b", "ex:word": "mango"}
                ]
            });
            let result = fluree
                .insert_with_opts(
                    ledger,
                    &indexed,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("indexed insert");
            support::trigger_index_and_wait_outcome(&handle, ledger_id, result.ledger.t()).await;

            // This commit stays ABOVE the index head — no reindex wait.
            let novelty = json!({
                "@context": { "ex": "http://example.org/ns/" },
                "@graph": [
                    {"@id": "ex:c", "ex:word": "apple"}
                ]
            });
            fluree
                .insert_with_opts(
                    result.ledger,
                    &novelty,
                    TxnOpts::default(),
                    CommitOpts::default(),
                    &index_cfg,
                )
                .await
                .expect("novelty insert");
            let ledger_state = fluree.ledger(ledger_id).await.expect("ledger state");

            let row = run_scalar(
                &fluree,
                &ledger_state,
                r"PREFIX ex: <http://example.org/ns/>
                  SELECT (MIN(?o) AS ?min) WHERE { ?s ex:word ?o }",
            )
            .await;
            assert_eq!(row["min"]["value"], "apple", "MIN must see novelty");

            let row = run_scalar(
                &fluree,
                &ledger_state,
                r"PREFIX ex: <http://example.org/ns/>
                  SELECT (COUNT(DISTINCT ?o) AS ?count) WHERE { ?s ex:word ?o }",
            )
            .await;
            assert_eq!(
                row["count"]["value"], "3",
                "COUNT(DISTINCT) must see novelty"
            );

            // zebra(5) + mango(5) + apple(5)
            let row = run_scalar(
                &fluree,
                &ledger_state,
                r"PREFIX ex: <http://example.org/ns/>
                  SELECT (SUM(STRLEN(?o)) AS ?sum) WHERE { ?s ex:word ?o }",
            )
            .await;
            assert_eq!(row["sum"]["value"], "15", "SUM(STRLEN) must see novelty");

            let row = run_scalar(
                &fluree,
                &ledger_state,
                r#"PREFIX ex: <http://example.org/ns/>
                  SELECT (COUNT(*) AS ?c) WHERE { ?s ex:word ?o FILTER CONTAINS(?o, "a") }"#,
            )
            .await;
            assert_eq!(row["c"]["value"], "3", "COUNT(CONTAINS) must see novelty");
        })
        .await;
}

/// Mixed string/numeric objects: the string path declines (non-string-dict
/// candidate) and the numeric path declines (non-numeric leaflet), so the
/// planned pipeline answers. Just assert it returns a single bound row.
#[tokio::test]
async fn mixed_type_predicate_falls_back() {
    let (_db_dir, _data_dir, fluree, ledger_id) = bulk_import_multilang().await;
    let ledger = fluree.ledger(&ledger_id).await.expect("load");

    let row = run_scalar(
        &fluree,
        &ledger,
        r"PREFIX ex: <http://example.org/ns/>
          SELECT (MIN(?o) AS ?min) WHERE { ?s ex:mixed ?o }",
    )
    .await;
    assert!(
        row["min"]["value"].is_string(),
        "mixed-type MIN should still bind a value, got {row}"
    );
}
