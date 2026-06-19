//! Per-predicate `COUNT(DISTINCT ?o)` via the POST lead-group directory walk
//! (`fast_count.rs::count_distinct_objects_for_predicate`).
//!
//! `lead_group_count` counts distinct `(o_type, o_key)` per leaflet, so the
//! count is exact for every object type — langStrings included (the
//! Sparqloscope `distinct-count-object-low-multiplicity` shape) — and needs no
//! value ordering, so it works on incrementally built indexes too.
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

/// `ex:color`: "red"@en ×3 subjects, "red"@de ×2, "blue"@en, plain "red" —
/// 4 distinct terms (language tags and datatypes distinguish literals).
/// `ex:n`: integers with duplicates — 3 distinct. `ex:link`: IRI refs with a
/// duplicate target — 2 distinct.
fn distinct_ttl() -> &'static str {
    r#"
@prefix ex: <http://example.org/ns/> .

ex:s1 ex:color "red"@en, "red"@de ; ex:n 1 ; ex:link ex:t1 .
ex:s2 ex:color "red"@en ; ex:n 1 ; ex:link ex:t1 .
ex:s3 ex:color "red"@en ; ex:n 2 ; ex:link ex:t2 .
ex:s4 ex:color "red"@de ; ex:n 3 .
ex:s5 ex:color "blue"@en .
ex:s6 ex:color "red" .
"#
}

async fn bulk_import_distinct() -> (TempDir, TempDir, fluree_db_api::Fluree, String) {
    let db_dir = TempDir::new().expect("db tmpdir");
    let data_dir = TempDir::new().expect("data tmpdir");

    write_ttl(data_dir.path(), "00-distinct.ttl", distinct_ttl());

    let fluree = FlureeBuilder::file(db_dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");

    let ledger_id = "test/count-distinct-objects:main".to_string();
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

async fn distinct_count(
    fluree: &fluree_db_api::Fluree,
    ledger: &fluree_db_api::LedgerState,
    body: &str,
) -> serde_json::Value {
    let sparql = format!(
        r"PREFIX ex: <http://example.org/ns/>
          SELECT (COUNT(DISTINCT ?o) AS ?count) WHERE {{ {body} }}"
    );
    let bindings = support::query_sparql(fluree, ledger, &sparql)
        .await
        .expect("query")
        .to_sparql_json(&ledger.snapshot)
        .expect("to_sparql_json");
    let arr = bindings["results"]["bindings"]
        .as_array()
        .expect("bindings array");
    assert_eq!(arr.len(), 1, "scalar aggregate should yield one row");
    arr[0]["count"]["value"].clone()
}

#[tokio::test]
async fn bulk_import_distinct_objects_all_types() {
    let (_db_dir, _data_dir, fluree, ledger_id) = bulk_import_distinct().await;
    let ledger = fluree.ledger(&ledger_id).await.expect("load");

    let langs = distinct_count(&fluree, &ledger, "?s ex:color ?o").await;
    assert_eq!(langs, "4", "lang tags and datatype distinguish literals");

    let nums = distinct_count(&fluree, &ledger, "?s ex:n ?o").await;
    assert_eq!(nums, "3", "duplicate numbers dedup");

    let refs = distinct_count(&fluree, &ledger, "?s ex:link ?o").await;
    assert_eq!(refs, "2", "duplicate ref targets dedup");
}

/// `FILTER(BOUND(?o))` is a tautology that disqualifies the fused fast path,
/// so the same aggregate runs through the planned pipeline on the same index.
#[tokio::test]
async fn distinct_objects_agree_with_fallback_pipeline() {
    let (_db_dir, _data_dir, fluree, ledger_id) = bulk_import_distinct().await;
    let ledger = fluree.ledger(&ledger_id).await.expect("load");

    for pred in ["ex:color", "ex:n", "ex:link"] {
        let fast = distinct_count(&fluree, &ledger, &format!("?s {pred} ?o")).await;
        let fallback = distinct_count(
            &fluree,
            &ledger,
            &format!("?s {pred} ?o . FILTER(BOUND(?o))"),
        )
        .await;
        assert_eq!(fast, fallback, "{pred} fast path must agree with pipeline");
    }
}

/// Incremental indexes have insertion-ordered string IDs; distinctness is
/// order-independent so the metadata walk stays exact across commits.
#[tokio::test]
async fn incremental_index_distinct_objects_exact() {
    use fluree_db_api::{IndexConfig, LedgerManagerConfig};
    use fluree_db_transact::{CommitOpts, TxnOpts};
    use serde_json::json;

    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/count-distinct-incremental:main";

    let (local, handle) = support::start_background_indexer_local(
        fluree.backend().clone(),
        fluree
            .nameservice_mode()
            .as_arc_indexing_nameservice()
            .expect("test fluree has writable nameservice"),
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
                    {"@id": "ex:b", "ex:word": "zebra"},
                    {"@id": "ex:c", "ex:word": "mango"}
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
                    {"@id": "ex:d", "ex:word": "zebra"},
                    {"@id": "ex:e", "ex:word": "apple"}
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

            let count = distinct_count(&fluree, &ledger_state, "?s ex:word ?o").await;
            assert_eq!(count, "3", "zebra/mango/apple across two commits");
        })
        .await;
}
