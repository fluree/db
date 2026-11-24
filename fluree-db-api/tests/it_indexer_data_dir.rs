//! An instance's index builds stage under its indexer data directory, set by
//! `FlureeBuilder::with_indexer_data_dir` (the server's `--indexer-data-dir`)
//! or by the `data_dir` of `with_indexer_config`: the background worker's
//! builds, a reindex and `build_index`, with or without background indexing.
//!
//! A full rebuild leaves `{data_dir}/{name}/{branch}/tmp_import` behind once
//! it has removed its session directory, which is how these tests see where
//! it staged.

#![cfg(feature = "native")]

use fluree_db_api::{Fluree, FlureeBuilder, ReindexOptions, TriggerIndexOptions};
use serde_json::json;
use std::path::Path;

async fn seed(fluree: &Fluree, ledger_id: &str) {
    let ledger = fluree
        .create_ledger(ledger_id)
        .await
        .expect("create ledger");
    let tx = json!({
        "@context": { "ex": "http://example.org/" },
        "@id": "ex:alice",
        "ex:name": "Alice",
    });
    fluree.insert(ledger, &tx).await.expect("insert");
}

/// Whether a rebuild of `name`'s `main` branch staged under `data_dir`.
fn staged_under(data_dir: &Path, name: &str) -> bool {
    data_dir.join(name).join("main").join("tmp_import").is_dir()
}

#[tokio::test]
async fn reindex_stages_under_the_builders_data_dir() {
    let staging = tempfile::tempdir().expect("tempdir");
    let fluree = FlureeBuilder::memory()
        .with_indexer_data_dir(staging.path())
        .build_memory();
    assert_eq!(fluree.indexer_data_dir(), Some(staging.path()));
    seed(&fluree, "it/data-dir-reindex:main").await;

    fluree
        .reindex("it/data-dir-reindex:main", ReindexOptions::default())
        .await
        .expect("reindex");
    assert!(staged_under(staging.path(), "it/data-dir-reindex"));
}

#[tokio::test]
async fn the_data_dir_outlives_without_indexing() {
    let storage = tempfile::tempdir().expect("tempdir");
    let staging = tempfile::tempdir().expect("tempdir");
    let fluree = FlureeBuilder::file(storage.path().to_string_lossy().to_string())
        .with_indexer_data_dir(staging.path())
        .without_indexing()
        .build()
        .expect("build");
    seed(&fluree, "it/data-dir-unindexed:main").await;

    fluree
        .reindex("it/data-dir-unindexed:main", ReindexOptions::default())
        .await
        .expect("reindex");
    assert!(staged_under(staging.path(), "it/data-dir-unindexed"));
}

#[tokio::test]
async fn a_data_dir_in_the_indexer_config_reaches_a_reindex() {
    let staging = tempfile::tempdir().expect("tempdir");
    let fluree = FlureeBuilder::memory()
        .with_indexer_config(
            fluree_db_indexer::IndexerConfig::default().with_data_dir(staging.path()),
        )
        .build_memory();
    seed(&fluree, "it/data-dir-config:main").await;

    fluree
        .reindex("it/data-dir-config:main", ReindexOptions::default())
        .await
        .expect("reindex");
    assert!(staged_under(staging.path(), "it/data-dir-config"));
}

#[tokio::test]
async fn build_index_stages_under_the_data_dir() {
    let staging = tempfile::tempdir().expect("tempdir");
    let fluree = FlureeBuilder::memory()
        .with_indexer_data_dir(staging.path())
        .build_memory();
    seed(&fluree, "it/data-dir-build:main").await;

    // No index yet, so this build is a full rebuild.
    fluree
        .build_index("it/data-dir-build:main")
        .await
        .expect("build_index");
    assert!(staged_under(staging.path(), "it/data-dir-build"));
}

#[tokio::test]
async fn the_background_worker_stages_under_the_builders_data_dir() {
    let storage = tempfile::tempdir().expect("tempdir");
    let staging = tempfile::tempdir().expect("tempdir");
    let fluree = FlureeBuilder::file(storage.path().to_string_lossy().to_string())
        .with_indexer_data_dir(staging.path())
        .build()
        .expect("build");
    seed(&fluree, "it/data-dir-worker:main").await;

    // The ledger's first index is a full rebuild.
    fluree
        .trigger_index("it/data-dir-worker:main", TriggerIndexOptions::default())
        .await
        .expect("trigger_index");
    assert!(staged_under(staging.path(), "it/data-dir-worker"));
}
