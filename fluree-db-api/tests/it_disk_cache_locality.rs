//! The disk artifact cache serves remote storage only: a copy of a local file,
//! or of bytes already in memory, can never be cheaper than the read it would
//! replace.

#![cfg(feature = "native")]

use crate::support;
use fluree_db_api::tx::IndexingMode;
use fluree_db_api::{
    BackgroundIndexerWorker, EncryptedStorage, EncryptionKey, Fluree, FlureeBuilder, IndexerConfig,
    LedgerManagerConfig, NameServiceMode, StaticKeyProvider, TriggerIndexOptions,
};
use fluree_db_binary_index::format::index_root::IndexRoot;
use fluree_db_connection::config::ConnectionConfig;
use fluree_db_core::{ContentId, ContentStore, MemoryStorage, Storage, StorageBackend};
use fluree_db_nameservice::memory::MemoryNameService;
use serde_json::json;
use std::path::{Path, PathBuf};
use std::sync::Arc;

fn files_under(dir: &Path) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    entries
        .flatten()
        .flat_map(|entry| {
            let path = entry.path();
            if path.is_dir() {
                files_under(&path)
            } else {
                vec![path]
            }
        })
        .collect()
}

fn person(i: usize) -> serde_json::Value {
    json!({
        "@context": {"ex": "http://example.org/"},
        "@id": format!("ex:p{i}"),
        "@type": "ex:Person",
        "ex:name": format!("Person {i}")
    })
}

async fn assert_people(fluree: &Fluree, ledger_id: &str, expected: usize) {
    let ledger = fluree.ledger(ledger_id).await.expect("load");
    let rows = support::query_jsonld_formatted(
        fluree,
        &ledger,
        &json!({
            "@context": {"ex": "http://example.org/"},
            "select": "?name",
            "where": {"@id": "?s", "@type": "ex:Person", "ex:name": "?name"}
        }),
    )
    .await
    .expect("query");
    assert_eq!(rows.as_array().map(Vec::len), Some(expected), "{rows}");
}

/// Builds and queries over local file storage read the store's own files, so
/// nothing lands in the disk cache.
#[tokio::test(flavor = "multi_thread")]
async fn local_file_storage_writes_nothing_to_the_disk_cache() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let indexer_config = IndexerConfig::small().with_data_dir(tmp.path().join("data"));
    let cache_dir = indexer_config.artifact_cache_dir();
    let mut fluree = FlureeBuilder::file(tmp.path().join("storage").to_string_lossy().to_string())
        .with_ledger_cache_config(LedgerManagerConfig {
            cache_dir: cache_dir.clone(),
            ..LedgerManagerConfig::default()
        })
        .build()
        .expect("build");
    let (worker, handle) = BackgroundIndexerWorker::new(
        fluree.backend().clone(),
        fluree
            .nameservice_mode()
            .publisher_arc()
            .expect("test setup requires ReadWrite nameservice mode"),
        indexer_config,
    );
    tokio::spawn(worker.run());
    fluree.set_indexing_mode(IndexingMode::Background(handle));

    let ledger_id = "it/disk-cache-local:main";
    fluree.create_ledger(ledger_id).await.expect("create");
    // The first build is a full rebuild; the rest are incremental, which is
    // where the indexer copies what it writes into the cache.
    for round in 0..3 {
        let ledger = fluree.ledger(ledger_id).await.expect("load");
        fluree.insert(ledger, &person(round)).await.expect("insert");
        fluree
            .trigger_index(ledger_id, TriggerIndexOptions::default())
            .await
            .expect("trigger_index");
    }
    assert_people(&fluree, ledger_id, 3).await;

    let cached = files_under(&cache_dir);
    assert!(
        cached.is_empty(),
        "local storage was copied into the disk cache: {cached:?}"
    );
}

/// A ledger on `storage` after a full rebuild, an incremental build and a
/// query, with the disk cache of both its indexer and its readers in one
/// private directory.
struct Built {
    fluree: Fluree,
    ledger_id: &'static str,
    root_id: ContentId,
    cache_dir: PathBuf,
    _tmp: tempfile::TempDir,
}

async fn build_twice_and_query<S: Storage + Clone + 'static>(storage: S) -> Built {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let indexer_config = IndexerConfig::small().with_data_dir(tmp.path().join("data"));
    let cache_dir = indexer_config.artifact_cache_dir();
    let nameservice = MemoryNameService::new();
    let mut fluree: Fluree = Fluree::new(
        ConnectionConfig::memory(),
        storage.clone(),
        NameServiceMode::ReadWrite(Arc::new(nameservice.clone())),
    );
    let (worker, handle) = BackgroundIndexerWorker::new(
        StorageBackend::Managed(Arc::new(storage)),
        Arc::new(nameservice),
        indexer_config,
    );
    tokio::spawn(worker.run());
    fluree.set_indexing_mode(IndexingMode::Background(handle));

    let ledger_id = "it/disk-cache:main";
    let mut ledger = support::genesis_ledger_for_fluree(&fluree, ledger_id);
    let mut root_id = None;
    for round in 0..2 {
        ledger = fluree
            .insert(ledger, &person(round))
            .await
            .expect("insert")
            .ledger;
        root_id = fluree
            .trigger_index(ledger_id, TriggerIndexOptions::default())
            .await
            .expect("trigger_index")
            .root_id;
    }
    assert_people(&fluree, ledger_id, 2).await;
    Built {
        fluree,
        ledger_id,
        root_id: root_id.expect("the build published a root"),
        cache_dir,
        _tmp: tmp,
    }
}

/// Memory storage has no local files to skip, so only its own answer keeps
/// it out of the cache.
#[tokio::test(flavor = "multi_thread")]
async fn memory_storage_writes_nothing_to_the_disk_cache() {
    let built = build_twice_and_query(MemoryStorage::new()).await;
    let cached = files_under(&built.cache_dir);
    assert!(
        cached.is_empty(),
        "memory storage was copied into the disk cache: {cached:?}"
    );
}

/// Remote storage keeps the cache: an incremental build copies what it wrote
/// there, so the first read does not fetch it back. Checked on the new stats
/// sketch because nothing reads one until the next build — the build's own
/// read-through of the previous version fills the cache regardless.
#[tokio::test(flavor = "multi_thread")]
async fn remote_storage_builds_seed_the_disk_cache() {
    let built = build_twice_and_query(MemoryStorage::new().simulating_remote()).await;

    let root_bytes = built
        .fluree
        .content_store(built.ledger_id)
        .get(&built.root_id)
        .await
        .expect("root bytes");
    let sketch = IndexRoot::decode(&root_bytes)
        .expect("decode root")
        .sketch_ref
        .expect("the build wrote a stats sketch");
    assert!(
        built.cache_dir.join(sketch.to_string()).exists(),
        "an incremental build over remote storage did not seed what it wrote"
    );
}

/// Encrypted remote storage reads back plaintext, so nothing of it may land in
/// the cache — not what the indexer wrote, nor what readers fetched. The
/// remote test above is the same flow without the key.
#[tokio::test(flavor = "multi_thread")]
async fn encrypted_remote_storage_writes_nothing_to_the_disk_cache() {
    let storage = EncryptedStorage::new(
        MemoryStorage::new().simulating_remote(),
        StaticKeyProvider::new(EncryptionKey::new([0x42; 32], 0)),
    );
    let built = build_twice_and_query(storage).await;
    let cached = files_under(&built.cache_dir);
    assert!(
        cached.is_empty(),
        "encrypted storage spilled plaintext to the disk cache: {cached:?}"
    );
}
