//! A full rebuild stages plaintext ledger content on local disk — sorted
//! commit runs, dictionaries, leaves — under session directories that default
//! to the system temp directory. Those directories must be gone on every
//! exit, not only the successful one: a rebuild that fails part-way through
//! used to leave its staging tree behind.

#![cfg(feature = "native")]

use async_trait::async_trait;
use fluree_db_api::{FlureeBuilder, IndexConfig, LedgerState, Novelty};
use fluree_db_core::error::Result as StorageResult;
use fluree_db_core::{ContentId, ContentKind, ContentStore, LedgerSnapshot};
use serde_json::json;
use std::path::Path;
use std::sync::Arc;

/// Wraps a content store and fails every envelope range read, which is the
/// first read the rebuild issues after creating its session directories.
#[derive(Clone, Debug)]
struct RangeReadsFail(Arc<dyn ContentStore>);

#[async_trait]
impl ContentStore for RangeReadsFail {
    async fn has(&self, id: &ContentId) -> StorageResult<bool> {
        self.0.has(id).await
    }

    async fn get(&self, id: &ContentId) -> StorageResult<Vec<u8>> {
        self.0.get(id).await
    }

    async fn put(&self, kind: ContentKind, bytes: &[u8]) -> StorageResult<ContentId> {
        self.0.put(kind, bytes).await
    }

    async fn put_with_id(&self, id: &ContentId, bytes: &[u8]) -> StorageResult<()> {
        self.0.put_with_id(id, bytes).await
    }

    async fn release(&self, id: &ContentId) -> StorageResult<()> {
        self.0.release(id).await
    }

    async fn get_range(
        &self,
        _id: &ContentId,
        _range: std::ops::Range<u64>,
    ) -> StorageResult<Vec<u8>> {
        Err(fluree_db_core::Error::storage(
            "injected range read failure",
        ))
    }
}

async fn seed_commits(fluree: &fluree_db_api::Fluree, ledger_id: &str, n: usize) -> LedgerState {
    let mut ledger = LedgerState::new(LedgerSnapshot::genesis(ledger_id), Novelty::new(0));
    let idx_cfg = IndexConfig {
        reindex_min_bytes: 0,
        reindex_max_bytes: 10_000_000,
    };
    for i in 0..n {
        let tx = json!({
            "@context": { "ex": "http://example.org/" },
            "@id": format!("ex:person{i}"),
            "@type": "ex:Person",
            "ex:name": format!("Person {i}"),
        });
        ledger = fluree
            .insert_with_opts(
                ledger,
                &tx,
                Default::default(),
                Default::default(),
                &idx_cfg,
            )
            .await
            .expect("insert_with_opts")
            .ledger;
    }
    ledger
}

/// Session directories under `data_dir`, at any depth: `tmp_import/<id>` and
/// `index/<id>` for the ledger.
fn session_dirs_under(root: &Path) -> Vec<std::path::PathBuf> {
    let mut found = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let parent = dir.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if parent == "tmp_import" || parent == "index" {
                found.push(path);
            } else {
                stack.push(path);
            }
        }
    }
    found
}

async fn record_for(
    fluree: &fluree_db_api::Fluree,
    ledger_id: &str,
) -> fluree_db_nameservice::NsRecord {
    fluree
        .nameservice()
        .lookup(ledger_id)
        .await
        .expect("ns lookup")
        .expect("ns record exists")
}

#[tokio::test]
async fn failed_rebuild_removes_its_session_directories() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/rebuild-staging-fail:main";
    seed_commits(&fluree, ledger_id, 3).await;
    let record = record_for(&fluree, ledger_id).await;

    let data_dir = tempfile::TempDir::new().expect("tempdir");
    let config = fluree_db_indexer::IndexerConfig::default().with_data_dir(data_dir.path());

    let result = fluree_db_indexer::rebuild_index_from_commits_with_store(
        RangeReadsFail(fluree.content_store(ledger_id)),
        ledger_id,
        &record,
        config,
    )
    .await;
    assert!(
        result.is_err(),
        "the injected read failure must fail the rebuild"
    );

    let leftover = session_dirs_under(data_dir.path());
    assert!(
        leftover.is_empty(),
        "failed rebuild left staging directories behind: {leftover:?}"
    );
}

#[tokio::test]
async fn successful_rebuild_removes_its_session_directories() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/rebuild-staging-ok:main";
    seed_commits(&fluree, ledger_id, 3).await;
    let record = record_for(&fluree, ledger_id).await;

    let data_dir = tempfile::TempDir::new().expect("tempdir");
    let config = fluree_db_indexer::IndexerConfig::default().with_data_dir(data_dir.path());

    fluree_db_indexer::rebuild_index_from_commits(
        fluree.content_store(ledger_id),
        ledger_id,
        &record,
        config,
    )
    .await
    .expect("rebuild");

    let leftover = session_dirs_under(data_dir.path());
    assert!(
        leftover.is_empty(),
        "successful rebuild left staging directories behind: {leftover:?}"
    );
}
