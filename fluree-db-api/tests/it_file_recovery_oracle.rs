//! Recovery oracle for the local file backend, independent of its live caches.
//!
//! This is a declared-frontier crash-image model, NOT a power-loss/fsync test or
//! a SIGKILL test. Successful writes are explicitly assumed durable at capture;
//! only later, explicitly unacknowledged objects may be dropped or torn. It tests
//! the real file reader/replay against those images and validates the oracle with
//! deliberately invalid images. It does not establish that the production write
//! path actually creates that frontier. A syscall-level fault model is still a
//! prerequisite to claiming a future WAL is crash safe.
//! The experimental `journal` submodule below instead derives journal loss images
//! from intercepted real file writes/syncs. Its materializer is test-only; neither
//! test family enables the production transaction WAL or proves device power loss.

#![cfg(feature = "native")]

use crate::support;
use async_trait::async_trait;
use fluree_db_api::{CommitOpts, Fluree, FlureeBuilder, IndexConfig, NameServiceMode};
use fluree_db_core::{
    commit::codec::read_commit, content_path, CasAction, CasOutcome, ContentId, ContentKind,
    ContentStore, Durability, FileStorage, ListResult, StorageCas, StorageExtError,
    StorageExtResult, StorageList, StorageRead, StorageWrite,
};
use fluree_db_nameservice::StorageNameService;
use fluree_db_transact::{ir::TxnType, CommitReceipt, TransactError, TxnOpts};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const LEDGER: &str = "recovery-oracle:main";
const HEAD_PATH: &str = "ns@v2/recovery-oracle/main.json";

#[cfg(all(feature = "experimental-local-journal", unix))]
#[path = "support/journal_recovery.rs"]
mod journal;

/// The generic nameservice requires StorageList; FileStorage exposes the same
/// operation through StorageRead. Keep that adapter in the fixture, leaving
/// every persistence operation delegated to the real file implementation.
#[derive(Debug, Clone)]
struct ListedFileStorage(FileStorage);

fn file_address(key: &str) -> String {
    // FileStorage's raw-address shorthand appends `.json`; the generic
    // nameservice supplies complete keys, so use the canonical address form.
    if key.starts_with("fluree:") {
        key.to_owned()
    } else {
        format!("fluree:file://{key}")
    }
}

#[async_trait]
impl StorageRead for ListedFileStorage {
    async fn read_bytes(&self, address: &str) -> fluree_db_core::Result<Vec<u8>> {
        self.0.read_bytes(&file_address(address)).await
    }

    async fn exists(&self, address: &str) -> fluree_db_core::Result<bool> {
        self.0.exists(&file_address(address)).await
    }

    async fn list_prefix(&self, prefix: &str) -> fluree_db_core::Result<Vec<String>> {
        self.0.list_prefix(prefix).await
    }
}

#[async_trait]
impl StorageWrite for ListedFileStorage {
    async fn write_bytes(&self, address: &str, bytes: &[u8]) -> fluree_db_core::Result<()> {
        self.0.write_bytes(&file_address(address), bytes).await
    }

    async fn delete(&self, address: &str) -> fluree_db_core::Result<()> {
        self.0.delete(&file_address(address)).await
    }
}

#[async_trait]
impl StorageCas for ListedFileStorage {
    async fn insert(&self, address: &str, bytes: &[u8]) -> StorageExtResult<bool> {
        self.0.insert(&file_address(address), bytes).await
    }

    async fn compare_and_swap<T, F>(&self, address: &str, f: F) -> StorageExtResult<CasOutcome<T>>
    where
        F: Fn(Option<&[u8]>) -> StorageExtResult<CasAction<T>> + Send + Sync,
        T: Send,
    {
        self.0.compare_and_swap(&file_address(address), f).await
    }
}

#[async_trait]
impl StorageList for ListedFileStorage {
    async fn list_prefix(&self, prefix: &str) -> StorageExtResult<Vec<String>> {
        let mut keys: Vec<_> = self
            .0
            .list_prefix(prefix)
            .await
            .map_err(|e| StorageExtError::io(e.to_string()))?
            .into_iter()
            .map(|key| {
                key.strip_prefix("fluree:file://")
                    .unwrap_or(&key)
                    .to_owned()
            })
            .collect();
        keys.sort();
        Ok(keys)
    }

    async fn list_prefix_paginated(
        &self,
        prefix: &str,
        continuation_token: Option<String>,
        max_keys: usize,
    ) -> StorageExtResult<ListResult> {
        if max_keys == 0 {
            return Err(StorageExtError::io("max_keys must be positive"));
        }
        let mut keys = StorageList::list_prefix(self, prefix).await?;
        if let Some(after) = continuation_token {
            keys.retain(|key| key > &after);
        }
        let truncated = keys.len() > max_keys;
        keys.truncate(max_keys);
        let next = if truncated {
            keys.last().cloned()
        } else {
            None
        };
        Ok(ListResult::new(keys, next, truncated))
    }
}

fn writer(root: &Path) -> Fluree {
    // Explicit Sync for both content and CAS, independent of process-wide test
    // settings. StorageNameService uses the same ns@v2 format as FileNameService;
    // recovery below deliberately uses the normal file builder/reader instead.
    let storage = FileStorage::new(root).with_durability(Durability::Sync);
    let nameservice = StorageNameService::new(ListedFileStorage(storage.clone()), "");
    FlureeBuilder::file(root.to_string_lossy().to_string())
        .without_indexing()
        .build_with(storage, NameServiceMode::ReadWrite(Arc::new(nameservice)))
}

fn config() -> IndexConfig {
    IndexConfig {
        reindex_min_bytes: 1_000_000,
        reindex_max_bytes: 10_000_000,
    }
}

fn mutation(marker: &str, value: i64) -> Value {
    json!({
        "@context": {"ex": "http://example.org/recovery/"},
        "@graph": [{"@id": format!("ex:{marker}"), "ex:marker": marker, "ex:value": value}]
    })
}

#[derive(Debug, Serialize, Deserialize)]
struct AcknowledgedCommit {
    t: i64,
    commit_id: ContentId,
    commit_bytes: Vec<u8>,
    raw_id: ContentId,
    raw_bytes: Vec<u8>,
}

impl AcknowledgedCommit {
    async fn capture(fluree: &Fluree, receipt: &CommitReceipt, body: &Value) -> Self {
        let store = fluree.content_store(LEDGER);
        let commit_bytes = store.get(&receipt.commit_id).await.expect("ack commit");
        let commit = read_commit(&commit_bytes).expect("decode ack commit");
        let raw_id = commit.txn.expect("requested raw transaction reference");
        let raw_bytes = store.get(&raw_id).await.expect("ack raw bytes");
        assert_eq!(raw_bytes, serde_json::to_vec(body).unwrap());
        Self {
            t: receipt.t,
            commit_id: receipt.commit_id.clone(),
            commit_bytes,
            raw_id,
            raw_bytes,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct AckOracle {
    commits: Vec<AcknowledgedCommit>,
    expected_rows: Value,
}

async fn rows(fluree: &Fluree, ledger: &fluree_db_api::LedgerState) -> Result<Value, String> {
    let query = json!({
        "@context": {"ex": "http://example.org/recovery/"},
        "select": ["?id", "?marker", "?value"],
        "where": {"@id": "?id", "ex:marker": "?marker", "ex:value": "?value"}
    });
    let mut value = support::query_jsonld_formatted(fluree, ledger, &query)
        .await
        .map_err(|e| format!("query recovered state: {e}"))?;
    value
        .as_array_mut()
        .ok_or("expected query rows")?
        .sort_by_key(Value::to_string);
    Ok(value)
}

impl AckOracle {
    fn persist(&self, path: &Path) {
        // The controller's receipt log is outside every database/crash-image
        // directory. Recovery cannot manufacture its expected answers.
        let mut file = std::fs::File::create(path).unwrap();
        file.write_all(&serde_json::to_vec(self).unwrap()).unwrap();
        file.sync_all().unwrap();
        #[cfg(unix)]
        std::fs::File::open(path.parent().unwrap())
            .unwrap()
            .sync_all()
            .unwrap();
    }

    async fn check(&self, root: &Path) -> Result<(), String> {
        let fluree = FlureeBuilder::file(root.to_string_lossy().to_string())
            .without_indexing()
            .build()
            .map_err(|e| format!("open image: {e}"))?;
        let ledger = fluree
            .ledger(LEDGER)
            .await
            .map_err(|e| format!("recover ledger: {e}"))?;
        let latest = self.commits.last().unwrap();
        if ledger.t() != latest.t || ledger.head_commit_id.as_ref() != Some(&latest.commit_id) {
            return Err("recovered head differs from acknowledged head".into());
        }
        let store = fluree.content_store(LEDGER);
        for (index, ack) in self.commits.iter().enumerate() {
            let bytes = store
                .get(&ack.commit_id)
                .await
                .map_err(|e| format!("acknowledged commit missing: {e}"))?;
            if bytes != ack.commit_bytes {
                return Err("acknowledged commit bytes changed".into());
            }
            let decoded = read_commit(&bytes).map_err(|e| format!("decode commit: {e}"))?;
            if decoded.t != ack.t || decoded.txn.as_ref() != Some(&ack.raw_id) {
                return Err("acknowledged commit identity/provenance changed".into());
            }
            if index > 0 && decoded.parents != vec![self.commits[index - 1].commit_id.clone()] {
                return Err("acknowledged commit chain changed".into());
            }
            let raw = store
                .get(&ack.raw_id)
                .await
                .map_err(|e| format!("acknowledged raw content missing: {e}"))?;
            if raw != ack.raw_bytes {
                return Err("acknowledged raw content changed".into());
            }
        }
        if rows(&fluree, &ledger).await? != self.expected_rows {
            return Err("acknowledged query results changed or duplicated".into());
        }
        Ok(())
    }
}

/// A deliberately declared persistence frontier, not an observation of fsync.
/// Later fault injection can replace this producer without changing AckOracle.
#[derive(Clone)]
struct DeclaredFrontier(BTreeMap<PathBuf, Vec<u8>>);

impl DeclaredFrontier {
    fn capture(root: &Path) -> Self {
        fn visit(root: &Path, dir: &Path, files: &mut BTreeMap<PathBuf, Vec<u8>>) {
            for entry in std::fs::read_dir(dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    visit(root, &path, files);
                } else if path.is_file() && path.extension().is_none_or(|ext| ext != "lock") {
                    files.insert(
                        path.strip_prefix(root).unwrap().into(),
                        std::fs::read(path).unwrap(),
                    );
                }
            }
        }
        let mut files = BTreeMap::new();
        visit(root, root, &mut files);
        Self(files)
    }

    fn materialize(&self, root: &Path) {
        for (path, bytes) in &self.0 {
            let target = root.join(path);
            std::fs::create_dir_all(target.parent().unwrap()).unwrap();
            std::fs::write(target, bytes).unwrap();
        }
    }
}

struct Fixture {
    oracle: AckOracle,
    frontier: DeclaredFrontier,
    loser_id: ContentId,
    orphan_id: ContentId,
    orphan_bytes: Vec<u8>,
    last_body: Value,
    #[cfg(all(feature = "experimental-local-journal", unix))]
    first_frontier: DeclaredFrontier,
}

async fn fixture() -> Fixture {
    let root = tempfile::tempdir().unwrap();
    let controller = tempfile::tempdir().unwrap();
    let fluree = writer(root.path());
    let first_body = mutation("ack-one", 10);
    let first = fluree
        .transact(
            fluree.create_ledger(LEDGER).await.unwrap(),
            TxnType::Upsert,
            &first_body,
            TxnOpts::default().store_raw_txn(true),
            CommitOpts::default(),
            &config(),
        )
        .await
        .unwrap();
    let ack1 = AcknowledgedCommit::capture(&fluree, &first.receipt, &first_body).await;
    #[cfg(all(feature = "experimental-local-journal", unix))]
    let first_frontier = DeclaredFrontier::capture(root.path());

    // Build before another connection wins; apply afterwards. This must reach
    // real file CAS and leave a valid, persisted losing commit blob behind.
    let competitor = writer(root.path());
    let handle = competitor.ledger_cached(LEDGER).await.unwrap();
    let rejected_body = mutation("cas-loser", 999);
    let (guard, loser) = competitor
        .stage(&handle)
        .upsert(&rejected_body)
        .build_commit()
        .await
        .unwrap()
        .unwrap();
    let loser_id = loser.commit.id.clone().unwrap();

    let last_body = mutation("ack-two", 20);
    let second = fluree
        .transact(
            first.ledger,
            TxnType::Upsert,
            &last_body,
            TxnOpts::default().store_raw_txn(true),
            CommitOpts::default(),
            &config(),
        )
        .await
        .unwrap();
    let ack2 = AcknowledgedCommit::capture(&fluree, &second.receipt, &last_body).await;
    let expected_rows = rows(&fluree, &second.ledger).await.unwrap();
    assert_eq!(expected_rows.as_array().unwrap().len(), 2);
    let oracle = AckOracle {
        commits: vec![ack1, ack2],
        expected_rows,
    };
    let receipt_path = controller.path().join("acknowledgments.json");
    oracle.persist(&receipt_path);

    let store = competitor.content_store(LEDGER);
    let error = loser
        .apply(
            store.as_ref(),
            competitor.nameservice_mode().publisher().unwrap(),
        )
        .await
        .expect_err("stale expected head must lose CAS");
    assert!(matches!(error, TransactError::PublishLostRace { .. }));
    assert!(
        store.get(&loser_id).await.is_ok(),
        "CAS loser blob must exist"
    );
    drop(guard);

    let frontier = DeclaredFrontier::capture(root.path());
    assert!(
        frontier.0.contains_key(Path::new(HEAD_PATH)),
        "writer must produce the ordinary FileNameService pathname"
    );
    let fresh = writer(root.path());
    let handle = fresh.ledger_cached(LEDGER).await.unwrap();
    let orphan_body = mutation("unpublished-newest", 1000);
    let (guard, orphan) = fresh
        .stage(&handle)
        .upsert(&orphan_body)
        .build_commit()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(orphan.commit.t, 3, "orphan is newer than acknowledged head");
    let orphan_id = orphan.commit.id.clone().unwrap();
    let orphan_bytes = orphan.commit_bytes.clone();
    FileStorage::new(root.path())
        .with_durability(Durability::PageCache)
        .write_bytes(
            &file_address(&content_path(
                ContentKind::Commit,
                LEDGER,
                &orphan_id.digest_hex(),
            )),
            &orphan_bytes,
        )
        .await
        .unwrap();
    assert_eq!(
        std::fs::read(root.path().join(content_path(
            ContentKind::Commit,
            LEDGER,
            &orphan_id.digest_hex(),
        )))
        .unwrap(),
        orphan_bytes
    );
    drop(guard);
    // Read expectations back from the controller log, not from live handles.
    let oracle = serde_json::from_slice(&std::fs::read(receipt_path).unwrap()).unwrap();
    Fixture {
        oracle,
        frontier,
        loser_id,
        orphan_id,
        orphan_bytes,
        last_body,
        #[cfg(all(feature = "experimental-local-journal", unix))]
        first_frontier,
    }
}

#[tokio::test]
async fn declared_loss_images_recover_exact_acknowledged_chain_and_raw_content() {
    let fixture = fixture().await;
    let orphan_path = PathBuf::from(content_path(
        ContentKind::Commit,
        LEDGER,
        &fixture.orphan_id.digest_hex(),
    ));
    let staging_path = PathBuf::from(format!("{HEAD_PATH}.1.0123456789abcdef.1.tmp"));

    // All, some, or none of a post-frontier unsynced blob reaches the image.
    // A torn staging head must not replace the acknowledged head. Even a fully
    // surviving newer commit and the durable CAS loser are not accepted heads.
    for retained in [
        0,
        fixture.orphan_bytes.len() / 2,
        fixture.orphan_bytes.len(),
    ] {
        let mut image = fixture.frontier.clone();
        if retained > 0 {
            image.0.insert(
                orphan_path.clone(),
                fixture.orphan_bytes[..retained].to_vec(),
            );
        }
        image
            .0
            .insert(staging_path.clone(), b"{\"f:commitCid\":\"torn".to_vec());
        let root = tempfile::tempdir().unwrap();
        image.materialize(root.path());
        for _ in 0..2 {
            fixture.oracle.check(root.path()).await.unwrap();
        }
        assert_eq!(
            DeclaredFrontier::capture(root.path()).0,
            image.0,
            "read/replay must not manufacture commits or mutate persisted state"
        );
    }

    // A reconciled duplicate upsert is a no-op, not another acknowledgment.
    // In particular its placeholder receipt CID must not replace the real head.
    let root = tempfile::tempdir().unwrap();
    fixture.frontier.materialize(root.path());
    let fluree = writer(root.path());
    let before = fluree.ledger(LEDGER).await.unwrap();
    let no_op = fluree
        .transact(
            before,
            TxnType::Upsert,
            &fixture.last_body,
            TxnOpts::default(),
            CommitOpts::default(),
            &config(),
        )
        .await
        .unwrap();
    assert_eq!(no_op.receipt.flake_count, 0);
    assert_eq!(no_op.receipt.t, 2);
    drop(no_op);
    drop(fluree);
    fixture.oracle.check(root.path()).await.unwrap();
}

#[tokio::test]
async fn recovery_oracle_rejects_lost_acknowledgments_and_promoted_orphans() {
    let fixture = fixture().await;
    let first = &fixture.oracle.commits[0];
    for (kind, id) in [
        (ContentKind::Commit, &first.commit_id),
        (ContentKind::Txn, &first.raw_id),
    ] {
        let mut invalid = fixture.frontier.clone();
        let path = PathBuf::from(content_path(kind, LEDGER, &id.digest_hex()));
        assert!(
            invalid.0.remove(&path).is_some(),
            "negative control removes an existing acknowledged object"
        );
        let root = tempfile::tempdir().unwrap();
        invalid.materialize(root.path());
        let error = fixture
            .oracle
            .check(root.path())
            .await
            .expect_err("oracle must detect lost acknowledged content");
        if kind == ContentKind::Txn {
            assert!(
                error.contains("acknowledged raw content missing"),
                "{error}"
            );
        }
    }

    for (id, t) in [(&fixture.loser_id, 2), (&fixture.orphan_id, 3)] {
        let mut invalid = fixture.frontier.clone();
        invalid.0.insert(
            PathBuf::from(content_path(
                ContentKind::Commit,
                LEDGER,
                &fixture.orphan_id.digest_hex(),
            )),
            fixture.orphan_bytes.clone(),
        );
        let head = invalid.0.get_mut(Path::new(HEAD_PATH)).unwrap();
        let mut record: Value = serde_json::from_slice(head).unwrap();
        record["f:commitCid"] = json!(id.to_string());
        record["f:t"] = json!(t);
        *head = serde_json::to_vec(&record).unwrap();
        let root = tempfile::tempdir().unwrap();
        invalid.materialize(root.path());
        let error = fixture
            .oracle
            .check(root.path())
            .await
            .expect_err("oracle must reject a CAS loser or unpublished head");
        assert_eq!(error, "recovered head differs from acknowledged head");
    }
}
