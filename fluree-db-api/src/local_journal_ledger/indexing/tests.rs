use super::*;
use fluree_db_core::storage::{FileStorage, StorageContentStore};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::Notify;

async fn ledger() -> (tempfile::TempDir, JournalLedger, AcceptedCommit) {
    let dir = tempfile::tempdir().unwrap();
    let ledger = JournalLedger::initialize(dir.path().into(), "async:main".into(), "g1".into())
        .await
        .unwrap();
    let ack = ledger
        .transact(
            TxnType::Insert,
            &json!({"@id":"http://example.org/one", "http://example.org/value": 1}),
        )
        .await
        .unwrap()
        .unwrap();
    (dir, ledger, ack)
}
fn output(path: &std::path::Path) -> (FileStorage, Arc<dyn ContentStore>) {
    let storage = FileStorage::new(path);
    let cs = Arc::new(StorageContentStore::new(
        storage.clone(),
        "async:main",
        "file",
    ));
    (storage, cs)
}
fn config() -> fluree_db_indexer::IndexerConfig {
    fluree_db_indexer::IndexerConfig {
        ..Default::default()
    }
}
#[derive(Debug)]
struct Paused {
    store: Arc<dyn ContentStore>,
    pause_get: AtomicBool,
    pause_put: AtomicBool,
    entered: Notify,
    resume: Notify,
    fail: bool,
}
impl Paused {
    fn new(store: Arc<dyn ContentStore>, get: bool, put: bool, fail: bool) -> Arc<Self> {
        Arc::new(Self {
            store,
            pause_get: AtomicBool::new(get),
            pause_put: AtomicBool::new(put),
            entered: Notify::new(),
            resume: Notify::new(),
            fail,
        })
    }
    async fn pause(&self, flag: &AtomicBool) -> fluree_db_core::Result<()> {
        if flag.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            self.resume.notified().await;
            if self.fail {
                return Err(fluree_db_core::Error::storage("index worker failure"));
            }
        }
        Ok(())
    }
}
#[async_trait]
impl ContentStore for Paused {
    async fn has(&self, id: &ContentId) -> fluree_db_core::Result<bool> {
        self.store.has(id).await
    }
    async fn get(&self, id: &ContentId) -> fluree_db_core::Result<Vec<u8>> {
        self.pause(&self.pause_get).await?;
        self.store.get(id).await
    }
    async fn put(&self, k: ContentKind, b: &[u8]) -> fluree_db_core::Result<ContentId> {
        self.pause(&self.pause_put).await?;
        self.store.put(k, b).await
    }
    async fn put_with_id(&self, id: &ContentId, b: &[u8]) -> fluree_db_core::Result<()> {
        self.pause(&self.pause_put).await?;
        self.store.put_with_id(id, b).await
    }
    async fn release(&self, id: &ContentId) -> fluree_db_core::Result<()> {
        self.store.release(id).await
    }
}
async fn next(ledger: &JournalLedger, n: i64) -> AcceptedCommit {
    tokio::time::timeout(
        Duration::from_secs(5),
        ledger.transact(
            TxnType::Upsert,
            &json!({"@id":"http://example.org/one", "http://example.org/value": n}),
        ),
    )
    .await
    .expect("transaction waited for paused index I/O")
    .unwrap()
    .unwrap()
}
async fn value(ledger: &JournalLedger) -> Value {
    ledger.query(&json!({"select":["?v"],"where":{"@id":"http://example.org/one","http://example.org/value":"?v"}})).await.unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_indexer_paused_output_and_failure_do_not_delay_acknowledgment() {
    for fail in [false, true] {
        let (_dir, ledger, first) = ledger().await;
        let snapshot = ledger.index_input().await.unwrap();
        let dir = tempfile::tempdir().unwrap();
        let (disk, cs) = output(dir.path());
        let pause = Paused::new(cs, false, true, fail);
        let worker = {
            let input = snapshot.clone();
            let store = input.with_index_storage(pause.clone());
            tokio::spawn(async move {
                fluree_db_indexer::build_index_for_record(store, input.record(), config()).await
            })
        };
        tokio::time::timeout(Duration::from_secs(20), pause.entered.notified())
            .await
            .unwrap();
        let second = next(&ledger, 2).await;
        assert_eq!(value(&ledger).await, json!([[2]]));
        assert_eq!(snapshot.record().commit_t, first.commit.t);
        assert!(snapshot
            .content()
            .get(&second.commit.commit_id)
            .await
            .is_err());
        assert!(snapshot
            .content()
            .get(&first.commit.commit_id)
            .await
            .is_ok());
        pause.resume.notify_one();
        let built = worker.await.unwrap();
        if fail {
            assert!(built.is_err());
        } else {
            assert_eq!(built.unwrap().index_t, first.commit.t);
        }
        assert_eq!(disk.fsyncs_issued(), 0, "derived index writes added fsync");
        next(&ledger, 3).await;
        ledger.recover().await.unwrap();
        assert_eq!(value(&ledger).await, json!([[3]]));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn adoption_io_is_detached_races_retry_and_restart_needs_no_new_index() {
    let (dir, ledger, _) = ledger().await;
    let input = ledger.index_input().await.unwrap();
    let outputs = tempfile::tempdir().unwrap();
    let (_, cs) = output(outputs.path());
    let built = fluree_db_indexer::build_index_for_record(
        input.with_index_storage(cs.clone()),
        input.record(),
        config(),
    )
    .await
    .unwrap();
    let pause = Paused::new(cs.clone(), true, false, false);
    let adopter = {
        let ledger = ledger.clone();
        let id = built.root_id.clone();
        let pause = pause.clone();
        tokio::spawn(async move { ledger.adopt_index(&id, pause).await })
    };
    tokio::time::timeout(Duration::from_secs(20), pause.entered.notified())
        .await
        .unwrap();
    let second = next(&ledger, 2).await;
    pause.resume.notify_one();
    assert!(
        !adopter.await.unwrap().unwrap(),
        "raced adoption must retry"
    );
    let before = std::fs::read(dir.path().join(".fluree-wal/journal")).unwrap();
    assert!(ledger
        .adopt_index(&built.root_id, cs.clone())
        .await
        .unwrap());
    assert_eq!(
        std::fs::read(dir.path().join(".fluree-wal/journal")).unwrap(),
        before
    );
    assert_eq!(value(&ledger).await, json!([[2]]));
    let fresh = ledger.index_input().await.unwrap();
    assert_eq!(fresh.record().index_t, built.index_t);
    let third = next(&ledger, 3).await;
    assert_eq!(
        ledger.index_input().await.unwrap().record().index_t,
        built.index_t
    );
    assert_eq!(value(&ledger).await, json!([[3]]));
    assert!(fresh.content().get(&third.commit.commit_id).await.is_err());
    let fourth = ledger.transact(TxnType::Insert, &json!({
        "@id":"http://late.example/new", "http://late.example/friend":{"@id":"http://example.org/one"}, "http://late.example/label":"late string"
    })).await.unwrap().unwrap();
    let bound = json!({"select":["?id","?label"],"where":{"@id":"?id","http://late.example/friend":{"@id":"http://example.org/one"},"http://late.example/label":"?label"}});
    let expected_bound = json!([["http://late.example/new", "late string"]]);
    assert_eq!(ledger.query(&bound).await.unwrap(), expected_bound);
    drop(fresh);
    drop(input);
    drop(pause);
    drop(cs);
    drop(ledger);
    outputs.close().unwrap();
    for _ in 0..2 {
        std::fs::remove_dir_all(dir.path().join(".fluree-wal/data")).unwrap();
        std::fs::create_dir(dir.path().join(".fluree-wal/data")).unwrap();
        let recovered = JournalLedger::open(dir.path().into()).await.unwrap();
        assert_eq!(value(&recovered).await, json!([[3]]));
        assert_eq!(
            recovered.head().await.unwrap().unwrap().id,
            Some(fourth.commit.commit_id.clone())
        );
        assert_eq!(recovered.query(&bound).await.unwrap(), expected_bound);
        for ack in [&second, &third, &fourth] {
            assert!(recovered.content(&ack.raw_txn_id).await.is_ok());
            assert!(recovered.content(&ack.commit.commit_id).await.is_ok());
        }
    }
}

#[tokio::test]
async fn index_capabilities_cannot_write_transactions_or_inject_future_commits() {
    let (_d, ledger, first) = ledger().await;
    let input = ledger.index_input().await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let (_, output) = output(dir.path());
    let store = input.with_index_storage(output.clone());
    let second = next(&ledger, 2).await;
    output
        .put_with_id(
            &second.commit.commit_id,
            &ledger.content(&second.commit.commit_id).await.unwrap(),
        )
        .await
        .unwrap();
    assert!(store.get(&second.commit.commit_id).await.is_err());
    assert!(store.put(ContentKind::Commit, b"bad").await.is_err());
    assert!(store.put(ContentKind::Txn, b"bad").await.is_err());
    assert!(store
        .put_with_id(&first.commit.commit_id, b"bad")
        .await
        .is_err());
    assert!(store.release(&first.commit.commit_id).await.is_err());
    assert!(input
        .content()
        .put(ContentKind::IndexLeaf, b"bad")
        .await
        .is_err());
    assert!(input
        .content()
        .resolve_local_path(&first.commit.commit_id)
        .is_none());
    assert!(ledger
        .adopt_index(&first.commit.commit_id, output)
        .await
        .is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn incomplete_failed_cancelled_or_busy_adoption_never_changes_acceptance() {
    let (_dir, ledger, _) = ledger().await;
    let input = ledger.index_input().await.unwrap();
    let outputs = tempfile::tempdir().unwrap();
    let (_, cs) = output(outputs.path());
    let store = input.with_index_storage(cs.clone());
    let built = fluree_db_indexer::build_index_for_record(
        store.clone(),
        input.record(),
        input.configure_indexer(config()),
    )
    .await
    .unwrap();
    let ids = crate::pack::compute_missing_index_artifacts(&store, &built.root_id, None)
        .await
        .unwrap();
    let leaf = ids
        .into_iter()
        .find(|id| id.content_kind() == Some(ContentKind::IndexLeaf))
        .unwrap();
    let bytes = cs.get(&leaf).await.unwrap();
    let path = outputs.path().join(content_path(
        ContentKind::IndexLeaf,
        "async:main",
        &leaf.digest_hex(),
    ));
    std::fs::write(&path, b"corrupt").unwrap();
    let before = ledger.head().await.unwrap();
    assert!(ledger
        .adopt_index(&built.root_id, cs.clone())
        .await
        .is_err());
    assert_eq!(ledger.head().await.unwrap(), before);
    assert_eq!(value(&ledger).await, json!([[1]]));
    // Restore the same bytes for later independent fault cases.
    std::fs::write(&path, bytes).unwrap();
    for fail in [false, true] {
        let pause = Paused::new(cs.clone(), true, false, fail);
        let worker = {
            let ledger = ledger.clone();
            let id = built.root_id.clone();
            let pause = pause.clone();
            tokio::spawn(async move { ledger.adopt_index(&id, pause).await })
        };
        tokio::time::timeout(Duration::from_secs(20), pause.entered.notified())
            .await
            .unwrap();
        if fail {
            pause.resume.notify_one();
            assert!(worker.await.unwrap().is_err());
        } else {
            worker.abort();
            assert!(worker.await.unwrap_err().is_cancelled());
        }
        assert_eq!(ledger.head().await.unwrap(), before);
        assert_eq!(value(&ledger).await, json!([[1]]));
    }
    let pause = Paused::new(cs.clone(), true, false, false);
    let worker = {
        let ledger = ledger.clone();
        let id = built.root_id.clone();
        let pause = pause.clone();
        tokio::spawn(async move { ledger.adopt_index(&id, pause).await })
    };
    tokio::time::timeout(Duration::from_secs(20), pause.entered.notified())
        .await
        .unwrap();
    let guard = ledger.0.cache.lock().await;
    pause.resume.notify_one();
    assert!(!tokio::time::timeout(Duration::from_secs(20), worker)
        .await
        .unwrap()
        .unwrap()
        .unwrap());
    drop(guard);
    assert!(ledger.adopt_index(&built.root_id, cs).await.unwrap());
    next(&ledger, 2).await;
}
