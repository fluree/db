use super::*;
use std::time::Duration;
const ID: &str = "bridge:main";

#[tokio::test]
async fn unavailable_connection_rejects_cached_reads_and_held_handle_noops() {
    let (_root, _indexes, fluree) = connection(false).await;
    let handle = fluree.ledger_cached(ID).await.unwrap();
    fluree
        .journal
        .as_ref()
        .unwrap()
        .surface
        .healthy
        .store(false, Ordering::Release);
    assert!(fluree.db(ID).await.is_err());
    let body = json!({"where": {"@id": "?s", "urn:missing": 1},
                      "insert": {"@id": "?s", "urn:value": 2}});
    assert!(fluree.stage(&handle).update(&body).execute().await.is_err());
    fluree.disconnect().await;
}
async fn connection(indexing: bool) -> (tempfile::TempDir, tempfile::TempDir, Fluree) {
    let root = tempfile::tempdir().unwrap();
    let indexes = tempfile::tempdir().unwrap();
    drop(
        JournalLedger::initialize(root.path().into(), ID.into(), "bridge".into())
            .await
            .unwrap(),
    );
    let builder = FlureeBuilder::file(root.path().to_string_lossy().to_string());
    let builder = if indexing {
        builder.with_indexing_thresholds(100, 64 * 1024 * 1024)
    } else {
        builder.without_indexing()
    };
    let fluree = builder
        .build_local_journal(root.path().into(), indexes.path().into())
        .await
        .unwrap();
    (root, indexes, fluree)
}
async fn insert(fluree: &Fluree, id: &str) -> crate::Result<crate::tx::TransactResultRef> {
    let handle = fluree.ledger_cached(ID).await?;
    let body = json!({"@id":id,"urn:value":1});
    let opts = CommitOpts::default().with_raw_txn_spawned(fluree.content_store(ID), body.clone());
    fluree
        .stage(&handle)
        .insert(&body)
        .commit_opts(opts)
        .execute()
        .await
}
async fn query(fluree: &Fluree) -> Value {
    let view = fluree.db(ID).await.unwrap();
    let q = json!({"select":["?s"],"where":{"@id":"?s","urn:value":1},"orderBy":"?s"});
    fluree
        .query(&view, &q)
        .await
        .unwrap()
        .to_jsonld(&view.snapshot)
        .unwrap()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn background_publication_wait_cannot_hold_up_ordinary_acknowledgments() {
    let (_root, _indexes, fluree) = connection(true).await;
    let journal = fluree.journal.as_ref().unwrap();
    let gate = journal.surface.index_gate.lock().await;
    tokio::time::timeout(Duration::from_secs(5), insert(&fluree, "urn:one"))
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), insert(&fluree, "urn:two"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(query(&fluree).await, json!([["urn:one"], ["urn:two"]]));
    // No duplicate transaction-state snapshot survives in the journal bridge.
    assert!(journal.ledger.0.cache.lock().await.state.is_none());
    drop(gate);
    tokio::time::timeout(
        Duration::from_secs(20),
        fluree.indexer_handle().unwrap().wait_for_idle(ID),
    )
    .await
    .unwrap();
    let record = fluree.nameservice().lookup(ID).await.unwrap().unwrap();
    assert_eq!(record.index_t, 2);
    assert_eq!(query(&fluree).await, json!([["urn:one"], ["urn:two"]]));
    fluree.disconnect().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_install_self_reconciles_exact_commit_and_repairs_ordinary_cache() {
    let (_root, _indexes, fluree) = connection(false).await;
    let journal = fluree.journal.as_ref().unwrap();
    *journal.install_hook.lock().unwrap() = Some(Box::new(|| {
        Err(JournalError::Invalid("injected install failure"))
    }));
    assert!(insert(&fluree, "urn:one").await.is_err());
    assert_eq!(query(&fluree).await, json!([["urn:one"]]));
    let record = fluree.nameservice().lookup(ID).await.unwrap().unwrap();
    assert_eq!(record.commit_t, 1);
    let bytes = fluree
        .content_store(ID)
        .get(record.commit_head_id.as_ref().unwrap())
        .await
        .unwrap();
    let commit = fluree_db_core::commit::codec::read_commit(&bytes).unwrap();
    let raw = fluree
        .content_store(ID)
        .get(&commit.txn.unwrap())
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&raw).unwrap(),
        json!({"@id":"urn:one","urn:value":1})
    );
    insert(&fluree, "urn:two").await.unwrap();
    assert_eq!(query(&fluree).await, json!([["urn:one"], ["urn:two"]]));
    fluree.disconnect().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn caller_cancellation_after_flush_cannot_cancel_ordinary_installation() {
    let (_root, _indexes, fluree) = connection(false).await;
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    *fluree
        .journal
        .as_ref()
        .unwrap()
        .install_hook
        .lock()
        .unwrap() = Some(Box::new(move || {
        entered_tx.send(()).unwrap();
        release_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        Ok(())
    }));
    let copy = fluree.clone();
    let caller = tokio::spawn(async move { insert(&copy, "urn:one").await });
    tokio::time::timeout(Duration::from_secs(10), entered_rx)
        .await
        .unwrap()
        .unwrap();
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    release_tx.send(()).unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(10), query(&fluree))
            .await
            .unwrap(),
        json!([["urn:one"]])
    );
    insert(&fluree, "urn:two").await.unwrap();
    fluree.disconnect().await;
}
#[tokio::test]
async fn provisional_payloads_and_bypass_mutations_do_not_become_accepted_content() {
    let (_root, _indexes, fluree) = connection(false).await;
    let store = fluree.content_store(ID);
    let raw = store
        .put(ContentKind::Txn, b"{\"private\":true}")
        .await
        .unwrap();
    assert!(!store.has(&raw).await.unwrap());
    assert!(store.get(&raw).await.is_err());
    assert!(store.put(ContentKind::Commit, b"bypass").await.is_err());
    assert!(fluree.create_ledger("another").await.is_err());
    let publisher = fluree.publisher().unwrap();
    assert!(publisher
        .compare_and_set_ref(
            ID,
            fluree_db_nameservice::RefKind::CommitHead,
            None,
            &RefValue {
                id: Some(raw.clone()),
                t: 1
            }
        )
        .await
        .is_err());
    assert!(store.release(&raw).await.is_err());
    assert_eq!(
        fluree
            .nameservice()
            .lookup(ID)
            .await
            .unwrap()
            .unwrap()
            .commit_t,
        0
    );
    insert(&fluree, "urn:one").await.unwrap();
    fluree.disconnect().await;
}
