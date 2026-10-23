//! Commit and transaction garbage: what a failed writer leaves is recorded,
//! checked against every branch, and deleted; what any branch references
//! stays.

use std::time::Duration;

use fluree_db_api::{
    CommitOpts, ConflictStrategy, Fluree, FlureeBuilder, GarbageReport, IndexConfig,
};
use fluree_db_core::{ContentId, ContentKind, ContentStore as _};
use fluree_db_transact::{ir::TxnType, TxnOpts};
use serde_json::{json, Value as JsonValue};

async fn file_fluree(dir: &tempfile::TempDir) -> Fluree {
    FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .build_async()
        .await
        .expect("build")
}

fn insert(name: &str) -> JsonValue {
    json!({
        "@context": {"ex": "http://example.org/ns/"},
        "insert": {"@id": format!("ex:{name}"), "ex:name": name}
    })
}

/// Fails while staging, after its body is stored.
fn failing() -> JsonValue {
    json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "xsd": "http://www.w3.org/2001/XMLSchema#"
        },
        "insert": {
            "@id": "ex:x",
            "ex:age": {"@value": "not a number", "@type": "xsd:integer"}
        }
    })
}

/// Transact `body` on `ledger_id`, storing it raw.
async fn transact(
    fluree: &Fluree,
    ledger_id: &str,
    body: &JsonValue,
) -> fluree_db_api::Result<fluree_db_api::TransactResult> {
    let ledger = fluree.ledger(ledger_id).await?;
    fluree
        .transact(
            ledger,
            TxnType::Update,
            body,
            TxnOpts::default().store_raw_txn(true),
            CommitOpts::default(),
            &IndexConfig {
                reindex_min_bytes: 100_000,
                reindex_max_bytes: 1_000_000_000,
            },
        )
        .await
}

fn body_id(body: &JsonValue) -> ContentId {
    ContentId::new(ContentKind::Txn, &serde_json::to_vec(body).unwrap())
}

async fn stored(fluree: &Fluree, ledger_id: &str, id: &ContentId) -> bool {
    let namespace = fluree.storage_namespace(ledger_id).await.unwrap();
    fluree.content_store(&namespace).has(id).await.unwrap()
}

/// Collect with no grace period until `done` holds of the summed reports;
/// an abandoned upload reaches the collector from a background task.
async fn collect_until(fluree: &Fluree, done: impl Fn(&GarbageReport) -> bool) -> GarbageReport {
    let mut total = GarbageReport::default();
    for _ in 0..100 {
        let report = fluree.collect_garbage(Duration::ZERO).await.unwrap();
        total.recorded += report.recorded;
        total.deleted += report.deleted;
        total.resolved += report.resolved;
        total.deferred += report.deferred;
        if done(&total) {
            return total;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("collection never got there: {total:?}");
}

#[tokio::test]
async fn a_failed_transactions_body_is_collected() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = file_fluree(&dir).await;
    fluree.create_ledger("garbage").await.unwrap();
    let kept = insert("alice");
    transact(&fluree, "garbage:main", &kept).await.unwrap();
    transact(&fluree, "garbage:main", &failing())
        .await
        .expect_err("staging fails");

    let report = collect_until(&fluree, |r| r.deleted == 1).await;
    assert_eq!(report.recorded, 1);
    assert!(!stored(&fluree, "garbage:main", &body_id(&failing())).await);
    assert!(stored(&fluree, "garbage:main", &body_id(&kept)).await);
}

#[tokio::test]
async fn a_body_a_published_commit_names_outlives_a_repeat_that_failed() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = file_fluree(&dir).await;
    fluree.create_ledger("garbage").await.unwrap();
    transact(&fluree, "garbage:main", &insert("alice"))
        .await
        .unwrap();
    let body = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "where": {"@id": "ex:alice", "ex:name": "alice"},
        "delete": {"@id": "ex:alice", "ex:name": "alice"},
        "insert": {"@id": "ex:alice", "ex:name": "Alice"}
    });
    let first = transact(&fluree, "garbage:main", &body).await.unwrap();
    // The same body again matches nothing, so it commits nothing and its
    // upload is abandoned.
    let again = transact(&fluree, "garbage:main", &body).await.unwrap();
    assert_eq!(again.receipt.t, first.receipt.t);

    let report = collect_until(&fluree, |r| r.resolved == 1).await;
    assert_eq!(report.deleted, 0);
    assert!(stored(&fluree, "garbage:main", &body_id(&body)).await);
}

#[tokio::test]
async fn reconciliation_records_a_blob_no_branch_references() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = file_fluree(&dir).await;
    fluree.create_ledger("garbage").await.unwrap();
    let kept = insert("alice");
    transact(&fluree, "garbage:main", &kept).await.unwrap();
    // As a crash between storing a body and abandoning it leaves one.
    let namespace = fluree.storage_namespace("garbage:main").await.unwrap();
    let orphan = fluree
        .content_store(&namespace)
        .put(ContentKind::Txn, b"{\"crashed\":true}")
        .await
        .unwrap();

    let report = fluree
        .reconcile_garbage("garbage")
        .await
        .unwrap()
        .expect("settled");
    assert_eq!(report.recorded, 1);
    // Recorded files are not recorded again.
    let again = fluree.reconcile_garbage("garbage").await.unwrap().unwrap();
    assert_eq!(again.recorded, 0);

    collect_until(&fluree, |r| r.deleted == 1).await;
    assert!(!stored(&fluree, "garbage:main", &orphan).await);
    assert!(stored(&fluree, "garbage:main", &body_id(&kept)).await);
}

#[tokio::test]
async fn commits_a_merge_copied_in_stay_after_their_branch_goes() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = file_fluree(&dir).await;
    fluree.create_ledger("garbage").await.unwrap();
    transact(&fluree, "garbage:main", &insert("alice"))
        .await
        .unwrap();
    fluree
        .create_branch("garbage", "dev", None, None)
        .await
        .unwrap();
    let dev = [insert("bob"), insert("carol")];
    for body in &dev {
        transact(&fluree, "garbage:dev", body).await.unwrap();
    }
    transact(&fluree, "garbage:main", &insert("dave"))
        .await
        .unwrap();
    let merged = fluree
        .merge_branch("garbage", "dev", None, ConflictStrategy::default())
        .await
        .unwrap();
    assert!(!merged.fast_forward);
    fluree.drop_branch("garbage", "dev").await.unwrap();

    let report = fluree
        .reconcile_garbage("garbage")
        .await
        .unwrap()
        .expect("settled");
    assert_eq!(report.recorded, 0, "{report:?}");
    for body in &dev {
        assert!(stored(&fluree, "garbage:main", &body_id(body)).await);
    }
    let verified = fluree.verify_ledger("garbage:main", None).await.unwrap();
    assert!(verified.problems.is_empty(), "{:?}", verified.problems);
}
