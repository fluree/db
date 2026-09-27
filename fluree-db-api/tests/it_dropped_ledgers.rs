//! Dropped ledgers: a drop frees the name at once, and the dropped ledger is
//! restored or purged without touching whatever has reused its name.

use crate::support;
use fluree_db_api::{DropMode, DropStatus, DroppedData, DroppedLedgerState, Fluree, FlureeBuilder};
use serde_json::json;

/// Create `id` holding one value, `marker`.
async fn create_with_marker(fluree: &Fluree, id: &str, marker: &str) {
    let ledger = fluree.create_ledger(id).await.expect("create");
    let txn = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:marker", "ex:value": marker}]
    });
    fluree.insert(ledger, &txn).await.expect("insert");
}

/// The values `id` holds.
async fn markers(fluree: &Fluree, id: &str) -> serde_json::Value {
    let ledger = fluree.ledger(id).await.expect("load");
    let query = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "select": "?v",
        "where": {"@id": "ex:marker", "ex:value": "?v"}
    });
    support::query_jsonld(fluree, &ledger, &query)
        .await
        .expect("query")
        .to_jsonld(&ledger.snapshot)
        .expect("format")
}

async fn file_fluree() -> (Fluree, tempfile::TempDir) {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let fluree = FlureeBuilder::file(tmp.path().to_string_lossy().to_string())
        .build()
        .expect("build");
    (fluree, tmp)
}

/// Drop, recreate, then purge the old ledger: the new ledger is intact.
#[tokio::test]
async fn purging_a_dropped_ledger_leaves_the_one_that_reused_its_name() {
    let (fluree, _tmp) = file_fluree().await;
    create_with_marker(&fluree, "mydb", "old").await;
    let dropped = fluree.drop_ledger("mydb", DropMode::Soft).await.unwrap();
    assert!(dropped.name_released);
    assert_eq!(dropped.data, Some(DroppedData::Retained));
    let instance = dropped.instance.expect("a bound ledger");

    create_with_marker(&fluree, "mydb", "new").await;
    assert_eq!(markers(&fluree, "mydb").await, json!(["new"]));

    let purged = fluree.purge_dropped(instance.as_str()).await.unwrap();
    assert_eq!(purged.data, Some(DroppedData::Deleted));
    assert!(purged.artifacts_deleted > 0);
    assert!(fluree.list_dropped().await.unwrap().is_empty());

    fluree.disconnect_ledger("mydb").await;
    assert_eq!(markers(&fluree, "mydb").await, json!(["new"]));
}

/// Drop, recreate, then hard-drop the new ledger: the old one is intact and
/// restorable, under its own data.
#[tokio::test]
async fn hard_dropping_the_replacement_leaves_the_dropped_ledger_restorable() {
    let (fluree, _tmp) = file_fluree().await;
    create_with_marker(&fluree, "mydb", "old").await;
    let dropped = fluree.drop_ledger("mydb", DropMode::Soft).await.unwrap();
    let instance = dropped.instance.expect("a bound ledger");

    create_with_marker(&fluree, "mydb", "new").await;
    let hard = fluree.drop_ledger("mydb", DropMode::Hard).await.unwrap();
    assert_eq!(hard.data, Some(DroppedData::Deleted));
    assert_ne!(hard.instance.as_ref(), Some(&instance));

    let listed = fluree.list_dropped().await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].instance, instance);
    assert_eq!(listed[0].state, DroppedLedgerState::Dropped);
    assert_eq!(listed[0].branches, vec!["main".to_string()]);

    let restored = fluree.restore_dropped(instance.as_str()).await.unwrap();
    assert_eq!(restored.name, "mydb");
    assert!(fluree.list_dropped().await.unwrap().is_empty());
    assert_eq!(markers(&fluree, "mydb").await, json!(["old"]));
}

/// Restoring into a name another ledger holds is a conflict, and leaves both
/// the live ledger and the dropped one as they were.
#[tokio::test]
async fn restore_into_a_taken_name_conflicts() {
    let (fluree, _tmp) = file_fluree().await;
    create_with_marker(&fluree, "mydb", "old").await;
    let instance = fluree
        .drop_ledger("mydb", DropMode::Soft)
        .await
        .unwrap()
        .instance
        .unwrap();
    create_with_marker(&fluree, "mydb", "new").await;

    let err = fluree.restore_dropped(instance.as_str()).await.unwrap_err();
    assert_eq!(err.status_code(), 409, "{err}");
    assert_eq!(markers(&fluree, "mydb").await, json!(["new"]));
    let listed = fluree.list_dropped().await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].state, DroppedLedgerState::Dropped);
}

/// A branch dropped before its ledger stays dropped when the ledger is
/// restored; the branches that were live come back live.
#[tokio::test]
async fn restore_keeps_a_dropped_branch_dropped() {
    let (fluree, _tmp) = file_fluree().await;
    create_with_marker(&fluree, "mydb", "main").await;
    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();
    fluree
        .create_branch("mydb", "fx", Some("dev"), None)
        .await
        .unwrap();
    let dev = fluree.drop_branch("mydb", "dev").await.unwrap();
    assert!(dev.deferred, "dev keeps its data for fx");

    let instance = fluree
        .drop_ledger("mydb", DropMode::Soft)
        .await
        .unwrap()
        .instance
        .unwrap();
    let listed = fluree.list_dropped().await.unwrap();
    assert_eq!(
        listed[0].branches,
        vec!["main".to_string(), "fx".to_string()]
    );

    fluree.restore_dropped(instance.as_str()).await.unwrap();
    let mut live: Vec<String> = fluree
        .nameservice()
        .list_branches("mydb")
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.branch)
        .collect();
    live.sort();
    assert_eq!(live, vec!["fx".to_string(), "main".to_string()]);
    assert_eq!(markers(&fluree, "mydb:fx").await, json!(["main"]));

    // Dropping fx now takes dev with it.
    let fx = fluree.drop_branch("mydb", "fx").await.unwrap();
    assert_eq!(fx.status, DropStatus::Dropped);
    assert_eq!(fx.cascaded, vec!["mydb:dev".to_string()]);
}

/// Purging something that is not a dropped ledger is a clean not-found, and
/// a malformed id is a client error.
#[tokio::test]
async fn purge_and_restore_reject_unknown_instances() {
    let (fluree, _tmp) = file_fluree().await;
    let unknown = "01JB8ZK4X5Y6Z7A8B9C0D1E2F3";
    assert_eq!(
        fluree
            .purge_dropped(unknown)
            .await
            .unwrap_err()
            .status_code(),
        404
    );
    assert_eq!(
        fluree
            .restore_dropped(unknown)
            .await
            .unwrap_err()
            .status_code(),
        404
    );
    assert_eq!(
        fluree
            .purge_dropped("mydb")
            .await
            .unwrap_err()
            .status_code(),
        400
    );
}
