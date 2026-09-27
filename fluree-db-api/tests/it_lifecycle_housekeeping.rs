//! Lifecycle housekeeping: finishing the drops, restores and purges a crash
//! interrupted, leaving creates alone, and the scheduled orphan sweep.
//!
//! Each test leaves the nameservice exactly as a crash at one step would,
//! then drives the housekeeping's ticks directly. An operation is resumed on
//! the second tick that sees it unchanged.

#![cfg(feature = "native")]

use fluree_db_api::{Fluree, FlureeBuilder};
use fluree_db_core::{LedgerId, LedgerName};
use fluree_db_nameservice::{
    lifecycle, BindingState, BranchFence, DroppedState, Fence, NameBinding,
};
use serde_json::json;
use std::sync::Arc;

async fn fluree_with(ledgers: &[&str]) -> (tempfile::TempDir, Arc<Fluree>) {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let fluree = Arc::new(
        FlureeBuilder::file(tmp.path().to_string_lossy().to_string())
            .build()
            .expect("build"),
    );
    let tx = json!({"@context": {"ex": "http://example.org/"}, "@id": "ex:a", "ex:name": "A"});
    for id in ledgers {
        let ledger = fluree.create_ledger(id).await.expect("create");
        fluree.insert(ledger, &tx).await.expect("insert");
    }
    (tmp, fluree)
}

fn name(s: &str) -> LedgerName {
    LedgerName::parse(s).unwrap()
}

/// Two ticks: the first sees the state, the second finds it unchanged.
async fn tick_twice(fluree: &Arc<Fluree>) {
    let housekeeping = fluree.lifecycle_housekeeping(None);
    housekeeping.tick().await;
    housekeeping.tick().await;
}

/// Put `name`'s binding in `state`, as the first step of an operation does.
async fn set_binding_state(fluree: &Fluree, ledger: &str, state: BindingState) {
    let store = fluree.publisher().unwrap();
    let current = store.get_binding(ledger).await.unwrap().expect("binding");
    let next = NameBinding {
        state,
        ..current.value
    };
    store
        .cas_binding(ledger, Some(current.version), Some(&next))
        .await
        .unwrap();
}

async fn files_under(fluree: &Fluree, root: &str) -> usize {
    let storage = fluree.admin_storage().expect("managed backend");
    storage
        .list_prefix(&format!("fluree:file://{root}/"))
        .await
        .expect("list")
        .len()
}

#[tokio::test]
async fn a_drop_stopped_after_claiming_the_name_is_finished() {
    let (_tmp, fluree) = fluree_with(&["stuck:main"]).await;
    let root = fluree
        .storage_namespace("stuck:main")
        .await
        .unwrap()
        .root()
        .to_string();
    set_binding_state(&fluree, "stuck", BindingState::Dropping { hard: true }).await;

    let housekeeping = fluree.lifecycle_housekeeping(None);
    housekeeping.tick().await;
    let store = fluree.publisher().unwrap();
    assert!(
        store.get_binding("stuck").await.unwrap().is_some(),
        "the first tick only looks"
    );
    housekeeping.tick().await;

    assert!(
        store.get_binding("stuck").await.unwrap().is_none(),
        "name freed"
    );
    assert!(
        fluree.list_dropped().await.unwrap().is_empty(),
        "hard: purged"
    );
    assert_eq!(files_under(&fluree, &root).await, 0, "data deleted");
}

#[tokio::test]
async fn a_purge_stopped_before_deleting_is_finished() {
    let (_tmp, fluree) = fluree_with(&["purged:main"]).await;
    let root = fluree
        .storage_namespace("purged:main")
        .await
        .unwrap()
        .root()
        .to_string();
    let instance = fluree
        .drop_ledger("purged", fluree_db_api::DropMode::Soft)
        .await
        .unwrap()
        .instance
        .unwrap();
    lifecycle::begin_purge(fluree.publisher().unwrap(), &instance)
        .await
        .unwrap();
    assert!(files_under(&fluree, &root).await > 0);

    tick_twice(&fluree).await;

    assert!(fluree.list_dropped().await.unwrap().is_empty());
    assert_eq!(files_under(&fluree, &root).await, 0);
}

#[tokio::test]
async fn a_restore_stopped_after_marking_its_entry_is_finished() {
    let (_tmp, fluree) = fluree_with(&["restored:main"]).await;
    let instance = fluree
        .drop_ledger("restored", fluree_db_api::DropMode::Soft)
        .await
        .unwrap()
        .instance
        .unwrap();
    let store = fluree.publisher().unwrap();
    let entry = store.get_dropped(&instance).await.unwrap().unwrap();
    let fences = entry
        .value
        .branches
        .iter()
        .map(|r| BranchFence::new(&r.branch, Fence::generate()))
        .collect();
    let restoring = fluree_db_nameservice::DroppedLedger {
        state: DroppedState::Restoring { fences },
        ..entry.value
    };
    store
        .cas_dropped(&instance, Some(entry.version), Some(&restoring))
        .await
        .unwrap();

    tick_twice(&fluree).await;

    assert!(fluree.list_dropped().await.unwrap().is_empty());
    assert!(fluree.db("restored:main").await.expect("restored").t > 0);
}

#[tokio::test]
async fn a_branch_drop_stopped_before_deleting_is_finished() {
    let (_tmp, fluree) = fluree_with(&["branched:main"]).await;
    fluree
        .create_branch("branched", "dev", None, None)
        .await
        .unwrap();
    let dev_root = fluree
        .storage_namespace("branched:dev")
        .await
        .unwrap()
        .branch_prefix()
        .to_string();
    let begun = lifecycle::begin_drop_branch(fluree.publisher().unwrap(), &name("branched"), "dev")
        .await
        .unwrap();
    assert!(matches!(begun, lifecycle::BranchDrop::Purge(_)));

    tick_twice(&fluree).await;

    let binding = fluree
        .publisher()
        .unwrap()
        .get_binding("branched")
        .await
        .unwrap()
        .unwrap();
    assert!(binding.value.listing("dev").is_none(), "unlisted");
    assert_eq!(files_under(&fluree, &dev_root).await, 0);
    assert!(fluree.db("branched:main").await.unwrap().t > 0);
}

/// An import holds its create open while it runs, so an unfinished create
/// is never rolled back.
#[tokio::test]
async fn an_unfinished_create_is_left_alone() {
    let (_tmp, fluree) = fluree_with(&[]).await;
    lifecycle::begin_create(
        fluree.publisher().unwrap(),
        &LedgerId::parse("importing:main").unwrap(),
    )
    .await
    .unwrap();

    let housekeeping = fluree.lifecycle_housekeeping(None);
    for _ in 0..3 {
        housekeeping.tick().await;
    }

    let binding = fluree
        .publisher()
        .unwrap()
        .get_binding("importing")
        .await
        .unwrap()
        .expect("still claimed");
    assert_eq!(binding.value.state, BindingState::Creating);
}

/// A drop that finishes between two ticks, followed by a new ledger under
/// the name, is not resumed into dropping the new ledger.
#[tokio::test]
async fn a_name_reused_between_ticks_is_left_alone() {
    let (_tmp, fluree) = fluree_with(&["reused:main"]).await;
    set_binding_state(&fluree, "reused", BindingState::Dropping { hard: true }).await;
    let housekeeping = fluree.lifecycle_housekeeping(None);
    housekeeping.tick().await;

    fluree
        .drop_ledger("reused", fluree_db_api::DropMode::Hard)
        .await
        .unwrap();
    let ledger = fluree.create_ledger("reused:main").await.unwrap();
    let tx = json!({"@context": {"ex": "http://example.org/"}, "@id": "ex:b", "ex:name": "B"});
    fluree.insert(ledger, &tx).await.unwrap();
    housekeeping.tick().await;
    housekeeping.tick().await;

    assert!(fluree.db("reused:main").await.expect("the new ledger").t > 0);
}

#[tokio::test]
async fn the_orphan_sweep_runs_only_when_an_interval_is_set() {
    let (_tmp, fluree) = fluree_with(&[]).await;
    let orphan = "fluree:file://gone/@01JB8ZK4X5Y6Z7A8B9C0D1E2F3/main/commit/x.fcv2";
    let storage = fluree.admin_storage().unwrap();
    storage.write_bytes(orphan, b"x").await.unwrap();

    fluree.lifecycle_housekeeping(None).tick().await;
    assert!(
        storage.exists(orphan).await.unwrap(),
        "no interval: no sweep"
    );

    fluree
        .lifecycle_housekeeping(Some(std::time::Duration::ZERO))
        .tick()
        .await;
    assert!(!storage.exists(orphan).await.unwrap());
}
