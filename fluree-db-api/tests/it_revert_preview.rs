//! Revert preview integration tests
//!
//! Cover the three preview entry points (single, set, range), conflict
//! detection, the `revertable` verdict for each strategy, the no-mutation
//! guarantee, and the cap/include flags on `RevertPreviewOpts`.

mod support;

use fluree_db_api::{
    CommitRef, ConflictStrategy, FlureeBuilder, RevertPreviewOpts,
};
use serde_json::json;

fn doc(id: &str, name: &str) -> serde_json::Value {
    json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": id, "ex:name": name}]
    })
}

/// Bootstrap an initial commit on `main` so subsequent commits aren't genesis.
async fn seed_anchor(
    fluree: &support::MemoryFluree,
    ledger: support::MemoryLedger,
) -> support::MemoryLedger {
    fluree
        .insert(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [{"@id": "ex:seed", "ex:tag": "anchor"}]
            }),
        )
        .await
        .unwrap()
        .ledger
}

// =============================================================================
// Happy path: single-commit preview, no conflicts
// =============================================================================

#[tokio::test]
async fn preview_single_commit_no_conflicts() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();
    let r1 = fluree.insert(ledger, &doc("ex:alice", "Alice")).await.unwrap();
    let r2 = fluree
        .insert(r1.ledger, &doc("ex:bob", "Bob"))
        .await
        .unwrap();
    let bob_cid = r2.receipt.commit_id.clone();

    let pre_record = fluree
        .nameservice()
        .lookup("mydb:main")
        .await
        .unwrap()
        .unwrap();

    let preview = fluree
        .revert_commit_preview("mydb", "main", CommitRef::Exact(bob_cid.clone()))
        .await
        .unwrap();

    assert_eq!(preview.branch, "main");
    assert_eq!(preview.reverted_count, 1);
    assert_eq!(preview.reverted_commits.len(), 1);
    assert_eq!(preview.reverted_commits[0].commit_id, bob_cid);
    assert!(!preview.truncated);
    assert_eq!(preview.conflicts.count, 0);
    assert!(preview.conflicts.keys.is_empty());
    assert!(preview.revertable);

    // Preview is read-only — branch state unchanged.
    let post_record = fluree
        .nameservice()
        .lookup("mydb:main")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pre_record.commit_t, post_record.commit_t);
    assert_eq!(pre_record.commit_head_id, post_record.commit_head_id);
}

// =============================================================================
// Range preview
// =============================================================================

#[tokio::test]
async fn preview_range_lists_inclusive_to_exclusive_from() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();
    let r1 = fluree.insert(ledger, &doc("ex:alice", "Alice")).await.unwrap();
    let alice_cid = r1.receipt.commit_id.clone();
    let r2 = fluree
        .insert(r1.ledger, &doc("ex:bob", "Bob"))
        .await
        .unwrap();
    let bob_cid = r2.receipt.commit_id.clone();
    let r3 = fluree
        .insert(r2.ledger, &doc("ex:carol", "Carol"))
        .await
        .unwrap();
    let carol_cid = r3.receipt.commit_id.clone();

    let preview = fluree
        .revert_range_preview(
            "mydb",
            "main",
            CommitRef::Exact(alice_cid),
            CommitRef::Exact(carol_cid.clone()),
        )
        .await
        .unwrap();

    assert_eq!(preview.reverted_count, 2);
    let cids: Vec<_> = preview
        .reverted_commits
        .iter()
        .map(|c| c.commit_id.clone())
        .collect();
    // Newest-first: carol then bob.
    assert_eq!(cids, vec![carol_cid, bob_cid]);
    assert_eq!(preview.conflicts.count, 0);
    assert!(preview.revertable);
}

// =============================================================================
// Set preview
// =============================================================================

#[tokio::test]
async fn preview_commits_set() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();
    let r1 = fluree.insert(ledger, &doc("ex:alice", "Alice")).await.unwrap();
    let r2 = fluree
        .insert(r1.ledger, &doc("ex:bob", "Bob"))
        .await
        .unwrap();
    let bob_cid = r2.receipt.commit_id.clone();
    let r3 = fluree
        .insert(r2.ledger, &doc("ex:carol", "Carol"))
        .await
        .unwrap();
    let _ = r3;

    let preview = fluree
        .revert_commits_preview("mydb", "main", vec![CommitRef::Exact(bob_cid.clone())])
        .await
        .unwrap();

    assert_eq!(preview.reverted_count, 1);
    assert_eq!(preview.reverted_commits[0].commit_id, bob_cid);
    assert_eq!(preview.conflicts.count, 0);
}

// =============================================================================
// Conflict detection
// =============================================================================

#[tokio::test]
async fn preview_reports_conflict_when_intervening_commit_overlaps() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();
    let anchor = seed_anchor(&fluree, ledger).await;

    let r1 = fluree
        .insert(anchor, &doc("ex:alice", "Alice"))
        .await
        .unwrap();
    let alice_v1_cid = r1.receipt.commit_id.clone();
    let _r2 = fluree
        .upsert(
            r1.ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [{"@id": "ex:alice", "ex:name": "Alice-v2"}]
            }),
        )
        .await
        .unwrap();

    let preview = fluree
        .revert_commit_preview("mydb", "main", CommitRef::Exact(alice_v1_cid))
        .await
        .unwrap();

    assert_eq!(preview.conflicts.count, 1, "expected one conflict key");
    assert_eq!(preview.conflicts.keys.len(), 1);
    assert!(!preview.revertable, "Abort + conflict ⇒ not revertable");
}

// =============================================================================
// Strategy controls the `revertable` verdict
// =============================================================================

#[tokio::test]
async fn preview_take_source_marks_conflict_revertable() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();
    let anchor = seed_anchor(&fluree, ledger).await;

    let r1 = fluree
        .insert(anchor, &doc("ex:alice", "Alice"))
        .await
        .unwrap();
    let alice_v1_cid = r1.receipt.commit_id.clone();
    let _r2 = fluree
        .upsert(
            r1.ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [{"@id": "ex:alice", "ex:name": "Alice-v2"}]
            }),
        )
        .await
        .unwrap();

    let preview = fluree
        .revert_commit_preview_with(
            "mydb",
            "main",
            CommitRef::Exact(alice_v1_cid),
            RevertPreviewOpts {
                conflict_strategy: ConflictStrategy::TakeSource,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    assert_eq!(preview.conflicts.count, 1);
    assert!(preview.revertable, "TakeSource always reports revertable");
}

// =============================================================================
// Caps / opt-outs
// =============================================================================

#[tokio::test]
async fn preview_max_commits_truncates() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();
    let r1 = fluree.insert(ledger, &doc("ex:alice", "Alice")).await.unwrap();
    let alice_cid = r1.receipt.commit_id.clone();
    let r2 = fluree
        .insert(r1.ledger, &doc("ex:bob", "Bob"))
        .await
        .unwrap();
    let r3 = fluree
        .insert(r2.ledger, &doc("ex:carol", "Carol"))
        .await
        .unwrap();
    let carol_cid = r3.receipt.commit_id.clone();
    let r4 = fluree
        .insert(r3.ledger, &doc("ex:dave", "Dave"))
        .await
        .unwrap();
    let _ = r4;

    let preview = fluree
        .revert_range_preview_with(
            "mydb",
            "main",
            CommitRef::Exact(alice_cid),
            CommitRef::Exact(carol_cid),
            RevertPreviewOpts {
                max_commits: Some(1),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    assert_eq!(preview.reverted_count, 2);
    assert_eq!(preview.reverted_commits.len(), 1);
    assert!(preview.truncated);
}

#[tokio::test]
async fn preview_include_conflicts_false_skips_conflict_computation() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();
    let anchor = seed_anchor(&fluree, ledger).await;

    let r1 = fluree
        .insert(anchor, &doc("ex:alice", "Alice"))
        .await
        .unwrap();
    let alice_v1_cid = r1.receipt.commit_id.clone();
    let _r2 = fluree
        .upsert(
            r1.ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [{"@id": "ex:alice", "ex:name": "Alice-v2"}]
            }),
        )
        .await
        .unwrap();

    let preview = fluree
        .revert_commit_preview_with(
            "mydb",
            "main",
            CommitRef::Exact(alice_v1_cid),
            RevertPreviewOpts {
                include_conflicts: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    // include_conflicts=false ⇒ empty conflict report regardless of reality.
    // `revertable` defers to the count, which is 0 here.
    assert_eq!(preview.conflicts.count, 0);
    assert!(preview.conflicts.keys.is_empty());
    assert!(preview.revertable);
}

// =============================================================================
// Validation: same checks as the mutating call
// =============================================================================

#[tokio::test]
async fn preview_rejects_genesis_commit() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();
    let r1 = fluree.insert(ledger, &doc("ex:alice", "Alice")).await.unwrap();
    let genesis_cid = r1.receipt.commit_id.clone();

    let err = fluree
        .revert_commit_preview("mydb", "main", CommitRef::Exact(genesis_cid))
        .await
        .expect_err("genesis cannot be reverted");

    assert!(
        err.to_string().to_lowercase().contains("genesis"),
        "expected genesis-rejection, got: {err}"
    );
}

#[tokio::test]
async fn preview_rejects_take_both_strategy() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();
    let r = fluree
        .insert(ledger, &doc("ex:alice", "Alice"))
        .await
        .unwrap();

    let err = fluree
        .revert_commit_preview_with(
            "mydb",
            "main",
            CommitRef::Exact(r.receipt.commit_id),
            RevertPreviewOpts {
                conflict_strategy: ConflictStrategy::TakeBoth,
                ..Default::default()
            },
        )
        .await
        .expect_err("TakeBoth is not supported for revert preview");

    assert!(err.to_string().contains("TakeBoth"));
}

#[tokio::test]
async fn preview_rejects_empty_commit_set() {
    let fluree = FlureeBuilder::memory().build_memory();
    fluree.create_ledger("mydb").await.unwrap();

    let err = fluree
        .revert_commits_preview("mydb", "main", vec![])
        .await
        .expect_err("empty commit set should be rejected");

    assert!(
        err.to_string().to_lowercase().contains("at least one"),
        "expected empty-list rejection, got: {err}"
    );
}
