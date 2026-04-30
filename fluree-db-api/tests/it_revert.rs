//! Revert integration tests
//!
//! Cover the linear revert lifecycle: single-commit revert, multi-commit
//! range revert (`A..B` syntax), arbitrary commit-set (cherry-pick) revert,
//! conflict-resolution strategies (`Abort`/`TakeSource`/`TakeBranch`), and
//! validation errors (merge commits, unreachable commit IDs, unsupported strategies).

mod support;

use fluree_db_api::{CommitRef, ConflictStrategy, FlureeBuilder};
use serde_json::json;

/// Extract sorted name strings from a JSON-LD query result row array.
fn extract_names(rows: &serde_json::Value) -> Vec<String> {
    let mut names: Vec<String> = rows
        .as_array()
        .expect("query result should be an array")
        .iter()
        .map(|r| {
            r.as_str()
                .map(std::string::ToString::to_string)
                .or_else(|| {
                    r.as_array().and_then(|a| {
                        a.first()
                            .and_then(|v| v.as_str())
                            .map(std::string::ToString::to_string)
                    })
                })
                .expect("each row should contain a string value")
        })
        .collect();
    names.sort();
    names
}

async fn query_all_names(fluree: &support::MemoryFluree, ledger_id: &str) -> Vec<String> {
    let ledger = fluree.ledger(ledger_id).await.unwrap();
    let query = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "select": ["?name"],
        "where": {"@id": "?s", "ex:name": "?name"}
    });
    let result = support::query_jsonld(fluree, &ledger, &query)
        .await
        .unwrap();
    let rows = result.to_jsonld(&ledger.snapshot).unwrap();
    extract_names(&rows)
}

fn doc(id: &str, name: &str) -> serde_json::Value {
    json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": id, "ex:name": name}]
    })
}

/// Bootstrap an initial commit on `main` so subsequent commits aren't genesis
/// (genesis commits cannot be reverted). Uses a predicate other than `ex:name`
/// so it is invisible to `query_all_names`.
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
// Happy path: single-commit revert (linear, no conflicts)
// =============================================================================

#[tokio::test]
async fn revert_single_commit_undoes_assertions() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();

    let r1 = fluree
        .insert(ledger, &doc("ex:alice", "Alice"))
        .await
        .unwrap();
    let r2 = fluree
        .insert(r1.ledger, &doc("ex:bob", "Bob"))
        .await
        .unwrap();
    let bob_cid = r2.receipt.commit_id.clone();

    // Sanity: both names present before revert.
    let before = query_all_names(&fluree, "mydb:main").await;
    assert_eq!(before, vec!["Alice", "Bob"]);

    let report = fluree
        .revert_commit(
            "mydb",
            "main",
            CommitRef::Exact(bob_cid.clone()),
            ConflictStrategy::TakeSource,
        )
        .await
        .unwrap();

    assert_eq!(report.reverted_commits, vec![bob_cid]);
    assert_eq!(report.conflict_count, 0);
    assert!(report.new_head_t > r2.receipt.t);

    // Bob's assertions should be undone; Alice still here.
    let after = query_all_names(&fluree, "mydb:main").await;
    assert_eq!(after, vec!["Alice"]);
}

// =============================================================================
// Happy path: multi-commit range revert (A..B git syntax)
// =============================================================================

#[tokio::test]
async fn revert_range_undoes_inclusive_to_exclusive_from() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();

    let r1 = fluree
        .insert(ledger, &doc("ex:alice", "Alice"))
        .await
        .unwrap();
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

    // alice_cid..carol_cid → reverts (alice, carol] = {bob, carol}.
    let report = fluree
        .revert_range(
            "mydb",
            "main",
            CommitRef::Exact(alice_cid),
            CommitRef::Exact(carol_cid),
            ConflictStrategy::TakeSource,
        )
        .await
        .unwrap();

    assert_eq!(
        report.reverted_commits.len(),
        2,
        "both bob and carol reverted"
    );
    assert_eq!(report.conflict_count, 0);

    let after = query_all_names(&fluree, "mydb:main").await;
    assert_eq!(after, vec!["Alice"]);
}

// =============================================================================
// Happy path: arbitrary commit-set (cherry-pick) revert
// =============================================================================

#[tokio::test]
async fn revert_commit_set_skips_intervening_commits() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();

    let r1 = fluree
        .insert(ledger, &doc("ex:alice", "Alice"))
        .await
        .unwrap();
    let r2 = fluree
        .insert(r1.ledger, &doc("ex:bob", "Bob"))
        .await
        .unwrap();
    let bob_cid = r2.receipt.commit_id.clone();
    let r3 = fluree
        .insert(r2.ledger, &doc("ex:carol", "Carol"))
        .await
        .unwrap();
    let r4 = fluree
        .insert(r3.ledger, &doc("ex:dave", "Dave"))
        .await
        .unwrap();
    let dave_cid = r4.receipt.commit_id.clone();

    // Revert just Bob and Dave; Carol should remain.
    let report = fluree
        .revert_commits(
            "mydb",
            "main",
            vec![CommitRef::Exact(bob_cid), CommitRef::Exact(dave_cid)],
            ConflictStrategy::TakeSource,
        )
        .await
        .unwrap();

    assert_eq!(report.reverted_commits.len(), 2);
    assert_eq!(report.conflict_count, 0);

    let after = query_all_names(&fluree, "mydb:main").await;
    assert_eq!(after, vec!["Alice", "Carol"]);
}

// =============================================================================
// Conflict resolution: Abort
// =============================================================================

#[tokio::test]
async fn revert_abort_on_conflict_leaves_branch_unchanged() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();
    let anchor = seed_anchor(&fluree, ledger).await;

    // alice@v1, then upsert alice@v2; reverting v1 conflicts with v2 on (alice, ex:name).
    let r1 = fluree
        .insert(
            anchor,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
            }),
        )
        .await
        .unwrap();
    let alice_v1 = r1.receipt.commit_id.clone();

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

    let pre_record = fluree
        .nameservice()
        .lookup("mydb:main")
        .await
        .unwrap()
        .unwrap();

    let err = fluree
        .revert_commits(
            "mydb",
            "main",
            vec![CommitRef::Exact(alice_v1)],
            ConflictStrategy::Abort,
        )
        .await
        .expect_err("revert with Abort should fail on conflict");

    assert!(
        err.to_string().to_lowercase().contains("conflict"),
        "expected conflict error, got: {err}"
    );

    let post_record = fluree
        .nameservice()
        .lookup("mydb:main")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pre_record.commit_t, post_record.commit_t);
    assert_eq!(pre_record.commit_head_id, post_record.commit_head_id);

    // Data unchanged: HEAD value still applies.
    let names = query_all_names(&fluree, "mydb:main").await;
    assert_eq!(names, vec!["Alice-v2"]);
}

// =============================================================================
// Conflict resolution: TakeSource (revert wins)
// =============================================================================

#[tokio::test]
async fn revert_take_source_overrides_intervening_value() {
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

    let report = fluree
        .revert_commits(
            "mydb",
            "main",
            vec![CommitRef::Exact(alice_v1_cid)],
            ConflictStrategy::TakeSource,
        )
        .await
        .unwrap();

    assert_eq!(report.conflict_count, 1);
    assert_eq!(report.strategy, "take-source");

    // Revert wins: ex:name on alice should now be empty.
    let names = query_all_names(&fluree, "mydb:main").await;
    assert!(
        !names.contains(&"Alice".to_string()) && !names.contains(&"Alice-v2".to_string()),
        "expected alice's name to be retracted, got: {names:?}"
    );
}

// =============================================================================
// Conflict resolution: TakeBranch (HEAD wins)
// =============================================================================

#[tokio::test]
async fn revert_take_branch_preserves_head_value() {
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

    let report = fluree
        .revert_commits(
            "mydb",
            "main",
            vec![CommitRef::Exact(alice_v1_cid)],
            ConflictStrategy::TakeBranch,
        )
        .await
        .unwrap();

    assert_eq!(report.conflict_count, 1);
    assert_eq!(report.strategy, "take-branch");

    // HEAD wins: Alice-v2 still there.
    let names = query_all_names(&fluree, "mydb:main").await;
    assert_eq!(names, vec!["Alice-v2"]);
}

// =============================================================================
// Validation: rejects merge commits in the range
// =============================================================================

#[tokio::test]
async fn revert_rejects_merge_commits_in_range() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();

    let main_ledger = fluree
        .insert(ledger, &doc("ex:alice", "Alice"))
        .await
        .unwrap()
        .ledger;

    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();

    let dev_ledger = fluree.ledger("mydb:dev").await.unwrap();
    fluree
        .insert(dev_ledger, &doc("ex:bob", "Bob"))
        .await
        .unwrap();

    fluree
        .insert(main_ledger, &doc("ex:carol", "Carol"))
        .await
        .unwrap();

    // General merge produces a 2-parent merge commit on main.
    let merge_report = fluree
        .merge_branch("mydb", "dev", None, ConflictStrategy::default())
        .await
        .unwrap();
    assert!(!merge_report.fast_forward);
    let merge_cid = merge_report.new_head_id;

    let err = fluree
        .revert_commits(
            "mydb",
            "main",
            vec![CommitRef::Exact(merge_cid)],
            ConflictStrategy::TakeSource,
        )
        .await
        .expect_err("merge commits cannot be reverted in v1");

    assert!(
        err.to_string().to_lowercase().contains("merge"),
        "expected merge-commit rejection, got: {err}"
    );
}

// =============================================================================
// Validation: unreachable commit ID
// =============================================================================

#[tokio::test]
async fn revert_rejects_commit_not_in_branch_ancestry() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();

    let _r1 = fluree
        .insert(ledger, &doc("ex:alice", "Alice"))
        .await
        .unwrap();

    // Commit IDs from a sibling ledger are unreachable in this branch.
    let other_ledger = fluree.create_ledger("other").await.unwrap();
    let other_r = fluree
        .insert(other_ledger, &doc("ex:zelda", "Zelda"))
        .await
        .unwrap();
    let foreign_cid = other_r.receipt.commit_id;

    let err = fluree
        .revert_commits(
            "mydb",
            "main",
            vec![CommitRef::Exact(foreign_cid)],
            ConflictStrategy::TakeSource,
        )
        .await
        .expect_err("unreachable commit ID should fail validation");

    assert!(
        err.to_string().to_lowercase().contains("not reachable"),
        "expected reachability error, got: {err}"
    );
}

// =============================================================================
// Validation: unsupported strategies
// =============================================================================

#[tokio::test]
async fn revert_rejects_take_both_strategy() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();
    let r = fluree
        .insert(ledger, &doc("ex:alice", "Alice"))
        .await
        .unwrap();

    let err = fluree
        .revert_commits(
            "mydb",
            "main",
            vec![CommitRef::Exact(r.receipt.commit_id)],
            ConflictStrategy::TakeBoth,
        )
        .await
        .expect_err("TakeBoth is not supported for revert");

    assert!(
        err.to_string().contains("TakeBoth"),
        "expected TakeBoth-rejection, got: {err}"
    );
}

#[tokio::test]
async fn revert_rejects_skip_strategy() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();
    let r = fluree
        .insert(ledger, &doc("ex:alice", "Alice"))
        .await
        .unwrap();

    let err = fluree
        .revert_commits(
            "mydb",
            "main",
            vec![CommitRef::Exact(r.receipt.commit_id)],
            ConflictStrategy::Skip,
        )
        .await
        .expect_err("Skip is not supported for revert");

    assert!(
        err.to_string().contains("Skip"),
        "expected Skip-rejection, got: {err}"
    );
}

// =============================================================================
// Validation: range start equal to end
// =============================================================================

#[tokio::test]
async fn revert_rejects_range_with_no_commits() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();
    let r = fluree
        .insert(ledger, &doc("ex:alice", "Alice"))
        .await
        .unwrap();
    let cid = r.receipt.commit_id.clone();

    let err = fluree
        .revert_range(
            "mydb",
            "main",
            CommitRef::Exact(cid.clone()),
            CommitRef::Exact(cid),
            ConflictStrategy::TakeSource,
        )
        .await
        .expect_err("zero-commit range should be rejected");

    assert!(
        err.to_string().to_lowercase().contains("ancestor")
            || err.to_string().to_lowercase().contains("zero"),
        "expected empty-range error, got: {err}"
    );
}

// =============================================================================
// Provenance: txn_meta records reverted commit IDs
// =============================================================================

#[tokio::test]
async fn revert_records_reverted_commit_ids_in_txn_meta() {
    use fluree_db_core::commit::{load_commit_by_id, TxnMetaValue};
    use fluree_db_core::ContentStore;

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();
    let r1 = fluree
        .insert(ledger, &doc("ex:alice", "Alice"))
        .await
        .unwrap();
    let r2 = fluree
        .insert(r1.ledger, &doc("ex:bob", "Bob"))
        .await
        .unwrap();
    let bob_cid = r2.receipt.commit_id.clone();

    let report = fluree
        .revert_commits(
            "mydb",
            "main",
            vec![CommitRef::Exact(bob_cid.clone())],
            ConflictStrategy::TakeSource,
        )
        .await
        .unwrap();

    let store = fluree.content_store("mydb:main");
    let _ = ContentStore::get(store.as_ref(), &report.new_head_id).await;
    let revert_commit = load_commit_by_id(store.as_ref(), &report.new_head_id)
        .await
        .unwrap();

    let reverts: Vec<&TxnMetaValue> = revert_commit
        .txn_meta
        .iter()
        .filter(|e| {
            e.predicate_ns == fluree_vocab::namespaces::FLUREE_DB
                && e.predicate_name == fluree_vocab::db::REVERTS
        })
        .map(|e| &e.value)
        .collect();
    assert_eq!(reverts.len(), 1);
    match reverts[0] {
        TxnMetaValue::String(s) => assert_eq!(s, &bob_cid.to_string()),
        other => panic!("expected string-valued reverts entry, got {other:?}"),
    }
}
