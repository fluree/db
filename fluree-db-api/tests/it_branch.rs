//! Branch integration tests
//!
//! Tests the branch lifecycle: creating branches, transacting on branches
//! independently, and verifying data isolation between branches.

mod support;

use fluree_db_api::{CommitRef, FlureeBuilder};
use serde_json::json;

/// Extract sorted name strings from query result rows.
///
/// Handles both flat strings and single-element arrays, which are the two
/// formats that single-variable `select` may return.
fn extract_names(rows: &serde_json::Value) -> Vec<String> {
    let mut names: Vec<String> = rows
        .as_array()
        .expect("query result should be an array")
        .iter()
        .map(|r| {
            r.as_str()
                .map(std::string::ToString::to_string)
                .or_else(|| {
                    r.as_array()
                        .and_then(|a| a[0].as_str().map(std::string::ToString::to_string))
                })
                .expect("each row should contain a string value")
        })
        .collect();
    names.sort();
    names
}

/// Create a branch and verify it appears in the branch list.
#[tokio::test]
async fn create_and_list_branches() {
    let fluree = FlureeBuilder::memory().build_memory();

    // Create ledger and transact initial data so commit_head_id is set
    let ledger = fluree.create_ledger("mydb").await.unwrap();
    let txn = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:seed", "ex:val": 1}]
    });
    fluree.insert(ledger, &txn).await.unwrap();

    // Create a branch
    let record = fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();
    assert_eq!(record.branch, "dev");
    assert_eq!(record.ledger_id, "mydb:dev");
    assert_eq!(
        record.source_branch.as_deref(),
        Some("main"),
        "branch should record its source"
    );

    // List branches
    let branches = fluree.list_branches("mydb").await.unwrap();
    let mut names: Vec<&str> = branches.iter().map(|r| r.branch.as_str()).collect();
    names.sort();
    assert_eq!(names, vec!["dev", "main"]);
}

/// Creating a duplicate branch returns a LedgerExists error.
#[tokio::test]
async fn create_branch_duplicate_fails() {
    let fluree = FlureeBuilder::memory().build_memory();

    let ledger = fluree.create_ledger("mydb").await.unwrap();
    let txn = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:seed", "ex:val": 1}]
    });
    fluree.insert(ledger, &txn).await.unwrap();

    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();
    let err = fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .expect_err("duplicate branch creation should fail");
    assert!(
        err.to_string().contains("already exists"),
        "expected LedgerExists error, got: {err}"
    );
}

/// Invalid branch names are rejected.
#[tokio::test]
async fn create_branch_invalid_name() {
    let fluree = FlureeBuilder::memory().build_memory();

    let ledger = fluree.create_ledger("mydb").await.unwrap();
    let txn = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:seed", "ex:val": 1}]
    });
    fluree.insert(ledger, &txn).await.unwrap();

    // Empty name
    assert!(fluree.create_branch("mydb", "", None, None).await.is_err());

    // Contains colon
    assert!(fluree
        .create_branch("mydb", "foo:bar", None, None)
        .await
        .is_err());

    // Contains @
    assert!(fluree
        .create_branch("mydb", "foo@bar", None, None)
        .await
        .is_err());

    // Path traversal
    assert!(fluree
        .create_branch("mydb", "..", None, None)
        .await
        .is_err());
}

/// Creating a branch from a non-existent source returns not-found.
#[tokio::test]
async fn create_branch_missing_source() {
    let fluree = FlureeBuilder::memory().build_memory();
    fluree.create_ledger("mydb").await.unwrap();

    let err = fluree
        .create_branch("mydb", "dev", Some("nonexistent"), None)
        .await
        .expect_err("missing source branch should fail");
    assert!(
        err.to_string().contains("not found") || err.to_string().contains("Not found"),
        "expected NotFound error, got: {err}"
    );
}

/// Transact divergent data on two branches and verify isolation.
///
/// This is the core branching test: after branching, transactions on one
/// branch must not be visible on the other.
#[tokio::test]
async fn branch_data_isolation() {
    let fluree = FlureeBuilder::memory().build_memory();

    // 1. Create ledger and insert shared base data on main
    let ledger = fluree.create_ledger("mydb").await.unwrap();
    let base_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [
            {"@id": "ex:alice", "ex:name": "Alice"}
        ]
    });
    let result = fluree.insert(ledger, &base_data).await.unwrap();
    let main_after_base = result.ledger;

    // 2. Create branch "dev" from main
    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();

    // 3. Transact data only on main
    let main_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [
            {"@id": "ex:bob", "ex:name": "Bob"}
        ]
    });
    let result = fluree.insert(main_after_base, &main_data).await.unwrap();
    let main_latest = result.ledger;

    // 4. Transact different data only on dev
    let dev_ledger = fluree.ledger("mydb:dev").await.unwrap();
    let dev_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [
            {"@id": "ex:carol", "ex:name": "Carol"}
        ]
    });
    let result = fluree.insert(dev_ledger, &dev_data).await.unwrap();
    let dev_latest = result.ledger;

    // 5. Query both branches for all names
    let query = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "select": ["?name"],
        "where": {"@id": "?s", "ex:name": "?name"}
    });

    let main_result = support::query_jsonld(&fluree, &main_latest, &query)
        .await
        .unwrap();
    let main_rows = main_result.to_jsonld(&main_latest.snapshot).unwrap();

    let dev_result = support::query_jsonld(&fluree, &dev_latest, &query)
        .await
        .unwrap();
    let dev_rows = dev_result.to_jsonld(&dev_latest.snapshot).unwrap();

    // Main has Alice (base) + Bob (main-only)
    assert_eq!(extract_names(&main_rows), vec!["Alice", "Bob"]);

    // Dev has Alice (base) + Carol (dev-only), but NOT Bob
    assert_eq!(extract_names(&dev_rows), vec!["Alice", "Carol"]);
}

/// A branch starts at the same t as the source and advances independently.
#[tokio::test]
async fn branch_t_advances_independently() {
    let fluree = FlureeBuilder::memory().build_memory();

    let ledger = fluree.create_ledger("mydb").await.unwrap();
    let txn = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:seed", "ex:val": 1}]
    });
    let result = fluree.insert(ledger, &txn).await.unwrap();
    assert_eq!(result.receipt.t, 1);

    // Branch at t=1
    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();

    // Transact twice on dev
    let dev = fluree.ledger("mydb:dev").await.unwrap();
    let txn2 = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:a", "ex:val": 2}]
    });
    let result = fluree.insert(dev, &txn2).await.unwrap();
    assert_eq!(result.receipt.t, 2);

    let txn3 = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:b", "ex:val": 3}]
    });
    let result = fluree.insert(result.ledger, &txn3).await.unwrap();
    assert_eq!(result.receipt.t, 3);

    // Main is still at t=1
    let main = fluree.ledger("mydb:main").await.unwrap();
    assert_eq!(main.t(), 1);

    // Dev is at t=3
    let dev = fluree.ledger("mydb:dev").await.unwrap();
    assert_eq!(dev.t(), 3);
}

/// Branch at a historical commit via `CommitRef::Exact`.
///
/// Main advances to t=3. We branch at the t=2 commit and verify the new
/// branch starts at t=2 with no index (replay-from-genesis path).
#[tokio::test]
async fn create_branch_at_historical_commit() {
    let fluree = FlureeBuilder::memory().build_memory();

    let ledger = fluree.create_ledger("mydb").await.unwrap();
    let ctx = json!({"ex": "http://example.org/ns/"});

    // t=1, t=2, t=3 on main — capture the t=2 commit id
    let r1 = fluree
        .insert(
            ledger,
            &json!({"@context": ctx, "@graph": [{"@id": "ex:a", "ex:val": 1}]}),
        )
        .await
        .unwrap();
    let r2 = fluree
        .insert(
            r1.ledger,
            &json!({"@context": ctx, "@graph": [{"@id": "ex:b", "ex:val": 2}]}),
        )
        .await
        .unwrap();
    let t2_commit_id = r2.receipt.commit_id.clone();
    let _r3 = fluree
        .insert(
            r2.ledger,
            &json!({"@context": ctx, "@graph": [{"@id": "ex:c", "ex:val": 3}]}),
        )
        .await
        .unwrap();

    // Branch at t=2
    let record = fluree
        .create_branch(
            "mydb",
            "historical",
            None,
            Some(CommitRef::Exact(t2_commit_id.clone())),
        )
        .await
        .unwrap();

    assert_eq!(record.commit_head_id.as_ref(), Some(&t2_commit_id));
    assert_eq!(record.commit_t, 2);
    assert!(
        record.index_head_id.is_none(),
        "historical branch should skip index copy"
    );
    assert_eq!(record.source_branch.as_deref(), Some("main"));

    // The branch loads at t=2, not at main's current head (t=3)
    let branch = fluree.ledger("mydb:historical").await.unwrap();
    assert_eq!(branch.t(), 2);
}

/// Branch at a historical commit via `CommitRef::T`.
///
/// Resolution scans the txn-meta graph for a commit with matching `t`.
/// The scan includes the novelty overlay, so freshly committed transactions
/// are visible without running the indexer.
#[tokio::test]
async fn create_branch_at_t() {
    let fluree = FlureeBuilder::memory().build_memory();

    let ledger = fluree.create_ledger("mydb").await.unwrap();
    let ctx = json!({"ex": "http://example.org/ns/"});

    let r1 = fluree
        .insert(
            ledger,
            &json!({"@context": ctx, "@graph": [{"@id": "ex:a", "ex:val": 1}]}),
        )
        .await
        .unwrap();
    let r2 = fluree
        .insert(
            r1.ledger,
            &json!({"@context": ctx, "@graph": [{"@id": "ex:b", "ex:val": 2}]}),
        )
        .await
        .unwrap();
    let t2_commit_id = r2.receipt.commit_id.clone();
    let _r3 = fluree
        .insert(
            r2.ledger,
            &json!({"@context": ctx, "@graph": [{"@id": "ex:c", "ex:val": 3}]}),
        )
        .await
        .unwrap();

    let record = fluree
        .create_branch("mydb", "historical", None, Some(CommitRef::T(2)))
        .await
        .unwrap();

    assert_eq!(record.commit_head_id.as_ref(), Some(&t2_commit_id));
    assert_eq!(record.commit_t, 2);
}

/// Branch at a historical commit via `CommitRef::Prefix` using a hex digest.
///
/// Like `T`, the prefix resolver scans novelty and tolerates unindexed sources.
#[tokio::test]
async fn create_branch_at_prefix() {
    let fluree = FlureeBuilder::memory().build_memory();

    let ledger = fluree.create_ledger("mydb").await.unwrap();
    let ctx = json!({"ex": "http://example.org/ns/"});

    let r1 = fluree
        .insert(
            ledger,
            &json!({"@context": ctx, "@graph": [{"@id": "ex:a", "ex:val": 1}]}),
        )
        .await
        .unwrap();
    let r2 = fluree
        .insert(
            r1.ledger,
            &json!({"@context": ctx, "@graph": [{"@id": "ex:b", "ex:val": 2}]}),
        )
        .await
        .unwrap();
    let t2_commit_id = r2.receipt.commit_id.clone();
    let _r3 = fluree
        .insert(
            r2.ledger,
            &json!({"@context": ctx, "@graph": [{"@id": "ex:c", "ex:val": 3}]}),
        )
        .await
        .unwrap();

    // An 8-hex-char prefix is plenty to uniquely identify the commit in a
    // tiny test ledger; the resolver requires >= 6 chars.
    let prefix = t2_commit_id.digest_hex()[..8].to_string();
    let record = fluree
        .create_branch("mydb", "historical", None, Some(CommitRef::Prefix(prefix)))
        .await
        .unwrap();

    assert_eq!(record.commit_head_id.as_ref(), Some(&t2_commit_id));
    assert_eq!(record.commit_t, 2);
}

/// Branching from a commit that isn't reachable from source HEAD is rejected.
#[tokio::test]
async fn create_branch_at_non_ancestor_commit_fails() {
    use fluree_db_api::ContentId;
    use fluree_db_core::ContentKind;

    let fluree = FlureeBuilder::memory().build_memory();

    let ledger = fluree.create_ledger("mydb").await.unwrap();
    fluree
        .insert(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [{"@id": "ex:seed", "ex:val": 1}]
            }),
        )
        .await
        .unwrap();

    // Fabricate a CID that isn't in the ledger's history
    let bogus = ContentId::new(ContentKind::Commit, b"not-a-real-commit");

    let err = fluree
        .create_branch("mydb", "dev", None, Some(CommitRef::Exact(bogus)))
        .await
        .expect_err("non-ancestor commit should be rejected");
    let msg = err.to_string();
    assert!(
        msg.contains("not found") || msg.contains("not an ancestor"),
        "expected not-found/not-ancestor error, got: {msg}"
    );
}

/// Dropping a leaf branch (no children) fully deletes it.
#[tokio::test]
async fn drop_branch_leaf() {
    let fluree = FlureeBuilder::memory().build_memory();

    let ledger = fluree.create_ledger("mydb").await.unwrap();
    let txn = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:seed", "ex:val": 1}]
    });
    fluree.insert(ledger, &txn).await.unwrap();

    // Create and then drop a leaf branch
    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();
    let report = fluree.drop_branch("mydb", "dev").await.unwrap();

    assert!(!report.deferred, "leaf branch should not be deferred");
    assert_eq!(report.ledger_id, "mydb:dev");

    // Branch should no longer be in the list
    let branches = fluree.list_branches("mydb").await.unwrap();
    let names: Vec<&str> = branches.iter().map(|r| r.branch.as_str()).collect();
    assert_eq!(names, vec!["main"]);
}

/// Cannot drop the root branch. "main" is the default, so dropping it on a
/// freshly-created ledger is refused — but the refusal is record-based
/// (source_branch.is_none()), not name-based.
#[tokio::test]
async fn drop_main_refused() {
    let fluree = FlureeBuilder::memory().build_memory();
    fluree.create_ledger("mydb").await.unwrap();

    let err = fluree
        .drop_branch("mydb", "main")
        .await
        .expect_err("dropping main should fail");
    assert!(
        err.to_string().contains("root branch"),
        "error should mention root branch: {err}"
    );
}

/// The root-branch refusal is structural — a ledger whose initial branch is
/// "trunk" (not the default "main") has `trunk` as its root, and
/// `drop_branch` must refuse `trunk` regardless of its name.
#[tokio::test]
async fn drop_branch_refuses_non_main_root() {
    let fluree = FlureeBuilder::memory().build_memory();
    fluree.create_ledger("mydb:trunk").await.unwrap();

    let err = fluree
        .drop_branch("mydb", "trunk")
        .await
        .expect_err("dropping the root (named trunk) should fail");
    let msg = err.to_string();
    assert!(
        msg.contains("root branch") && msg.contains("trunk"),
        "error should mention root and trunk: {msg}"
    );
}

/// Conversely, a non-root branch named "main" is droppable — the refusal
/// triggers on the record's source_branch, not on the literal string "main".
#[tokio::test]
async fn drop_branch_allows_non_root_named_main() {
    let fluree = FlureeBuilder::memory().build_memory();
    let trunk = fluree.create_ledger("mydb:trunk").await.unwrap();
    let txn = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:seed", "ex:val": 1}]
    });
    fluree.insert(trunk, &txn).await.unwrap();

    fluree
        .create_branch("mydb", "main", Some("trunk"), None)
        .await
        .unwrap();

    let report = fluree.drop_branch("mydb", "main").await.unwrap();
    assert_eq!(report.status, fluree_db_api::DropStatus::Dropped);
}

/// branches count is incremented on create and decremented on drop.
#[tokio::test]
async fn branches_count_tracks_children() {
    let fluree = FlureeBuilder::memory().build_memory();

    let ledger = fluree.create_ledger("mydb").await.unwrap();
    let txn = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:seed", "ex:val": 1}]
    });
    fluree.insert(ledger, &txn).await.unwrap();

    // main starts with branches == 0
    let record = fluree
        .nameservice()
        .lookup("mydb:main")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.branches, 0);

    // Create two child branches
    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();
    let record = fluree
        .nameservice()
        .lookup("mydb:main")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.branches, 1);

    fluree
        .create_branch("mydb", "staging", None, None)
        .await
        .unwrap();
    let record = fluree
        .nameservice()
        .lookup("mydb:main")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.branches, 2);

    // Drop one
    fluree.drop_branch("mydb", "dev").await.unwrap();
    let record = fluree
        .nameservice()
        .lookup("mydb:main")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.branches, 1);

    // Drop the other
    fluree.drop_branch("mydb", "staging").await.unwrap();
    let record = fluree
        .nameservice()
        .lookup("mydb:main")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.branches, 0);
}

/// Branch with children is retracted (deferred) but storage is preserved.
/// Children can still operate after parent is deferred.
#[tokio::test]
async fn drop_branch_with_children_deferred() {
    let fluree = FlureeBuilder::memory().build_memory();

    let ledger = fluree.create_ledger("mydb").await.unwrap();
    let txn = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:seed", "ex:val": 1}]
    });
    fluree.insert(ledger, &txn).await.unwrap();

    // Create dev, then branch feature from dev
    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();
    fluree
        .create_branch("mydb", "feature", Some("dev"), None)
        .await
        .unwrap();

    // dev now has branches == 1
    let record = fluree
        .nameservice()
        .lookup("mydb:dev")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.branches, 1);

    // Drop dev — should be deferred because it has a child
    let report = fluree.drop_branch("mydb", "dev").await.unwrap();
    assert!(report.deferred, "branch with children should be deferred");

    // dev is retracted but still present
    let record = fluree
        .nameservice()
        .lookup("mydb:dev")
        .await
        .unwrap()
        .unwrap();
    assert!(record.retracted);

    // feature still works (can transact)
    let feature = fluree.ledger("mydb:feature").await.unwrap();
    let txn2 = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:new", "ex:val": 2}]
    });
    fluree.insert(feature, &txn2).await.unwrap();
}

/// Transacting on a retracted branch fails.
#[tokio::test]
async fn transact_on_retracted_branch_fails() {
    let fluree = FlureeBuilder::memory().build_memory();

    let ledger = fluree.create_ledger("mydb").await.unwrap();
    let txn = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:seed", "ex:val": 1}]
    });
    fluree.insert(ledger, &txn).await.unwrap();

    // Create dev, then a child so dev can be retracted (not purged)
    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();
    fluree
        .create_branch("mydb", "feature", Some("dev"), None)
        .await
        .unwrap();

    // Load dev ledger state before retraction
    let dev = fluree.ledger("mydb:dev").await.unwrap();

    // Retract dev (deferred because it has a child)
    fluree.drop_branch("mydb", "dev").await.unwrap();

    // Attempting to transact on the retracted branch should fail
    let txn2 = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:new", "ex:val": 2}]
    });
    let err = fluree
        .insert(dev, &txn2)
        .await
        .expect_err("transacting on retracted branch should fail");
    assert!(
        err.to_string().to_lowercase().contains("retracted"),
        "error should mention retraction: {err}"
    );
}

/// Dropping the last child of a retracted parent cascades to purge the parent.
#[tokio::test]
async fn drop_branch_cascade() {
    let fluree = FlureeBuilder::memory().build_memory();

    let ledger = fluree.create_ledger("mydb").await.unwrap();
    let txn = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:seed", "ex:val": 1}]
    });
    fluree.insert(ledger, &txn).await.unwrap();

    // main -> dev -> feature
    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();
    fluree
        .create_branch("mydb", "feature", Some("dev"), None)
        .await
        .unwrap();

    // Retract dev (deferred because feature exists)
    let report = fluree.drop_branch("mydb", "dev").await.unwrap();
    assert!(report.deferred);

    // Drop feature — last child of retracted dev — should cascade
    let report = fluree.drop_branch("mydb", "feature").await.unwrap();
    assert!(!report.deferred, "leaf branch should not be deferred");
    assert!(
        report.cascaded.contains(&"mydb:dev".to_string()),
        "cascade should include dev: {:?}",
        report.cascaded
    );

    // dev should be purged entirely
    let record = fluree.nameservice().lookup("mydb:dev").await.unwrap();
    assert!(record.is_none(), "dev should be purged after cascade");

    // Only main remains
    let branches = fluree.list_branches("mydb").await.unwrap();
    let names: Vec<&str> = branches.iter().map(|r| r.branch.as_str()).collect();
    assert_eq!(names, vec!["main"]);
}

/// Branching from a branch (nested branches) correctly chains namespace fallback.
///
/// main (t=1: seed) -> dev (t=2: dev-data) -> feature (t=2: feature-data)
/// Feature should see seed from main, dev-data from dev, and feature-data from itself.
#[tokio::test]
async fn nested_branch_data_isolation() {
    let fluree = FlureeBuilder::memory().build_memory();

    // 1. Create ledger and insert seed data on main
    let ledger = fluree.create_ledger("mydb").await.unwrap();
    let seed = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
    });
    let result = fluree.insert(ledger, &seed).await.unwrap();
    let main_ledger = result.ledger;

    // 2. Branch dev from main
    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();

    // 3. Transact on dev
    let dev = fluree.ledger("mydb:dev").await.unwrap();
    let dev_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:bob", "ex:name": "Bob"}]
    });
    fluree.insert(dev, &dev_data).await.unwrap();

    // 4. Branch feature from dev (nested branch)
    fluree
        .create_branch("mydb", "feature", Some("dev"), None)
        .await
        .unwrap();

    // 5. Transact on feature
    let feature = fluree.ledger("mydb:feature").await.unwrap();
    let feature_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:carol", "ex:name": "Carol"}]
    });
    fluree.insert(feature, &feature_data).await.unwrap();

    // 6. Also transact something on main that should NOT appear on dev or feature
    let main_only = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:dave", "ex:name": "Dave"}]
    });
    fluree.insert(main_ledger, &main_only).await.unwrap();

    // 7. Query all three branches for names
    let query = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "select": ["?name"],
        "where": {"@id": "?s", "ex:name": "?name"}
    });

    // Main: Alice (seed) + Dave (main-only)
    let main = fluree.ledger("mydb:main").await.unwrap();
    let result = support::query_jsonld(&fluree, &main, &query).await.unwrap();
    let rows = result.to_jsonld(&main.snapshot).unwrap();
    assert_eq!(extract_names(&rows), vec!["Alice", "Dave"]);

    // Dev: Alice (seed from main) + Bob (dev-only), NOT Dave or Carol
    let dev = fluree.ledger("mydb:dev").await.unwrap();
    let result = support::query_jsonld(&fluree, &dev, &query).await.unwrap();
    let rows = result.to_jsonld(&dev.snapshot).unwrap();
    assert_eq!(extract_names(&rows), vec!["Alice", "Bob"]);

    // Feature: Alice (seed from main->dev chain) + Bob (from dev) + Carol (feature-only)
    let feature = fluree.ledger("mydb:feature").await.unwrap();
    let result = support::query_jsonld(&fluree, &feature, &query)
        .await
        .unwrap();
    let rows = result.to_jsonld(&feature.snapshot).unwrap();
    assert_eq!(extract_names(&rows), vec!["Alice", "Bob", "Carol"]);
}

/// Incremental indexing on a branch must walk pre-fork ancestors.
///
/// Regression test for the bug where the background indexer scoped its
/// content store to the branch's own namespace. Once the branch's first
/// commit referenced a parent that lives under the source branch's
/// prefix, both incremental indexing AND the full-rebuild fallback
/// would 404 on every retry.
///
/// Also covers the binary-store attach path: after the branch is
/// indexed, opening it must successfully read the index root that was
/// just written under the branch namespace AND fall back to the
/// parent's namespace for any inherited blobs.
#[cfg(feature = "native")]
#[tokio::test]
async fn branch_incremental_index_resolves_pre_fork_parent() {
    use fluree_db_api::tx::IndexingMode;
    use fluree_db_api::TriggerIndexOptions;
    use std::sync::Arc;

    let mut fluree = FlureeBuilder::memory().build_memory();
    let (local, indexer_handle) = support::start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::default(),
    );
    fluree.set_indexing_mode(IndexingMode::Background(indexer_handle));

    local
        .run_until(async move {
            // 1. Create main and seed two commits, then index — main has its
            //    own commit chain and a published index root under main's prefix.
            let ledger = fluree.create_ledger("mydb-bidx").await.unwrap();
            let seed = json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
            });
            let r1 = fluree.insert(ledger, &seed).await.unwrap();
            let main_after = r1.ledger;
            let bob = json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [{"@id": "ex:bob", "ex:name": "Bob"}]
            });
            let r2 = fluree.insert(main_after, &bob).await.unwrap();
            let main_t = r2.ledger.t();
            assert!(main_t >= 2);

            // Index main.
            fluree
                .trigger_index("mydb-bidx:main", TriggerIndexOptions::default())
                .await
                .expect("index main");

            // 2. Branch dev from main. Dev inherits main's commit_head_id and
            //    index_head_id at fork time — both point at blobs in main's
            //    namespace.
            fluree
                .create_branch("mydb-bidx", "dev", None, None)
                .await
                .expect("create_branch");

            // 3. Insert one commit on dev. Its `previous` points at main's
            //    head — a CID whose blob lives only under main's prefix.
            let dev_ledger = fluree.ledger("mydb-bidx:dev").await.expect("open dev");
            let dev_commit = json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [{"@id": "ex:carol", "ex:name": "Carol"}]
            });
            let r3 = fluree
                .insert(dev_ledger, &dev_commit)
                .await
                .expect("dev insert");
            let dev_t = r3.ledger.t();
            assert_eq!(dev_t, main_t + 1, "dev should advance from main's head");

            // 4. Trigger indexing on dev. The indexer's commit-chain walk
            //    crosses the fork: dev@dev_t in dev's namespace → previous
            //    main@main_t in main's namespace. Without the branched store,
            //    both incremental and the full-rebuild fallback 404 here —
            //    and `process_ledger` keeps retrying without notifying
            //    waiters, so the trigger would hang forever. The explicit
            //    timeout turns a regression into a clean failure instead
            //    of an infinite hang in CI.
            let res = fluree
                .trigger_index(
                    "mydb-bidx:dev",
                    TriggerIndexOptions::default().with_timeout(15_000),
                )
                .await
                .expect("incremental index on dev should succeed across fork");
            assert!(
                res.index_t >= dev_t,
                "dev's index_t={} should advance to cover commit_t={}",
                res.index_t,
                dev_t
            );

            // 5. Re-open dev to exercise the binary-store attach path. This
            //    used to fail too — the index root and any inherited
            //    leaf/branch blobs that live under main's namespace would 404
            //    against dev's flat store.
            let dev_after = fluree
                .ledger("mydb-bidx:dev")
                .await
                .expect("re-open dev after indexing");
            assert!(
                dev_after.index_t() >= dev_t,
                "re-opened dev should see the published index"
            );

            // 6. Sanity: the indexed branch returns content from across the
            //    fork (Alice and Bob from main pre-fork, Carol from dev's own commit).
            let query = json!({
                "@context": {"ex": "http://example.org/ns/"},
                "select": ["?name"],
                "where": {"@id": "?s", "ex:name": "?name"}
            });
            let result = support::query_jsonld(&fluree, &dev_after, &query)
                .await
                .unwrap();
            let rows = result.to_jsonld(&dev_after.snapshot).unwrap();
            assert_eq!(
                extract_names(&rows),
                vec!["Alice", "Bob", "Carol"],
                "indexed branch should see pre-fork ancestors and its own commits"
            );
        })
        .await;
}
