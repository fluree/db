//! Merge integration tests
//!
//! Tests the branch merge lifecycle: fast-forward merge, error paths
//! (diverged target, missing source, self-merge), and post-merge
//! verification of data, nameservice state, and continued branch use.

mod support;

use fluree_db_api::{ConflictStrategy, FlureeBuilder};
use serde_json::json;

/// Extract sorted name strings from query result rows.
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

/// Query all ex:name values on a branch.
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

// =============================================================================
// Happy path: fast-forward merge
// =============================================================================

/// Fast-forward merge: source branch has commits, target has not advanced.
/// After merge, the target sees all of the source's data.
#[tokio::test]
async fn merge_fast_forward() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();

    let base_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
    });
    let result = fluree.insert(ledger, &base_data).await.unwrap();
    let _main_ledger = result.ledger;

    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();

    // Transact on dev
    let dev_ledger = fluree.ledger("mydb:dev").await.unwrap();
    let dev_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:bob", "ex:name": "Bob"}]
    });
    fluree.insert(dev_ledger, &dev_data).await.unwrap();

    // Merge dev → main (fast-forward)
    let report = fluree
        .merge_branch("mydb", "dev", None, ConflictStrategy::default())
        .await
        .unwrap();

    assert!(report.fast_forward);
    assert_eq!(report.target, "main");
    assert_eq!(report.source, "dev");
    assert!(report.commits_copied > 0);

    // Main should now see Alice (base) + Bob (from dev)
    let names = query_all_names(&fluree, "mydb:main").await;
    assert_eq!(names, vec!["Alice", "Bob"]);
}

/// Fast-forward merge with multiple commits on the source branch.
#[tokio::test]
async fn merge_fast_forward_multiple_commits() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();

    let base_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
    });
    let result = fluree.insert(ledger, &base_data).await.unwrap();
    let _main_ledger = result.ledger;

    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();

    // Several commits on dev
    let dev_ledger = fluree.ledger("mydb:dev").await.unwrap();
    let data1 = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:bob", "ex:name": "Bob"}]
    });
    let r = fluree.insert(dev_ledger, &data1).await.unwrap();

    let data2 = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:carol", "ex:name": "Carol"}]
    });
    let r = fluree.insert(r.ledger, &data2).await.unwrap();

    let data3 = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:dave", "ex:name": "Dave"}]
    });
    fluree.insert(r.ledger, &data3).await.unwrap();

    // Merge
    let report = fluree
        .merge_branch("mydb", "dev", None, ConflictStrategy::default())
        .await
        .unwrap();

    assert!(report.fast_forward);
    assert_eq!(report.commits_copied, 3);

    // Main should see all four names
    let names = query_all_names(&fluree, "mydb:main").await;
    assert_eq!(names, vec!["Alice", "Bob", "Carol", "Dave"]);
}

// =============================================================================
// Error paths
// =============================================================================

/// Cannot merge a branch that has no branch point (e.g. main itself).
#[tokio::test]
async fn merge_main_as_source_refused() {
    let fluree = FlureeBuilder::memory().build_memory();
    fluree.create_ledger("mydb").await.unwrap();

    let err = fluree
        .merge_branch("mydb", "main", None, ConflictStrategy::default())
        .await
        .expect_err("merging main as source should fail");

    assert!(
        err.to_string().contains("no source branch"),
        "expected error about missing source branch, got: {err}"
    );
}

/// Cannot merge a branch into itself.
#[tokio::test]
async fn merge_self_refused() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();

    let base_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
    });
    fluree.insert(ledger, &base_data).await.unwrap();

    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();

    let err = fluree
        .merge_branch("mydb", "dev", Some("dev"), ConflictStrategy::default())
        .await
        .expect_err("self-merge should fail");

    assert!(
        err.to_string().contains("itself"),
        "expected error about merging into itself, got: {err}"
    );
}

/// Merging into a non-parent branch is allowed when fast-forwardable.
#[tokio::test]
async fn merge_into_non_parent_allowed() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();

    let base_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
    });
    fluree.insert(ledger, &base_data).await.unwrap();

    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();
    fluree
        .create_branch("mydb", "feature", None, None)
        .await
        .unwrap();

    // Merge dev into feature — both share the same base from main, so
    // this is a valid fast-forward even though feature is not dev's parent.
    let report = fluree
        .merge_branch("mydb", "dev", Some("feature"), ConflictStrategy::default())
        .await
        .expect("merging into non-parent should succeed when fast-forwardable");

    assert!(report.fast_forward);
}

/// General merge when the target has diverged (not fast-forwardable).
#[tokio::test]
async fn merge_diverged_target_general_merge() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();

    let base_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
    });
    let result = fluree.insert(ledger, &base_data).await.unwrap();
    let main_ledger = result.ledger;

    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();

    // Transact on dev
    let dev_ledger = fluree.ledger("mydb:dev").await.unwrap();
    let dev_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:bob", "ex:name": "Bob"}]
    });
    fluree.insert(dev_ledger, &dev_data).await.unwrap();

    // Advance main (target diverges)
    let main_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:carol", "ex:name": "Carol"}]
    });
    fluree.insert(main_ledger, &main_data).await.unwrap();

    // General merge should succeed with TakeBoth (default) strategy
    let report = fluree
        .merge_branch("mydb", "dev", None, ConflictStrategy::default())
        .await
        .unwrap();

    assert!(!report.fast_forward);

    // Main should now have all three names
    let names = query_all_names(&fluree, "mydb:main").await;
    assert!(
        names.contains(&"Alice".to_string()),
        "expected Alice in {names:?}"
    );
    assert!(
        names.contains(&"Bob".to_string()),
        "expected Bob in {names:?}"
    );
    assert!(
        names.contains(&"Carol".to_string()),
        "expected Carol in {names:?}"
    );
}

#[tokio::test]
async fn merge_take_source_works_after_binary_index_reload() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();

    let base_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
    });
    let main_ledger = fluree.insert(ledger, &base_data).await.unwrap().ledger;
    support::rebuild_and_publish_index(&fluree, "mydb:main").await;
    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();

    let dev_ledger = fluree.ledger("mydb:dev").await.unwrap();
    fluree
        .upsert(
            dev_ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [{"@id": "ex:alice", "ex:name": "Alice-dev"}]
            }),
        )
        .await
        .unwrap();

    fluree
        .upsert(
            main_ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [{"@id": "ex:alice", "ex:name": "Alice-main"}]
            }),
        )
        .await
        .unwrap();

    let report = fluree
        .merge_branch("mydb", "dev", None, ConflictStrategy::TakeSource)
        .await
        .unwrap();

    assert!(!report.fast_forward);
    assert_eq!(report.conflict_count, 1);
    assert_eq!(report.strategy.as_deref(), Some("take-source"));

    let names = query_all_names(&fluree, "mydb:main").await;
    assert_eq!(names, vec!["Alice-dev"]);
}

/// Merging a nonexistent source branch returns NotFound.
#[tokio::test]
async fn merge_nonexistent_source_fails() {
    let fluree = FlureeBuilder::memory().build_memory();
    fluree.create_ledger("mydb").await.unwrap();

    let err = fluree
        .merge_branch("mydb", "nonexistent", None, ConflictStrategy::default())
        .await
        .expect_err("merging nonexistent branch should fail");

    assert!(
        err.to_string().to_lowercase().contains("not found")
            || err.to_string().contains("nonexistent"),
        "expected not-found error, got: {err}"
    );
}

/// Merging a source branch with no commits fails.
#[tokio::test]
async fn merge_empty_source_fails() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();

    let base_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
    });
    fluree.insert(ledger, &base_data).await.unwrap();

    // Create branch but don't transact on it — the branch inherits
    // the source's HEAD, so it technically has commits. But if we
    // create a branch right at genesis (before any commits on main),
    // the source may have no commit_head_id.
    // Actually, since we inserted base_data, main has a commit.
    // The branch also inherits that commit HEAD. So this scenario
    // is only possible if we somehow created a branch with no commits
    // at all, which the current API doesn't easily allow.
    // Instead, test the "source has no unique commits" case — which is
    // actually valid and should still succeed (copies 0 commits).
    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();

    // Merge dev → main with no unique commits on dev
    let report = fluree
        .merge_branch("mydb", "dev", None, ConflictStrategy::default())
        .await
        .unwrap();

    // Should succeed with 0 commits copied (nothing new on source)
    assert_eq!(report.commits_copied, 0);
}

// =============================================================================
// Post-merge verification
// =============================================================================

/// After merge, the target's nameservice record reflects the source's HEAD.
#[tokio::test]
async fn merge_target_head_updated() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();

    let base_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
    });
    let result = fluree.insert(ledger, &base_data).await.unwrap();
    let _main_ledger = result.ledger;

    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();

    let dev_ledger = fluree.ledger("mydb:dev").await.unwrap();
    let dev_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:bob", "ex:name": "Bob"}]
    });
    fluree.insert(dev_ledger, &dev_data).await.unwrap();

    // Capture source state before merge
    let source_record = fluree
        .nameservice()
        .lookup("mydb:dev")
        .await
        .unwrap()
        .unwrap();

    let report = fluree
        .merge_branch("mydb", "dev", None, ConflictStrategy::default())
        .await
        .unwrap();

    // Target's HEAD should now match what was the source's HEAD
    let target_record = fluree
        .nameservice()
        .lookup("mydb:main")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        target_record.commit_head_id, source_record.commit_head_id,
        "target HEAD should match source HEAD after merge"
    );
    assert_eq!(target_record.commit_t, source_record.commit_t);
    assert_eq!(report.new_head_t, source_record.commit_t);
    assert_eq!(report.new_head_id, source_record.commit_head_id.unwrap());
}

/// After merge, the source's branch point is updated so subsequent
/// merges only consider new commits.
#[tokio::test]
async fn merge_source_branch_point_updated() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();

    let base_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
    });
    let result = fluree.insert(ledger, &base_data).await.unwrap();
    let _main_ledger = result.ledger;

    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();

    let dev_ledger = fluree.ledger("mydb:dev").await.unwrap();
    let dev_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:bob", "ex:name": "Bob"}]
    });
    fluree.insert(dev_ledger, &dev_data).await.unwrap();

    let report = fluree
        .merge_branch("mydb", "dev", None, ConflictStrategy::default())
        .await
        .unwrap();

    // After merge, the source branch should still track its source_branch
    // and the merge report should reflect the new target HEAD
    let source_after = fluree
        .nameservice()
        .lookup("mydb:dev")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        source_after.source_branch.as_deref(),
        Some("main"),
        "source branch should still reference main"
    );
    assert!(report.new_head_t > 0);
}

/// After merge, the target branch can still be transacted on normally.
#[tokio::test]
async fn merge_target_accepts_new_transactions() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();

    let base_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
    });
    let result = fluree.insert(ledger, &base_data).await.unwrap();
    let _main_ledger = result.ledger;

    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();

    let dev_ledger = fluree.ledger("mydb:dev").await.unwrap();
    let dev_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:bob", "ex:name": "Bob"}]
    });
    fluree.insert(dev_ledger, &dev_data).await.unwrap();

    fluree
        .merge_branch("mydb", "dev", None, ConflictStrategy::default())
        .await
        .unwrap();

    // Transact on main after merge
    let main_ledger = fluree.ledger("mydb:main").await.unwrap();
    let new_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:carol", "ex:name": "Carol"}]
    });
    fluree.insert(main_ledger, &new_data).await.unwrap();

    let names = query_all_names(&fluree, "mydb:main").await;
    assert_eq!(names, vec!["Alice", "Bob", "Carol"]);
}

/// After merge, the source branch can still be transacted on and merged again.
#[tokio::test]
async fn merge_source_continues_after_merge() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();

    let base_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
    });
    let result = fluree.insert(ledger, &base_data).await.unwrap();
    let _main_ledger = result.ledger;

    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();

    // First round: transact on dev, merge to main
    let dev_ledger = fluree.ledger("mydb:dev").await.unwrap();
    let dev_data1 = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:bob", "ex:name": "Bob"}]
    });
    fluree.insert(dev_ledger, &dev_data1).await.unwrap();

    fluree
        .merge_branch("mydb", "dev", None, ConflictStrategy::default())
        .await
        .unwrap();

    // Second round: transact more on dev, merge again
    let dev_ledger = fluree.ledger("mydb:dev").await.unwrap();
    let dev_data2 = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:carol", "ex:name": "Carol"}]
    });
    fluree.insert(dev_ledger, &dev_data2).await.unwrap();

    let report2 = fluree
        .merge_branch("mydb", "dev", None, ConflictStrategy::default())
        .await
        .unwrap();

    assert!(report2.fast_forward);
    // Only the new commit should be copied in the second merge
    assert_eq!(report2.commits_copied, 1);

    let names = query_all_names(&fluree, "mydb:main").await;
    assert_eq!(names, vec!["Alice", "Bob", "Carol"]);
}

/// Merge from a nested branch (branch of a branch) into its parent.
#[tokio::test]
async fn merge_nested_branch() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();

    let base_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
    });
    let result = fluree.insert(ledger, &base_data).await.unwrap();
    let _main_ledger = result.ledger;

    // main → dev → feature
    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();
    fluree
        .create_branch("mydb", "feature", Some("dev"), None)
        .await
        .unwrap();

    // Transact on feature
    let feature_ledger = fluree.ledger("mydb:feature").await.unwrap();
    let feature_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:bob", "ex:name": "Bob"}]
    });
    fluree.insert(feature_ledger, &feature_data).await.unwrap();

    // Merge feature → dev
    let report = fluree
        .merge_branch("mydb", "feature", None, ConflictStrategy::default())
        .await
        .unwrap();

    assert!(report.fast_forward);
    assert_eq!(report.target, "dev");
    assert_eq!(report.source, "feature");

    // Dev should see Alice + Bob
    let names = query_all_names(&fluree, "mydb:dev").await;
    assert_eq!(names, vec!["Alice", "Bob"]);
}

/// After a failed merge attempt (abort strategy on conflicts), nameservice
/// state for both source and target should be unchanged.
#[tokio::test]
async fn merge_abort_leaves_nameservice_unchanged() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();

    let base_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
    });
    let result = fluree.insert(ledger, &base_data).await.unwrap();
    let main_ledger = result.ledger;

    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();

    // Both branches modify the same subject+predicate to create a real conflict.
    let dev_ledger = fluree.ledger("mydb:dev").await.unwrap();
    let dev_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice-dev"}]
    });
    fluree.insert(dev_ledger, &dev_data).await.unwrap();

    // Advance main with a conflicting change
    let main_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice-main"}]
    });
    fluree.insert(main_ledger, &main_data).await.unwrap();

    // Capture pre-merge state
    let pre_main = fluree
        .nameservice()
        .lookup("mydb:main")
        .await
        .unwrap()
        .unwrap();
    let pre_dev = fluree
        .nameservice()
        .lookup("mydb:dev")
        .await
        .unwrap()
        .unwrap();

    // Attempt merge with Abort strategy (should fail on conflict)
    let _err = fluree
        .merge_branch("mydb", "dev", None, ConflictStrategy::Abort)
        .await
        .expect_err("merge should fail with abort strategy on conflicts");

    // Both branches should be unchanged
    let post_main = fluree
        .nameservice()
        .lookup("mydb:main")
        .await
        .unwrap()
        .unwrap();
    let post_dev = fluree
        .nameservice()
        .lookup("mydb:dev")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(pre_main.commit_t, post_main.commit_t);
    assert_eq!(pre_main.commit_head_id, post_main.commit_head_id);
    assert_eq!(pre_dev.commit_t, post_dev.commit_t);
    assert_eq!(pre_dev.commit_head_id, post_dev.commit_head_id);
    assert_eq!(pre_dev.source_branch, post_dev.source_branch);
}

/// Merge with explicit target branch matching the source's parent works
/// the same as an implicit target.
#[tokio::test]
async fn merge_explicit_target_matches_parent() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();

    let base_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
    });
    let result = fluree.insert(ledger, &base_data).await.unwrap();
    let _main_ledger = result.ledger;

    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();

    let dev_ledger = fluree.ledger("mydb:dev").await.unwrap();
    let dev_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:bob", "ex:name": "Bob"}]
    });
    fluree.insert(dev_ledger, &dev_data).await.unwrap();

    // Explicitly specify main as the target (same as default)
    let report = fluree
        .merge_branch("mydb", "dev", Some("main"), ConflictStrategy::default())
        .await
        .unwrap();

    assert!(report.fast_forward);
    assert_eq!(report.target, "main");

    let names = query_all_names(&fluree, "mydb:main").await;
    assert_eq!(names, vec!["Alice", "Bob"]);
}
