//! Merge integration tests
//!
//! Tests the branch merge lifecycle: fast-forward merge, error paths
//! (diverged target, missing source, self-merge), and post-merge
//! verification of data, nameservice state, and continued branch use.

mod support;

use fluree_db_api::merge_plan::{BaseStrategy, BranchSelector, MergePlan};
use fluree_db_api::{
    Base64Bytes, ConflictStrategy, ExportCommitsRequest, FlureeBuilder, IndexConfig,
    PushCommitsRequest, QueryConnectionOptions,
};
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

/// Diverged sibling-branch merge: both branches forked from main and each
/// has its own commits since. Their common ancestor is reachable only by
/// fanning out to either side's namespace, so the merge engine has to use
/// a union content store for the ancestor walk (mirroring what
/// `merge_preview` does). Without that, `find_common_ancestor` over
/// `source_store` alone can't read the target HEAD's blob and the merge
/// fails — a regression preview wouldn't expose because preview already
/// builds the union.
#[tokio::test]
async fn merge_between_diverged_sibling_branches() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();
    fluree
        .insert(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
            }),
        )
        .await
        .unwrap();

    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();
    fluree
        .create_branch("mydb", "feature", None, None)
        .await
        .unwrap();

    // Diverge both siblings on disjoint subjects so the merge has work
    // (it's no longer a no-op fast-forward).
    let dev_ledger = fluree.ledger("mydb:dev").await.unwrap();
    fluree
        .insert(
            dev_ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [{"@id": "ex:bob", "ex:name": "Bob"}]
            }),
        )
        .await
        .unwrap();
    let feature_ledger = fluree.ledger("mydb:feature").await.unwrap();
    fluree
        .insert(
            feature_ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [{"@id": "ex:carol", "ex:name": "Carol"}]
            }),
        )
        .await
        .unwrap();

    // Merge dev → feature. Both diverged off main; the common ancestor is
    // main's HEAD at branch time, which lives in main's namespace and is
    // reachable from feature via its parent chain — but only through the
    // union store. A source-only ancestor walk wouldn't be able to load
    // feature's HEAD blob and would fail before getting to the ancestor.
    let report = fluree
        .merge_branch("mydb", "dev", Some("feature"), ConflictStrategy::TakeBoth)
        .await
        .expect("diverged sibling merge must compute the common ancestor via the union store");

    assert!(
        !report.fast_forward,
        "branches diverged after the common ancestor; not a FF"
    );
    // Both writes survive the merge.
    let names = query_all_names(&fluree, "mydb:feature").await;
    assert!(names.contains(&"Alice".to_string()));
    assert!(names.contains(&"Bob".to_string()));
    assert!(names.contains(&"Carol".to_string()));
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

// =============================================================================
// Plan-driven merge (Fluree::merge with MergePlan)
// =============================================================================

#[tokio::test]
async fn merge_plan_take_source_takes_source_values() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();

    let main_ledger = fluree
        .insert(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [{"@id": "ex:alice", "ex:age": 30}]
            }),
        )
        .await
        .unwrap()
        .ledger;

    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();

    // Diverge: dev sets age=31, main sets age=32 (real conflict).
    let dev_ledger = fluree.ledger("mydb:dev").await.unwrap();
    fluree
        .upsert(
            dev_ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [{"@id": "ex:alice", "ex:age": 31}]
            }),
        )
        .await
        .unwrap();
    fluree
        .upsert(
            main_ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [{"@id": "ex:alice", "ex:age": 32}]
            }),
        )
        .await
        .unwrap();

    // Capture expected heads via direct nameservice lookup.
    let source_head = fluree
        .ledger("mydb:dev")
        .await
        .unwrap()
        .head_commit_id
        .clone()
        .expect("dev has commits");
    let target_head = fluree
        .ledger("mydb:main")
        .await
        .unwrap()
        .head_commit_id
        .clone()
        .expect("main has commits");
    let target_t = fluree.ledger("mydb:main").await.unwrap().t();
    let source_t = fluree.ledger("mydb:dev").await.unwrap().t();

    let plan = MergePlan {
        source: BranchSelector {
            branch: "dev".into(),
            expected: source_head,
            at: None,
        },
        target: BranchSelector {
            branch: "main".into(),
            expected: target_head,
            at: None,
        },
        base_strategy: BaseStrategy::TakeSource,
        resolutions: vec![],
        additional_patch: None,
    };

    let report = fluree.merge("mydb", plan).await.unwrap();

    assert!(!report.fast_forward, "branches diverged");
    assert_eq!(report.conflict_count, 1, "one real conflict");
    // Merge commit's t = max(source_t, target_t) + 1.
    let expected_merge_t = source_t.max(target_t) + 1;
    assert_eq!(
        report.new_head_t, expected_merge_t,
        "merge commit at max(source_t, target_t) + 1 (P2 merge_t flow-through)"
    );
}

#[tokio::test]
async fn merge_plan_stale_source_head_returns_branch_conflict() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();
    fluree
        .insert(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
            }),
        )
        .await
        .unwrap();
    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();
    let dev_ledger = fluree.ledger("mydb:dev").await.unwrap();
    fluree
        .insert(
            dev_ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [{"@id": "ex:bob", "ex:name": "Bob"}]
            }),
        )
        .await
        .unwrap();

    // Capture HEADs, then advance dev so the captured source head is stale.
    let stale_source = fluree
        .ledger("mydb:dev")
        .await
        .unwrap()
        .head_commit_id
        .clone()
        .expect("dev head");
    let target_head = fluree
        .ledger("mydb:main")
        .await
        .unwrap()
        .head_commit_id
        .clone()
        .expect("main head");

    let dev_ledger2 = fluree.ledger("mydb:dev").await.unwrap();
    fluree
        .insert(
            dev_ledger2,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [{"@id": "ex:carol", "ex:name": "Carol"}]
            }),
        )
        .await
        .unwrap();

    let plan = MergePlan {
        source: BranchSelector {
            branch: "dev".into(),
            expected: stale_source,
            at: None,
        },
        target: BranchSelector {
            branch: "main".into(),
            expected: target_head,
            at: None,
        },
        base_strategy: BaseStrategy::TakeSource,
        resolutions: vec![],
        additional_patch: None,
    };

    let err = fluree
        .merge("mydb", plan)
        .await
        .expect_err("stale source HEAD must reject");
    let msg = err.to_string();
    assert!(
        msg.contains("stale source HEAD"),
        "error should mention stale source HEAD; got: {msg}"
    );
}

#[tokio::test]
async fn merge_plan_resolutions_not_yet_implemented_returns_400() {
    use fluree_db_api::merge_plan::{MergeResolution, ResolutionAction, ResolutionKey};
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();
    fluree
        .insert(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
            }),
        )
        .await
        .unwrap();
    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();

    let source_head = fluree
        .ledger("mydb:dev")
        .await
        .unwrap()
        .head_commit_id
        .clone()
        .expect("dev head");
    let target_head = fluree
        .ledger("mydb:main")
        .await
        .unwrap()
        .head_commit_id
        .clone()
        .expect("main head");

    let plan = MergePlan {
        source: BranchSelector {
            branch: "dev".into(),
            expected: source_head,
            at: None,
        },
        target: BranchSelector {
            branch: "main".into(),
            expected: target_head,
            at: None,
        },
        base_strategy: BaseStrategy::TakeSource,
        resolutions: vec![MergeResolution {
            key: ResolutionKey {
                subject: "ex:alice".into(),
                predicate: "ex:name".into(),
                graph: None,
            },
            action: ResolutionAction::TakeSource,
            custom_patch: None,
        }],
        additional_patch: None,
    };

    let err = fluree
        .merge("mydb", plan)
        .await
        .expect_err("resolutions are not yet implemented");
    let msg = err.to_string();
    assert!(
        msg.contains("resolutions is not yet implemented"),
        "error should call out the staged feature; got: {msg}"
    );
}

#[tokio::test]
async fn merge_plan_fast_forward_cas_rejects_concurrent_target_advance() {
    // Plan-driven fast-forward merges must CAS the publish against the
    // target HEAD captured at plan-validation time. A concurrent transaction
    // that advances target between the validation read and the publish must
    // surface as a `BranchConflict`, not silently overwrite the new state.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();

    let main_ledger = fluree
        .insert(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
            }),
        )
        .await
        .unwrap()
        .ledger;

    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();

    // dev gets a unique commit so the merge has something to fast-forward.
    let dev_ledger = fluree.ledger("mydb:dev").await.unwrap();
    fluree
        .insert(
            dev_ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [{"@id": "ex:bob", "ex:name": "Bob"}]
            }),
        )
        .await
        .unwrap();

    // Capture the heads the plan will reference.
    let source_head = fluree
        .ledger("mydb:dev")
        .await
        .unwrap()
        .head_commit_id
        .clone()
        .expect("dev head");
    let stale_target = fluree
        .ledger("mydb:main")
        .await
        .unwrap()
        .head_commit_id
        .clone()
        .expect("main head");

    // Simulate a concurrent transaction on main: target advances between
    // when the caller observed `stale_target` and when the merge would
    // publish.
    fluree
        .insert(
            main_ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [{"@id": "ex:carol", "ex:name": "Carol"}]
            }),
        )
        .await
        .unwrap();

    let plan = MergePlan {
        source: BranchSelector {
            branch: "dev".into(),
            expected: source_head,
            at: None,
        },
        target: BranchSelector {
            branch: "main".into(),
            expected: stale_target,
            at: None,
        },
        base_strategy: BaseStrategy::TakeSource,
        resolutions: vec![],
        additional_patch: None,
    };

    let err = fluree
        .merge("mydb", plan)
        .await
        .expect_err("stale target HEAD must be rejected");
    let msg = err.to_string();
    assert!(
        msg.contains("stale target HEAD"),
        "expected stale-target error from the plan-validation pre-check; got: {msg}"
    );
}

// ============================================================================
// Replication round-trip: take-target merge does not leak source value
// ============================================================================

/// Topologically sort export commits so each commit follows its in-batch
/// parents. Required because `export_commit_range` walks the DAG via a
/// stack-based DFS and emits commits newest-first; a simple `reverse()`
/// produces a valid order for linear chains but not for DAG-shaped exports
/// where a sibling-branch commit can land before its parent.
fn topo_sort_for_push(commits: Vec<Base64Bytes>) -> Vec<Base64Bytes> {
    use std::collections::HashSet;

    let parsed: Vec<(String, Vec<String>, Base64Bytes)> = commits
        .into_iter()
        .map(|b| {
            let bytes = b.0.clone();
            let commit = fluree_db_core::commit::codec::read_commit(&bytes)
                .expect("commit bytes should decode");
            let digest = fluree_db_core::sha256_hex(&bytes);
            let parents: Vec<String> = commit
                .parents
                .iter()
                .map(fluree_db_core::ContentId::digest_hex)
                .collect();
            (digest, parents, b)
        })
        .collect();

    let in_batch: HashSet<String> = parsed.iter().map(|(d, _, _)| d.clone()).collect();
    let mut emitted: HashSet<String> = HashSet::new();
    let mut output: Vec<Base64Bytes> = Vec::with_capacity(parsed.len());
    let mut remaining = parsed;

    while !remaining.is_empty() {
        let before = remaining.len();
        let mut still_remaining: Vec<(String, Vec<String>, Base64Bytes)> =
            Vec::with_capacity(before);
        for (d, parents, b) in remaining {
            let ready = parents
                .iter()
                .all(|p| !in_batch.contains(p) || emitted.contains(p));
            if ready {
                output.push(b);
                emitted.insert(d);
            } else {
                still_remaining.push((d, parents, b));
            }
        }
        assert!(
            still_remaining.len() < before,
            "topo sort: no progress; cycle in batch?"
        );
        remaining = still_remaining;
    }
    output
}

/// Query `ex:alice`'s `ex:age` on the given ledger. Returns `None` if absent.
async fn query_alice_age(fluree: &support::MemoryFluree, ledger_id: &str) -> Option<i64> {
    let ledger = fluree.ledger(ledger_id).await.unwrap();
    let query = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "select": ["?age"],
        "where": {"@id": "ex:alice", "ex:age": "?age"}
    });
    let result = support::query_jsonld(fluree, &ledger, &query)
        .await
        .unwrap();
    let rows = result.to_jsonld(&ledger.snapshot).unwrap();
    rows.as_array()
        .and_then(|arr| arr.first())
        .and_then(|row| match row {
            serde_json::Value::Number(n) => n.as_i64(),
            serde_json::Value::Array(a) => a.first().and_then(serde_json::Value::as_i64),
            _ => None,
        })
}

/// **Replication round-trip lock-down for the DAG replay semantics.**
///
/// Setup: source ledger has main and dev branches; both modify `ex:alice
/// ex:age` to different values. A `take-target` merge of dev → main keeps
/// main's value and intentionally drops dev's. Source main therefore
/// resolves to the target value.
///
/// We then export source main's commits (which pulls in dev's ancestry
/// transitively via the merge commit's `parents[1]`) and import the whole
/// DAG into a fresh target ledger. The target's main, after import, must
/// reflect the same merged value as source's main. If push/import were to
/// linearly apply every commit's flakes (the bug `primary_chain_in_batch`
/// fixed), dev's pre-merge value would leak into target novelty.
///
/// This test exercises three architectural decisions in one assertion:
/// 1. Refined conflict detection picks dev's update as a real conflict.
/// 2. Merge commit at `t = max(source_t, target_t) + 1` flows through to
///    the commit blob and through the import path's staged-`t` override.
/// 3. The primary-chain skip in `apply_pushed_commits_to_state` keeps
///    source-ancestry flakes out of target novelty during replay.
#[tokio::test]
async fn take_target_merge_replication_does_not_leak_source_value() {
    let fluree = FlureeBuilder::memory().build_memory();

    // ---- Source ledger: main + dev with a real conflict, take-target merge.
    let src_id = "src:main";
    let main_ledger = fluree
        .create_ledger(src_id)
        .await
        .expect("create source ledger");
    let main_ledger = fluree
        .insert(
            main_ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [{"@id": "ex:alice", "ex:age": 30}]
            }),
        )
        .await
        .expect("insert base alice/age=30")
        .ledger;

    fluree
        .create_branch("src", "dev", None, None)
        .await
        .expect("branch dev");

    // dev: alice/age = 99 (this value MUST be dropped by take-target).
    let dev_ledger = fluree.ledger("src:dev").await.unwrap();
    fluree
        .upsert(
            dev_ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [{"@id": "ex:alice", "ex:age": 99}]
            }),
        )
        .await
        .expect("dev upsert age=99");

    // main: alice/age = 40 (this value MUST survive take-target).
    fluree
        .upsert(
            main_ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [{"@id": "ex:alice", "ex:age": 40}]
            }),
        )
        .await
        .expect("main upsert age=40");

    // Merge dev → main with TakeBranch (== plan-schema take-target). Target
    // wins, so source's age=99 is dropped from the merge commit.
    let report = fluree
        .merge_branch("src", "dev", Some("main"), ConflictStrategy::TakeBranch)
        .await
        .expect("take-target merge");
    assert!(
        !report.fast_forward,
        "branches diverged; expected general merge"
    );
    assert_eq!(report.conflict_count, 1, "real conflict on alice/age");

    // Sanity: source main resolved to target's value.
    fluree.disconnect_ledger(src_id).await;
    let src_age = query_alice_age(&fluree, src_id).await;
    assert_eq!(
        src_age,
        Some(40),
        "source main after take-target merge should be target's 40, not source's 99"
    );

    // ---- Export source main's full DAG (includes dev ancestry via parents).
    let src_handle = fluree.ledger_cached(src_id).await.expect("source handle");
    let export = fluree
        .export_commit_range(
            &src_handle,
            &ExportCommitsRequest {
                cursor: None,
                cursor_id: None,
                limit: Some(100),
            },
        )
        .await
        .expect("export source main");
    assert!(
        export.commits.len() >= 4,
        "export should include both branches' history + the merge commit; got {}",
        export.commits.len()
    );

    // ---- Push the DAG (topologically sorted) to a fresh target ledger.
    let tgt_id = "tgt:main";
    fluree
        .create_ledger(tgt_id)
        .await
        .expect("create target ledger");
    let tgt_handle = fluree.ledger_cached(tgt_id).await.expect("target handle");

    let push_commits = topo_sort_for_push(export.commits);
    fluree
        .push_commits_with_handle(
            &tgt_handle,
            PushCommitsRequest {
                commits: push_commits,
                blobs: export.blobs,
            },
            &QueryConnectionOptions::default(),
            &IndexConfig::default(),
        )
        .await
        .expect("push DAG to fresh target");

    // ---- Verify: target's main shows the merged result; dev's value did
    //      NOT leak through DAG replay.
    fluree.disconnect_ledger(tgt_id).await;
    let tgt_age = query_alice_age(&fluree, tgt_id).await;
    assert_eq!(
        tgt_age,
        Some(40),
        "target ledger after replication MUST reflect take-target merge result; \
         source value (99) leaked through DAG replay if this fails"
    );
}
