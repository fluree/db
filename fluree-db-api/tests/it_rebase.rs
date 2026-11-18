//! Rebase integration tests
//!
//! Tests the branch rebase lifecycle: fast-forward, clean replay,
//! conflict detection with various resolution strategies, and edge cases.

use crate::support;
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
    query_names(fluree, &ledger).await
}

/// Query all ex:name values on a branch as its cached handle holds it.
async fn query_cached_names(fluree: &support::MemoryFluree, ledger_id: &str) -> Vec<String> {
    let handle = fluree.ledger_cached(ledger_id).await.unwrap();
    query_names(fluree, &handle.snapshot().await.to_ledger_state()).await
}

async fn query_names(
    fluree: &support::MemoryFluree,
    ledger: &fluree_db_api::LedgerState,
) -> Vec<String> {
    let query = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "select": ["?name"],
        "where": {"@id": "?s", "ex:name": "?name"}
    });
    let result = support::query_jsonld(fluree, ledger, &query).await.unwrap();
    let rows = result.to_jsonld(&ledger.snapshot).unwrap();
    extract_names(&rows)
}

/// Fast-forward rebase: branch has no unique commits, source advanced.
/// After rebase, the branch sees the source's new data.
#[tokio::test]
async fn rebase_fast_forward() {
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

    // Advance main
    let main_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:carol", "ex:name": "Carol"}]
    });
    fluree.insert(main_ledger, &main_data).await.unwrap();

    // Rebase dev (no unique commits → fast-forward)
    let report = fluree
        .rebase_branch("mydb", "dev", ConflictStrategy::default())
        .await
        .unwrap();

    assert!(report.fast_forward);
    assert_eq!(report.replayed, 0);
    assert_eq!(report.total_commits, 0);

    // Dev should now see Carol (from source) + Alice (base)
    let names = query_all_names(&fluree, "mydb:dev").await;
    assert_eq!(names, vec!["Alice", "Carol"]);
}

/// Clean replay: non-overlapping changes on both branches.
/// After rebase, the branch sees both its data and the source's new data.
#[tokio::test]
async fn rebase_clean_replay() {
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

    // Transact on dev (non-overlapping)
    let dev_ledger = fluree.ledger("mydb:dev").await.unwrap();
    let dev_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:dave", "ex:name": "Dave"}]
    });
    fluree.insert(dev_ledger, &dev_data).await.unwrap();

    // Advance main with different data
    let main_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:carol", "ex:name": "Carol"}]
    });
    fluree.insert(main_ledger, &main_data).await.unwrap();

    // Rebase
    let report = fluree
        .rebase_branch("mydb", "dev", ConflictStrategy::TakeBoth)
        .await
        .unwrap();

    assert!(!report.fast_forward);
    assert_eq!(report.replayed, 1);
    assert!(report.conflicts.is_empty());
    assert!(report.failures.is_empty());

    // Dev should see Alice (base) + Carol (source) + Dave (replayed)
    let names = query_all_names(&fluree, "mydb:dev").await;
    assert_eq!(names, vec!["Alice", "Carol", "Dave"]);
}

/// Abort strategy: fail on first conflict, no changes applied.
/// After abort, the branch state is unchanged.
#[tokio::test]
async fn rebase_abort_on_conflict() {
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

    // Overlapping data on dev
    let dev_ledger = fluree.ledger("mydb:dev").await.unwrap();
    let dev_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice-dev"}]
    });
    fluree.insert(dev_ledger, &dev_data).await.unwrap();

    // Overlapping data on main
    let main_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice-main"}]
    });
    fluree.insert(main_ledger, &main_data).await.unwrap();

    // Capture pre-rebase state
    let pre_record = fluree
        .nameservice()
        .lookup("mydb:dev")
        .await
        .unwrap()
        .unwrap();

    // Rebase with abort should fail
    let err = fluree
        .rebase_branch("mydb", "dev", ConflictStrategy::Abort)
        .await
        .expect_err("abort strategy should fail on conflict");

    assert!(
        err.to_string().to_lowercase().contains("abort")
            || err.to_string().to_lowercase().contains("conflict"),
        "expected conflict/abort error, got: {err}"
    );

    // Branch should be unchanged (no commits written)
    let post_record = fluree
        .nameservice()
        .lookup("mydb:dev")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pre_record.commit_t, post_record.commit_t);
    assert_eq!(pre_record.commit_head_id, post_record.commit_head_id);
}

/// TakeSource strategy: source's values win, conflicting flakes dropped.
/// Non-conflicting flakes from the branch are kept.
#[tokio::test]
async fn rebase_take_source() {
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

    // Dev: overlapping + unique data in one commit
    let dev_ledger = fluree.ledger("mydb:dev").await.unwrap();
    let dev_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [
            {"@id": "ex:alice", "ex:name": "Alice-dev"},
            {"@id": "ex:dave", "ex:name": "Dave"}
        ]
    });
    fluree.insert(dev_ledger, &dev_data).await.unwrap();

    // Main: overlapping data
    let main_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice-main"}]
    });
    fluree.insert(main_ledger, &main_data).await.unwrap();

    let report = fluree
        .rebase_branch("mydb", "dev", ConflictStrategy::TakeSource)
        .await
        .unwrap();

    assert!(!report.fast_forward);
    assert!(!report.conflicts.is_empty());

    // Dev should have: Alice-main (source wins), Dave (non-conflicting kept)
    // The conflicting "Alice-dev" flakes were dropped.
    let names = query_all_names(&fluree, "mydb:dev").await;
    assert!(names.contains(&"Dave".to_string()));
    assert!(names.contains(&"Alice-main".to_string()));
    assert!(!names.contains(&"Alice-dev".to_string()));
}

/// TakeBranch strategy: branch's values win, source's conflicting values retracted.
#[tokio::test]
async fn rebase_take_branch() {
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

    // Dev: change Alice's name
    let dev_ledger = fluree.ledger("mydb:dev").await.unwrap();
    let dev_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice-dev"}]
    });
    fluree.insert(dev_ledger, &dev_data).await.unwrap();

    // Main: also change Alice's name differently
    let main_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice-main"}]
    });
    fluree.insert(main_ledger, &main_data).await.unwrap();

    let report = fluree
        .rebase_branch("mydb", "dev", ConflictStrategy::TakeBranch)
        .await
        .unwrap();

    assert!(!report.fast_forward);
    assert!(!report.conflicts.is_empty());
    assert_eq!(report.replayed, 1);

    // Dev should have Alice-dev (branch wins), NOT Alice-main
    let names = query_all_names(&fluree, "mydb:dev").await;
    assert!(
        names.contains(&"Alice-dev".to_string()),
        "expected Alice-dev in {names:?}"
    );
    assert!(
        !names.contains(&"Alice-main".to_string()),
        "Alice-main should be retracted, got {names:?}"
    );
}

/// TakeBoth strategy: both values coexist after rebase.
#[tokio::test]
async fn rebase_take_both() {
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

    // Overlapping data on dev
    let dev_ledger = fluree.ledger("mydb:dev").await.unwrap();
    let dev_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice-dev"}]
    });
    fluree.insert(dev_ledger, &dev_data).await.unwrap();

    // Overlapping data on main
    let main_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice-main"}]
    });
    fluree.insert(main_ledger, &main_data).await.unwrap();

    let report = fluree
        .rebase_branch("mydb", "dev", ConflictStrategy::TakeBoth)
        .await
        .unwrap();

    assert!(!report.fast_forward);
    assert!(!report.conflicts.is_empty());
    assert_eq!(report.replayed, 1);

    // Dev should have both Alice-dev and Alice-main (multi-cardinality)
    let names = query_all_names(&fluree, "mydb:dev").await;
    assert!(
        names.contains(&"Alice-dev".to_string()),
        "expected Alice-dev in {names:?}"
    );
    assert!(
        names.contains(&"Alice-main".to_string()),
        "expected Alice-main in {names:?}"
    );
}

/// Skip strategy: skip conflicting commits, replay non-conflicting.
/// After rebase, the non-conflicting data is present, conflicting data is not.
#[tokio::test]
async fn rebase_skip_conflicting() {
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

    // Two commits on dev: first overlaps, second doesn't
    let dev_ledger = fluree.ledger("mydb:dev").await.unwrap();
    let dev_data1 = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice-dev"}]
    });
    let result = fluree.insert(dev_ledger, &dev_data1).await.unwrap();

    let dev_data2 = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:eve", "ex:name": "Eve"}]
    });
    fluree.insert(result.ledger, &dev_data2).await.unwrap();

    // Overlapping data on main
    let main_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice-main"}]
    });
    fluree.insert(main_ledger, &main_data).await.unwrap();

    let report = fluree
        .rebase_branch("mydb", "dev", ConflictStrategy::Skip)
        .await
        .unwrap();

    assert!(!report.fast_forward);
    assert_eq!(report.skipped, 1);
    assert_eq!(report.replayed, 1);
    assert_eq!(report.total_commits, 2);

    // Dev should have Eve (non-conflicting, replayed onto source state)
    let names = query_all_names(&fluree, "mydb:dev").await;
    assert!(
        names.contains(&"Eve".to_string()),
        "expected Eve in {names:?}"
    );
}

/// Branch point is updated after rebase.
#[tokio::test]
async fn rebase_branch_point_updated() {
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

    // Advance main
    let main_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:carol", "ex:name": "Carol"}]
    });
    fluree.insert(main_ledger, &main_data).await.unwrap();

    let source_after = fluree
        .nameservice()
        .lookup("mydb:main")
        .await
        .unwrap()
        .unwrap();

    // Rebase
    fluree
        .rebase_branch("mydb", "dev", ConflictStrategy::default())
        .await
        .unwrap();

    // Verify branch point updated
    let dev_record = fluree
        .nameservice()
        .lookup("mydb:dev")
        .await
        .unwrap()
        .unwrap();
    // After rebase, the branch's commit_t should match the source's commit_t
    // (fast-forward rebase advances the branch to the source HEAD).
    assert_eq!(dev_record.commit_t, source_after.commit_t);
}

/// Cannot rebase the root branch — refusal is structural, not name-based.
/// "main" is the default and so the root on most ledgers, but a ledger
/// created with a different initial branch (e.g. "trunk") will refuse
/// rebases against that branch instead, while allowing rebases on a
/// non-root branch that happens to be named "main".
#[tokio::test]
async fn rebase_main_refused() {
    let fluree = FlureeBuilder::memory().build_memory();
    fluree.create_ledger("mydb").await.unwrap();

    let err = fluree
        .rebase_branch("mydb", "main", ConflictStrategy::default())
        .await
        .expect_err("rebasing main should fail");

    let msg = err.to_string();
    assert!(
        msg.contains("root") && msg.contains("main"),
        "expected error about root, got: {msg}"
    );
}

/// Mirror of the structural refusal for a non-main root.
#[tokio::test]
async fn rebase_refuses_non_main_root() {
    let fluree = FlureeBuilder::memory().build_memory();
    fluree.create_ledger("mydb:trunk").await.unwrap();

    let err = fluree
        .rebase_branch("mydb", "trunk", ConflictStrategy::default())
        .await
        .expect_err("rebasing non-main root should fail");

    let msg = err.to_string();
    assert!(
        msg.contains("root") && msg.contains("trunk"),
        "expected error about root, got: {msg}"
    );
}

/// Large rebase triggers mid-replay index build when novelty exceeds threshold.
/// Uses a low reindex_min_bytes so a few commits are enough to trigger the flush.
#[tokio::test]
async fn rebase_flush_novelty_mid_replay() {
    // Set a very low soft threshold so novelty flush triggers after a couple of commits.
    let fluree = FlureeBuilder::memory()
        .with_indexing_thresholds(500, 1_000_000)
        .build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();

    let base = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:seed", "ex:val": 1}]
    });
    let result = fluree.insert(ledger, &base).await.unwrap();
    let main_ledger = result.ledger;

    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();

    // Make several commits on dev so replaying them exceeds the threshold.
    let dev_ledger = fluree.ledger("mydb:dev").await.unwrap();
    let mut current = dev_ledger;
    for i in 0..5 {
        let data = json!({
            "@context": {"ex": "http://example.org/ns/"},
            "@graph": [{"@id": format!("ex:item-{i}"), "ex:name": format!("Item {i}")}]
        });
        let r = fluree.insert(current, &data).await.unwrap();
        current = r.ledger;
    }

    // Advance main so it's not a fast-forward.
    let main_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:main-only", "ex:name": "MainOnly"}]
    });
    fluree.insert(main_ledger, &main_data).await.unwrap();

    // Rebase — this should trigger at least one mid-replay index build.
    let report = fluree
        .rebase_branch("mydb", "dev", ConflictStrategy::TakeBoth)
        .await
        .unwrap();

    assert!(!report.fast_forward);
    assert_eq!(report.replayed, 5);
    assert!(report.failures.is_empty());

    // Verify replayed data is accessible after rebase.
    let names = query_all_names(&fluree, "mydb:dev").await;
    assert!(
        names.contains(&"Item 0".to_string()),
        "missing Item 0 in {names:?}"
    );
    assert!(
        names.contains(&"Item 4".to_string()),
        "missing Item 4 in {names:?}"
    );
}

/// Rebasing a branch-of-a-branch works correctly.
/// The BranchedContentStore must traverse two levels of ancestry.
#[tokio::test]
async fn rebase_nested_branch() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();

    let base = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
    });
    let result = fluree.insert(ledger, &base).await.unwrap();
    let main_ledger = result.ledger;

    // Create dev from main
    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();

    // Create feature from dev
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

    // Advance dev (feature's source)
    let dev_ledger = fluree.ledger("mydb:dev").await.unwrap();
    let dev_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:carol", "ex:name": "Carol"}]
    });
    fluree.insert(dev_ledger, &dev_data).await.unwrap();

    // Also advance main (dev's source) — not directly relevant but exercises
    // the full ancestry chain.
    let main_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:dave", "ex:name": "Dave"}]
    });
    fluree.insert(main_ledger, &main_data).await.unwrap();

    // Rebase feature onto dev's current HEAD
    let report = fluree
        .rebase_branch("mydb", "feature", ConflictStrategy::TakeBoth)
        .await
        .unwrap();

    assert!(!report.fast_forward);
    assert_eq!(report.replayed, 1);
    assert!(report.conflicts.is_empty());

    // Feature should see Alice (base), Carol (from dev), and Bob (replayed)
    let names = query_all_names(&fluree, "mydb:feature").await;
    assert!(
        names.contains(&"Alice".to_string()),
        "missing Alice in {names:?}"
    );
    assert!(
        names.contains(&"Carol".to_string()),
        "missing Carol in {names:?}"
    );
    assert!(
        names.contains(&"Bob".to_string()),
        "missing Bob in {names:?}"
    );

    // Feature should NOT see Dave (main-only, not in dev's ancestry at branch time)
    // Note: Dave was added to main after dev was created, and dev hasn't been rebased.
}

/// TakeBranch retracts the source's values under each conflict key before
/// replaying the branch commit. When both sides made the identical change,
/// the branch's assert is a no-op and an unfiltered retract would wipe the
/// value both sides agree on.
#[tokio::test]
async fn rebase_take_branch_keeps_value_both_sides_asserted() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();
    let main = fluree
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

    let replace = |name: &str| {
        json!({
            "@context": {"ex": "http://example.org/ns/"},
            "where": {"@id": "ex:alice", "ex:name": "?old"},
            "delete": {"@id": "ex:alice", "ex:name": "?old"},
            "insert": {"@id": "ex:alice", "ex:name": name}
        })
    };
    let dev = fluree.ledger("mydb:dev").await.unwrap();
    fluree.update(dev, &replace("C")).await.unwrap();
    fluree.update(main, &replace("C")).await.unwrap();

    let report = fluree
        .rebase_branch("mydb", "dev", ConflictStrategy::TakeBranch)
        .await
        .unwrap();
    assert_eq!(report.replayed, 1);
    assert_eq!(report.conflicts.len(), 1);

    assert_eq!(query_all_names(&fluree, "mydb:dev").await, vec!["C"]);
}

/// Rebasing a branch that has already merged its source in.
///
/// The branch's own commits sit under a merge commit, and the source's head
/// arrived through that merge. The branch's commits must still be replayed.
#[tokio::test]
async fn rebase_after_merging_the_source_in() {
    let fluree = FlureeBuilder::memory().build_memory();
    fluree.create_ledger("mydb").await.unwrap();
    let insert = |ledger: &'static str, id: &'static str| {
        let fluree = &fluree;
        async move {
            let state = fluree.ledger(ledger).await.unwrap();
            fluree
                .insert(
                    state,
                    &json!({
                        "@context": {"ex": "http://example.org/ns/"},
                        "@graph": [{"@id": format!("ex:{id}"), "ex:name": id}]
                    }),
                )
                .await
                .unwrap();
        }
    };
    insert("mydb:main", "a").await;
    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();
    insert("mydb:dev", "d2").await;
    // main runs ahead of dev, so its `t` values cover dev's own.
    for m in ["m2", "m3", "m4", "m5"] {
        insert("mydb:main", m).await;
    }

    // dev merges main in, then commits again.
    fluree
        .merge_branch("mydb", "main", Some("dev"), ConflictStrategy::default())
        .await
        .unwrap();
    insert("mydb:dev", "d4").await;
    insert("mydb:main", "m6").await;

    let report = fluree
        .rebase_branch("mydb", "dev", ConflictStrategy::default())
        .await
        .unwrap();
    assert!(!report.fast_forward, "dev has commits of its own");
    assert_eq!(
        query_all_names(&fluree, "mydb:dev").await,
        ["a", "d2", "d4", "m2", "m3", "m4", "m5", "m6"]
    );
}

/// Rebasing after a sync, where both sides changed the same fact since.
///
/// The conflict is between the branch's own commit and what the source did
/// after the sync. The sync itself is not part of either side's changes.
#[tokio::test]
async fn rebase_after_sync_detects_conflict() {
    let fluree = FlureeBuilder::memory().build_memory();
    fluree.create_ledger("mydb").await.unwrap();
    let rename = |ledger: &'static str, name: &'static str| {
        let fluree = &fluree;
        async move {
            let state = fluree.ledger(ledger).await.unwrap();
            fluree
                .update(
                    state,
                    &json!({
                        "@context": {"ex": "http://example.org/ns/"},
                        "where": {"@id": "ex:alice", "ex:name": "?old"},
                        "delete": {"@id": "ex:alice", "ex:name": "?old"},
                        "insert": {"@id": "ex:alice", "ex:name": name}
                    }),
                )
                .await
                .unwrap();
        }
    };
    let main = fluree.ledger("mydb:main").await.unwrap();
    fluree
        .insert(
            main,
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

    // dev syncs main in, then both rename the same fact.
    rename("mydb:main", "from-main").await;
    fluree
        .merge_branch("mydb", "main", Some("dev"), ConflictStrategy::default())
        .await
        .unwrap();
    rename("mydb:dev", "from-dev").await;
    rename("mydb:main", "main-again").await;

    let err = fluree
        .rebase_branch("mydb", "dev", ConflictStrategy::Abort)
        .await
        .expect_err("both sides renamed alice after the sync");
    assert!(err.to_string().contains("conflict"), "{err}");

    let report = fluree
        .rebase_branch("mydb", "dev", ConflictStrategy::TakeBranch)
        .await
        .unwrap();
    assert_eq!(report.replayed, 1, "dev's own rename");
    assert_eq!(query_all_names(&fluree, "mydb:dev").await, ["from-dev"]);
}

/// Set up a branch that synced its source in through a real merge commit,
/// after which the source changed one of the synced facts again.
///
/// main holds `ex:alice`. dev commits `ex:d1` first, so the sync is a merge
/// rather than a fast-forward, then syncs main's `v1` rename in. main then
/// renames to `v2`.
async fn sync_then_source_moves_on() -> support::MemoryFluree {
    let fluree = FlureeBuilder::memory().build_memory();
    fluree.create_ledger("mydb").await.unwrap();
    let insert = |ledger: &'static str, id: &'static str, name: &'static str| {
        let fluree = &fluree;
        async move {
            let state = fluree.ledger(ledger).await.unwrap();
            fluree
                .insert(
                    state,
                    &json!({
                        "@context": {"ex": "http://example.org/ns/"},
                        "@graph": [{"@id": id, "ex:name": name}]
                    }),
                )
                .await
                .unwrap();
        }
    };
    let rename = |ledger: &'static str, name: &'static str| {
        let fluree = &fluree;
        async move {
            let state = fluree.ledger(ledger).await.unwrap();
            fluree
                .update(
                    state,
                    &json!({
                        "@context": {"ex": "http://example.org/ns/"},
                        "where": {"@id": "ex:alice", "ex:name": "?old"},
                        "delete": {"@id": "ex:alice", "ex:name": "?old"},
                        "insert": {"@id": "ex:alice", "ex:name": name}
                    }),
                )
                .await
                .unwrap();
        }
    };

    insert("mydb:main", "ex:alice", "Alice").await;
    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();
    insert("mydb:dev", "ex:d1", "d1").await;
    rename("mydb:main", "v1").await;
    fluree
        .merge_branch("mydb", "main", Some("dev"), ConflictStrategy::default())
        .await
        .unwrap();
    rename("mydb:main", "v2").await;
    fluree
}

/// The sync's copy of the source's changes is not the branch's own change,
/// so it cannot conflict with what the source did afterwards.
#[tokio::test]
async fn rebase_after_sync_does_not_conflict_on_the_syncs_copy() {
    let fluree = sync_then_source_moves_on().await;

    let report = fluree
        .rebase_branch("mydb", "dev", ConflictStrategy::Abort)
        .await
        .expect("dev never touched alice, so there is nothing to conflict");
    assert_eq!(report.replayed, 1, "dev's own commit");
    assert_eq!(query_all_names(&fluree, "mydb:dev").await, ["d1", "v2"]);
}

/// Replaying the sync must not write the source's old value back over the
/// value the source holds now.
#[tokio::test]
async fn rebase_after_sync_keeps_the_sources_later_value() {
    let fluree = sync_then_source_moves_on().await;

    fluree
        .rebase_branch("mydb", "dev", ConflictStrategy::TakeBranch)
        .await
        .unwrap();
    assert_eq!(query_all_names(&fluree, "mydb:dev").await, ["d1", "v2"]);
}

/// Replaying a commit that created a named graph onto a base that never
/// registered it routes the graph's flakes and registers the graph.
#[tokio::test]
async fn rebase_replays_commit_that_creates_named_graph() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();
    let ctx = json!({"ex": "http://example.org/ns/"});
    let main_ledger = fluree
        .insert(
            ledger,
            &json!({"@context": ctx, "@id": "ex:alice", "ex:name": "Alice"}),
        )
        .await
        .unwrap()
        .ledger;
    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();

    let graph = "http://example.org/g1";
    let dev = fluree.ledger("mydb:dev").await.unwrap();
    fluree
        .update(
            dev,
            &json!({"@context": ctx, "graph": graph, "insert": {"@id": "ex:dave", "ex:name": "Dave"}}),
        )
        .await
        .unwrap();
    fluree
        .insert(
            main_ledger,
            &json!({"@context": ctx, "@id": "ex:carol", "ex:name": "Carol"}),
        )
        .await
        .unwrap();

    let report = fluree
        .rebase_branch("mydb", "dev", ConflictStrategy::TakeBoth)
        .await
        .expect("rebase should replay the graph-creating commit");
    assert!(!report.fast_forward);
    assert_eq!(report.replayed, 1);
    assert!(report.failures.is_empty(), "{:?}", report.failures);

    let dev = fluree.ledger("mydb:dev").await.unwrap();
    let q = json!({
        "@context": ctx,
        "from": format!("mydb:dev#{graph}"),
        "select": "?n",
        "where": {"@id": "ex:dave", "ex:name": "?n"}
    });
    let rows = fluree
        .query_connection(&q)
        .await
        .expect("the replayed graph must be queryable")
        .to_jsonld(&dev.snapshot)
        .unwrap();
    assert_eq!(rows, json!(["Dave"]));
}

/// A branch with its own commits, `bob` and `carol` after the fork at
/// `alice`, and a source that moved on to `dave`.
async fn diverged(fluree: &fluree_db_api::Fluree) {
    let name = |n: &str| {
        json!({"@context": {"ex": "http://example.org/ns/"},
               "@graph": [{"@id": format!("ex:{n}"), "ex:name": n}]})
    };
    let main = fluree.create_ledger("rb").await.unwrap();
    let main = fluree.insert(main, &name("alice")).await.unwrap().ledger;
    fluree.create_branch("rb", "dev", None, None).await.unwrap();
    for n in ["bob", "carol"] {
        let dev = fluree.ledger("rb:dev").await.unwrap();
        fluree.insert(dev, &name(n)).await.unwrap();
    }
    fluree.insert(main, &name("dave")).await.unwrap();
}

fn file_fluree(dir: &tempfile::TempDir) -> fluree_db_api::Fluree {
    FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .without_indexing()
        .build()
        .unwrap()
}

const EVERY_NAME: [&str; 4] = ["alice", "bob", "carol", "dave"];

/// A rebase that replays the branch's commits takes the branch's index of
/// the line it replaced away, even where the source's index is older or
/// absent: a fresh load of the branch reads its new line, not that index
/// with the new line's later commits on top.
#[tokio::test]
async fn a_rebase_takes_away_the_index_of_the_line_it_replaced() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = file_fluree(&dir);
    diverged(&fluree).await;
    support::rebuild_and_publish_index(&fluree, "rb:dev").await;

    let report = fluree
        .rebase_branch("rb", "dev", ConflictStrategy::default())
        .await
        .unwrap();
    assert_eq!(report.replayed, 2);
    drop(fluree);

    let fresh = file_fluree(&dir);
    assert_eq!(query_all_names(&fresh, "rb:dev").await, EVERY_NAME);
}

/// An index of a branch's line built before a rebase and published after it
/// is not extended over the new line: the incremental build refuses it, and
/// the build falls back to a full rebuild of the new line. An index on the
/// head's line is extended as before.
#[tokio::test]
async fn an_index_of_a_replaced_line_is_not_extended() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = file_fluree(&dir);
    diverged(&fluree).await;
    let before = fluree
        .nameservice()
        .lookup("rb:dev")
        .await
        .unwrap()
        .unwrap();
    let store = fluree.branched_content_store("rb:dev").await.unwrap();
    let stale = fluree_db_indexer::rebuild_index_from_commits(
        store,
        "rb:dev",
        &before,
        fluree_db_indexer::IndexerConfig::default(),
    )
    .await
    .unwrap();

    fluree
        .rebase_branch("rb", "dev", ConflictStrategy::default())
        .await
        .unwrap();
    support::publish_built_index(&fluree, "rb:dev", stale.index_t, &stale.root_id).await;

    let record = fluree
        .nameservice()
        .lookup("rb:dev")
        .await
        .unwrap()
        .unwrap();
    let refused = fluree_db_indexer::build_incremental_index_for_record(
        fluree.branched_content_store("rb:dev").await.unwrap(),
        &record,
        fluree_db_indexer::IndexerConfig::default(),
    )
    .await;
    assert!(refused.is_err(), "the replaced line's index was extended");
    support::build_and_publish_index(&fluree, "rb:dev").await;
    drop(fluree);
    let fresh = file_fluree(&dir);
    assert_eq!(query_all_names(&fresh, "rb:dev").await, EVERY_NAME);

    // On the head's line, the index is extended.
    let dev = fresh.ledger("rb:dev").await.unwrap();
    fresh
        .insert(
            dev,
            &json!({"@context": {"ex": "http://example.org/ns/"},
                    "@id": "ex:erin", "ex:name": "erin"}),
        )
        .await
        .unwrap();
    support::build_incrementally_and_publish_index(&fresh, "rb:dev").await;
}

/// The nameservice's commit-CID index keeps the entries of the line a rebase
/// replaced below the new head, so a build handed that index for its walk
/// sees every `t` covered while the entries are not the head's line. The
/// build checks the entries link and walks the line itself when they do not.
#[tokio::test]
async fn a_build_from_the_commit_index_reads_the_rebased_line() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = file_fluree(&dir);
    let name = |n: &str| {
        json!({"@context": {"ex": "http://example.org/ns/"},
               "@graph": [{"@id": format!("ex:{n}"), "ex:name": n}]})
    };
    let main = fluree.create_ledger("rb").await.unwrap();
    let main = fluree.insert(main, &name("alice")).await.unwrap().ledger;
    support::rebuild_and_publish_index(&fluree, "rb:main").await;
    fluree.create_branch("rb", "dev", None, None).await.unwrap();
    for n in ["bob", "carol"] {
        let dev = fluree.ledger("rb:dev").await.unwrap();
        fluree.insert(dev, &name(n)).await.unwrap();
    }
    fluree.insert(main, &name("dave")).await.unwrap();
    fluree
        .rebase_branch("rb", "dev", ConflictStrategy::default())
        .await
        .unwrap();

    let record = fluree
        .nameservice()
        .lookup("rb:dev")
        .await
        .unwrap()
        .unwrap();
    let pending = fluree
        .branch_admin()
        .unwrap()
        .pending_commit_cids("rb:dev", record.index_t)
        .await
        .unwrap();
    let covered: Vec<i64> = pending.iter().flatten().map(|(t, _)| *t).collect();
    assert_eq!(
        covered,
        (record.index_t + 1..=record.commit_t).collect::<Vec<_>>(),
        "the commit index no longer covers the range, so the build would not use it"
    );
    let config = fluree_db_indexer::IndexerConfig {
        pending_commit_cids: pending,
        ..Default::default()
    };
    let built = fluree_db_indexer::build_incremental_index_for_record(
        fluree.branched_content_store("rb:dev").await.unwrap(),
        &record,
        config,
    )
    .await
    .unwrap();
    support::publish_built_index(&fluree, "rb:dev", built.index_t, &built.root_id).await;
    drop(fluree);

    let fresh = file_fluree(&dir);
    assert_eq!(query_all_names(&fresh, "rb:dev").await, EVERY_NAME);
}

/// A `Skip` rebase that skips every commit makes the branch the source: its
/// head and index move to the source's together, so it reads the source's
/// line and writes on top of the source's head.
#[tokio::test]
async fn a_rebase_that_skips_every_commit_makes_the_branch_the_source() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = file_fluree(&dir);
    let name = |s: &str, n: &str| {
        json!({"@context": {"ex": "http://example.org/ns/"},
               "@graph": [{"@id": format!("ex:{s}"), "ex:name": n}]})
    };
    let main = fluree.create_ledger("rb").await.unwrap();
    let main = fluree
        .insert(main, &name("alice", "alice"))
        .await
        .unwrap()
        .ledger;
    fluree.create_branch("rb", "dev", None, None).await.unwrap();
    let dev = fluree.ledger("rb:dev").await.unwrap();
    fluree
        .insert(dev, &name("alice", "alice-dev"))
        .await
        .unwrap();
    let main = fluree
        .insert(main, &name("alice", "alice-main"))
        .await
        .unwrap()
        .ledger;
    fluree.insert(main, &name("dave", "dave")).await.unwrap();
    support::rebuild_and_publish_index(&fluree, "rb:main").await;

    let report = fluree
        .rebase_branch("rb", "dev", ConflictStrategy::Skip)
        .await
        .unwrap();
    assert_eq!((report.replayed, report.skipped), (0, 1));

    let lookup = |id: &'static str| fluree.nameservice().lookup(id);
    let (source, branch) = (
        lookup("rb:main").await.unwrap().unwrap(),
        lookup("rb:dev").await.unwrap().unwrap(),
    );
    assert_eq!(
        (branch.commit_head_id, branch.commit_t, branch.index_t),
        (source.commit_head_id, source.commit_t, source.index_t)
    );
    let source_line = ["alice", "alice-main", "dave"];
    assert_eq!(query_all_names(&fluree, "rb:dev").await, source_line);
    let dev = fluree.ledger("rb:dev").await.unwrap();
    fluree
        .insert(dev, &name("erin", "erin"))
        .await
        .expect("the branch writes on the source's head");
    drop(fluree);

    let fresh = file_fluree(&dir);
    assert_eq!(
        query_all_names(&fresh, "rb:dev").await,
        ["alice", "alice-main", "dave", "erin"]
    );
}

/// An instance holding the branch warm across another instance's rebase
/// does not apply the new line's commits onto the line they replaced: the
/// commits sit on another commit at its `t`, so it reloads.
#[tokio::test]
async fn a_warm_cache_reloads_across_another_instances_rebase() {
    let dir = tempfile::tempdir().unwrap();
    let rebaser = file_fluree(&dir);
    diverged(&rebaser).await;
    let warm = file_fluree(&dir);
    assert_eq!(
        query_cached_names(&warm, "rb:dev").await,
        ["alice", "bob", "carol"]
    );

    rebaser
        .rebase_branch("rb", "dev", ConflictStrategy::default())
        .await
        .unwrap();
    let refreshed = warm
        .refresh("rb:dev", Default::default())
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(refreshed.action, fluree_db_api::NotifyResult::Reloaded),
        "{:?}",
        refreshed.action
    );
    assert_eq!(query_cached_names(&warm, "rb:dev").await, EVERY_NAME);
}

async fn dev_record(fluree: &fluree_db_api::Fluree) -> fluree_db_nameservice::NsRecord {
    fluree
        .nameservice()
        .lookup("rb:dev")
        .await
        .unwrap()
        .unwrap()
}

/// Whether the indexer refuses to publish a build of `built` on `rb:dev`.
async fn refused(fluree: &fluree_db_api::Fluree, built: &fluree_db_nameservice::NsRecord) -> bool {
    let store = fluree.branched_content_store("rb:dev").await.unwrap();
    fluree_db_indexer::refuse_build_off_the_line(fluree.nameservice(), store.as_ref(), built)
        .await
        .is_err()
}

/// A build from the branch's record before a rebase is not published after
/// it: its head is no longer on the branch's line. A build of the head's
/// line is published, including one that commits have since moved past.
#[tokio::test]
async fn a_build_of_the_replaced_line_is_not_published() {
    let dir = tempfile::tempdir().unwrap();
    let fluree = file_fluree(&dir);
    diverged(&fluree).await;
    let before = dev_record(&fluree).await;
    assert!(!refused(&fluree, &before).await, "a build of the head");

    fluree
        .rebase_branch("rb", "dev", ConflictStrategy::default())
        .await
        .unwrap();
    assert!(
        refused(&fluree, &before).await,
        "a build of the replaced line"
    );

    let rebased = dev_record(&fluree).await;
    let dev = fluree.ledger("rb:dev").await.unwrap();
    fluree
        .insert(
            dev,
            &json!({"@context": {"ex": "http://example.org/ns/"},
                    "@id": "ex:erin", "ex:name": "erin"}),
        )
        .await
        .unwrap();
    assert!(
        !refused(&fluree, &rebased).await,
        "a build of the head's line, behind the head"
    );
}

// A query peer reads the writer's root and follows the writer's records
// (`LedgerManager::follow`). A record can trail the peer's cache, which a
// load reads from the nameservice ahead of the event stream; only one that
// replaces the cached line reloads it.

async fn peer_fluree(dir: &tempfile::TempDir) -> fluree_db_api::Fluree {
    FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .read_only()
        .build_client()
        .await
        .unwrap()
}

async fn follow(
    peer: &fluree_db_api::Fluree,
    record: fluree_db_nameservice::NsRecord,
) -> fluree_db_api::NotifyResult {
    peer.ledger_manager()
        .expect("a peer caches ledgers")
        .follow(record)
        .await
        .unwrap()
}

/// A branch whose `dev_commits` commits after the fork each conflict with
/// the source's `alice-main`, and a source that moved on to `dave` at `t` 3:
/// a `Skip` rebase skips them all and moves the branch to the source's head.
async fn conflicting_branch(fluree: &fluree_db_api::Fluree, dev_commits: usize) {
    let name = |s: &str, n: &str| {
        json!({"@context": {"ex": "http://example.org/ns/"},
               "@graph": [{"@id": format!("ex:{s}"), "ex:name": n}]})
    };
    let main = fluree.create_ledger("rb").await.unwrap();
    let main = fluree
        .insert(main, &name("alice", "alice"))
        .await
        .unwrap()
        .ledger;
    fluree.create_branch("rb", "dev", None, None).await.unwrap();
    for i in 0..dev_commits {
        let dev = fluree.ledger("rb:dev").await.unwrap();
        fluree
            .insert(dev, &name("alice", &format!("alice-dev{i}")))
            .await
            .unwrap();
    }
    let main = fluree
        .insert(main, &name("alice", "alice-main"))
        .await
        .unwrap()
        .ledger;
    fluree.insert(main, &name("dave", "dave")).await.unwrap();
}

/// A record behind the peer's cache, on its line, is stale: the peer keeps
/// its state and does not reload. The writer's record is left as it was.
#[tokio::test]
async fn a_peer_keeps_its_cache_over_a_record_it_has_passed() {
    let dir = tempfile::tempdir().unwrap();
    let writer = file_fluree(&dir);
    diverged(&writer).await;
    let behind = dev_record(&writer).await;
    let dev = writer.ledger("rb:dev").await.unwrap();
    writer
        .insert(
            dev,
            &json!({"@context": {"ex": "http://example.org/ns/"},
                    "@id": "ex:erin", "ex:name": "erin"}),
        )
        .await
        .unwrap();
    let head = dev_record(&writer).await;

    let peer = peer_fluree(&dir).await;
    assert!(peer.nameservice_mode().is_read_only());
    assert_eq!(
        query_cached_names(&peer, "rb:dev").await,
        ["alice", "bob", "carol", "erin"]
    );
    assert!(behind.commit_t < head.commit_t);
    assert_eq!(
        follow(&peer, behind).await,
        fluree_db_api::NotifyResult::Current
    );
    assert_eq!(
        query_cached_names(&peer, "rb:dev").await,
        ["alice", "bob", "carol", "erin"]
    );
    let after = dev_record(&writer).await;
    assert_eq!(
        (after.commit_head_id, after.commit_t),
        (head.commit_head_id, head.commit_t)
    );
}

/// A rebase can move a branch's head below the peer's cached `t`, onto
/// another line: the peer reloads and reads the new line.
#[tokio::test]
async fn a_peer_follows_a_rebase_that_moves_the_head_back() {
    let dir = tempfile::tempdir().unwrap();
    let writer = file_fluree(&dir);
    conflicting_branch(&writer, 3).await;
    let peer = peer_fluree(&dir).await;
    assert_eq!(
        query_cached_names(&peer, "rb:dev").await,
        ["alice", "alice-dev0", "alice-dev1", "alice-dev2"]
    );

    writer
        .rebase_branch("rb", "dev", ConflictStrategy::Skip)
        .await
        .unwrap();
    let rebased = dev_record(&writer).await;
    assert_eq!(rebased.commit_t, 3, "below the peer's t of 4");
    assert_eq!(
        follow(&peer, rebased).await,
        fluree_db_api::NotifyResult::Reloaded
    );
    assert_eq!(
        query_cached_names(&peer, "rb:dev").await,
        ["alice", "alice-main", "dave"]
    );
}

/// A rebase can leave a branch's head at the peer's cached `t`, on another
/// commit: the peer reloads and reads the new line.
#[tokio::test]
async fn a_peer_follows_a_rebase_to_another_head_at_its_t() {
    let dir = tempfile::tempdir().unwrap();
    let writer = file_fluree(&dir);
    conflicting_branch(&writer, 2).await;
    let peer = peer_fluree(&dir).await;
    assert_eq!(
        query_cached_names(&peer, "rb:dev").await,
        ["alice", "alice-dev0", "alice-dev1"]
    );

    writer
        .rebase_branch("rb", "dev", ConflictStrategy::Skip)
        .await
        .unwrap();
    let rebased = dev_record(&writer).await;
    assert_eq!(rebased.commit_t, 3, "the peer's t");
    assert_eq!(
        follow(&peer, rebased).await,
        fluree_db_api::NotifyResult::Reloaded
    );
    assert_eq!(
        query_cached_names(&peer, "rb:dev").await,
        ["alice", "alice-main", "dave"]
    );
}

/// A rebase resets the branch's index to the source's or to none: once the
/// nameservice holds the peer's head with a lower index, or none, the peer
/// reloads and drops the index. A record saying so while the nameservice
/// still holds the index is not a reset, and changes nothing.
#[tokio::test]
async fn a_peer_follows_a_reset_that_lowers_the_index_of_its_head() {
    let dir = tempfile::tempdir().unwrap();
    let writer = file_fluree(&dir);
    diverged(&writer).await;
    support::rebuild_and_publish_index(&writer, "rb:dev").await;
    let indexed = dev_record(&writer).await;
    assert!(indexed.index_head_id.is_some());

    let peer = peer_fluree(&dir).await;
    let cached_index = || async {
        let handle = peer.ledger_cached("rb:dev").await.unwrap();
        handle.state_metrics().await.2
    };
    assert_eq!(cached_index().await, indexed.index_head_id);
    assert_eq!(
        follow(&peer, indexed.clone()).await,
        fluree_db_api::NotifyResult::Current
    );
    let unindexed = fluree_db_nameservice::NsRecord {
        index_head_id: None,
        index_t: 0,
        ..indexed.clone()
    };
    assert_eq!(
        follow(&peer, unindexed).await,
        fluree_db_api::NotifyResult::Current,
        "the nameservice still holds the index"
    );
    assert_eq!(cached_index().await, indexed.index_head_id);

    writer
        .branch_admin()
        .unwrap()
        .reset_head(
            "rb:dev",
            fluree_db_nameservice::NsRecordSnapshot {
                commit_head_id: indexed.commit_head_id.clone(),
                commit_t: indexed.commit_t,
                index_head_id: None,
                index_t: 0,
            },
        )
        .await
        .unwrap();
    let reset = dev_record(&writer).await;
    assert_eq!(reset.index_head_id, None);
    assert_eq!(
        follow(&peer, reset).await,
        fluree_db_api::NotifyResult::Reloaded
    );
    assert_eq!(cached_index().await, None);
    assert_eq!(
        query_cached_names(&peer, "rb:dev").await,
        ["alice", "bob", "carol"]
    );
}

/// A record naming the peer's head with an index the nameservice no longer
/// holds (one sent before a reset, or a reset read half-written) does not
/// install that index.
#[tokio::test]
async fn a_peer_does_not_install_an_index_a_reset_took_away() {
    let dir = tempfile::tempdir().unwrap();
    let writer = file_fluree(&dir);
    diverged(&writer).await;
    support::rebuild_and_publish_index(&writer, "rb:dev").await;
    let indexed = dev_record(&writer).await;
    assert!(indexed.index_head_id.is_some());
    writer
        .branch_admin()
        .unwrap()
        .reset_head(
            "rb:dev",
            fluree_db_nameservice::NsRecordSnapshot {
                commit_head_id: indexed.commit_head_id.clone(),
                commit_t: indexed.commit_t,
                index_head_id: None,
                index_t: 0,
            },
        )
        .await
        .unwrap();

    let peer = peer_fluree(&dir).await;
    let cached_index_t = || async {
        let handle = peer.ledger_cached("rb:dev").await.unwrap();
        handle.index_t().await
    };
    assert_eq!(cached_index_t().await, 0);
    assert_eq!(
        follow(&peer, indexed).await,
        fluree_db_api::NotifyResult::Current
    );
    assert_eq!(cached_index_t().await, 0, "no index installed");
}

/// A peer draining a backlog sees records far behind its cache, from its own
/// line: each changes nothing, however far behind it is.
#[tokio::test]
async fn a_peer_keeps_its_cache_over_a_record_far_behind_it() {
    let dir = tempfile::tempdir().unwrap();
    let writer = file_fluree(&dir);
    let mut ledger = writer.create_ledger("bk").await.unwrap();
    let mut first = None;
    for n in 0..8 {
        ledger = writer
            .insert(
                ledger,
                &json!({"@context": {"ex": "http://example.org/ns/"},
                        "@id": format!("ex:s{n}"), "ex:name": format!("s{n}")}),
            )
            .await
            .unwrap()
            .ledger;
        if first.is_none() {
            first = writer.nameservice().lookup("bk:main").await.unwrap();
        }
    }
    let first = first.unwrap();
    assert_eq!(first.commit_t, 1);

    let peer = peer_fluree(&dir).await;
    peer.ledger_cached("bk:main").await.unwrap();
    assert_eq!(
        follow(&peer, first).await,
        fluree_db_api::NotifyResult::Current
    );
}

/// A nameservice whose lookup of one ledger, while armed, reads the record
/// and then waits to return it: a load that read the nameservice before a
/// rebase and lands after it.
#[derive(Debug)]
struct PausedLookup {
    inner: std::sync::Arc<dyn fluree_db_nameservice::NameServiceLookup>,
    ledger_id: &'static str,
    armed: std::sync::atomic::AtomicBool,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

mod paused_lookup {
    use super::PausedLookup;
    use async_trait::async_trait;
    use fluree_db_nameservice::{
        ConfigLookup, ConfigValue, GraphSourceLookup, GraphSourceRecord, LedgerHeads,
        NameServiceLookup, NsLookupResult, NsRecord, RefKind, RefLookup, RefValue, Result,
        StatusLookup, StatusValue,
    };
    use std::sync::atomic::Ordering;

    #[async_trait]
    impl GraphSourceLookup for PausedLookup {
        async fn lookup_graph_source(&self, id: &str) -> Result<Option<GraphSourceRecord>> {
            self.inner.lookup_graph_source(id).await
        }
        async fn lookup_any(&self, id: &str) -> Result<NsLookupResult> {
            self.inner.lookup_any(id).await
        }
        async fn all_graph_source_records(&self) -> Result<Vec<GraphSourceRecord>> {
            self.inner.all_graph_source_records().await
        }
    }

    #[async_trait]
    impl RefLookup for PausedLookup {
        async fn get_ref(&self, id: &str, kind: RefKind) -> Result<Option<RefValue>> {
            self.inner.get_ref(id, kind).await
        }
    }

    #[async_trait]
    impl StatusLookup for PausedLookup {
        async fn get_status(&self, id: &str) -> Result<Option<StatusValue>> {
            self.inner.get_status(id).await
        }
    }

    #[async_trait]
    impl ConfigLookup for PausedLookup {
        async fn get_config(&self, id: &str) -> Result<Option<ConfigValue>> {
            self.inner.get_config(id).await
        }
    }

    #[async_trait]
    impl NameServiceLookup for PausedLookup {
        async fn lookup(&self, id: &str) -> Result<Option<NsRecord>> {
            let record = self.inner.lookup(id).await?;
            if id == self.ledger_id && self.armed.swap(false, Ordering::SeqCst) {
                self.entered.notify_one();
                self.release.notified().await;
            }
            Ok(record)
        }
        async fn all_records(&self) -> Result<Vec<NsRecord>> {
            self.inner.all_records().await
        }
        async fn branch_records(&self, name: &str) -> Result<Vec<NsRecord>> {
            self.inner.branch_records(name).await
        }
        async fn heads(&self, id: &str) -> Result<Option<LedgerHeads>> {
            self.inner.heads(id).await
        }
    }
}

/// An index update for the branch's old line, still installing when a
/// rebase is followed, does not land after it: the peer ends on the new
/// line, not on the old line's index. The update comes from `follow` itself
/// or from another path that brings the cache up to date, `refresh`.
async fn a_rebase_outlasts_an_index_install_in_flight(via_refresh: bool) {
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();
    let writer = file_fluree(&dir);
    conflicting_branch(&writer, 2).await;

    let storage = || {
        fluree_db_core::FileStorage::new(dir.path())
            .with_durability(fluree_db_core::Durability::Sync)
            .without_wal_replay()
    };
    // Building the branch's store for the index looks up its source branch.
    let lookup = Arc::new(PausedLookup {
        inner: Arc::new(fluree_db_nameservice::file::FileNameService::with_storage(
            storage(),
        )),
        ledger_id: "rb:main",
        armed: false.into(),
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
    });
    let peer = FlureeBuilder::memory().build_with(
        storage(),
        fluree_db_api::NameServiceMode::ReadOnly(Arc::clone(&lookup) as _),
    );
    assert_eq!(
        query_cached_names(&peer, "rb:dev").await,
        ["alice", "alice-dev0", "alice-dev1"]
    );

    support::rebuild_and_publish_index(&writer, "rb:dev").await;
    let indexed = dev_record(&writer).await;
    lookup
        .armed
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let index_update = {
        let peer = peer.clone();
        tokio::spawn(async move {
            if via_refresh {
                peer.refresh("rb:dev", Default::default()).await.unwrap();
            } else {
                follow(&peer, indexed).await;
            }
        })
    };
    lookup.entered.notified().await;

    writer
        .rebase_branch("rb", "dev", ConflictStrategy::Skip)
        .await
        .unwrap();
    let rebased = dev_record(&writer).await;
    assert_eq!(
        follow(&peer, rebased).await,
        fluree_db_api::NotifyResult::Reloaded
    );
    lookup.release.notify_one();
    index_update.await.unwrap();

    assert_eq!(
        query_cached_names(&peer, "rb:dev").await,
        ["alice", "alice-main", "dave"]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rebase_outlasts_an_index_install_follow_started() {
    a_rebase_outlasts_an_index_install_in_flight(false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rebase_outlasts_an_index_install_refresh_started() {
    a_rebase_outlasts_an_index_install_in_flight(true).await;
}

/// On the writer, an index install planned on the branch's old line, which
/// arrives after the writer's own rebase, is not made: the rebase starts a
/// new line, so the cache stays on the rebased commits.
#[tokio::test(flavor = "multi_thread")]
async fn the_writers_rebase_outlasts_an_index_install_planned_before_it() {
    let dir = tempfile::tempdir().unwrap();
    let writer = file_fluree(&dir);
    conflicting_branch(&writer, 2).await;
    assert_eq!(
        query_cached_names(&writer, "rb:dev").await,
        ["alice", "alice-dev0", "alice-dev1"]
    );

    let handle = writer.ledger_cached("rb:dev").await.unwrap();
    let planned_on = handle.line();
    support::rebuild_and_publish_index(&writer, "rb:dev").await;
    let old_index = dev_record(&writer).await.index_head_id.unwrap();
    let store = writer.branched_content_store("rb:dev").await.unwrap();

    writer
        .rebase_branch("rb", "dev", ConflictStrategy::Skip)
        .await
        .unwrap();
    let cache_dir = tempfile::tempdir().unwrap();
    let installed = handle
        .apply_index_v2(&old_index, store, cache_dir.path(), None, planned_on)
        .await
        .unwrap();

    assert!(
        !installed,
        "the old line's index landed on the rebased state"
    );
    assert_eq!(
        query_cached_names(&writer, "rb:dev").await,
        ["alice", "alice-main", "dave"]
    );
}

/// A refresh that read the branch's record before a rebase, and goes on
/// after the rebase was followed, does not install that record's index of
/// the old line.
#[tokio::test(flavor = "multi_thread")]
async fn a_rebase_outlasts_a_refresh_that_read_the_record_before_it() {
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();
    let writer = file_fluree(&dir);
    conflicting_branch(&writer, 2).await;

    let storage = || {
        fluree_db_core::FileStorage::new(dir.path())
            .with_durability(fluree_db_core::Durability::Sync)
            .without_wal_replay()
    };
    let lookup = Arc::new(PausedLookup {
        inner: Arc::new(fluree_db_nameservice::file::FileNameService::with_storage(
            storage(),
        )),
        ledger_id: "rb:dev",
        armed: false.into(),
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
    });
    let peer = FlureeBuilder::memory().build_with(
        storage(),
        fluree_db_api::NameServiceMode::ReadOnly(Arc::clone(&lookup) as _),
    );
    assert_eq!(
        query_cached_names(&peer, "rb:dev").await,
        ["alice", "alice-dev0", "alice-dev1"]
    );

    support::rebuild_and_publish_index(&writer, "rb:dev").await;
    lookup
        .armed
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let refresh = {
        let peer = peer.clone();
        tokio::spawn(async move { peer.refresh("rb:dev", Default::default()).await })
    };
    lookup.entered.notified().await;

    writer
        .rebase_branch("rb", "dev", ConflictStrategy::Skip)
        .await
        .unwrap();
    let rebased = dev_record(&writer).await;
    assert_eq!(
        follow(&peer, rebased).await,
        fluree_db_api::NotifyResult::Reloaded
    );
    lookup.release.notify_one();
    refresh.await.unwrap().unwrap();

    assert_eq!(
        query_cached_names(&peer, "rb:dev").await,
        ["alice", "alice-main", "dave"]
    );
}

/// A rebase published while a peer loads the branch, after the load read the
/// nameservice: the peer follows the record once the load lands, rather than
/// serving the replaced line until the next event.
#[tokio::test(flavor = "multi_thread")]
async fn a_peer_follows_a_rebase_sent_while_it_loads_the_branch() {
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();
    let writer = file_fluree(&dir);
    conflicting_branch(&writer, 2).await;

    let storage = || {
        fluree_db_core::FileStorage::new(dir.path())
            .with_durability(fluree_db_core::Durability::Sync)
            .without_wal_replay()
    };
    let lookup = Arc::new(PausedLookup {
        inner: Arc::new(fluree_db_nameservice::file::FileNameService::with_storage(
            storage(),
        )),
        ledger_id: "rb:dev",
        armed: true.into(),
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
    });
    let peer = FlureeBuilder::memory().build_with(
        storage(),
        fluree_db_api::NameServiceMode::ReadOnly(Arc::clone(&lookup) as _),
    );

    let loading = {
        let peer = peer.clone();
        tokio::spawn(async move { peer.ledger_cached("rb:dev").await.map(drop) })
    };
    lookup.entered.notified().await;
    writer
        .rebase_branch("rb", "dev", ConflictStrategy::Skip)
        .await
        .unwrap();
    let rebased = dev_record(&writer).await;
    let following = {
        let peer = peer.clone();
        tokio::spawn(async move { follow(&peer, rebased).await })
    };
    lookup.release.notify_one();
    loading.await.unwrap().unwrap();

    assert_eq!(
        following.await.unwrap(),
        fluree_db_api::NotifyResult::Reloaded
    );
    assert_eq!(
        query_cached_names(&peer, "rb:dev").await,
        ["alice", "alice-main", "dave"]
    );
}
