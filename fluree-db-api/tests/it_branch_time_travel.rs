//! Time-travel reads across branch operations (merge, rebase, revert).
//!
//! A flake's `t` is a per-branch logical clock. Merge, rebase, and revert
//! transplant flakes from another timeline (or synthesize retractions), and
//! every flake landing in the new commit must carry that commit's `t` on the
//! *target's* clock. These tests pin the `--at N` view of a branch below the
//! point where a branch operation landed — HEAD reads resolve by apply order
//! and stay correct even when the stamps are wrong, so only `@t:` reads
//! expose the skew.
//!
//! Regression coverage for #1783.

use crate::support;
use fluree_db_api::{CommitRef, ConflictStrategy, FlureeBuilder};
use serde_json::json;

const EX: &str = "http://example.org/ns/";

fn ctx() -> serde_json::Value {
    json!({"ex": EX})
}

fn insert_name(id: &str, name: &str) -> serde_json::Value {
    json!({
        "@context": ctx(),
        "@graph": [{"@id": id, "ex:name": name}]
    })
}

/// Replace-style update: `delete ?old, insert new` on `ex:alice`'s name.
fn replace_alice(name: &str) -> serde_json::Value {
    json!({
        "@context": ctx(),
        "where": {"@id": "ex:alice", "ex:name": "?old"},
        "delete": {"@id": "ex:alice", "ex:name": "?old"},
        "insert": {"@id": "ex:alice", "ex:name": name}
    })
}

/// `ex:alice`'s name values on `ledger_id` as of `t` (sorted), read through
/// the connection-level `from: "<ledger>@t:<t>"` time-travel path.
async fn alice_at(fluree: &fluree_db_api::Fluree, ledger_id: &str, t: i64) -> Vec<String> {
    let head = fluree.ledger(ledger_id).await.unwrap();
    let q = json!({
        "@context": ctx(),
        "from": [format!("{ledger_id}@t:{t}")],
        "select": ["?name"],
        "where": [{"@id": "ex:alice", "ex:name": "?name"}]
    });
    let result = fluree.query_connection(&q).await.expect("query_connection");
    let jsonld = result
        .to_jsonld_async(head.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async");
    let mut names: Vec<String> = support::normalize_rows(&jsonld)
        .iter()
        .map(|row| {
            row.as_array()
                .and_then(|a| a.first())
                .and_then(|v| v.as_str())
                .map(ToString::to_string)
                .expect("row should be [name]")
        })
        .collect();
    names.sort();
    names
}

/// `ex:alice`'s name values at HEAD of `ledger_id` (sorted), read through the
/// plain (non-time-travel) query path against the loaded ledger state.
async fn alice_head(fluree: &fluree_db_api::Fluree, ledger_id: &str) -> Vec<String> {
    let head = fluree.ledger(ledger_id).await.unwrap();
    let q = json!({
        "@context": ctx(),
        "select": ["?name"],
        "where": [{"@id": "ex:alice", "ex:name": "?name"}]
    });
    let result = support::query_jsonld(fluree, &head, &q).await.unwrap();
    let rows = result.to_jsonld(&head.snapshot).unwrap();
    let mut names: Vec<String> = support::normalize_rows(&rows)
        .iter()
        .map(|row| {
            row.as_array()
                .and_then(|a| a.first())
                .and_then(|v| v.as_str())
                .map(ToString::to_string)
                .expect("row should be [name]")
        })
        .collect();
    names.sort();
    names
}

// =============================================================================
// Merge
// =============================================================================

/// A conflict-free merge must not change what the target held *before* the
/// merge commit. Setup: main t1 Alice; dev replaces Alice → Alice-dev (dev
/// t2); main t2, t3 touch unrelated subjects; merge lands at main t4.
#[tokio::test]
async fn merge_does_not_rewrite_target_history_below_merge_point() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();
    let main = fluree
        .insert(ledger, &insert_name("ex:alice", "Alice"))
        .await
        .unwrap()
        .ledger;

    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();
    let dev = fluree.ledger("mydb:dev").await.unwrap();
    fluree
        .update(dev, &replace_alice("Alice-dev"))
        .await
        .unwrap();

    let main = fluree
        .insert(main, &insert_name("ex:bob", "Bob"))
        .await
        .unwrap()
        .ledger;
    let main = fluree
        .insert(main, &insert_name("ex:carol", "Carol"))
        .await
        .unwrap()
        .ledger;
    assert_eq!(main.t(), 3);

    assert_eq!(alice_at(&fluree, "mydb:main", 2).await, vec!["Alice"]);
    assert_eq!(alice_at(&fluree, "mydb:main", 3).await, vec!["Alice"]);

    let report = fluree
        .merge_branch("mydb", "dev", None, ConflictStrategy::default())
        .await
        .unwrap();
    assert!(!report.fast_forward);
    assert_eq!(report.conflict_count, 0);

    assert_eq!(alice_head(&fluree, "mydb:main").await, vec!["Alice-dev"]);
    assert_eq!(alice_at(&fluree, "mydb:main", 4).await, vec!["Alice-dev"]);
    // Below the merge point main's own history is unchanged.
    assert_eq!(alice_at(&fluree, "mydb:main", 1).await, vec!["Alice"]);
    assert_eq!(alice_at(&fluree, "mydb:main", 2).await, vec!["Alice"]);
    assert_eq!(alice_at(&fluree, "mydb:main", 3).await, vec!["Alice"]);
}

/// Take-both on a conflicting key: the branch's flakes must not surface at
/// their branch-local `t` alongside the target's own values. Setup: main
/// A → C (t2) → A (t3); dev A → B (dev t2); merge at main t4.
#[tokio::test]
async fn merge_does_not_create_phantom_duplicates_at_historical_t() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();
    let main = fluree
        .insert(ledger, &insert_name("ex:alice", "A"))
        .await
        .unwrap()
        .ledger;

    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();
    let dev = fluree.ledger("mydb:dev").await.unwrap();
    fluree.update(dev, &replace_alice("B")).await.unwrap();

    let main = fluree
        .update(main, &replace_alice("C"))
        .await
        .unwrap()
        .ledger;
    let main = fluree
        .update(main, &replace_alice("A"))
        .await
        .unwrap()
        .ledger;
    assert_eq!(main.t(), 3);

    let report = fluree
        .merge_branch("mydb", "dev", None, ConflictStrategy::TakeBoth)
        .await
        .unwrap();
    assert!(!report.fast_forward);
    assert_eq!(report.conflict_count, 1);

    assert_eq!(alice_head(&fluree, "mydb:main").await, vec!["B"]);
    assert_eq!(alice_at(&fluree, "mydb:main", 1).await, vec!["A"]);
    assert_eq!(alice_at(&fluree, "mydb:main", 2).await, vec!["C"]);
    assert_eq!(alice_at(&fluree, "mydb:main", 3).await, vec!["A"]);
    assert_eq!(alice_at(&fluree, "mydb:main", 4).await, vec!["B"]);
}

/// Take-source synthesizes retractions of the target's current values. Those
/// retractions must be stamped at the merge commit's `t`, not `0`, or they
/// cancel the target's value at every historical `t`. Setup: main A → C
/// (t2); dev A → B (dev t2); take-source merge at main t3.
#[tokio::test]
async fn merge_take_source_retractions_carry_merge_t() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();
    let main = fluree
        .insert(ledger, &insert_name("ex:alice", "A"))
        .await
        .unwrap()
        .ledger;

    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();
    let dev = fluree.ledger("mydb:dev").await.unwrap();
    fluree.update(dev, &replace_alice("B")).await.unwrap();

    let main = fluree
        .update(main, &replace_alice("C"))
        .await
        .unwrap()
        .ledger;
    assert_eq!(main.t(), 2);

    let report = fluree
        .merge_branch("mydb", "dev", None, ConflictStrategy::TakeSource)
        .await
        .unwrap();
    assert_eq!(report.conflict_count, 1);

    assert_eq!(alice_head(&fluree, "mydb:main").await, vec!["B"]);
    assert_eq!(alice_at(&fluree, "mydb:main", 1).await, vec!["A"]);
    assert_eq!(alice_at(&fluree, "mydb:main", 2).await, vec!["C"]);
    assert_eq!(alice_at(&fluree, "mydb:main", 3).await, vec!["B"]);
}

/// Same as the first merge test, but the reads happen on a fresh instance
/// that rebuilds novelty from the commit chain on disk. This pins the
/// load-time half: a merge commit already carries the folded source flakes,
/// so the loader must not *also* descend into the source branch's commits
/// (whose header `t` is on the source's clock).
#[tokio::test]
async fn merge_time_travel_survives_reload_from_commits() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let path = tmp.path().to_str().unwrap().to_string();

    {
        let fluree = FlureeBuilder::file(&path).build().expect("build");
        let ledger = fluree.create_ledger("mydb").await.unwrap();
        let main = fluree
            .insert(ledger, &insert_name("ex:alice", "Alice"))
            .await
            .unwrap()
            .ledger;

        fluree
            .create_branch("mydb", "dev", None, None)
            .await
            .unwrap();
        let dev = fluree.ledger("mydb:dev").await.unwrap();
        fluree
            .update(dev, &replace_alice("Alice-dev"))
            .await
            .unwrap();

        let main = fluree
            .insert(main, &insert_name("ex:bob", "Bob"))
            .await
            .unwrap()
            .ledger;
        fluree
            .insert(main, &insert_name("ex:carol", "Carol"))
            .await
            .unwrap();

        let report = fluree
            .merge_branch("mydb", "dev", None, ConflictStrategy::default())
            .await
            .unwrap();
        assert!(!report.fast_forward);
        assert_eq!(report.conflict_count, 0);
    }

    let fluree = FlureeBuilder::file(&path).build().expect("rebuild");
    assert_eq!(alice_head(&fluree, "mydb:main").await, vec!["Alice-dev"]);
    assert_eq!(alice_at(&fluree, "mydb:main", 4).await, vec!["Alice-dev"]);
    assert_eq!(alice_at(&fluree, "mydb:main", 1).await, vec!["Alice"]);
    assert_eq!(alice_at(&fluree, "mydb:main", 2).await, vec!["Alice"]);
    assert_eq!(alice_at(&fluree, "mydb:main", 3).await, vec!["Alice"]);
}

/// Take-branch drops the source's flakes for conflicting keys, so the merge
/// commit records nothing for them. If the loader descends into the source
/// branch's commits, the dropped flakes come back after a restart and HEAD
/// itself changes. Setup: main A → C (t2); dev A → B (dev t2); take-branch
/// merge at main t3.
#[tokio::test]
async fn merge_take_branch_head_survives_reload_from_commits() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let path = tmp.path().to_str().unwrap().to_string();

    {
        let fluree = FlureeBuilder::file(&path).build().expect("build");
        let ledger = fluree.create_ledger("mydb").await.unwrap();
        let main = fluree
            .insert(ledger, &insert_name("ex:alice", "A"))
            .await
            .unwrap()
            .ledger;

        fluree
            .create_branch("mydb", "dev", None, None)
            .await
            .unwrap();
        let dev = fluree.ledger("mydb:dev").await.unwrap();
        fluree.update(dev, &replace_alice("B")).await.unwrap();

        fluree.update(main, &replace_alice("C")).await.unwrap();

        let report = fluree
            .merge_branch("mydb", "dev", None, ConflictStrategy::TakeBranch)
            .await
            .unwrap();
        assert_eq!(report.conflict_count, 1);
        assert_eq!(alice_head(&fluree, "mydb:main").await, vec!["C"]);
    }

    let fluree = FlureeBuilder::file(&path).build().expect("rebuild");
    assert_eq!(alice_head(&fluree, "mydb:main").await, vec!["C"]);
    assert_eq!(alice_at(&fluree, "mydb:main", 2).await, vec!["C"]);
    assert_eq!(alice_at(&fluree, "mydb:main", 3).await, vec!["C"]);
}

// =============================================================================
// Rebase
// =============================================================================

/// Replayed commits land at new positions on the branch's clock, and their
/// flakes must be restamped to match. Setup: main t1 Alice; dev replaces
/// Alice → Alice-dev (dev t2); main t2 inserts Bob; rebase dev, so its
/// chain becomes t1 Alice, t2 Bob (from main), t3 replayed Alice-dev.
#[tokio::test]
async fn rebase_replayed_flakes_carry_replay_t() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();
    let main = fluree
        .insert(ledger, &insert_name("ex:alice", "Alice"))
        .await
        .unwrap()
        .ledger;

    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();
    let dev = fluree.ledger("mydb:dev").await.unwrap();
    fluree
        .update(dev, &replace_alice("Alice-dev"))
        .await
        .unwrap();
    assert_eq!(alice_at(&fluree, "mydb:dev", 2).await, vec!["Alice-dev"]);

    fluree
        .insert(main, &insert_name("ex:bob", "Bob"))
        .await
        .unwrap();

    let report = fluree
        .rebase_branch("mydb", "dev", ConflictStrategy::default())
        .await
        .unwrap();
    assert!(!report.fast_forward);
    assert_eq!(report.replayed, 1);

    assert_eq!(alice_head(&fluree, "mydb:dev").await, vec!["Alice-dev"]);
    assert_eq!(alice_at(&fluree, "mydb:dev", 1).await, vec!["Alice"]);
    // t2 on the rebased chain is main's Bob commit; Alice is still Alice.
    assert_eq!(alice_at(&fluree, "mydb:dev", 2).await, vec!["Alice"]);
    assert_eq!(alice_at(&fluree, "mydb:dev", 3).await, vec!["Alice-dev"]);
}

// =============================================================================
// Revert
// =============================================================================

/// Revert inverts the reverted commit's flakes. The inversions must be
/// stamped at the revert commit's `t`, not `0`, or the reverted state is
/// erased from every historical `t`. Setup: t1 Alice; t2 Alice → Alice-2;
/// revert t2's commit at t3.
#[tokio::test]
async fn revert_inversions_carry_revert_t() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();
    let r1 = fluree
        .insert(ledger, &insert_name("ex:alice", "Alice"))
        .await
        .unwrap();
    let r2 = fluree
        .update(r1.ledger, &replace_alice("Alice-2"))
        .await
        .unwrap();
    assert_eq!(r2.receipt.t, 2);
    assert_eq!(alice_at(&fluree, "mydb:main", 2).await, vec!["Alice-2"]);

    let report = fluree
        .revert_commit(
            "mydb",
            "main",
            CommitRef::Exact(r2.receipt.commit_id.clone()),
            ConflictStrategy::TakeSource,
        )
        .await
        .unwrap();
    assert_eq!(report.new_head_t, 3);

    assert_eq!(alice_head(&fluree, "mydb:main").await, vec!["Alice"]);
    assert_eq!(alice_at(&fluree, "mydb:main", 1).await, vec!["Alice"]);
    assert_eq!(alice_at(&fluree, "mydb:main", 2).await, vec!["Alice-2"]);
    assert_eq!(alice_at(&fluree, "mydb:main", 3).await, vec!["Alice"]);
}
