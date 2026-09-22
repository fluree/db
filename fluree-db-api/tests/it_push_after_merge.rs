//! Pushing and pulling a branch whose history contains a general merge.
//!
//! The receiver replays the branch's first-parent line. The commits that the
//! line's merges brought in travel beside it and are stored without being
//! replayed.

use crate::support;
use fluree_db_api::{
    Base64Bytes, ConflictStrategy, ExportCommitsRequest, FlureeBuilder, GovernanceOptions,
    IndexConfig, PushCommitsRequest,
};
use fluree_db_core::{collect_dag_cids, plan_commit_transfer, ContentId, ContentStore};
use serde_json::json;
use std::collections::HashMap;

fn index_config() -> IndexConfig {
    IndexConfig {
        reindex_min_bytes: 100_000,
        reindex_max_bytes: 1_000_000_000,
    }
}

/// Insert one named subject into `ledger_id`.
async fn insert_name(fluree: &support::MemoryFluree, ledger_id: &str, id: &str, name: &str) {
    let ledger = fluree.ledger(ledger_id).await.unwrap();
    let data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": format!("ex:{id}"), "ex:name": name}]
    });
    fluree.insert(ledger, &data).await.unwrap();
}

/// Query all `ex:name` values on a branch, sorted.
async fn names(fluree: &support::MemoryFluree, ledger_id: &str) -> Vec<String> {
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
    let mut names: Vec<String> = rows
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            let value = row.as_array().map_or(row, |cells| &cells[0]);
            value.as_str().expect("name is a string").to_string()
        })
        .collect();
    names.sort();
    names
}

/// Read each commit's bytes, collecting the txn blobs they reference.
async fn read_commits(
    store: &dyn ContentStore,
    cids: &[ContentId],
    blobs: &mut HashMap<String, Base64Bytes>,
) -> Vec<Base64Bytes> {
    let mut out = Vec::with_capacity(cids.len());
    for cid in cids {
        let bytes = store.get(cid).await.unwrap();
        let commit = fluree_db_core::commit::codec::read_commit(&bytes).unwrap();
        if let Some(txn_cid) = &commit.txn {
            let txn_bytes = store.get(txn_cid).await.unwrap();
            blobs.insert(txn_cid.to_string(), Base64Bytes(txn_bytes));
        }
        out.push(Base64Bytes(bytes));
    }
    out
}

/// Build the push bundle a sender would produce for a ledger with no
/// counterpart on the receiver.
async fn build_bundle(fluree: &support::MemoryFluree, ledger_id: &str) -> PushCommitsRequest {
    build_bundle_from(fluree, ledger_id, None).await
}

/// Build the push bundle a sender would produce for a receiver at `base`.
async fn build_bundle_from(
    fluree: &support::MemoryFluree,
    ledger_id: &str,
    base: Option<&ContentId>,
) -> PushCommitsRequest {
    let store = fluree.branched_content_store(ledger_id).await.unwrap();
    let head = fluree
        .ledger(ledger_id)
        .await
        .unwrap()
        .head_commit_id
        .clone()
        .expect("ledger has a head");
    let plan = plan_commit_transfer(store.as_ref(), &head, base)
        .await
        .unwrap()
        .expect("base is on the head's line");

    let mut blobs = HashMap::new();
    let commits = read_commits(store.as_ref(), &plan.lineage, &mut blobs).await;
    let merged_commits = read_commits(store.as_ref(), &plan.merged, &mut blobs).await;

    PushCommitsRequest {
        commits,
        blobs,
        missing_blobs: Vec::new(),
        merged_commits,
    }
}

/// Build a ledger whose main branch ends in a general merge of `dev`.
async fn merged_history(fluree: &support::MemoryFluree) {
    fluree.create_ledger("mydb").await.unwrap();
    insert_name(fluree, "mydb:main", "alice", "Alice").await;

    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();
    insert_name(fluree, "mydb:dev", "bob", "Bob").await;

    // Main advances too, so the merge cannot fast-forward.
    insert_name(fluree, "mydb:main", "carol", "Carol").await;

    let report = fluree
        .merge_branch("mydb", "dev", None, ConflictStrategy::default())
        .await
        .unwrap();
    assert!(!report.fast_forward, "merge should not fast-forward");
}

#[tokio::test]
async fn push_carries_the_commits_a_merge_brought_in() {
    let fluree = FlureeBuilder::memory().build_memory();
    merged_history(&fluree).await;

    let bundle = build_bundle(&fluree, "mydb:main").await;
    assert_eq!(bundle.commits.len(), 3, "alice, carol, and the merge");
    assert_eq!(bundle.merged_commits.len(), 1, "dev's commit");

    let target = "it/push-merge-tgt:main";
    fluree.create_ledger(target).await.unwrap();
    let response = fluree
        .push_commits(
            target,
            bundle,
            &GovernanceOptions::default(),
            &index_config(),
        )
        .await
        .expect("push should be accepted");

    assert_eq!(response.accepted, 3, "only the line is replayed");
    assert_eq!(response.head.t, 3);
    assert_eq!(names(&fluree, target).await, ["Alice", "Bob", "Carol"]);

    // The merged-in commit is stored, so a DAG walk on the target resolves
    // every parent the merge commit names.
    let store = fluree.branched_content_store(target).await.unwrap();
    let head = fluree
        .ledger(target)
        .await
        .unwrap()
        .head_commit_id
        .clone()
        .unwrap();
    let dag = collect_dag_cids(store.as_ref(), &head, 0).await.unwrap();
    assert_eq!(dag.len(), 4, "three on the line plus dev's commit");
}

#[tokio::test]
async fn push_after_a_second_merge_sends_only_the_new_commits() {
    let fluree = FlureeBuilder::memory().build_memory();
    merged_history(&fluree).await;

    let target = "it/push-merge-again:main";
    fluree.create_ledger(target).await.unwrap();
    fluree
        .push_commits(
            target,
            build_bundle(&fluree, "mydb:main").await,
            &GovernanceOptions::default(),
            &index_config(),
        )
        .await
        .expect("the first push should be accepted");
    let base = fluree
        .ledger(target)
        .await
        .unwrap()
        .head_commit_id
        .clone()
        .unwrap();

    // Both branches move on, then dev is merged a second time.
    insert_name(&fluree, "mydb:dev", "dan", "Dan").await;
    insert_name(&fluree, "mydb:main", "erin", "Erin").await;
    let report = fluree
        .merge_branch("mydb", "dev", None, ConflictStrategy::default())
        .await
        .unwrap();
    assert!(!report.fast_forward, "merge should not fast-forward");

    let bundle = build_bundle_from(&fluree, "mydb:main", Some(&base)).await;
    assert_eq!(bundle.commits.len(), 2, "erin and the second merge");
    assert_eq!(
        bundle.merged_commits.len(),
        1,
        "only dev's new commit; the receiver holds the one the first merge brought in"
    );

    let response = fluree
        .push_commits(
            target,
            bundle,
            &GovernanceOptions::default(),
            &index_config(),
        )
        .await
        .expect("the second push should be accepted");

    assert_eq!(response.accepted, 2);
    assert_eq!(
        names(&fluree, target).await,
        ["Alice", "Bob", "Carol", "Dan", "Erin"]
    );
}

#[tokio::test]
async fn push_accepts_a_merge_commit_with_no_flakes() {
    let fluree = FlureeBuilder::memory().build_memory();
    fluree.create_ledger("mydb").await.unwrap();
    insert_name(&fluree, "mydb:main", "alice", "Alice").await;

    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();
    // Both branches rename the same subject, so every source flake conflicts.
    insert_name(&fluree, "mydb:dev", "alice", "Ada").await;
    insert_name(&fluree, "mydb:main", "alice", "Alicia").await;

    let report = fluree
        .merge_branch("mydb", "dev", None, ConflictStrategy::TakeBranch)
        .await
        .unwrap();
    assert!(!report.fast_forward);

    let store = fluree.branched_content_store("mydb:main").await.unwrap();
    let head = fluree
        .ledger("mydb:main")
        .await
        .unwrap()
        .head_commit_id
        .clone()
        .unwrap();
    let merge_commit = fluree_db_core::load_commit_by_id(store.as_ref(), &head)
        .await
        .unwrap();
    assert!(
        merge_commit.flakes.is_empty(),
        "the target won every conflict, so the merge commit carries no flakes"
    );

    let target = "it/push-merge-empty:main";
    fluree.create_ledger(target).await.unwrap();
    let response = fluree
        .push_commits(
            target,
            build_bundle(&fluree, "mydb:main").await,
            &GovernanceOptions::default(),
            &index_config(),
        )
        .await
        .expect("a merge commit with no flakes is still a commit");

    assert_eq!(response.head.t, merge_commit.t);
    // Names accumulate, and the merge added none of dev's.
    assert_eq!(names(&fluree, target).await, ["Alice", "Alicia"]);
}

#[tokio::test]
async fn push_is_rejected_when_a_merged_commit_is_missing() {
    let fluree = FlureeBuilder::memory().build_memory();
    merged_history(&fluree).await;

    let mut bundle = build_bundle(&fluree, "mydb:main").await;
    bundle.merged_commits.clear();

    let target = "it/push-merge-missing:main";
    fluree.create_ledger(target).await.unwrap();
    let error = fluree
        .push_commits(
            target,
            bundle,
            &GovernanceOptions::default(),
            &index_config(),
        )
        .await
        .expect_err("push should be rejected");

    let message = error.to_string();
    assert!(
        message.contains("neither in the push nor in storage"),
        "unexpected error: {message}"
    );
}

#[tokio::test]
async fn push_is_rejected_when_a_merged_commit_is_unreachable() {
    let fluree = FlureeBuilder::memory().build_memory();
    merged_history(&fluree).await;

    // A commit from an unrelated ledger. It has no parents, so it passes
    // the parent check, but nothing in the push merges it.
    fluree.create_ledger("other").await.unwrap();
    insert_name(&fluree, "other:main", "mallory", "Mallory").await;
    let stray = build_bundle(&fluree, "other:main").await.commits.remove(0);

    let mut bundle = build_bundle(&fluree, "mydb:main").await;
    bundle.merged_commits.push(stray);

    let target = "it/push-merge-stray:main";
    fluree.create_ledger(target).await.unwrap();
    let error = fluree
        .push_commits(
            target,
            bundle,
            &GovernanceOptions::default(),
            &index_config(),
        )
        .await
        .expect_err("push should be rejected");

    let message = error.to_string();
    assert!(
        message.contains("not reachable from any merge"),
        "unexpected error: {message}"
    );
}

/// A target ledger holding main's first two commits, the head a puller has
/// before the merge. Returns that head.
async fn target_before_merge(fluree: &support::MemoryFluree, target: &str) -> ContentId {
    let mut bundle = build_bundle(fluree, "mydb:main").await;
    bundle.commits.truncate(2);
    bundle.merged_commits.clear();
    fluree.create_ledger(target).await.unwrap();
    fluree
        .push_commits(
            target,
            bundle,
            &GovernanceOptions::default(),
            &index_config(),
        )
        .await
        .unwrap()
        .head
        .commit_id
}

async fn export_lineage(
    fluree: &support::MemoryFluree,
    cursor_id: Option<ContentId>,
    base_id: Option<ContentId>,
    limit: usize,
) -> fluree_db_api::Result<fluree_db_api::ExportCommitsResponse> {
    let handle = fluree.ledger_cached("mydb:main").await.unwrap();
    fluree
        .export_commit_range(
            &handle,
            &ExportCommitsRequest {
                cursor_id,
                base_id,
                limit: Some(limit),
                lineage: true,
                ..Default::default()
            },
        )
        .await
}

#[tokio::test]
async fn lineage_export_pulls_a_merge() {
    let fluree = FlureeBuilder::memory().build_memory();
    merged_history(&fluree).await;
    let target = "it/pull-merge-tgt:main";
    let base = target_before_merge(&fluree, target).await;

    let page = export_lineage(&fluree, None, Some(base), 100)
        .await
        .unwrap();
    assert!(page.lineage, "the export echoes the mode");
    assert_eq!(page.commits.len(), 1, "only the merge is above the base");
    assert_eq!(page.merged_commits.len(), 1, "dev's commit");
    assert_eq!(page.next_cursor_id, None);

    let result = fluree
        .import_commits_incremental(target, page.commits, page.merged_commits, page.blobs)
        .await
        .expect("import should be accepted");
    assert_eq!(result.head_t, 3);
    assert_eq!(names(&fluree, target).await, ["Alice", "Bob", "Carol"]);
}

#[tokio::test]
async fn lineage_export_pages_the_line() {
    let fluree = FlureeBuilder::memory().build_memory();
    merged_history(&fluree).await;

    let mut lines = 0;
    let mut merged = 0;
    let mut cursor = None;
    loop {
        let page = export_lineage(&fluree, cursor, None, 1).await.unwrap();
        assert_eq!(page.count, 1);
        lines += page.commits.len();
        merged += page.merged_commits.len();
        match page.next_cursor_id {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    assert_eq!(lines, 3, "alice, carol, and the merge, down to genesis");
    assert_eq!(merged, 1, "dev's commit, on the merge's page");
}

#[tokio::test]
async fn lineage_export_rejects_a_base_off_the_line() {
    let fluree = FlureeBuilder::memory().build_memory();
    merged_history(&fluree).await;

    // Dev's head is a merge parent of main's head, not on main's line.
    let dev_head = fluree
        .ledger("mydb:dev")
        .await
        .unwrap()
        .head_commit_id
        .clone()
        .unwrap();
    let error = export_lineage(&fluree, None, Some(dev_head), 100)
        .await
        .expect_err("a diverged base should be refused");
    assert!(error.to_string().contains("diverged"), "{error}");
}
