//! Integration tests for `Fluree::merge_preview` — the read-only branch
//! diff. Mirrors the structure of `it_merge.rs`.

mod support;

use fluree_db_api::{FlureeBuilder, MergePreviewOpts};
use serde_json::json;

// =============================================================================
// 1. Fast-forward
// =============================================================================

#[tokio::test]
async fn preview_fast_forward() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();

    let base = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
    });
    fluree.insert(ledger, &base).await.unwrap();

    fluree.create_branch("mydb", "dev", None).await.unwrap();

    let dev_ledger = fluree.ledger("mydb:dev").await.unwrap();
    let dev_data = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:bob", "ex:name": "Bob"}]
    });
    fluree.insert(dev_ledger, &dev_data).await.unwrap();

    let preview = fluree.merge_preview("mydb", "dev", None).await.unwrap();

    assert_eq!(preview.source, "dev");
    assert_eq!(preview.target, "main");
    assert!(preview.fast_forward, "expected fast-forward");
    assert!(preview.ahead.count > 0, "expected commits ahead");
    assert_eq!(preview.behind.count, 0, "expected nothing behind");
    assert!(!preview.ahead.commits.is_empty());
    assert_eq!(preview.conflicts.count, 0);
    assert!(preview.ancestor.is_some());
}

// =============================================================================
// 2. Diverged, no conflicts
// =============================================================================

#[tokio::test]
async fn preview_diverged_no_conflicts() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();

    let base = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
    });
    let main_ledger = fluree.insert(ledger, &base).await.unwrap().ledger;

    fluree.create_branch("mydb", "dev", None).await.unwrap();

    // Disjoint subjects on each side.
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

    let preview = fluree.merge_preview("mydb", "dev", None).await.unwrap();

    assert!(!preview.fast_forward);
    assert!(preview.ahead.count > 0);
    assert!(preview.behind.count > 0);
    assert_eq!(preview.conflicts.count, 0);
    assert!(preview.conflicts.keys.is_empty());
}

// =============================================================================
// 3. Diverged with conflicts
// =============================================================================

#[tokio::test]
async fn preview_diverged_with_conflicts() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();

    let base = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
    });
    let main_ledger = fluree.insert(ledger, &base).await.unwrap().ledger;

    fluree.create_branch("mydb", "dev", None).await.unwrap();

    // Both branches modify ex:alice / ex:name.
    let dev_ledger = fluree.ledger("mydb:dev").await.unwrap();
    fluree
        .insert(
            dev_ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [{"@id": "ex:alice", "ex:name": "Alice-dev"}]
            }),
        )
        .await
        .unwrap();

    fluree
        .insert(
            main_ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [{"@id": "ex:alice", "ex:name": "Alice-main"}]
            }),
        )
        .await
        .unwrap();

    let preview = fluree.merge_preview("mydb", "dev", None).await.unwrap();

    assert!(!preview.fast_forward);
    assert!(
        preview.conflicts.count > 0,
        "expected conflicts on ex:alice/ex:name, got {:?}",
        preview.conflicts
    );
    assert!(!preview.conflicts.keys.is_empty());
}

// =============================================================================
// 4. Equal heads (no-op)
// =============================================================================

#[tokio::test]
async fn preview_equal_heads_is_fast_forward_with_empty_deltas() {
    // dev branched from main, but neither side advances.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();

    let base = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
    });
    fluree.insert(ledger, &base).await.unwrap();

    fluree.create_branch("mydb", "dev", None).await.unwrap();

    let preview = fluree.merge_preview("mydb", "dev", None).await.unwrap();

    assert!(preview.fast_forward);
    assert_eq!(preview.ahead.count, 0);
    assert_eq!(preview.behind.count, 0);
    assert_eq!(preview.conflicts.count, 0);
    assert!(preview.ancestor.is_some());
}

// =============================================================================
// 5. Behind only — target advanced, source did not
// =============================================================================

#[tokio::test]
async fn preview_behind_only() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();

    let base = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
    });
    let main_ledger = fluree.insert(ledger, &base).await.unwrap().ledger;

    fluree.create_branch("mydb", "dev", None).await.unwrap();

    // Target advances, source does not.
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

    let preview = fluree.merge_preview("mydb", "dev", None).await.unwrap();

    assert_eq!(preview.ahead.count, 0);
    assert!(preview.behind.count > 0);
    assert!(!preview.fast_forward);
    assert_eq!(preview.conflicts.count, 0);
}

// =============================================================================
// 6. Default target resolves to source's parent
// =============================================================================

#[tokio::test]
async fn preview_default_target_uses_source_parent() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();

    let base = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
    });
    fluree.insert(ledger, &base).await.unwrap();

    fluree.create_branch("mydb", "dev", None).await.unwrap();

    let preview = fluree.merge_preview("mydb", "dev", None).await.unwrap();
    assert_eq!(preview.target, "main");
}

// =============================================================================
// 7. Self-merge rejected
// =============================================================================

#[tokio::test]
async fn preview_self_merge_rejected() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();

    let base = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
    });
    fluree.insert(ledger, &base).await.unwrap();

    fluree.create_branch("mydb", "dev", None).await.unwrap();

    let err = fluree
        .merge_preview("mydb", "dev", Some("dev"))
        .await
        .expect_err("self-merge preview should fail");
    assert!(
        err.to_string().contains("itself"),
        "expected error about merging into itself, got: {err}"
    );
}

// =============================================================================
// 8. Truncation — max_commits caps the list but not the count
// =============================================================================

#[tokio::test]
async fn preview_truncation_caps_commits_list() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();

    let base = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
    });
    fluree.insert(ledger, &base).await.unwrap();

    fluree.create_branch("mydb", "dev", None).await.unwrap();

    // Five commits on dev.
    let mut dev_ledger = fluree.ledger("mydb:dev").await.unwrap();
    for (i, name) in ["B", "C", "D", "E", "F"].iter().enumerate() {
        let data = json!({
            "@context": {"ex": "http://example.org/ns/"},
            "@graph": [{"@id": format!("ex:p{i}"), "ex:name": *name}]
        });
        dev_ledger = fluree.insert(dev_ledger, &data).await.unwrap().ledger;
    }

    let preview = fluree
        .merge_preview_with(
            "mydb",
            "dev",
            None,
            MergePreviewOpts {
                max_commits: Some(2),
                ..MergePreviewOpts::default()
            },
        )
        .await
        .unwrap();

    assert_eq!(preview.ahead.count, 5, "5 commits diverged on dev");
    assert_eq!(preview.ahead.commits.len(), 2, "list capped at 2");
    assert!(preview.ahead.truncated);
    // Strictly t-descending.
    for pair in preview.ahead.commits.windows(2) {
        assert!(pair[0].t > pair[1].t);
    }
}

// =============================================================================
// 9. include_conflicts = false short-circuits the delta walks
// =============================================================================

#[tokio::test]
async fn preview_include_conflicts_false_returns_empty_conflicts() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();

    let base = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
    });
    let main_ledger = fluree.insert(ledger, &base).await.unwrap().ledger;

    fluree.create_branch("mydb", "dev", None).await.unwrap();

    // Real conflict on ex:alice/ex:name.
    let dev_ledger = fluree.ledger("mydb:dev").await.unwrap();
    fluree
        .insert(
            dev_ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [{"@id": "ex:alice", "ex:name": "Alice-dev"}]
            }),
        )
        .await
        .unwrap();
    fluree
        .insert(
            main_ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [{"@id": "ex:alice", "ex:name": "Alice-main"}]
            }),
        )
        .await
        .unwrap();

    let preview = fluree
        .merge_preview_with(
            "mydb",
            "dev",
            None,
            MergePreviewOpts {
                include_conflicts: false,
                ..MergePreviewOpts::default()
            },
        )
        .await
        .unwrap();

    assert!(!preview.fast_forward);
    assert_eq!(preview.conflicts.count, 0);
    assert!(preview.conflicts.keys.is_empty());
    assert!(!preview.conflicts.truncated);
}

// =============================================================================
// 10. Read-only invariant — no nameservice mutations
// =============================================================================

#[tokio::test]
async fn preview_does_not_mutate_nameservice() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();

    let base = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
    });
    let main_ledger = fluree.insert(ledger, &base).await.unwrap().ledger;

    fluree.create_branch("mydb", "dev", None).await.unwrap();

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

    let _preview = fluree.merge_preview("mydb", "dev", None).await.unwrap();

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
}

// =============================================================================
// 11. Source has no source_branch — same error as merge_branch
// =============================================================================

#[tokio::test]
async fn preview_main_as_source_refused() {
    let fluree = FlureeBuilder::memory().build_memory();
    fluree.create_ledger("mydb").await.unwrap();

    let err = fluree
        .merge_preview("mydb", "main", None)
        .await
        .expect_err("preview of main as source should fail (no source_branch)");
    assert!(
        err.to_string().contains("no source branch"),
        "expected error about missing source branch, got: {err}"
    );
}

// =============================================================================
// 12. Nonexistent source
// =============================================================================

#[tokio::test]
async fn preview_nonexistent_source_fails() {
    let fluree = FlureeBuilder::memory().build_memory();
    fluree.create_ledger("mydb").await.unwrap();

    let err = fluree
        .merge_preview("mydb", "nonexistent", None)
        .await
        .expect_err("preview of nonexistent branch should fail");
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("not found") || msg.contains("nonexistent"),
        "expected not-found error, got: {err}"
    );
}

// =============================================================================
// 13. Sibling branches — explicit target across branch namespaces
//
// Regression test for the cross-branch ancestor lookup bug. Source `dev` and
// target `feature` are siblings off `main`, both have advanced. The ancestor
// walk must read commits from both branches' namespaces.
// =============================================================================

#[tokio::test]
async fn preview_between_sibling_branches() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();

    let base = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
    });
    fluree.insert(ledger, &base).await.unwrap();

    fluree.create_branch("mydb", "dev", None).await.unwrap();
    fluree.create_branch("mydb", "feature", None).await.unwrap();

    // Advance dev (source).
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

    // Advance feature (target).
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

    let preview = fluree
        .merge_preview("mydb", "dev", Some("feature"))
        .await
        .unwrap();

    assert_eq!(preview.source, "dev");
    assert_eq!(preview.target, "feature");
    assert!(
        preview.ancestor.is_some(),
        "ancestor must resolve across sibling branches"
    );
    assert!(preview.ahead.count >= 1, "dev has 1 commit");
    assert!(preview.behind.count >= 1, "feature has 1 commit");
    assert!(!preview.fast_forward);
}

// =============================================================================
// 14. Unbounded — opts.max_commits = None returns the full divergence
// =============================================================================

#[tokio::test]
async fn preview_max_commits_none_is_unbounded() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();

    let base = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
    });
    fluree.insert(ledger, &base).await.unwrap();

    fluree.create_branch("mydb", "dev", None).await.unwrap();

    // 5 commits on dev.
    let mut dev_ledger = fluree.ledger("mydb:dev").await.unwrap();
    for (i, name) in ["B", "C", "D", "E", "F"].iter().enumerate() {
        let data = json!({
            "@context": {"ex": "http://example.org/ns/"},
            "@graph": [{"@id": format!("ex:p{i}"), "ex:name": *name}]
        });
        dev_ledger = fluree.insert(dev_ledger, &data).await.unwrap().ledger;
    }

    let preview = fluree
        .merge_preview_with(
            "mydb",
            "dev",
            None,
            MergePreviewOpts {
                max_commits: None,
                max_conflict_keys: None,
                include_conflicts: true,
            },
        )
        .await
        .unwrap();

    assert_eq!(preview.ahead.count, 5);
    assert_eq!(
        preview.ahead.commits.len(),
        5,
        "None should return the full list, not the default cap"
    );
    assert!(!preview.ahead.truncated);
}

// =============================================================================
// 15. Default opts cap commit lists at 500 (and conflict keys at 200)
// =============================================================================

#[tokio::test]
async fn preview_default_opts_carry_caps() {
    let opts = MergePreviewOpts::default();
    assert_eq!(opts.max_commits, Some(500));
    assert_eq!(opts.max_conflict_keys, Some(200));
    assert!(opts.include_conflicts);
}

// =============================================================================
// 16. Conflict keys are sorted (stable across builds)
// =============================================================================

#[tokio::test]
async fn preview_conflict_keys_are_sorted() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();

    // Seed several subjects so we can produce multiple conflicts.
    let base = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [
            {"@id": "ex:alice", "ex:name": "Alice"},
            {"@id": "ex:bob",   "ex:name": "Bob"},
            {"@id": "ex:carol", "ex:name": "Carol"},
        ]
    });
    let main_ledger = fluree.insert(ledger, &base).await.unwrap().ledger;

    fluree.create_branch("mydb", "dev", None).await.unwrap();

    // Modify the same predicate on each subject from both branches.
    let dev_ledger = fluree.ledger("mydb:dev").await.unwrap();
    fluree
        .insert(
            dev_ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [
                    {"@id": "ex:alice", "ex:name": "A-dev"},
                    {"@id": "ex:bob",   "ex:name": "B-dev"},
                    {"@id": "ex:carol", "ex:name": "C-dev"},
                ]
            }),
        )
        .await
        .unwrap();
    fluree
        .insert(
            main_ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [
                    {"@id": "ex:alice", "ex:name": "A-main"},
                    {"@id": "ex:bob",   "ex:name": "B-main"},
                    {"@id": "ex:carol", "ex:name": "C-main"},
                ]
            }),
        )
        .await
        .unwrap();

    let preview = fluree.merge_preview("mydb", "dev", None).await.unwrap();

    assert!(preview.conflicts.count >= 3);
    let keys = &preview.conflicts.keys;
    for pair in keys.windows(2) {
        assert!(pair[0] <= pair[1], "conflict keys must be sorted");
    }
}
