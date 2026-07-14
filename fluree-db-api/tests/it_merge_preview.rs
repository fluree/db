//! Integration tests for `Fluree::merge_preview` — the read-only branch
//! diff. Mirrors the structure of `it_merge.rs`.

use crate::support;
use fluree_db_api::{ConflictStrategy, FlureeBuilder, MergePreviewOpts};
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

#[tokio::test]
async fn preview_fast_forward_with_conflict_details_is_empty_and_mergeable() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();

    let base = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
    });
    fluree.insert(ledger, &base).await.unwrap();

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

    let preview = fluree
        .merge_preview_with(
            "mydb",
            "dev",
            None,
            MergePreviewOpts {
                include_conflict_details: true,
                conflict_strategy: ConflictStrategy::Abort,
                ..MergePreviewOpts::default()
            },
        )
        .await
        .unwrap();

    assert!(preview.fast_forward);
    assert_eq!(preview.conflicts.count, 0);
    assert!(preview.conflicts.details.is_empty());
    assert!(preview.mergeable);
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

    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();

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

    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();

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
        .upsert(
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

#[tokio::test]
async fn preview_conflict_details_include_values_and_strategy_labels() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();

    let base = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
    });
    let main_ledger = fluree.insert(ledger, &base).await.unwrap().ledger;

    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();

    let mut dev_ledger = fluree.ledger("mydb:dev").await.unwrap();
    dev_ledger = fluree
        .upsert(
            dev_ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [{"@id": "ex:alice", "ex:name": "Alice-dev-stale"}]
            }),
        )
        .await
        .unwrap()
        .ledger;
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

    let preview = fluree
        .merge_preview_with(
            "mydb",
            "dev",
            None,
            MergePreviewOpts {
                include_conflict_details: true,
                conflict_strategy: ConflictStrategy::TakeSource,
                ..MergePreviewOpts::default()
            },
        )
        .await
        .unwrap();

    assert_eq!(preview.conflicts.strategy.as_deref(), Some("take-source"));
    assert_eq!(
        preview.conflicts.details.len(),
        preview.conflicts.keys.len()
    );

    let detail = preview
        .conflicts
        .details
        .first()
        .expect("conflict details should be returned");
    assert_eq!(detail.resolution.source_action, "kept");
    assert_eq!(detail.resolution.target_action, "retracted");
    assert_eq!(detail.resolution.outcome, "source-wins");
    assert!(preview.mergeable);

    let source_values = serde_json::to_string(&detail.source_values).unwrap();
    let target_values = serde_json::to_string(&detail.target_values).unwrap();
    assert!(source_values.contains("Alice-dev"), "{source_values}");
    assert!(
        !source_values.contains("Alice-dev-stale"),
        "{source_values}"
    );
    assert!(target_values.contains("Alice-main"), "{target_values}");
}

#[tokio::test]
async fn preview_conflict_details_cover_take_branch_and_abort_labels() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();

    let base = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
    });
    let main_ledger = fluree.insert(ledger, &base).await.unwrap().ledger;
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

    let take_branch = fluree
        .merge_preview_with(
            "mydb",
            "dev",
            None,
            MergePreviewOpts {
                include_conflict_details: true,
                conflict_strategy: ConflictStrategy::TakeBranch,
                ..MergePreviewOpts::default()
            },
        )
        .await
        .unwrap();
    let detail = take_branch.conflicts.details.first().unwrap();
    assert_eq!(detail.resolution.source_action, "dropped");
    assert_eq!(detail.resolution.target_action, "kept");
    assert_eq!(detail.resolution.outcome, "target-wins");
    assert!(take_branch.mergeable);

    let abort = fluree
        .merge_preview_with(
            "mydb",
            "dev",
            None,
            MergePreviewOpts {
                include_conflict_details: true,
                conflict_strategy: ConflictStrategy::Abort,
                ..MergePreviewOpts::default()
            },
        )
        .await
        .unwrap();
    let detail = abort.conflicts.details.first().unwrap();
    assert_eq!(detail.resolution.source_action, "unchanged");
    assert_eq!(detail.resolution.target_action, "unchanged");
    assert_eq!(detail.resolution.outcome, "merge-aborts");
    assert!(!abort.mergeable);
}

#[tokio::test]
async fn preview_conflict_details_follow_conflict_key_truncation() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();

    let base = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [
            {"@id": "ex:alice", "ex:name": "Alice"},
            {"@id": "ex:bob", "ex:name": "Bob"}
        ]
    });
    let main_ledger = fluree.insert(ledger, &base).await.unwrap().ledger;
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
                "@graph": [
                    {"@id": "ex:alice", "ex:name": "Alice-dev"},
                    {"@id": "ex:bob", "ex:name": "Bob-dev"}
                ]
            }),
        )
        .await
        .unwrap();
    fluree
        .upsert(
            main_ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [
                    {"@id": "ex:alice", "ex:name": "Alice-main"},
                    {"@id": "ex:bob", "ex:name": "Bob-main"}
                ]
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
                max_conflict_keys: Some(1),
                include_conflict_details: true,
                ..MergePreviewOpts::default()
            },
        )
        .await
        .unwrap();

    assert!(preview.conflicts.truncated);
    assert!(preview.conflicts.count >= 2);
    assert_eq!(preview.conflicts.keys.len(), 1);
    assert_eq!(preview.conflicts.details.len(), 1);
    assert_eq!(preview.conflicts.details[0].key, preview.conflicts.keys[0]);
}

#[tokio::test]
async fn preview_conflict_details_preserve_key_order() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();

    let base = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [
            {"@id": "ex:alice", "ex:name": "Alice"},
            {"@id": "ex:bob", "ex:name": "Bob"},
            {"@id": "ex:carol", "ex:name": "Carol"}
        ]
    });
    let main_ledger = fluree.insert(ledger, &base).await.unwrap().ledger;
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
                "@graph": [
                    {"@id": "ex:alice", "ex:name": "Alice-dev"},
                    {"@id": "ex:bob", "ex:name": "Bob-dev"},
                    {"@id": "ex:carol", "ex:name": "Carol-dev"}
                ]
            }),
        )
        .await
        .unwrap();
    fluree
        .upsert(
            main_ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [
                    {"@id": "ex:alice", "ex:name": "Alice-main"},
                    {"@id": "ex:bob", "ex:name": "Bob-main"},
                    {"@id": "ex:carol", "ex:name": "Carol-main"}
                ]
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
                include_conflict_details: true,
                ..MergePreviewOpts::default()
            },
        )
        .await
        .unwrap();

    assert!(preview.conflicts.keys.len() >= 3);
    assert_eq!(
        preview.conflicts.details.len(),
        preview.conflicts.keys.len()
    );
    for (detail, key) in preview
        .conflicts
        .details
        .iter()
        .zip(&preview.conflicts.keys)
    {
        assert_eq!(&detail.key, key);
    }
}

#[tokio::test]
async fn preview_conflict_details_work_after_binary_index_reload() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();

    let base = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
    });
    let main_ledger = fluree.insert(ledger, &base).await.unwrap().ledger;
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

    let preview = fluree
        .merge_preview_with(
            "mydb",
            "dev",
            None,
            MergePreviewOpts {
                include_conflict_details: true,
                conflict_strategy: ConflictStrategy::TakeSource,
                ..MergePreviewOpts::default()
            },
        )
        .await
        .unwrap();

    assert_eq!(preview.conflicts.count, 1);
    assert_eq!(preview.conflicts.details.len(), 1);
    let detail = &preview.conflicts.details[0];
    let source_values = serde_json::to_string(&detail.source_values).unwrap();
    let target_values = serde_json::to_string(&detail.target_values).unwrap();
    assert!(source_values.contains("Alice-dev"), "{source_values}");
    assert!(target_values.contains("Alice-main"), "{target_values}");
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

    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();

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

    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();

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

    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();

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

    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();

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

    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();

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

    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();

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

#[tokio::test]
async fn preview_conflict_details_require_conflict_computation() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();

    let base = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
    });
    fluree.insert(ledger, &base).await.unwrap();
    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();

    let err = fluree
        .merge_preview_with(
            "mydb",
            "dev",
            None,
            MergePreviewOpts {
                include_conflicts: false,
                include_conflict_details: true,
                ..MergePreviewOpts::default()
            },
        )
        .await
        .expect_err("conflict details without conflict computation should fail");

    assert!(err
        .to_string()
        .contains("include_conflict_details requires include_conflicts=true"));
}

#[tokio::test]
async fn preview_abort_strategy_requires_conflict_computation() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();

    let base = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
    });
    fluree.insert(ledger, &base).await.unwrap();
    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();

    let err = fluree
        .merge_preview_with(
            "mydb",
            "dev",
            None,
            MergePreviewOpts {
                include_conflicts: false,
                conflict_strategy: ConflictStrategy::Abort,
                ..MergePreviewOpts::default()
            },
        )
        .await
        .expect_err("abort mergeability requires conflict computation");

    assert!(err
        .to_string()
        .contains("strategy=abort requires include_conflicts=true"));
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

    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();
    fluree
        .create_branch("mydb", "feature", None, None)
        .await
        .unwrap();

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

    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();

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
                ..MergePreviewOpts::default()
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

    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();

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

// =============================================================================
// 9. Aggregate change set (include_changes)
// =============================================================================

/// Standard fixture: `main` holds ex:alice, branch `dev` is created from it.
async fn setup_branch(fluree: &fluree_db_api::Fluree) -> fluree_db_api::LedgerState {
    let ledger = fluree.create_ledger("mydb").await.unwrap();
    let base = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
    });
    let main_ledger = fluree.insert(ledger, &base).await.unwrap().ledger;
    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();
    main_ledger
}

fn changes_opts() -> MergePreviewOpts {
    MergePreviewOpts {
        include_changes: true,
        ..MergePreviewOpts::default()
    }
}

#[tokio::test]
async fn changes_absent_by_default() {
    let fluree = FlureeBuilder::memory().build_memory();
    setup_branch(&fluree).await;

    let preview = fluree.merge_preview("mydb", "dev", None).await.unwrap();
    assert!(preview.changes.is_none());
}

#[tokio::test]
async fn changes_fast_forward_basic() {
    let fluree = FlureeBuilder::memory().build_memory();
    setup_branch(&fluree).await;

    let dev = fluree.ledger("mydb:dev").await.unwrap();
    fluree
        .insert(
            dev,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [{"@id": "ex:bob", "ex:name": "Bob"}]
            }),
        )
        .await
        .unwrap();

    let preview = fluree
        .merge_preview_with("mydb", "dev", None, changes_opts())
        .await
        .unwrap();

    assert!(preview.fast_forward);
    let changes = preview.changes.expect("include_changes was set");
    assert_eq!(changes.assert_count, 1);
    assert_eq!(changes.retract_count, 0);
    assert_eq!(changes.subject_count, 1);
    assert!(!changes.truncated);
    assert!(changes.next_cursor.is_none());
    assert_eq!(changes.entries.len(), 1);
    let entry = &changes.entries[0];
    assert_eq!(entry.subject, "http://example.org/ns/bob");
    assert_eq!(entry.asserts.len(), 1);
    assert!(entry.retracts.is_empty());
    assert!(entry.asserts[0].op);
}

#[tokio::test]
async fn changes_netting_cancels_churn() {
    let fluree = FlureeBuilder::memory().build_memory();
    setup_branch(&fluree).await;

    // Create bob, then delete him again — the net diff must be empty.
    let dev = fluree.ledger("mydb:dev").await.unwrap();
    let dev = fluree
        .insert(
            dev,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [{"@id": "ex:bob", "ex:name": "Bob"}]
            }),
        )
        .await
        .unwrap()
        .ledger;
    fluree
        .update(
            dev,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "delete": {"@id": "ex:bob", "ex:name": "Bob"}
            }),
        )
        .await
        .unwrap();

    let preview = fluree
        .merge_preview_with("mydb", "dev", None, changes_opts())
        .await
        .unwrap();

    let changes = preview.changes.expect("include_changes was set");
    assert_eq!(changes.assert_count, 0, "churn must cancel");
    assert_eq!(changes.retract_count, 0);
    assert_eq!(changes.subject_count, 0);
    assert!(changes.entries.is_empty());
    assert!(!changes.truncated);
    // The commits themselves are still visible — only the net diff is empty.
    assert!(preview.ahead.count >= 2);
}

#[tokio::test]
async fn changes_update_shows_both_sides() {
    let fluree = FlureeBuilder::memory().build_memory();
    setup_branch(&fluree).await;

    // Rename alice on the branch: net = one retract (old) + one assert (new).
    let dev = fluree.ledger("mydb:dev").await.unwrap();
    fluree
        .update(
            dev,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "delete": {"@id": "ex:alice", "ex:name": "Alice"},
                "insert": {"@id": "ex:alice", "ex:name": "Alicia"}
            }),
        )
        .await
        .unwrap();

    let preview = fluree
        .merge_preview_with("mydb", "dev", None, changes_opts())
        .await
        .unwrap();

    let changes = preview.changes.expect("include_changes was set");
    assert_eq!(changes.assert_count, 1);
    assert_eq!(changes.retract_count, 1);
    assert_eq!(changes.subject_count, 1);
    assert_eq!(changes.entries.len(), 1);
    let entry = &changes.entries[0];
    assert_eq!(entry.subject, "http://example.org/ns/alice");
    assert_eq!(entry.asserts.len(), 1);
    assert_eq!(entry.retracts.len(), 1);
}

#[tokio::test]
async fn changes_pure_retract_of_ancestor_fact() {
    let fluree = FlureeBuilder::memory().build_memory();
    setup_branch(&fluree).await;

    let dev = fluree.ledger("mydb:dev").await.unwrap();
    fluree
        .update(
            dev,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "delete": {"@id": "ex:alice", "ex:name": "Alice"}
            }),
        )
        .await
        .unwrap();

    let preview = fluree
        .merge_preview_with("mydb", "dev", None, changes_opts())
        .await
        .unwrap();

    let changes = preview.changes.expect("include_changes was set");
    assert_eq!(changes.assert_count, 0);
    assert_eq!(changes.retract_count, 1);
    assert_eq!(changes.subject_count, 1);
    assert!(!changes.entries[0].retracts[0].op);
}

#[tokio::test]
async fn changes_truncation_cuts_at_subject_boundary_and_pages() {
    let fluree = FlureeBuilder::memory().build_memory();
    setup_branch(&fluree).await;

    // Three subjects, two facts each: 6 net flakes total.
    let dev = fluree.ledger("mydb:dev").await.unwrap();
    fluree
        .insert(
            dev,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [
                    {"@id": "ex:s1", "ex:name": "One",   "ex:rank": 1},
                    {"@id": "ex:s2", "ex:name": "Two",   "ex:rank": 2},
                    {"@id": "ex:s3", "ex:name": "Three", "ex:rank": 3},
                ]
            }),
        )
        .await
        .unwrap();

    let page = |after: Option<String>| {
        let fluree = &fluree;
        async move {
            fluree
                .merge_preview_with(
                    "mydb",
                    "dev",
                    None,
                    MergePreviewOpts {
                        include_changes: true,
                        max_changes: Some(2),
                        changes_after_subject: after,
                        ..MergePreviewOpts::default()
                    },
                )
                .await
                .unwrap()
                .changes
                .expect("include_changes was set")
        }
    };

    // Page 1: cap of 2 flakes fits exactly one subject.
    let p1 = page(None).await;
    assert_eq!(p1.assert_count, 6, "counts stay exact under the cap");
    assert_eq!(p1.subject_count, 3);
    assert_eq!(p1.entries.len(), 1);
    assert!(p1.truncated);
    let cursor1 = p1.next_cursor.clone().expect("truncated page has cursor");
    assert_eq!(cursor1, p1.entries[0].subject);

    // Page 2 resumes strictly after the cursor.
    let p2 = page(Some(cursor1.clone())).await;
    assert_eq!(p2.entries.len(), 1);
    assert!(p2.entries[0].subject > cursor1, "no overlap between pages");
    assert!(p2.truncated);

    // Page 3 is the last.
    let p3 = page(p2.next_cursor.clone()).await;
    assert_eq!(p3.entries.len(), 1);
    assert!(!p3.truncated);
    assert!(p3.next_cursor.is_none());

    let mut subjects: Vec<String> = [&p1, &p2, &p3]
        .iter()
        .flat_map(|p| p.entries.iter().map(|e| e.subject.clone()))
        .collect();
    let ordered = subjects.clone();
    subjects.sort();
    subjects.dedup();
    assert_eq!(subjects.len(), 3, "pages cover all subjects exactly once");
    assert_eq!(ordered, subjects, "subjects arrive in IRI order");
}

#[tokio::test]
async fn changes_stats_only_mode() {
    let fluree = FlureeBuilder::memory().build_memory();
    setup_branch(&fluree).await;

    let dev = fluree.ledger("mydb:dev").await.unwrap();
    fluree
        .insert(
            dev,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [
                    {"@id": "ex:bob", "ex:name": "Bob"},
                    {"@id": "ex:carol", "ex:name": "Carol"},
                ]
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
                include_changes: true,
                max_changes: Some(0),
                ..MergePreviewOpts::default()
            },
        )
        .await
        .unwrap();

    let changes = preview.changes.expect("include_changes was set");
    assert_eq!(changes.assert_count, 2);
    assert_eq!(changes.retract_count, 0);
    assert_eq!(changes.subject_count, 2);
    assert!(changes.entries.is_empty());
    assert!(changes.truncated, "payload was withheld");
    assert!(changes.next_cursor.is_none());
}

#[tokio::test]
async fn changes_alongside_conflicts_and_details() {
    let fluree = FlureeBuilder::memory().build_memory();
    let main_ledger = setup_branch(&fluree).await;

    // Diverge: both sides rename alice.
    let dev = fluree.ledger("mydb:dev").await.unwrap();
    fluree
        .insert(
            dev,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [{"@id": "ex:alice", "ex:name": "A-dev"}]
            }),
        )
        .await
        .unwrap();
    fluree
        .insert(
            main_ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [{"@id": "ex:alice", "ex:name": "A-main"}]
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
                include_changes: true,
                include_conflict_details: true,
                ..MergePreviewOpts::default()
            },
        )
        .await
        .unwrap();

    assert!(!preview.fast_forward);
    assert!(preview.conflicts.count >= 1);
    assert!(!preview.conflicts.details.is_empty());
    let changes = preview.changes.expect("include_changes was set");
    assert_eq!(changes.assert_count, 1, "source-side net assert");
    assert_eq!(changes.entries[0].subject, "http://example.org/ns/alice");
}

#[tokio::test]
async fn changes_cursor_requires_include_changes() {
    let fluree = FlureeBuilder::memory().build_memory();
    setup_branch(&fluree).await;

    let err = fluree
        .merge_preview_with(
            "mydb",
            "dev",
            None,
            MergePreviewOpts {
                changes_after_subject: Some("http://example.org/ns/a".to_string()),
                ..MergePreviewOpts::default()
            },
        )
        .await
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("changes_after_subject requires include_changes"));
}

#[tokio::test]
async fn changes_multi_value_predicate_nets_independently() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger("mydb").await.unwrap();
    // Alice starts with two nicknames.
    fluree
        .insert(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [{"@id": "ex:alice", "ex:nick": ["Al", "Ali"]}]
            }),
        )
        .await
        .unwrap();
    fluree
        .create_branch("mydb", "dev", None, None)
        .await
        .unwrap();

    // Remove one of the two values on the branch.
    let dev = fluree.ledger("mydb:dev").await.unwrap();
    fluree
        .update(
            dev,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "delete": {"@id": "ex:alice", "ex:nick": "Al"}
            }),
        )
        .await
        .unwrap();

    let preview = fluree
        .merge_preview_with("mydb", "dev", None, changes_opts())
        .await
        .unwrap();

    let changes = preview.changes.expect("include_changes was set");
    assert_eq!(changes.assert_count, 0);
    assert_eq!(
        changes.retract_count, 1,
        "only the deleted value appears; the sibling value is untouched"
    );
}
