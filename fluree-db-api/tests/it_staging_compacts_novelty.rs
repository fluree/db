//! A writer that never queries still stages on compacted novelty.
//!
//! Novelty holds one segment per commit until a compaction merges them, and
//! each read a transaction stages — a DELETE's WHERE, an upsert's prior
//! values — searches every segment. A query snapshot compacts first; so must
//! the lock-held write path, which a policy-gated request (as every server
//! request is) takes, and a push, which reads novelty for every retraction it
//! carries.

use crate::support;
use fluree_db_api::{
    ExportCommitsRequest, FlureeBuilder, GovernanceOptions, IndexConfig, PolicyContext,
    PolicyWrapper, PushCommitsRequest,
};
use serde_json::json;

#[tokio::test]
async fn policy_gated_updates_stage_on_compacted_novelty() {
    let fluree = FlureeBuilder::memory().without_indexing().build_memory();
    let ledger_id = "test/staging-compaction:main";
    fluree.create_ledger(ledger_id).await.expect("create");
    let handle = fluree.ledger_cached(ledger_id).await.expect("handle");
    let tier_width = 4;
    handle.set_tier_width(tier_width);

    let commits = 40u32;
    for n in 0..commits {
        let update = format!(
            "PREFIX ex: <http://example.org/> \
             DELETE {{ ?s ?p ?o }} WHERE {{ VALUES ?s {{ ex:s{prev} }} ?s ?p ?o }} ; \
             INSERT DATA {{ ex:s{n} ex:n {n} }}",
            prev = n.saturating_sub(1)
        );
        fluree
            .stage(&handle)
            .sparql_update(&update)
            .policy(PolicyContext::new(PolicyWrapper::root(), None))
            .execute()
            .await
            .expect("update");
    }

    let segments = handle
        .lock_for_write()
        .await
        .state()
        .novelty
        .max_segment_count();
    assert!(
        segments <= tier_width * 2,
        "{commits} commits left {segments} novelty segments; staging must compact"
    );

    let ledger = fluree.ledger(ledger_id).await.expect("reload");
    let rows = support::query_sparql_formatted(
        &fluree,
        &ledger,
        "PREFIX ex: <http://example.org/> SELECT ?s ?o WHERE { ?s ex:n ?o }",
    )
    .await
    .expect("query");
    assert_eq!(
        rows,
        json!([["ex:s39", 39]]),
        "each update replaced the last"
    );
}

#[tokio::test]
async fn pushes_to_a_ledger_nothing_queries_stage_on_compacted_novelty() {
    let ledger_id = "test/push-compaction:main";
    let source = FlureeBuilder::memory().without_indexing().build_memory();
    source
        .create_ledger(ledger_id)
        .await
        .expect("create source");
    let commits = 40u32;
    for n in 0..commits {
        source
            .graph(ledger_id)
            .transact()
            .sparql_update(&format!(
                "PREFIX ex: <http://example.org/> INSERT DATA {{ ex:s{n} ex:n {n} }}"
            ))
            .commit()
            .await
            .expect("source write");
    }
    let source_handle = source.ledger_cached(ledger_id).await.expect("source");
    let mut exported = source
        .export_commit_range(
            &source_handle,
            &ExportCommitsRequest {
                limit: Some(100),
                ..Default::default()
            },
        )
        .await
        .expect("export")
        .commits;
    exported.reverse();
    assert_eq!(exported.len(), commits as usize);

    let target = FlureeBuilder::memory().without_indexing().build_memory();
    target
        .create_ledger(ledger_id)
        .await
        .expect("create target");
    let handle = target.ledger_cached(ledger_id).await.expect("handle");
    let tier_width = 4;
    handle.set_tier_width(tier_width);
    let config = IndexConfig {
        reindex_min_bytes: 1_000_000_000,
        reindex_max_bytes: 2_000_000_000,
    };
    for commit in exported {
        let request = PushCommitsRequest {
            commits: vec![commit],
            blobs: Default::default(),
            missing_blobs: Vec::new(),
            merged_commits: Vec::new(),
        };
        let pushed = target
            .push_commits(ledger_id, request, &GovernanceOptions::default(), &config)
            .await
            .expect("push");
        assert_eq!(pushed.accepted, 1);
    }

    let segments = handle
        .lock_for_write()
        .await
        .state()
        .novelty
        .max_segment_count();
    assert!(
        segments <= tier_width * 2,
        "{commits} pushes left {segments} novelty segments; a push must stage on compacted novelty"
    );
}
