//! A writer that never queries still stages on compacted novelty.
//!
//! Novelty holds one segment per commit until a compaction merges them, and
//! each read a transaction stages — a DELETE's WHERE, an upsert's prior
//! values — searches every segment. A query snapshot compacts first; so must
//! the lock-held write path, which a policy-gated request (as every server
//! request is) takes.

use crate::support;
use fluree_db_api::{FlureeBuilder, PolicyContext, PolicyWrapper};
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
