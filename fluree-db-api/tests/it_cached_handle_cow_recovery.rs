//! A failed commit must leave the cached ledger handle usable.
//!
//! The cached-handle commit path empties the cache slot for the duration of a
//! commit — it takes the cached `LedgerState` out and drops it so the commit
//! uniquely owns the dictionaries it extends (see `it_cached_handle_cow`) —
//! and the slot holds a genesis placeholder until the committed state is
//! installed. A commit that fails in between must refill the slot from
//! storage; if it does not, the handle every caller already holds reads an
//! empty ledger at `t = 0`.
//!
//! Forcing a failure *inside* the commit is the whole difficulty: most
//! rejections happen during staging, before the slot is ever emptied, and
//! would make this test vacuous. The novelty ceiling is checked twice with
//! different predicates — staging rejects when novelty is *already* at the
//! ceiling, while the commit path also rejects when this transaction's delta
//! *would* cross it. A ceiling one byte above the current novelty size passes
//! the first and fails the second, which lands the failure exactly where the
//! placeholder is live. `NoveltyWouldExceed` is also not a retryable commit
//! conflict, so no reconcile-and-retry runs afterwards to mask a broken
//! recovery.

use crate::support;
use fluree_db_api::FlureeBuilder;
use fluree_db_ledger::IndexConfig;
use serde_json::json;

#[tokio::test]
async fn failed_commit_leaves_the_cached_handle_usable() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let fluree = FlureeBuilder::file(tmp.path().to_string_lossy().to_string())
        .without_indexing()
        .build()
        .expect("build");

    let ledger_id = "it/cow-recovery:main";
    fluree
        .create_ledger(ledger_id)
        .await
        .expect("create ledger");
    let handle = fluree
        .ledger_cached(ledger_id)
        .await
        .expect("cache the ledger");

    let first = json!({
        "@context": { "ex": "http://example.org/" },
        "@id": "ex:alice",
        "ex:name": "Alice"
    });
    let ok = fluree
        .stage(&handle)
        .insert(&first)
        .execute()
        .await
        .expect("first commit");
    assert_eq!(ok.receipt.t, 1);

    // One byte above the novelty already committed: staging's "already at the
    // ceiling" check passes, the commit path's "this delta would cross it"
    // check fails.
    let ceiling = handle.snapshot().await.novelty.size + 1;
    let doomed = json!({
        "@context": { "ex": "http://example.org/" },
        "@id": "ex:bob",
        "ex:name": "Bob"
    });
    let err = fluree
        .stage(&handle)
        .insert(&doomed)
        .index_config(IndexConfig {
            reindex_min_bytes: 1,
            reindex_max_bytes: ceiling,
        })
        .execute()
        .await
        .expect_err("this commit must cross the novelty ceiling");
    assert!(
        format!("{err}").to_lowercase().contains("novelty"),
        "expected the commit path's novelty-ceiling rejection, got: {err}"
    );

    // The cache must hold the real ledger, not the placeholder the failed
    // commit left behind. `t = 0` here is the regression this test exists for.
    assert_eq!(
        handle.t().await,
        1,
        "the cached handle must still be at the committed head after a failed commit"
    );

    // And it must still serve that ledger's data.
    let state = handle.snapshot().await.to_ledger_state();
    let query = json!({
        "@context": { "ex": "http://example.org/" },
        "select": { "ex:alice": ["*"] }
    });
    let rows = support::query_jsonld(&fluree, &state, &query)
        .await
        .expect("query through the recovered cache");
    assert!(
        !rows.is_empty(),
        "the recovered cache must still resolve data committed before the failure"
    );

    // A subsequent commit through the same handle proceeds normally.
    let third = json!({
        "@context": { "ex": "http://example.org/" },
        "@id": "ex:carol",
        "ex:name": "Carol"
    });
    let after = fluree
        .stage(&handle)
        .insert(&third)
        .execute()
        .await
        .expect("commit after the failed one");
    assert_eq!(
        after.receipt.t, 2,
        "the next commit continues from the real head"
    );
    assert_eq!(handle.t().await, 2);
}
