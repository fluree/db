//! Parallel upload of raw transaction JSON (`store_raw_txn` opt-in).
//!
//! Exercises `PendingRawTxnUpload` and `CommitOpts::with_raw_txn_spawned`:
//! the raw JSON is uploaded on a Tokio task spawned at the top of the
//! transaction pipeline, and `commit()` awaits that handle just before
//! writing the commit blob. On success, the commit record references the
//! raw-txn ContentId and the bytes are retrievable from the content store.

mod support;

use fluree_db_api::{CommitOpts, FlureeBuilder, IndexConfig, LedgerState, Novelty};
use fluree_db_core::{commit::codec::read_commit, ContentKind, LedgerSnapshot};
use fluree_db_transact::{ir::TxnType, TxnOpts as IrTxnOpts};
use serde_json::{json, Value as JsonValue};

fn ctx() -> JsonValue {
    json!({
        "id": "@id",
        "type": "@type",
        "ex": "http://example.org/ns/"
    })
}

#[tokio::test]
async fn store_raw_txn_roundtrip_via_parallel_upload() {
    let ledger_id = "it/raw-txn:parallel-roundtrip";
    let fluree = FlureeBuilder::memory().build_memory();
    let db0 = LedgerSnapshot::genesis(ledger_id);
    let ledger0 = LedgerState::new(db0, Novelty::new(0));

    let txn_json = json!({
        "@context": ctx(),
        "insert": { "@graph": [
            { "id": "ex:alice", "ex:name": "Alice" }
        ]}
    });

    let txn_opts = IrTxnOpts::default().store_raw_txn(true);
    let index_config = IndexConfig {
        reindex_min_bytes: 100_000,
        reindex_max_bytes: 1_000_000_000,
    };

    // If the spawned upload had failed, commit() would have aborted here and
    // this call would return Err.
    let result = fluree
        .transact(
            ledger0,
            TxnType::Update,
            &txn_json,
            txn_opts,
            CommitOpts::default(),
            &index_config,
        )
        .await
        .expect("transaction should succeed with parallel raw-txn upload");

    // Fetch the commit blob, decode it, and confirm it references a txn CID.
    let content_store = fluree.content_store(ledger_id);
    let commit_bytes = content_store
        .get(&result.receipt.commit_id)
        .await
        .expect("commit blob should be retrievable");
    let commit = read_commit(&commit_bytes).expect("commit decodes");
    let txn_cid = commit
        .txn
        .clone()
        .expect("commit record should hold a txn CID when store_raw_txn is enabled");
    assert_eq!(
        txn_cid.content_kind(),
        Some(ContentKind::Txn),
        "referenced CID must be a Txn"
    );

    // Fetch the raw-txn bytes and confirm they match the originally-submitted JSON.
    let txn_bytes = content_store
        .get(&txn_cid)
        .await
        .expect("raw txn bytes should be retrievable from content store");
    let stored: JsonValue =
        serde_json::from_slice(&txn_bytes).expect("raw txn bytes should decode as JSON");
    assert_eq!(
        stored, txn_json,
        "stored raw txn should exactly match submitted JSON"
    );
}
