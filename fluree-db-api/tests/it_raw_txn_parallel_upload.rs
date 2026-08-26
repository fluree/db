//! Parallel upload of raw transaction JSON (`store_raw_txn` opt-in).
//!
//! Exercises `PendingRawTxnUpload` and `CommitOpts::with_raw_txn_spawned`:
//! the raw JSON is uploaded on a Tokio task spawned at the top of the
//! transaction pipeline, and `commit()` awaits that handle just before
//! writing the commit blob. On success, the commit record references the
//! raw-txn ContentId and the bytes are retrievable from the content store.

use fluree_db_api::{CommitOpts, FlureeBuilder, IndexConfig, LedgerState, Novelty};
use fluree_db_core::{commit::codec::read_commit, ContentKind, ContentStore as _, LedgerSnapshot};
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

fn upsert_alice() -> JsonValue {
    json!({
        "@context": ctx(),
        "@graph": [
            { "id": "ex:alice", "ex:name": "Alice" }
        ]
    })
}

fn raw_txn_config() -> IndexConfig {
    IndexConfig {
        reindex_min_bytes: 100_000,
        reindex_max_bytes: 1_000_000_000,
    }
}

async fn txn_cid_of(
    fluree: &fluree_db_api::Fluree,
    ledger_id: &str,
    commit_id: &fluree_db_core::ContentId,
) -> fluree_db_core::ContentId {
    let bytes = fluree
        .content_store(ledger_id)
        .get(commit_id)
        .await
        .expect("commit blob readable");
    read_commit(&bytes)
        .expect("commit decodes")
        .txn
        .expect("commit references a txn blob")
}

/// Regression for a dangling `commit.txn` seen in production: raw-txn blobs
/// are content-addressed, so a redelivered byte-identical body maps to the
/// SAME CID as the commit that already landed. That redelivery stages to
/// zero flakes (`EmptyTransaction`) — and used to *delete* the shared blob
/// on its error path, orphaning the first commit's provenance pointer.
/// (At the API layer the no-op short-circuits before `build_commit`; the
/// pending upload is dropped either way, which is the path that used to
/// delete.) Multi-threaded runtime so the spawned upload actually completes
/// while staging runs, as it does in production.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_body_failure_keeps_first_commits_txn_blob() {
    let ledger_id = "it/raw-txn:duplicate-body";
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = LedgerState::new(LedgerSnapshot::genesis(ledger_id), Novelty::new(0));
    let txn_opts = IrTxnOpts::default().store_raw_txn(true);

    let first = fluree
        .transact(
            ledger0,
            TxnType::Upsert,
            &upsert_alice(),
            txn_opts.clone(),
            CommitOpts::default(),
            &raw_txn_config(),
        )
        .await
        .expect("first commit succeeds");
    let txn_cid = txn_cid_of(&fluree, ledger_id, &first.receipt.commit_id).await;

    // Same body again: no new flakes → EmptyTransaction.
    let second = fluree
        .transact(
            first.ledger,
            TxnType::Upsert,
            &upsert_alice(),
            txn_opts,
            CommitOpts::default(),
            &raw_txn_config(),
        )
        .await;
    // Either an error or a zero-flake no-op is acceptable; what matters is
    // that no second commit lands and the shared blob is untouched.
    match second.map(|r| (r.receipt.t, r.receipt.flake_count)) {
        Err(_) => {}
        Ok((t, flakes)) => assert!(
            t == 1 && flakes == 0,
            "identical redelivery must not produce a new commit (t={t}, flakes={flakes})"
        ),
    }

    // Give any detached cleanup task (the old Drop-guard delete) a chance to
    // run before asserting — the bug was a delete landing *after* the caller
    // had moved on.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(
        fluree.content_store(ledger_id).get(&txn_cid).await.is_ok(),
        "first commit's raw-txn blob must survive the duplicate's failure"
    );

    let report = fluree
        .verify_ledger(ledger_id, None)
        .await
        .expect("verify runs");
    assert!(
        report.is_healthy(),
        "unexpected problems: {:?}",
        report.problems
    );
    assert_eq!(report.commits_checked, 1);
    assert_eq!(report.txn_refs_checked, 1);
}

/// A ledger whose commit references a missing txn blob must still be
/// diagnosable (`verify_ledger`) and replicable (`export_commit_range`
/// reports the gap instead of failing the whole export).
#[tokio::test]
async fn verify_and_export_tolerate_missing_txn_blob() {
    use fluree_db_api::{ExportCommitsRequest, VerifyProblem, VerifySeverity};

    let ledger_id = "it/raw-txn:missing-blob";
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = LedgerState::new(LedgerSnapshot::genesis(ledger_id), Novelty::new(0));

    let result = fluree
        .transact(
            ledger0,
            TxnType::Upsert,
            &upsert_alice(),
            IrTxnOpts::default().store_raw_txn(true),
            CommitOpts::default(),
            &raw_txn_config(),
        )
        .await
        .expect("commit succeeds");
    let commit_id = result.receipt.commit_id.clone();
    let txn_cid = txn_cid_of(&fluree, ledger_id, &commit_id).await;

    // Simulate the production damage: the blob vanishes out from under the commit.
    fluree
        .content_store(ledger_id)
        .release(&txn_cid)
        .await
        .expect("release");

    let report = fluree
        .verify_ledger(ledger_id, None)
        .await
        .expect("verify runs");
    assert_eq!(
        report.problems,
        vec![VerifyProblem::MissingTxnBlob {
            t: 1,
            commit_id: commit_id.clone(),
            txn_id: txn_cid.clone(),
        }]
    );
    assert!(!report.is_healthy());
    // Provenance-only: state and every replication path still work, so the
    // CLI exits 3 (gate-able) rather than 4 (chain broken).
    assert_eq!(report.severity(), VerifySeverity::Provenance);

    let handle = fluree.ledger_cached(ledger_id).await.expect("handle");
    let export = fluree
        .export_commit_range(
            &handle,
            &ExportCommitsRequest {
                cursor: None,
                cursor_id: None,
                limit: Some(10),
            },
        )
        .await
        .expect("export must not fail on a missing txn blob");
    assert_eq!(export.count, 1);
    assert!(export.blobs.is_empty());
    assert_eq!(export.missing_blobs, vec![txn_cid.to_string()]);
}

/// The receiving side of the same gap: a push whose commits reference a txn
/// blob the sender cannot supply is accepted, whether the sender declares
/// the gap or (as a sender predating `missing_blobs` would) leaves it
/// undeclared. Refusing it is what made the incident ledger unreplicable.
#[tokio::test]
async fn push_accepts_commits_whose_txn_blob_is_missing() {
    use fluree_db_api::{
        ExportCommitsRequest, GovernanceOptions, PushCommitsRequest, VerifyProblem, VerifySeverity,
    };

    async fn export_with_gap(
        fluree: &fluree_db_api::Fluree,
        ledger_id: &str,
    ) -> (Vec<fluree_db_api::Base64Bytes>, Vec<String>) {
        let ledger0 = LedgerState::new(LedgerSnapshot::genesis(ledger_id), Novelty::new(0));
        let result = fluree
            .transact(
                ledger0,
                TxnType::Upsert,
                &upsert_alice(),
                IrTxnOpts::default().store_raw_txn(true),
                CommitOpts::default(),
                &raw_txn_config(),
            )
            .await
            .expect("commit succeeds");
        let txn_cid = txn_cid_of(fluree, ledger_id, &result.receipt.commit_id).await;
        fluree
            .content_store(ledger_id)
            .release(&txn_cid)
            .await
            .expect("release");

        let handle = fluree.ledger_cached(ledger_id).await.expect("handle");
        let export = fluree
            .export_commit_range(
                &handle,
                &ExportCommitsRequest {
                    cursor: None,
                    cursor_id: None,
                    limit: Some(10),
                },
            )
            .await
            .expect("export tolerates the gap");
        assert_eq!(export.missing_blobs, vec![txn_cid.to_string()]);

        // Export returns newest → oldest; push needs oldest → newest.
        let mut commits = export.commits;
        commits.reverse();
        (commits, export.missing_blobs)
    }

    let index_config = raw_txn_config();

    // Declared: the sender names the CID it could not supply.
    {
        let fluree = FlureeBuilder::memory().build_memory();
        let (commits, missing_blobs) =
            export_with_gap(&fluree, "it/raw-txn:push-src-declared").await;

        let tgt = "it/raw-txn:push-tgt-declared";
        fluree.create_ledger(tgt).await.expect("create target");
        let resp = fluree
            .push_commits(
                tgt,
                PushCommitsRequest {
                    commits,
                    blobs: Default::default(),
                    missing_blobs,
                },
                &GovernanceOptions::default(),
                &index_config,
            )
            .await
            .expect("push must not fail on a declared txn-blob gap");
        assert_eq!(resp.accepted, 1);
        assert_eq!(resp.head.t, 1);

        // The gap replicated as a gap: verify names it, and classifies it as
        // provenance-only so `fluree verify` exits 3 rather than 4.
        let report = fluree.verify_ledger(tgt, None).await.expect("verify runs");
        assert_eq!(report.severity(), VerifySeverity::Provenance);
        assert!(matches!(
            report.problems.as_slice(),
            [VerifyProblem::MissingTxnBlob { t: 1, .. }]
        ));
    }

    // Undeclared: a sender that predates `missing_blobs` sends the same
    // bundle with an empty declaration. Still accepted (logged as a warning).
    {
        let fluree = FlureeBuilder::memory().build_memory();
        let (commits, _) = export_with_gap(&fluree, "it/raw-txn:push-src-undeclared").await;

        let tgt = "it/raw-txn:push-tgt-undeclared";
        fluree.create_ledger(tgt).await.expect("create target");
        let resp = fluree
            .push_commits(
                tgt,
                PushCommitsRequest {
                    commits,
                    blobs: Default::default(),
                    missing_blobs: Vec::new(),
                },
                &GovernanceOptions::default(),
                &index_config,
            )
            .await
            .expect("push must not fail on an undeclared txn-blob gap");
        assert_eq!(resp.accepted, 1);
    }
}
