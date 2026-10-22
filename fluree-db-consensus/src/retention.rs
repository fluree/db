//! The retention worker's writer over a consensus committer.
//!
//! Expiration batches go through the deployment's ordered write path like
//! any other transaction: under Raft, the queued transactor and the branch's
//! commit worker, so they serialize with every other write to the branch
//! and survive a leader change through the replicated idempotency map.

use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use fluree_db_api::retention::{
    RetentionBatch, RetentionWriter, SubmitOutcome, DEFERRED, HEAD_MOVED,
};
use fluree_db_api::{ApiError, GovernanceOptions};
use fluree_db_transact::{CommitOpts, TxnOpts};
use sha2::{Digest, Sha256};

use crate::{
    IdempotencyKey, SubmissionError, SubmittingCommitter, TransactionBody, TransactionRequest,
};

/// Submits expiration batches through a committer installed once it exists.
pub struct CommitterRetentionWriter {
    committer: Arc<OnceLock<Arc<dyn SubmittingCommitter>>>,
}

impl CommitterRetentionWriter {
    /// A writer whose committer is set into `committer` later; batches wait
    /// (are deferred) until it is.
    pub fn new(committer: Arc<OnceLock<Arc<dyn SubmittingCommitter>>>) -> Self {
        Self { committer }
    }
}

#[async_trait]
impl RetentionWriter for CommitterRetentionWriter {
    async fn submit(
        &self,
        ledger_id: &str,
        batch: RetentionBatch,
    ) -> fluree_db_api::Result<SubmitOutcome> {
        let Some(committer) = self.committer.get() else {
            return Ok(SubmitOutcome::Deferred);
        };
        let expected_t = batch.expected_t;
        let bytes = batch.encode()?;
        let digest = Sha256::digest(&bytes);
        // The same batch against the same head is the same submission, so a
        // retry after a lost response (or a new leader) is deduplicated.
        let key = IdempotencyKey::new(format!(
            "retention:{expected_t}:{}",
            hex::encode(&digest[..16])
        ))
        .map_err(|e| ApiError::internal(format!("retention idempotency key: {e}")))?;
        let request = TransactionRequest {
            idempotency_key: Some(key),
            ledger_id: ledger_id.to_string(),
            body: TransactionBody::Retention {
                batch: hex::encode(&bytes),
            },
            txn_opts: TxnOpts::default(),
            commit_opts: CommitOpts::default(),
            tracking: None,
            governance: GovernanceOptions::default(),
        };
        match committer.transact(request).await {
            Ok(receipt) => Ok(SubmitOutcome::Committed {
                t: receipt.commit.t,
                commit_id: receipt.commit.commit_id,
            }),
            Err(SubmissionError::Execution { status, message })
                if status == 409 || message.contains(HEAD_MOVED) =>
            {
                Ok(SubmitOutcome::HeadMoved)
            }
            Err(SubmissionError::Execution { message, .. }) if message.contains(DEFERRED) => {
                Ok(SubmitOutcome::Deferred)
            }
            Err(SubmissionError::NoveltyBackpressure { .. } | SubmissionError::AlreadyInFlight) => {
                Ok(SubmitOutcome::Deferred)
            }
            Err(e) => Err(ApiError::internal(format!(
                "retention batch submission: {e}"
            ))),
        }
    }
}
