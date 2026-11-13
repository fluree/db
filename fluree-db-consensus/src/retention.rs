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
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |since| since.as_millis() as i64);
        let key = IdempotencyKey::new(submission_key(expected_t, &bytes, now_ms))
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
            Err(e) => Err(submission_failure(e)),
        }
    }
}

/// How long one key stands for a batch (ms).
const KEY_WINDOW_MS: i64 = 60_000;

/// The idempotency key of a batch against a head. The same batch against
/// the same head is the same submission, so a retry after a lost response
/// (or a new leader) is deduplicated; but only within a window. A batch the
/// queue deferred is recorded against its key like any outcome, and while
/// the head stays where it was the next pass builds the same bytes: under
/// one key for good, it would be answered "deferred" from that record long
/// after the novelty that deferred it drained.
fn submission_key(expected_t: i64, batch: &[u8], now_ms: i64) -> String {
    let digest = Sha256::digest(batch);
    format!(
        "retention:{expected_t}:{}:{}",
        now_ms.div_euclid(KEY_WINDOW_MS),
        hex::encode(&digest[..16])
    )
}

/// A submission's failure as the request that made it should answer: with
/// the status the write path gave it, a retryable one included, not a 500.
fn submission_failure(e: SubmissionError) -> ApiError {
    use fluree_db_nameservice::NameServiceError;
    let described = format!("retention batch submission: {e}");
    match e {
        SubmissionError::Execution { status, .. } => ApiError::http(status, described),
        SubmissionError::Overloaded => ApiError::http(503, described),
        SubmissionError::KeyCollision => ApiError::http(409, described),
        SubmissionError::CommitNotFound { message } => ApiError::CommitNotFound(message),
        SubmissionError::Fenced { message } => {
            ApiError::NameService(NameServiceError::Fenced(message))
        }
        SubmissionError::LifecycleConflict { message } => {
            ApiError::NameService(NameServiceError::Conflict(message))
        }
        SubmissionError::ConfigUnavailable { message } => ApiError::ConfigUnavailable(message),
        _ => ApiError::internal(described),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_batch_keeps_its_key_within_a_window_only() {
        let key = |t, batch: &[u8], ms| submission_key(t, batch, ms);
        assert_eq!(key(7, b"batch", 120_000), key(7, b"batch", 179_999));
        assert_ne!(key(7, b"batch", 120_000), key(7, b"batch", 180_000));
        assert_ne!(key(7, b"batch", 120_000), key(8, b"batch", 120_000));
        assert_ne!(key(7, b"batch", 120_000), key(7, b"other", 120_000));
    }

    #[test]
    fn a_failed_submission_keeps_the_status_it_was_given() {
        let status = |e: SubmissionError| submission_failure(e).status_code();
        let execution = |status| SubmissionError::Execution {
            status,
            message: "refused".into(),
        };
        assert_eq!(status(execution(503)), 503);
        assert_eq!(status(execution(404)), 404);
        assert_eq!(status(SubmissionError::Overloaded), 503);
        assert_eq!(
            status(SubmissionError::LifecycleConflict {
                message: "dropping".into()
            }),
            409
        );
    }
}
