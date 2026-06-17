//! Thin [`Committer`](crate::Committer) impl that routes `transact`
//! through the Raft command queue.
//!
//! On submission, the transactor:
//! 1. Builds the [`QueuedRequest`](crate::QueuedRequest) envelope
//!    around the request body + per-request context.
//! 2. Writes the envelope to shared CAS — the resulting CID is
//!    `request_cid`.
//! 3. Proposes [`Command::EnqueueCommand`](super::state_machine::Command::EnqueueCommand)
//!    through Raft; the state machine appends a `QueueEntry` and
//!    returns the assigned `queue_id`.
//! 4. Registers a waiter on `queue_id` against the shared
//!    [`WaiterMap`](super::waiter::WaiterMap) and awaits the outcome.
//! 5. Translates [`WaiterOutcome::Applied`](super::waiter::WaiterOutcome)
//!    into a [`TransactionReceipt`], or `Aborted` into a
//!    [`SubmissionError`].
//!
//! Non-`transact` methods (`revert`, `merge`, `rebase`, `push`)
//! delegate to a fallback `Committer` during the migration — the
//! legacy `RaftCommitter` while we move call sites onto the queue.

use crate::raft::state_machine::{
    BodyKind, Command as SmCommand, EnqueueCommandArgs, RefKey, Response as SmResponse,
};
use crate::raft::waiter::{AbortReason, WaiterMap, WaiterOutcome};
use crate::raft::TypeConfig;
use crate::{
    Committer, IdempotencyCacheKey, MergeReceipt, MergeRequest, PushReceipt, PushRequest,
    QueuedPush, QueuedRequest, QueuedTransact, RebaseReceipt, RebaseRequest, RevertReceipt,
    RevertRequest, SubmissionError, TransactionReceipt, TransactionRequest,
};
use async_trait::async_trait;
use fluree_db_api::{CommitReceipt, Fluree};
use fluree_db_core::ledger_id::split_ledger_id;
use fluree_db_core::ContentKind;
use fluree_db_transact::CommitOptsRequest;
use openraft::Raft;
use std::sync::Arc;
use std::time::SystemTime;

/// Committer that routes transactions through the per-branch Raft
/// queue.
///
/// Cloning is cheap (`Arc` clones). Hand to a
/// [`CachingCommitter`](crate::CachingCommitter) wrap if you want
/// in-process idempotency dedup before proposing.
pub struct QueuedTransactor {
    raft: Arc<Raft<TypeConfig>>,
    fluree: Arc<Fluree>,
    waiter_map: Arc<WaiterMap>,
    /// Non-transact submissions still flow through the legacy
    /// `RaftCommitter`. Each path migrates onto the queue
    /// independently; until then the fallback covers them.
    fallback: Arc<dyn Committer>,
}

impl QueuedTransactor {
    pub fn new(
        raft: Arc<Raft<TypeConfig>>,
        fluree: Arc<Fluree>,
        waiter_map: Arc<WaiterMap>,
        fallback: Arc<dyn Committer>,
    ) -> Self {
        Self {
            raft,
            fluree,
            waiter_map,
            fallback,
        }
    }
}

#[async_trait]
impl Committer for QueuedTransactor {
    async fn transact(
        &self,
        request: TransactionRequest,
    ) -> Result<TransactionReceipt, SubmissionError> {
        let TransactionRequest {
            idempotency_key,
            ledger_id,
            body,
            txn_opts,
            commit_opts,
            tracking,
            governance,
        } = request;

        let (ledger_name, branch) = split_ledger_id(&ledger_id).map_err(|e| {
            SubmissionError::Execution {
                status: 400,
                message: format!("invalid ledger_id: {e}"),
            }
        })?;
        let ref_key = RefKey::new(&ledger_name, &branch);

        let idempotency_cache_key = idempotency_key
            .as_ref()
            .map(|k| IdempotencyCacheKey::new(ledger_id.clone(), k.clone()));

        let body_kind = BodyKind::from(&body);
        let envelope = QueuedRequest::Transact(QueuedTransact {
            body,
            txn_opts,
            commit_opts: CommitOptsRequest::from(&commit_opts),
            tracking,
            governance,
        });
        let bytes = envelope
            .to_bytes()
            .map_err(|e| SubmissionError::Execution {
                status: 500,
                message: format!("QueuedRequest encode failed: {e}"),
            })?;
        let request_cid = self
            .fluree
            .content_store(&ledger_id)
            .put(ContentKind::Txn, &bytes)
            .await
            .map_err(|e| SubmissionError::Execution {
                status: 500,
                message: format!("QueuedRequest CAS write failed: {e}"),
            })?;

        let cmd = SmCommand::EnqueueCommand(EnqueueCommandArgs {
            ledger_id: ledger_name,
            branch,
            idempotency: idempotency_cache_key,
            request_cid,
            body_kind,
            applied_at_millis: current_millis(),
        });

        let response = self
            .raft
            .client_write(cmd)
            .await
            .map_err(|e| SubmissionError::Execution {
                status: 500,
                message: format!("raft client_write failed: {e}"),
            })?;

        let waiter_rx = match response.data {
            SmResponse::Enqueued { queue_id, .. } | SmResponse::InFlight { queue_id, .. } => {
                self.waiter_map.register(queue_id, ref_key)
            }
            SmResponse::IdempotencyHit { record } => {
                return Ok(TransactionReceipt {
                    idempotency_key,
                    commit: CommitReceipt {
                        commit_id: record.head,
                        t: record.t,
                        flake_count: 0,
                    },
                    tally: record.tally.map(Into::into),
                });
            }
            SmResponse::IdempotencyFailed { record } => {
                return Err(SubmissionError::Execution {
                    status: 500,
                    message: format!("cached failure: {:?}", record.reason),
                });
            }
            SmResponse::BodyHashMismatch => return Err(SubmissionError::KeyCollision),
            SmResponse::QueueFull { .. } => return Err(SubmissionError::Overloaded),
            SmResponse::LedgerNotFound { ledger_id } => {
                return Err(SubmissionError::Execution {
                    status: 404,
                    message: format!("ledger not found: {ledger_id}"),
                });
            }
            other => {
                return Err(SubmissionError::Execution {
                    status: 500,
                    message: format!("unexpected Response variant for EnqueueCommand: {other:?}"),
                });
            }
        };

        let outcome = waiter_rx
            .await
            .map_err(|_| SubmissionError::Execution {
                status: 503,
                message: "queue waiter dropped before outcome — leader transition stranded the \
                          submission; retry with the same idempotency key"
                    .into(),
            })?;

        match outcome {
            WaiterOutcome::Applied { commit_id, commit_t } => Ok(TransactionReceipt {
                idempotency_key,
                commit: CommitReceipt {
                    commit_id,
                    t: commit_t,
                    flake_count: 0,
                },
                tally: None,
            }),
            WaiterOutcome::Aborted(reason) => Err(submission_error_from_abort(reason)),
        }
    }

    async fn revert(&self, request: RevertRequest) -> Result<RevertReceipt, SubmissionError> {
        self.fallback.revert(request).await
    }

    async fn merge(&self, request: MergeRequest) -> Result<MergeReceipt, SubmissionError> {
        self.fallback.merge(request).await
    }

    async fn rebase(&self, request: RebaseRequest) -> Result<RebaseReceipt, SubmissionError> {
        self.fallback.rebase(request).await
    }

    async fn push(&self, request: PushRequest) -> Result<PushReceipt, SubmissionError> {
        let PushRequest {
            idempotency_key,
            ledger_id,
            commits,
            blobs,
            governance,
        } = request;

        let (ledger_name, branch) = split_ledger_id(&ledger_id).map_err(|e| {
            SubmissionError::Execution {
                status: 400,
                message: format!("invalid ledger_id: {e}"),
            }
        })?;
        let ref_key = RefKey::new(&ledger_name, &branch);

        let idempotency_cache_key = idempotency_key
            .as_ref()
            .map(|k| IdempotencyCacheKey::new(ledger_id.clone(), k.clone()));

        let envelope = QueuedRequest::Push(QueuedPush {
            commits,
            blobs,
            governance,
        });
        let bytes = envelope
            .to_bytes()
            .map_err(|e| SubmissionError::Execution {
                status: 500,
                message: format!("QueuedRequest encode failed: {e}"),
            })?;
        let request_cid = self
            .fluree
            .content_store(&ledger_id)
            .put(ContentKind::Txn, &bytes)
            .await
            .map_err(|e| SubmissionError::Execution {
                status: 500,
                message: format!("QueuedRequest CAS write failed: {e}"),
            })?;

        let cmd = SmCommand::EnqueueCommand(EnqueueCommandArgs {
            ledger_id: ledger_name,
            branch,
            idempotency: idempotency_cache_key,
            request_cid,
            body_kind: BodyKind::Pushed,
            applied_at_millis: current_millis(),
        });

        let response = self
            .raft
            .client_write(cmd)
            .await
            .map_err(|e| SubmissionError::Execution {
                status: 500,
                message: format!("raft client_write failed: {e}"),
            })?;

        let waiter_rx = match response.data {
            SmResponse::Enqueued { queue_id, .. } | SmResponse::InFlight { queue_id, .. } => {
                self.waiter_map.register(queue_id, ref_key)
            }
            SmResponse::IdempotencyHit { record } => {
                // Push idempotency hit: the original push produced
                // this head. We don't track the `accepted` count or
                // a fresh `IndexingStatus` from the cache record, so
                // surface conservative defaults (0 / Idle) — clients
                // that need an exact `accepted` count on retry can
                // resubmit with a fresh key.
                return Ok(PushReceipt {
                    idempotency_key,
                    ledger: ledger_id,
                    accepted: 0,
                    head_t: record.t,
                    head_id: record.head,
                    indexing: idle_indexing_status(record.t),
                });
            }
            SmResponse::IdempotencyFailed { record } => {
                return Err(SubmissionError::Execution {
                    status: 500,
                    message: format!("cached failure: {:?}", record.reason),
                });
            }
            SmResponse::BodyHashMismatch => return Err(SubmissionError::KeyCollision),
            SmResponse::QueueFull { .. } => return Err(SubmissionError::Overloaded),
            SmResponse::LedgerNotFound { ledger_id } => {
                return Err(SubmissionError::Execution {
                    status: 404,
                    message: format!("ledger not found: {ledger_id}"),
                });
            }
            other => {
                return Err(SubmissionError::Execution {
                    status: 500,
                    message: format!("unexpected Response variant for EnqueueCommand: {other:?}"),
                });
            }
        };

        let outcome = waiter_rx
            .await
            .map_err(|_| SubmissionError::Execution {
                status: 503,
                message: "queue waiter dropped before outcome — leader transition stranded the \
                          push; retry with the same idempotency key"
                    .into(),
            })?;

        match outcome {
            WaiterOutcome::Applied { commit_id, commit_t } => Ok(PushReceipt {
                idempotency_key,
                ledger: ledger_id,
                accepted: 0,
                head_t: commit_t,
                head_id: commit_id,
                indexing: idle_indexing_status(commit_t),
            }),
            WaiterOutcome::Aborted(reason) => Err(submission_error_from_abort(reason)),
        }
    }
}

fn current_millis() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Conservative `IndexingStatus` for paths that don't observe the
/// worker's post-stage novelty / index-t snapshot — used by the
/// idempotency-hit branch and by the queued push receipt until the
/// worker surfaces those fields through the waiter map.
fn idle_indexing_status(commit_t: i64) -> fluree_db_api::IndexingStatus {
    fluree_db_api::IndexingStatus {
        enabled: false,
        needed: false,
        novelty_size: 0,
        index_t: commit_t,
        commit_t,
    }
}

fn submission_error_from_abort(reason: AbortReason) -> SubmissionError {
    match reason {
        AbortReason::BranchDropped => SubmissionError::Execution {
            status: 410,
            message: "branch dropped while submission was queued".into(),
        },
        AbortReason::BranchPurged => SubmissionError::Execution {
            status: 410,
            message: "ledger purged while submission was queued".into(),
        },
        AbortReason::BranchHeadReset => SubmissionError::Execution {
            status: 409,
            message: "branch head reset while submission was queued; retry".into(),
        },
        AbortReason::Poisoned(reason) => SubmissionError::Execution {
            status: 422,
            message: format!("submission poisoned: {reason:?}"),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::state_machine::PoisonReason;

    fn status(err: &SubmissionError) -> u16 {
        match err {
            SubmissionError::Execution { status, .. } => *status,
            other => panic!("expected Execution, got {other:?}"),
        }
    }

    #[test]
    fn branch_dropped_maps_to_410() {
        assert_eq!(
            status(&submission_error_from_abort(AbortReason::BranchDropped)),
            410
        );
    }

    #[test]
    fn branch_purged_maps_to_410() {
        assert_eq!(
            status(&submission_error_from_abort(AbortReason::BranchPurged)),
            410
        );
    }

    #[test]
    fn branch_head_reset_maps_to_409() {
        assert_eq!(
            status(&submission_error_from_abort(AbortReason::BranchHeadReset)),
            409
        );
    }

    #[test]
    fn poisoned_maps_to_422_with_reason_in_message() {
        let err = submission_error_from_abort(AbortReason::Poisoned(
            PoisonReason::BodyMalformed {
                error: "bad turtle".into(),
            },
        ));
        assert_eq!(status(&err), 422);
        match err {
            SubmissionError::Execution { message, .. } => {
                assert!(message.contains("BodyMalformed"), "got: {message}");
                assert!(message.contains("bad turtle"), "got: {message}");
            }
            _ => unreachable!(),
        }
    }
}
