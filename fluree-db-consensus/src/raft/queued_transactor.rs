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
use crate::raft::staged_receipt::AppliedReceipt;
use crate::raft::state_machine_adapter::SharedState;
use crate::raft::waiter::{AbortReason, WaiterMap, WaiterOutcome};
use crate::raft::TypeConfig;
use crate::{
    Committer, IdempotencyCacheKey, MergeReceipt, MergeRequest, PushReceipt, PushRequest,
    QueuedMerge, QueuedPush, QueuedRebase, QueuedRequest, QueuedRevert, QueuedTransact,
    RebaseReceipt, RebaseRequest, RevertReceipt, RevertRequest, SubmissionError,
    TransactionReceipt, TransactionRequest,
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
    /// Read-only view of the replicated state machine. Used by
    /// `merge` to resolve a missing `target_branch` to the source's
    /// parent before deciding which per-branch queue the entry rides
    /// on.
    shared_state: SharedState,
}

impl QueuedTransactor {
    pub fn new(
        raft: Arc<Raft<TypeConfig>>,
        fluree: Arc<Fluree>,
        waiter_map: Arc<WaiterMap>,
        shared_state: SharedState,
    ) -> Self {
        Self {
            raft,
            fluree,
            waiter_map,
            shared_state,
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
            WaiterOutcome::Applied(receipt) => Ok(transaction_receipt_from(idempotency_key, receipt)?),
            WaiterOutcome::Aborted(reason) => Err(submission_error_from_abort(reason)),
        }
    }

    async fn revert(&self, request: RevertRequest) -> Result<RevertReceipt, SubmissionError> {
        let RevertRequest {
            idempotency_key,
            ledger_name,
            branch,
            selection,
            strategy,
        } = request;

        let ref_key = RefKey::new(&ledger_name, &branch);
        let full_ledger_id = format!("{ledger_name}:{branch}");

        let idempotency_cache_key = idempotency_key
            .as_ref()
            .map(|k| IdempotencyCacheKey::new(full_ledger_id.clone(), k.clone()));

        let envelope = QueuedRequest::Revert(QueuedRevert {
            selection,
            strategy,
        });
        let bytes = envelope
            .to_bytes()
            .map_err(|e| SubmissionError::Execution {
                status: 500,
                message: format!("QueuedRequest encode failed: {e}"),
            })?;
        let request_cid = self
            .fluree
            .content_store(&full_ledger_id)
            .put(ContentKind::Txn, &bytes)
            .await
            .map_err(|e| SubmissionError::Execution {
                status: 500,
                message: format!("QueuedRequest CAS write failed: {e}"),
            })?;

        let cmd = SmCommand::EnqueueCommand(EnqueueCommandArgs {
            ledger_id: ledger_name,
            branch: branch.clone(),
            idempotency: idempotency_cache_key,
            request_cid,
            body_kind: BodyKind::Revert,
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
                // Revert idempotency hit: the original revert produced
                // this head. The cache record doesn't carry
                // `reverted_commits` / `conflict_count` / strategy
                // detail — surface conservative defaults the client
                // can ignore or refresh with a query against the
                // branch.
                return Ok(RevertReceipt {
                    idempotency_key,
                    branch,
                    reverted_commits: Vec::new(),
                    conflict_count: 0,
                    strategy,
                    new_head_t: record.t,
                    new_head_id: record.head,
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
                          revert; retry with the same idempotency key"
                    .into(),
            })?;

        match outcome {
            WaiterOutcome::Applied(receipt) => Ok(revert_receipt_from(
                idempotency_key,
                branch,
                strategy,
                receipt,
            )?),
            WaiterOutcome::Aborted(reason) => Err(submission_error_from_abort(reason)),
        }
    }

    async fn merge(&self, request: MergeRequest) -> Result<MergeReceipt, SubmissionError> {
        let MergeRequest {
            idempotency_key,
            ledger_name,
            source_branch,
            target_branch,
            strategy,
        } = request;

        // The queue is per-branch, so the entry must be routed to a
        // concrete target up front. When the caller omits
        // `target_branch`, fall back to the source's recorded
        // parent — same semantic the legacy committer's
        // `prepare_merge` used to default to.
        let target_for_queue = match target_branch.clone() {
            Some(t) => t,
            None => {
                let state = self.shared_state.read().await;
                let source_ref =
                    state.refs.get(&RefKey::new(&ledger_name, &source_branch));
                match source_ref.and_then(|r| r.source_branch.clone()) {
                    Some(parent) => parent,
                    None => {
                        return Err(SubmissionError::Execution {
                            status: 400,
                            message: format!(
                                "merge target unresolved: source branch '{source_branch}' has \
                                 no recorded parent; specify target_branch explicitly"
                            ),
                        });
                    }
                }
            }
        };

        let ref_key = RefKey::new(&ledger_name, &target_for_queue);
        let full_ledger_id = format!("{ledger_name}:{target_for_queue}");

        let idempotency_cache_key = idempotency_key
            .as_ref()
            .map(|k| IdempotencyCacheKey::new(full_ledger_id.clone(), k.clone()));

        let envelope = QueuedRequest::Merge(QueuedMerge {
            source_branch: source_branch.clone(),
            target_branch,
            strategy,
        });
        let bytes = envelope
            .to_bytes()
            .map_err(|e| SubmissionError::Execution {
                status: 500,
                message: format!("QueuedRequest encode failed: {e}"),
            })?;
        let request_cid = self
            .fluree
            .content_store(&full_ledger_id)
            .put(ContentKind::Txn, &bytes)
            .await
            .map_err(|e| SubmissionError::Execution {
                status: 500,
                message: format!("QueuedRequest CAS write failed: {e}"),
            })?;

        let cmd = SmCommand::EnqueueCommand(EnqueueCommandArgs {
            ledger_id: ledger_name,
            branch: target_for_queue.clone(),
            idempotency: idempotency_cache_key,
            request_cid,
            body_kind: BodyKind::Merge,
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
                return Ok(MergeReceipt {
                    idempotency_key,
                    source: source_branch,
                    target: target_for_queue,
                    fast_forward: false,
                    new_head_t: record.t,
                    new_head_id: record.head,
                    commits_copied: 0,
                    conflict_count: 0,
                    strategy,
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
                          merge; retry with the same idempotency key"
                    .into(),
            })?;

        match outcome {
            WaiterOutcome::Applied(receipt) => Ok(merge_receipt_from(
                idempotency_key,
                source_branch,
                target_for_queue,
                strategy,
                receipt,
            )?),
            WaiterOutcome::Aborted(reason) => Err(submission_error_from_abort(reason)),
        }
    }

    async fn rebase(&self, request: RebaseRequest) -> Result<RebaseReceipt, SubmissionError> {
        let RebaseRequest {
            idempotency_key,
            ledger_name,
            branch,
            strategy,
        } = request;

        let ref_key = RefKey::new(&ledger_name, &branch);
        let full_ledger_id = format!("{ledger_name}:{branch}");

        let idempotency_cache_key = idempotency_key
            .as_ref()
            .map(|k| IdempotencyCacheKey::new(full_ledger_id.clone(), k.clone()));

        let envelope = QueuedRequest::Rebase(QueuedRebase { strategy });
        let bytes = envelope
            .to_bytes()
            .map_err(|e| SubmissionError::Execution {
                status: 500,
                message: format!("QueuedRequest encode failed: {e}"),
            })?;
        let request_cid = self
            .fluree
            .content_store(&full_ledger_id)
            .put(ContentKind::Txn, &bytes)
            .await
            .map_err(|e| SubmissionError::Execution {
                status: 500,
                message: format!("QueuedRequest CAS write failed: {e}"),
            })?;

        let cmd = SmCommand::EnqueueCommand(EnqueueCommandArgs {
            ledger_id: ledger_name,
            branch: branch.clone(),
            idempotency: idempotency_cache_key,
            request_cid,
            body_kind: BodyKind::Rebase,
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
                return Ok(RebaseReceipt {
                    idempotency_key,
                    branch,
                    fast_forward: false,
                    replayed: 0,
                    skipped: 0,
                    conflicts: 0,
                    failures: 0,
                    total_commits: 0,
                    source_head_t: record.t,
                    source_head_id: record.head,
                    strategy,
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
                          rebase; retry with the same idempotency key"
                    .into(),
            })?;

        match outcome {
            WaiterOutcome::Applied(receipt) => Ok(rebase_receipt_from(
                idempotency_key,
                branch,
                strategy,
                receipt,
            )?),
            WaiterOutcome::Aborted(reason) => Err(submission_error_from_abort(reason)),
        }
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
            WaiterOutcome::Applied(receipt) => Ok(push_receipt_from(
                idempotency_key,
                ledger_id,
                receipt,
            )?),
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

/// Error surfaced when the adapter delivers a receipt variant whose
/// operation type doesn't match the one the transactor submitted.
/// Should never happen — same queue_id can't belong to two
/// operations — so it's a state machine / wiring bug.
fn variant_mismatch(expected: &'static str, got: &AppliedReceipt) -> SubmissionError {
    SubmissionError::Execution {
        status: 500,
        message: format!(
            "applied receipt mismatch: expected {expected}, got {got:?}"
        ),
    }
}

fn transaction_receipt_from(
    idempotency_key: Option<crate::IdempotencyKey>,
    receipt: AppliedReceipt,
) -> Result<TransactionReceipt, SubmissionError> {
    use crate::raft::staged_receipt::TransactApplied;
    let (commit_id, commit_t, tally) = match receipt {
        AppliedReceipt::Transact(TransactApplied {
            commit_id,
            commit_t,
            tally,
        }) => (commit_id, commit_t, tally),
        AppliedReceipt::Minimal {
            commit_id,
            commit_t,
        } => (commit_id, commit_t, None),
        other => return Err(variant_mismatch("Transact", &other)),
    };
    Ok(TransactionReceipt {
        idempotency_key,
        commit: CommitReceipt {
            commit_id,
            t: commit_t,
            flake_count: 0,
        },
        tally: tally.map(Into::into),
    })
}

fn push_receipt_from(
    idempotency_key: Option<crate::IdempotencyKey>,
    ledger_id: String,
    receipt: AppliedReceipt,
) -> Result<PushReceipt, SubmissionError> {
    use crate::raft::staged_receipt::PushApplied;
    let (commit_id, commit_t, accepted, indexing) = match receipt {
        AppliedReceipt::Push(PushApplied {
            commit_id,
            commit_t,
            accepted,
            indexing,
        }) => (commit_id, commit_t, accepted, indexing),
        AppliedReceipt::Minimal {
            commit_id,
            commit_t,
        } => (commit_id, commit_t, 0, idle_indexing_status(commit_t)),
        other => return Err(variant_mismatch("Push", &other)),
    };
    Ok(PushReceipt {
        idempotency_key,
        ledger: ledger_id,
        accepted,
        head_t: commit_t,
        head_id: commit_id,
        indexing,
    })
}

fn revert_receipt_from(
    idempotency_key: Option<crate::IdempotencyKey>,
    branch: String,
    strategy: fluree_db_api::ConflictStrategy,
    receipt: AppliedReceipt,
) -> Result<RevertReceipt, SubmissionError> {
    use crate::raft::staged_receipt::RevertApplied;
    let (commit_id, commit_t, reverted_commits, conflict_count, strategy_out) = match receipt {
        AppliedReceipt::Revert(RevertApplied {
            commit_id,
            commit_t,
            reverted_commits,
            conflict_count,
            strategy,
        }) => (commit_id, commit_t, reverted_commits, conflict_count, strategy),
        AppliedReceipt::Minimal {
            commit_id,
            commit_t,
        } => (commit_id, commit_t, Vec::new(), 0, strategy),
        other => return Err(variant_mismatch("Revert", &other)),
    };
    Ok(RevertReceipt {
        idempotency_key,
        branch,
        reverted_commits,
        conflict_count,
        strategy: strategy_out,
        new_head_t: commit_t,
        new_head_id: commit_id,
    })
}

fn merge_receipt_from(
    idempotency_key: Option<crate::IdempotencyKey>,
    source: String,
    target: String,
    strategy: fluree_db_api::ConflictStrategy,
    receipt: AppliedReceipt,
) -> Result<MergeReceipt, SubmissionError> {
    use crate::raft::staged_receipt::MergeApplied;
    let (commit_id, commit_t, fast_forward, commits_copied, conflict_count, strategy_out) =
        match receipt {
            AppliedReceipt::Merge(MergeApplied {
                commit_id,
                commit_t,
                fast_forward,
                commits_copied,
                conflict_count,
                strategy,
            }) => (
                commit_id,
                commit_t,
                fast_forward,
                commits_copied,
                conflict_count,
                strategy,
            ),
            AppliedReceipt::Minimal {
                commit_id,
                commit_t,
            } => (commit_id, commit_t, false, 0, 0, strategy),
            other => return Err(variant_mismatch("Merge", &other)),
        };
    Ok(MergeReceipt {
        idempotency_key,
        source,
        target,
        fast_forward,
        new_head_t: commit_t,
        new_head_id: commit_id,
        commits_copied,
        conflict_count,
        strategy: strategy_out,
    })
}

fn rebase_receipt_from(
    idempotency_key: Option<crate::IdempotencyKey>,
    branch: String,
    strategy: fluree_db_api::ConflictStrategy,
    receipt: AppliedReceipt,
) -> Result<RebaseReceipt, SubmissionError> {
    use crate::raft::staged_receipt::RebaseApplied;
    match receipt {
        AppliedReceipt::Rebase(RebaseApplied {
            commit_id: _,
            commit_t: _,
            fast_forward,
            replayed,
            skipped,
            conflicts,
            failures,
            total_commits,
            source_head_t,
            source_head_id,
            strategy,
        }) => Ok(RebaseReceipt {
            idempotency_key,
            branch,
            fast_forward,
            replayed,
            skipped,
            conflicts,
            failures,
            total_commits,
            source_head_t,
            source_head_id,
            strategy,
        }),
        AppliedReceipt::Minimal {
            commit_id,
            commit_t,
        } => Ok(RebaseReceipt {
            idempotency_key,
            branch,
            fast_forward: false,
            replayed: 0,
            skipped: 0,
            conflicts: 0,
            failures: 0,
            total_commits: 0,
            source_head_t: commit_t,
            source_head_id: commit_id,
            strategy,
        }),
        other => Err(variant_mismatch("Rebase", &other)),
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
