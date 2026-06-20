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

use crate::raft::staged_receipt::AppliedReceipt;
use crate::raft::state_machine::{
    ApplyOutcome, ApplyRecord, BodyKind, Command as SmCommand, EnqueueCommandArgs, PoisonRecord,
    RefKey, Response as SmResponse,
};
use crate::raft::state_machine_adapter::SharedState;
use crate::raft::waiter::{AbortReason, WaiterMap, WaiterOutcome};
use crate::raft::TypeConfig;
use crate::{
    Committer, IdempotencyCacheKey, IdempotencyKey, MergeReceipt, MergeRequest, PushReceipt,
    PushRequest, QueuedMerge, QueuedPush, QueuedRebase, QueuedRequest, QueuedRevert, QueuedTransact,
    RebaseReceipt, RebaseRequest, RevertReceipt, RevertRequest, SubmissionError, SubmissionLookup,
    SubmissionState, TransactionReceipt, TransactionRequest,
};
use async_trait::async_trait;
use fluree_db_api::{CommitReceipt, Fluree};
use fluree_db_core::ledger_id::split_ledger_id;
use fluree_db_core::ContentId;
use fluree_db_core::ContentKind;
use fluree_db_transact::CommitOptsRequest;
use openraft::Raft;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

/// How long a single waiter `await` blocks before the transactor
/// considers the call stranded by a leader transition and either
/// re-issues (idempotent submissions) or errors out (anonymous
/// submissions). Conservative — the typical Raft round-trip is
/// sub-second; this is the budget for "something went wrong."
const DEFAULT_WAIT_TIMEOUT: Duration = Duration::from_secs(8);

/// Number of (propose → register waiter → await) attempts before the
/// transactor gives up on an idempotent submission. Each attempt
/// re-proposes the same `EnqueueCommand`; the state machine's
/// idempotency cache makes repeats safe.
const DEFAULT_MAX_RETRIES: usize = 3;

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
    wait_timeout: Duration,
    max_retries: usize,
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
            wait_timeout: DEFAULT_WAIT_TIMEOUT,
            max_retries: DEFAULT_MAX_RETRIES,
        }
    }

    /// Override the per-attempt waiter timeout (default 8s).
    pub fn with_wait_timeout(mut self, timeout: Duration) -> Self {
        self.wait_timeout = timeout;
        self
    }

    /// Override the retry budget for idempotency-keyed submissions
    /// (default 3 attempts total). Floored at 1 — a budget of zero
    /// would skip the propose loop entirely and surface every
    /// submission as `stranded` without ever talking to Raft, which
    /// is never what callers want.
    pub fn with_max_retries(mut self, max_retries: usize) -> Self {
        self.max_retries = max_retries.max(1);
        self
    }

    /// Submit an `EnqueueCommand`, register a waiter on
    /// `Enqueued`/`InFlight` responses, and await the outcome with a
    /// bounded timeout. On timeout — or on a stale waiter whose
    /// channel got overridden — re-propose the same command iff
    /// `retry_eligible` is true; the state machine's idempotency
    /// cache short-circuits to `IdempotencyHit` once the original
    /// processing completes.
    ///
    /// `retry_eligible` should mirror whether the command carries
    /// an idempotency key: without one, re-proposing would create a
    /// duplicate queue entry rather than hitting the cache.
    ///
    /// When the state machine rejects the propose (`BodyHashMismatch`,
    /// `QueueFull`, `LedgerNotFound`, unexpected variants, or the raft
    /// layer itself errors), the envelope at `request_cid` is orphaned
    /// in CAS — nothing else references it. This function releases it
    /// before returning. `Enqueued`/`InFlight`/`IdempotencyHit`/
    /// `IdempotencyFailed` paths leave the envelope owned by the
    /// queue or idempotency cache; eviction / admin clear releases it
    /// via the state-machine adapter's release channel.
    async fn submit_and_await(
        &self,
        args: EnqueueCommandArgs,
        ref_key: RefKey,
        retry_eligible: bool,
    ) -> Result<SubmissionOutcome, SubmissionError> {
        let request_cid = args.request_cid.clone();
        let full_ledger_id = format!("{}:{}", args.ledger_id, args.branch);
        let cmd = SmCommand::EnqueueCommand(args);
        let attempts_allowed = if retry_eligible { self.max_retries } else { 1 };
        for attempt in 0..attempts_allowed {
            let response = match self.raft.client_write(cmd.clone()).await {
                Ok(response) => response,
                Err(e) => {
                    self.release_envelope(&full_ledger_id, &request_cid).await;
                    return Err(SubmissionError::Execution {
                        status: 500,
                        message: format!("raft client_write failed: {e}"),
                    });
                }
            };
            match response.data {
                SmResponse::Enqueued { queue_id, .. } | SmResponse::InFlight { queue_id, .. } => {
                    let rx = self.waiter_map.register(queue_id, ref_key.clone());
                    match tokio::time::timeout(self.wait_timeout, rx).await {
                        Ok(Ok(outcome)) => return Ok(SubmissionOutcome::Waiter(outcome)),
                        Ok(Err(_recv)) => {
                            // The sender for this queue_id was
                            // dropped — most likely a duplicate
                            // `register` overrode it. Treat the same
                            // as a timeout: retry if eligible, error
                            // otherwise.
                            if attempt + 1 >= attempts_allowed {
                                return Err(self.stranded_error(retry_eligible));
                            }
                        }
                        Err(_elapsed) => {
                            if attempt + 1 >= attempts_allowed {
                                return Err(self.stranded_error(retry_eligible));
                            }
                        }
                    }
                }
                SmResponse::IdempotencyHit { record } => {
                    return Ok(SubmissionOutcome::Cached(record));
                }
                SmResponse::IdempotencyFailed { record } => {
                    return Ok(SubmissionOutcome::CachedFailure(record));
                }
                SmResponse::BodyHashMismatch => {
                    self.release_envelope(&full_ledger_id, &request_cid).await;
                    return Err(SubmissionError::KeyCollision);
                }
                SmResponse::QueueFull { .. } => {
                    self.release_envelope(&full_ledger_id, &request_cid).await;
                    return Err(SubmissionError::Overloaded);
                }
                SmResponse::LedgerNotFound { ledger_id } => {
                    self.release_envelope(&full_ledger_id, &request_cid).await;
                    return Err(SubmissionError::Execution {
                        status: 404,
                        message: format!("ledger not found: {ledger_id}"),
                    });
                }
                SmResponse::LedgerRetracted { ledger_id } => {
                    // Retracted branches are tombstoned at the state
                    // machine; the alias can't be reused without a
                    // purge + re-create. 410 Gone matches the
                    // semantics — the resource existed, the client
                    // shouldn't retry with the same alias.
                    self.release_envelope(&full_ledger_id, &request_cid).await;
                    return Err(SubmissionError::Execution {
                        status: 410,
                        message: format!("ledger retracted: {ledger_id}"),
                    });
                }
                other => {
                    self.release_envelope(&full_ledger_id, &request_cid).await;
                    return Err(SubmissionError::Execution {
                        status: 500,
                        message: format!(
                            "unexpected Response variant for EnqueueCommand: {other:?}"
                        ),
                    });
                }
            }
        }
        Err(self.stranded_error(retry_eligible))
    }

    fn stranded_error(&self, retry_eligible: bool) -> SubmissionError {
        SubmissionError::Execution {
            status: 504,
            message: if retry_eligible {
                "submission retry budget exhausted while waiting for queue resolution".into()
            } else {
                "submission stranded by leader transition; retry with an idempotency key".into()
            },
        }
    }

    /// Hash the envelope's canonical body bytes into a comparison-only
    /// `ContentId`. Distinct from the `request_cid` returned by the
    /// CAS put: the envelope as-stored includes per-request transient
    /// fields (timestamps, tracking) whereas the body CID covers only
    /// what defines the request semantically.
    fn canonical_body_cid(envelope: &QueuedRequest) -> Result<ContentId, SubmissionError> {
        let bytes = envelope
            .canonical_body_bytes()
            .map_err(|e| SubmissionError::Execution {
                status: 500,
                message: format!("canonical body encode failed: {e}"),
            })?;
        Ok(ContentId::new(ContentKind::Txn, &bytes))
    }

    /// Decrement (or remove) an orphaned envelope's CAS entry. The
    /// content store's `release` is idempotent, so a double-release
    /// on the same CID is harmless — log on failure but don't escalate
    /// to the caller, since the submission has already returned its
    /// terminal status.
    async fn release_envelope(&self, ledger_id: &str, request_cid: &ContentId) {
        if let Err(err) = self
            .fluree
            .content_store(ledger_id)
            .release(request_cid)
            .await
        {
            tracing::warn!(
                %ledger_id,
                cid = %request_cid,
                error = %err,
                "failed to release orphaned QueuedRequest envelope"
            );
        }
    }
}

/// Internal result shape `submit_and_await` returns to each
/// `Committer` method. The method matches on the variants to build
/// its operation-specific receipt or error.
enum SubmissionOutcome {
    /// Waiter resolved with an outcome (success or abort).
    Waiter(WaiterOutcome),
    /// Idempotency cache had a recorded success for the key.
    Cached(ApplyRecord),
    /// Idempotency cache had a recorded poison for the key.
    CachedFailure(PoisonRecord),
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

        let (ledger_name, branch) =
            split_ledger_id(&ledger_id).map_err(|e| SubmissionError::Execution {
                status: 400,
                message: format!("invalid ledger_id: {e}"),
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

        let body_cid = Self::canonical_body_cid(&envelope)?;
        let retry_eligible = idempotency_cache_key.is_some();
        let args = EnqueueCommandArgs {
            ledger_id: ledger_name,
            branch,
            idempotency: idempotency_cache_key,
            request_cid,
            body_cid,
            body_kind,
            applied_at_millis: current_millis(),
        };

        match self.submit_and_await(args, ref_key, retry_eligible).await? {
            SubmissionOutcome::Waiter(WaiterOutcome::Applied(receipt)) => {
                transaction_receipt_from(idempotency_key, receipt)
            }
            SubmissionOutcome::Waiter(WaiterOutcome::Aborted(reason)) => {
                Err(submission_error_from_abort(reason))
            }
            SubmissionOutcome::Cached(record) => Ok(TransactionReceipt {
                idempotency_key,
                commit: CommitReceipt {
                    commit_id: record.head,
                    t: record.t,
                    flake_count: 0,
                },
                tally: record.tally.map(Into::into),
            }),
            SubmissionOutcome::CachedFailure(record) => Err(SubmissionError::Execution {
                status: 500,
                message: format!("cached failure: {:?}", record.reason),
            }),
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

        let body_cid = Self::canonical_body_cid(&envelope)?;
        let retry_eligible = idempotency_cache_key.is_some();
        let args = EnqueueCommandArgs {
            ledger_id: ledger_name,
            branch: branch.clone(),
            idempotency: idempotency_cache_key,
            request_cid,
            body_cid,
            body_kind: BodyKind::Revert,
            applied_at_millis: current_millis(),
        };

        match self.submit_and_await(args, ref_key, retry_eligible).await? {
            SubmissionOutcome::Waiter(WaiterOutcome::Applied(receipt)) => {
                revert_receipt_from(idempotency_key, branch, strategy, receipt)
            }
            SubmissionOutcome::Waiter(WaiterOutcome::Aborted(reason)) => {
                Err(submission_error_from_abort(reason))
            }
            SubmissionOutcome::Cached(record) => Ok(RevertReceipt {
                idempotency_key,
                branch,
                reverted_commits: Vec::new(),
                conflict_count: 0,
                strategy,
                new_head_t: record.t,
                new_head_id: record.head,
            }),
            SubmissionOutcome::CachedFailure(record) => Err(SubmissionError::Execution {
                status: 500,
                message: format!("cached failure: {:?}", record.reason),
            }),
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
                let source_ref = state.refs.get(&RefKey::new(&ledger_name, &source_branch));
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

        let body_cid = Self::canonical_body_cid(&envelope)?;
        let retry_eligible = idempotency_cache_key.is_some();
        let args = EnqueueCommandArgs {
            ledger_id: ledger_name,
            branch: target_for_queue.clone(),
            idempotency: idempotency_cache_key,
            request_cid,
            body_cid,
            body_kind: BodyKind::Merge,
            applied_at_millis: current_millis(),
        };

        match self.submit_and_await(args, ref_key, retry_eligible).await? {
            SubmissionOutcome::Waiter(WaiterOutcome::Applied(receipt)) => merge_receipt_from(
                idempotency_key,
                source_branch,
                target_for_queue,
                strategy,
                receipt,
            ),
            SubmissionOutcome::Waiter(WaiterOutcome::Aborted(reason)) => {
                Err(submission_error_from_abort(reason))
            }
            SubmissionOutcome::Cached(record) => Ok(MergeReceipt {
                idempotency_key,
                source: source_branch,
                target: target_for_queue,
                fast_forward: false,
                new_head_t: record.t,
                new_head_id: record.head,
                commits_copied: 0,
                conflict_count: 0,
                strategy,
            }),
            SubmissionOutcome::CachedFailure(record) => Err(SubmissionError::Execution {
                status: 500,
                message: format!("cached failure: {:?}", record.reason),
            }),
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

        let body_cid = Self::canonical_body_cid(&envelope)?;
        let retry_eligible = idempotency_cache_key.is_some();
        let args = EnqueueCommandArgs {
            ledger_id: ledger_name,
            branch: branch.clone(),
            idempotency: idempotency_cache_key,
            request_cid,
            body_cid,
            body_kind: BodyKind::Rebase,
            applied_at_millis: current_millis(),
        };

        match self.submit_and_await(args, ref_key, retry_eligible).await? {
            SubmissionOutcome::Waiter(WaiterOutcome::Applied(receipt)) => {
                rebase_receipt_from(idempotency_key, branch, strategy, receipt)
            }
            SubmissionOutcome::Waiter(WaiterOutcome::Aborted(reason)) => {
                Err(submission_error_from_abort(reason))
            }
            SubmissionOutcome::Cached(record) => Ok(RebaseReceipt {
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
            }),
            SubmissionOutcome::CachedFailure(record) => Err(SubmissionError::Execution {
                status: 500,
                message: format!("cached failure: {:?}", record.reason),
            }),
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

        let (ledger_name, branch) =
            split_ledger_id(&ledger_id).map_err(|e| SubmissionError::Execution {
                status: 400,
                message: format!("invalid ledger_id: {e}"),
            })?;
        let ref_key = RefKey::new(&ledger_name, &branch);

        let idempotency_cache_key = idempotency_key
            .as_ref()
            .map(|k| IdempotencyCacheKey::new(ledger_id.clone(), k.clone()));

        // Upload each commit's bytes to the per-ledger content store
        // and record its CID. The envelope carries only the CIDs;
        // the worker reads the bytes back when staging.
        let content_store = self.fluree.content_store(&ledger_id);
        let mut commit_cids = Vec::with_capacity(commits.len());
        for commit_bytes in &commits {
            let cid = content_store
                .put(ContentKind::Commit, commit_bytes)
                .await
                .map_err(|e| SubmissionError::Execution {
                    status: 500,
                    message: format!("push commit CAS write failed: {e}"),
                })?;
            commit_cids.push(cid);
        }

        let envelope = QueuedRequest::Push(QueuedPush {
            commit_cids,
            blobs,
            governance,
        });
        let bytes = envelope
            .to_bytes()
            .map_err(|e| SubmissionError::Execution {
                status: 500,
                message: format!("QueuedRequest encode failed: {e}"),
            })?;
        let request_cid = content_store
            .put(ContentKind::Txn, &bytes)
            .await
            .map_err(|e| SubmissionError::Execution {
                status: 500,
                message: format!("QueuedRequest CAS write failed: {e}"),
            })?;

        let body_cid = Self::canonical_body_cid(&envelope)?;
        let retry_eligible = idempotency_cache_key.is_some();
        let args = EnqueueCommandArgs {
            ledger_id: ledger_name,
            branch,
            idempotency: idempotency_cache_key,
            request_cid,
            body_cid,
            body_kind: BodyKind::Pushed,
            applied_at_millis: current_millis(),
        };

        match self.submit_and_await(args, ref_key, retry_eligible).await? {
            SubmissionOutcome::Waiter(WaiterOutcome::Applied(receipt)) => {
                push_receipt_from(idempotency_key, ledger_id, receipt)
            }
            SubmissionOutcome::Waiter(WaiterOutcome::Aborted(reason)) => {
                Err(submission_error_from_abort(reason))
            }
            SubmissionOutcome::Cached(record) => {
                // Push idempotency hit: the original push produced
                // this head. The cache record doesn't carry
                // `accepted` or a fresh `IndexingStatus`, so surface
                // conservative defaults — clients that need an
                // exact `accepted` count on retry can resubmit with
                // a fresh key.
                Ok(PushReceipt {
                    idempotency_key,
                    ledger: ledger_id,
                    accepted: 0,
                    head_t: record.t,
                    head_id: record.head,
                    indexing: idle_indexing_status(record.t),
                })
            }
            SubmissionOutcome::CachedFailure(record) => Err(SubmissionError::Execution {
                status: 500,
                message: format!("cached failure: {:?}", record.reason),
            }),
        }
    }
}

/// Status lookup backed by the replicated idempotency state.
///
/// The wrapping [`CachingCommitter`](crate::CachingCommitter) checks
/// its in-process moka cache first — that has the full typed
/// [`OperationReceipt`] the originating node produced and returns
/// [`SubmissionState::Committed`] with `receipt: Some(...)`. Only on
/// a cache miss does the call land here, and that's the path that
/// closes the post-leader-transition gap: the new leader's moka
/// won't carry entries the old leader served, but
/// `state.idempotency` is replicated, so this lookup still returns
/// `Committed` for any submission that completed before the
/// transition.
///
/// `receipt` is `None` on this path — the per-op detail (conflict
/// counts, merge target branch, etc.) isn't replicated. The
/// canonical kit (`commit_id`, `t`, `kind`, `tally`) is enough for
/// clients verifying "did my submission land?"; richer detail can
/// be chased through the commit-log endpoint.
#[async_trait]
impl SubmissionLookup for QueuedTransactor {
    async fn status(&self, ledger_id: &str, key: &IdempotencyKey) -> SubmissionState {
        let cache_key = IdempotencyCacheKey::new(ledger_id, key.clone());
        let state = self.shared_state.read().await;
        match state.idempotency.get(&cache_key) {
            Some(ApplyOutcome::Applied(record)) => committed_from_applied(key.clone(), record),
            Some(ApplyOutcome::Failed(record)) => {
                SubmissionState::Failed(failure_from_poison(record))
            }
            None => SubmissionState::Unknown,
        }
    }
}

fn committed_from_applied(key: IdempotencyKey, record: &ApplyRecord) -> SubmissionState {
    SubmissionState::Committed {
        idempotency_key: Some(key),
        kind: record.body_kind,
        commit_id: record.head.clone(),
        t: record.t,
        tally: record.tally.clone().map(Into::into),
        receipt: None,
    }
}

fn failure_from_poison(record: &PoisonRecord) -> SubmissionError {
    // Failure shape — the replicated `PoisonRecord` only carries the
    // poison reason (e.g. `BodyMalformed`, `StagingFailed`); surface
    // it as an `Execution` error with the reason embedded. Clients
    // that need richer typing can use the body via the commit log;
    // the status route only promises pass/fail + identity.
    SubmissionError::Execution {
        status: 500,
        message: format!("submission failed: {:?}", record.reason),
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
        message: format!("applied receipt mismatch: expected {expected}, got {got:?}"),
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
        tally,
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
        }) => (
            commit_id,
            commit_t,
            reverted_commits,
            conflict_count,
            strategy,
        ),
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
        // Fallback for stranded staged-receipts (former-leader path):
        // we only know the resulting head, not whether this was a
        // fast-forward or an actual replay. Reporting `fast_forward:
        // true` keeps the receipt internally consistent — in a true
        // fast-forward `source_head_id` equals the branch's new head,
        // which is what `commit_id` carries here. The counters at
        // their defaults (0 replayed / skipped / conflicts) line up
        // with that interpretation; the alternative would mislabel
        // the result head as the source head under a non-fast-forward
        // rebase.
        AppliedReceipt::Minimal {
            commit_id,
            commit_t,
        } => Ok(RebaseReceipt {
            idempotency_key,
            branch,
            fast_forward: true,
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
        AbortReason::BranchRetracted => SubmissionError::Execution {
            status: 410,
            message: "branch retracted while submission was queued".into(),
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
        let err = submission_error_from_abort(AbortReason::Poisoned(PoisonReason::BodyMalformed {
            error: "bad turtle".into(),
        }));
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
