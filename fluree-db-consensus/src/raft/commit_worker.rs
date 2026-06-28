//! Per-branch staging tasks that drain [`NameServiceState`] queues
//! into head advances.
//!
//! The state machine appends `QueueEntry` rows to per-branch FIFOs as
//! [`Command::EnqueueCommand`](crate::raft::state_machine::Command::EnqueueCommand)
//! applies. One [`Worker`] runs per active branch on whichever node
//! the rendezvous-hash owner resolves to; each polls its branch's
//! queue front, restages the commit locally, writes the commit blob
//! to shared CAS, and publishes the head advance through
//! [`CommitPublisher::publish_commit`] — which proposes
//! [`Command::ApplyHead`](crate::raft::state_machine::Command::ApplyHead).
//! The entry pops and the head advances on that apply.
//!
//! [`WorkerSupervisor`] runs at node lifetime on every node. Each
//! tick it computes the desired set (branches in
//! [`NameServiceState::queues`] whose rendezvous owner is this node)
//! and reconciles its running workers to match — spawning new ones,
//! aborting reassigned ones. A cancellation token drives graceful
//! shutdown; the supervisor exits its loop, aborts every per-branch
//! worker, then returns.
//!
//! Scope cuts for v1 (tracked in `docs/design/raft-command-queue.md`):
//! - No retry budget. Any staging failure poisons the entry.
//! - [`BodyKind::Pushed`] handling is deferred; the worker poisons
//!   any pushed entry until that path lands.
//! - Idle branches are not reaped; workers run until ownership
//!   moves or the supervisor shuts down.
//! - Token-bearing fields in [`crate::QueuedRequest::governance`]
//!   travel verbatim; redaction is future work.

use async_trait::async_trait;

use crate::local::build_policy_context;
use crate::raft::ownership::owner;
use crate::raft::staged_receipt::{
    AppliedReceipt, MergeApplied, PushApplied, RebaseApplied, RevertApplied, StagedReceiptMap,
    StashGuard, TransactApplied,
};
use crate::raft::state_machine::{BodyKind, PoisonReason, QueueEntry, RefKey};
use crate::raft::state_machine_adapter::SharedState;
use crate::raft::{NodeId, TypeConfig};
use crate::{
    QueuedMerge, QueuedPush, QueuedRebase, QueuedRequest, QueuedRevert, QueuedTransact,
    SubmissionError, TransactionBody,
};
use fluree_db_api::{
    ApiError, Base64Bytes, Fluree, PushCommitsRequest, RefreshOpts, StagedMerge, StagedPush,
    StagedRebase, StagedRevert,
};
use fluree_db_core::ContentId;
use fluree_db_ledger::IndexConfig;
use fluree_db_nameservice::{CommitPublisher, NameServiceError};
use futures::FutureExt;
use openraft::Raft;
use std::collections::{HashMap, HashSet};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

/// [`Worker`] poll interval when the previous tick found nothing on
/// its branch queue.
const POLL_INTERVAL: Duration = Duration::from_millis(50);

/// [`WorkerSupervisor`] scan interval for branches in
/// [`NameServiceState::queues`] without a running worker. Bounds
/// the time-to-spawn for a freshly-seen branch.
const SUPERVISOR_POLL_INTERVAL: Duration = Duration::from_millis(250);

/// [`Worker`] backoff after a Raft propose failure before the next
/// retry.
const RAFT_BACKOFF: Duration = Duration::from_millis(250);

/// Max staging attempts before [`process_entry`] gives up and
/// proposes a poison. Only [`PoisonReason::StagingFailed`] is
/// retried; the other variants are deterministic.
const MAX_STAGE_ATTEMPTS: u32 = 3;

/// First-attempt backoff between staging retries; doubles per
/// subsequent attempt. With [`MAX_STAGE_ATTEMPTS`] = 3, the worst-
/// case wait before poisoning is `100 + 200 = 300ms`.
const STAGE_RETRY_BASE_BACKOFF: Duration = Duration::from_millis(100);

/// Cross-node propose path for [`SmCommand::PoisonQueueEntry`].
///
/// `Worker`s running on a follower can't propose to raft directly —
/// `client_write` returns `ForwardToLeader` for non-leader proposes
/// — so a deterministic poison (e.g. [`PoisonReason::BodyMalformed`])
/// would otherwise bounce forever and head-of-line-block the
/// branch. Implementations dispatch by local-leader status, mirroring
/// how [`CommitPublisher::publish_commit`] routes `ApplyHead` via
/// `RaftNameService::publish_commit_via_leader` on a follower.
#[async_trait]
pub trait QueuePoisonPublisher: Send + Sync {
    /// Propose `PoisonQueueEntry` for the given queue entry. On a
    /// follower, ferries the args to the current leader's
    /// `apply_queue_poison` endpoint; on the leader, calls
    /// `client_write` directly. Either way returns `Ok(())` once
    /// the poison is durably proposed.
    async fn poison_queue_entry(
        &self,
        ref_key: &RefKey,
        queue_id: u64,
        reason: PoisonReason,
    ) -> Result<(), QueuePoisonError>;
}

/// Failure modes for [`QueuePoisonPublisher::poison_queue_entry`].
///
/// All variants are surfaced to the worker's outer `WorkerError::Raft`
/// path, which logs and backs off. Distinguishing them at this layer
/// keeps the cause explicit in logs without forcing the worker to
/// switch on opaque strings.
#[derive(Debug, Error)]
pub enum QueuePoisonError {
    /// Local `client_write` rejected the propose (leader-side path),
    /// or the leader's `client_write` rejected it after the
    /// follower forwarded.
    #[error("raft propose failed: {0}")]
    RaftPropose(String),
    /// HTTP-level failure ferrying the poison to the leader: connect
    /// refused, timeout, leader address missing from membership, etc.
    #[error("apply_queue_poison transport: {0}")]
    Transport(String),
    /// Leader returned a non-success HTTP status (transport-layer
    /// failure on the leader side — distinct from a structured
    /// `Result::Err` body returned with 200).
    #[error("apply_queue_poison returned HTTP {0}")]
    HttpStatus(reqwest::StatusCode),
    /// Postcard encode/decode of the request or response failed.
    #[error("apply_queue_poison codec: {0}")]
    Codec(String),
    /// No leader currently elected — `current_leader()` returned
    /// `None`. The worker backs off and retries on the next loop tick.
    #[error("no leader currently elected; poison deferred")]
    NoLeader,
}

/// Handles a [`Worker`] uses to commit its staged work.
///
/// - [`commits`](Self::commits) proposes `ApplyHead` after a
///   successful stage. Dispatches via `client_write` on the leader
///   and via the `apply_staged_commit` HTTP endpoint on a follower.
/// - [`poison`](Self::poison) proposes `PoisonQueueEntry` for
///   deterministic failures, with the same dispatch via the
///   `apply_queue_poison` endpoint.
/// - [`staged_receipts`](Self::staged_receipts) is the per-leader
///   stash the `apply_staged_commit` path reads typed receipt detail
///   from. The worker writes to it before proposing through
///   [`commits`](Self::commits); on propose failure the worker takes
///   the entry back out; on success the state-machine adapter takes
///   during waiter resolution.
#[derive(Clone)]
pub struct PublishingChannel {
    pub commits: Arc<dyn CommitPublisher>,
    pub poison: Arc<dyn QueuePoisonPublisher>,
    pub staged_receipts: Arc<StagedReceiptMap>,
}

/// Handles a [`Worker`] uses to stage a commit locally.
///
/// - [`fluree`](Self::fluree) owns content storage, the per-branch
///   write guards, and the staging machinery (`stage`, `prepare_*`,
///   `content_store`).
/// - [`index_config`](Self::index_config) shapes what staging
///   produces. Consumed by `fluree`'s commit path.
#[derive(Clone)]
pub struct StagingContext {
    pub fluree: Arc<Fluree>,
    pub index_config: IndexConfig,
}

/// Per-branch staging task.
///
/// Drains a single branch's queue: peeks the front, stages the
/// commit, writes the commit blob, publishes the head advance through
/// [`CommitPublisher::publish_commit`], and retires the entry. One
/// worker runs per active [`RefKey`] on whichever node the rendezvous
/// owner resolves to; cross-branch concurrency is the supervisor's
/// responsibility.
///
/// Head advances flow through [`PublishingChannel::commits`] — a
/// Raft deployment supplies a
/// [`RaftNameService`](crate::raft::nameservice::RaftNameService)
/// here, which proposes `ApplyHead` against the current per-branch
/// queue front (forwarding to the leader when this worker runs on a
/// follower). Deterministic failures route through
/// [`PublishingChannel::poison`] with the same dispatch.
#[derive(Clone)]
pub struct Worker {
    ref_key: RefKey,
    shared_state: SharedState,
    publishing: PublishingChannel,
    staging: StagingContext,
}

impl Worker {
    fn new(
        ref_key: RefKey,
        shared_state: SharedState,
        publishing: PublishingChannel,
        staging: StagingContext,
    ) -> Self {
        Self {
            ref_key,
            shared_state,
            publishing,
            staging,
        }
    }

    /// Process a single queue entry end-to-end.
    ///
    /// Reads the [`QueuedRequest`] envelope from CAS, stages the
    /// commit, writes the commit blob, and publishes the head
    /// advance through the [`CommitPublisher`]. Transient staging
    /// failures (`PoisonReason::StagingFailed`) retry with
    /// exponential backoff up to [`MAX_STAGE_ATTEMPTS`] times before
    /// poisoning — the other `PoisonReason` variants are
    /// deterministic (`BodyMalformed`, `PolicyViolation`, etc.) so
    /// retrying them just burns a worker round. Between retries
    /// the local Fluree cache is `refresh()`ed against the durable
    /// nameservice head so a conflict rooted in stale state (e.g.
    /// a namespace allocation this node missed because it took
    /// leadership mid-write) heals instead of producing the same
    /// failure forever. Returns once the entry has reached a
    /// terminal state in the queue (advanced or poisoned).
    async fn process_entry(&self, entry: QueueEntry) -> Result<(), WorkerError> {
        let mut attempt: u32 = 0;
        loop {
            attempt += 1;
            match self.try_advance_head(&entry).await {
                Ok(()) => return Ok(()),
                Err(WorkerError::Transient(error)) => {
                    if attempt < MAX_STAGE_ATTEMPTS {
                        let backoff = STAGE_RETRY_BASE_BACKOFF * (1u32 << (attempt - 1));
                        debug!(
                            queue_id = entry.queue_id,
                            attempt,
                            backoff_ms = backoff.as_millis() as u64,
                            %error,
                            "transient staging failure, retrying"
                        );
                        tokio::time::sleep(backoff).await;
                        // Reconcile cached ledger state with the
                        // durable head before re-staging — without
                        // this, conflicts rooted in stale local
                        // state (`NamespaceConflict` after a
                        // leader transition is the canonical case)
                        // reproduce on every attempt and the entry
                        // poisons.
                        let ledger_id = self.ref_key.ledger_id();
                        if let Err(refresh_err) = self
                            .staging
                            .fluree
                            .refresh(&ledger_id, RefreshOpts::default())
                            .await
                        {
                            warn!(
                                queue_id = entry.queue_id,
                                attempt,
                                error = %refresh_err,
                                "refresh during staging retry failed; retrying anyway"
                            );
                        }
                        continue;
                    }
                    warn!(
                        queue_id = entry.queue_id,
                        attempts = attempt,
                        %error,
                        "exhausted staging retries, poisoning entry"
                    );
                    return self
                        .propose_poison(
                            entry.queue_id,
                            PoisonReason::StagingFailed {
                                error,
                                attempts: attempt,
                            },
                        )
                        .await;
                }
                Err(WorkerError::Stage(reason)) => {
                    return self.propose_poison(entry.queue_id, *reason).await;
                }
                Err(other) => return Err(other),
            }
        }
    }

    async fn try_advance_head(&self, entry: &QueueEntry) -> Result<(), WorkerError> {
        let envelope = self.load_envelope(entry).await?;
        // The state machine doesn't introspect the envelope at enqueue
        // time, so the inline discriminator could disagree with what
        // the leader actually wrote. Bail explicitly rather than
        // process under a kind the queue didn't declare.
        check_envelope_kind(entry.body_kind, &envelope)?;

        let StagedOutcome { receipt, install } = match envelope {
            QueuedRequest::Transact(transact) => self.stage_and_persist(*transact).await?,
            QueuedRequest::Push(push) => self.process_push(*push).await?,
            QueuedRequest::Revert(revert) => self.process_revert(revert).await?,
            QueuedRequest::Merge(merge) => self.process_merge(merge).await?,
            QueuedRequest::Rebase(rebase) => self.process_rebase(rebase).await?,
        };
        let commit_id = receipt.commit_id().clone();
        let commit_t = receipt.commit_t();

        // Stash the typed receipt through `StashGuard` so an abort
        // mid-publish (ownership flap during the propose await)
        // doesn't strand the receipt. The error path can't blindly
        // drop `install` — `publish_head_advance` may return `Err`
        // (lost response, stepped-down leader after local apply,
        // post-apply fatal) for an `ApplyHead` that actually
        // committed and was applied on this node. In that case the
        // replicated `state.refs` reflects our advance but dropping
        // the install leaves the local Fluree cache at the
        // pre-stage head → silent stale reads. So on error we
        // reconcile against the replicated head and finalize the
        // install only if our commit landed.
        let _stash = StashGuard::stash(
            &self.publishing.staged_receipts,
            entry.queue_id,
            self.ref_key.clone(),
            receipt,
        );
        match self.publish_head_advance(commit_id.clone(), commit_t).await {
            Ok(()) => {
                if let Some(install) = install {
                    self.finalize_after_publish(entry.queue_id, commit_t, install)
                        .await;
                }
                Ok(())
            }
            Err(err) => {
                let landed = self.commit_replicated(&commit_id).await;
                if landed {
                    // Apply landed via another path (idempotency hit
                    // or a sibling worker's race). Finalize the local
                    // cache so this node catches up with the
                    // replicated head.
                    if let Some(install) = install {
                        self.finalize_after_publish(entry.queue_id, commit_t, install)
                            .await;
                    }
                    Ok(())
                } else if matches!(err, WorkerError::Stale(_)) {
                    // Queue front moved past our queue_id (admin
                    // clear or sibling worker race that landed a
                    // different commit). Our commit didn't land and
                    // never will — drop the local install
                    // (write_guard releases without `replace`, the
                    // Fluree handle stays at its pre-stage head) and
                    // let the run loop pick up the new queue front.
                    Ok(())
                } else {
                    // Apply didn't land. Drop `install` (write_guard
                    // releases without calling `replace`, so this
                    // node's Fluree handle stays at its pre-stage
                    // head — same as every other node) and propagate
                    // the error for the outer retry/poison logic.
                    Err(err)
                }
            }
        }
        // `_stash`'s Drop fires on function return (or on abort
        // mid-await) — idempotent `take` cleans the slot regardless
        // of which path won.
    }

    /// Finalize the local cache after a confirmed publish, treating
    /// any failure as non-fatal. The commit is already replicated in
    /// the state machine — the replicated head advance is the source
    /// of truth — so a transient failure to install the staged state
    /// locally just leaves the cache momentarily stale; the per-node
    /// commit-event listener reconciles it on the next apply. Crucially,
    /// we do NOT return the error: re-running the staging loop after
    /// a successful publish would call `publish_commit` a second time,
    /// and the publisher samples the queue front at that moment — which
    /// has already advanced past our entry, so the second `ApplyHead`
    /// would consume the NEXT queue entry while carrying our commit
    /// body. That race produces a duplicate of our commit and silently
    /// drops the next entry.
    async fn finalize_after_publish(
        &self,
        queue_id: u64,
        commit_t: i64,
        install: StagedStateInstall,
    ) {
        if let Err(err) = self.finalize_local_state(install).await {
            warn!(
                queue_id,
                commit_t,
                error = %err,
                "finalize_local_state failed after publish; commit is replicated, \
                 local cache will catch up via event listener"
            );
        }
    }

    /// Read the replicated state to see whether `commit_id` is the
    /// current head for this worker's [`RefKey`]. Used by
    /// [`Self::try_advance_head`] to disambiguate a publish error
    /// that may have committed anyway (lost response, post-apply
    /// step-down) from one that genuinely failed.
    ///
    /// Only an exact head match is treated as "landed." A different
    /// head — even at a higher `t` — means something other than our
    /// install reached consensus, so dropping the install is the
    /// correct outcome (the next drain pass will re-derive state
    /// from the replicated head).
    async fn commit_replicated(&self, commit_id: &ContentId) -> bool {
        let state = self.shared_state.read().await;
        state
            .refs
            .get(&self.ref_key)
            .is_some_and(|entry| &entry.head == commit_id)
    }

    /// Install staged ledger state through the held write guard
    /// after the head advance has been replicated. Called only on
    /// the publish-success path so the local cache never gets ahead
    /// of consensus.
    async fn finalize_local_state(&self, install: StagedStateInstall) -> Result<(), WorkerError> {
        let StagedStateInstall {
            write_guard,
            new_state,
            commit_t,
        } = install;
        let needs_reindex = new_state.should_reindex(&self.staging.index_config);
        self.staging
            .fluree
            .finalize_commit(write_guard, new_state, commit_t, needs_reindex)
            .await
            .map_err(api_error_to_stage)
    }

    async fn load_envelope(&self, entry: &QueueEntry) -> Result<QueuedRequest, WorkerError> {
        let ledger_id = self.ref_key.ledger_id();
        let bytes = self
            .staging
            .fluree
            .content_store(&ledger_id)
            .get(&entry.request_cid)
            .await
            .map_err(|e| WorkerError::Transient(format!("CAS read of request_cid failed: {e}")))?;
        QueuedRequest::from_bytes(&bytes).map_err(|e| {
            stage(PoisonReason::BodyMalformed {
                error: format!("QueuedRequest decode failed: {e}"),
            })
        })
    }

    /// Resolve the ledger handle, dispatch on body kind, stage the
    /// commit, and write the commit blob to CAS. Returns a
    /// [`StagedOutcome`] carrying the typed receipt plus the
    /// deferred local-state install — `try_advance_head` only
    /// finalizes the install after the publish succeeds.
    async fn stage_and_persist(
        &self,
        transact: QueuedTransact,
    ) -> Result<StagedOutcome, WorkerError> {
        let QueuedTransact {
            body,
            txn_opts,
            commit_opts,
            tracking,
            governance,
        } = transact;

        let ledger_id = self.ref_key.ledger_id();
        let ledger_manager = self
            .staging
            .fluree
            .ledger_manager()
            .ok_or_else(|| stage_failure("LedgerManager is not configured on Fluree"))?;
        let ledger_handle = ledger_manager
            .get_or_load(&ledger_id)
            .await
            .map_err(|e| stage_failure(&format!("ledger load failed: {e}")))?;

        let policy_ctx = build_policy_context(&ledger_handle, &governance)
            .await
            .map_err(submission_to_stage)?;

        let staged = self.staging.fluree.stage(&ledger_handle);
        let staged = match &body {
            TransactionBody::JsonLdInsert(json) => staged.insert(json),
            TransactionBody::JsonLdUpsert(json) => staged.upsert(json),
            TransactionBody::JsonLdUpdate(json) => staged.update(json),
            TransactionBody::TurtleInsert(text) => staged.insert_turtle(text.as_str()),
            TransactionBody::TurtleUpsert(text) | TransactionBody::TrigUpsert(text) => {
                staged.upsert_turtle(text.as_str())
            }
            TransactionBody::Sparql(query) => staged.sparql_update(query.as_str()),
        };

        let mut builder = staged
            .txn_opts(txn_opts)
            .commit_opts(commit_opts.into_commit_opts())
            .index_config(self.staging.index_config.clone());
        if let Some(tracking) = tracking {
            builder = builder.tracking(tracking);
        }
        if let Some(policy) = policy_ctx {
            builder = builder.policy(policy);
        }

        let (write_guard, staged_commit) = builder
            .build_commit()
            .await
            .map_err(|e| stage_failure(&format!("build_commit failed: {e}")))?;

        let commit_cid = staged_commit.commit.id.clone().ok_or_else(|| {
            stage(PoisonReason::WorkerPanic {
                message: "build_commit produced staged commit without commit.id".into(),
            })
        })?;
        let commit_t = staged_commit.commit.t;
        // Pull tally out before `finalize_state` consumes the staged
        // commit. The receipt the worker hands back through the side
        // channel carries it so clients that requested tracking get
        // the same fuel/time/policy snapshot the staging path
        // produced.
        let tally = staged_commit.tally.clone();

        let content_store = self.staging.fluree.content_store(&ledger_id);
        content_store
            .put_with_id(&commit_cid, &staged_commit.commit_bytes)
            .await
            .map_err(|e| stage_failure(&format!("commit blob write failed: {e}")))?;
        for (cid, bytes) in &staged_commit.referenced_bytes {
            content_store
                .put_with_id(cid, bytes)
                .await
                .map_err(|e| stage_failure(&format!("referenced blob write failed: {e}")))?;
        }

        // Derive post-commit state but do NOT call finalize_commit
        // here — local install runs after the publish confirms the
        // head landed in the cluster. If publish fails, the
        // write_guard drops without `replace`, leaving the local
        // Fluree handle at its pre-stage head.
        let (receipt, new_state) = staged_commit
            .finalize_state()
            .map_err(|e| stage_failure(&format!("finalize_state failed: {e}")))?;

        Ok(StagedOutcome {
            receipt: AppliedReceipt::Transact(TransactApplied {
                commit_id: commit_cid,
                commit_t,
                flake_count: receipt.flake_count,
                tally,
            }),
            install: Some(StagedStateInstall {
                write_guard,
                new_state,
                commit_t,
            }),
        })
    }

    /// Re-stage the revert worker-side, write the inverse commit
    /// blob to CAS, finalize local state, and return the new head
    /// identity. NoOp short-circuits (the conflict strategy dropped
    /// every reverted flake) republish the existing head so the
    /// queue entry completes cleanly without advancing — `ApplyHead`
    /// against the same head is a stale-write that the state machine
    /// surfaces via `QueueDesync::WrongFront` only if another
    /// transactor jumped ahead, which is exactly the race the queue
    /// already serializes against.
    async fn process_revert(&self, revert: QueuedRevert) -> Result<StagedOutcome, WorkerError> {
        use fluree_db_api::GuardedStagedCommit;

        let QueuedRevert {
            selection,
            strategy,
        } = revert;

        let ledger_name = self.ref_key.ledger_name.clone();
        let branch = self.ref_key.branch.clone();
        let StagedRevert {
            reverted_commits,
            conflict_count,
            strategy: applied_strategy,
            rollback_snapshot: _,
            current_head_t,
            current_head_id,
            commit,
            ..
        } = self
            .staging
            .fluree
            .prepare_revert(&ledger_name, &branch, selection, strategy)
            .await
            .map_err(|e| stage_failure(&format!("prepare_revert failed: {e}")))?;

        let Some(GuardedStagedCommit {
            write_guard,
            staged: staged_commit,
        }) = commit
        else {
            // NoOp short-circuit: nothing changes. Return the current
            // head with `install: None` so we don't touch the local
            // Fluree state and don't trigger a re-finalize.
            return Ok(StagedOutcome {
                receipt: AppliedReceipt::Revert(RevertApplied {
                    commit_id: current_head_id,
                    commit_t: current_head_t,
                    reverted_commits,
                    conflict_count,
                    strategy: applied_strategy,
                }),
                install: None,
            });
        };

        let commit_cid = staged_commit.commit.id.clone().ok_or_else(|| {
            stage(PoisonReason::WorkerPanic {
                message: "prepare_revert produced staged commit without commit.id".into(),
            })
        })?;
        let commit_t = staged_commit.commit.t;

        let ledger_id = self.ref_key.ledger_id();
        let content_store = self.staging.fluree.content_store(&ledger_id);
        content_store
            .put_with_id(&commit_cid, &staged_commit.commit_bytes)
            .await
            .map_err(|e| stage_failure(&format!("revert commit blob write failed: {e}")))?;
        for (cid, bytes) in &staged_commit.referenced_bytes {
            content_store
                .put_with_id(cid, bytes)
                .await
                .map_err(|e| stage_failure(&format!("revert referenced blob write failed: {e}")))?;
        }

        let (_receipt, new_state) = staged_commit
            .finalize_state()
            .map_err(|e| stage_failure(&format!("revert finalize_state failed: {e}")))?;

        Ok(StagedOutcome {
            receipt: AppliedReceipt::Revert(RevertApplied {
                commit_id: commit_cid,
                commit_t,
                reverted_commits,
                conflict_count,
                strategy: applied_strategy,
            }),
            install: write_guard.map(|guard| StagedStateInstall {
                write_guard: guard,
                new_state,
                commit_t,
            }),
        })
    }

    /// Decode the queued push, hand it to `Fluree::prepare_push` for
    /// validation + CAS persistence + local state derivation, then
    /// finalize through the held write guard so this node's cache
    /// catches up with the head we're about to publish.
    async fn process_push(&self, push: QueuedPush) -> Result<StagedOutcome, WorkerError> {
        let QueuedPush {
            commit_cids,
            blobs,
            governance,
        } = push;
        let ledger_id = self.ref_key.ledger_id();
        let content_store = self.staging.fluree.content_store(&ledger_id);
        // Read each commit's bytes back from CAS by CID. The
        // transactor wrote them before enqueueing, so a definitive
        // `NotFound` means the blob has been GC'd (or never landed)
        // — retrying won't recover it, so poison immediately as a
        // malformed body. Any other error is a transport / backend
        // hiccup; raise as `Transient` so the retry/backoff loop in
        // `process_entry` heals it.
        let mut commits = Vec::with_capacity(commit_cids.len());
        for cid in &commit_cids {
            let bytes = content_store.get(cid).await.map_err(|e| {
                if matches!(e, fluree_db_core::Error::NotFound(_)) {
                    stage(PoisonReason::BodyMalformed {
                        error: format!("push commit {cid} missing from CAS: {e}"),
                    })
                } else {
                    WorkerError::Transient(format!("push commit {cid} CAS read failed: {e}"))
                }
            })?;
            commits.push(Base64Bytes(bytes));
        }
        let payload = PushCommitsRequest {
            commits,
            blobs: blobs
                .into_iter()
                .map(|(k, v)| (k, Base64Bytes(v)))
                .collect(),
        };
        let StagedPush {
            accepted,
            new_head_id,
            new_head_t,
            write_guard,
            final_state,
            indexing_status,
            ..
        } = self
            .staging
            .fluree
            .prepare_push(&ledger_id, payload, &governance, &self.staging.index_config)
            .await
            .map_err(|e| stage_failure(&format!("prepare_push failed: {e}")))?;

        Ok(StagedOutcome {
            receipt: AppliedReceipt::Push(PushApplied {
                commit_id: new_head_id,
                commit_t: new_head_t,
                accepted,
                indexing: indexing_status,
            }),
            install: Some(StagedStateInstall {
                write_guard,
                new_state: final_state,
                commit_t: new_head_t,
            }),
        })
    }

    /// Re-stage the merge worker-side. Fast-forward merges have no
    /// new commit body (the source's commits are already in the
    /// target namespace from the build phase); general merges write
    /// the merge commit blob. Either path produces a `(new_head_id,
    /// new_head_t)` pair for the publisher.
    async fn process_merge(&self, merge: QueuedMerge) -> Result<StagedOutcome, WorkerError> {
        use fluree_db_api::GuardedStagedCommit;

        let QueuedMerge {
            source_branch,
            target_branch,
            strategy,
        } = merge;
        let ledger_name = self.ref_key.ledger_name.clone();
        let StagedMerge {
            target,
            target_id,
            fast_forward,
            conflict_count,
            commits_copied,
            strategy: applied_strategy,
            new_head_id,
            new_head_t,
            commit,
            ..
        } = self
            .staging
            .fluree
            .prepare_merge(
                &ledger_name,
                &source_branch,
                target_branch.as_deref(),
                strategy,
            )
            .await
            .map_err(|e| stage_failure(&format!("prepare_merge failed: {e}")))?;

        // Defensive: if prepare_merge resolves a different target
        // than the queue entry's branch, the transactor and worker
        // disagree about which queue the entry belongs on. Poison
        // rather than advance the wrong branch.
        if target != self.ref_key.branch {
            return Err(stage(PoisonReason::BodyMalformed {
                error: format!(
                    "queue entry on branch {} but prepare_merge resolved target to {target}",
                    self.ref_key.branch
                ),
            }));
        }

        // General-merge paths produce a commit blob and a state
        // delta; fast-forward merges have neither (the source's
        // commits are already in the target namespace). The
        // `install` slot reflects that — Some for general merges
        // that earned both a write_guard and post-commit state,
        // None otherwise.
        let install = if let Some(GuardedStagedCommit {
            write_guard,
            staged,
        }) = commit
        {
            let commit_cid = staged.commit.id.clone().ok_or_else(|| {
                stage(PoisonReason::WorkerPanic {
                    message: "build_merge_general produced staged commit without commit.id".into(),
                })
            })?;
            let content_store = self.staging.fluree.content_store(&target_id);
            content_store
                .put_with_id(&commit_cid, &staged.commit_bytes)
                .await
                .map_err(|e| stage_failure(&format!("merge commit blob write failed: {e}")))?;
            for (cid, bytes) in &staged.referenced_bytes {
                content_store.put_with_id(cid, bytes).await.map_err(|e| {
                    stage_failure(&format!("merge referenced blob write failed: {e}"))
                })?;
            }
            let (_receipt, new_state) = staged
                .finalize_state()
                .map_err(|e| stage_failure(&format!("merge finalize_state failed: {e}")))?;
            write_guard.map(|guard| StagedStateInstall {
                write_guard: guard,
                new_state,
                commit_t: new_head_t,
            })
        } else {
            None
        };

        Ok(StagedOutcome {
            receipt: AppliedReceipt::Merge(MergeApplied {
                commit_id: new_head_id,
                commit_t: new_head_t,
                fast_forward,
                commits_copied,
                conflict_count,
                strategy: applied_strategy.unwrap_or(strategy),
            }),
            install,
        })
    }

    /// Re-stage the rebase worker-side. Writes any replay blobs to
    /// CAS, finalizes local state, and returns the head identity to
    /// publish. No-op rebases (every conflicting commit dropped by
    /// `Skip`, or every replay had empty flakes) republish the
    /// pre-rebase head so the queue entry completes without
    /// observable mutation.
    async fn process_rebase(&self, rebase: QueuedRebase) -> Result<StagedOutcome, WorkerError> {
        let QueuedRebase { strategy } = rebase;
        let ledger_name = self.ref_key.ledger_name.clone();
        let branch = self.ref_key.branch.clone();
        let StagedRebase {
            branch_id,
            source_head_id,
            source_head_t,
            fast_forward,
            total_commits,
            replayed,
            skipped,
            conflicts,
            pre_rebase_head_id,
            pre_rebase_head_t,
            new_head_id,
            new_head_t,
            write_guard,
            final_state,
            pending_replays,
            ..
        } = self
            .staging
            .fluree
            .prepare_rebase(&ledger_name, &branch, strategy)
            .await
            .map_err(|e| stage_failure(&format!("prepare_rebase failed: {e}")))?;

        // No advance: every replay was skipped or had no effect.
        // Republish the pre-rebase head so the queue entry completes
        // without observable mutation. If the branch was at genesis
        // with no head to fall back to, the situation is anomalous
        // — poison rather than fabricate a head.
        let (advance_to, advance_t) = match (new_head_id, pre_rebase_head_id) {
            (Some(head), _) => (head, new_head_t),
            (None, Some(head)) => (head, pre_rebase_head_t),
            (None, None) => {
                return Err(stage(PoisonReason::WorkerPanic {
                    message: "rebase produced no advance and the branch had no pre-rebase head"
                        .into(),
                }));
            }
        };

        let content_store = self.staging.fluree.content_store(&branch_id);
        for replay in &pending_replays {
            content_store
                .put_with_id(&replay.commit_id, &replay.commit_bytes)
                .await
                .map_err(|e| stage_failure(&format!("rebase commit blob write failed: {e}")))?;
        }

        Ok(StagedOutcome {
            receipt: AppliedReceipt::Rebase(RebaseApplied {
                commit_id: advance_to,
                commit_t: advance_t,
                fast_forward,
                replayed,
                skipped,
                conflicts: conflicts.len(),
                failures: 0,
                total_commits,
                source_head_t,
                source_head_id,
                strategy,
            }),
            install: write_guard.map(|guard| StagedStateInstall {
                write_guard: guard,
                new_state: final_state,
                commit_t: advance_t,
            }),
        })
    }

    async fn publish_head_advance(
        &self,
        commit_id: ContentId,
        commit_t: i64,
    ) -> Result<(), WorkerError> {
        let full_ledger_id = self.ref_key.ledger_id();
        match self
            .publishing
            .commits
            .publish_commit(&full_ledger_id, commit_t, &commit_id)
            .await
        {
            Ok(()) => Ok(()),
            // Terminal classifications from the publisher (notably
            // the leader-forwarded apply_staged_commit) route into
            // the poison path so the worker doesn't loop a known-
            // unfixable error forever and head-of-line-block the
            // branch's queue.
            Err(NameServiceError::NotFound(id)) => {
                Err(stage(PoisonReason::LedgerNotFound { ledger_id: id }))
            }
            // Admin-driven retraction. The branch's queue was
            // drained by `RetractLedger` (waiters resolved at drain
            // time), so the staged commit has nowhere to land. Map
            // to `Stale` — same drop-install-and-advance semantics
            // as a queue-front race, no spurious poison record for
            // an entry that's already gone.
            Err(NameServiceError::Retracted(msg)) => Err(WorkerError::Stale(msg)),
            Err(NameServiceError::ApplyRejected(msg)) => Err(stage(PoisonReason::WorkerPanic {
                message: format!("state machine rejected ApplyHead: {msg}"),
            })),
            // The queue front advanced past our queue_id (racing
            // worker or admin clear). Retrying won't help — the
            // caller drops the install and lets the run loop pick
            // up the new queue front once local raft applies the
            // pop.
            Err(NameServiceError::ApplyStale(msg)) => Err(WorkerError::Stale(msg)),
            Err(e) => Err(WorkerError::Raft(format!("publish_commit failed: {e}"))),
        }
    }

    async fn propose_poison(&self, queue_id: u64, reason: PoisonReason) -> Result<(), WorkerError> {
        // Route through the publisher's leader-aware path: on a
        // follower this ferries to the leader's `apply_queue_poison`
        // endpoint, on the leader it calls `client_write` directly.
        // Without the indirection a follower-owned worker would
        // bounce `ForwardToLeader` indefinitely and the branch's
        // queue front would stall on a deterministic poison.
        self.publishing
            .poison
            .poison_queue_entry(&self.ref_key, queue_id, reason)
            .await
            .map_err(|e| WorkerError::Raft(format!("PoisonQueueEntry propose failed: {e}")))
    }

    /// Drain this branch's queue until aborted by the caller.
    ///
    /// Each tick peeks the front of [`Self::ref_key`]'s queue and
    /// runs it through [`Self::process_entry`]. The per-branch FIFO
    /// ordering the design promises is preserved because only the
    /// front is sampled. Cross-branch concurrency is the supervisor's
    /// responsibility — each branch runs in its own worker task.
    pub async fn run(self) {
        // Track the most recently committed queue_id so we don't re-stage
        // an entry our locally-replicated state hasn't yet observed the
        // pop for. After a successful publish, the leader's state machine
        // pops the entry, but the follower's local apply lags behind the
        // round-trip HTTP response — `snapshot_front` would still return
        // the same `queue_id` until the follower applies. Re-staging it
        // would cause a duplicate commit, because the publisher's
        // `build_apply_head_command` samples the leader's current queue
        // front at propose time (which has already advanced to the
        // *next* entry), so the second ApplyHead carries our body but
        // pops the next entry — silently dropping that entry's work.
        let mut last_committed: Option<u64> = None;
        loop {
            let Some(entry) = self.snapshot_front().await else {
                tokio::time::sleep(POLL_INTERVAL).await;
                continue;
            };
            if let Some(last) = last_committed {
                if entry.queue_id <= last {
                    // Local state hasn't yet observed the pop for the
                    // entry we just published. Wait briefly for raft
                    // replication to catch up rather than spin or
                    // re-stage the same body against a stale front.
                    tokio::time::sleep(POLL_INTERVAL).await;
                    continue;
                }
            }
            let queue_id = entry.queue_id;
            // Wrap `process_entry` in `catch_unwind` so a panic
            // (third-party crate, future-proofed invariant slip)
            // poisons this entry rather than killing the worker task
            // and halting drain for this branch. `process_entry`
            // consumes `Stage` failures by proposing
            // `PoisonQueueEntry`, so only `Raft` propagates from the
            // `Ok` arm.
            let outcome = AssertUnwindSafe(self.process_entry(entry))
                .catch_unwind()
                .await;
            match outcome {
                Ok(Ok(())) => {
                    last_committed = Some(queue_id);
                }
                Ok(Err(WorkerError::Transient(_) | WorkerError::Stage(_))) => {
                    unreachable!("process_entry maps Transient/Stage failures to PoisonQueueEntry")
                }
                Ok(Err(WorkerError::Stale(_))) => {
                    unreachable!("try_advance_head consumes Stale internally and returns Ok")
                }
                Ok(Err(WorkerError::Raft(propose_error))) => {
                    // Raft propose failed (leader stepped down, quorum
                    // unreachable, branch-specific reconcile bug).
                    // Back off so we don't tight-loop, then retry the
                    // same front on the next tick. Other branches'
                    // workers are independent — they progress or fail
                    // on their own.
                    warn!(
                        ledger_id = %self.ref_key.ledger_name,
                        branch = %self.ref_key.branch,
                        queue_id,
                        error = %propose_error,
                        "raft publish failed; backing off and retrying"
                    );
                    tokio::time::sleep(RAFT_BACKOFF).await;
                }
                Err(panic_payload) => {
                    let message = panic_message(panic_payload);
                    error!(
                        ledger_id = %self.ref_key.ledger_name,
                        branch = %self.ref_key.branch,
                        queue_id,
                        panic = %message,
                        "worker panicked while processing entry; poisoning and continuing"
                    );
                    match self
                        .propose_poison(queue_id, PoisonReason::WorkerPanic { message })
                        .await
                    {
                        Ok(()) => last_committed = Some(queue_id),
                        Err(propose_error) => {
                            warn!(
                                ledger_id = %self.ref_key.ledger_name,
                                branch = %self.ref_key.branch,
                                queue_id,
                                error = %propose_error,
                                "failed to publish poison after worker panic; entry stays at queue head"
                            );
                            tokio::time::sleep(RAFT_BACKOFF).await;
                        }
                    }
                }
            }
        }
    }

    async fn snapshot_front(&self) -> Option<QueueEntry> {
        let state = self.shared_state.read().await;
        state
            .queues
            .get(&self.ref_key)
            .and_then(|queue| queue.front().cloned())
    }
}

/// Signal `abort` on every handle, then `await` each one. Returns
/// after every task has reached its `Drop`, so a follow-up spawn
/// for the same `RefKey` can't race the aborting task on shared
/// state like [`StagedReceiptMap`]. `JoinError::Cancelled` and
/// task panics surfaced through the join are discarded.
async fn abort_and_await<I>(handles: I)
where
    I: IntoIterator<Item = JoinHandle<()>>,
{
    let handles: Vec<_> = handles.into_iter().collect();
    for handle in &handles {
        handle.abort();
    }
    for handle in handles {
        let _ = handle.await;
    }
}

/// Supervises per-branch [`Worker`]s on this node. Watches the
/// replicated [`NameServiceState::queues`] and the cluster's voter
/// set, computes which branches this node owns under rendezvous
/// hashing, and reconciles the running worker set to match.
///
/// Spawned at process start (not bound to leadership) so every node
/// can host workers under distributed assignment. Shutdown is
/// driven through a [`CancellationToken`]; the supervisor's
/// `select!` loop catches the cancel, exits, and aborts every
/// per-branch worker it owns.
#[derive(Clone)]
pub struct WorkerSupervisor {
    id: NodeId,
    raft: Arc<Raft<TypeConfig>>,
    shared_state: SharedState,
    publishing: PublishingChannel,
    staging: StagingContext,
}

impl WorkerSupervisor {
    pub fn new(
        id: NodeId,
        raft: Arc<Raft<TypeConfig>>,
        shared_state: SharedState,
        publishing: PublishingChannel,
        staging: StagingContext,
    ) -> Self {
        Self {
            id,
            raft,
            shared_state,
            publishing,
            staging,
        }
    }

    /// Reconcile the set of running workers against the branches
    /// this node owns. Runs until the cancellation token is signaled.
    ///
    /// Each tick:
    /// 1. Compute the desired set: branches in
    ///    [`NameServiceState::queues`] whose rendezvous owner under
    ///    the current voter set is this node.
    /// 2. Spawn a worker for any newly-desired branch; abort the
    ///    worker for any branch this node no longer owns.
    /// 3. Sleep until either the metrics watch fires (membership /
    ///    leader changed → recompute ownership), the poll interval
    ///    elapses (queues may have new branches), or the cancel
    ///    token signals shutdown.
    ///
    /// On shutdown, every still-running worker is aborted before the
    /// loop returns so the caller's `JoinHandle::await` sees the
    /// supervisor stop only after its children have.
    pub async fn run(self, cancel: CancellationToken) {
        let mut workers: HashMap<RefKey, JoinHandle<()>> = HashMap::new();
        let mut metrics_rx = self.raft.metrics();

        loop {
            let desired = self.compute_desired_owners().await;
            self.reconcile_workers(&mut workers, desired).await;

            tokio::select! {
                // Metrics changed (membership, leader, term, index, …).
                // Recompute ownership on the next iteration. `Err`
                // means the Raft handle has been dropped — nothing
                // more to observe, exit.
                changed = metrics_rx.changed() => {
                    if changed.is_err() {
                        break;
                    }
                }
                () = tokio::time::sleep(SUPERVISOR_POLL_INTERVAL) => {}
                () = cancel.cancelled() => break,
            }
        }

        // Shutdown drain: same await-on-abort discipline reconcile
        // uses. The caller wraps `run` in `CancellableTaskHandle::shutdown`,
        // which awaits the supervisor's JoinHandle — without the
        // inner await here, that outer await returns while workers
        // are still racing on shutdown.
        abort_and_await(workers.drain().map(|(_, h)| h)).await;
    }

    async fn compute_desired_owners(&self) -> HashSet<RefKey> {
        // Steady-state path: one lock acquisition, no allocation.
        // `desired_owners` iterates `worker_eligible_voters`
        // (a `BTreeSet`) through `owner`'s borrowed-iter input, so
        // passing an empty fallback slice costs nothing — it's never
        // consulted when the eligible set is non-empty.
        {
            let state = self.shared_state.read().await;
            if !state.worker_eligible_voters.is_empty() {
                return desired_owners(&state, self.id, &[]);
            }
        }
        // Boot-window fallback: the lock is released before the
        // `watch::Ref` borrow so the apply path's writer isn't
        // contended on the membership read. Re-acquire to iterate
        // queues. The eligible set being empty is monotonic — once
        // the first membership-apply populates it the quorum-floor
        // invariant keeps it non-empty — so racing back to empty
        // between these two acquisitions can't happen in practice.
        let fallback = self.current_voters();
        let state = self.shared_state.read().await;
        desired_owners(&state, self.id, &fallback)
    }

    fn current_voters(&self) -> Vec<NodeId> {
        self.raft
            .metrics()
            .borrow()
            .membership_config
            .membership()
            .voter_ids()
            .collect()
    }

    /// Spawn workers for newly-desired branches; respawn workers
    /// whose handle has finished (panic outside `catch_unwind`, or
    /// any other path that exits `Worker::run` unexpectedly); tear
    /// down workers for branches no longer owned. Without the
    /// respawn step, a single panic in an unguarded path
    /// (`unreachable!()` arm, `propose_poison` await, lock
    /// acquisition) would leave a dead handle in the map and stall
    /// that branch's queue until ownership rotates off the node.
    ///
    /// Teardown uses [`abort_and_await`] — every aborted handle is
    /// awaited before this method returns. That serialization is
    /// what guarantees a fast ownership flap (the same `RefKey`
    /// moves away and back inside one supervisor tick) can't
    /// respawn a worker before the previous task has fully yielded;
    /// the two would otherwise race on the shared
    /// [`StagedReceiptMap`] under that `RefKey`.
    async fn reconcile_workers(
        &self,
        workers: &mut HashMap<RefKey, JoinHandle<()>>,
        desired: HashSet<RefKey>,
    ) {
        // Drain dead handles for still-desired branches first so the
        // spawn pass below treats them as missing entries. Joining
        // returns immediately (the handles are already finished);
        // log the cause so a panic outside `catch_unwind` is visible
        // instead of silently respawning.
        for dead in drain_dead_workers(workers, &desired) {
            match dead.await {
                Ok(()) => warn!(
                    "worker task exited cleanly — Worker::run is supposed to loop forever; respawning"
                ),
                Err(err) if err.is_panic() => warn!(
                    panic = %panic_message(err.into_panic()),
                    "worker task panicked outside catch_unwind — respawning"
                ),
                Err(err) => warn!(error = ?err, "worker task ended unexpectedly — respawning"),
            }
        }
        for key in &desired {
            if !workers.contains_key(key) {
                let handle = tokio::spawn(self.make_worker(key.clone()).run());
                workers.insert(key.clone(), handle);
            }
        }
        // Two-step removal: HashMap::extract_if is unstable, so
        // collect the keys we no longer want first, then pull each
        // handle out by key. The intermediate Vec is bounded by the
        // number of branches whose ownership just moved off this
        // node — small under any realistic deployment.
        let to_drop: Vec<RefKey> = workers
            .keys()
            .filter(|key| !desired.contains(*key))
            .cloned()
            .collect();
        let dropped = to_drop.into_iter().filter_map(|key| workers.remove(&key));
        abort_and_await(dropped).await;
    }

    fn make_worker(&self, ref_key: RefKey) -> Worker {
        Worker::new(
            ref_key,
            self.shared_state.clone(),
            self.publishing.clone(),
            self.staging.clone(),
        )
    }
}

/// Pure computation of the branches this node owns under
/// rendezvous-hash assignment. Reads `state.worker_eligible_voters`
/// when populated (the replicated, monitor-curated voter set);
/// otherwise uses `fallback_voters` (the raft membership view that
/// only the boot window sees). Empty voter set under either path
/// yields no owners.
///
/// Pulled out as a free function so the same logic can be exercised
/// by unit tests without constructing an `Arc<Raft<TypeConfig>>` —
/// the async wrapper at
/// [`WorkerSupervisor::compute_desired_owners`] just supplies the
/// fallback from `raft.metrics()` when needed.
fn desired_owners(
    state: &crate::raft::state_machine::NameServiceState,
    id: NodeId,
    fallback_voters: &[NodeId],
) -> HashSet<RefKey> {
    if !state.worker_eligible_voters.is_empty() {
        return state
            .queues
            .keys()
            .filter(|ref_key| owner(ref_key, &state.worker_eligible_voters) == Some(id))
            .cloned()
            .collect();
    }
    if fallback_voters.is_empty() {
        return HashSet::new();
    }
    state
        .queues
        .keys()
        .filter(|ref_key| owner(ref_key, fallback_voters) == Some(id))
        .cloned()
        .collect()
}

/// Remove and return the [`JoinHandle`]s for `workers` entries whose
/// task has finished and whose [`RefKey`] is still in `desired`.
/// Caller awaits each (returns immediately since they're already
/// finished) to surface the panic cause, then the spawn pass
/// recreates the worker. Dead handles whose key has dropped out of
/// `desired` are left for the teardown pass to clean up.
fn drain_dead_workers(
    workers: &mut HashMap<RefKey, JoinHandle<()>>,
    desired: &HashSet<RefKey>,
) -> Vec<JoinHandle<()>> {
    let dead_keys: Vec<RefKey> = workers
        .iter()
        .filter(|(key, handle)| desired.contains(*key) && handle.is_finished())
        .map(|(key, _)| key.clone())
        .collect();
    dead_keys
        .into_iter()
        .filter_map(|key| workers.remove(&key))
        .collect()
}

/// Cross-check the queue entry's inline discriminator against the
/// envelope payload variant. A mismatch is a state-machine /
/// committer bug — surface loudly so the entry poisons rather than
/// processing under the wrong path.
fn check_envelope_kind(body_kind: BodyKind, envelope: &QueuedRequest) -> Result<(), WorkerError> {
    let expected = match envelope {
        QueuedRequest::Transact(t) => BodyKind::from(&t.body),
        QueuedRequest::Push(_) => BodyKind::Pushed,
        QueuedRequest::Revert(_) => BodyKind::Revert,
        QueuedRequest::Merge(_) => BodyKind::Merge,
        QueuedRequest::Rebase(_) => BodyKind::Rebase,
    };
    if expected == body_kind {
        Ok(())
    } else {
        Err(stage(PoisonReason::BodyMalformed {
            error: format!(
                "queue entry body_kind {body_kind:?} does not match envelope variant {expected:?}"
            ),
        }))
    }
}

fn stage_failure(message: &str) -> WorkerError {
    WorkerError::Transient(message.into())
}

/// Best-effort string extraction from a `catch_unwind` payload —
/// covers the `panic!("literal")` and `panic!("{fmt}")` cases that
/// produce `&'static str` and `String` payloads respectively.
fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        return (*s).to_string();
    }
    if let Some(s) = payload.downcast_ref::<String>() {
        return s.clone();
    }
    "non-string panic payload".to_string()
}

fn submission_to_stage(err: SubmissionError) -> WorkerError {
    WorkerError::Transient(err.to_string())
}

fn api_error_to_stage(err: ApiError) -> WorkerError {
    WorkerError::Transient(err.to_string())
}

/// Wrap a [`PoisonReason`] into the boxed [`WorkerError::Stage`]
/// variant. Centralizes the `Box::new` so the construction sites
/// don't have to repeat it.
fn stage(reason: PoisonReason) -> WorkerError {
    WorkerError::Stage(Box::new(reason))
}

/// Internal classification for staging outcomes.
///
/// `Transient` is a retryable hiccup — the per-entry loop in
/// [`Worker::process_entry`] retries with backoff up to
/// [`MAX_STAGE_ATTEMPTS`] and then promotes the carried message
/// into [`PoisonReason::StagingFailed`] stamped with the real
/// final attempt count. Producers don't need to know how many
/// attempts have run.
///
/// `Stage` carries a deterministic [`PoisonReason`] (e.g.
/// `BodyMalformed`, `PolicyViolation`, `WorkerPanic`) the worker
/// proposes verbatim via `PoisonQueueEntry` — retrying these would
/// just burn a worker round.
///
/// `Raft` is reserved for propose failures the caller propagates —
/// those mean the cluster fundamentally can't accept commands
/// right now (leader changed, quorum lost) and the run loop should
/// yield.
#[derive(Debug, Error)]
pub enum WorkerError {
    /// Retryable backend hiccup. Promoted to
    /// [`PoisonReason::StagingFailed`] by the retry loop only after
    /// the budget is exhausted, so the recorded `attempts` reflects
    /// the actual count rather than a placeholder.
    #[error("transient staging error: {0}")]
    Transient(String),
    /// `PoisonReason` is boxed so this enum stays small even though
    /// `PushCasFailed` carries two `Option<ContentId>`s — without
    /// the indirection every `Result<(), WorkerError>` in the worker
    /// pays that variant's footprint even on the happy path.
    #[error("staging poisoned: {0:?}")]
    Stage(Box<PoisonReason>),
    #[error("raft propose: {0}")]
    Raft(String),
    /// The replicated apply observed the queue front had moved past
    /// our `queue_id` (racing worker or admin clear). Distinct from
    /// [`Self::Raft`] so the retry path treats it as "drop work and
    /// move on" rather than backing off and trying the same
    /// `snapshot_front` again.
    #[error("apply stale: {0}")]
    Stale(String),
}

/// Output of a per-op staging path before consensus has confirmed
/// the head advance. Carries the typed receipt the adapter delivers
/// through the waiter map plus any local state install the worker
/// should run *after* the publish succeeds — never before, so a
/// failed publish leaves this node's Fluree cache at its pre-stage
/// head.
pub(crate) struct StagedOutcome {
    receipt: AppliedReceipt,
    install: Option<StagedStateInstall>,
}

/// Local state install owed once the publish succeeds. `None` when
/// nothing changed locally (fast-forward merge, no-op rebase or
/// revert) — in those cases there's no write guard to release and
/// no new LedgerState to swap in.
pub(crate) struct StagedStateInstall {
    write_guard: fluree_db_api::LedgerWriteGuard,
    new_state: fluree_db_ledger::LedgerState,
    commit_t: i64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::state_machine::{NameServiceState, QueueEntry};
    use crate::{IdempotencyCacheKey, IdempotencyKey};
    use fluree_db_api::{CommitOptsRequest, GovernanceOptions, TrackingOptions};
    use fluree_db_core::ContentKind;
    use fluree_db_transact::TxnOpts;
    use serde_json::json;
    use std::collections::{HashMap, HashSet, VecDeque};
    use tokio::sync::RwLock;

    fn cid(seed: u8) -> ContentId {
        ContentId::new(ContentKind::Commit, &[seed])
    }

    fn sample_transact_envelope() -> QueuedRequest {
        QueuedRequest::Transact(Box::new(QueuedTransact {
            body: TransactionBody::JsonLdInsert(json!({"@id": "ex:s", "ex:p": "ex:o"})),
            txn_opts: TxnOpts::default(),
            commit_opts: CommitOptsRequest::default(),
            tracking: Some(TrackingOptions::default()),
            governance: GovernanceOptions::default(),
        }))
    }

    fn sample_push_envelope() -> QueuedRequest {
        QueuedRequest::Push(Box::new(QueuedPush {
            commit_cids: vec![cid(5)],
            blobs: HashMap::new(),
            governance: GovernanceOptions::default(),
        }))
    }

    #[test]
    fn queued_transact_roundtrips_through_codec() {
        let envelope = sample_transact_envelope();
        let bytes = envelope.to_bytes().expect("encode");
        let decoded = QueuedRequest::from_bytes(&bytes).expect("decode");

        match decoded {
            QueuedRequest::Transact(t) => {
                assert!(matches!(t.body, TransactionBody::JsonLdInsert(_)));
                assert!(t.tracking.is_some());
            }
            other => panic!("expected Transact, got {other:?}"),
        }
    }

    #[test]
    fn queued_push_roundtrips_through_codec() {
        let envelope = sample_push_envelope();
        let bytes = envelope.to_bytes().expect("encode");
        let decoded = QueuedRequest::from_bytes(&bytes).expect("decode");
        match decoded {
            QueuedRequest::Push(p) => {
                assert_eq!(p.commit_cids.len(), 1);
                assert_eq!(p.commit_cids[0], cid(5));
            }
            other => panic!("expected Push, got {other:?}"),
        }
    }

    #[test]
    fn check_envelope_kind_accepts_matching_pair() {
        assert!(check_envelope_kind(BodyKind::JsonLdInsert, &sample_transact_envelope()).is_ok());
        assert!(check_envelope_kind(BodyKind::Pushed, &sample_push_envelope()).is_ok());
    }

    #[test]
    fn check_envelope_kind_rejects_mismatched_pair() {
        // Transact envelope marked as Pushed
        assert!(check_envelope_kind(BodyKind::Pushed, &sample_transact_envelope()).is_err());
        // Push envelope marked as a JSON-LD insert
        assert!(check_envelope_kind(BodyKind::JsonLdInsert, &sample_push_envelope()).is_err());
    }

    fn enqueued_entry(queue_id: u64, request_cid: ContentId, kind: BodyKind) -> QueueEntry {
        QueueEntry {
            queue_id,
            enqueued_index: 1,
            enqueued_at_millis: 1_000,
            idempotency: Some(IdempotencyCacheKey::new(
                "test/db",
                IdempotencyKey::new("k1").expect("test key fits cap"),
            )),
            body_cid: request_cid.clone(),
            request_cid,
            body_kind: kind,
        }
    }

    fn install_queue(
        state: &mut NameServiceState,
        ledger_id: &str,
        branch: &str,
        entries: Vec<QueueEntry>,
    ) {
        state
            .queues
            .insert(RefKey::new(ledger_id, branch), VecDeque::from(entries));
    }

    /// Replicates the per-branch peek logic in [`Worker::snapshot_front`]
    /// against a bare `SharedState`. Lets us exercise the queue-front
    /// invariants without wiring up Raft + Fluree.
    async fn snapshot_front_for_test(state: &SharedState, ref_key: &RefKey) -> Option<QueueEntry> {
        let state = state.read().await;
        state
            .queues
            .get(ref_key)
            .and_then(|queue| queue.front().cloned())
    }

    /// Drive [`desired_owners`] through the same lock acquisition the
    /// supervisor's async wrapper does. Reads `worker_eligible_voters`
    /// from the shared state when populated; otherwise uses
    /// `fallback_voters` (the test stand-in for raft membership).
    async fn desired_owners_under_lock(
        shared: &SharedState,
        id: NodeId,
        fallback_voters: &[NodeId],
    ) -> HashSet<RefKey> {
        let state = shared.read().await;
        desired_owners(&state, id, fallback_voters)
    }

    /// A worker only ever takes the front of its own branch — never
    /// the second entry, never another branch's entry.
    #[tokio::test]
    async fn snapshot_front_returns_only_this_branches_front() {
        let mut state = NameServiceState::default();
        install_queue(
            &mut state,
            "test/db",
            "main",
            vec![
                enqueued_entry(7, cid(1), BodyKind::JsonLdInsert),
                enqueued_entry(8, cid(2), BodyKind::JsonLdInsert),
            ],
        );
        install_queue(
            &mut state,
            "test/db",
            "feature",
            vec![enqueued_entry(9, cid(3), BodyKind::Sparql)],
        );
        let shared = Arc::new(RwLock::new(state));

        let main_front = snapshot_front_for_test(&shared, &RefKey::new("test/db", "main"))
            .await
            .expect("main has a front");
        assert_eq!(main_front.queue_id, 7);

        let feature_front = snapshot_front_for_test(&shared, &RefKey::new("test/db", "feature"))
            .await
            .expect("feature has a front");
        assert_eq!(feature_front.queue_id, 9);
    }

    /// Empty queues and unknown branches both yield `None` — the
    /// worker treats them identically (sleep and re-poll).
    #[tokio::test]
    async fn snapshot_front_is_none_when_empty_or_missing() {
        let mut state = NameServiceState::default();
        install_queue(&mut state, "test/db", "empty", vec![]);
        let shared = Arc::new(RwLock::new(state));

        assert!(
            snapshot_front_for_test(&shared, &RefKey::new("test/db", "empty"))
                .await
                .is_none(),
            "empty queue has no front"
        );
        assert!(
            snapshot_front_for_test(&shared, &RefKey::new("test/db", "missing"))
                .await
                .is_none(),
            "absent ref_key has no front"
        );
    }

    /// On a single-voter cluster every branch is owned by the sole
    /// voter — supervisor running on that node claims them all.
    #[tokio::test]
    async fn supervisor_owns_every_branch_when_alone() {
        let mut state = NameServiceState::default();
        install_queue(
            &mut state,
            "test/db",
            "main",
            vec![enqueued_entry(7, cid(1), BodyKind::JsonLdInsert)],
        );
        install_queue(&mut state, "test/db", "feature", vec![]);
        let shared = Arc::new(RwLock::new(state));

        let desired = desired_owners_under_lock(&shared, 1, &[1]).await;
        assert_eq!(desired.len(), 2);
        assert!(desired.contains(&RefKey::new("test/db", "main")));
        assert!(desired.contains(&RefKey::new("test/db", "feature")));
    }

    /// On a multi-voter cluster ownership is partitioned: this node
    /// claims only the subset of branches whose rendezvous owner
    /// resolves to it. The complement is empty for branches owned by
    /// peers.
    #[tokio::test]
    async fn supervisor_owns_only_its_partition_share() {
        let mut state = NameServiceState::default();
        for i in 0..50 {
            install_queue(&mut state, "test/db", &format!("branch-{i}"), vec![]);
        }
        let shared = Arc::new(RwLock::new(state));
        let voters = vec![1u64, 2, 3, 4];

        let mut union = HashSet::new();
        for id in &voters {
            let mine = desired_owners_under_lock(&shared, *id, &voters).await;
            // Every claimed branch must belong to exactly one node.
            for k in &mine {
                assert!(
                    union.insert(k.clone()),
                    "branch {k:?} claimed by two voters"
                );
            }
        }
        assert_eq!(
            union.len(),
            50,
            "every branch must be claimed by exactly one voter"
        );
    }

    /// Empty voter set (cluster not yet bootstrapped or all voters
    /// dropped) → no branch resolves an owner, so the supervisor
    /// claims nothing rather than crashing on the empty hash input.
    #[tokio::test]
    async fn supervisor_claims_nothing_with_empty_voter_set() {
        let mut state = NameServiceState::default();
        install_queue(
            &mut state,
            "test/db",
            "main",
            vec![enqueued_entry(7, cid(1), BodyKind::JsonLdInsert)],
        );
        let shared = Arc::new(RwLock::new(state));

        let desired = desired_owners_under_lock(&shared, 1, &[]).await;
        assert!(desired.is_empty());
    }

    /// When `worker_eligible_voters` is populated, ownership ranks
    /// against that set — the fallback raft membership is ignored.
    /// Verifies the supervisor honors the leader's monitor demoting
    /// voter 3, even though raft still configures it as a voter.
    #[tokio::test]
    async fn supervisor_ranks_against_worker_eligible_voters_when_populated() {
        let mut state = NameServiceState::default();
        for i in 0..50 {
            install_queue(&mut state, "test/db", &format!("branch-{i}"), vec![]);
        }
        // Configured = {1, 2, 3}, but the monitor demoted 3 — so
        // only {1, 2} should host workers, and 3 must own nothing.
        state.configured_voters = [1, 2, 3].into_iter().collect();
        state.worker_eligible_voters = [1, 2].into_iter().collect();
        let shared = Arc::new(RwLock::new(state));
        let fallback = vec![1u64, 2, 3];

        // The demoted voter claims nothing despite still being in
        // the fallback list.
        let demoted_share = desired_owners_under_lock(&shared, 3, &fallback).await;
        assert!(
            demoted_share.is_empty(),
            "demoted voter must not own any branches"
        );

        // The remaining eligible voters partition every branch
        // between them — nothing strands on the demoted voter.
        let voter_1 = desired_owners_under_lock(&shared, 1, &fallback).await;
        let voter_2 = desired_owners_under_lock(&shared, 2, &fallback).await;
        let union: HashSet<RefKey> = voter_1.union(&voter_2).cloned().collect();
        assert_eq!(
            union.len(),
            50,
            "every branch must be claimed by exactly one eligible voter"
        );
        assert!(
            voter_1.is_disjoint(&voter_2),
            "no branch is double-claimed by two eligible voters"
        );
    }

    /// When `worker_eligible_voters` is empty (fresh boot, snapshot
    /// predating the field), the supervisor falls back to the raft
    /// membership input. Demonstrates the supervisor stays
    /// functional during the initial window before any
    /// membership-apply has populated the replicated set.
    #[tokio::test]
    async fn supervisor_falls_back_to_membership_when_eligible_set_empty() {
        let mut state = NameServiceState::default();
        for i in 0..20 {
            install_queue(&mut state, "test/db", &format!("branch-{i}"), vec![]);
        }
        // `worker_eligible_voters` deliberately left empty.
        let shared = Arc::new(RwLock::new(state));
        let fallback = vec![1u64, 2, 3];

        // Each of the three fallback voters covers a share of the
        // 20 branches, and they partition the full set.
        let mut union = HashSet::new();
        for id in &fallback {
            let mine = desired_owners_under_lock(&shared, *id, &fallback).await;
            for k in &mine {
                assert!(
                    union.insert(k.clone()),
                    "branch {k:?} claimed twice during fallback"
                );
            }
        }
        assert_eq!(union.len(), 20);
    }

    /// When `worker_eligible_voters` is populated but a voter not
    /// in the configured-voter input asks for its share, it gets
    /// nothing — the eligible set is authoritative regardless of
    /// what the caller passes as fallback.
    #[tokio::test]
    async fn supervisor_demoted_voter_claims_nothing_even_in_fallback() {
        let mut state = NameServiceState::default();
        install_queue(
            &mut state,
            "test/db",
            "main",
            vec![enqueued_entry(7, cid(1), BodyKind::JsonLdInsert)],
        );
        state.worker_eligible_voters = [1, 2].into_iter().collect();
        let shared = Arc::new(RwLock::new(state));
        // Voter 3 is in the fallback, but not in the eligible set.
        let desired = desired_owners_under_lock(&shared, 3, &[1, 2, 3]).await;
        assert!(desired.is_empty());
    }

    #[test]
    fn panic_message_extracts_static_str() {
        let payload: Box<dyn std::any::Any + Send> = Box::new("kaboom");
        assert_eq!(panic_message(payload), "kaboom");
    }

    #[test]
    fn panic_message_extracts_string() {
        let payload: Box<dyn std::any::Any + Send> = Box::new(String::from("formatted: 42"));
        assert_eq!(panic_message(payload), "formatted: 42");
    }

    #[test]
    fn panic_message_falls_back_for_unknown_payload() {
        let payload: Box<dyn std::any::Any + Send> = Box::new(42u32);
        assert_eq!(panic_message(payload), "non-string panic payload");
    }

    /// `abort_and_await` must return only after every aborted task
    /// has reached its `Drop`. Without that property, the
    /// supervisor's `reconcile_workers` could respawn a worker for
    /// a `RefKey` while the previous task is still in-flight on
    /// the shared `StagedReceiptMap`. The test stands in for the
    /// supervisor's contract: park a fleet of tasks on a never-
    /// resolving future, have each one decrement an `AliveGuard`
    /// in Drop, abort+await them as a batch, and assert the
    /// counter is back to zero by the time the await returns.
    #[tokio::test]
    async fn abort_and_await_waits_for_every_task_to_drop() {
        use std::future::pending;
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct AliveGuard(Arc<AtomicUsize>);
        impl Drop for AliveGuard {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::SeqCst);
            }
        }

        let alive = Arc::new(AtomicUsize::new(0));
        let handles: Vec<JoinHandle<()>> = (0..4)
            .map(|_| {
                alive.fetch_add(1, Ordering::SeqCst);
                let guard = AliveGuard(Arc::clone(&alive));
                tokio::spawn(async move {
                    let _g = guard;
                    pending::<()>().await;
                })
            })
            .collect();
        // Sanity check: every spawned task is alive before we
        // touch them. If this fails, the test setup is broken
        // and the assertion below would be misleading.
        assert_eq!(alive.load(Ordering::SeqCst), 4);

        super::abort_and_await(handles).await;

        assert_eq!(
            alive.load(Ordering::SeqCst),
            0,
            "every aborted task's Drop must run before abort_and_await returns"
        );
    }

    /// Without [`drain_dead_workers`], a worker that exited (panic
    /// outside `catch_unwind`, an `unreachable!()` arm tripping, the
    /// poison-publish path panicking under future invariants slip)
    /// would leave a finished [`JoinHandle`] in the supervisor's map,
    /// and the next `reconcile_workers` tick would skip respawning
    /// because `contains_key` is true. This test pins the helper:
    /// a finished handle for a still-desired branch is drained out
    /// so the spawn pass can replace it; a finished handle for a
    /// branch that's no longer desired is left for the teardown
    /// pass; a still-running handle is left alone.
    #[tokio::test]
    async fn drain_dead_workers_drains_only_finished_handles_for_desired_keys() {
        use crate::raft::state_machine::RefKey;
        use std::future::pending;

        // Spawn a task that returns immediately so its handle is
        // finished by the time we inspect it.
        let dead_desired = tokio::spawn(async {});
        let dead_undesired = tokio::spawn(async {});
        while !dead_desired.is_finished() || !dead_undesired.is_finished() {
            tokio::task::yield_now().await;
        }
        // Spawn a task that never resolves so its handle stays
        // unfinished — proves the helper doesn't touch live workers.
        let alive_desired = tokio::spawn(pending::<()>());

        let key_dead = RefKey::new("ledger", "dead");
        let key_undesired = RefKey::new("ledger", "undesired");
        let key_alive = RefKey::new("ledger", "alive");
        let mut workers = HashMap::new();
        workers.insert(key_dead.clone(), dead_desired);
        workers.insert(key_undesired.clone(), dead_undesired);
        workers.insert(key_alive.clone(), alive_desired);
        let desired = HashSet::from([key_dead.clone(), key_alive.clone()]);

        let drained = super::drain_dead_workers(&mut workers, &desired);

        assert_eq!(drained.len(), 1, "only the desired+dead handle drains");
        assert!(
            !workers.contains_key(&key_dead),
            "dead+desired handle removed so spawn pass can replace it"
        );
        assert!(
            workers.contains_key(&key_undesired),
            "dead+undesired handle stays for teardown pass"
        );
        assert!(workers.contains_key(&key_alive), "alive handle untouched");

        // Cleanup so the pending task doesn't leak into other tests.
        workers.remove(&key_alive).unwrap().abort();
        workers.remove(&key_undesired);
    }

    /// `StashGuard` removes the receipt when the guard goes out of
    /// scope. This is the load-bearing property for the abort case:
    /// if a worker is aborted between the stash and the propose
    /// completing, the future is dropped, all locals drop, the
    /// guard's `Drop` fires, and the receipt is cleaned up.
    #[tokio::test]
    async fn stash_guard_removes_receipt_on_drop() {
        use crate::raft::staged_receipt::{
            AppliedReceipt, StagedReceiptMap, StashGuard, TransactApplied,
        };
        use crate::raft::state_machine::RefKey;
        use fluree_db_api::{ContentId, ContentKind};

        let map = StagedReceiptMap::new();
        let receipt = AppliedReceipt::Transact(TransactApplied {
            commit_id: ContentId::new(ContentKind::Commit, &[1]),
            commit_t: 10,
            flake_count: 0,
            tally: None,
        });

        {
            let _guard = StashGuard::stash(&map, 42, RefKey::new("test/db", "main"), receipt);
            assert_eq!(map.len(), 1, "stash should populate the map");
        }
        assert_eq!(
            map.len(),
            0,
            "scope exit must drop the guard and clean the stash"
        );
    }

    /// Even when an unrelated path has already consumed the
    /// receipt (the state-machine adapter on apply, or the
    /// cross-node forward's pre-post take), the guard's `Drop` must
    /// be a safe no-op rather than panic or corrupt the map.
    #[tokio::test]
    async fn stash_guard_drop_is_idempotent_after_external_take() {
        use crate::raft::staged_receipt::{
            AppliedReceipt, StagedReceiptMap, StashGuard, TransactApplied,
        };
        use crate::raft::state_machine::RefKey;
        use fluree_db_api::{ContentId, ContentKind};

        let map = StagedReceiptMap::new();
        let receipt = AppliedReceipt::Transact(TransactApplied {
            commit_id: ContentId::new(ContentKind::Commit, &[1]),
            commit_t: 10,
            flake_count: 0,
            tally: None,
        });

        {
            let _guard = StashGuard::stash(&map, 42, RefKey::new("test/db", "main"), receipt);
            // Simulate the adapter / via-leader path taking the
            // receipt before the guard drops.
            assert!(map.take(42).is_some());
            assert_eq!(map.len(), 0);
        }
        // Guard's Drop ran on the empty slot — still empty, no
        // panic, no side effects.
        assert_eq!(map.len(), 0);
    }
}
