//! Per-branch staging tasks that drain [`NameServiceState`] queues
//! into head advances.
//!
//! The state machine appends `QueueEntry` rows to per-branch FIFOs as
//! [`Command::EnqueueCommand`](crate::raft::state_machine::Command::EnqueueCommand)
//! applies. The leader runs one [`Stager`] per active branch; each
//! polls its branch's queue front, restages the commit locally,
//! writes the commit blob to shared CAS, and publishes the head
//! advance through [`CommitPublisher::publish_commit`] — which
//! proposes [`Command::ApplyHead`](crate::raft::state_machine::Command::ApplyHead).
//! The entry pops and the head advances on that apply.
//!
//! [`StagerSupervisor`] watches [`NameServiceState::queues`] and
//! spawns a fresh `Stager` the first time it observes each `RefKey`.
//! Stagers run for the lifetime of the supervisor; leadership loss
//! aborts the supervisor task, dropping the inner `JoinSet` and
//! aborting every stager.
//!
//! Scope cuts for v1 (tracked in `docs/design/raft-command-queue.md`):
//! - No retry budget. Any staging failure poisons the entry.
//! - [`BodyKind::Pushed`] handling is deferred; the stager poisons
//!   any pushed entry until that path lands.
//! - Stagers run until aborted; idle branches are not reaped.
//! - Token-bearing fields in [`crate::QueuedRequest::governance`]
//!   travel verbatim; redaction is future work.

use crate::local::build_policy_context;
use crate::raft::ownership::owner;
use crate::raft::staged_receipt::{
    AppliedReceipt, MergeApplied, PushApplied, RebaseApplied, RevertApplied, StagedReceiptMap,
    TransactApplied,
};
use crate::raft::state_machine::{
    BodyKind, Command as SmCommand, PoisonQueueEntryArgs, PoisonReason, QueueEntry, RefKey,
};
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
use fluree_db_nameservice::CommitPublisher;
use futures::FutureExt;
use openraft::Raft;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use thiserror::Error;
use tracing::{debug, error, warn};

/// How often a [`Stager`] polls its branch queue when the previous
/// tick found nothing.
const POLL_INTERVAL: Duration = Duration::from_millis(50);

/// How often [`StagerSupervisor`] scans [`NameServiceState::queues`]
/// for branches it hasn't yet spawned a stager for. Longer than
/// [`POLL_INTERVAL`] because per-branch work latency is bounded by
/// the stager's own poll, not by how fast the supervisor notices new
/// keys — this just bounds the time-to-spawn for a freshly-seen
/// branch.
const SUPERVISOR_POLL_INTERVAL: Duration = Duration::from_millis(250);

/// How long a [`Stager`] waits after a Raft propose failure before
/// trying again. Long enough that we don't tight-loop against a lost
/// leader; short enough that recovery feels responsive.
const RAFT_BACKOFF: Duration = Duration::from_millis(250);

/// Max staging attempts before [`process_entry`] gives up and
/// proposes a poison. Only `PoisonReason::StagingFailed` is
/// retried — the other variants are deterministic and would just
/// burn worker rounds. Three tries balances "recover from a
/// transient CAS hiccup or lock contention" against "don't hold a
/// branch's queue front hostage indefinitely."
const MAX_STAGE_ATTEMPTS: u32 = 3;

/// First-attempt backoff between staging retries; subsequent
/// attempts double it. With three total attempts the worst-case
/// wait before poisoning is `100 + 200 = 300ms` — short enough
/// that downstream waiters don't time out, long enough to ride
/// through a transient hiccup.
const STAGE_RETRY_BASE_BACKOFF: Duration = Duration::from_millis(100);

/// Per-branch staging task.
///
/// Drains a single branch's queue: peeks the front, stages the
/// commit, writes the commit blob, publishes the head advance through
/// [`CommitPublisher::publish_commit`], and retires the entry. One
/// stager runs per active [`RefKey`] on the leader; cross-branch
/// concurrency is the supervisor's responsibility.
///
/// Head advances flow through `publisher.publish_commit` — a Raft
/// deployment supplies a [`RaftNameService`](crate::raft::nameservice::RaftNameService)
/// here, which proposes `ApplyHead` against the current per-branch
/// queue front. Poisoning still goes direct to Raft because there's
/// no trait surface for "fail this queue entry."
#[derive(Clone)]
pub struct Stager {
    ref_key: RefKey,
    raft: Arc<Raft<TypeConfig>>,
    publisher: Arc<dyn CommitPublisher>,
    fluree: Arc<Fluree>,
    index_config: IndexConfig,
    shared_state: SharedState,
    /// Side channel paired with the state-machine adapter's
    /// `StagedReceiptMap`. The stager stashes a typed
    /// [`AppliedReceipt`] here before proposing `ApplyHead`; the
    /// adapter takes it during waiter resolution. Cleanup on
    /// propose failure prevents stale receipts from accumulating.
    staged_receipts: Arc<StagedReceiptMap>,
}

impl Stager {
    fn new(
        ref_key: RefKey,
        raft: Arc<Raft<TypeConfig>>,
        publisher: Arc<dyn CommitPublisher>,
        fluree: Arc<Fluree>,
        index_config: IndexConfig,
        shared_state: SharedState,
        staged_receipts: Arc<StagedReceiptMap>,
    ) -> Self {
        Self {
            ref_key,
            raft,
            publisher,
            fluree,
            index_config,
            shared_state,
            staged_receipts,
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
    /// retrying them just burns a stager round. Between retries
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

        // Stash the typed receipt, then propose. The error path
        // can't blindly drop `install` — `publish_head_advance` may
        // return `Err` (lost response, stepped-down leader after
        // local apply, post-apply fatal) for an `ApplyHead` that
        // actually committed and was applied on this node. In that
        // case the replicated `state.refs` reflects our advance but
        // dropping the install leaves the local Fluree cache at the
        // pre-stage head → silent stale reads. So on error we
        // reconcile against the replicated head and finalize the
        // install only if our commit landed.
        self.staged_receipts
            .stash(entry.queue_id, self.ref_key.clone(), receipt);
        match self.publish_head_advance(commit_id.clone(), commit_t).await {
            Ok(()) => {
                if let Some(install) = install {
                    self.finalize_local_state(install).await?;
                }
                Ok(())
            }
            Err(err) => {
                if self.commit_replicated(&commit_id).await {
                    // Apply landed; the adapter already took the
                    // stash during waiter resolution, so our own
                    // `take` here is a defensive no-op. Finalize the
                    // local cache so this node catches up with the
                    // replicated head.
                    self.staged_receipts.take(entry.queue_id);
                    if let Some(install) = install {
                        self.finalize_local_state(install).await?;
                    }
                    Ok(())
                } else {
                    // Apply didn't land. Clean up the staged receipt
                    // and drop `install`: write_guard releases
                    // without calling `replace`, so this node's
                    // Fluree handle stays at its pre-stage head —
                    // same as every other node.
                    self.staged_receipts.take(entry.queue_id);
                    Err(err)
                }
            }
        }
    }

    /// Read the replicated state to see whether `commit_id` is the
    /// current head for this stager's [`RefKey`]. Used by
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
        let needs_reindex = new_state.should_reindex(&self.index_config);
        self.fluree
            .finalize_commit(write_guard, new_state, commit_t, needs_reindex)
            .await
            .map_err(api_error_to_stage)
    }

    async fn load_envelope(&self, entry: &QueueEntry) -> Result<QueuedRequest, WorkerError> {
        let ledger_id = self.ref_key.ledger_id();
        let bytes = self
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

        let staged = self.fluree.stage(&ledger_handle);
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
            .index_config(self.index_config.clone());
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

        let content_store = self.fluree.content_store(&ledger_id);
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
    async fn process_revert(
        &self,
        revert: QueuedRevert,
    ) -> Result<StagedOutcome, WorkerError> {
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
        let content_store = self.fluree.content_store(&ledger_id);
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
        let content_store = self.fluree.content_store(&ledger_id);
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
            .fluree
            .prepare_push(&ledger_id, payload, &governance, &self.index_config)
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
        // than the queue entry's branch, the transactor and stager
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
            let content_store = self.fluree.content_store(&target_id);
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

        let content_store = self.fluree.content_store(&branch_id);
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
        self.publisher
            .publish_commit(&full_ledger_id, commit_t, &commit_id)
            .await
            .map_err(|e| WorkerError::Raft(format!("publish_commit failed: {e}")))
    }

    async fn propose_poison(
        &self,
        queue_id: u64,
        reason: PoisonReason,
    ) -> Result<(), WorkerError> {
        let cmd = SmCommand::PoisonQueueEntry(PoisonQueueEntryArgs {
            ledger_id: self.ref_key.ledger_name.clone(),
            branch: self.ref_key.branch.clone(),
            queue_id,
            reason,
            applied_at_millis: current_millis(),
        });
        // The state-machine response (`Poisoned` vs `QueueDesync`) is
        // informational once the poison is durably proposed — either
        // way the entry is done from the stager's perspective. Only
        // surface Raft-side failures.
        self.raft
            .client_write(cmd)
            .await
            .map(|_| ())
            .map_err(|e| WorkerError::Raft(format!("PoisonQueueEntry propose failed: {e}")))
    }

    /// Drain this branch's queue until aborted by the caller.
    ///
    /// Each tick peeks the front of [`Self::ref_key`]'s queue and
    /// runs it through [`Self::process_entry`]. The per-branch FIFO
    /// ordering the design promises is preserved because only the
    /// front is sampled. Cross-branch concurrency is the supervisor's
    /// responsibility — each branch runs in its own stager task.
    pub async fn run(self) {
        loop {
            let Some(entry) = self.snapshot_front().await else {
                tokio::time::sleep(POLL_INTERVAL).await;
                continue;
            };
            let queue_id = entry.queue_id;
            // Wrap `process_entry` in `catch_unwind` so a panic
            // (third-party crate, future-proofed invariant slip)
            // poisons this entry rather than killing the stager task
            // and halting drain for this branch. `process_entry`
            // consumes `Stage` failures by proposing
            // `PoisonQueueEntry`, so only `Raft` propagates from the
            // `Ok` arm.
            let outcome = AssertUnwindSafe(self.process_entry(entry))
                .catch_unwind()
                .await;
            match outcome {
                Ok(Ok(())) => {}
                Ok(Err(WorkerError::Transient(_) | WorkerError::Stage(_))) => {
                    unreachable!("process_entry maps Transient/Stage failures to PoisonQueueEntry")
                }
                Ok(Err(WorkerError::Raft(propose_error))) => {
                    // Raft propose failed (leader stepped down, quorum
                    // unreachable, branch-specific reconcile bug).
                    // Back off so we don't tight-loop, then retry the
                    // same front on the next tick. Other branches'
                    // stagers are independent — they progress or fail
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
                        "stager panicked while processing entry; poisoning and continuing"
                    );
                    if let Err(propose_error) = self
                        .propose_poison(queue_id, PoisonReason::WorkerPanic { message })
                        .await
                    {
                        warn!(
                            ledger_id = %self.ref_key.ledger_name,
                            branch = %self.ref_key.branch,
                            queue_id,
                            error = %propose_error,
                            "failed to publish poison after stager panic; entry stays at queue head"
                        );
                        tokio::time::sleep(RAFT_BACKOFF).await;
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

/// Supervises per-branch [`Stager`]s on this node. Watches the
/// replicated [`NameServiceState::queues`] and the cluster's voter
/// set, computes which branches this node owns under rendezvous
/// hashing, and reconciles the running stager set to match.
///
/// Lifecycle: spawned at process start (not bound to leadership)
/// since every node potentially hosts stagers under distributed
/// assignment. Graceful shutdown is driven through a
/// [`CancellationToken`]; the supervisor's `select!` loop catches
/// the cancel, exits, and aborts every per-branch stager it owns.
///
/// Cloning is cheap (`Arc` clones plus a `NodeId`); the embedding
/// server typically constructs once and never clones, but the type
/// stays `Clone` to match the rest of the integration surface.
#[derive(Clone)]
pub struct StagerSupervisor {
    id: NodeId,
    raft: Arc<Raft<TypeConfig>>,
    publisher: Arc<dyn CommitPublisher>,
    fluree: Arc<Fluree>,
    index_config: IndexConfig,
    shared_state: SharedState,
    staged_receipts: Arc<StagedReceiptMap>,
}

impl StagerSupervisor {
    pub fn new(
        id: NodeId,
        raft: Arc<Raft<TypeConfig>>,
        publisher: Arc<dyn CommitPublisher>,
        fluree: Arc<Fluree>,
        index_config: IndexConfig,
        shared_state: SharedState,
        staged_receipts: Arc<StagedReceiptMap>,
    ) -> Self {
        Self {
            id,
            raft,
            publisher,
            fluree,
            index_config,
            shared_state,
            staged_receipts,
        }
    }

    /// Reconcile the set of running stagers against the branches
    /// this node owns. Runs until the cancellation token is signaled.
    ///
    /// Each tick:
    /// 1. Compute the desired set: branches in
    ///    [`NameServiceState::queues`] whose rendezvous owner under
    ///    the current voter set is this node.
    /// 2. Spawn a stager for any newly-desired branch; abort the
    ///    stager for any branch this node no longer owns.
    /// 3. Sleep until either the metrics watch fires (membership /
    ///    leader changed → recompute ownership), the poll interval
    ///    elapses (queues may have new branches), or the cancel
    ///    token signals shutdown.
    ///
    /// On shutdown, every still-running stager is aborted before the
    /// loop returns so the caller's `JoinHandle::await` sees the
    /// supervisor stop only after its children have.
    pub async fn run(self, cancel: tokio_util::sync::CancellationToken) {
        let mut stagers: std::collections::HashMap<RefKey, tokio::task::JoinHandle<()>> =
            std::collections::HashMap::new();
        let mut metrics_rx = self.raft.metrics();

        loop {
            let desired = self.compute_desired_owners().await;
            self.reconcile_stagers(&mut stagers, desired);

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

        for (_, handle) in stagers.drain() {
            handle.abort();
        }
    }

    async fn compute_desired_owners(&self) -> std::collections::HashSet<RefKey> {
        let voters = self.current_voters();
        // Empty voter set = cluster not yet bootstrapped (or every
        // voter dropped); no branch can resolve an owner, so claim
        // nothing this tick.
        if voters.is_empty() {
            return std::collections::HashSet::new();
        }
        let state = self.shared_state.read().await;
        state
            .queues
            .keys()
            .filter(|ref_key| owner(ref_key, &voters) == Some(self.id))
            .cloned()
            .collect()
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

    /// Spawn stagers for newly-desired branches; abort stagers for
    /// branches no longer owned. Dead handles (a stager that exited
    /// on its own — should not happen under normal operation since
    /// `Stager::run` loops forever and panics are caught per-entry)
    /// are left in place: they'll be removed on the next ownership
    /// change, or stay dormant until the supervisor restarts.
    fn reconcile_stagers(
        &self,
        stagers: &mut std::collections::HashMap<RefKey, tokio::task::JoinHandle<()>>,
        desired: std::collections::HashSet<RefKey>,
    ) {
        for key in &desired {
            if !stagers.contains_key(key) {
                let handle = tokio::spawn(self.make_stager(key.clone()).run());
                stagers.insert(key.clone(), handle);
            }
        }
        stagers.retain(|key, handle| {
            if desired.contains(key) {
                true
            } else {
                handle.abort();
                false
            }
        });
    }

    fn make_stager(&self, ref_key: RefKey) -> Stager {
        Stager::new(
            ref_key,
            Arc::clone(&self.raft),
            Arc::clone(&self.publisher),
            Arc::clone(&self.fluree),
            self.index_config.clone(),
            self.shared_state.clone(),
            Arc::clone(&self.staged_receipts),
        )
    }
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

fn current_millis() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
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
/// [`Stager::process_entry`] retries with backoff up to
/// [`MAX_STAGE_ATTEMPTS`] and then promotes the carried message
/// into [`PoisonReason::StagingFailed`] stamped with the real
/// final attempt count. Producers don't need to know how many
/// attempts have run.
///
/// `Stage` carries a deterministic [`PoisonReason`] (e.g.
/// `BodyMalformed`, `PolicyViolation`, `WorkerPanic`) the stager
/// proposes verbatim via `PoisonQueueEntry` — retrying these would
/// just burn a stager round.
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
    /// the indirection every `Result<(), WorkerError>` in the stager
    /// pays that variant's footprint even on the happy path.
    #[error("staging poisoned: {0:?}")]
    Stage(Box<PoisonReason>),
    #[error("raft propose: {0}")]
    Raft(String),
}

/// Output of a per-op staging path before consensus has confirmed
/// the head advance. Carries the typed receipt the adapter delivers
/// through the waiter map plus any local state install the stager
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
    use std::collections::VecDeque;
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
            blobs: std::collections::HashMap::new(),
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

    /// Replicates the per-branch peek logic in [`Stager::snapshot_front`]
    /// against a bare `SharedState`. Lets us exercise the queue-front
    /// invariants without wiring up Raft + Fluree.
    async fn snapshot_front_for_test(state: &SharedState, ref_key: &RefKey) -> Option<QueueEntry> {
        let state = state.read().await;
        state
            .queues
            .get(ref_key)
            .and_then(|queue| queue.front().cloned())
    }

    /// Replicates [`StagerSupervisor::compute_desired_owners`] against
    /// a bare `SharedState` plus an explicit voter list. Exercises
    /// the supervisor's ownership filter without wiring up Raft.
    async fn compute_desired_owners_for_test(
        state: &SharedState,
        id: NodeId,
        voters: &[NodeId],
    ) -> std::collections::HashSet<RefKey> {
        use crate::raft::ownership::owner;
        if voters.is_empty() {
            return std::collections::HashSet::new();
        }
        let state = state.read().await;
        state
            .queues
            .keys()
            .filter(|ref_key| owner(ref_key, voters) == Some(id))
            .cloned()
            .collect()
    }

    /// A stager only ever takes the front of its own branch — never
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
    /// stager treats them identically (sleep and re-poll).
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

        let desired = compute_desired_owners_for_test(&shared, 1, &[1]).await;
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

        let mut union = std::collections::HashSet::new();
        for id in &voters {
            let mine = compute_desired_owners_for_test(&shared, *id, &voters).await;
            // Every claimed branch must belong to exactly one node.
            for k in &mine {
                assert!(union.insert(k.clone()), "branch {k:?} claimed by two voters");
            }
        }
        assert_eq!(union.len(), 50, "every branch must be claimed by exactly one voter");
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

        let desired = compute_desired_owners_for_test(&shared, 1, &[]).await;
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
}
