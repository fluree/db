//! Raft-replicated [`Committer`] implementation.
//!
//! Compiled only under the `raft` feature so non-replicated builds
//! don't take the openraft dependency. The leader runs the inner
//! [`Committer`] (typically a [`LocalCommitter`]) to *stage* a commit,
//! proposes the resulting commit blob through the Raft log, and
//! replies to the caller once the entry has reached quorum. Followers'
//! state machines apply the committed blob to their local content
//! store and advance their head ref.
//!
//! # Status
//!
//! This module is a scaffold: the public types and trait impls are
//! in place so the rest of the crate (and downstream callers) can
//! name [`RaftCommitter`] and the on-the-wire shapes, but
//! [`Committer`]-trait behaviour is currently stubbed with `todo!()`.
//! Submodules carry the ready-to-use pieces:
//!
//! - [`state_machine`] — the replicated nameservice, [`Command`] /
//!   [`Response`] enums, and the pure `apply` function. Fully wired
//!   and tested.
//! - [`storage`] — the [`RaftStorage`](storage::RaftStorage) trait
//!   and in-memory / filesystem backends.
//!
//! Filling in the [`Committer`] surface proceeds in this order:
//!
//! 1. openraft `RaftStateMachine` adapter that delegates to
//!    [`state_machine::apply`].
//! 2. openraft `RaftLogStorage` adapter over [`storage::RaftStorage`].
//! 3. `RaftNetwork` impl (HTTP for v1).
//! 4. Leader pipeline: stage the operation, write the commit blob to
//!    the content store, propose an [`state_machine::Command::AdvanceRef`]
//!    through Raft, await quorum, build the typed receipt.
//! 5. Receiver-side body storage + leader forwarding.
//!
//! [`LocalCommitter`]: crate::LocalCommitter

pub mod admin;
pub mod execution_record;
pub mod forward;
pub mod index_publisher;
pub mod log_adapter;
pub mod nameservice;
pub mod network;
pub mod state_machine;
pub mod state_machine_adapter;
pub mod storage;

pub use state_machine::{Command, Response};

use crate::local::{build_policy_context, execution_failure};
use crate::raft::execution_record::ExecutionRecordRef;
use crate::raft::state_machine::{
    AdvanceRefArgs, Command as SmCommand, IdempotencyContext as SmIdempotencyContext,
    RecordedTally, Response as SmResponse,
};
use crate::{
    Committer, IdempotencyCacheKey, MergeReceipt, MergeRequest, PushReceipt, PushRequest,
    RebaseReceipt, RebaseRequest, RevertReceipt, RevertRequest, SubmissionError, TransactionBody,
    TransactionReceipt, TransactionRequest,
};
use async_trait::async_trait;
use fluree_db_api::{
    ConflictStrategy, Fluree, GuardedStagedCommit, PushCommitsRequest, StagedMerge, StagedPush,
    StagedRebase, StagedRevert,
};
use fluree_db_core::{CommitId, ContentId};
use fluree_db_ledger::IndexConfig;
use openraft::Raft;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::SystemTime;

/// Identifier for a node in the Raft cluster.
///
/// Plain `u64`; the address pair (raft RPC URL + client-facing URL) is
/// carried on the [`ClusterNode`] entries supplied at cluster-membership
/// time.
pub type NodeId = u64;

/// Address pair for a Raft cluster member.
///
/// Replaces openraft's [`BasicNode`](openraft::BasicNode) so both
/// endpoints — the inter-node Raft RPC URL **and** the client-facing
/// URL the follower-forward middleware needs — travel together through
/// membership changes. Storing both inside the Raft state machine means
/// adding a peer at runtime (via [`admin::RaftAdmin::add_learner`])
/// makes its client URL immediately resolvable on every other node, no
/// restart required.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterNode {
    /// Base URL of the peer's inter-node Raft RPC endpoint, e.g.
    /// `"http://node-2:9090/raft"`. See [`network`] for how this is
    /// consumed.
    pub raft_addr: String,
    /// Base URL of the peer's client-facing endpoint, e.g.
    /// `"http://node-2:8080"`. See [`forward`] for how this is
    /// consumed by the follower-forward middleware.
    pub client_addr: String,
}

impl ClusterNode {
    pub fn new(raft_addr: impl Into<String>, client_addr: impl Into<String>) -> Self {
        Self {
            raft_addr: raft_addr.into(),
            client_addr: client_addr.into(),
        }
    }
}

impl std::fmt::Display for ClusterNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ClusterNode {{ raft: {}, client: {} }}",
            self.raft_addr, self.client_addr
        )
    }
}

openraft::declare_raft_types!(
    /// Type config wiring [`Command`] / [`Response`] into openraft.
    pub TypeConfig:
        D = Command,
        R = Response,
        NodeId = NodeId,
        Node = ClusterNode,
        Entry = openraft::Entry<TypeConfig>,
        SnapshotData = std::io::Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);

/// Raft-replicated [`Committer`].
///
/// Single instance per node. On the leader, the typed
/// `Committer::transact` / `revert` / `merge` / `rebase` / `push`
/// methods stage the operation locally and propose the resulting
/// commit blobs through the Raft log; followers apply via the state
/// machine. Writes against a follower return `SubmissionError`
/// pointing at the current leader (TBD: structured leader-redirect).
pub struct RaftCommitter {
    raft: Arc<Raft<TypeConfig>>,
    fluree: Arc<Fluree>,
    index_config: IndexConfig,
}

impl RaftCommitter {
    /// Construct from the openraft handle, the Fluree instance whose
    /// state we drive, and the index-thresholds config used during
    /// staging. The content store for each commit-blob write is
    /// resolved per-request via [`Fluree::content_store`] so writes
    /// land in the right per-ledger namespace.
    pub fn new(
        raft: Arc<Raft<TypeConfig>>,
        fluree: Arc<Fluree>,
        index_config: IndexConfig,
    ) -> Self {
        Self {
            raft,
            fluree,
            index_config,
        }
    }

    /// Borrow the underlying Raft handle. Used by admin endpoints
    /// (add-learner, change-membership, current-leader lookup) that
    /// don't fit on the `Committer` surface.
    pub fn raft(&self) -> &Arc<Raft<TypeConfig>> {
        &self.raft
    }
}

#[async_trait]
impl Committer for RaftCommitter {
    async fn transact(
        &self,
        request: TransactionRequest,
    ) -> Result<TransactionReceipt, SubmissionError> {
        // 1. Materialize idempotency context (consensus-side only; never
        //    flows into execution).
        let idempotency_ctx = request.idempotency_key.as_ref().map(|key| {
            SmIdempotencyContext {
                key: IdempotencyCacheKey::new(request.ledger_id.clone(), key.clone()),
                body_hash: request.body.body_hash(),
            }
        });

        let TransactionRequest {
            idempotency_key,
            ledger_id,
            body,
            txn_opts,
            commit_opts,
            tracking,
            governance,
        } = request;

        // 2. Resolve the ledger handle + split ledger_id into name + branch.
        let ledger_manager = self
            .fluree
            .ledger_manager()
            .ok_or_else(|| SubmissionError::Execution {
                status: 500,
                message: "LedgerManager is not configured on the Fluree instance".into(),
            })?;
        let ledger_handle = ledger_manager
            .get_or_load(&ledger_id)
            .await
            .map_err(execution_failure)?;
        let (ledger_name, branch) = split_ledger_id(&ledger_id)?;

        // 3. Build policy context.
        let policy_ctx = build_policy_context(&ledger_handle, &governance).await?;

        // 4. Construct the stage builder + run the dry-run terminal.
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
            .commit_opts(commit_opts)
            .index_config(self.index_config.clone());
        if let Some(tracking) = tracking {
            builder = builder.tracking(tracking);
        }
        if let Some(policy) = policy_ctx {
            builder = builder.policy(policy);
        }

        // 5. Stage locally → produces the commit blob in memory. The
        //    write guard is held across the AdvanceRef round-trip; on
        //    Applied we install the new state into it before drop.
        let (write_guard, staged_commit) = builder.build_commit().await.map_err(execution_failure)?;

        let commit_cid = staged_commit
            .commit
            .id
            .clone()
            .expect("build_commit guarantees commit.id is set");
        let new_t = staged_commit.commit.t;
        let flake_count = staged_commit.commit.flakes.len();
        let expected_prev = staged_commit
            .expected_head_ref
            .as_ref()
            .and_then(|r| r.id.clone());

        // 6. Write the commit blob to the per-ledger content store.
        let content_store = self.fluree.content_store(&ledger_id);
        content_store
            .put_with_id(&commit_cid, &staged_commit.commit_bytes)
            .await
            .map_err(|e| SubmissionError::Execution {
                status: 500,
                message: format!("commit blob write failed: {e}"),
            })?;

        // 7. Write referenced blobs (v1: empty).
        for (cid, bytes) in &staged_commit.referenced_bytes {
            content_store
                .put_with_id(cid, bytes)
                .await
                .map_err(|e| SubmissionError::Execution {
                    status: 500,
                    message: format!("referenced blob write failed: {e}"),
                })?;
        }

        // 8. Construct the AdvanceRef proposal.
        let recorded_tally = staged_commit.tally.as_ref().map(RecordedTally::from);
        let applied_at_millis = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let cmd = SmCommand::AdvanceRef(AdvanceRefArgs {
            ledger_id: ledger_name,
            branch,
            expected_prev,
            new_head: commit_cid.clone(),
            t: new_t,
            applied_at_millis,
            idempotency: idempotency_ctx,
            release: Vec::<ExecutionRecordRef>::new(),
            tally: recorded_tally,
        });

        // 9. Propose through Raft, await quorum.
        let resp =
            self.raft
                .client_write(cmd)
                .await
                .map_err(|e| SubmissionError::Execution {
                    status: 500,
                    message: format!("raft client_write failed: {e}"),
                })?;

        // 10. Translate Response into TransactionReceipt. On the
        //     Applied branch we also derive the post-commit
        //     LedgerState from the staged commit and install it via
        //     the held write guard, so this node's cached state is
        //     not stale on the next stage.
        match resp.data {
            SmResponse::Applied {
                head_t,
                head_id,
                accepted: _,
                release: _,
                tally,
            } => {
                let (_receipt, new_state) =
                    staged_commit
                        .finalize_state()
                        .map_err(|e| SubmissionError::Execution {
                            status: 500,
                            message: format!("finalize_state failed: {e}"),
                        })?;
                let needs_reindex = new_state.should_reindex(&self.index_config);
                self.fluree
                    .finalize_commit(write_guard, new_state, head_t, needs_reindex)
                    .await
                    .map_err(execution_failure)?;
                Ok(TransactionReceipt {
                    idempotency_key,
                    commit: fluree_db_api::CommitReceipt {
                        commit_id: head_id,
                        t: head_t,
                        flake_count,
                    },
                    tally: tally.map(Into::into),
                })
            }
            SmResponse::Conflict {
                current_head: _,
                current_t: _,
            } => Err(SubmissionError::Execution {
                status: 409,
                message: "raft CAS conflict on AdvanceRef".into(),
            }),
            SmResponse::BodyHashMismatch => Err(SubmissionError::KeyCollision),
            SmResponse::LedgerNotFound { ledger_id } => Err(SubmissionError::Execution {
                status: 404,
                message: format!("ledger not found: {ledger_id}"),
            }),
            SmResponse::Created { .. }
            | SmResponse::Deleted { .. }
            | SmResponse::AlreadyExists { .. }
            | SmResponse::NoOp
            | SmResponse::IndexAdvanced { .. }
            | SmResponse::IndexStale { .. }
            | SmResponse::IndexAhead { .. } => Err(SubmissionError::Execution {
                status: 500,
                message: "unexpected Response variant for AdvanceRef".into(),
            }),
        }
        // On non-Applied paths, the write guard drops here without
        // installing new state — the local cache remains at the
        // pre-build snapshot (correct: nothing advanced).
    }

    async fn revert(&self, request: RevertRequest) -> Result<RevertReceipt, SubmissionError> {
        let RevertRequest {
            idempotency_key,
            ledger_name,
            branch,
            selection,
            strategy,
        } = request;

        // 1. Build the staged revert. Strategy validation, conflict-key
        //    computation, and the StagedCommit construction all live
        //    inside `prepare_revert`.
        let StagedRevert {
            branch_id,
            branch: branch_name,
            reverted_commits,
            conflict_count,
            strategy,
            rollback_snapshot: _,
            current_head_t,
            current_head_id,
            commit,
        } = self
            .fluree
            .prepare_revert(&ledger_name, &branch, selection, strategy)
            .await
            .map_err(execution_failure)?;

        // 2. NoOp short-circuit: the conflict strategy dropped every
        //    reverted flake. Nothing to propose — return the head ref
        //    the build phase observed.
        let GuardedStagedCommit {
            write_guard,
            staged: staged_commit,
        } = match commit {
            Some(c) => c,
            None => {
                return Ok(RevertReceipt {
                    idempotency_key,
                    branch: branch_name,
                    reverted_commits,
                    conflict_count,
                    strategy,
                    new_head_t: current_head_t,
                    new_head_id: current_head_id,
                });
            }
        };

        // 3. Materialize idempotency context (consensus-side only).
        let idempotency_ctx = idempotency_key.as_ref().map(|key| SmIdempotencyContext {
            key: IdempotencyCacheKey::new(branch_id.clone(), key.clone()),
            body_hash: revert_body_hash(&reverted_commits, &strategy),
        });

        let commit_cid = staged_commit
            .commit
            .id
            .clone()
            .expect("build_revert_commit guarantees commit.id is set");
        let new_t = staged_commit.commit.t;
        let expected_prev = staged_commit
            .expected_head_ref
            .as_ref()
            .and_then(|r| r.id.clone());

        // 4. Write the commit blob to the per-ledger content store.
        let content_store = self.fluree.content_store(&branch_id);
        content_store
            .put_with_id(&commit_cid, &staged_commit.commit_bytes)
            .await
            .map_err(|e| SubmissionError::Execution {
                status: 500,
                message: format!("revert commit blob write failed: {e}"),
            })?;

        // 5. Write any referenced blobs (v1: empty).
        for (cid, bytes) in &staged_commit.referenced_bytes {
            content_store
                .put_with_id(cid, bytes)
                .await
                .map_err(|e| SubmissionError::Execution {
                    status: 500,
                    message: format!("revert referenced blob write failed: {e}"),
                })?;
        }

        // 6. Construct the AdvanceRef proposal.
        let applied_at_millis = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let cmd = SmCommand::AdvanceRef(AdvanceRefArgs {
            ledger_id: ledger_name,
            branch: branch_name.clone(),
            expected_prev,
            new_head: commit_cid.clone(),
            t: new_t,
            applied_at_millis,
            idempotency: idempotency_ctx,
            release: Vec::<ExecutionRecordRef>::new(),
            tally: None,
        });

        // 7. Propose through Raft, await quorum.
        let resp =
            self.raft
                .client_write(cmd)
                .await
                .map_err(|e| SubmissionError::Execution {
                    status: 500,
                    message: format!("raft client_write failed: {e}"),
                })?;

        // 8. Translate Response into RevertReceipt; on Applied, derive
        //    the post-commit LedgerState and install it through the
        //    held write guard before dropping.
        match resp.data {
            SmResponse::Applied {
                head_t,
                head_id,
                accepted: _,
                release: _,
                tally: _,
            } => {
                let (_receipt, new_state) =
                    staged_commit
                        .finalize_state()
                        .map_err(|e| SubmissionError::Execution {
                            status: 500,
                            message: format!("revert finalize_state failed: {e}"),
                        })?;
                if let Some(guard) = write_guard {
                    let needs_reindex = new_state.should_reindex(&self.index_config);
                    self.fluree
                        .finalize_commit(guard, new_state, head_t, needs_reindex)
                        .await
                        .map_err(execution_failure)?;
                }
                Ok(RevertReceipt {
                    idempotency_key,
                    branch: branch_name,
                    reverted_commits,
                    conflict_count,
                    strategy,
                    new_head_t: head_t,
                    new_head_id: head_id,
                })
            }
            SmResponse::Conflict {
                current_head: _,
                current_t: _,
            } => Err(SubmissionError::Execution {
                status: 409,
                message: "raft CAS conflict on AdvanceRef for revert".into(),
            }),
            SmResponse::BodyHashMismatch => Err(SubmissionError::KeyCollision),
            SmResponse::LedgerNotFound { ledger_id } => Err(SubmissionError::Execution {
                status: 404,
                message: format!("ledger not found: {ledger_id}"),
            }),
            SmResponse::Created { .. }
            | SmResponse::Deleted { .. }
            | SmResponse::AlreadyExists { .. }
            | SmResponse::NoOp
            | SmResponse::IndexAdvanced { .. }
            | SmResponse::IndexStale { .. }
            | SmResponse::IndexAhead { .. } => Err(SubmissionError::Execution {
                status: 500,
                message: "unexpected Response variant for AdvanceRef".into(),
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

        // 1. Build the staged merge. Source/target resolution,
        //    common-ancestor calculation, fast-forward dispatch, and
        //    (for general merges) the merge-commit construction all
        //    live inside `prepare_merge`. The build phase already
        //    copied source commits into the target namespace.
        let StagedMerge {
            target,
            source,
            target_id,
            source_id: _,
            source_ledger_id: _,
            fast_forward,
            conflict_count,
            strategy: staged_strategy,
            commits_copied,
            rollback_snapshot: _,
            current_head_t: _,
            current_head_id,
            new_head_t,
            new_head_id,
            commit,
            source_index_for_publish: _,
        } = self
            .fluree
            .prepare_merge(
                &ledger_name,
                &source_branch,
                target_branch.as_deref(),
                strategy,
            )
            .await
            .map_err(execution_failure)?;

        // 2. Write any new commit blob (general merge) to the shared
        //    content store. Fast-forward has no new commit body — the
        //    source's commits are already in the target namespace
        //    from the build phase.
        let (write_guard, staged_for_finalize) = match commit {
            Some(GuardedStagedCommit {
                write_guard,
                staged,
            }) => {
                let commit_cid = staged
                    .commit
                    .id
                    .clone()
                    .expect("build_merge_general guarantees commit.id is set");
                let content_store = self.fluree.content_store(&target_id);
                content_store
                    .put_with_id(&commit_cid, &staged.commit_bytes)
                    .await
                    .map_err(|e| SubmissionError::Execution {
                        status: 500,
                        message: format!("merge commit blob write failed: {e}"),
                    })?;
                for (cid, bytes) in &staged.referenced_bytes {
                    content_store
                        .put_with_id(cid, bytes)
                        .await
                        .map_err(|e| SubmissionError::Execution {
                            status: 500,
                            message: format!("merge referenced blob write failed: {e}"),
                        })?;
                }
                (write_guard, Some(staged))
            }
            None => (None, None),
        };

        // 3. Materialize idempotency context (consensus-side only).
        let idempotency_ctx = idempotency_key.as_ref().map(|key| SmIdempotencyContext {
            key: IdempotencyCacheKey::new(target_id.clone(), key.clone()),
            body_hash: merge_body_hash(&source, &target, fast_forward, staged_strategy.as_ref()),
        });

        // 4. Construct the AdvanceRef proposal. expected_prev is the
        //    target's pre-merge head (None if the target was empty).
        let applied_at_millis = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let cmd = SmCommand::AdvanceRef(AdvanceRefArgs {
            ledger_id: ledger_name,
            branch: target.clone(),
            expected_prev: current_head_id,
            new_head: new_head_id.clone(),
            t: new_head_t,
            applied_at_millis,
            idempotency: idempotency_ctx,
            release: Vec::<ExecutionRecordRef>::new(),
            tally: None,
        });

        // 5. Propose through Raft, await quorum.
        let resp =
            self.raft
                .client_write(cmd)
                .await
                .map_err(|e| SubmissionError::Execution {
                    status: 500,
                    message: format!("raft client_write failed: {e}"),
                })?;

        // 6. Translate Response → MergeReceipt; on Applied, derive
        //    the post-commit LedgerState (general merge only) and
        //    install it through the held write guard before drop.
        let request_strategy = strategy;
        match resp.data {
            SmResponse::Applied {
                head_t,
                head_id,
                accepted: _,
                release: _,
                tally: _,
            } => {
                if let Some(staged) = staged_for_finalize {
                    let (_receipt, new_state) =
                        staged.finalize_state().map_err(|e| SubmissionError::Execution {
                            status: 500,
                            message: format!("merge finalize_state failed: {e}"),
                        })?;
                    if let Some(guard) = write_guard {
                        let needs_reindex = new_state.should_reindex(&self.index_config);
                        self.fluree
                            .finalize_commit(guard, new_state, head_t, needs_reindex)
                            .await
                            .map_err(execution_failure)?;
                    }
                }
                Ok(MergeReceipt {
                    idempotency_key,
                    source,
                    target,
                    fast_forward,
                    new_head_t: head_t,
                    new_head_id: head_id,
                    commits_copied,
                    conflict_count,
                    strategy: request_strategy,
                })
            }
            SmResponse::Conflict {
                current_head: _,
                current_t: _,
            } => Err(SubmissionError::Execution {
                status: 409,
                message: "raft CAS conflict on AdvanceRef for merge".into(),
            }),
            SmResponse::BodyHashMismatch => Err(SubmissionError::KeyCollision),
            SmResponse::LedgerNotFound { ledger_id } => Err(SubmissionError::Execution {
                status: 404,
                message: format!("ledger not found: {ledger_id}"),
            }),
            SmResponse::Created { .. }
            | SmResponse::Deleted { .. }
            | SmResponse::AlreadyExists { .. }
            | SmResponse::NoOp
            | SmResponse::IndexAdvanced { .. }
            | SmResponse::IndexStale { .. }
            | SmResponse::IndexAhead { .. } => Err(SubmissionError::Execution {
                status: 500,
                message: "unexpected Response variant for AdvanceRef".into(),
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

        // 1. Build the staged rebase. Validation, FF detection,
        //    summary scan, abort check, source-index copy, write
        //    guard acquisition, and the chained build of N replay
        //    commits all live inside `prepare_rebase`. On success
        //    the build phase has done no durable writes — blobs and
        //    ref advance happen in (4-5) below.
        let StagedRebase {
            branch: branch_name,
            branch_id,
            source: _,
            source_id: _,
            source_head_id,
            source_head_t,
            fast_forward,
            total_commits,
            replayed,
            skipped,
            conflicts,
            rollback_snapshot: _,
            pre_rebase_head_id,
            pre_rebase_head_t: _,
            new_head_id,
            new_head_t,
            write_guard,
            final_state,
            pending_replays,
        } = self
            .fluree
            .prepare_rebase(&ledger_name, &branch, strategy)
            .await
            .map_err(execution_failure)?;

        // 2. All-skipped no-op: every conflicting commit was dropped
        //    by `Skip` (or every replay had empty flakes after
        //    resolution). Nothing to propose; return the receipt
        //    with `replayed: 0`.
        let Some(advance_to) = new_head_id else {
            return Ok(RebaseReceipt {
                idempotency_key,
                branch: branch_name,
                fast_forward,
                replayed,
                skipped,
                conflicts: conflicts.len(),
                failures: 0,
                total_commits,
                source_head_t,
                source_head_id,
                strategy,
            });
        };

        // 3. Write all built replay blobs to the per-ledger content
        //    store. Fast-forward has no `pending_replays` (source's
        //    commits are already in their namespace and addressable
        //    through the branched store fallback); only general
        //    rebase writes blobs here.
        let content_store = self.fluree.content_store(&branch_id);
        for replay in &pending_replays {
            content_store
                .put_with_id(&replay.commit_id, &replay.commit_bytes)
                .await
                .map_err(|e| SubmissionError::Execution {
                    status: 500,
                    message: format!("rebase commit blob write failed: {e}"),
                })?;
        }

        // 4. Materialize idempotency context.
        let idempotency_ctx = idempotency_key.as_ref().map(|key| SmIdempotencyContext {
            key: IdempotencyCacheKey::new(branch_id.clone(), key.clone()),
            body_hash: rebase_body_hash(&branch_id, fast_forward, &strategy, &advance_to),
        });

        // 5. Single atomic AdvanceRef proposal: jumps branch HEAD
        //    from its pre-rebase position past all intermediate
        //    replays to `advance_to`. Intermediate replays are
        //    addressable in the content store but aren't on the
        //    active head chain until this advance commits.
        let applied_at_millis = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let cmd = SmCommand::AdvanceRef(AdvanceRefArgs {
            ledger_id: ledger_name,
            branch: branch_name.clone(),
            expected_prev: pre_rebase_head_id,
            new_head: advance_to.clone(),
            t: new_head_t,
            applied_at_millis,
            idempotency: idempotency_ctx,
            release: Vec::<ExecutionRecordRef>::new(),
            tally: None,
        });

        let resp =
            self.raft
                .client_write(cmd)
                .await
                .map_err(|e| SubmissionError::Execution {
                    status: 500,
                    message: format!("raft client_write failed: {e}"),
                })?;

        // 6. Translate Response → RebaseReceipt; on Applied, install
        //    the cumulative `final_state` through the held write
        //    guard so the leader's cache catches up with consensus.
        match resp.data {
            SmResponse::Applied {
                head_t,
                head_id,
                accepted: _,
                release: _,
                tally: _,
            } => {
                if let Some(guard) = write_guard {
                    let needs_reindex = final_state.should_reindex(&self.index_config);
                    self.fluree
                        .finalize_commit(guard, final_state, head_t, needs_reindex)
                        .await
                        .map_err(execution_failure)?;
                }
                Ok(RebaseReceipt {
                    idempotency_key,
                    branch: branch_name,
                    fast_forward,
                    replayed,
                    skipped,
                    conflicts: conflicts.len(),
                    failures: 0,
                    total_commits,
                    source_head_t: head_t,
                    source_head_id: head_id,
                    strategy,
                })
            }
            SmResponse::Conflict {
                current_head: _,
                current_t: _,
            } => Err(SubmissionError::Execution {
                status: 409,
                message: "raft CAS conflict on AdvanceRef for rebase".into(),
            }),
            SmResponse::BodyHashMismatch => Err(SubmissionError::KeyCollision),
            SmResponse::LedgerNotFound { ledger_id } => Err(SubmissionError::Execution {
                status: 404,
                message: format!("ledger not found: {ledger_id}"),
            }),
            SmResponse::Created { .. }
            | SmResponse::Deleted { .. }
            | SmResponse::AlreadyExists { .. }
            | SmResponse::NoOp
            | SmResponse::IndexAdvanced { .. }
            | SmResponse::IndexStale { .. }
            | SmResponse::IndexAhead { .. } => Err(SubmissionError::Execution {
                status: 500,
                message: "unexpected Response variant for AdvanceRef".into(),
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

        // 1. Build the staged push. Decode + validate the commit
        //    chain, evolve novelty + apply policy/SHACL per commit,
        //    write all blobs to the shared content store (the
        //    push's content-addressed writes are already idempotent),
        //    and derive `final_state` + `indexing_status`. The build
        //    phase performs no ref advance.
        let payload = PushCommitsRequest {
            commits: commits.into_iter().map(fluree_db_api::Base64Bytes).collect(),
            blobs: blobs
                .into_iter()
                .map(|(k, v)| (k, fluree_db_api::Base64Bytes(v)))
                .collect(),
        };
        let StagedPush {
            ledger,
            accepted,
            rollback_snapshot: _,
            pre_push_head,
            new_head_id,
            new_head_t,
            write_guard,
            final_state,
            indexing_status,
        } = self
            .fluree
            .prepare_push(&ledger_id, payload, &governance, &self.index_config)
            .await
            .map_err(execution_failure)?;

        // 2. Materialize idempotency context. Push doesn't have a
        //    request body the way transact does, but the resulting
        //    new head is content-derived from the inputs, so it
        //    serves as a deterministic body marker on retries.
        let idempotency_ctx = idempotency_key.as_ref().map(|key| SmIdempotencyContext {
            key: IdempotencyCacheKey::new(ledger.clone(), key.clone()),
            body_hash: push_body_hash(&ledger, &new_head_id, accepted, new_head_t),
        });

        // 3. Split `ledger` into `(ledger_name, branch)` for the
        //    state-machine command. Push targets a single branch
        //    (the ledger_id is already `"<name>:<branch>"`).
        let (ledger_name, branch) = split_ledger_id(&ledger)?;

        // 4. Construct the AdvanceRef proposal. expected_prev is the
        //    branch's pre-push head (`None` only on genesis push,
        //    which is rare — push usually appends to an existing
        //    chain).
        let applied_at_millis = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let cmd = SmCommand::AdvanceRef(AdvanceRefArgs {
            ledger_id: ledger_name,
            branch,
            expected_prev: pre_push_head.id.clone(),
            new_head: new_head_id.clone(),
            t: new_head_t,
            applied_at_millis,
            idempotency: idempotency_ctx,
            release: Vec::<ExecutionRecordRef>::new(),
            tally: None,
        });

        let resp =
            self.raft
                .client_write(cmd)
                .await
                .map_err(|e| SubmissionError::Execution {
                    status: 500,
                    message: format!("raft client_write failed: {e}"),
                })?;

        // 5. Translate Response → PushReceipt; on Applied, install
        //    the cumulative `final_state` through the held write
        //    guard so the leader's cache catches up with consensus.
        match resp.data {
            SmResponse::Applied {
                head_t,
                head_id,
                accepted: _,
                release: _,
                tally: _,
            } => {
                let needs_reindex = final_state.should_reindex(&self.index_config);
                self.fluree
                    .finalize_commit(write_guard, final_state, head_t, needs_reindex)
                    .await
                    .map_err(execution_failure)?;
                Ok(PushReceipt {
                    idempotency_key,
                    ledger,
                    accepted,
                    head_t,
                    head_id,
                    indexing: indexing_status,
                })
            }
            SmResponse::Conflict {
                current_head: _,
                current_t: _,
            } => Err(SubmissionError::Execution {
                status: 409,
                message: "raft CAS conflict on AdvanceRef for push".into(),
            }),
            SmResponse::BodyHashMismatch => Err(SubmissionError::KeyCollision),
            SmResponse::LedgerNotFound { ledger_id } => Err(SubmissionError::Execution {
                status: 404,
                message: format!("ledger not found: {ledger_id}"),
            }),
            SmResponse::Created { .. }
            | SmResponse::Deleted { .. }
            | SmResponse::AlreadyExists { .. }
            | SmResponse::NoOp
            | SmResponse::IndexAdvanced { .. }
            | SmResponse::IndexStale { .. }
            | SmResponse::IndexAhead { .. } => Err(SubmissionError::Execution {
                status: 500,
                message: "unexpected Response variant for AdvanceRef".into(),
            }),
        }
    }
}

/// Body hash for push idempotency. Domain-tagged so a key reused
/// across operation kinds is a collision rather than a cross-shape
/// dedup. Push's request body is large (N commit blobs); we hash
/// the validated outputs the state machine can re-derive on retry:
/// target ledger id, the new head id + t, and the accepted count.
/// All four are content-derived from the inputs, so a retry of the
/// same payload produces the same hash.
fn push_body_hash(
    ledger: &str,
    new_head: &ContentId,
    accepted: usize,
    new_head_t: i64,
) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(b"push");
    hasher.update(ledger.as_bytes());
    hasher.update(b"\0");
    hasher.update(new_head.to_string().as_bytes());
    hasher.update(b"\0");
    hasher.update(accepted.to_le_bytes());
    hasher.update(new_head_t.to_le_bytes());
    hasher.finalize().into()
}

/// Body hash for rebase idempotency. Domain-tagged so a key reused
/// across operation kinds is a collision rather than a cross-shape
/// dedup. Inputs are the validated request fields the state machine
/// can verify on retry: the resolved branch id, the fast-forward
/// bit, the conflict strategy, and the new head the proposal would
/// advance the ref to (which is content-derived from the rebase
/// inputs, so a retry of the same request produces the same value).
fn rebase_body_hash(
    branch_id: &str,
    fast_forward: bool,
    strategy: &ConflictStrategy,
    new_head: &ContentId,
) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(b"rebase");
    hasher.update(branch_id.as_bytes());
    hasher.update(b"\0");
    hasher.update(if fast_forward {
        b"ff".as_slice()
    } else {
        b"general".as_slice()
    });
    hasher.update(b"\0");
    hasher.update(strategy.as_str().as_bytes());
    hasher.update(b"\0");
    hasher.update(new_head.to_string().as_bytes());
    hasher.finalize().into()
}

/// Body hash for merge idempotency. Domain-tagged so a key reused
/// across operation kinds is a collision rather than a cross-shape
/// dedup. Inputs are the validated request fields the state machine
/// can verify on retry: source + resolved-target branch names, the
/// fast-forward bit, and the conflict strategy (`None` on
/// fast-forward).
fn merge_body_hash(
    source: &str,
    target: &str,
    fast_forward: bool,
    strategy: Option<&ConflictStrategy>,
) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(b"merge");
    hasher.update(source.as_bytes());
    hasher.update(b"\0");
    hasher.update(target.as_bytes());
    hasher.update(b"\0");
    hasher.update(if fast_forward {
        b"ff".as_slice()
    } else {
        b"general".as_slice()
    });
    hasher.update(b"\0");
    if let Some(s) = strategy {
        hasher.update(s.as_str().as_bytes());
    }
    hasher.finalize().into()
}

/// Body hash for revert idempotency: domain-tagged so an
/// idempotency key reused across operation kinds (transact / revert /
/// merge / rebase) flags as a collision rather than dedup-ing across
/// shapes. Hash inputs are the validated request fields the state
/// machine can verify on retry: the ordered list of reverted commits
/// and the conflict strategy.
fn revert_body_hash(reverted: &[CommitId], strategy: &ConflictStrategy) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(b"revert");
    for cid in reverted {
        hasher.update(cid.to_string().as_bytes());
        hasher.update(b"\0");
    }
    hasher.update(strategy.as_str().as_bytes());
    hasher.finalize().into()
}

/// Split a fully-qualified ledger id (`"name:branch"`) into its
/// components. Errors with a 400 [`SubmissionError`] if the format
/// doesn't parse.
fn split_ledger_id(ledger_id: &str) -> Result<(String, String), SubmissionError> {
    match ledger_id.split_once(':') {
        Some((name, branch)) => Ok((name.to_string(), branch.to_string())),
        None => Err(SubmissionError::Execution {
            status: 400,
            message: format!("invalid ledger_id (expected 'name:branch'): {ledger_id}"),
        }),
    }
}
