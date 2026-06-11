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

pub mod execution_record;
pub mod log_adapter;
pub mod nameservice;
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
use fluree_db_api::{ConflictStrategy, Fluree, StagedRevert, GuardedStagedCommit};
use fluree_db_core::{CommitId, ContentStore};
use fluree_db_ledger::IndexConfig;
use openraft::{BasicNode, Raft};
use std::sync::Arc;
use std::time::SystemTime;

/// Identifier for a node in the Raft cluster.
///
/// Plain `u64` for now; concrete node addressing (URL, gRPC endpoint,
/// etc.) is carried separately on [`BasicNode`] entries supplied at
/// cluster-membership time.
pub type NodeId = u64;

openraft::declare_raft_types!(
    /// Type config wiring [`Command`] / [`Response`] into openraft.
    pub TypeConfig:
        D = Command,
        R = Response,
        NodeId = NodeId,
        Node = BasicNode,
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
    content_store: Arc<dyn ContentStore>,
    index_config: IndexConfig,
}

impl RaftCommitter {
    /// Construct from the openraft handle, the Fluree instance whose
    /// state we drive, the shared content store all replicas read
    /// from, and the index-thresholds config used during staging.
    pub fn new(
        raft: Arc<Raft<TypeConfig>>,
        fluree: Arc<Fluree>,
        content_store: Arc<dyn ContentStore>,
        index_config: IndexConfig,
    ) -> Self {
        Self {
            raft,
            fluree,
            content_store,
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

        // 6. Write the commit blob to the shared content store.
        self.content_store
            .put_with_id(&commit_cid, &staged_commit.commit_bytes)
            .await
            .map_err(|e| SubmissionError::Execution {
                status: 500,
                message: format!("commit blob write failed: {e}"),
            })?;

        // 7. Write referenced blobs (v1: empty).
        for (cid, bytes) in &staged_commit.referenced_bytes {
            self.content_store
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
            | SmResponse::NoOp => Err(SubmissionError::Execution {
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

        // 4. Write the commit blob to the shared content store.
        self.content_store
            .put_with_id(&commit_cid, &staged_commit.commit_bytes)
            .await
            .map_err(|e| SubmissionError::Execution {
                status: 500,
                message: format!("revert commit blob write failed: {e}"),
            })?;

        // 5. Write any referenced blobs (v1: empty).
        for (cid, bytes) in &staged_commit.referenced_bytes {
            self.content_store
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
            | SmResponse::NoOp => Err(SubmissionError::Execution {
                status: 500,
                message: "unexpected Response variant for AdvanceRef".into(),
            }),
        }
    }

    async fn merge(&self, _request: MergeRequest) -> Result<MergeReceipt, SubmissionError> {
        todo!("RaftCommitter::merge — pending merge/rebase/revert dry-run terminals")
    }

    async fn rebase(&self, _request: RebaseRequest) -> Result<RebaseReceipt, SubmissionError> {
        todo!("RaftCommitter::rebase — pending merge/rebase/revert dry-run terminals")
    }

    async fn push(&self, _request: PushRequest) -> Result<PushReceipt, SubmissionError> {
        todo!("RaftCommitter::push — pending push dry-run terminal")
    }
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
