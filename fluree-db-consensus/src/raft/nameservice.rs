//! `NameService` adapter over the replicated state machine.
//!
//! Cluster nodes need a way to resolve current ledger heads and to
//! publish index-build results without faking the broader nameservice
//! trait surface. [`RaftNameService`] does both:
//!
//! - **Reads** ([`NameServiceLookup`]) go straight to the shared
//!   [`SharedState`](super::state_machine_adapter::SharedState) the
//!   state-machine adapter writes to under apply — followers (and
//!   the leader, before its local cache catches up) observe
//!   committed log state without an openraft RPC.
//! - **Index publishing** ([`IndexPublisher`]) proposes
//!   [`Command::AdvanceIndexHead`](super::state_machine::Command::AdvanceIndexHead)
//!   through Raft via the held handle. Every node's state machine
//!   then updates its
//!   [`RefEntry::index`](super::state_machine::RefEntry::index) under
//!   apply, so subsequent reads observe the new index head as soon
//!   as the entry commits.
//! - **Commit publishing** ([`CommitPublisher`]) proposes
//!   [`Command::ApplyHead`](super::state_machine::Command::ApplyHead)
//!   against the current per-branch queue front. The caller (the
//!   queue worker) stages and writes the commit blob before invoking
//!   `publish_commit`; the trait method only handles the head
//!   advance. The `queue_id` is sampled from the shared state at
//!   call time — the state machine validates the front matches at
//!   apply time and returns [`QueueDesync`](super::state_machine::DesyncReason)
//!   on a race.
//!
//! # Stale-publish handling
//!
//! Only the current leader has a meaningful publish path. A
//! stepped-down leader whose in-flight indexer build finishes a tick
//! after the transition hits openraft's
//! [`ClientWriteError::ForwardToLeader`] — we map that to `Ok(())`
//! because the new leader will run its own build against its own
//! state, and rejecting the call would just produce log noise.
//!
//! # What's NOT tracked here
//!
//! The state machine carries ledger lifecycle, branch commit heads,
//! and published index heads — but not default contexts, ledger
//! configuration, or graph-source records. The lookup methods report
//! those as absent (None / 0 / empty), which is what
//! [`LedgerState::load`] needs for a follower reload: it falls back
//! to genesis-snapshot replay from the content store using the
//! branch head walked from `commit_head_id`.
//!
//! [`LedgerState::load`]: fluree_db_ledger::LedgerState::load

use crate::raft::staged_receipt::StagedReceiptMap;
use crate::raft::state_machine::{
    AdvanceIndexHeadArgs, ApplyHeadArgs, Command as SmCommand, CreateBranchArgs, CreateLedgerArgs,
    DesyncReason, NameServiceState, PushConfigArgs, RecordedTally, RefKey, ResetHeadSnapshot,
    Response as SmResponse,
};
use crate::raft::state_machine_adapter::SharedState;
use crate::raft::TypeConfig;
use async_trait::async_trait;
use fluree_db_core::ledger_id::split_ledger_id;
use fluree_db_core::ContentId;
use fluree_db_nameservice::{
    AdminPublisher, BranchLifecycle, CasResult, CommitPublisher, ConfigCasResult, ConfigLookup,
    ConfigPublisher, ConfigValue, GraphSourceLookup, GraphSourcePublisher, GraphSourceRecord,
    GraphSourceType, IndexPublisher, LedgerLifecycle, NameServiceError, NameServiceLookup,
    NsLookupResult, NsRecord, NsRecordSnapshot, RefKind, RefLookup, RefPublisher, RefValue, Result,
    StatusCasResult, StatusLookup, StatusPublisher, StatusValue,
};
use openraft::error::{ClientWriteError, RaftError};
use openraft::Raft;
use std::fmt;
use std::sync::Arc;
use std::time::SystemTime;

/// `NameService` adapter over the replicated state machine — reads
/// the shared state directly, writes the index head through Raft.
///
/// Construct with the same [`SharedState`] handle the state-machine
/// adapter holds and the same `Arc<Raft<TypeConfig>>` the rest of
/// the integration uses; the type owns its references so it's safe
/// to clone freely.
pub struct RaftNameService {
    state: SharedState,
    raft: Arc<Raft<TypeConfig>>,
    /// When set, `publish_commit` peeks the staged receipt for the
    /// queue front and threads its tally into [`ApplyHeadArgs::tally`]
    /// so the cached idempotency record carries the same tally a
    /// later retry would expect to see. Off → tally lands as `None`,
    /// which only affects metric reporting on idempotent retries.
    staged_receipts: Option<Arc<StagedReceiptMap>>,
}

impl RaftNameService {
    pub fn new(state: SharedState, raft: Arc<Raft<TypeConfig>>) -> Self {
        Self {
            state,
            raft,
            staged_receipts: None,
        }
    }

    /// Wire the [`StagedReceiptMap`] this nameservice peeks for
    /// per-queue tally values. Pair it with the same handle the
    /// [`CommitWorker`](super::commit_worker::CommitWorker) stashes
    /// into and the
    /// [`StateMachineAdapter`](super::state_machine_adapter::StateMachineAdapter)
    /// consumes from.
    pub fn with_staged_receipts(mut self, staged_receipts: Arc<StagedReceiptMap>) -> Self {
        self.staged_receipts = Some(staged_receipts);
        self
    }
}

impl fmt::Debug for RaftNameService {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RaftNameService").finish()
    }
}

/// Build the state-machine command an `IndexPublisher::publish_index`
/// call translates into. Extracted so the construction is testable
/// without spinning up a Raft instance.
fn build_advance_index_command(
    ledger_id: &str,
    index_t: i64,
    index_id: &ContentId,
) -> std::result::Result<SmCommand, NameServiceError> {
    let args = build_index_head_args(ledger_id, index_t, index_id)?;
    Ok(SmCommand::AdvanceIndexHead(args))
}

/// Build the state-machine command an
/// `AdminPublisher::publish_index_allow_equal` call translates into.
fn build_rewrite_index_command(
    ledger_id: &str,
    index_t: i64,
    index_id: &ContentId,
) -> std::result::Result<SmCommand, NameServiceError> {
    let args = build_index_head_args(ledger_id, index_t, index_id)?;
    Ok(SmCommand::RewriteIndexHead(args))
}

fn build_index_head_args(
    ledger_id: &str,
    index_t: i64,
    index_id: &ContentId,
) -> std::result::Result<AdvanceIndexHeadArgs, NameServiceError> {
    let (ledger_name, branch) = split_ledger_id(ledger_id)?;
    let applied_at_millis = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    Ok(AdvanceIndexHeadArgs {
        ledger_id: ledger_name,
        branch,
        new_index_head: index_id.clone(),
        t: index_t,
        applied_at_millis,
    })
}

/// Translate the apply outcome into the `IndexPublisher::publish_index`
/// result.
///
/// - [`SmResponse::IndexAdvanced`] / [`SmResponse::IndexStale`] →
///   `Ok(())`. Stale is the racing-indexer case: another publisher
///   landed at a `t` ≥ ours. The cluster's view of the latest index
///   is already at-least-as-fresh as the one we tried to publish.
/// - [`SmResponse::IndexAhead`] → `Err(Storage)`. The proposer's view
///   of `commit_t` was wrong (almost always: a leadership transition
///   where the new leader had reset to an older state). The caller's
///   indexer should re-stage against the current commit head.
/// - [`SmResponse::LedgerNotFound`] → `Err(not_found)`. Ledger gone
///   mid-build (drop / membership change).
/// - Anything else → `Err(Storage)` "unexpected variant". None of the
///   other variants are reachable for this command; if one appears
///   it's a state-machine bug worth surfacing rather than swallowing.
fn map_advance_index_response(resp: SmResponse) -> Result<()> {
    match resp {
        SmResponse::IndexAdvanced { .. } => Ok(()),
        // Stale = concurrent indexer published a t >= ours. The
        // cluster's view of the latest index is already at-least-as
        // fresh; nothing to surface.
        SmResponse::IndexStale { .. } => Ok(()),
        SmResponse::IndexAhead {
            commit_t,
            proposed_t,
        } => Err(NameServiceError::storage(format!(
            "raft AdvanceIndexHead rejected: index_t={proposed_t} > commit_t={commit_t} \
             (proposer ran ahead of applied state; re-stage from current commit head)"
        ))),
        SmResponse::LedgerNotFound { ledger_id } => Err(NameServiceError::not_found(ledger_id)),
        other => Err(NameServiceError::storage(format!(
            "unexpected Response variant for AdvanceIndexHead: {other:?}"
        ))),
    }
}

/// Construct an [`NsRecord`] from the state machine's view of a
/// single branch. Returns `None` when no [`LedgerRecord`] exists for
/// `ledger_name` or the branch isn't registered on it.
///
/// Fields the state machine doesn't track
/// (`default_context`/`config_id`) fall back to their `NsRecord::new`
/// defaults — see the module docs for why that's enough for
/// follower reload.
fn record_from_state(
    state: &NameServiceState,
    ledger_name: &str,
    branch: &str,
) -> Option<NsRecord> {
    let ledger = state.ledgers.get(ledger_name)?;
    if !ledger.branches.iter().any(|b| b == branch) {
        return None;
    }
    let mut record = NsRecord::new(ledger_name, branch);
    let ref_key = RefKey::new(ledger_name, branch);
    if let Some(entry) = state.refs.get(&ref_key) {
        record.commit_head_id = Some(entry.head.clone());
        record.commit_t = entry.t;
        if let Some(index) = &entry.index {
            record.index_head_id = Some(index.head.clone());
            record.index_t = index.t;
        }
        record.source_branch = entry.source_branch.clone();
        record.branches = entry.branches;
    }
    record.retracted = state.retracted.contains(&ref_key);
    Some(record)
}

#[async_trait]
impl NameServiceLookup for RaftNameService {
    async fn lookup(&self, ledger_id: &str) -> Result<Option<NsRecord>> {
        let (name, branch) = split_ledger_id(ledger_id)?;
        let state = self.state.read().await;
        Ok(record_from_state(&state, &name, &branch))
    }

    async fn all_records(&self) -> Result<Vec<NsRecord>> {
        let state = self.state.read().await;
        let mut records = Vec::new();
        for (ledger_name, ledger) in &state.ledgers {
            for branch in &ledger.branches {
                if let Some(record) = record_from_state(&state, ledger_name, branch) {
                    records.push(record);
                }
            }
        }
        Ok(records)
    }
}

#[async_trait]
impl IndexPublisher for RaftNameService {
    async fn publish_index(
        &self,
        ledger_id: &str,
        index_t: i64,
        index_id: &ContentId,
    ) -> Result<()> {
        let cmd = build_advance_index_command(ledger_id, index_t, index_id)?;
        match self.raft.client_write(cmd).await {
            Ok(resp) => map_advance_index_response(resp.data),
            // A stepped-down leader's straggling publish call. The
            // new leader will run its own build; nothing for us to
            // do except not propagate the error.
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(_))) => Ok(()),
            // ChangeMembershipError can't surface here — this
            // command isn't a membership change. Treat as
            // unreachable but report rather than panic.
            Err(RaftError::APIError(ClientWriteError::ChangeMembershipError(e))) => {
                Err(NameServiceError::storage(format!(
                    "unexpected ChangeMembershipError on AdvanceIndexHead: {e}"
                )))
            }
            Err(RaftError::Fatal(f)) => Err(NameServiceError::storage(format!(
                "raft fatal during AdvanceIndexHead: {f}"
            ))),
        }
    }
}

#[async_trait]
impl AdminPublisher for RaftNameService {
    async fn publish_index_allow_equal(
        &self,
        ledger_id: &str,
        index_t: i64,
        index_id: &ContentId,
    ) -> Result<()> {
        let cmd = build_rewrite_index_command(ledger_id, index_t, index_id)?;
        match self.raft.client_write(cmd).await {
            Ok(resp) => map_advance_index_response(resp.data),
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(_))) => Ok(()),
            Err(RaftError::APIError(ClientWriteError::ChangeMembershipError(e))) => {
                Err(NameServiceError::storage(format!(
                    "unexpected ChangeMembershipError on RewriteIndexHead: {e}"
                )))
            }
            Err(RaftError::Fatal(f)) => Err(NameServiceError::storage(format!(
                "raft fatal during RewriteIndexHead: {f}"
            ))),
        }
    }
}

/// Build the state-machine command a [`CommitPublisher::publish_commit`]
/// call translates into.
///
/// The `queue_id` is sampled from the current per-branch queue front:
/// the worker only ever calls `publish_commit` after staging the entry
/// currently at the front, so peeking here recovers the queue_id the
/// caller observed without requiring it to thread through the trait.
/// If the front shifts between peek and apply (admin clear, leader
/// change), the state machine returns [`SmResponse::QueueDesync`] and
/// the caller surfaces it as an error — the race is the same one
/// `QueueDesync` was designed to catch.
fn build_apply_head_command(
    state: &NameServiceState,
    ledger_id: &str,
    commit_t: i64,
    commit_id: &ContentId,
    staged_receipts: Option<&StagedReceiptMap>,
) -> std::result::Result<SmCommand, NameServiceError> {
    let (ledger_name, branch) = split_ledger_id(ledger_id)?;
    let ref_key = RefKey::new(&ledger_name, &branch);
    let queue_id = state
        .queues
        .get(&ref_key)
        .and_then(|q| q.front())
        .map(|entry| entry.queue_id)
        .ok_or_else(|| {
            NameServiceError::storage(format!(
                "publish_commit on {ledger_id}: per-branch queue is empty — \
                 nothing staged for this branch"
            ))
        })?;
    let applied_at_millis = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let metadata = staged_receipts.and_then(|s| s.peek_transact_metadata(queue_id));
    let tally = metadata
        .as_ref()
        .and_then(|m| m.tally.as_ref())
        .map(RecordedTally::from);
    let flake_count = metadata.map(|m| m.flake_count as u64).unwrap_or(0);
    Ok(SmCommand::ApplyHead(ApplyHeadArgs {
        ledger_id: ledger_name,
        branch,
        queue_id,
        commit_id: commit_id.clone(),
        commit_t,
        applied_at_millis,
        tally,
        flake_count,
    }))
}

/// Translate the apply outcome into the `CommitPublisher::publish_commit`
/// result.
///
/// - [`SmResponse::HeadApplied`] → `Ok(())`.
/// - [`SmResponse::QueueDesync`] → `Err(Storage)` with the reason
///   inlined so callers can decide whether to retry. Common causes are
///   admin preemption (`QueueCleared`), former-leader straggler
///   proposals (`WrongFront`), or a state-machine invariant break.
/// - [`SmResponse::LedgerNotFound`] → `Err(not_found)`.
/// - Anything else → `Err(Storage)`. None of the other variants are
///   reachable for an ApplyHead command; if one appears it's a
///   state-machine bug worth surfacing rather than swallowing.
fn map_apply_head_response(resp: SmResponse) -> Result<()> {
    match resp {
        SmResponse::HeadApplied { .. } => Ok(()),
        SmResponse::QueueDesync {
            ledger_id,
            requested_queue_id,
            reason,
        } => Err(NameServiceError::storage(format!(
            "raft ApplyHead desynced on {ledger_id} (queue_id={requested_queue_id}): \
             {}",
            describe_desync_reason(&reason)
        ))),
        SmResponse::LedgerNotFound { ledger_id } => Err(NameServiceError::not_found(ledger_id)),
        other => Err(NameServiceError::storage(format!(
            "unexpected Response variant for ApplyHead: {other:?}"
        ))),
    }
}

fn describe_desync_reason(reason: &DesyncReason) -> String {
    match reason {
        DesyncReason::WrongFront { actual_queue_id } => {
            format!("queue front is now {actual_queue_id}")
        }
        DesyncReason::QueueCleared { reason } => {
            format!("queue cleared by admin command ({reason:?})")
        }
        DesyncReason::InvariantViolated { description } => {
            format!("state-machine invariant violated: {description}")
        }
    }
}

#[async_trait]
impl CommitPublisher for RaftNameService {
    async fn publish_commit(
        &self,
        ledger_id: &str,
        commit_t: i64,
        commit_id: &ContentId,
    ) -> Result<()> {
        // Peek the queue front under the read lock; `client_write`
        // happens after the lock drops so we don't hold it across an
        // async wait. The lock-then-propose sequence is safe because
        // the state machine validates the queue_id at apply time.
        let cmd = {
            let state = self.state.read().await;
            build_apply_head_command(
                &state,
                ledger_id,
                commit_t,
                commit_id,
                self.staged_receipts.as_deref(),
            )?
        };

        match self.raft.client_write(cmd).await {
            Ok(resp) => map_apply_head_response(resp.data),
            // Stepped-down leader: the new leader's worker will
            // observe the same queue entry and re-stage. We must
            // surface this as an error (unlike `publish_index`) so
            // the caller's stash-cleanup runs — silently returning
            // Ok would tell the worker the head landed, leaving the
            // staged receipt in place to later override the
            // genuinely-committed receipt from the new leader.
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(_))) => {
                Err(NameServiceError::storage(
                    "ApplyHead forwarded to leader (stepped down between stage and propose); \
                     caller should drop the stash and let the new leader's worker re-stage"
                        .to_string(),
                ))
            }
            Err(RaftError::APIError(ClientWriteError::ChangeMembershipError(e))) => {
                Err(NameServiceError::storage(format!(
                    "unexpected ChangeMembershipError on ApplyHead: {e}"
                )))
            }
            Err(RaftError::Fatal(f)) => Err(NameServiceError::storage(format!(
                "raft fatal during ApplyHead: {f}"
            ))),
        }
    }

    /// The state-machine carries every published commit under its
    /// `ledger_id:branch` alias, so the alias to write into the
    /// commit's `ns` field is the same string the caller passed in.
    /// Private publishing isn't a concept here — the cluster is the
    /// only nameservice.
    fn publishing_ledger_id(&self, ledger_id: &str) -> Option<String> {
        Some(ledger_id.to_string())
    }
}

/// Build the state-machine command for [`LedgerLifecycle::init`].
fn build_create_command(ledger_id: &str) -> std::result::Result<SmCommand, NameServiceError> {
    let (ledger_name, branch) = split_ledger_id(ledger_id)?;
    let applied_at_millis = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    Ok(SmCommand::CreateLedger(CreateLedgerArgs {
        ledger_id: ledger_name,
        branch,
        created_at_millis: applied_at_millis,
    }))
}

fn build_retract_command(ledger_id: &str) -> std::result::Result<SmCommand, NameServiceError> {
    let (ledger_name, branch) = split_ledger_id(ledger_id)?;
    Ok(SmCommand::RetractLedger {
        ledger_id: ledger_name,
        branch,
        applied_at_millis: current_millis(),
    })
}

fn build_purge_command(ledger_id: &str) -> std::result::Result<SmCommand, NameServiceError> {
    let (ledger_name, branch) = split_ledger_id(ledger_id)?;
    Ok(SmCommand::PurgeLedger {
        ledger_id: ledger_name,
        branch,
        applied_at_millis: current_millis(),
    })
}

fn map_create_response(resp: SmResponse) -> Result<()> {
    match resp {
        SmResponse::Created { .. } => Ok(()),
        SmResponse::AlreadyExists { ledger_id } => {
            Err(NameServiceError::ledger_already_exists(ledger_id))
        }
        other => Err(NameServiceError::storage(format!(
            "unexpected Response variant for CreateLedger: {other:?}"
        ))),
    }
}

fn map_retract_response(resp: SmResponse) -> Result<()> {
    match resp {
        SmResponse::Retracted { .. } | SmResponse::AlreadyRetracted { .. } => Ok(()),
        other => Err(NameServiceError::storage(format!(
            "unexpected Response variant for RetractLedger: {other:?}"
        ))),
    }
}

fn map_purge_response(resp: SmResponse) -> Result<()> {
    match resp {
        SmResponse::Purged { .. } | SmResponse::AlreadyPurged { .. } => Ok(()),
        other => Err(NameServiceError::storage(format!(
            "unexpected Response variant for PurgeLedger: {other:?}"
        ))),
    }
}

impl RaftNameService {
    /// Submit a lifecycle command through Raft and surface the
    /// response. Maps `ForwardToLeader` to a storage error so callers
    /// (typically the HTTP route layer) can redirect — silently
    /// swallowing a lifecycle failure would let the client believe
    /// the change took effect when it didn't.
    async fn submit_lifecycle(&self, cmd: SmCommand) -> Result<SmResponse> {
        match self.raft.client_write(cmd).await {
            Ok(resp) => Ok(resp.data),
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(leader))) => {
                Err(NameServiceError::storage(format!(
                    "raft client_write rejected — not leader (leader: {leader:?})"
                )))
            }
            Err(RaftError::APIError(ClientWriteError::ChangeMembershipError(e))) => {
                Err(NameServiceError::storage(format!(
                    "unexpected ChangeMembershipError on lifecycle command: {e}"
                )))
            }
            Err(RaftError::Fatal(f)) => Err(NameServiceError::storage(format!(
                "raft fatal during lifecycle command: {f}"
            ))),
        }
    }
}

#[async_trait]
impl LedgerLifecycle for RaftNameService {
    async fn init(&self, ledger_id: &str) -> Result<()> {
        let cmd = build_create_command(ledger_id)?;
        map_create_response(self.submit_lifecycle(cmd).await?)
    }

    async fn retract(&self, ledger_id: &str) -> Result<()> {
        let cmd = build_retract_command(ledger_id)?;
        map_retract_response(self.submit_lifecycle(cmd).await?)
    }

    async fn purge(&self, ledger_id: &str) -> Result<()> {
        let cmd = build_purge_command(ledger_id)?;
        map_purge_response(self.submit_lifecycle(cmd).await?)
    }
}

fn build_create_branch_command(
    ledger_name: &str,
    new_branch: &str,
    source_branch: &str,
    at_commit: Option<(ContentId, i64)>,
) -> std::result::Result<SmCommand, NameServiceError> {
    let applied_at_millis = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    Ok(SmCommand::CreateBranch(CreateBranchArgs {
        ledger_id: ledger_name.into(),
        branch: new_branch.into(),
        source_branch: source_branch.into(),
        at_commit,
        applied_at_millis,
    }))
}

fn build_drop_branch_command(ledger_id: &str) -> std::result::Result<SmCommand, NameServiceError> {
    let (ledger_name, branch) = split_ledger_id(ledger_id)?;
    Ok(SmCommand::DropBranch {
        ledger_id: ledger_name,
        branch,
        applied_at_millis: current_millis(),
    })
}

fn build_reset_head_command(
    ledger_id: &str,
    snapshot: NsRecordSnapshot,
) -> std::result::Result<SmCommand, NameServiceError> {
    let (ledger_name, branch) = split_ledger_id(ledger_id)?;
    Ok(SmCommand::ResetHead {
        ledger_id: ledger_name,
        branch,
        snapshot: ResetHeadSnapshot {
            commit_head_id: snapshot.commit_head_id,
            commit_t: snapshot.commit_t,
            index_head_id: snapshot.index_head_id,
            index_t: snapshot.index_t,
        },
        applied_at_millis: current_millis(),
    })
}

fn current_millis() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn map_create_branch_response(resp: SmResponse) -> Result<()> {
    match resp {
        SmResponse::BranchCreated { .. } => Ok(()),
        SmResponse::AlreadyExists { ledger_id } => {
            Err(NameServiceError::ledger_already_exists(ledger_id))
        }
        SmResponse::LedgerNotFound { ledger_id }
        | SmResponse::SourceBranchNotFound { ledger_id } => {
            Err(NameServiceError::not_found(ledger_id))
        }
        other => Err(NameServiceError::storage(format!(
            "unexpected Response variant for CreateBranch: {other:?}"
        ))),
    }
}

fn map_drop_branch_response(resp: SmResponse) -> Result<Option<u32>> {
    match resp {
        SmResponse::BranchDropped {
            parent_branches, ..
        } => Ok(parent_branches),
        SmResponse::BranchHasChildren {
            ledger_id,
            children,
        } => Err(NameServiceError::storage(format!(
            "drop_branch refused: {ledger_id} still has {children} child branch(es)"
        ))),
        SmResponse::LedgerNotFound { ledger_id } => Err(NameServiceError::not_found(ledger_id)),
        other => Err(NameServiceError::storage(format!(
            "unexpected Response variant for DropBranch: {other:?}"
        ))),
    }
}

fn map_reset_head_response(resp: SmResponse) -> Result<()> {
    match resp {
        SmResponse::HeadReset { .. } => Ok(()),
        SmResponse::LedgerNotFound { ledger_id } => Err(NameServiceError::not_found(ledger_id)),
        other => Err(NameServiceError::storage(format!(
            "unexpected Response variant for ResetHead: {other:?}"
        ))),
    }
}

#[async_trait]
impl BranchLifecycle for RaftNameService {
    async fn create_branch(
        &self,
        ledger_name: &str,
        new_branch: &str,
        source_branch: &str,
        at_commit: Option<(ContentId, i64)>,
    ) -> Result<()> {
        let cmd = build_create_branch_command(ledger_name, new_branch, source_branch, at_commit)?;
        map_create_branch_response(self.submit_lifecycle(cmd).await?)
    }

    async fn drop_branch(&self, ledger_id: &str) -> Result<Option<u32>> {
        let cmd = build_drop_branch_command(ledger_id)?;
        map_drop_branch_response(self.submit_lifecycle(cmd).await?)
    }

    async fn reset_head(&self, ledger_id: &str, snapshot: NsRecordSnapshot) -> Result<()> {
        let cmd = build_reset_head_command(ledger_id, snapshot)?;
        map_reset_head_response(self.submit_lifecycle(cmd).await?)
    }
}

#[async_trait]
impl RefLookup for RaftNameService {
    async fn get_ref(&self, ledger_id: &str, kind: RefKind) -> Result<Option<RefValue>> {
        let (name, branch) = split_ledger_id(ledger_id)?;
        let state = self.state.read().await;
        if !state.ledgers.contains_key(&name) {
            return Ok(None);
        }
        // Retracted branches are tombstoned — the `lookup` surface
        // returns the full `NsRecord` so tools can see the
        // `retracted: true` flag, but the active-read surface
        // (`get_ref`) reports the branch as gone. Without this
        // pairing the data-plane query path would happily resolve
        // a head ref for a branch the operator soft-deleted.
        let ref_key = RefKey::new(&name, &branch);
        if state.retracted.contains(&ref_key) {
            return Ok(None);
        }
        match kind {
            RefKind::CommitHead => {
                let entry = state.refs.get(&ref_key);
                Ok(Some(RefValue {
                    id: entry.map(|e| e.head.clone()),
                    t: entry.map(|e| e.t).unwrap_or(0),
                }))
            }
            RefKind::IndexHead => {
                let index = state.refs.get(&ref_key).and_then(|e| e.index.as_ref());
                Ok(Some(RefValue {
                    id: index.map(|i| i.head.clone()),
                    t: index.map(|i| i.t).unwrap_or(0),
                }))
            }
        }
    }
}

#[async_trait]
impl RefPublisher for RaftNameService {
    async fn compare_and_set_ref(
        &self,
        ledger_id: &str,
        kind: RefKind,
        expected: Option<&RefValue>,
        new: &RefValue,
    ) -> Result<CasResult> {
        let (ledger_name, branch) = split_ledger_id(ledger_id)?;
        let cmd = SmCommand::CompareAndSetRef {
            ledger_id: ledger_name,
            branch,
            kind,
            expected: expected.cloned(),
            new: new.clone(),
            applied_at_millis: current_millis(),
        };
        match self.submit_lifecycle(cmd).await? {
            SmResponse::RefCasUpdated => Ok(CasResult::Updated),
            SmResponse::RefCasConflict { actual } => Ok(CasResult::Conflict { actual }),
            SmResponse::LedgerNotFound { ledger_id } => Err(NameServiceError::not_found(ledger_id)),
            // `IndexAhead` from an `IndexHead` CAS proposing past
            // the branch's commit watermark maps to a `Conflict`
            // with no actual value.
            SmResponse::IndexAhead { .. } => Ok(CasResult::Conflict { actual: None }),
            other => Err(NameServiceError::storage(format!(
                "unexpected Response variant for CompareAndSetRef: {other:?}"
            ))),
        }
    }
}

#[async_trait]
impl GraphSourceLookup for RaftNameService {
    async fn lookup_graph_source(
        &self,
        graph_source_id: &str,
    ) -> Result<Option<GraphSourceRecord>> {
        let state = self.state.read().await;
        Ok(state.graph_sources.get(graph_source_id).cloned())
    }

    /// Resolve `resource_id` against the ledger map first, then the
    /// graph-source map. Ledger ids and graph-source ids share the
    /// `name:branch` namespace; the ledger record wins on collision.
    async fn lookup_any(&self, resource_id: &str) -> Result<NsLookupResult> {
        if let Some(record) = self.lookup(resource_id).await? {
            return Ok(NsLookupResult::Ledger(record));
        }
        let state = self.state.read().await;
        if let Some(record) = state.graph_sources.get(resource_id).cloned() {
            return Ok(NsLookupResult::GraphSource(record));
        }
        Ok(NsLookupResult::NotFound)
    }

    async fn all_graph_source_records(&self) -> Result<Vec<GraphSourceRecord>> {
        let state = self.state.read().await;
        Ok(state.graph_sources.values().cloned().collect())
    }
}

#[async_trait]
impl GraphSourcePublisher for RaftNameService {
    async fn publish_graph_source(
        &self,
        name: &str,
        branch: &str,
        source_type: GraphSourceType,
        config: &str,
        dependencies: &[String],
    ) -> Result<()> {
        let cmd = SmCommand::PublishGraphSource {
            name: name.to_string(),
            branch: branch.to_string(),
            source_type,
            config: config.to_string(),
            dependencies: dependencies.to_vec(),
        };
        match self.submit_lifecycle(cmd).await? {
            SmResponse::GraphSourcePublished => Ok(()),
            other => Err(NameServiceError::storage(format!(
                "unexpected Response variant for PublishGraphSource: {other:?}"
            ))),
        }
    }

    async fn publish_graph_source_index(
        &self,
        name: &str,
        branch: &str,
        index_id: &ContentId,
        index_t: i64,
    ) -> Result<()> {
        let cmd = SmCommand::PublishGraphSourceIndex {
            name: name.to_string(),
            branch: branch.to_string(),
            index_id: index_id.clone(),
            index_t,
        };
        match self.submit_lifecycle(cmd).await? {
            SmResponse::GraphSourceIndexAdvanced { .. }
            | SmResponse::GraphSourceIndexStale { .. }
            | SmResponse::GraphSourceNotFound { .. } => Ok(()),
            other => Err(NameServiceError::storage(format!(
                "unexpected Response variant for PublishGraphSourceIndex: {other:?}"
            ))),
        }
    }

    async fn retract_graph_source(&self, name: &str, branch: &str) -> Result<()> {
        let cmd = SmCommand::RetractGraphSource {
            name: name.to_string(),
            branch: branch.to_string(),
        };
        match self.submit_lifecycle(cmd).await? {
            SmResponse::GraphSourceRetracted { .. }
            | SmResponse::GraphSourceAlreadyRetracted { .. } => Ok(()),
            other => Err(NameServiceError::storage(format!(
                "unexpected Response variant for RetractGraphSource: {other:?}"
            ))),
        }
    }
}

#[async_trait]
impl StatusLookup for RaftNameService {
    async fn get_status(&self, ledger_id: &str) -> Result<Option<StatusValue>> {
        let (name, branch) = split_ledger_id(ledger_id)?;
        let state = self.state.read().await;
        let branch_registered = state
            .ledgers
            .get(&name)
            .is_some_and(|l| l.branches.iter().any(|b| b == &branch));
        if !branch_registered {
            return Ok(None);
        }
        Ok(Some(
            state
                .status
                .get(ledger_id)
                .cloned()
                .unwrap_or_else(StatusValue::initial),
        ))
    }
}

#[async_trait]
impl StatusPublisher for RaftNameService {
    async fn push_status(
        &self,
        ledger_id: &str,
        expected: Option<&StatusValue>,
        new: &StatusValue,
    ) -> Result<StatusCasResult> {
        let cmd = SmCommand::PushStatus {
            ledger_id: ledger_id.to_string(),
            expected: expected.cloned(),
            new: new.clone(),
        };
        match self.submit_lifecycle(cmd).await? {
            SmResponse::StatusUpdated => Ok(StatusCasResult::Updated),
            SmResponse::StatusConflict { actual } => Ok(StatusCasResult::Conflict { actual }),
            other => Err(NameServiceError::storage(format!(
                "unexpected Response variant for PushStatus: {other:?}"
            ))),
        }
    }
}

#[async_trait]
impl ConfigLookup for RaftNameService {
    async fn get_config(&self, ledger_id: &str) -> Result<Option<ConfigValue>> {
        let (name, branch) = split_ledger_id(ledger_id)?;
        let state = self.state.read().await;
        let branch_registered = state
            .ledgers
            .get(&name)
            .is_some_and(|l| l.branches.iter().any(|b| b == &branch));
        if !branch_registered {
            return Ok(None);
        }
        Ok(Some(
            state
                .config
                .get(ledger_id)
                .cloned()
                .unwrap_or_else(ConfigValue::unborn),
        ))
    }
}

#[async_trait]
impl ConfigPublisher for RaftNameService {
    async fn push_config(
        &self,
        ledger_id: &str,
        expected: Option<&ConfigValue>,
        new: &ConfigValue,
    ) -> Result<ConfigCasResult> {
        let cmd = SmCommand::PushConfig(Box::new(PushConfigArgs {
            ledger_id: ledger_id.to_string(),
            expected: expected.cloned(),
            new: new.clone(),
        }));
        match self.submit_lifecycle(cmd).await? {
            SmResponse::ConfigUpdated => Ok(ConfigCasResult::Updated),
            SmResponse::ConfigConflict { actual } => Ok(ConfigCasResult::Conflict { actual }),
            other => Err(NameServiceError::storage(format!(
                "unexpected Response variant for PushConfig: {other:?}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    //! Two test surfaces here:
    //!
    //! - **Pure helpers** ([`build_advance_index_command`],
    //!   [`map_advance_index_response`]) — synchronous, no Raft, no
    //!   state. Verify the command construction and outcome mapping
    //!   the publisher side of `IndexPublisher` translates through.
    //! - **`NameServiceLookup` impl** — drive `SharedState`
    //!   directly via the state-machine's `apply` function, then
    //!   exercise `lookup` / `get_ref` / `all_records`. Each test
    //!   needs a `RaftNameService`, which now requires an
    //!   `Arc<Raft<TypeConfig>>`. We bootstrap a stub Raft once
    //!   per test via [`stub_raft`]; the publish-time roundtrip
    //!   through a live Raft cluster lives in
    //!   `tests/single_node_round_trip.rs`.

    use super::*;
    use crate::raft::state_machine::{
        AdvanceIndexHeadArgs, Command, CreateLedgerArgs, NameServiceState, RefEntry, Response,
    };
    use crate::raft::{ClusterNode, NodeId};
    use fluree_db_api::{ContentId, ContentKind};
    use openraft::error::{InstallSnapshotError, RPCError, RaftError};
    use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
    use openraft::raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    };
    use openraft::Config;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    fn cid(seed: u8) -> ContentId {
        ContentId::new(ContentKind::Commit, &[seed])
    }

    fn fresh_state() -> SharedState {
        Arc::new(RwLock::new(NameServiceState::default()))
    }

    async fn apply_cmd(state: &SharedState, cmd: Command, index: u64) -> Response {
        let mut guard = state.write().await;
        crate::raft::state_machine::apply(&mut guard, cmd, index)
    }

    /// Slim init command: registers `(ledger_id, branch)` on the
    /// state machine. Leaves the branch unborn — the caller follows
    /// with `AdvanceRef(expected_prev=None, …)` to seed the head.
    fn init_cmd(ledger_id: &str, branch: &str) -> Command {
        Command::CreateLedger(CreateLedgerArgs {
            ledger_id: ledger_id.into(),
            branch: branch.into(),
            created_at_millis: 1_000,
        })
    }

    /// Network-factory stub. `RaftNameService` tests in this module
    /// only exercise the read paths and the pure publisher helpers
    /// — the constructed `Raft` is never asked to make outbound RPC
    /// calls. If openraft ever does invoke one of these in a unit
    /// test, that's a real bug to investigate and a panic surfaces
    /// it louder than a quiet stub.
    struct StubFactory;
    struct StubNetwork;

    impl RaftNetworkFactory<crate::raft::TypeConfig> for StubFactory {
        type Network = StubNetwork;

        async fn new_client(&mut self, _target: NodeId, _node: &ClusterNode) -> Self::Network {
            StubNetwork
        }
    }

    impl RaftNetwork<crate::raft::TypeConfig> for StubNetwork {
        async fn append_entries(
            &mut self,
            _rpc: AppendEntriesRequest<crate::raft::TypeConfig>,
            _option: RPCOption,
        ) -> std::result::Result<
            AppendEntriesResponse<NodeId>,
            RPCError<NodeId, ClusterNode, RaftError<NodeId>>,
        > {
            panic!("unit-test Raft should not invoke append_entries");
        }

        async fn install_snapshot(
            &mut self,
            _rpc: InstallSnapshotRequest<crate::raft::TypeConfig>,
            _option: RPCOption,
        ) -> std::result::Result<
            InstallSnapshotResponse<NodeId>,
            RPCError<NodeId, ClusterNode, RaftError<NodeId, InstallSnapshotError>>,
        > {
            panic!("unit-test Raft should not invoke install_snapshot");
        }

        async fn vote(
            &mut self,
            _rpc: VoteRequest<NodeId>,
            _option: RPCOption,
        ) -> std::result::Result<
            VoteResponse<NodeId>,
            RPCError<NodeId, ClusterNode, RaftError<NodeId>>,
        > {
            panic!("unit-test Raft should not invoke vote");
        }
    }

    /// Build an idle `Raft` handle for read-side tests. The
    /// returned handle is never initialized into a cluster — the
    /// tests only exercise the data path that reads `SharedState`,
    /// not the openraft consensus loop.
    async fn stub_raft() -> Arc<Raft<crate::raft::TypeConfig>> {
        use crate::raft::log_adapter::LogAdapter;
        use crate::raft::state_machine_adapter::StateMachineAdapter;
        use crate::raft::storage::memory::MemoryRaftStorage;

        let storage = Arc::new(MemoryRaftStorage::new());
        let log = LogAdapter::new(Arc::clone(&storage));
        let sm = StateMachineAdapter::new(Arc::clone(&storage));
        let config = Arc::new(Config::default().validate().expect("config validates"));
        Arc::new(
            Raft::new(1, config, StubFactory, log, sm)
                .await
                .expect("stub raft constructs"),
        )
    }

    // ----------------------------------------------------------------
    // Pure publish-helper tests
    // ----------------------------------------------------------------

    #[test]
    fn build_advance_index_command_splits_ledger_id_into_name_and_branch() {
        let cmd = build_advance_index_command("test/db:main", 7, &cid(42)).expect("build");
        let SmCommand::AdvanceIndexHead(args) = cmd else {
            panic!("expected AdvanceIndexHead");
        };
        assert_eq!(args.ledger_id, "test/db");
        assert_eq!(args.branch, "main");
        assert_eq!(args.new_index_head, cid(42));
        assert_eq!(args.t, 7);
        assert!(args.applied_at_millis > 0);
    }

    #[test]
    fn build_advance_index_command_defaults_branch_when_omitted() {
        let cmd = build_advance_index_command("test/db", 7, &cid(42)).expect("build");
        let SmCommand::AdvanceIndexHead(args) = cmd else {
            panic!("expected AdvanceIndexHead");
        };
        assert_eq!(args.ledger_id, "test/db");
        assert_eq!(args.branch, "main");
    }

    #[test]
    fn build_advance_index_command_rejects_empty_ledger_id() {
        assert!(build_advance_index_command("", 7, &cid(42)).is_err());
    }

    #[test]
    fn map_advance_index_response_advanced_is_ok() {
        let r = map_advance_index_response(SmResponse::IndexAdvanced {
            index_t: 5,
            index_head: cid(1),
        });
        assert!(r.is_ok());
    }

    #[test]
    fn map_advance_index_response_stale_is_ok() {
        let r = map_advance_index_response(SmResponse::IndexStale { current_t: 9 });
        assert!(r.is_ok());
    }

    #[test]
    fn map_advance_index_response_ahead_is_err_with_both_t_values_in_message() {
        let r = map_advance_index_response(SmResponse::IndexAhead {
            commit_t: 3,
            proposed_t: 9,
        });
        let msg = r.expect_err("ahead is error").to_string();
        assert!(msg.contains("commit_t=3"), "got: {msg}");
        assert!(msg.contains("index_t=9"), "got: {msg}");
    }

    #[test]
    fn map_advance_index_response_ledger_not_found_is_err() {
        let r = map_advance_index_response(SmResponse::LedgerNotFound {
            ledger_id: "gone/db".into(),
        });
        let msg = r.expect_err("ledger not found is error").to_string();
        assert!(msg.contains("gone/db"), "got: {msg}");
    }

    #[test]
    fn map_advance_index_response_unexpected_variant_is_err() {
        // `NoOp` can't be the state-machine reply to AdvanceIndexHead,
        // but if it ever were, the publisher should surface rather
        // than swallow.
        assert!(map_advance_index_response(SmResponse::NoOp).is_err());
    }

    // ----------------------------------------------------------------
    // build_apply_head_command + map_apply_head_response
    // ----------------------------------------------------------------

    fn install_queue_front(state: &mut NameServiceState, ledger_id: &str, queue_id: u64) {
        use crate::raft::state_machine::{BodyKind, QueueEntry};
        use std::collections::VecDeque;
        let (ledger_name, branch) = split_ledger_id(ledger_id).expect("test ledger_id parses");
        let mut queue = VecDeque::new();
        queue.push_back(QueueEntry {
            queue_id,
            enqueued_index: 1,
            enqueued_at_millis: 1_000,
            idempotency: None,
            request_cid: cid(0),
            body_cid: cid(0),
            body_kind: BodyKind::JsonLdInsert,
        });
        state
            .queues
            .insert(RefKey::new(&ledger_name, &branch), queue);
    }

    #[test]
    fn build_apply_head_command_samples_queue_front_queue_id() {
        let mut state = NameServiceState::default();
        install_queue_front(&mut state, "test/db:main", 42);

        let cmd =
            build_apply_head_command(&state, "test/db:main", 7, &cid(99), None).expect("build");
        let SmCommand::ApplyHead(args) = cmd else {
            panic!("expected ApplyHead");
        };
        assert_eq!(args.ledger_id, "test/db");
        assert_eq!(args.branch, "main");
        assert_eq!(args.queue_id, 42);
        assert_eq!(args.commit_id, cid(99));
        assert_eq!(args.commit_t, 7);
        assert!(args.applied_at_millis > 0);
    }

    #[test]
    fn build_apply_head_command_errors_when_queue_empty() {
        // The state machine guarantees that worker calls to
        // publish_commit happen only when a queue entry exists; an
        // empty queue means somebody routed a stage through here
        // without enqueueing first, which is a bug worth surfacing.
        let state = NameServiceState::default();
        let err = build_apply_head_command(&state, "test/db:main", 7, &cid(99), None)
            .expect_err("expected empty-queue error");
        assert!(err.to_string().contains("queue is empty"), "got: {err}");
    }

    #[test]
    fn build_apply_head_command_rejects_invalid_ledger_id() {
        let state = NameServiceState::default();
        assert!(build_apply_head_command(&state, "", 7, &cid(99), None).is_err());
    }

    #[test]
    fn map_apply_head_response_head_applied_is_ok() {
        let r = map_apply_head_response(SmResponse::HeadApplied {
            ledger_id: "test/db:main".into(),
            commit_id: cid(1),
            commit_t: 1,
        });
        assert!(r.is_ok());
    }

    #[test]
    fn map_apply_head_response_wrong_front_surfaces_actual_queue_id() {
        let r = map_apply_head_response(SmResponse::QueueDesync {
            ledger_id: "test/db:main".into(),
            requested_queue_id: 7,
            reason: DesyncReason::WrongFront {
                actual_queue_id: 12,
            },
        });
        let msg = r.expect_err("desync is error").to_string();
        assert!(msg.contains("queue_id=7"), "got: {msg}");
        assert!(msg.contains("12"), "got: {msg}");
    }

    #[test]
    fn map_apply_head_response_queue_cleared_surfaces_clear_reason() {
        use crate::raft::state_machine::ClearReason;
        let r = map_apply_head_response(SmResponse::QueueDesync {
            ledger_id: "test/db:main".into(),
            requested_queue_id: 7,
            reason: DesyncReason::QueueCleared {
                reason: ClearReason::BranchDropped,
            },
        });
        let msg = r.expect_err("desync is error").to_string();
        assert!(msg.contains("BranchDropped"), "got: {msg}");
    }

    #[test]
    fn map_apply_head_response_invariant_violated_surfaces_description() {
        let r = map_apply_head_response(SmResponse::QueueDesync {
            ledger_id: "test/db:main".into(),
            requested_queue_id: 0,
            reason: DesyncReason::InvariantViolated {
                description: "queue missing".into(),
            },
        });
        let msg = r.expect_err("desync is error").to_string();
        assert!(msg.contains("queue missing"), "got: {msg}");
    }

    #[test]
    fn map_apply_head_response_ledger_not_found_is_err() {
        let r = map_apply_head_response(SmResponse::LedgerNotFound {
            ledger_id: "gone/db".into(),
        });
        let msg = r.expect_err("not-found is error").to_string();
        assert!(msg.contains("gone/db"), "got: {msg}");
    }

    #[test]
    fn map_apply_head_response_unexpected_variant_is_err() {
        assert!(map_apply_head_response(SmResponse::NoOp).is_err());
    }

    // ----------------------------------------------------------------
    // CommitPublisher::publishing_ledger_id
    // ----------------------------------------------------------------

    #[tokio::test]
    async fn publishing_ledger_id_echoes_input() {
        let ns = RaftNameService::new(fresh_state(), stub_raft().await);
        assert_eq!(
            ns.publishing_ledger_id("test/db:main"),
            Some("test/db:main".to_string())
        );
    }

    // ----------------------------------------------------------------
    // NameServiceLookup tests
    // ----------------------------------------------------------------

    /// Direct state mutation that seeds a branch head without going
    /// through the queue. Used by lookup-side tests that need a
    /// populated head but aren't exercising the apply pipeline.
    async fn seed_head(
        state: &SharedState,
        ledger_id: &str,
        branch: &str,
        head: ContentId,
        t: i64,
    ) {
        let mut guard = state.write().await;
        if let Some(ledger) = guard.ledgers.get_mut(ledger_id) {
            if !ledger.branches.iter().any(|b| b == branch) {
                ledger.branches.push(branch.to_string());
            }
        }
        let ref_key = RefKey::new(ledger_id, branch);
        let (prior_index, prior_source, prior_branches) = guard
            .refs
            .get(&ref_key)
            .map(|r| (r.index.clone(), r.source_branch.clone(), r.branches))
            .unwrap_or_default();
        guard.refs.insert(
            ref_key,
            RefEntry {
                head,
                t,
                last_advanced_at_millis: 2_000,
                last_advanced_index: 1,
                index: prior_index,
                source_branch: prior_source,
                branches: prior_branches,
            },
        );
    }

    #[tokio::test]
    async fn lookup_returns_none_when_ledger_missing() {
        let ns = RaftNameService::new(fresh_state(), stub_raft().await);
        assert!(ns.lookup("test/db:main").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn lookup_returns_record_with_head() {
        let state = fresh_state();
        let _ = apply_cmd(&state, init_cmd("test/db", "main"), 1).await;
        seed_head(&state, "test/db", "main", cid(5), 7).await;

        let ns = RaftNameService::new(state, stub_raft().await);
        let record = ns.lookup("test/db:main").await.unwrap().expect("record");
        assert_eq!(record.ledger_id, "test/db:main");
        assert_eq!(record.commit_head_id, Some(cid(5)));
        assert_eq!(record.commit_t, 7);
        assert_eq!(record.index_head_id, None);
    }

    #[tokio::test]
    async fn get_ref_returns_head_for_commit_kind() {
        let state = fresh_state();
        apply_cmd(&state, init_cmd("test/db", "main"), 1).await;
        seed_head(&state, "test/db", "main", cid(9), 3).await;

        let ns = RaftNameService::new(state, stub_raft().await);
        let ref_value = ns
            .get_ref("test/db:main", RefKind::CommitHead)
            .await
            .unwrap()
            .expect("ref value");
        assert_eq!(ref_value.id, Some(cid(9)));
        assert_eq!(ref_value.t, 3);

        let index_ref = ns
            .get_ref("test/db:main", RefKind::IndexHead)
            .await
            .unwrap()
            .expect("index ref");
        assert!(index_ref.id.is_none());
        assert_eq!(index_ref.t, 0);
    }

    /// Convenience for the index-head tests: create a ledger and
    /// seed its commit head + t. Returns the shared state.
    async fn ledger_at_commit(commit_head: u8, commit_t: i64) -> SharedState {
        let state = fresh_state();
        let _ = apply_cmd(&state, init_cmd("test/db", "main"), 1).await;
        seed_head(&state, "test/db", "main", cid(commit_head), commit_t).await;
        state
    }

    #[tokio::test]
    async fn lookup_returns_index_after_advance_index_head() {
        let state = ledger_at_commit(7, 10).await;
        let _ = apply_cmd(
            &state,
            Command::AdvanceIndexHead(AdvanceIndexHeadArgs {
                ledger_id: "test/db".into(),
                branch: "main".into(),
                new_index_head: cid(42),
                t: 10,
                applied_at_millis: 3_000,
            }),
            3,
        )
        .await;

        let ns = RaftNameService::new(state, stub_raft().await);
        let record = ns.lookup("test/db:main").await.unwrap().expect("record");
        assert_eq!(record.commit_head_id, Some(cid(7)));
        assert_eq!(record.commit_t, 10);
        assert_eq!(record.index_head_id, Some(cid(42)));
        assert_eq!(record.index_t, 10);
    }

    #[tokio::test]
    async fn get_ref_returns_index_head_after_advance() {
        let state = ledger_at_commit(7, 10).await;
        let _ = apply_cmd(
            &state,
            Command::AdvanceIndexHead(AdvanceIndexHeadArgs {
                ledger_id: "test/db".into(),
                branch: "main".into(),
                new_index_head: cid(42),
                t: 10,
                applied_at_millis: 3_000,
            }),
            3,
        )
        .await;

        let ns = RaftNameService::new(state, stub_raft().await);
        let ref_value = ns
            .get_ref("test/db:main", RefKind::IndexHead)
            .await
            .unwrap()
            .expect("ref value");
        assert_eq!(ref_value.id, Some(cid(42)));
        assert_eq!(ref_value.t, 10);
    }

    #[tokio::test]
    async fn lookup_carries_index_forward_when_commit_advances_again() {
        // Index at t=10, then advance the commit to t=20. The lookup
        // should still report the t=10 index head — the next commit
        // didn't re-index itself, but it also shouldn't drop the
        // pointer to the latest available index.
        let state = ledger_at_commit(7, 10).await;
        let _ = apply_cmd(
            &state,
            Command::AdvanceIndexHead(AdvanceIndexHeadArgs {
                ledger_id: "test/db".into(),
                branch: "main".into(),
                new_index_head: cid(42),
                t: 10,
                applied_at_millis: 3_000,
            }),
            3,
        )
        .await;
        seed_head(&state, "test/db", "main", cid(8), 20).await;

        let ns = RaftNameService::new(state, stub_raft().await);
        let record = ns.lookup("test/db:main").await.unwrap().expect("record");
        assert_eq!(record.commit_head_id, Some(cid(8)));
        assert_eq!(record.commit_t, 20);
        // Index pointer unchanged.
        assert_eq!(record.index_head_id, Some(cid(42)));
        assert_eq!(record.index_t, 10);
    }

    #[tokio::test]
    async fn all_records_enumerates_every_branch() {
        let state = fresh_state();
        apply_cmd(&state, init_cmd("a/db", "main"), 1).await;
        seed_head(&state, "a/db", "feat", cid(1), 1).await;

        let ns = RaftNameService::new(state, stub_raft().await);
        let mut ids: Vec<_> = ns
            .all_records()
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.ledger_id)
            .collect();
        ids.sort();
        assert_eq!(ids, vec!["a/db:feat".to_string(), "a/db:main".to_string()]);
    }
}
