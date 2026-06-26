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

use crate::raft::commit_worker::{QueuePoisonError, QueuePoisonPublisher};
use crate::raft::staged_receipt::{AppliedReceipt, StagedReceiptMap};
use crate::raft::state_machine::{
    Command as SmCommand, DesyncReason, EntryPoisoning, NameServiceState, NewBranch, NewIndexHead,
    NewLedger, PoisonReason, ConfigUpdate, RecordedTally, RefKey, ResetHeadSnapshot,
    Response as SmResponse, StagedHead,
};
use crate::raft::state_machine_adapter::SharedState;
use crate::raft::{ClusterNode, NodeId, TypeConfig};
use async_trait::async_trait;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::Router;
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
use serde::{Deserialize, Serialize};
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
    /// queue front and threads its tally into [`StagedHead::tally`]
    /// so the cached idempotency record carries the same tally a
    /// later retry would expect to see. Off → tally lands as `None`,
    /// which only affects metric reporting on idempotent retries.
    staged_receipts: Option<Arc<StagedReceiptMap>>,
    /// Pair set by [`Self::with_forwarding`] to make `publish_commit`
    /// role-aware: on a non-leader node it ferries the staged receipt
    /// to the current leader's `apply_staged_commit` endpoint
    /// instead of trying to propose locally (which would just return
    /// `ForwardToLeader`). When unset, `publish_commit` behaves as it
    /// did before distributed workers — assumes leader-only callers.
    forwarding: Option<ForwardingConfig>,
}

/// Per-node forwarding configuration for [`RaftNameService`]. Held as
/// a unit so callers can't partially configure (e.g. set `id` but
/// forget `http_client`) — all three are needed for any forwarding
/// to happen.
#[derive(Clone)]
struct ForwardingConfig {
    id: NodeId,
    http_client: reqwest::Client,
    /// Per-request timeout for the `apply_staged_commit` /
    /// `apply_queue_poison` POSTs. The shared `http_client` carries
    /// a `connect_timeout` for dead-peer detection but no request
    /// timeout — without an explicit cap here, a connected-but-
    /// stalled leader hangs the worker on `send().await`
    /// indefinitely instead of falling through to the outer
    /// backoff/retry path. Sourced from
    /// [`NetworkConfig::cross_node_propose_timeout`] at construction.
    request_timeout: std::time::Duration,
}

impl RaftNameService {
    pub fn new(state: SharedState, raft: Arc<Raft<TypeConfig>>) -> Self {
        Self {
            state,
            raft,
            staged_receipts: None,
            forwarding: None,
        }
    }

    /// Wire the [`StagedReceiptMap`] this nameservice peeks for
    /// per-queue tally values. Pair it with the same handle the
    /// [`Worker`](super::commit_worker::Worker) stashes into and the
    /// [`StateMachineAdapter`](super::state_machine_adapter::StateMachineAdapter)
    /// consumes from.
    pub fn with_staged_receipts(mut self, staged_receipts: Arc<StagedReceiptMap>) -> Self {
        self.staged_receipts = Some(staged_receipts);
        self
    }

    /// Enable cross-node `apply_staged_commit` / `apply_queue_poison`
    /// forwarding from follower nodes. With this set,
    /// [`Self::publish_commit`] and
    /// [`Self::poison_queue_entry`](QueuePoisonPublisher::poison_queue_entry)
    /// on a non-leader node POST to the current leader's matching
    /// endpoint instead of attempting a local propose; without it,
    /// the previous leader-only contract holds.
    ///
    /// `request_timeout` is the per-request cap on those POSTs —
    /// typically threaded through from
    /// [`NetworkConfig::cross_node_propose_timeout`](crate::raft::network::NetworkConfig::cross_node_propose_timeout).
    pub fn with_forwarding(
        mut self,
        id: NodeId,
        http_client: reqwest::Client,
        request_timeout: std::time::Duration,
    ) -> Self {
        self.forwarding = Some(ForwardingConfig {
            id,
            http_client,
            request_timeout,
        });
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
) -> std::result::Result<NewIndexHead, NameServiceError> {
    let (ledger_name, branch) = split_ledger_id(ledger_id)?;
    let applied_at_millis = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    Ok(NewIndexHead {
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

/// Peek the queue_id at the front of `ref_key`'s queue; surface an
/// empty queue as a `publish_commit` error so callers stash-cleanup.
fn peek_queue_front_id(
    state: &NameServiceState,
    ref_key: &RefKey,
) -> std::result::Result<u64, NameServiceError> {
    state
        .queues
        .get(ref_key)
        .and_then(|q| q.front())
        .map(|entry| entry.queue_id)
        .ok_or_else(|| {
            NameServiceError::storage(format!(
                "publish_commit on {}: per-branch queue is empty — \
                 nothing staged for this branch",
                ref_key.ledger_id()
            ))
        })
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
    let queue_id = peek_queue_front_id(state, &ref_key)?;
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
    Ok(SmCommand::ApplyHead(StagedHead {
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

// ===========================================================================
// Cross-node ApplyHead: types + handler method
// ===========================================================================

/// RPC payload a follower's worker sends to the current leader after
/// it finishes staging a queue entry. The leader stashes the typed
/// receipt and proposes [`Command::ApplyHead`] on the follower's
/// behalf; the apply resolves the parked waiter on the leader with
/// the ferried receipt instead of falling back to
/// [`AppliedReceipt::Minimal`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StagedCommit {
    /// Branch identity that owns the staged entry.
    pub ref_key: RefKey,
    /// Queue ID of the entry the follower staged. The state machine
    /// re-validates that this matches the queue front at apply time;
    /// a mismatch is reported back as [`ApplyStagedCommitResponse::Stale`].
    pub queue_id: u64,
    /// Content id of the commit blob the follower wrote to CAS.
    pub commit_id: ContentId,
    /// Logical time of the staged commit.
    pub commit_t: i64,
    /// Typed staging receipt to stash on the leader so the waiter
    /// resolves with full per-op detail.
    pub receipt: AppliedReceipt,
}

mod wire;

/// Outcome of a cross-node ApplyHead proposal.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ApplyStagedCommitResponse {
    /// The head was applied at `commit_t`. The follower's worker can
    /// retire the queue entry locally.
    Applied { commit_t: i64 },
    /// The queue front no longer matches the proposed `queue_id`.
    /// Typically a racing worker already advanced past this entry;
    /// the caller should drop its local stash and re-poll its queue.
    Stale {
        /// Queue ID currently at the front, if any. `None` means the
        /// queue is empty (likely admin-cleared between stage and propose).
        current_front_queue_id: Option<u64>,
    },
}

/// Errors returned by [`RaftNameService::apply_staged_commit`]. Sent
/// to the follower over the wire — kept `Serialize`/`Deserialize` so
/// the HTTP transport carries structured failures, not just opaque
/// status codes.
#[derive(Clone, Debug, Serialize, Deserialize, thiserror::Error)]
pub enum ApplyStagedCommitError {
    /// The replicated state has no `LedgerRecord` for the named
    /// ledger — possibly purged between the follower's stage and
    /// this propose attempt.
    #[error("ledger {0} not found")]
    LedgerNotFound(String),
    /// The receiving node isn't the current leader. The caller
    /// should look up the new leader (if known) and retry.
    #[error("not the leader; current leader: {leader:?}")]
    NotLeader { leader: Option<NodeId> },
    /// Raft `client_write` reported a non-forwardable failure
    /// (membership change error, fatal storage error). The message
    /// is the underlying error's `Display`.
    #[error("raft propose failed: {0}")]
    RaftPropose(String),
    /// The state machine returned a [`SmResponse`] variant that's
    /// not reachable for an `ApplyHead` command — points to a
    /// state-machine bug, not a caller mistake.
    #[error("state machine invariant violated: {0}")]
    InvariantViolated(String),
}

/// RPC payload a follower's worker sends to the current leader when
/// it wants to poison a queue entry it can't successfully stage.
/// The leader proposes [`Command::PoisonQueueEntry`] on the
/// follower's behalf.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueuePoison {
    pub ref_key: RefKey,
    pub queue_id: u64,
    pub reason: PoisonReason,
}

/// Errors returned by [`RaftNameService::apply_queue_poison`]. Sent
/// to the follower over the wire — kept `Serialize`/`Deserialize` so
/// the HTTP transport carries structured failures, not just opaque
/// status codes.
#[derive(Clone, Debug, Serialize, Deserialize, thiserror::Error)]
pub enum ApplyQueuePoisonError {
    /// The receiving node isn't the current leader. The caller
    /// should look up the new leader (if known) and retry.
    #[error("not the leader; current leader: {leader:?}")]
    NotLeader { leader: Option<NodeId> },
    /// Raft `client_write` reported a non-forwardable failure
    /// (membership change error, fatal storage error). The message
    /// is the underlying error's `Display`.
    #[error("raft propose failed: {0}")]
    RaftPropose(String),
}

impl RaftNameService {
    /// Handle the cross-node ApplyHead RPC: stash the ferried
    /// receipt, propose `Command::ApplyHead`, return the outcome.
    /// Called by the HTTP handler on the leader after a follower's
    /// worker forwards its staged work.
    ///
    /// Receipt-stash lifecycle: stashed before propose so the
    /// state-machine adapter has it when the apply fires; taken back
    /// on the [`Stale`](ApplyStagedCommitResponse::Stale) path
    /// (entry never applies, stash would leak), and on any error
    /// before propose-returns (same reason).
    pub async fn apply_staged_commit(
        &self,
        args: StagedCommit,
    ) -> std::result::Result<ApplyStagedCommitResponse, ApplyStagedCommitError> {
        let StagedCommit {
            ref_key,
            queue_id,
            commit_id,
            commit_t,
            receipt,
        } = args;

        let (tally, flake_count) = match &receipt {
            AppliedReceipt::Transact(t) => (
                t.tally.as_ref().map(RecordedTally::from),
                t.flake_count as u64,
            ),
            _ => (None, 0),
        };
        let cmd = SmCommand::ApplyHead(StagedHead {
            ledger_id: ref_key.ledger_name.clone(),
            branch: ref_key.branch.clone(),
            queue_id,
            commit_id: commit_id.clone(),
            commit_t,
            applied_at_millis: current_millis(),
            tally,
            flake_count,
        });

        if let Some(stash) = &self.staged_receipts {
            stash.stash(queue_id, ref_key.clone(), receipt);
        }

        let resp = match self.raft.client_write(cmd).await {
            Ok(resp) => resp,
            Err(err) => {
                self.drop_staged_receipt(queue_id);
                return Err(map_propose_error(err));
            }
        };

        match resp.data {
            SmResponse::HeadApplied { commit_t, .. } => {
                Ok(ApplyStagedCommitResponse::Applied { commit_t })
            }
            SmResponse::QueueDesync { .. } => {
                self.drop_staged_receipt(queue_id);
                Ok(ApplyStagedCommitResponse::Stale {
                    current_front_queue_id: self.current_front_queue_id(&ref_key).await,
                })
            }
            SmResponse::LedgerNotFound { ledger_id } => {
                self.drop_staged_receipt(queue_id);
                Err(ApplyStagedCommitError::LedgerNotFound(ledger_id))
            }
            other => {
                self.drop_staged_receipt(queue_id);
                Err(ApplyStagedCommitError::InvariantViolated(format!(
                    "unexpected Response variant for ApplyHead: {other:?}"
                )))
            }
        }
    }

    /// Discard a stashed receipt when the propose path won't apply
    /// it — `Stale`, `LedgerNotFound`, propose error, or an
    /// unreachable response variant. The state-machine adapter
    /// `take`s on the success path, so this is a no-op there.
    fn drop_staged_receipt(&self, queue_id: u64) {
        if let Some(stash) = &self.staged_receipts {
            stash.take(queue_id);
        }
    }

    /// Peek the current queue front's `queue_id`, or `None` if the
    /// queue is empty / absent. Used to give followers a hint about
    /// why their proposal raced.
    async fn current_front_queue_id(&self, ref_key: &RefKey) -> Option<u64> {
        let state = self.state.read().await;
        state
            .queues
            .get(ref_key)
            .and_then(|q| q.front())
            .map(|entry| entry.queue_id)
    }

    /// Handle the cross-node `PoisonQueueEntry` RPC: propose the
    /// poison from the leader and return the outcome. Called by the
    /// HTTP handler on the leader after a follower-owned worker
    /// forwards a deterministic staging failure.
    ///
    /// The state-machine response (`Poisoned` vs `QueueDesync`) is
    /// informational once the poison is durably proposed — either
    /// way the entry is done from the worker's perspective — so
    /// success collapses to `Ok(())`. Only Raft-side failures
    /// surface as `Err`.
    pub async fn apply_queue_poison(
        &self,
        args: QueuePoison,
    ) -> std::result::Result<(), ApplyQueuePoisonError> {
        let QueuePoison {
            ref_key,
            queue_id,
            reason,
        } = args;
        let cmd = SmCommand::PoisonQueueEntry(EntryPoisoning {
            ledger_id: ref_key.ledger_name.clone(),
            branch: ref_key.branch.clone(),
            queue_id,
            reason,
            applied_at_millis: current_millis(),
        });
        match self.raft.client_write(cmd).await {
            Ok(_) => Ok(()),
            Err(err) => Err(map_queue_poison_propose_error(err)),
        }
    }
}

/// Path under [`apply_staged_commit_router`]'s root for the cross-node
/// ApplyHead RPC. Exposed so the client side (see
/// [`Self::publish_commit`] on a follower) can build the outbound URL
/// without hardcoding the route string twice.
pub const APPLY_STAGED_COMMIT_PATH: &str = "/apply_staged_commit";

const POSTCARD_MIME: &str = "application/octet-stream";

/// Axum router exposing the cross-node ApplyHead RPC under
/// [`APPLY_STAGED_COMMIT_PATH`]. Caller nests this under whatever
/// base path the openraft RPC router (see
/// [`crate::raft::network::router`]) is mounted at — typically `/raft`
/// — so the full URL becomes `<base>/raft/apply_staged_commit`.
///
/// `network_config` supplies the per-route body-size cap. Without an
/// explicit cap the route falls back to axum's 2 MiB default —
/// the sibling openraft RPCs all set their own cap, and an
/// oversize receipt 413s into the follower worker's retry-forever
/// path.
///
/// No auth layer is applied. The endpoint follows the same
/// trust-the-VPC posture as the openraft RPCs: operators bind the
/// raft port to peer addresses only.
pub fn apply_staged_commit_router(
    ns: Arc<RaftNameService>,
    network_config: &crate::raft::network::NetworkConfig,
) -> Router {
    Router::new()
        .route(
            APPLY_STAGED_COMMIT_PATH,
            post(handle_apply_staged_commit).layer(DefaultBodyLimit::max(
                network_config.apply_staged_commit_max_body_bytes,
            )),
        )
        .with_state(ns)
}

/// HTTP handler for [`APPLY_STAGED_COMMIT_PATH`]. Decodes a postcard
/// [`StagedCommit`] body, dispatches to
/// [`RaftNameService::apply_staged_commit`], and encodes the
/// outcome (success **or** logical failure) as a postcard
/// `Result<ApplyStagedCommitResponse, ApplyStagedCommitError>` body
/// with HTTP 200. HTTP-level statuses are reserved for
/// transport-level failures (decode error, encode error).
async fn handle_apply_staged_commit(
    State(ns): State<Arc<RaftNameService>>,
    body: axum::body::Bytes,
) -> Response {
    let args: StagedCommit = match postcard::from_bytes::<wire::StagedCommit>(&body) {
        Ok(args) => args.into(),
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                format!("postcard decode error on StagedCommit: {e}"),
            )
                .into_response();
        }
    };

    let outcome = ns.apply_staged_commit(args).await;
    encode_apply_staged_commit_response(outcome)
}

/// Encode the application-level `Result` as a postcard body with HTTP
/// 200. Both `Ok` and `Err` variants travel in the body so the client
/// can pattern-match on structured outcomes; HTTP non-2xx is reserved
/// for transport failures.
fn encode_apply_staged_commit_response(
    outcome: std::result::Result<ApplyStagedCommitResponse, ApplyStagedCommitError>,
) -> Response {
    match postcard::to_allocvec(&outcome) {
        Ok(bytes) => (
            StatusCode::OK,
            [(reqwest::header::CONTENT_TYPE, POSTCARD_MIME)],
            bytes,
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("postcard encode error on apply_staged_commit response: {e}"),
        )
            .into_response(),
    }
}

/// Path under [`apply_queue_poison_router`]'s root for the cross-node
/// `PoisonQueueEntry` RPC. Exposed so the client side (see
/// `RaftNameService`'s [`QueuePoisonPublisher`] impl) can build the
/// outbound URL without hardcoding the route string twice.
pub const APPLY_QUEUE_POISON_PATH: &str = "/apply_queue_poison";

/// Axum router exposing the cross-node `PoisonQueueEntry` RPC under
/// [`APPLY_QUEUE_POISON_PATH`]. Caller nests this under the same
/// `/raft` mount the openraft RPCs and `apply_staged_commit` use —
/// same intra-cluster trust posture, no auth layer.
///
/// `network_config` supplies the per-route body-size cap. The body
/// is tiny (`ref_key` + `queue_id` + structured `PoisonReason`),
/// but the cap is pinned to a deliberate value for parity with the
/// sibling RPCs and so the route can't silently inherit the axum
/// default if someone ever changes how the router is mounted.
pub fn apply_queue_poison_router(
    ns: Arc<RaftNameService>,
    network_config: &crate::raft::network::NetworkConfig,
) -> Router {
    Router::new()
        .route(
            APPLY_QUEUE_POISON_PATH,
            post(handle_apply_queue_poison).layer(DefaultBodyLimit::max(
                network_config.apply_queue_poison_max_body_bytes,
            )),
        )
        .with_state(ns)
}

/// HTTP handler for [`APPLY_QUEUE_POISON_PATH`]. Decodes a postcard
/// [`QueuePoison`] body, dispatches to
/// [`RaftNameService::apply_queue_poison`], and encodes the
/// outcome (success **or** logical failure) as a postcard
/// `Result<(), ApplyQueuePoisonError>` body with HTTP 200.
/// HTTP-level statuses are reserved for transport-level failures.
async fn handle_apply_queue_poison(
    State(ns): State<Arc<RaftNameService>>,
    body: axum::body::Bytes,
) -> Response {
    let args: QueuePoison = match postcard::from_bytes(&body) {
        Ok(args) => args,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                format!("postcard decode error on QueuePoison: {e}"),
            )
                .into_response();
        }
    };

    let outcome = ns.apply_queue_poison(args).await;
    match postcard::to_allocvec(&outcome) {
        Ok(bytes) => (
            StatusCode::OK,
            [(reqwest::header::CONTENT_TYPE, POSTCARD_MIME)],
            bytes,
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("postcard encode error on apply_queue_poison response: {e}"),
        )
            .into_response(),
    }
}

fn map_propose_error(
    err: RaftError<NodeId, ClientWriteError<NodeId, ClusterNode>>,
) -> ApplyStagedCommitError {
    match err {
        RaftError::APIError(ClientWriteError::ForwardToLeader(forward)) => {
            ApplyStagedCommitError::NotLeader {
                leader: forward.leader_id,
            }
        }
        RaftError::APIError(ClientWriteError::ChangeMembershipError(e)) => {
            ApplyStagedCommitError::RaftPropose(format!(
                "unexpected ChangeMembershipError on ApplyHead: {e}"
            ))
        }
        RaftError::Fatal(f) => {
            ApplyStagedCommitError::RaftPropose(format!("raft fatal during ApplyHead: {f}"))
        }
    }
}

/// Classify the structured `apply_staged_commit` response into the
/// trait-surface [`NameServiceError`] the caller's outer retry vs
/// poison logic switches on.
///
/// Terminal failures route to variants the worker's
/// `publish_head_advance` recognizes as poison-worthy
/// ([`NameServiceError::NotFound`] for missing ledgers,
/// [`NameServiceError::ApplyRejected`] for state-machine invariant
/// violations); transient failures (leader transition, raft
/// `Fatal`, stale queue front) collapse to [`NameServiceError::Storage`]
/// so the outer loop continues to retry. Without this classification
/// every cross-node failure flattened to `Storage` and a terminal
/// `InvariantViolated` would spin forever, head-of-line-blocking the
/// branch's queue.
fn classify_apply_staged_commit_outcome(
    outcome: std::result::Result<ApplyStagedCommitResponse, ApplyStagedCommitError>,
    queue_id: u64,
) -> std::result::Result<(), NameServiceError> {
    match outcome {
        Ok(ApplyStagedCommitResponse::Applied { .. }) => Ok(()),
        Ok(ApplyStagedCommitResponse::Stale {
            current_front_queue_id,
        }) => Err(NameServiceError::storage(format!(
            "apply_staged_commit stale: queue_id {queue_id} no longer at front \
             (current front: {current_front_queue_id:?})"
        ))),
        Err(ApplyStagedCommitError::LedgerNotFound(id)) => Err(NameServiceError::not_found(id)),
        Err(ApplyStagedCommitError::InvariantViolated(msg)) => {
            Err(NameServiceError::apply_rejected(format!(
                "apply_staged_commit invariant violated: {msg}"
            )))
        }
        Err(
            e @ (ApplyStagedCommitError::NotLeader { .. } | ApplyStagedCommitError::RaftPropose(_)),
        ) => Err(NameServiceError::storage(format!(
            "leader rejected apply_staged_commit: {e}"
        ))),
    }
}

fn map_queue_poison_propose_error(
    err: RaftError<NodeId, ClientWriteError<NodeId, ClusterNode>>,
) -> ApplyQueuePoisonError {
    match err {
        RaftError::APIError(ClientWriteError::ForwardToLeader(forward)) => {
            ApplyQueuePoisonError::NotLeader {
                leader: forward.leader_id,
            }
        }
        RaftError::APIError(ClientWriteError::ChangeMembershipError(e)) => {
            ApplyQueuePoisonError::RaftPropose(format!(
                "unexpected ChangeMembershipError on PoisonQueueEntry: {e}"
            ))
        }
        RaftError::Fatal(f) => {
            ApplyQueuePoisonError::RaftPropose(format!("raft fatal during PoisonQueueEntry: {f}"))
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
        // With forwarding enabled, a non-leader node ferries the
        // staged work to the current leader's `apply_staged_commit`
        // endpoint instead of attempting a local propose (which would
        // just bounce back as ForwardToLeader). Without forwarding,
        // the legacy leader-only contract applies.
        if let Some(forwarding) = &self.forwarding {
            if !self.is_local_leader(forwarding.id).await {
                return self
                    .publish_commit_via_leader(ledger_id, commit_t, commit_id, forwarding)
                    .await;
            }
        }

        self.publish_commit_locally(ledger_id, commit_t, commit_id)
            .await
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

impl RaftNameService {
    async fn is_local_leader(&self, id: NodeId) -> bool {
        self.raft.current_leader().await == Some(id)
    }

    /// In-process propose path — used on the leader and as the
    /// legacy fallback when forwarding isn't configured. Peeks the
    /// queue front under the read lock, drops the lock before
    /// `client_write`, then maps the apply outcome onto the trait's
    /// `Result<(), NameServiceError>` contract.
    async fn publish_commit_locally(
        &self,
        ledger_id: &str,
        commit_t: i64,
        commit_id: &ContentId,
    ) -> Result<()> {
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

    /// Follower path — pulls the local queue_id and stashed receipt,
    /// POSTs them to the current leader's `apply_staged_commit`
    /// endpoint, maps the structured response onto the trait's
    /// `Result<(), NameServiceError>` contract.
    ///
    /// Taking the receipt out of the local stash is deliberate: if
    /// the RPC succeeds the leader's stash takes over, and if it
    /// fails the worker's outer error path re-stages the same entry
    /// (idempotent against the state machine's `queue_id` check).
    /// Holding a duplicate on the follower would leak.
    async fn publish_commit_via_leader(
        &self,
        ledger_id: &str,
        commit_t: i64,
        commit_id: &ContentId,
        forwarding: &ForwardingConfig,
    ) -> Result<()> {
        let (ledger_name, branch) = split_ledger_id(ledger_id)?;
        let ref_key = RefKey::new(&ledger_name, &branch);

        let queue_id = {
            let state = self.state.read().await;
            peek_queue_front_id(&state, &ref_key)?
        };

        let receipt = self
            .staged_receipts
            .as_ref()
            .and_then(|s| s.take(queue_id))
            .unwrap_or_else(|| AppliedReceipt::Minimal {
                commit_id: commit_id.clone(),
                commit_t,
            });

        let args = StagedCommit {
            ref_key,
            queue_id,
            commit_id: commit_id.clone(),
            commit_t,
            receipt,
        };

        let leader_url = self.lookup_leader_raft_url().await.ok_or_else(|| {
            NameServiceError::storage(
                "no leader currently elected; cross-node ApplyHead deferred".to_string(),
            )
        })?;
        let target = format!(
            "{}{}",
            leader_url.trim_end_matches('/'),
            APPLY_STAGED_COMMIT_PATH
        );

        let body = postcard::to_allocvec(&wire::StagedCommit::from(args)).map_err(|e| {
            NameServiceError::storage(format!("postcard encode of StagedCommit: {e}"))
        })?;

        let resp = forwarding
            .http_client
            .post(&target)
            .header(reqwest::header::CONTENT_TYPE, POSTCARD_MIME)
            .body(body)
            .timeout(forwarding.request_timeout)
            .send()
            .await
            .map_err(|e| {
                NameServiceError::storage(format!("apply_staged_commit POST to leader: {e}"))
            })?;

        if !resp.status().is_success() {
            return Err(NameServiceError::storage(format!(
                "apply_staged_commit returned HTTP {}",
                resp.status()
            )));
        }

        let body_bytes = resp.bytes().await.map_err(|e| {
            NameServiceError::storage(format!("read apply_staged_commit body: {e}"))
        })?;
        let outcome: std::result::Result<ApplyStagedCommitResponse, ApplyStagedCommitError> =
            postcard::from_bytes(&body_bytes).map_err(|e| {
                NameServiceError::storage(format!(
                    "postcard decode of apply_staged_commit response: {e}"
                ))
            })?;

        classify_apply_staged_commit_outcome(outcome, queue_id)
    }

    /// Resolve the current leader's `raft_addr` from the replicated
    /// membership snapshot. `None` if no leader is currently elected
    /// or the membership entry for the leader is somehow missing.
    async fn lookup_leader_raft_url(&self) -> Option<String> {
        let leader_id = self.raft.current_leader().await?;
        self.raft
            .metrics()
            .borrow()
            .membership_config
            .nodes()
            .find(|(id, _)| **id == leader_id)
            .map(|(_, node)| node.raft_addr.clone())
    }

    /// Forward [`SmCommand::PoisonQueueEntry`] to the current leader's
    /// `apply_queue_poison` endpoint. Parallel to
    /// [`Self::publish_commit_via_leader`] — same `ForwardingConfig`
    /// for leader lookup + outbound HTTP, same wire transport
    /// (postcard body, structured result), same trust posture.
    async fn poison_queue_entry_via_leader(
        &self,
        ref_key: &RefKey,
        queue_id: u64,
        reason: PoisonReason,
        forwarding: &ForwardingConfig,
    ) -> std::result::Result<(), QueuePoisonError> {
        let args = QueuePoison {
            ref_key: ref_key.clone(),
            queue_id,
            reason,
        };

        let leader_url = self
            .lookup_leader_raft_url()
            .await
            .ok_or(QueuePoisonError::NoLeader)?;
        let target = format!(
            "{}{}",
            leader_url.trim_end_matches('/'),
            APPLY_QUEUE_POISON_PATH
        );

        let body = postcard::to_allocvec(&args)
            .map_err(|e| QueuePoisonError::Codec(format!("encode QueuePoison: {e}")))?;

        let resp = forwarding
            .http_client
            .post(&target)
            .header(reqwest::header::CONTENT_TYPE, POSTCARD_MIME)
            .body(body)
            .timeout(forwarding.request_timeout)
            .send()
            .await
            .map_err(|e| QueuePoisonError::Transport(e.to_string()))?;

        if !resp.status().is_success() {
            return Err(QueuePoisonError::HttpStatus(resp.status().to_string()));
        }

        let body_bytes = resp
            .bytes()
            .await
            .map_err(|e| QueuePoisonError::Transport(format!("read response body: {e}")))?;
        let outcome: std::result::Result<(), ApplyQueuePoisonError> =
            postcard::from_bytes(&body_bytes).map_err(|e| {
                QueuePoisonError::Codec(format!("decode apply_queue_poison response: {e}"))
            })?;
        outcome.map_err(|e| QueuePoisonError::RaftPropose(e.to_string()))
    }

    /// Leader-side fast path: propose `PoisonQueueEntry` via local
    /// `client_write`. Used when forwarding isn't configured or this
    /// node currently is the leader. Mirrors
    /// [`Self::publish_commit_locally`].
    async fn poison_queue_entry_locally(
        &self,
        ref_key: &RefKey,
        queue_id: u64,
        reason: PoisonReason,
    ) -> std::result::Result<(), QueuePoisonError> {
        self.apply_queue_poison(QueuePoison {
            ref_key: ref_key.clone(),
            queue_id,
            reason,
        })
        .await
        .map_err(|e| QueuePoisonError::RaftPropose(e.to_string()))
    }
}

#[async_trait]
impl QueuePoisonPublisher for RaftNameService {
    async fn poison_queue_entry(
        &self,
        ref_key: &RefKey,
        queue_id: u64,
        reason: PoisonReason,
    ) -> std::result::Result<(), QueuePoisonError> {
        // Dispatch by local-leader status, the same shape
        // `publish_commit` uses for ApplyHead. Without forwarding,
        // assume the legacy leader-only contract — caller knows it's
        // the leader.
        if let Some(forwarding) = &self.forwarding {
            if !self.is_local_leader(forwarding.id).await {
                return self
                    .poison_queue_entry_via_leader(ref_key, queue_id, reason, forwarding)
                    .await;
            }
        }
        self.poison_queue_entry_locally(ref_key, queue_id, reason)
            .await
    }
}

/// Build the state-machine command for [`LedgerLifecycle::init`].
fn build_create_command(ledger_id: &str) -> std::result::Result<SmCommand, NameServiceError> {
    let (ledger_name, branch) = split_ledger_id(ledger_id)?;
    let applied_at_millis = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    Ok(SmCommand::CreateLedger(NewLedger {
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
    Ok(SmCommand::CreateBranch(NewBranch {
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
        let cmd = SmCommand::PushConfig(Box::new(ConfigUpdate {
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
        Command, NameServiceState, NewIndexHead, NewLedger, RefEntry, Response,
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
        Command::CreateLedger(NewLedger {
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
    // StagedCommit postcard round-trip
    // ----------------------------------------------------------------

    use crate::raft::staged_receipt::{AppliedReceipt, TransactApplied};
    use fluree_db_api::{PolicyStats, TrackingTally};

    /// Build a [`TrackingTally`] populated with values in every field
    /// of every nested struct. Any `skip_serializing_if` field that
    /// leaks into the wire format will surface as a decode mismatch.
    fn fully_populated_tracking_tally() -> TrackingTally {
        use fluree_db_core::tracking::ReasoningTally;
        use std::collections::HashMap;
        let mut policy = HashMap::new();
        policy.insert(
            "policy:a".to_string(),
            PolicyStats {
                executed: 7,
                allowed: 5,
            },
        );
        TrackingTally {
            time: Some("12.34ms".to_string()),
            fuel: Some(1.5),
            policy: Some(policy),
            reasoning: Some(ReasoningTally {
                capped: true,
                capped_reason: Some("budget".to_string()),
                derived_facts: 42,
                iterations: 3,
                duration_ms: 17,
            }),
        }
    }

    fn args_for_round_trip(receipt: AppliedReceipt) -> StagedCommit {
        StagedCommit {
            ref_key: RefKey::new("test/db", "main"),
            queue_id: 9,
            commit_id: cid(1),
            commit_t: 17,
            receipt,
        }
    }

    fn assert_args_round_trip(args: StagedCommit) {
        let encoded = wire::StagedCommit::from(args.clone());
        let bytes = postcard::to_allocvec(&encoded).expect("encode");
        let decoded: wire::StagedCommit = postcard::from_bytes(&bytes).expect("decode");
        let round_tripped: StagedCommit = decoded.into();
        assert_eq!(round_tripped.ref_key, args.ref_key);
        assert_eq!(round_tripped.queue_id, args.queue_id);
        assert_eq!(round_tripped.commit_id, args.commit_id);
        assert_eq!(round_tripped.commit_t, args.commit_t);
        // `AppliedReceipt` doesn't derive `PartialEq`, so compare via
        // the wire form (which has all the same fields, by construction).
        let want = wire::AppliedReceipt::from(args.receipt);
        let got = wire::AppliedReceipt::from(round_tripped.receipt);
        // The wire enum doesn't impl `PartialEq` either; round-trip
        // through postcard a second time and compare the bytes.
        let want_bytes = postcard::to_allocvec(&want).expect("encode want");
        let got_bytes = postcard::to_allocvec(&got).expect("encode got");
        assert_eq!(want_bytes, got_bytes);
    }

    #[test]
    fn apply_staged_commit_args_round_trips_transact_with_fully_populated_tally() {
        let receipt = AppliedReceipt::Transact(TransactApplied {
            commit_id: cid(2),
            commit_t: 17,
            flake_count: 99,
            tally: Some(fully_populated_tracking_tally()),
        });
        assert_args_round_trip(args_for_round_trip(receipt));
    }

    #[test]
    fn apply_staged_commit_args_round_trips_transact_with_none_tally() {
        // The previous case proves all-fields-populated survives.
        // This one proves the None-everywhere case survives too — i.e.
        // the wire codec also handles the all-skips-fire branch that
        // exposes the bug most directly.
        let receipt = AppliedReceipt::Transact(TransactApplied {
            commit_id: cid(2),
            commit_t: 17,
            flake_count: 99,
            tally: None,
        });
        assert_args_round_trip(args_for_round_trip(receipt));
    }

    #[test]
    fn apply_staged_commit_args_round_trips_transact_with_partial_tally() {
        // Mixed-presence ReasoningTally is the canonical
        // `skip_serializing_if` shape — every other field present,
        // `capped_reason` None — that quietly corrupted the wire on
        // the old codec.
        use fluree_db_core::tracking::ReasoningTally;
        let tally = TrackingTally {
            time: None,
            fuel: Some(1.0),
            policy: None,
            reasoning: Some(ReasoningTally {
                capped: false,
                capped_reason: None,
                derived_facts: 10,
                iterations: 1,
                duration_ms: 5,
            }),
        };
        let receipt = AppliedReceipt::Transact(TransactApplied {
            commit_id: cid(2),
            commit_t: 17,
            flake_count: 1,
            tally: Some(tally),
        });
        assert_args_round_trip(args_for_round_trip(receipt));
    }

    #[test]
    fn apply_staged_commit_args_round_trips_minimal_receipt() {
        // The fallback variant the adapter resolves with when no
        // typed receipt is stashed. Plain `commit_id` + `commit_t`,
        // no nested skip-fielded types — establishes the baseline.
        let receipt = AppliedReceipt::Minimal {
            commit_id: cid(2),
            commit_t: 17,
        };
        assert_args_round_trip(args_for_round_trip(receipt));
    }

    // ----------------------------------------------------------------
    // classify_apply_staged_commit_outcome
    // ----------------------------------------------------------------
    //
    // The leader-forwarded apply_staged_commit response used to
    // flatten every error variant into `NameServiceError::Storage`,
    // which the worker's outer loop treats as transient. Terminal
    // failures (a vanished ledger, a state-machine invariant
    // violation) would then loop forever, head-of-line-blocking the
    // branch's queue. Each test below pins one variant to the
    // specific `NameServiceError` shape the worker pattern-matches
    // on to decide retry vs poison.

    #[test]
    fn classify_outcome_applied_is_ok() {
        let r = classify_apply_staged_commit_outcome(
            Ok(ApplyStagedCommitResponse::Applied { commit_t: 9 }),
            7,
        );
        assert!(r.is_ok());
    }

    #[test]
    fn classify_outcome_stale_is_transient_storage() {
        // The `Stale` shape is transient: a racing worker already
        // popped the front while ours was in flight; the next round
        // of the supervisor will reconcile. `Storage` is the
        // intended retry signal.
        let r = classify_apply_staged_commit_outcome(
            Ok(ApplyStagedCommitResponse::Stale {
                current_front_queue_id: Some(8),
            }),
            7,
        );
        let err = r.expect_err("stale must be Err");
        assert!(
            matches!(err, NameServiceError::Storage(_)),
            "expected Storage, got {err:?}"
        );
        assert!(err.to_string().contains("queue_id 7"));
        assert!(err.to_string().contains("Some(8)"));
    }

    #[test]
    fn classify_outcome_ledger_not_found_is_terminal_not_found() {
        // `LedgerNotFound` is terminal — the named ledger is gone
        // and no amount of retrying will bring it back. The worker
        // recognizes `NotFound` and routes to a
        // `PoisonReason::LedgerNotFound` instead of looping.
        let r = classify_apply_staged_commit_outcome(
            Err(ApplyStagedCommitError::LedgerNotFound(
                "test/db:main".into(),
            )),
            7,
        );
        let err = r.expect_err("ledger_not_found must be Err");
        match err {
            NameServiceError::NotFound(id) => assert_eq!(id, "test/db:main"),
            other => panic!("expected NotFound, got {other:?}"),
        }
    }

    #[test]
    fn classify_outcome_invariant_violated_is_terminal_apply_rejected() {
        // The crux of this comment's fix: an `InvariantViolated`
        // surfacing from the leader's state machine is a deeper
        // bug, not a hiccup. Map it to `ApplyRejected` so the
        // worker poisons the entry instead of spinning forever on
        // a propose that can never succeed.
        let r = classify_apply_staged_commit_outcome(
            Err(ApplyStagedCommitError::InvariantViolated(
                "unexpected Response variant for ApplyHead: NoOp".into(),
            )),
            7,
        );
        let err = r.expect_err("invariant_violated must be Err");
        assert!(
            matches!(err, NameServiceError::ApplyRejected(_)),
            "expected ApplyRejected, got {err:?}"
        );
        assert!(err.to_string().contains("unexpected Response variant"));
    }

    #[test]
    fn classify_outcome_not_leader_is_transient_storage() {
        // Mid-flight leader change. Transient: the next round
        // discovers the new leader and retries against it.
        let r = classify_apply_staged_commit_outcome(
            Err(ApplyStagedCommitError::NotLeader { leader: Some(2) }),
            7,
        );
        let err = r.expect_err("not_leader must be Err");
        assert!(
            matches!(err, NameServiceError::Storage(_)),
            "expected Storage, got {err:?}"
        );
    }

    #[test]
    fn classify_outcome_raft_propose_is_transient_storage() {
        // Raft fatal (membership-change error, log fsync stuck,
        // etc.). Transient at this layer — openraft's own retry
        // machinery and the worker's backoff handle recovery.
        let r = classify_apply_staged_commit_outcome(
            Err(ApplyStagedCommitError::RaftPropose(
                "log fsync failed".into(),
            )),
            7,
        );
        let err = r.expect_err("raft_propose must be Err");
        assert!(
            matches!(err, NameServiceError::Storage(_)),
            "expected Storage, got {err:?}"
        );
    }

    #[test]
    fn wire_mirror_stays_in_lockstep_with_in_memory_receipt_graph() {
        // Structural drift sentinel: pins the wire mirror's field set
        // against the in-memory receipt graph and surfaces any silent
        // skew the next time someone touches either side.
        //
        // The construction below uses struct literals with no `..`
        // rest pattern at every level: `TransactApplied`,
        // `TrackingTally`, and `ReasoningTally`. Adding a field to
        // any of these types stops the test from compiling here
        // until the contributor either threads the new field through
        // the wire form or explicitly opts out — and the
        // destructures after the round-trip have the same property
        // for the decoded side, so a field added to the wire mirror
        // alone (e.g. through a `..Default::default()` refactor of
        // the `From` impls that silently drops fields) surfaces as a
        // missing-field destructure error.
        //
        // We also assert each destructured field round-trips by
        // value, so a refactor that compiles but loses data on the
        // wire fails the assertion rather than silently passing.
        use fluree_db_core::tracking::ReasoningTally;
        let original_reasoning = ReasoningTally {
            capped: true,
            capped_reason: Some("budget".to_string()),
            derived_facts: 42,
            iterations: 3,
            duration_ms: 17,
        };
        let original_tally = TrackingTally {
            time: Some("12.34ms".to_string()),
            fuel: Some(1.5),
            policy: Some({
                let mut m = std::collections::HashMap::new();
                m.insert(
                    "policy:a".to_string(),
                    PolicyStats {
                        executed: 7,
                        allowed: 5,
                    },
                );
                m
            }),
            reasoning: Some(original_reasoning),
        };
        let original_transact = TransactApplied {
            commit_id: cid(2),
            commit_t: 17,
            flake_count: 99,
            tally: Some(original_tally.clone()),
        };
        let args = args_for_round_trip(AppliedReceipt::Transact(original_transact.clone()));

        let encoded = wire::StagedCommit::from(args.clone());
        let bytes = postcard::to_allocvec(&encoded).expect("encode");
        let decoded: wire::StagedCommit = postcard::from_bytes(&bytes).expect("decode");
        let round_tripped: StagedCommit = decoded.into();

        let StagedCommit {
            ref_key,
            queue_id,
            commit_id,
            commit_t,
            receipt,
        } = round_tripped;
        assert_eq!(ref_key, args.ref_key);
        assert_eq!(queue_id, args.queue_id);
        assert_eq!(commit_id, args.commit_id);
        assert_eq!(commit_t, args.commit_t);

        let rt_transact = match receipt {
            AppliedReceipt::Transact(t) => t,
            other => panic!("expected Transact, got {other:?}"),
        };
        let TransactApplied {
            commit_id: rt_commit_id,
            commit_t: rt_commit_t,
            flake_count: rt_flake_count,
            tally: rt_tally,
        } = rt_transact;
        assert_eq!(rt_commit_id, original_transact.commit_id);
        assert_eq!(rt_commit_t, original_transact.commit_t);
        assert_eq!(rt_flake_count, original_transact.flake_count);

        let TrackingTally {
            time: rt_time,
            fuel: rt_fuel,
            policy: rt_policy,
            reasoning: rt_reasoning,
        } = rt_tally.expect("tally Some on round-trip");
        assert_eq!(rt_time, original_tally.time);
        assert_eq!(rt_fuel, original_tally.fuel);
        assert_eq!(rt_policy, original_tally.policy);

        let ReasoningTally {
            capped: rt_capped,
            capped_reason: rt_capped_reason,
            derived_facts: rt_derived_facts,
            iterations: rt_iterations,
            duration_ms: rt_duration_ms,
        } = rt_reasoning.expect("reasoning Some on round-trip");
        let original_reasoning = original_tally.reasoning.expect("reasoning populated above");
        assert_eq!(rt_capped, original_reasoning.capped);
        assert_eq!(rt_capped_reason, original_reasoning.capped_reason);
        assert_eq!(rt_derived_facts, original_reasoning.derived_facts);
        assert_eq!(rt_iterations, original_reasoning.iterations);
        assert_eq!(rt_duration_ms, original_reasoning.duration_ms);
    }

    // ----------------------------------------------------------------
    // QueuePoison postcard round-trip
    // ----------------------------------------------------------------

    fn assert_queue_poison_args_round_trip(args: QueuePoison) {
        let bytes = postcard::to_allocvec(&args).expect("encode");
        let decoded: QueuePoison = postcard::from_bytes(&bytes).expect("decode");
        assert_eq!(decoded.ref_key, args.ref_key);
        assert_eq!(decoded.queue_id, args.queue_id);
        assert_eq!(decoded.reason, args.reason);
    }

    #[test]
    fn apply_queue_poison_args_round_trips_staging_failed_variant() {
        // `StagingFailed` is the variant emitted when the worker
        // exhausts its retry budget on a transient hiccup. Carries
        // a payload (error message + final attempt count) that has
        // to survive the wire.
        assert_queue_poison_args_round_trip(QueuePoison {
            ref_key: RefKey::new("test/db", "main"),
            queue_id: 17,
            reason: PoisonReason::StagingFailed {
                error: "CAS read timeout".to_string(),
                attempts: 3,
            },
        });
    }

    #[test]
    fn apply_queue_poison_args_round_trips_body_malformed_variant() {
        // Canonical deterministic poison — the one the original
        // local-`client_write` path would have bounced forever on a
        // follower, the very symptom the leader-forward exists to
        // close.
        assert_queue_poison_args_round_trip(QueuePoison {
            ref_key: RefKey::new("test/db", "main"),
            queue_id: 0,
            reason: PoisonReason::BodyMalformed {
                error: "invalid JSON-LD".to_string(),
            },
        });
    }

    #[test]
    fn apply_queue_poison_args_round_trips_push_cas_failed_variant() {
        // The widest variant payload-wise — two `Option<ContentId>`s
        // wrapped in the struct. Postcard handles `Option<ContentId>`
        // positionally and these fields aren't skip-attribute'd, so
        // the round-trip should preserve all four (Some, Some) /
        // (Some, None) / (None, Some) / (None, None) combinations
        // unchanged. We pick the most representative.
        assert_queue_poison_args_round_trip(QueuePoison {
            ref_key: RefKey::new("test/db", "feature"),
            queue_id: 42,
            reason: PoisonReason::PushCasFailed {
                head_at_worker: Some(cid(2)),
                expected_by_chain: Some(cid(3)),
            },
        });
    }

    #[test]
    fn apply_queue_poison_args_round_trips_worker_panic_variant() {
        // The last-resort variant. Carries an arbitrary string
        // payload; round-trip preserves it byte-for-byte.
        assert_queue_poison_args_round_trip(QueuePoison {
            ref_key: RefKey::new("test/db", "main"),
            queue_id: 5,
            reason: PoisonReason::WorkerPanic {
                message: "future-proofed invariant slip in third-party crate".to_string(),
            },
        });
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
            Command::AdvanceIndexHead(NewIndexHead {
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
            Command::AdvanceIndexHead(NewIndexHead {
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
            Command::AdvanceIndexHead(NewIndexHead {
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
