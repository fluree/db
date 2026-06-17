//! Raft state machine — the replicated nameservice.
//!
//! The state machine is the authoritative source of:
//!
//! - Ledger lifecycle (which ledgers exist, their governance config,
//!   their branches) — [`NameServiceState::ledgers`].
//! - Branch heads (the canonical commit hash for each `ledger:branch`)
//!   — [`NameServiceState::refs`].
//! - Replicated idempotency cache, so cross-leader retries dedup —
//!   [`NameServiceState::idempotency`].
//!
//! All state is small, in-memory, and serializable as one blob for
//! snapshot/restore. Commit and transaction-body bytes live in the
//! content store — the state machine only carries content ids.
//!
//! [`apply`] is the entry point: pure logic over `&mut NameServiceState`,
//! a [`Command`], and the log index the command was committed at;
//! returns a [`Response`] describing the outcome.

use crate::raft::execution_record::ExecutionRecordRef;
use crate::IdempotencyCacheKey;
use fluree_db_api::{ContentId, PolicyStats, TrackingTally};
use fluree_db_core::ledger_id::format_ledger_id;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use thiserror::Error;

/// Postcard-friendly mirror of [`TrackingTally`].
///
/// `TrackingTally` uses `#[serde(skip_serializing_if = "Option::is_none")]`
/// — intended for JSON, but postcard is a positional binary format
/// and silently drops the corresponding fields at deserialize time.
/// We mirror the shape here without the skip attribute and convert
/// at the consensus boundary. Carried on [`AdvanceRefArgs`] and
/// cached in [`ApplyRecord`] so idempotent retries return the
/// original submission's tally.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RecordedTally {
    pub time: Option<String>,
    pub fuel: Option<f64>,
    pub policy: Option<HashMap<String, PolicyStats>>,
}

impl From<&TrackingTally> for RecordedTally {
    fn from(t: &TrackingTally) -> Self {
        Self {
            time: t.time.clone(),
            fuel: t.fuel,
            policy: t.policy.clone(),
        }
    }
}

impl From<RecordedTally> for TrackingTally {
    fn from(r: RecordedTally) -> Self {
        TrackingTally {
            time: r.time,
            fuel: r.fuel,
            policy: r.policy,
        }
    }
}

/// Composite identity of a single branch within a ledger.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RefKey {
    pub ledger_id: String,
    pub branch: String,
}

impl RefKey {
    pub fn new(ledger_id: impl Into<String>, branch: impl Into<String>) -> Self {
        Self {
            ledger_id: ledger_id.into(),
            branch: branch.into(),
        }
    }
}

/// Latest published index for a branch: the content id of the
/// index root plus the logical time it covers up through. Bundled
/// so the "head present" and "t present" cases can't drift apart
/// at the type level.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexState {
    /// Content id of the index root blob.
    pub head: ContentId,
    /// Logical time the index covers up through. Always `<=` the
    /// containing [`RefEntry::t`] (we never publish an index over
    /// commits we haven't applied) and strictly monotonic across
    /// [`Command::AdvanceIndexHead`] applies.
    pub t: i64,
}

/// Authoritative head for one branch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefEntry {
    /// Content id of the commit blob this branch currently points at.
    pub head: ContentId,
    /// Logical time of the head commit.
    pub t: i64,
    /// Leader-supplied wall-clock at advancement, milliseconds since
    /// the Unix epoch. Metadata only — not used for equality,
    /// ordering, or eviction decisions.
    pub last_advanced_at_millis: u64,
    /// Log index at which the advancement was committed by Raft.
    /// Source of truth for time-of-event lookups and any index-based
    /// eviction policy.
    pub last_advanced_index: u64,
    /// Latest published index head + t for this branch, or `None`
    /// if no index has been built yet. Advanced by
    /// [`Command::AdvanceIndexHead`] (typically driven by the
    /// indexer running on the current leader).
    pub index: Option<IndexState>,
    /// Branch this one was forked from, or `None` for roots and for
    /// branches that came into being through [`Command::AdvanceRef`]'s
    /// self-healing path. Always serialized — postcard is positional
    /// and would drop a `skip_serializing_if` field on the wire.
    pub source_branch: Option<String>,
    /// Count of child branches forked from this one via
    /// [`Command::CreateBranch`]. [`Command::DropBranch`] refuses to
    /// remove a branch whose `branches` count is non-zero.
    pub branches: u32,
}

/// Lifecycle record for one ledger.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LedgerRecord {
    /// Leader-supplied wall-clock at creation, milliseconds since the
    /// Unix epoch. Metadata only.
    pub created_at_millis: u64,
    /// Log index at which the ledger was created.
    pub created_index: u64,
    /// Branches registered on this ledger. Populated by
    /// [`Command::CreateLedger`] (on init) and the self-healing branch
    /// add inside [`Command::AdvanceRef`]. Drained by
    /// [`Command::PurgeLedger`]; an empty `branches` list triggers
    /// removal of the `LedgerRecord` so the ledger name can be reused.
    pub branches: Vec<String>,
}

/// Replicated idempotency cache entry: enough state to answer a
/// duplicate submission, but no leader-side details. A different
/// leader handling a retry can't reconstruct fuel tallies or typed
/// receipts, so this layer doesn't promise them.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ApplyRecord {
    /// SHA-256 of the request body. Lets a retry detect "same key,
    /// different body" — a client bug that should surface rather
    /// than silently dedup. Populated by the legacy [`Command::AdvanceRef`]
    /// path. Queue-mediated commits use [`Self::body_cid`] instead.
    pub body_hash: [u8; 32],
    /// CAS identifier of the request body for queue-mediated commits
    /// (see `Command::EnqueueCommand`). `None` for legacy AdvanceRef
    /// applies. During the migration window both fields coexist; the
    /// AdvanceRef path goes away in a later step and this becomes
    /// the sole body identity.
    #[serde(default)]
    pub body_cid: Option<ContentId>,
    /// Wall-clock at which the cache entry was recorded, milliseconds
    /// since the Unix epoch. Used by `Command::EvictIdempotency` to
    /// age out entries past their TTL.
    #[serde(default)]
    pub recorded_at_millis: u64,
    /// Head commit produced by the original submission.
    pub head: ContentId,
    /// Logical time of that commit.
    pub t: i64,
    /// Log index at which the cache entry was recorded.
    pub recorded_index: u64,
    /// Tracking accounting from the original submission. `None` if
    /// the original didn't request tracking; carried so idempotent
    /// retries return what the original asked for.
    pub tally: Option<RecordedTally>,
}

/// Idempotency context attached to [`Command::AdvanceRef`]. The
/// leader supplies it so the state machine can populate the
/// replicated cache atomically with the ref advancement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdempotencyContext {
    pub key: IdempotencyCacheKey,
    pub body_hash: [u8; 32],
}

/// Cached outcome of a previously-processed request, keyed in
/// [`NameServiceState::idempotency`] by its [`IdempotencyCacheKey`].
/// One enum spanning both success and failure cases — `K` was
/// processed once, here's what happened. Retries with the same
/// `K` and matching `body_cid` short-circuit to the cached
/// outcome without re-running.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ApplyOutcome {
    /// Request applied successfully; head advanced to the
    /// referenced commit.
    Applied(ApplyRecord),
    /// Request was poisoned — permanent failure or retry budget
    /// exhausted. Future retries with the same `K` get the
    /// cached failure.
    Failed(PoisonRecord),
}

impl ApplyOutcome {
    /// Body CID this outcome was recorded against. `None` only
    /// for legacy [`Command::AdvanceRef`] applies that pre-date
    /// the queue work; goes away when AdvanceRef is removed.
    pub fn body_cid(&self) -> Option<&ContentId> {
        match self {
            ApplyOutcome::Applied(r) => r.body_cid.as_ref(),
            ApplyOutcome::Failed(r) => Some(&r.body_cid),
        }
    }

    /// Wall-clock the entry was recorded at, in millis since epoch.
    pub fn recorded_at_millis(&self) -> u64 {
        match self {
            ApplyOutcome::Applied(r) => r.recorded_at_millis,
            ApplyOutcome::Failed(r) => r.recorded_at_millis,
        }
    }

    /// Raft log index that recorded the entry. Unique across
    /// idempotency cache entries (each apply produces at most one),
    /// so it doubles as a deterministic tiebreaker for eviction
    /// ordering.
    pub fn recorded_index(&self) -> u64 {
        match self {
            ApplyOutcome::Applied(r) => r.recorded_index,
            ApplyOutcome::Failed(r) => r.recorded_index,
        }
    }
}

/// One pending transactor request awaiting worker processing. The
/// body itself lives in shared CAS — only the CID and a kind
/// discriminator travel through Raft. See the design doc for the
/// full rationale.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueEntry {
    pub queue_id: u64,
    /// Log index at which [`Command::EnqueueCommand`] applied.
    pub enqueued_index: u64,
    /// Leader-supplied wall-clock at enqueue, milliseconds since
    /// the Unix epoch.
    pub enqueued_at_millis: u64,
    /// Idempotency key if the caller supplied one. The matching
    /// `body_cid` check uses [`Self::body_cid`].
    pub idempotency: Option<IdempotencyCacheKey>,
    /// CAS identifier for the request body. Always Some — the
    /// leader writes the body to CAS before proposing.
    pub body_cid: ContentId,
    /// Discriminator the worker uses to choose its processing
    /// path (stage vs verify-pushed-chain).
    pub body_kind: BodyKind,
}

/// Mirror of [`crate::TransactionBody`]'s discriminator,
/// carried inline on the queue entry so the worker can route
/// without first parsing the body from CAS.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BodyKind {
    JsonLdInsert,
    JsonLdUpsert,
    JsonLdUpdate,
    TurtleInsert,
    TurtleUpsert,
    TrigUpsert,
    Sparql,
    /// Body decodes as `Vec<ContentId>` — a pushed commit chain
    /// already present in CAS. Worker verifies the chain rather
    /// than restaging.
    Pushed,
}

/// Failure outcome recorded when a worker poisons a queue entry.
/// Distinct from [`ApplyRecord`] (which only records successes)
/// so the two cases are unambiguous at lookup time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PoisonRecord {
    pub body_cid: ContentId,
    pub reason: PoisonReason,
    pub recorded_index: u64,
    pub recorded_at_millis: u64,
}

/// Why a queue entry was poisoned. Carried in [`PoisonRecord`]
/// and in [`crate::SubmissionError`] flavours surfaced to clients.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PoisonReason {
    /// Worker exhausted its retry budget on transient errors.
    StagingFailed { error: String, attempts: u32 },
    /// Body refused at staging time — invalid JSON-LD / Turtle /
    /// SPARQL, schema violation, etc.
    BodyMalformed { error: String },
    /// Policy or SHACL rejected the staged commit.
    PolicyViolation { error: String },
    /// Body referenced a ledger that doesn't exist.
    LedgerNotFound { ledger_id: String },
    /// Push body's `commit_chain[0].parent` didn't match the
    /// branch's head at worker check time.
    PushCasFailed {
        head_at_worker: Option<ContentId>,
        expected_by_chain: Option<ContentId>,
    },
    /// Worker panicked. Last-resort variant; the rest are typed
    /// to encourage operator-friendly error reporting.
    WorkerPanic { message: String },
}

/// Reason a head-mutating admin command cleared a per-branch
/// queue. Recorded in [`NameServiceState::recently_cleared`]
/// so the next worker's [`Command::ApplyHead`] sees a meaningful
/// [`DesyncReason::QueueCleared`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClearReason {
    BranchDropped,
    BranchPurged,
    BranchHeadReset,
}

/// Bounds the replicated cost of the per-branch queues. Held on
/// [`NameServiceState`] so the apply path consults the same
/// values on every node (configured at bootstrap time via
/// `RaftBootstrapConfig`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueConfig {
    /// Maximum queue depth per `RefKey`. Isolates branches from
    /// each other.
    pub per_branch_cap: usize,
    /// Maximum sum across every branch. Safety net for "N branches
    /// each at cap."
    pub global_cap: usize,
}

impl QueueConfig {
    /// Defaults documented in `docs/design/raft-command-queue.md`.
    pub const DEFAULT_PER_BRANCH: usize = 1024;
    pub const DEFAULT_GLOBAL: usize = 16384;
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            per_branch_cap: Self::DEFAULT_PER_BRANCH,
            global_cap: Self::DEFAULT_GLOBAL,
        }
    }
}

/// State machine state. Serializable as a single blob for
/// snapshotting (see [`NameServiceState::to_snapshot`]).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct NameServiceState {
    pub refs: HashMap<RefKey, RefEntry>,
    pub ledgers: HashMap<String, LedgerRecord>,
    /// Branches marked retracted (soft-dropped) but not yet purged.
    /// The branch's [`LedgerRecord`] entry and [`RefEntry`] (if born)
    /// stay in place so the name can't be reused until
    /// [`Command::PurgeLedger`] runs.
    pub retracted: HashSet<RefKey>,
    /// One cache spanning successful and failed applies — see
    /// [`ApplyOutcome`]. A retry of `K` with matching `body_cid`
    /// returns the cached variant without re-running.
    pub idempotency: HashMap<IdempotencyCacheKey, ApplyOutcome>,
    /// Per-branch FIFO of transactor work pending worker processing.
    /// See `docs/design/raft-command-queue.md`.
    #[serde(default)]
    pub queues: HashMap<RefKey, VecDeque<QueueEntry>>,
    /// Monotonic generator for [`QueueEntry::queue_id`]. State-machine
    /// local; never exposed to clients.
    #[serde(default)]
    pub next_queue_id: u64,
    /// Short-lived markers a head-mutating admin command leaves so
    /// the next [`Command::ApplyHead`] for that branch reports a
    /// meaningful [`DesyncReason::QueueCleared`]. Cleared by the
    /// `ApplyHead` apply that observes them.
    #[serde(default)]
    pub recently_cleared: HashMap<RefKey, ClearReason>,
    /// Lifetime count of idempotency entries removed by
    /// `Command::EvictIdempotency`.
    #[serde(default)]
    pub evicted_idempotency_count: u64,
    /// Queue depth limits. Configured at bootstrap and replicated
    /// in state so every node enforces the same caps.
    #[serde(default)]
    pub queue_config: QueueConfig,
}

/// Replicated commands the state machine accepts.
///
/// One operation per variant — kept narrow so [`apply`] is
/// straightforward to reason about.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Command {
    /// CAS-advance a branch head. Apply checks `expected_prev`
    /// against the current head before writing.
    AdvanceRef(AdvanceRefArgs),
    /// Advance the published index head for a branch. Driven by the
    /// indexer running on the current leader after it finishes a
    /// build. Apply enforces strict monotonicity and rejects index
    /// claims for commits the state machine hasn't applied yet.
    AdvanceIndexHead(AdvanceIndexHeadArgs),
    /// Register a branch on a ledger. The branch starts unborn — no
    /// [`RefEntry`] is created until the first
    /// [`Command::AdvanceRef`] for the branch.
    CreateLedger(CreateLedgerArgs),
    /// Fork a new branch from an existing one. Increments the
    /// source branch's child counter and records parentage on the
    /// new [`RefEntry`]. The new branch is born with the source's
    /// current head (or `at_commit` if supplied).
    CreateBranch(CreateBranchArgs),
    /// Drop a branch created via [`Command::CreateBranch`] (or
    /// implicit branch creation through [`Command::AdvanceRef`]),
    /// decrementing the parent's child counter when applicable.
    /// Refuses to remove a branch whose own `branches` count is
    /// non-zero. Unlike [`Command::PurgeLedger`], not idempotent on
    /// missing branches — returns `LedgerNotFound`.
    DropBranch { ledger_id: String, branch: String },
    /// Non-monotonic head reset for rebase/merge rollback. Sets
    /// head, t, and index from the supplied snapshot regardless of
    /// the branch's current values. A `commit_head_id: None`
    /// snapshot removes the [`RefEntry`] (branch becomes unborn).
    ResetHead {
        ledger_id: String,
        branch: String,
        snapshot: ResetHeadSnapshot,
    },
    /// Soft-drop a branch: mark it retracted but leave its
    /// [`LedgerRecord`] and [`RefEntry`] entries in place so the
    /// alias can't be reused. Idempotent.
    RetractLedger { ledger_id: String, branch: String },
    /// Hard-drop a branch: remove its [`RefEntry`], retraction mark,
    /// and entry from the parent [`LedgerRecord::branches`]. Removes
    /// the `LedgerRecord` itself when its branches list empties.
    /// Idempotent.
    PurgeLedger { ledger_id: String, branch: String },
    /// Signal that the named content blob is no longer referenced
    /// and may be released by the content store. The state machine
    /// doesn't mutate state on this — the entry's role is to let
    /// every node's content store act in sync.
    ReleaseContent { id: ContentId },
    /// Append a transactor request to the per-branch queue. Apply
    /// checks idempotency, the in-flight queue, and the queue
    /// depth caps; on success appends a [`QueueEntry`] and returns
    /// [`Response::Enqueued`]. See `docs/design/raft-command-queue.md`.
    EnqueueCommand(EnqueueCommandArgs),
    /// Advance a branch head from a worker-staged commit. Pops the
    /// per-branch queue front (must match `queue_id`), records the
    /// idempotency outcome from the entry, and signals waiters.
    /// Replaces the role of `Command::AdvanceRef` in the queue
    /// migration path.
    ApplyHead(ApplyHeadArgs),
    /// Worker gave up on a queue entry. Pops the front, records
    /// the failure in the poisoned-idempotency map keyed by the
    /// entry's idempotency key, and signals the waiter with an
    /// abort outcome.
    PoisonQueueEntry(PoisonQueueEntryArgs),
    /// Periodic leader-proposed eviction of stale idempotency
    /// records. Removes entries whose `recorded_at_millis` is
    /// older than `cutoff_millis`, bounded per apply by an
    /// internal batch size.
    EvictIdempotency { cutoff_millis: u64 },
}

/// Payload for [`Command::EnqueueCommand`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnqueueCommandArgs {
    pub ledger_id: String,
    pub branch: String,
    pub idempotency: Option<IdempotencyCacheKey>,
    /// CAS identifier for the body the leader wrote before
    /// proposing. The worker reads this back to dispatch.
    pub body_cid: ContentId,
    pub body_kind: BodyKind,
    pub applied_at_millis: u64,
}

/// Payload for [`Command::ApplyHead`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApplyHeadArgs {
    pub ledger_id: String,
    pub branch: String,
    /// Queue entry this commit was staged from. Apply rejects
    /// with [`DesyncReason::WrongFront`] if this doesn't match
    /// the per-branch queue's front.
    pub queue_id: u64,
    pub commit_id: ContentId,
    pub commit_t: i64,
    pub applied_at_millis: u64,
}

/// Payload for [`Command::PoisonQueueEntry`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoisonQueueEntryArgs {
    pub ledger_id: String,
    pub branch: String,
    pub queue_id: u64,
    pub reason: PoisonReason,
    pub applied_at_millis: u64,
}

/// Payload for [`Command::AdvanceRef`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdvanceRefArgs {
    pub ledger_id: String,
    pub branch: String,
    /// Current head the leader observed. `None` only when the
    /// branch is being newly created. Apply returns
    /// [`Response::Conflict`] when this doesn't match the
    /// state machine's current head for the branch.
    pub expected_prev: Option<ContentId>,
    /// New head to write.
    pub new_head: ContentId,
    /// Logical time of the new head.
    pub t: i64,
    /// Leader's wall-clock at proposal, millis since epoch.
    pub applied_at_millis: u64,
    /// Optional idempotency context — present iff the request
    /// carried an idempotency key.
    pub idempotency: Option<IdempotencyContext>,
    /// Execution records to release after this advance is committed.
    /// Piggybacked from the leader's pending-releases buffer so we
    /// don't need a separate Raft round-trip for cleanup. Echoed back
    /// in [`Response::Applied`] so the wrapper performs the deletes.
    pub release: Vec<ExecutionRecordRef>,
    /// Tracking accounting from staging. `Some` iff the request had
    /// tracking enabled. Stored in the idempotency cache so retries
    /// return what the original requested.
    pub tally: Option<RecordedTally>,
}

/// Payload for [`Command::AdvanceIndexHead`].
///
/// Strict monotonic update: the new `t` must be greater than the
/// branch's current `index_t`, and must not exceed the branch's
/// current commit `t` (we never publish an index that claims to
/// cover commits the state machine hasn't applied).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdvanceIndexHeadArgs {
    pub ledger_id: String,
    pub branch: String,
    /// Content id of the new index root blob.
    pub new_index_head: ContentId,
    /// Logical time the new index covers up through.
    pub t: i64,
    /// Leader's wall-clock at proposal, milliseconds since epoch.
    pub applied_at_millis: u64,
}

/// Payload for [`Command::CreateLedger`].
///
/// `ledger_id` is the bare ledger name (no branch suffix); `branch`
/// names the branch to register on that ledger. The trait surface
/// (`LedgerLifecycle::init`) takes the full `name:branch` form; the
/// adapter at `RaftNameService::init` splits it before building this
/// command.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateLedgerArgs {
    pub ledger_id: String,
    pub branch: String,
    pub created_at_millis: u64,
}

/// Payload for [`Command::CreateBranch`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateBranchArgs {
    pub ledger_id: String,
    /// New branch name to create.
    pub branch: String,
    /// Existing branch this one forks from. Must have a born
    /// [`RefEntry`] or apply returns `SourceBranchNotFound`.
    pub source_branch: String,
    /// Optional starting commit. `None` means "fork from source's
    /// current head"; `Some((id, t))` overrides with a specific
    /// historical commit on the source's chain.
    pub at_commit: Option<(ContentId, i64)>,
    /// Leader's wall-clock at proposal, milliseconds since epoch.
    pub applied_at_millis: u64,
}

/// Payload for [`Command::ResetHead`]. Mirrors
/// [`fluree_db_nameservice::NsRecordSnapshot`] in a postcard-friendly
/// shape so the apply path can reconstruct the desired state without
/// taking a direct dependency on the read-side type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResetHeadSnapshot {
    pub commit_head_id: Option<ContentId>,
    pub commit_t: i64,
    pub index_head_id: Option<ContentId>,
    pub index_t: i64,
}

/// State-machine apply outcome. The leader's pipeline builds a typed
/// caller-facing receipt from this plus its pipeline-local context.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Response {
    /// The command was applied. For [`Command::AdvanceRef`],
    /// `accepted == 1` on a fresh advancement and `0` on an
    /// idempotent retry (the cached outcome was returned instead).
    Applied {
        head_t: i64,
        head_id: ContentId,
        accepted: usize,
        /// Execution records the wrapper should release after this
        /// response. Echoed from [`AdvanceRefArgs::release`] so apply
        /// stays pure — the side-effecting deletes happen at the
        /// adapter layer above.
        release: Vec<ExecutionRecordRef>,
        /// Tracking accounting — fresh apply returns what staging
        /// produced; idempotent retry returns the cached tally from
        /// the original submission. `None` when tracking wasn't
        /// requested.
        tally: Option<RecordedTally>,
    },
    /// CAS check failed — `expected_prev` didn't match the current
    /// head. Caller's writer needs to re-stage against the
    /// returned head.
    Conflict {
        current_head: Option<ContentId>,
        current_t: Option<i64>,
    },
    /// [`Command::AdvanceIndexHead`] succeeded. Carries the new
    /// index head + t so the proposing publisher can confirm what
    /// was committed.
    IndexAdvanced {
        index_t: i64,
        index_head: ContentId,
    },
    /// [`Command::AdvanceIndexHead`] was no-op'd because the
    /// branch's published index already covers the proposed t (or
    /// further). Not an error — usually means a concurrent indexer
    /// got there first, or the proposer is retrying after partial
    /// success.
    IndexStale {
        /// Current published index t for the branch — strictly
        /// greater than or equal to the proposed t.
        current_t: i64,
    },
    /// [`Command::AdvanceIndexHead`] proposed an `index_t` that
    /// exceeds the branch's current `commit_t`. The proposer is
    /// trying to publish an index over commits the state machine
    /// hasn't applied — usually means the indexer raced ahead of
    /// the leader's apply or applied state from an older snapshot.
    /// The proposer should re-stage from the current commit head.
    IndexAhead {
        /// Branch's current commit t.
        commit_t: i64,
        /// The proposed (rejected) index t.
        proposed_t: i64,
    },
    /// [`Command::CreateLedger`] succeeded. `ledger_id` is the full
    /// `name:branch` form.
    Created { ledger_id: String },
    /// [`Command::CreateLedger`] or [`Command::CreateBranch`] failed
    /// because the branch is already registered (whether retracted
    /// or not).
    AlreadyExists { ledger_id: String },
    /// [`Command::CreateBranch`] succeeded.
    BranchCreated {
        ledger_id: String,
        head: ContentId,
        t: i64,
    },
    /// [`Command::CreateBranch`] couldn't find a born source branch
    /// to fork from.
    SourceBranchNotFound { ledger_id: String },
    /// [`Command::DropBranch`] succeeded. `parent_branches` is the
    /// updated child count of the dropped branch's source (or `None`
    /// if the dropped branch had no recorded parent — root or
    /// self-healed).
    BranchDropped {
        ledger_id: String,
        parent_branches: Option<u32>,
    },
    /// [`Command::DropBranch`] refused because the branch still has
    /// children forked from it. Caller must drop the children first.
    BranchHasChildren { ledger_id: String, children: u32 },
    /// [`Command::ResetHead`] succeeded — the branch's head, t, and
    /// index were rewritten from the supplied snapshot.
    HeadReset { ledger_id: String },
    /// [`Command::RetractLedger`] flipped a branch from active to
    /// retracted.
    Retracted { ledger_id: String },
    /// [`Command::RetractLedger`] was a no-op — the branch was
    /// already retracted, or didn't exist. Idempotent.
    AlreadyRetracted { ledger_id: String },
    /// [`Command::PurgeLedger`] removed a registered branch (any
    /// retraction state).
    Purged { ledger_id: String },
    /// [`Command::PurgeLedger`] was a no-op — the branch wasn't
    /// registered. Idempotent at the trait layer; carried as a
    /// distinct variant so event emission can skip it.
    AlreadyPurged { ledger_id: String },
    /// [`Command::AdvanceRef`] referenced a ledger that doesn't
    /// exist in the state machine.
    LedgerNotFound { ledger_id: String },
    /// [`Command::AdvanceRef`] carried an idempotency key already
    /// recorded for a different body. A client bug; surfaces rather
    /// than silently dedup.
    BodyHashMismatch,
    /// [`Command::EnqueueCommand`] appended a fresh entry to the
    /// per-branch queue. Worker will pick it up.
    Enqueued { ledger_id: String, queue_id: u64 },
    /// [`Command::EnqueueCommand`] short-circuited on a cached
    /// outcome from a previous successful apply. The caller's
    /// idempotency key matched and the body CID matched.
    IdempotencyHit { record: ApplyRecord },
    /// [`Command::EnqueueCommand`] short-circuited on a cached
    /// failure outcome. Same matching rules as
    /// [`Self::IdempotencyHit`].
    IdempotencyFailed { record: PoisonRecord },
    /// [`Command::EnqueueCommand`] found the same idempotency key
    /// (and matching body CID) already in flight in the queue.
    /// Caller waits on the existing `queue_id`.
    InFlight { ledger_id: String, queue_id: u64 },
    /// [`Command::EnqueueCommand`] was rejected because the queue
    /// depth cap is reached. Caller backs off and retries.
    QueueFull {
        ledger_id: String,
        depth: usize,
        cap: usize,
        scope: QueueFullScope,
    },
    /// [`Command::ApplyHead`] popped the queue front and advanced
    /// the branch head.
    HeadApplied {
        ledger_id: String,
        commit_id: ContentId,
        commit_t: i64,
    },
    /// [`Command::ApplyHead`] or [`Command::PoisonQueueEntry`]
    /// found the queue front didn't match `queue_id`. State
    /// unchanged; worker recovers per `reason`.
    QueueDesync {
        ledger_id: String,
        requested_queue_id: u64,
        reason: DesyncReason,
    },
    /// [`Command::PoisonQueueEntry`] popped the front and
    /// recorded the failure.
    Poisoned {
        ledger_id: String,
        queue_id: u64,
        reason: PoisonReason,
    },
    /// [`Command::EvictIdempotency`] removed `removed` entries.
    /// `released_body_cids` carries CIDs the wrapper should fan
    /// out as `Command::ReleaseContent` (or piggyback on the next
    /// `ApplyHead`).
    EvictionApplied {
        removed: usize,
        released_body_cids: Vec<ContentId>,
    },
    /// Command was understood but no state change resulted (e.g.,
    /// [`Command::ReleaseContent`]).
    NoOp,
}

/// Which cap [`Response::QueueFull`] tripped — useful so clients
/// can distinguish "this branch is hot" from "the cluster is
/// saturated."
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueueFullScope {
    PerBranch,
    Global,
}

/// Why [`Response::QueueDesync`] fired. See the design doc.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DesyncReason {
    /// Some other proposal popped the entry the worker was
    /// trying to apply. `actual_queue_id` is whatever's at the
    /// front now (which may be the next entry, or 0 if empty).
    WrongFront { actual_queue_id: u64 },
    /// Per-branch queue was drained by a head-mutating admin
    /// command between the worker's stage and apply.
    QueueCleared { reason: ClearReason },
    /// State-machine invariant violation — apply was reached
    /// without a matching admin clear marker, but the queue is
    /// missing or empty. Surfaces as an error for investigation
    /// rather than silent recovery.
    InvariantViolated { description: String },
}

/// Errors raised during snapshot serialization or restore.
#[derive(Debug, Error)]
pub enum SnapshotError {
    #[error("snapshot postcard error: {0}")]
    Postcard(#[from] postcard::Error),
}

impl NameServiceState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Serialize the full state machine state for use as a Raft
    /// snapshot.
    pub fn to_snapshot(&self) -> Result<Vec<u8>, SnapshotError> {
        Ok(postcard::to_allocvec(self)?)
    }

    /// Restore state machine state from snapshot bytes produced by
    /// [`Self::to_snapshot`].
    pub fn from_snapshot(bytes: &[u8]) -> Result<Self, SnapshotError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// Apply a replicated command to the state machine. Pure logic — no
/// I/O. `log_index` is the index of the Raft log entry that carried
/// this command, used as a deterministic alternative to wall-clock
/// time for any state-machine bookkeeping that needs it.
pub fn apply(state: &mut NameServiceState, command: Command, log_index: u64) -> Response {
    match command {
        Command::AdvanceRef(args) => advance_ref(state, log_index, args),
        Command::AdvanceIndexHead(args) => advance_index_head(state, args),
        Command::CreateLedger(args) => create_ledger(state, log_index, args),
        Command::CreateBranch(args) => create_branch(state, log_index, args),
        Command::DropBranch { ledger_id, branch } => drop_branch(state, ledger_id, branch),
        Command::ResetHead {
            ledger_id,
            branch,
            snapshot,
        } => reset_head(state, ledger_id, branch, snapshot),
        Command::RetractLedger { ledger_id, branch } => retract_ledger(state, ledger_id, branch),
        Command::PurgeLedger { ledger_id, branch } => purge_ledger(state, ledger_id, branch),
        Command::ReleaseContent { id: _ } => Response::NoOp,
        // Queue-related commands. Apply paths land in subsequent
        // commits; see docs/design/raft-command-queue.md.
        Command::EnqueueCommand(args) => apply_enqueue_command(state, log_index, args),
        Command::ApplyHead(args) => apply_head(state, log_index, args),
        Command::PoisonQueueEntry(args) => apply_poison_queue_entry(state, log_index, args),
        Command::EvictIdempotency { cutoff_millis } => {
            apply_evict_idempotency(state, cutoff_millis)
        }
    }
}

fn advance_ref(
    state: &mut NameServiceState,
    log_index: u64,
    args: AdvanceRefArgs,
) -> Response {
    let AdvanceRefArgs {
        ledger_id,
        branch,
        expected_prev,
        new_head,
        t,
        applied_at_millis,
        idempotency,
        release,
        tally,
    } = args;

    // Idempotency hit takes precedence — return the cached outcome
    // (including the original submission's tally) without re-applying
    // so a duplicate proposal is a no-op. Release list still flows
    // through: it's about *other* submissions' cleanup, independent
    // of this command's apply outcome.
    //
    // AdvanceRef only matches `Applied` cached outcomes — a
    // `Failed` cache entry under the same key means the caller
    // shouldn't be using AdvanceRef (the queue path is what
    // produces Failed entries). Treat as a body-hash mismatch
    // since the comparison is undefined.
    if let Some(ctx) = &idempotency {
        match state.idempotency.get(&ctx.key) {
            Some(ApplyOutcome::Applied(existing)) => {
                if existing.body_hash != ctx.body_hash {
                    return Response::BodyHashMismatch;
                }
                return Response::Applied {
                    head_t: existing.t,
                    head_id: existing.head.clone(),
                    accepted: 0,
                    release,
                    tally: existing.tally.clone(),
                };
            }
            Some(ApplyOutcome::Failed(_)) => {
                return Response::BodyHashMismatch;
            }
            None => {}
        }
    }

    let Some(ledger) = state.ledgers.get_mut(&ledger_id) else {
        return Response::LedgerNotFound { ledger_id };
    };

    let ref_key = RefKey::new(&ledger_id, &branch);
    let current = state.refs.get(&ref_key);
    let current_head = current.map(|r| r.head.clone());
    let current_t = current.map(|r| r.t);

    if expected_prev != current_head {
        return Response::Conflict {
            current_head,
            current_t,
        };
    }

    if !ledger.branches.contains(&branch) {
        ledger.branches.push(branch.clone());
    }

    // Carry the existing index and lineage forward across commit
    // advances — the new commit doesn't index itself (that happens
    // later via `AdvanceIndexHead`), and it doesn't change parentage
    // or child count.
    let (prior_index, prior_source, prior_branches) = state
        .refs
        .get(&ref_key)
        .map(|r| (r.index.clone(), r.source_branch.clone(), r.branches))
        .unwrap_or_default();
    state.refs.insert(
        ref_key,
        RefEntry {
            head: new_head.clone(),
            t,
            last_advanced_at_millis: applied_at_millis,
            last_advanced_index: log_index,
            index: prior_index,
            source_branch: prior_source,
            branches: prior_branches,
        },
    );

    if let Some(ctx) = idempotency {
        state.idempotency.insert(
            ctx.key,
            ApplyOutcome::Applied(ApplyRecord {
                body_hash: ctx.body_hash,
                body_cid: None,
                recorded_at_millis: applied_at_millis,
                head: new_head.clone(),
                t,
                recorded_index: log_index,
                tally: tally.clone(),
            }),
        );
    }

    Response::Applied {
        head_t: t,
        head_id: new_head,
        accepted: 1,
        release,
        tally,
    }
}

fn create_ledger(
    state: &mut NameServiceState,
    log_index: u64,
    args: CreateLedgerArgs,
) -> Response {
    let CreateLedgerArgs {
        ledger_id,
        branch,
        created_at_millis,
    } = args;

    let ledger = state.ledgers.entry(ledger_id.clone()).or_insert_with(|| LedgerRecord {
        created_at_millis,
        created_index: log_index,
        branches: Vec::new(),
    });
    if ledger.branches.contains(&branch) {
        return Response::AlreadyExists {
            ledger_id: format_ledger_id(&ledger_id, &branch),
        };
    }
    ledger.branches.push(branch.clone());
    Response::Created {
        ledger_id: format_ledger_id(&ledger_id, &branch),
    }
}

fn retract_ledger(
    state: &mut NameServiceState,
    ledger_id: String,
    branch: String,
) -> Response {
    let key = RefKey::new(&ledger_id, &branch);
    let full = format_ledger_id(&ledger_id, &branch);
    let is_known = state
        .ledgers
        .get(&ledger_id)
        .is_some_and(|l| l.branches.contains(&branch));
    if !is_known {
        // Missing ledger or branch — idempotent no-op at the trait
        // boundary. Reuse `AlreadyRetracted` so callers (and event
        // emission) treat the result uniformly.
        return Response::AlreadyRetracted { ledger_id: full };
    }
    if state.retracted.insert(key) {
        Response::Retracted { ledger_id: full }
    } else {
        Response::AlreadyRetracted { ledger_id: full }
    }
}

fn purge_ledger(
    state: &mut NameServiceState,
    ledger_id: String,
    branch: String,
) -> Response {
    let key = RefKey::new(&ledger_id, &branch);
    let full = format_ledger_id(&ledger_id, &branch);
    let removed_entry = state.refs.remove(&key);
    let removed_ref = removed_entry.is_some();
    let removed_source = removed_entry.and_then(|r| r.source_branch);
    let removed_retraction = state.retracted.remove(&key);
    let removed_branch = match state.ledgers.get_mut(&ledger_id) {
        Some(ledger) => {
            let before = ledger.branches.len();
            ledger.branches.retain(|b| b != &branch);
            let removed = ledger.branches.len() < before;
            if ledger.branches.is_empty() {
                state.ledgers.remove(&ledger_id);
            }
            removed
        }
        None => false,
    };
    if let Some(parent) = removed_source {
        decrement_child_count(state, &ledger_id, &parent);
    }
    if removed_ref || removed_retraction || removed_branch {
        Response::Purged { ledger_id: full }
    } else {
        Response::AlreadyPurged { ledger_id: full }
    }
}

fn create_branch(
    state: &mut NameServiceState,
    log_index: u64,
    args: CreateBranchArgs,
) -> Response {
    let CreateBranchArgs {
        ledger_id,
        branch,
        source_branch,
        at_commit,
        applied_at_millis,
    } = args;
    let full = format_ledger_id(&ledger_id, &branch);

    let Some(ledger) = state.ledgers.get_mut(&ledger_id) else {
        return Response::LedgerNotFound { ledger_id };
    };
    if ledger.branches.contains(&branch) {
        return Response::AlreadyExists { ledger_id: full };
    }

    let source_key = RefKey::new(&ledger_id, &source_branch);
    let Some(source) = state.refs.get(&source_key) else {
        return Response::SourceBranchNotFound {
            ledger_id: format_ledger_id(&ledger_id, &source_branch),
        };
    };
    let (head, t) = at_commit.unwrap_or_else(|| (source.head.clone(), source.t));

    // Update LedgerRecord first so the borrow on `source` from
    // `state.refs` releases before we mutate refs further.
    state
        .ledgers
        .get_mut(&ledger_id)
        .expect("ledger checked above")
        .branches
        .push(branch.clone());

    // Bump the source's child count.
    if let Some(src) = state.refs.get_mut(&source_key) {
        src.branches = src.branches.saturating_add(1);
    }

    state.refs.insert(
        RefKey::new(&ledger_id, &branch),
        RefEntry {
            head: head.clone(),
            t,
            last_advanced_at_millis: applied_at_millis,
            last_advanced_index: log_index,
            index: None,
            source_branch: Some(source_branch),
            branches: 0,
        },
    );

    Response::BranchCreated {
        ledger_id: full,
        head,
        t,
    }
}

fn drop_branch(
    state: &mut NameServiceState,
    ledger_id: String,
    branch: String,
) -> Response {
    let key = RefKey::new(&ledger_id, &branch);
    let full = format_ledger_id(&ledger_id, &branch);

    let ledger_known = state
        .ledgers
        .get(&ledger_id)
        .is_some_and(|l| l.branches.iter().any(|b| b == &branch));
    if !ledger_known {
        return Response::LedgerNotFound { ledger_id: full };
    }

    if let Some(entry) = state.refs.get(&key) {
        if entry.branches > 0 {
            return Response::BranchHasChildren {
                ledger_id: full,
                children: entry.branches,
            };
        }
    }

    let removed_source = state.refs.remove(&key).and_then(|r| r.source_branch);
    state.retracted.remove(&key);
    if let Some(ledger) = state.ledgers.get_mut(&ledger_id) {
        ledger.branches.retain(|b| b != &branch);
        if ledger.branches.is_empty() {
            state.ledgers.remove(&ledger_id);
        }
    }
    let parent_branches = removed_source
        .as_deref()
        .map(|parent| decrement_child_count(state, &ledger_id, parent));

    Response::BranchDropped {
        ledger_id: full,
        parent_branches,
    }
}

fn reset_head(
    state: &mut NameServiceState,
    ledger_id: String,
    branch: String,
    snapshot: ResetHeadSnapshot,
) -> Response {
    let key = RefKey::new(&ledger_id, &branch);
    let full = format_ledger_id(&ledger_id, &branch);
    let ledger_known = state
        .ledgers
        .get(&ledger_id)
        .is_some_and(|l| l.branches.iter().any(|b| b == &branch));
    if !ledger_known {
        return Response::LedgerNotFound { ledger_id: full };
    }

    let ResetHeadSnapshot {
        commit_head_id,
        commit_t,
        index_head_id,
        index_t,
    } = snapshot;

    let Some(head) = commit_head_id else {
        // Snapshot is unborn — remove the RefEntry; the branch keeps
        // its slot on `LedgerRecord.branches`.
        state.refs.remove(&key);
        return Response::HeadReset { ledger_id: full };
    };

    let (prior_source, prior_branches) = state
        .refs
        .get(&key)
        .map(|r| (r.source_branch.clone(), r.branches))
        .unwrap_or_default();
    let index = index_head_id.map(|head| IndexState { head, t: index_t });
    state.refs.insert(
        key,
        RefEntry {
            head,
            t: commit_t,
            last_advanced_at_millis: 0,
            last_advanced_index: 0,
            index,
            source_branch: prior_source,
            branches: prior_branches,
        },
    );
    Response::HeadReset { ledger_id: full }
}

/// Saturating decrement of a parent branch's child counter. Returns
/// the post-decrement count, or `0` if the parent is gone.
fn decrement_child_count(
    state: &mut NameServiceState,
    ledger_id: &str,
    parent_branch: &str,
) -> u32 {
    if let Some(parent) = state.refs.get_mut(&RefKey::new(ledger_id, parent_branch)) {
        parent.branches = parent.branches.saturating_sub(1);
        parent.branches
    } else {
        0
    }
}

fn advance_index_head(
    state: &mut NameServiceState,
    args: AdvanceIndexHeadArgs,
) -> Response {
    let AdvanceIndexHeadArgs {
        ledger_id,
        branch,
        new_index_head,
        t,
        applied_at_millis,
    } = args;

    if !state.ledgers.contains_key(&ledger_id) {
        return Response::LedgerNotFound { ledger_id };
    }

    let ref_key = RefKey::new(&ledger_id, &branch);
    let Some(entry) = state.refs.get_mut(&ref_key) else {
        // No ref means no commits on this branch yet — nothing to
        // index. Reuse `LedgerNotFound` since `advance_ref`
        // does the same for the "no refs yet" case at the caller's
        // boundary, keeping the response surface narrow.
        return Response::LedgerNotFound { ledger_id };
    };

    // Strict monotonic — concurrent indexers racing to publish only
    // the latest survive. Equal `t` is treated as stale on purpose:
    // the existing entry already covers everything this proposal
    // would, so re-writing risks rewriting a content-equivalent
    // root with a different cid (e.g. different leaf ordering) for
    // no benefit.
    if let Some(existing) = &entry.index {
        if t <= existing.t {
            return Response::IndexStale {
                current_t: existing.t,
            };
        }
    }

    // The index can't claim to cover commits the state machine
    // hasn't applied. This shouldn't normally happen — the leader
    // runs the indexer against its own applied state — but a
    // leadership transition mid-build can race: a stepped-down
    // leader finishes a build after the new leader has reset to an
    // older state. Reject so the proposer re-stages.
    if t > entry.t {
        return Response::IndexAhead {
            commit_t: entry.t,
            proposed_t: t,
        };
    }

    entry.index = Some(IndexState {
        head: new_index_head.clone(),
        t,
    });
    entry.last_advanced_at_millis = applied_at_millis;

    Response::IndexAdvanced {
        index_t: t,
        index_head: new_index_head,
    }
}

fn apply_enqueue_command(
    state: &mut NameServiceState,
    log_index: u64,
    args: EnqueueCommandArgs,
) -> Response {
    let EnqueueCommandArgs {
        ledger_id,
        branch,
        idempotency,
        body_cid,
        body_kind,
        applied_at_millis,
    } = args;
    let full_ledger_id = format_ledger_id(&ledger_id, &branch);
    let ref_key = RefKey::new(&ledger_id, &branch);

    // 1. Idempotency cache — one lookup, branch on outcome
    //    variant. The body_cid must match; mismatched bodies
    //    under the same key are a client bug we surface rather
    //    than silently dedup. Legacy AdvanceRef-populated success
    //    entries have `body_cid: None`; any queue retry against
    //    one of those can't safely match and surfaces as a
    //    BodyHashMismatch (the comparison is undefined).
    if let Some(key) = idempotency.as_ref() {
        if let Some(outcome) = state.idempotency.get(key) {
            if outcome.body_cid() != Some(&body_cid) {
                return Response::BodyHashMismatch;
            }
            return match outcome {
                ApplyOutcome::Applied(record) => {
                    Response::IdempotencyHit { record: record.clone() }
                }
                ApplyOutcome::Failed(record) => {
                    Response::IdempotencyFailed { record: record.clone() }
                }
            };
        }
        // 2. In-flight queue scan. Same key + same body → ride the
        //    existing entry. Same key + different body → collision.
        if let Some(queue) = state.queues.get(&ref_key) {
            for entry in queue {
                if entry.idempotency.as_ref() == Some(key) {
                    return if entry.body_cid == body_cid {
                        Response::InFlight {
                            ledger_id: full_ledger_id,
                            queue_id: entry.queue_id,
                        }
                    } else {
                        Response::BodyHashMismatch
                    };
                }
            }
        }
    }

    // 3. Cap checks. Per-branch first (most isolation), then global.
    let per_branch_cap = state.queue_config.per_branch_cap;
    let per_branch_depth = state
        .queues
        .get(&ref_key)
        .map(VecDeque::len)
        .unwrap_or(0);
    if per_branch_depth >= per_branch_cap {
        return Response::QueueFull {
            ledger_id: full_ledger_id,
            depth: per_branch_depth,
            cap: per_branch_cap,
            scope: QueueFullScope::PerBranch,
        };
    }
    let global_cap = state.queue_config.global_cap;
    let global_depth: usize = state.queues.values().map(VecDeque::len).sum();
    if global_depth >= global_cap {
        return Response::QueueFull {
            ledger_id: full_ledger_id,
            depth: global_depth,
            cap: global_cap,
            scope: QueueFullScope::Global,
        };
    }

    // 4. Append.
    let queue_id = state.next_queue_id;
    state.next_queue_id = state.next_queue_id.wrapping_add(1);
    let entry = QueueEntry {
        queue_id,
        enqueued_index: log_index,
        enqueued_at_millis: applied_at_millis,
        idempotency,
        body_cid,
        body_kind,
    };
    state.queues.entry(ref_key).or_default().push_back(entry);

    Response::Enqueued {
        ledger_id: full_ledger_id,
        queue_id,
    }
}

/// Validate the queue front against the worker's claim and pop it.
///
/// Both `ApplyHead` and `PoisonQueueEntry` consume the same front entry
/// after the same three-step check (admin-preemption marker, queue
/// existence, front-id match). On any mismatch this returns the
/// `Response::QueueDesync` the caller should propagate. The `Response`
/// is boxed because the `Ok` path is the hot one and `Response` is
/// large; only the rare desync branch pays the allocation.
fn pop_validated_front(
    state: &mut NameServiceState,
    ref_key: &RefKey,
    full_ledger_id: &str,
    requested_queue_id: u64,
) -> Result<QueueEntry, Box<Response>> {
    if let Some(clear_reason) = state.recently_cleared.remove(ref_key) {
        return Err(Box::new(Response::QueueDesync {
            ledger_id: full_ledger_id.into(),
            requested_queue_id,
            reason: DesyncReason::QueueCleared {
                reason: clear_reason,
            },
        }));
    }

    let queue = match state.queues.get_mut(ref_key) {
        Some(q) if !q.is_empty() => q,
        _ => {
            return Err(Box::new(Response::QueueDesync {
                ledger_id: full_ledger_id.into(),
                requested_queue_id,
                reason: DesyncReason::InvariantViolated {
                    description: "queue missing or empty without recently_cleared marker"
                        .into(),
                },
            }));
        }
    };

    let actual_front = queue.front().expect("non-empty checked above").queue_id;
    if actual_front != requested_queue_id {
        return Err(Box::new(Response::QueueDesync {
            ledger_id: full_ledger_id.into(),
            requested_queue_id,
            reason: DesyncReason::WrongFront {
                actual_queue_id: actual_front,
            },
        }));
    }

    Ok(queue.pop_front().expect("non-empty checked above"))
}

fn apply_head(
    state: &mut NameServiceState,
    log_index: u64,
    args: ApplyHeadArgs,
) -> Response {
    let ApplyHeadArgs {
        ledger_id,
        branch,
        queue_id,
        commit_id,
        commit_t,
        applied_at_millis,
    } = args;
    let full_ledger_id = format_ledger_id(&ledger_id, &branch);
    let ref_key = RefKey::new(&ledger_id, &branch);

    let entry = match pop_validated_front(state, &ref_key, &full_ledger_id, queue_id) {
        Ok(entry) => entry,
        Err(resp) => return *resp,
    };

    // Advance the branch's `RefEntry`, carrying forward index +
    // lineage state from any existing entry (matches the
    // self-healing pattern in `advance_ref`).
    let (prior_index, prior_source, prior_branches) = state
        .refs
        .get(&ref_key)
        .map(|r| (r.index.clone(), r.source_branch.clone(), r.branches))
        .unwrap_or_default();
    state.refs.insert(
        ref_key.clone(),
        RefEntry {
            head: commit_id.clone(),
            t: commit_t,
            last_advanced_at_millis: applied_at_millis,
            last_advanced_index: log_index,
            index: prior_index,
            source_branch: prior_source,
            branches: prior_branches,
        },
    );

    // Self-healing branch registration on the `LedgerRecord`,
    // matching `advance_ref`'s behaviour so the queue path doesn't
    // diverge.
    if let Some(ledger) = state.ledgers.get_mut(&ledger_id) {
        if !ledger.branches.contains(&branch) {
            ledger.branches.push(branch.clone());
        }
    }

    if let Some(key) = entry.idempotency {
        state.idempotency.insert(
            key,
            ApplyOutcome::Applied(ApplyRecord {
                body_hash: [0u8; 32],
                body_cid: Some(entry.body_cid),
                recorded_at_millis: applied_at_millis,
                head: commit_id.clone(),
                t: commit_t,
                recorded_index: log_index,
                tally: None,
            }),
        );
    }

    Response::HeadApplied {
        ledger_id: full_ledger_id,
        commit_id,
        commit_t,
    }
}

fn apply_poison_queue_entry(
    state: &mut NameServiceState,
    log_index: u64,
    args: PoisonQueueEntryArgs,
) -> Response {
    let PoisonQueueEntryArgs {
        ledger_id,
        branch,
        queue_id,
        reason,
        applied_at_millis,
    } = args;
    let full_ledger_id = format_ledger_id(&ledger_id, &branch);
    let ref_key = RefKey::new(&ledger_id, &branch);

    let entry = match pop_validated_front(state, &ref_key, &full_ledger_id, queue_id) {
        Ok(entry) => entry,
        Err(resp) => return *resp,
    };

    if let Some(key) = entry.idempotency {
        state.idempotency.insert(
            key,
            ApplyOutcome::Failed(PoisonRecord {
                body_cid: entry.body_cid,
                reason: reason.clone(),
                recorded_index: log_index,
                recorded_at_millis: applied_at_millis,
            }),
        );
    }

    Response::Poisoned {
        ledger_id: full_ledger_id,
        queue_id,
        reason,
    }
}

/// Maximum entries removed by a single [`Command::EvictIdempotency`]
/// apply. Bounds the work each follower replays so a large backlog
/// can't stall replication; the periodic evictor schedules
/// additional commands when more remain.
const EVICT_BATCH_SIZE: usize = 1024;

fn apply_evict_idempotency(state: &mut NameServiceState, cutoff_millis: u64) -> Response {
    // HashMap iteration order is non-deterministic across replicas,
    // so we materialize the expired candidates and sort by
    // (recorded_at_millis, recorded_index) before truncating to the
    // batch cap. recorded_index is unique across cache entries —
    // each apply records at most one — so the sort is total.
    let mut expired: Vec<(u64, u64, IdempotencyCacheKey)> = state
        .idempotency
        .iter()
        .filter_map(|(key, outcome)| {
            let recorded_at = outcome.recorded_at_millis();
            (recorded_at < cutoff_millis)
                .then(|| (recorded_at, outcome.recorded_index(), key.clone()))
        })
        .collect();
    expired.sort_by_key(|(at, idx, _)| (*at, *idx));
    expired.truncate(EVICT_BATCH_SIZE);

    let mut released_body_cids = Vec::with_capacity(expired.len());
    for (_, _, key) in &expired {
        match state.idempotency.remove(key) {
            Some(ApplyOutcome::Applied(record)) => {
                if let Some(cid) = record.body_cid {
                    released_body_cids.push(cid);
                }
            }
            Some(ApplyOutcome::Failed(record)) => {
                released_body_cids.push(record.body_cid);
            }
            None => {}
        }
    }
    let removed = expired.len();
    state.evicted_idempotency_count += removed as u64;

    Response::EvictionApplied {
        removed,
        released_body_cids,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::IdempotencyKey;

    fn cid(seed: u8) -> ContentId {
        // Deterministic per-seed ContentId — `new` hashes the input
        // bytes, so distinct seeds produce distinct CIDs.
        ContentId::new(fluree_db_api::ContentKind::Commit, &[seed])
    }

    fn create_ledger(ledger_id: &str) -> Command {
        create_branch_cmd(ledger_id, "main")
    }

    fn create_branch_cmd(ledger_id: &str, branch: &str) -> Command {
        Command::CreateLedger(CreateLedgerArgs {
            ledger_id: ledger_id.into(),
            branch: branch.into(),
            created_at_millis: 1_000,
        })
    }

    /// Init the ledger and seed `main` at `t=0, head=cid(0)`. Most
    /// pre-slim tests assumed init produced this initial ref;
    /// keeping the helper preserves their semantics with one extra
    /// `apply` call.
    fn create_ledger_with_genesis(state: &mut NameServiceState, ledger_id: &str) {
        apply(state, create_ledger(ledger_id), 1);
        apply(
            state,
            advance(ledger_id, "main", None, cid(0), 0, None),
            1,
        );
    }

    fn advance(
        ledger_id: &str,
        branch: &str,
        expected_prev: Option<ContentId>,
        new_head: ContentId,
        t: i64,
        idempotency: Option<IdempotencyContext>,
    ) -> Command {
        advance_with_release(
            ledger_id,
            branch,
            expected_prev,
            new_head,
            t,
            idempotency,
            Vec::new(),
        )
    }

    fn advance_with_release(
        ledger_id: &str,
        branch: &str,
        expected_prev: Option<ContentId>,
        new_head: ContentId,
        t: i64,
        idempotency: Option<IdempotencyContext>,
        release: Vec<ExecutionRecordRef>,
    ) -> Command {
        Command::AdvanceRef(AdvanceRefArgs {
            ledger_id: ledger_id.into(),
            branch: branch.into(),
            expected_prev,
            new_head,
            t,
            applied_at_millis: 2_000,
            idempotency,
            release,
            tally: None,
        })
    }

    fn ctx(ledger_id: &str, key: &str, body_hash: [u8; 32]) -> IdempotencyContext {
        IdempotencyContext {
            key: IdempotencyCacheKey::new(ledger_id, IdempotencyKey::new(key)),
            body_hash,
        }
    }

    #[test]
    fn create_ledger_registers_branch_unborn() {
        let mut state = NameServiceState::new();
        let resp = apply(&mut state, create_ledger("test/db"), 1);
        assert_eq!(
            resp,
            Response::Created {
                ledger_id: "test/db:main".into()
            }
        );
        // LedgerRecord is created with the branch registered, but no
        // RefEntry yet — the branch is unborn until the first
        // AdvanceRef populates it.
        assert_eq!(state.ledgers.len(), 1);
        assert_eq!(state.refs.len(), 0);
        let ledger = state.ledgers.get("test/db").expect("ledger record present");
        assert_eq!(ledger.branches, vec!["main".to_string()]);
        assert_eq!(ledger.created_index, 1);
    }

    #[test]
    fn create_ledger_registers_multiple_branches_on_same_ledger() {
        let mut state = NameServiceState::new();
        apply(&mut state, create_branch_cmd("test/db", "main"), 1);
        let resp = apply(&mut state, create_branch_cmd("test/db", "feature"), 2);
        assert_eq!(
            resp,
            Response::Created {
                ledger_id: "test/db:feature".into()
            }
        );
        let ledger = state.ledgers.get("test/db").unwrap();
        assert_eq!(ledger.branches, vec!["main".to_string(), "feature".to_string()]);
    }

    #[test]
    fn create_ledger_returns_already_exists_on_duplicate_branch() {
        let mut state = NameServiceState::new();
        apply(&mut state, create_ledger("test/db"), 1);
        let resp = apply(&mut state, create_ledger("test/db"), 2);
        assert_eq!(
            resp,
            Response::AlreadyExists {
                ledger_id: "test/db:main".into()
            }
        );
        assert_eq!(state.ledgers.len(), 1);
    }

    #[test]
    fn create_ledger_returns_already_exists_even_when_branch_is_retracted() {
        let mut state = NameServiceState::new();
        apply(&mut state, create_ledger("test/db"), 1);
        apply(
            &mut state,
            Command::RetractLedger {
                ledger_id: "test/db".into(),
                branch: "main".into(),
            },
            2,
        );
        let resp = apply(&mut state, create_ledger("test/db"), 3);
        // Trait contract: retracted records hold the alias until
        // purged. Re-init has to fail so the caller is forced
        // through purge first.
        assert_eq!(
            resp,
            Response::AlreadyExists {
                ledger_id: "test/db:main".into()
            }
        );
    }

    #[test]
    fn advance_ref_succeeds_when_expected_prev_matches() {
        let mut state = NameServiceState::new();
        create_ledger_with_genesis(&mut state, "test/db");
        let resp = apply(
            &mut state,
            advance("test/db", "main", Some(cid(0)), cid(1), 1, None),
            2,
        );
        assert_eq!(
            resp,
            Response::Applied {
                head_t: 1,
                head_id: cid(1),
                accepted: 1,
                release: vec![],
                tally: None,
            }
        );
        let ref_entry = state.refs.get(&RefKey::new("test/db", "main")).unwrap();
        assert_eq!(ref_entry.head, cid(1));
        assert_eq!(ref_entry.last_advanced_index, 2);
    }

    #[test]
    fn advance_ref_returns_conflict_on_cas_mismatch() {
        let mut state = NameServiceState::new();
        create_ledger_with_genesis(&mut state, "test/db");
        let resp = apply(
            &mut state,
            advance("test/db", "main", Some(cid(99)), cid(1), 1, None),
            2,
        );
        assert_eq!(
            resp,
            Response::Conflict {
                current_head: Some(cid(0)),
                current_t: Some(0),
            }
        );
        // Ref untouched.
        let ref_entry = state.refs.get(&RefKey::new("test/db", "main")).unwrap();
        assert_eq!(ref_entry.head, cid(0));
    }

    #[test]
    fn advance_ref_creates_new_branch_when_expected_prev_is_none() {
        let mut state = NameServiceState::new();
        apply(&mut state, create_ledger("test/db"), 1);
        let resp = apply(
            &mut state,
            advance("test/db", "feature", None, cid(2), 5, None),
            3,
        );
        assert_eq!(
            resp,
            Response::Applied {
                head_t: 5,
                head_id: cid(2),
                accepted: 1,
                release: vec![],
                tally: None,
            }
        );
        let ledger = state.ledgers.get("test/db").unwrap();
        assert!(ledger.branches.contains(&"feature".to_string()));
    }

    #[test]
    fn advance_ref_returns_ledger_not_found_for_unknown_ledger() {
        let mut state = NameServiceState::new();
        let resp = apply(
            &mut state,
            advance("missing/db", "main", None, cid(1), 1, None),
            1,
        );
        assert_eq!(
            resp,
            Response::LedgerNotFound {
                ledger_id: "missing/db".into()
            }
        );
    }

    #[test]
    fn advance_ref_caches_idempotency_outcome() {
        let mut state = NameServiceState::new();
        create_ledger_with_genesis(&mut state, "test/db");

        let body_hash = [7u8; 32];
        let resp1 = apply(
            &mut state,
            advance(
                "test/db",
                "main",
                Some(cid(0)),
                cid(1),
                1,
                Some(ctx("test/db", "k1", body_hash)),
            ),
            2,
        );
        assert_eq!(
            resp1,
            Response::Applied {
                head_t: 1,
                head_id: cid(1),
                accepted: 1,
                release: vec![],
                tally: None,
            }
        );

        // Retry with the same key + body — no re-apply, cached
        // outcome returned, accepted == 0.
        let resp2 = apply(
            &mut state,
            advance(
                "test/db",
                "main",
                Some(cid(1)),
                cid(2),
                2,
                Some(ctx("test/db", "k1", body_hash)),
            ),
            3,
        );
        assert_eq!(
            resp2,
            Response::Applied {
                head_t: 1,
                head_id: cid(1),
                accepted: 0,
                release: vec![],
                tally: None,
            }
        );

        // Head unchanged.
        let ref_entry = state.refs.get(&RefKey::new("test/db", "main")).unwrap();
        assert_eq!(ref_entry.head, cid(1));
    }

    #[test]
    fn advance_ref_rejects_body_hash_collision_on_idempotent_retry() {
        let mut state = NameServiceState::new();
        create_ledger_with_genesis(&mut state, "test/db");

        apply(
            &mut state,
            advance(
                "test/db",
                "main",
                Some(cid(0)),
                cid(1),
                1,
                Some(ctx("test/db", "k1", [7u8; 32])),
            ),
            2,
        );

        let resp = apply(
            &mut state,
            advance(
                "test/db",
                "main",
                Some(cid(1)),
                cid(2),
                2,
                Some(ctx("test/db", "k1", [8u8; 32])),
            ),
            3,
        );
        assert_eq!(resp, Response::BodyHashMismatch);
    }

    #[test]
    fn retract_flips_active_branch_to_retracted() {
        let mut state = NameServiceState::new();
        apply(&mut state, create_ledger("test/db"), 1);
        let resp = apply(
            &mut state,
            Command::RetractLedger {
                ledger_id: "test/db".into(),
                branch: "main".into(),
            },
            2,
        );
        assert_eq!(
            resp,
            Response::Retracted {
                ledger_id: "test/db:main".into()
            }
        );
        assert!(state.retracted.contains(&RefKey::new("test/db", "main")));
    }

    #[test]
    fn retract_already_retracted_is_idempotent_noop() {
        let mut state = NameServiceState::new();
        apply(&mut state, create_ledger("test/db"), 1);
        apply(
            &mut state,
            Command::RetractLedger {
                ledger_id: "test/db".into(),
                branch: "main".into(),
            },
            2,
        );
        let resp = apply(
            &mut state,
            Command::RetractLedger {
                ledger_id: "test/db".into(),
                branch: "main".into(),
            },
            3,
        );
        assert_eq!(
            resp,
            Response::AlreadyRetracted {
                ledger_id: "test/db:main".into()
            }
        );
    }

    #[test]
    fn retract_unknown_branch_is_idempotent_noop() {
        let mut state = NameServiceState::new();
        // Even with no ledger record we report AlreadyRetracted so
        // the trait surface stays Ok.
        let resp = apply(
            &mut state,
            Command::RetractLedger {
                ledger_id: "missing".into(),
                branch: "main".into(),
            },
            1,
        );
        assert_eq!(
            resp,
            Response::AlreadyRetracted {
                ledger_id: "missing:main".into()
            }
        );
    }

    #[test]
    fn purge_removes_branch_and_ref_and_retraction_mark() {
        let mut state = NameServiceState::new();
        create_ledger_with_genesis(&mut state, "test/db");
        apply(
            &mut state,
            Command::RetractLedger {
                ledger_id: "test/db".into(),
                branch: "main".into(),
            },
            3,
        );
        assert!(state.refs.contains_key(&RefKey::new("test/db", "main")));
        assert!(state.retracted.contains(&RefKey::new("test/db", "main")));

        let resp = apply(
            &mut state,
            Command::PurgeLedger {
                ledger_id: "test/db".into(),
                branch: "main".into(),
            },
            4,
        );
        assert_eq!(
            resp,
            Response::Purged {
                ledger_id: "test/db:main".into()
            }
        );
        assert!(!state.refs.contains_key(&RefKey::new("test/db", "main")));
        assert!(!state.retracted.contains(&RefKey::new("test/db", "main")));
        // Last branch on the ledger — the LedgerRecord drops too so
        // the name can be reused.
        assert!(!state.ledgers.contains_key("test/db"));
    }

    #[test]
    fn purge_keeps_ledger_record_when_other_branches_remain() {
        let mut state = NameServiceState::new();
        apply(&mut state, create_branch_cmd("test/db", "main"), 1);
        apply(&mut state, create_branch_cmd("test/db", "feature"), 2);
        apply(
            &mut state,
            Command::PurgeLedger {
                ledger_id: "test/db".into(),
                branch: "feature".into(),
            },
            3,
        );
        let ledger = state.ledgers.get("test/db").expect("record still present");
        assert_eq!(ledger.branches, vec!["main".to_string()]);
    }

    #[test]
    fn purge_missing_branch_is_idempotent() {
        let mut state = NameServiceState::new();
        let resp = apply(
            &mut state,
            Command::PurgeLedger {
                ledger_id: "missing".into(),
                branch: "main".into(),
            },
            1,
        );
        assert_eq!(
            resp,
            Response::AlreadyPurged {
                ledger_id: "missing:main".into()
            }
        );
    }

    #[test]
    fn purge_clears_the_alias_so_init_can_reuse_it() {
        let mut state = NameServiceState::new();
        apply(&mut state, create_ledger("test/db"), 1);
        apply(
            &mut state,
            Command::PurgeLedger {
                ledger_id: "test/db".into(),
                branch: "main".into(),
            },
            2,
        );
        // After purge, init succeeds again — the alias was released.
        let resp = apply(&mut state, create_ledger("test/db"), 3);
        assert_eq!(
            resp,
            Response::Created {
                ledger_id: "test/db:main".into()
            }
        );
    }

    // ------------------------------------------------------------
    // CreateBranch / DropBranch / ResetHead
    // ------------------------------------------------------------

    fn create_branch_cmd_helper(
        ledger_id: &str,
        new_branch: &str,
        source_branch: &str,
        at_commit: Option<(ContentId, i64)>,
    ) -> Command {
        Command::CreateBranch(CreateBranchArgs {
            ledger_id: ledger_id.into(),
            branch: new_branch.into(),
            source_branch: source_branch.into(),
            at_commit,
            applied_at_millis: 2_000,
        })
    }

    #[test]
    fn create_branch_forks_from_source_head_when_at_commit_is_none() {
        let mut state = NameServiceState::new();
        create_ledger_with_genesis(&mut state, "test/db");
        apply(
            &mut state,
            advance("test/db", "main", Some(cid(0)), cid(1), 7, None),
            2,
        );

        let resp = apply(
            &mut state,
            create_branch_cmd_helper("test/db", "feature", "main", None),
            3,
        );
        assert_eq!(
            resp,
            Response::BranchCreated {
                ledger_id: "test/db:feature".into(),
                head: cid(1),
                t: 7,
            }
        );
        let feature = state.refs.get(&RefKey::new("test/db", "feature")).unwrap();
        assert_eq!(feature.head, cid(1));
        assert_eq!(feature.source_branch, Some("main".to_string()));
        let main = state.refs.get(&RefKey::new("test/db", "main")).unwrap();
        assert_eq!(main.branches, 1);
    }

    #[test]
    fn create_branch_uses_at_commit_when_supplied() {
        let mut state = NameServiceState::new();
        create_ledger_with_genesis(&mut state, "test/db");
        apply(
            &mut state,
            advance("test/db", "main", Some(cid(0)), cid(1), 7, None),
            2,
        );

        let resp = apply(
            &mut state,
            create_branch_cmd_helper("test/db", "feature", "main", Some((cid(0), 0))),
            3,
        );
        assert_eq!(
            resp,
            Response::BranchCreated {
                ledger_id: "test/db:feature".into(),
                head: cid(0),
                t: 0,
            }
        );
    }

    #[test]
    fn create_branch_rejects_when_ledger_missing() {
        let mut state = NameServiceState::new();
        let resp = apply(
            &mut state,
            create_branch_cmd_helper("missing", "feature", "main", None),
            1,
        );
        assert_eq!(
            resp,
            Response::LedgerNotFound {
                ledger_id: "missing".into()
            }
        );
    }

    #[test]
    fn create_branch_rejects_when_source_unborn() {
        // The source branch exists in `LedgerRecord.branches` but
        // has no RefEntry yet — it's unborn, so it has no head to
        // fork from.
        let mut state = NameServiceState::new();
        apply(&mut state, create_ledger("test/db"), 1);
        let resp = apply(
            &mut state,
            create_branch_cmd_helper("test/db", "feature", "main", None),
            2,
        );
        assert_eq!(
            resp,
            Response::SourceBranchNotFound {
                ledger_id: "test/db:main".into()
            }
        );
    }

    #[test]
    fn create_branch_rejects_duplicate() {
        let mut state = NameServiceState::new();
        create_ledger_with_genesis(&mut state, "test/db");
        apply(
            &mut state,
            create_branch_cmd_helper("test/db", "feature", "main", None),
            2,
        );
        let resp = apply(
            &mut state,
            create_branch_cmd_helper("test/db", "feature", "main", None),
            3,
        );
        assert_eq!(
            resp,
            Response::AlreadyExists {
                ledger_id: "test/db:feature".into()
            }
        );
    }

    #[test]
    fn drop_branch_removes_record_and_decrements_parent_counter() {
        let mut state = NameServiceState::new();
        create_ledger_with_genesis(&mut state, "test/db");
        apply(
            &mut state,
            create_branch_cmd_helper("test/db", "feature", "main", None),
            2,
        );
        let resp = apply(
            &mut state,
            Command::DropBranch {
                ledger_id: "test/db".into(),
                branch: "feature".into(),
            },
            3,
        );
        assert_eq!(
            resp,
            Response::BranchDropped {
                ledger_id: "test/db:feature".into(),
                parent_branches: Some(0),
            }
        );
        assert!(!state.refs.contains_key(&RefKey::new("test/db", "feature")));
        let main = state.refs.get(&RefKey::new("test/db", "main")).unwrap();
        assert_eq!(main.branches, 0);
    }

    #[test]
    fn drop_branch_refuses_when_branch_has_children() {
        let mut state = NameServiceState::new();
        create_ledger_with_genesis(&mut state, "test/db");
        apply(
            &mut state,
            create_branch_cmd_helper("test/db", "feature", "main", None),
            2,
        );
        // `main` has one child now — can't drop until the child is
        // dropped first.
        let resp = apply(
            &mut state,
            Command::DropBranch {
                ledger_id: "test/db".into(),
                branch: "main".into(),
            },
            3,
        );
        assert_eq!(
            resp,
            Response::BranchHasChildren {
                ledger_id: "test/db:main".into(),
                children: 1,
            }
        );
        // State untouched.
        assert!(state.refs.contains_key(&RefKey::new("test/db", "main")));
    }

    #[test]
    fn drop_branch_errors_when_branch_missing() {
        let mut state = NameServiceState::new();
        let resp = apply(
            &mut state,
            Command::DropBranch {
                ledger_id: "missing".into(),
                branch: "main".into(),
            },
            1,
        );
        assert_eq!(
            resp,
            Response::LedgerNotFound {
                ledger_id: "missing:main".into()
            }
        );
    }

    #[test]
    fn drop_branch_returns_none_parent_for_root() {
        let mut state = NameServiceState::new();
        create_ledger_with_genesis(&mut state, "test/db");
        // `main` was added via CreateLedger + AdvanceRef — no
        // recorded parent.
        let resp = apply(
            &mut state,
            Command::DropBranch {
                ledger_id: "test/db".into(),
                branch: "main".into(),
            },
            2,
        );
        assert_eq!(
            resp,
            Response::BranchDropped {
                ledger_id: "test/db:main".into(),
                parent_branches: None,
            }
        );
        // Last branch on the ledger — LedgerRecord drops too.
        assert!(!state.ledgers.contains_key("test/db"));
    }

    #[test]
    fn purge_decrements_parent_counter_when_branch_has_a_source() {
        let mut state = NameServiceState::new();
        create_ledger_with_genesis(&mut state, "test/db");
        apply(
            &mut state,
            create_branch_cmd_helper("test/db", "feature", "main", None),
            2,
        );
        apply(
            &mut state,
            Command::PurgeLedger {
                ledger_id: "test/db".into(),
                branch: "feature".into(),
            },
            3,
        );
        let main = state.refs.get(&RefKey::new("test/db", "main")).unwrap();
        assert_eq!(main.branches, 0);
    }

    #[test]
    fn reset_head_rewrites_branch_state_from_snapshot() {
        let mut state = NameServiceState::new();
        create_ledger_with_genesis(&mut state, "test/db");
        apply(
            &mut state,
            advance("test/db", "main", Some(cid(0)), cid(5), 10, None),
            2,
        );
        apply(
            &mut state,
            advance_index("test/db", "main", cid(42), 10),
            3,
        );

        let resp = apply(
            &mut state,
            Command::ResetHead {
                ledger_id: "test/db".into(),
                branch: "main".into(),
                snapshot: ResetHeadSnapshot {
                    commit_head_id: Some(cid(0)),
                    commit_t: 0,
                    index_head_id: None,
                    index_t: 0,
                },
            },
            4,
        );
        assert_eq!(
            resp,
            Response::HeadReset {
                ledger_id: "test/db:main".into()
            }
        );
        let entry = state.refs.get(&RefKey::new("test/db", "main")).unwrap();
        assert_eq!(entry.head, cid(0));
        assert_eq!(entry.t, 0);
        assert_eq!(entry.index, None);
    }

    #[test]
    fn reset_head_to_unborn_removes_the_ref_entry() {
        let mut state = NameServiceState::new();
        create_ledger_with_genesis(&mut state, "test/db");
        apply(
            &mut state,
            Command::ResetHead {
                ledger_id: "test/db".into(),
                branch: "main".into(),
                snapshot: ResetHeadSnapshot {
                    commit_head_id: None,
                    commit_t: 0,
                    index_head_id: None,
                    index_t: 0,
                },
            },
            2,
        );
        assert!(!state.refs.contains_key(&RefKey::new("test/db", "main")));
        // The LedgerRecord still has the branch registered, so it's
        // considered unborn — lookup still surfaces it.
        let ledger = state.ledgers.get("test/db").unwrap();
        assert!(ledger.branches.contains(&"main".to_string()));
    }

    #[test]
    fn reset_head_returns_not_found_for_unknown_branch() {
        let mut state = NameServiceState::new();
        let resp = apply(
            &mut state,
            Command::ResetHead {
                ledger_id: "missing".into(),
                branch: "main".into(),
                snapshot: ResetHeadSnapshot {
                    commit_head_id: Some(cid(0)),
                    commit_t: 0,
                    index_head_id: None,
                    index_t: 0,
                },
            },
            1,
        );
        assert_eq!(
            resp,
            Response::LedgerNotFound {
                ledger_id: "missing:main".into()
            }
        );
    }

    #[test]
    fn release_content_is_a_noop_at_state_machine_level() {
        let mut state = NameServiceState::new();
        let resp = apply(&mut state, Command::ReleaseContent { id: cid(0) }, 1);
        assert_eq!(resp, Response::NoOp);
        assert!(state.refs.is_empty());
        assert!(state.ledgers.is_empty());
    }

    #[test]
    fn snapshot_round_trip_preserves_state() {
        let mut state = NameServiceState::new();
        create_ledger_with_genesis(&mut state, "test/db");
        apply(
            &mut state,
            advance("test/db", "main", Some(cid(0)), cid(1), 1, None),
            2,
        );
        apply(
            &mut state,
            advance(
                "test/db",
                "feature",
                None,
                cid(2),
                5,
                Some(ctx("test/db", "k1", [7u8; 32])),
            ),
            3,
        );
        apply(
            &mut state,
            Command::RetractLedger {
                ledger_id: "test/db".into(),
                branch: "feature".into(),
            },
            4,
        );

        let bytes = state.to_snapshot().unwrap();
        let restored = NameServiceState::from_snapshot(&bytes).unwrap();
        assert_eq!(state, restored);
    }

    #[test]
    fn apply_then_snapshot_then_restore_then_apply_continues_correctly() {
        let mut state = NameServiceState::new();
        create_ledger_with_genesis(&mut state, "test/db");

        let bytes = state.to_snapshot().unwrap();
        let mut restored = NameServiceState::from_snapshot(&bytes).unwrap();

        let resp = apply(
            &mut restored,
            advance("test/db", "main", Some(cid(0)), cid(1), 1, None),
            2,
        );
        assert_eq!(
            resp,
            Response::Applied {
                head_t: 1,
                head_id: cid(1),
                accepted: 1,
                release: vec![],
                tally: None,
            }
        );
    }

    #[test]
    fn release_propagates_to_applied_response() {
        let mut state = NameServiceState::new();
        create_ledger_with_genesis(&mut state, "test/db");

        let releases = vec![
            ExecutionRecordRef::new(
                IdempotencyCacheKey::new("test/db", IdempotencyKey::new("k_old1")),
                [1u8; 32],
            ),
            ExecutionRecordRef::new(
                IdempotencyCacheKey::new("test/db", IdempotencyKey::new("k_old2")),
                [2u8; 32],
            ),
        ];

        let resp = apply(
            &mut state,
            advance_with_release(
                "test/db",
                "main",
                Some(cid(0)),
                cid(1),
                1,
                None,
                releases.clone(),
            ),
            2,
        );

        assert_eq!(
            resp,
            Response::Applied {
                head_t: 1,
                head_id: cid(1),
                accepted: 1,
                release: releases,
                tally: None,
            }
        );
    }

    #[test]
    fn release_propagates_on_idempotency_hit() {
        let mut state = NameServiceState::new();
        create_ledger_with_genesis(&mut state, "test/db");

        let body_hash = [7u8; 32];
        // First apply seeds the idempotency cache.
        apply(
            &mut state,
            advance(
                "test/db",
                "main",
                Some(cid(0)),
                cid(1),
                1,
                Some(ctx("test/db", "k1", body_hash)),
            ),
            2,
        );

        // Second apply with the same key + body hits the cache. The
        // release list it carries should still flow through to the
        // response so the wrapper performs the cleanup.
        let releases = vec![ExecutionRecordRef::new(
            IdempotencyCacheKey::new("test/db", IdempotencyKey::new("k_old")),
            [42u8; 32],
        )];

        let resp = apply(
            &mut state,
            advance_with_release(
                "test/db",
                "main",
                Some(cid(1)),
                cid(2),
                2,
                Some(ctx("test/db", "k1", body_hash)),
                releases.clone(),
            ),
            3,
        );

        assert_eq!(
            resp,
            Response::Applied {
                head_t: 1,
                head_id: cid(1),
                accepted: 0,
                release: releases,
                tally: None,
            }
        );
    }

    #[test]
    fn release_is_dropped_on_cas_conflict() {
        let mut state = NameServiceState::new();
        create_ledger_with_genesis(&mut state, "test/db");

        let releases = vec![ExecutionRecordRef::new(
            IdempotencyCacheKey::new("test/db", IdempotencyKey::new("k_old")),
            [99u8; 32],
        )];

        // Wrong expected_prev → Conflict, which has no release
        // field. The leader's buffer retains the releases and retries
        // them on the next successful proposal.
        let resp = apply(
            &mut state,
            advance_with_release("test/db", "main", Some(cid(99)), cid(1), 1, None, releases),
            2,
        );

        assert!(matches!(resp, Response::Conflict { .. }));
    }

    // -------------------------------------------------------------
    // AdvanceIndexHead — apply path
    // -------------------------------------------------------------

    fn advance_index(ledger_id: &str, branch: &str, head: ContentId, t: i64) -> Command {
        Command::AdvanceIndexHead(AdvanceIndexHeadArgs {
            ledger_id: ledger_id.into(),
            branch: branch.into(),
            new_index_head: head,
            t,
            applied_at_millis: 3_000,
        })
    }

    /// Set up a ledger with `main` at commit_t=10 — the baseline for
    /// the index tests below.
    fn ledger_with_commit_at_t10() -> NameServiceState {
        let mut state = NameServiceState::new();
        create_ledger_with_genesis(&mut state, "test/db");
        apply(
            &mut state,
            advance("test/db", "main", Some(cid(0)), cid(1), 10, None),
            2,
        );
        state
    }

    fn ref_entry<'a>(state: &'a NameServiceState, ledger: &str, branch: &str) -> &'a RefEntry {
        state
            .refs
            .get(&RefKey::new(ledger, branch))
            .expect("ref present")
    }

    #[test]
    fn advance_index_head_writes_into_existing_ref() {
        let mut state = ledger_with_commit_at_t10();
        let resp = apply(&mut state, advance_index("test/db", "main", cid(42), 10), 3);

        assert_eq!(
            resp,
            Response::IndexAdvanced {
                index_t: 10,
                index_head: cid(42),
            }
        );
        let entry = ref_entry(&state, "test/db", "main");
        assert_eq!(
            entry.index,
            Some(IndexState {
                head: cid(42),
                t: 10
            })
        );
        // Commit head untouched.
        assert_eq!(entry.head, cid(1));
        assert_eq!(entry.t, 10);
    }

    #[test]
    fn advance_index_head_is_strictly_monotonic() {
        let mut state = ledger_with_commit_at_t10();
        apply(&mut state, advance_index("test/db", "main", cid(42), 10), 3);

        // Re-proposing the same t is stale (not advanced again).
        let resp = apply(&mut state, advance_index("test/db", "main", cid(43), 10), 4);
        assert_eq!(resp, Response::IndexStale { current_t: 10 });

        // Lower t is also stale.
        let resp = apply(&mut state, advance_index("test/db", "main", cid(44), 5), 5);
        assert_eq!(resp, Response::IndexStale { current_t: 10 });

        // State unchanged after the failed advances.
        let entry = ref_entry(&state, "test/db", "main");
        assert_eq!(
            entry.index,
            Some(IndexState {
                head: cid(42),
                t: 10
            })
        );
    }

    #[test]
    fn advance_index_head_rejects_index_t_beyond_commit_t() {
        let mut state = ledger_with_commit_at_t10();
        // Branch is at commit_t=10; proposing index over t=15 means
        // the indexer's claim to have indexed commits 11..=15 has
        // no commits to back it on this node's state.
        let resp = apply(&mut state, advance_index("test/db", "main", cid(99), 15), 3);
        assert_eq!(
            resp,
            Response::IndexAhead {
                commit_t: 10,
                proposed_t: 15,
            }
        );
        // No write happened.
        let entry = ref_entry(&state, "test/db", "main");
        assert_eq!(entry.index, None);
    }

    #[test]
    fn advance_index_head_rejects_when_ledger_missing() {
        let mut state = NameServiceState::new();
        let resp = apply(&mut state, advance_index("nope/db", "main", cid(7), 1), 1);
        assert_eq!(
            resp,
            Response::LedgerNotFound {
                ledger_id: "nope/db".into()
            }
        );
    }

    #[test]
    fn advance_index_head_rejects_when_branch_has_no_ref() {
        // Ledger exists (its `main` ref was created), but `dev` has
        // never been touched — there's nothing to index there.
        let mut state = ledger_with_commit_at_t10();
        let resp = apply(&mut state, advance_index("test/db", "dev", cid(7), 1), 3);
        assert_eq!(
            resp,
            Response::LedgerNotFound {
                ledger_id: "test/db".into()
            }
        );
    }

    #[test]
    fn advance_ref_carries_index_head_forward_across_commits() {
        // Publish an index at t=10, then advance commit to t=20. The
        // index head should travel with the ref entry: the next
        // commit doesn't index itself, but it shouldn't lose the
        // pointer to the latest index either.
        let mut state = ledger_with_commit_at_t10();
        apply(&mut state, advance_index("test/db", "main", cid(42), 10), 3);
        apply(
            &mut state,
            advance("test/db", "main", Some(cid(1)), cid(2), 20, None),
            4,
        );

        let entry = ref_entry(&state, "test/db", "main");
        assert_eq!(entry.head, cid(2));
        assert_eq!(entry.t, 20);
        // Index still points at the t=10 root.
        assert_eq!(
            entry.index,
            Some(IndexState {
                head: cid(42),
                t: 10
            })
        );
    }

    // ====================================================================
    // EnqueueCommand
    // ====================================================================

    fn enqueue(
        ledger_id: &str,
        branch: &str,
        body_seed: u8,
        idempotency: Option<IdempotencyContext>,
    ) -> Command {
        Command::EnqueueCommand(EnqueueCommandArgs {
            ledger_id: ledger_id.into(),
            branch: branch.into(),
            idempotency: idempotency.map(|c| c.key),
            body_cid: cid(body_seed),
            body_kind: BodyKind::JsonLdInsert,
            applied_at_millis: 1_000,
        })
    }

    fn body_kid(key: &str) -> IdempotencyCacheKey {
        IdempotencyCacheKey::new("test/db:main", IdempotencyKey::new(key))
    }

    #[test]
    fn enqueue_appends_entry_and_returns_queue_id() {
        let mut state = NameServiceState::new();
        let resp = apply(&mut state, enqueue("test/db", "main", 7, None), 1);
        let queue_id = match resp {
            Response::Enqueued { ledger_id, queue_id } => {
                assert_eq!(ledger_id, "test/db:main");
                queue_id
            }
            other => panic!("expected Enqueued, got {other:?}"),
        };
        let key = RefKey::new("test/db", "main");
        assert_eq!(state.queues.get(&key).unwrap().len(), 1);
        let front = state.queues.get(&key).unwrap().front().unwrap();
        assert_eq!(front.queue_id, queue_id);
        assert_eq!(front.body_cid, cid(7));
        assert_eq!(state.next_queue_id, queue_id + 1);
    }

    #[test]
    fn enqueue_idempotency_hit_short_circuits_on_cached_success() {
        let mut state = NameServiceState::new();
        let key = body_kid("k1");
        // Pre-populate a cached success record with body_cid set.
        state.idempotency.insert(
            key.clone(),
            ApplyOutcome::Applied(ApplyRecord {
                body_hash: [0u8; 32],
                body_cid: Some(cid(7)),
                recorded_at_millis: 500,
                head: cid(42),
                t: 5,
                recorded_index: 9,
                tally: None,
            }),
        );
        let resp = apply(
            &mut state,
            enqueue(
                "test/db",
                "main",
                7,
                Some(IdempotencyContext {
                    key: key.clone(),
                    body_hash: [0u8; 32],
                }),
            ),
            10,
        );
        match resp {
            Response::IdempotencyHit { record } => {
                assert_eq!(record.head, cid(42));
                assert_eq!(record.t, 5);
            }
            other => panic!("expected IdempotencyHit, got {other:?}"),
        }
        // Nothing appended.
        assert!(state.queues.is_empty());
    }

    #[test]
    fn enqueue_body_hash_mismatch_on_same_key_different_body() {
        let mut state = NameServiceState::new();
        let key = body_kid("k1");
        state.idempotency.insert(
            key.clone(),
            ApplyOutcome::Applied(ApplyRecord {
                body_hash: [0u8; 32],
                body_cid: Some(cid(7)),
                recorded_at_millis: 500,
                head: cid(42),
                t: 5,
                recorded_index: 9,
                tally: None,
            }),
        );
        let resp = apply(
            &mut state,
            enqueue(
                "test/db",
                "main",
                8, // different body
                Some(IdempotencyContext {
                    key,
                    body_hash: [0u8; 32],
                }),
            ),
            10,
        );
        assert_eq!(resp, Response::BodyHashMismatch);
    }

    #[test]
    fn enqueue_in_flight_when_key_already_queued() {
        let mut state = NameServiceState::new();
        let key = body_kid("k1");
        let first = apply(
            &mut state,
            enqueue(
                "test/db",
                "main",
                7,
                Some(IdempotencyContext {
                    key: key.clone(),
                    body_hash: [0u8; 32],
                }),
            ),
            1,
        );
        let queue_id = match first {
            Response::Enqueued { queue_id, .. } => queue_id,
            other => panic!("first enqueue not Enqueued: {other:?}"),
        };
        let second = apply(
            &mut state,
            enqueue(
                "test/db",
                "main",
                7,
                Some(IdempotencyContext {
                    key,
                    body_hash: [0u8; 32],
                }),
            ),
            2,
        );
        match second {
            Response::InFlight {
                queue_id: q,
                ledger_id,
            } => {
                assert_eq!(q, queue_id);
                assert_eq!(ledger_id, "test/db:main");
            }
            other => panic!("expected InFlight, got {other:?}"),
        }
        // Still only one entry in the queue.
        let key = RefKey::new("test/db", "main");
        assert_eq!(state.queues.get(&key).unwrap().len(), 1);
    }

    #[test]
    fn enqueue_returns_queue_full_per_branch_when_cap_reached() {
        let mut state = NameServiceState::new();
        state.queue_config = QueueConfig {
            per_branch_cap: 2,
            global_cap: 100,
        };
        apply(&mut state, enqueue("test/db", "main", 1, None), 1);
        apply(&mut state, enqueue("test/db", "main", 2, None), 2);
        let resp = apply(&mut state, enqueue("test/db", "main", 3, None), 3);
        match resp {
            Response::QueueFull {
                cap,
                scope: QueueFullScope::PerBranch,
                ..
            } => assert_eq!(cap, 2),
            other => panic!("expected per-branch QueueFull, got {other:?}"),
        }
    }

    #[test]
    fn enqueue_returns_queue_full_global_when_summed_cap_reached() {
        let mut state = NameServiceState::new();
        state.queue_config = QueueConfig {
            per_branch_cap: 10,
            global_cap: 2,
        };
        apply(&mut state, enqueue("a/db", "main", 1, None), 1);
        apply(&mut state, enqueue("b/db", "main", 2, None), 2);
        let resp = apply(&mut state, enqueue("c/db", "main", 3, None), 3);
        match resp {
            Response::QueueFull {
                scope: QueueFullScope::Global,
                ..
            } => {}
            other => panic!("expected global QueueFull, got {other:?}"),
        }
    }

    // ====================================================================
    // ApplyHead
    // ====================================================================

    fn apply_head_cmd(ledger_id: &str, branch: &str, queue_id: u64, commit: ContentId, t: i64) -> Command {
        Command::ApplyHead(ApplyHeadArgs {
            ledger_id: ledger_id.into(),
            branch: branch.into(),
            queue_id,
            commit_id: commit,
            commit_t: t,
            applied_at_millis: 2_000,
        })
    }

    #[test]
    fn apply_head_advances_ref_and_caches_idempotency() {
        let mut state = NameServiceState::new();
        apply(&mut state, create_ledger("test/db"), 1);
        let key = body_kid("k1");
        let enq = apply(
            &mut state,
            enqueue(
                "test/db",
                "main",
                7,
                Some(IdempotencyContext {
                    key: key.clone(),
                    body_hash: [0u8; 32],
                }),
            ),
            2,
        );
        let queue_id = match enq {
            Response::Enqueued { queue_id, .. } => queue_id,
            other => panic!("not Enqueued: {other:?}"),
        };

        let resp = apply(
            &mut state,
            apply_head_cmd("test/db", "main", queue_id, cid(42), 10),
            3,
        );
        match resp {
            Response::HeadApplied {
                ledger_id,
                commit_id,
                commit_t,
            } => {
                assert_eq!(ledger_id, "test/db:main");
                assert_eq!(commit_id, cid(42));
                assert_eq!(commit_t, 10);
            }
            other => panic!("expected HeadApplied, got {other:?}"),
        }

        // Queue front popped.
        let ref_key = RefKey::new("test/db", "main");
        assert!(state.queues.get(&ref_key).map(VecDeque::is_empty).unwrap_or(true));

        // RefEntry advanced.
        let entry = state.refs.get(&ref_key).expect("ref present");
        assert_eq!(entry.head, cid(42));
        assert_eq!(entry.t, 10);
        assert_eq!(entry.last_advanced_index, 3);

        // Self-healing branch registration.
        let ledger = state.ledgers.get("test/db").unwrap();
        assert!(ledger.branches.contains(&"main".to_string()));

        // Idempotency cached with body_cid set (the new queue path).
        let record = match state.idempotency.get(&key).expect("idempotency cached") {
            ApplyOutcome::Applied(r) => r,
            ApplyOutcome::Failed(_) => panic!("expected Applied outcome"),
        };
        assert_eq!(record.body_cid.as_ref(), Some(&cid(7)));
        assert_eq!(record.head, cid(42));
    }

    #[test]
    fn apply_head_carries_index_forward_across_queue_commits() {
        let mut state = NameServiceState::new();
        create_ledger_with_genesis(&mut state, "test/db");
        // Establish an index head via the existing advance_index path.
        apply(&mut state, advance_index("test/db", "main", cid(42), 0), 2);

        // Enqueue + apply via the queue path. The index should
        // carry forward to the new RefEntry.
        let enq = apply(&mut state, enqueue("test/db", "main", 7, None), 3);
        let qid = match enq {
            Response::Enqueued { queue_id, .. } => queue_id,
            other => panic!("not Enqueued: {other:?}"),
        };
        apply(
            &mut state,
            apply_head_cmd("test/db", "main", qid, cid(99), 20),
            4,
        );

        let entry = ref_entry(&state, "test/db", "main");
        assert_eq!(entry.head, cid(99));
        assert_eq!(entry.t, 20);
        assert_eq!(
            entry.index,
            Some(IndexState {
                head: cid(42),
                t: 0,
            })
        );
    }

    #[test]
    fn apply_head_wrong_front_when_queue_id_mismatches() {
        let mut state = NameServiceState::new();
        apply(&mut state, create_ledger("test/db"), 1);
        apply(&mut state, enqueue("test/db", "main", 7, None), 2);

        // Worker proposes ApplyHead with the wrong queue_id (off by one).
        let resp = apply(
            &mut state,
            apply_head_cmd("test/db", "main", 9_999, cid(42), 10),
            3,
        );
        match resp {
            Response::QueueDesync {
                reason: DesyncReason::WrongFront { actual_queue_id },
                ..
            } => assert_eq!(actual_queue_id, 0),
            other => panic!("expected WrongFront, got {other:?}"),
        }
        // Front still in place.
        let ref_key = RefKey::new("test/db", "main");
        assert_eq!(state.queues.get(&ref_key).unwrap().len(), 1);
        // No RefEntry advance.
        assert!(!state.refs.contains_key(&ref_key));
    }

    #[test]
    fn apply_head_queue_cleared_when_admin_preempted() {
        let mut state = NameServiceState::new();
        apply(&mut state, create_ledger("test/db"), 1);
        apply(&mut state, enqueue("test/db", "main", 7, None), 2);

        // Simulate an admin command (e.g. DropBranch) clearing the
        // queue and recording a `recently_cleared` marker. This
        // mirrors what Task #74's admin-side change will do.
        let ref_key = RefKey::new("test/db", "main");
        state.queues.remove(&ref_key);
        state.recently_cleared
            .insert(ref_key.clone(), ClearReason::BranchDropped);

        let resp = apply(
            &mut state,
            apply_head_cmd("test/db", "main", 0, cid(42), 10),
            3,
        );
        match resp {
            Response::QueueDesync {
                reason: DesyncReason::QueueCleared {
                    reason: ClearReason::BranchDropped,
                },
                ..
            } => {}
            other => panic!("expected QueueCleared(BranchDropped), got {other:?}"),
        }
        // Marker is one-shot.
        assert!(!state.recently_cleared.contains_key(&ref_key));
    }

    #[test]
    fn apply_head_invariant_violated_when_queue_empty_without_marker() {
        let mut state = NameServiceState::new();
        apply(&mut state, create_ledger("test/db"), 1);
        // No EnqueueCommand, no recently_cleared marker — should
        // never happen in real flow.
        let resp = apply(
            &mut state,
            apply_head_cmd("test/db", "main", 0, cid(42), 10),
            2,
        );
        assert!(matches!(
            resp,
            Response::QueueDesync {
                reason: DesyncReason::InvariantViolated { .. },
                ..
            }
        ));
    }

    // ====================================================================
    // PoisonQueueEntry
    // ====================================================================

    fn poison_cmd(
        ledger_id: &str,
        branch: &str,
        queue_id: u64,
        reason: PoisonReason,
    ) -> Command {
        Command::PoisonQueueEntry(PoisonQueueEntryArgs {
            ledger_id: ledger_id.into(),
            branch: branch.into(),
            queue_id,
            reason,
            applied_at_millis: 2_000,
        })
    }

    fn body_malformed(msg: &str) -> PoisonReason {
        PoisonReason::BodyMalformed { error: msg.into() }
    }

    #[test]
    fn apply_poison_pops_front_and_caches_failure() {
        let mut state = NameServiceState::new();
        apply(&mut state, create_ledger("test/db"), 1);
        let key = body_kid("k1");
        let enq = apply(
            &mut state,
            enqueue(
                "test/db",
                "main",
                7,
                Some(IdempotencyContext {
                    key: key.clone(),
                    body_hash: [0u8; 32],
                }),
            ),
            2,
        );
        let queue_id = match enq {
            Response::Enqueued { queue_id, .. } => queue_id,
            other => panic!("not Enqueued: {other:?}"),
        };

        let resp = apply(
            &mut state,
            poison_cmd("test/db", "main", queue_id, body_malformed("bad turtle")),
            3,
        );
        match resp {
            Response::Poisoned {
                ledger_id,
                queue_id: qid,
                reason: PoisonReason::BodyMalformed { error },
            } => {
                assert_eq!(ledger_id, "test/db:main");
                assert_eq!(qid, queue_id);
                assert_eq!(error, "bad turtle");
            }
            other => panic!("expected Poisoned, got {other:?}"),
        }

        let ref_key = RefKey::new("test/db", "main");
        assert!(state.queues.get(&ref_key).map(VecDeque::is_empty).unwrap_or(true));
        // Ref untouched — no head advance on poison.
        assert!(!state.refs.contains_key(&ref_key));

        let record = match state.idempotency.get(&key).expect("idempotency cached") {
            ApplyOutcome::Failed(r) => r,
            ApplyOutcome::Applied(_) => panic!("expected Failed outcome"),
        };
        assert_eq!(record.body_cid, cid(7));
        assert_eq!(record.recorded_index, 3);
        assert!(matches!(
            record.reason,
            PoisonReason::BodyMalformed { .. }
        ));
    }

    #[test]
    fn apply_poison_without_idempotency_still_pops_front() {
        let mut state = NameServiceState::new();
        apply(&mut state, create_ledger("test/db"), 1);
        let enq = apply(&mut state, enqueue("test/db", "main", 7, None), 2);
        let queue_id = match enq {
            Response::Enqueued { queue_id, .. } => queue_id,
            other => panic!("not Enqueued: {other:?}"),
        };

        apply(
            &mut state,
            poison_cmd("test/db", "main", queue_id, body_malformed("nope")),
            3,
        );

        let ref_key = RefKey::new("test/db", "main");
        assert!(state.queues.get(&ref_key).map(VecDeque::is_empty).unwrap_or(true));
        // No idempotency key means nothing recorded — the cache stays empty.
        assert!(state.idempotency.is_empty());
    }

    #[test]
    fn apply_poison_wrong_front_when_queue_id_mismatches() {
        let mut state = NameServiceState::new();
        apply(&mut state, create_ledger("test/db"), 1);
        apply(&mut state, enqueue("test/db", "main", 7, None), 2);

        let resp = apply(
            &mut state,
            poison_cmd("test/db", "main", 9_999, body_malformed("x")),
            3,
        );
        match resp {
            Response::QueueDesync {
                reason: DesyncReason::WrongFront { actual_queue_id },
                ..
            } => assert_eq!(actual_queue_id, 0),
            other => panic!("expected WrongFront, got {other:?}"),
        }
        let ref_key = RefKey::new("test/db", "main");
        assert_eq!(state.queues.get(&ref_key).unwrap().len(), 1);
        assert!(state.idempotency.is_empty());
    }

    #[test]
    fn apply_poison_queue_cleared_when_admin_preempted() {
        let mut state = NameServiceState::new();
        apply(&mut state, create_ledger("test/db"), 1);
        apply(&mut state, enqueue("test/db", "main", 7, None), 2);

        let ref_key = RefKey::new("test/db", "main");
        state.queues.remove(&ref_key);
        state
            .recently_cleared
            .insert(ref_key.clone(), ClearReason::BranchPurged);

        let resp = apply(
            &mut state,
            poison_cmd("test/db", "main", 0, body_malformed("x")),
            3,
        );
        match resp {
            Response::QueueDesync {
                reason: DesyncReason::QueueCleared {
                    reason: ClearReason::BranchPurged,
                },
                ..
            } => {}
            other => panic!("expected QueueCleared(BranchPurged), got {other:?}"),
        }
        assert!(!state.recently_cleared.contains_key(&ref_key));
    }

    #[test]
    fn apply_poison_invariant_violated_when_queue_empty_without_marker() {
        let mut state = NameServiceState::new();
        apply(&mut state, create_ledger("test/db"), 1);
        let resp = apply(
            &mut state,
            poison_cmd("test/db", "main", 0, body_malformed("x")),
            2,
        );
        assert!(matches!(
            resp,
            Response::QueueDesync {
                reason: DesyncReason::InvariantViolated { .. },
                ..
            }
        ));
    }

    // ====================================================================
    // EvictIdempotency
    // ====================================================================

    fn applied_outcome(
        body_cid: Option<ContentId>,
        recorded_at_millis: u64,
        recorded_index: u64,
    ) -> ApplyOutcome {
        ApplyOutcome::Applied(ApplyRecord {
            body_hash: [0u8; 32],
            body_cid,
            recorded_at_millis,
            head: cid(99),
            t: 1,
            recorded_index,
            tally: None,
        })
    }

    fn failed_outcome(
        body_cid: ContentId,
        recorded_at_millis: u64,
        recorded_index: u64,
    ) -> ApplyOutcome {
        ApplyOutcome::Failed(PoisonRecord {
            body_cid,
            reason: PoisonReason::BodyMalformed {
                error: "test".into(),
            },
            recorded_index,
            recorded_at_millis,
        })
    }

    fn evict(cutoff_millis: u64) -> Command {
        Command::EvictIdempotency { cutoff_millis }
    }

    fn install_outcome(state: &mut NameServiceState, key: &str, outcome: ApplyOutcome) {
        state
            .idempotency
            .insert(body_kid(key), outcome);
    }

    #[test]
    fn evict_removes_entries_below_cutoff_and_returns_released_cids() {
        let mut state = NameServiceState::new();
        install_outcome(&mut state, "old_applied", applied_outcome(Some(cid(1)), 100, 1));
        install_outcome(&mut state, "old_failed", failed_outcome(cid(2), 150, 2));
        install_outcome(&mut state, "fresh_applied", applied_outcome(Some(cid(3)), 500, 3));

        let resp = apply(&mut state, evict(200), 4);

        match resp {
            Response::EvictionApplied {
                removed,
                released_body_cids,
            } => {
                assert_eq!(removed, 2);
                let cids: HashSet<_> = released_body_cids.into_iter().collect();
                assert_eq!(cids, HashSet::from([cid(1), cid(2)]));
            }
            other => panic!("expected EvictionApplied, got {other:?}"),
        }

        // Fresh entry survives.
        assert!(state.idempotency.contains_key(&body_kid("fresh_applied")));
        assert!(!state.idempotency.contains_key(&body_kid("old_applied")));
        assert!(!state.idempotency.contains_key(&body_kid("old_failed")));
        assert_eq!(state.evicted_idempotency_count, 2);
    }

    #[test]
    fn evict_excludes_entries_at_or_above_cutoff() {
        // The cutoff is strict: entries with `recorded_at_millis == cutoff`
        // are still considered fresh.
        let mut state = NameServiceState::new();
        install_outcome(&mut state, "boundary", applied_outcome(Some(cid(1)), 200, 1));

        let resp = apply(&mut state, evict(200), 2);
        match resp {
            Response::EvictionApplied { removed, .. } => assert_eq!(removed, 0),
            other => panic!("expected EvictionApplied, got {other:?}"),
        }
        assert!(state.idempotency.contains_key(&body_kid("boundary")));
    }

    #[test]
    fn evict_skips_applied_entries_with_no_body_cid() {
        // Legacy AdvanceRef applies set `body_cid: None`. Eviction
        // still removes the entry but the released list stays empty.
        let mut state = NameServiceState::new();
        install_outcome(&mut state, "legacy", applied_outcome(None, 100, 1));

        let resp = apply(&mut state, evict(200), 2);
        match resp {
            Response::EvictionApplied {
                removed,
                released_body_cids,
            } => {
                assert_eq!(removed, 1);
                assert!(released_body_cids.is_empty());
            }
            other => panic!("expected EvictionApplied, got {other:?}"),
        }
    }

    #[test]
    fn evict_caps_at_batch_size_and_takes_oldest_first() {
        let mut state = NameServiceState::new();
        // Insert EVICT_BATCH_SIZE + 50 expired entries. Use ascending
        // recorded_at_millis so we can verify the oldest 1024 leave
        // and the youngest 50 stay.
        let total = EVICT_BATCH_SIZE + 50;
        for i in 0..total {
            let key = format!("k_{i}");
            install_outcome(
                &mut state,
                &key,
                applied_outcome(Some(cid(i as u8 % 200)), i as u64, i as u64),
            );
        }

        let resp = apply(&mut state, evict(u64::MAX), (total + 1) as u64);
        match resp {
            Response::EvictionApplied { removed, .. } => {
                assert_eq!(removed, EVICT_BATCH_SIZE);
            }
            other => panic!("expected EvictionApplied, got {other:?}"),
        }
        assert_eq!(state.idempotency.len(), 50);
        assert_eq!(state.evicted_idempotency_count, EVICT_BATCH_SIZE as u64);

        // The 50 survivors should be the *youngest* (largest
        // recorded_at_millis) — oldest-first ordering.
        for i in EVICT_BATCH_SIZE..total {
            assert!(
                state.idempotency.contains_key(&body_kid(&format!("k_{i}"))),
                "expected k_{i} to survive"
            );
        }
    }

    #[test]
    fn evict_empty_cache_is_noop() {
        let mut state = NameServiceState::new();
        let resp = apply(&mut state, evict(1_000), 1);
        match resp {
            Response::EvictionApplied {
                removed,
                released_body_cids,
            } => {
                assert_eq!(removed, 0);
                assert!(released_body_cids.is_empty());
            }
            other => panic!("expected EvictionApplied, got {other:?}"),
        }
        assert_eq!(state.evicted_idempotency_count, 0);
    }
}
