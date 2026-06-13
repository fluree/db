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
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
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
}

/// Lifecycle record for one ledger.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LedgerRecord {
    /// Content id of the governance config blob. Treated opaquely
    /// here; the config itself lives in the content store.
    pub governance: ContentId,
    /// Leader-supplied wall-clock at creation, milliseconds since the
    /// Unix epoch. Metadata only.
    pub created_at_millis: u64,
    /// Log index at which the ledger was created.
    pub created_index: u64,
    /// Branches known on this ledger. Mirrors the ledger's keyspace
    /// in [`NameServiceState::refs`] so a ledger lookup can enumerate
    /// branches without scanning the refs map.
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
    /// than silently dedup.
    pub body_hash: [u8; 32],
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

/// State machine state. Serializable as a single blob for
/// snapshotting (see [`NameServiceState::to_snapshot`]).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct NameServiceState {
    pub refs: HashMap<RefKey, RefEntry>,
    pub ledgers: HashMap<String, LedgerRecord>,
    pub idempotency: HashMap<IdempotencyCacheKey, ApplyRecord>,
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
    /// Register a new ledger and set its initial branch head in one
    /// atomic step.
    CreateLedger(CreateLedgerArgs),
    /// Remove a ledger and all its refs.
    DeleteLedger { ledger_id: String },
    /// Signal that the named content blob is no longer referenced
    /// and may be released by the content store. The state machine
    /// doesn't mutate state on this — the entry's role is to let
    /// every node's content store act in sync.
    ReleaseContent { id: ContentId },
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateLedgerArgs {
    pub ledger_id: String,
    pub initial_branch: String,
    pub initial_head: ContentId,
    pub initial_t: i64,
    pub governance: ContentId,
    pub created_at_millis: u64,
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
    /// [`Command::CreateLedger`] succeeded.
    Created { ledger_id: String },
    /// [`Command::DeleteLedger`] succeeded.
    Deleted { ledger_id: String },
    /// [`Command::CreateLedger`] failed because the ledger already
    /// exists.
    AlreadyExists { ledger_id: String },
    /// [`Command::AdvanceRef`] or [`Command::DeleteLedger`] referenced
    /// a ledger that doesn't exist in the state machine.
    LedgerNotFound { ledger_id: String },
    /// [`Command::AdvanceRef`] carried an idempotency key already
    /// recorded for a different body. A client bug; surfaces rather
    /// than silently dedup.
    BodyHashMismatch,
    /// Command was understood but no state change resulted (e.g.,
    /// [`Command::ReleaseContent`]).
    NoOp,
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
        Command::DeleteLedger { ledger_id } => delete_ledger(state, ledger_id),
        Command::ReleaseContent { id: _ } => Response::NoOp,
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
    if let Some(ctx) = &idempotency {
        if let Some(existing) = state.idempotency.get(&ctx.key) {
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

    // Carry the existing index forward across commit advances — the
    // new commit doesn't index itself; that happens later via
    // `AdvanceIndexHead`.
    let prior_index = state.refs.get(&ref_key).and_then(|r| r.index.clone());
    state.refs.insert(
        ref_key,
        RefEntry {
            head: new_head.clone(),
            t,
            last_advanced_at_millis: applied_at_millis,
            last_advanced_index: log_index,
            index: prior_index,
        },
    );

    if let Some(ctx) = idempotency {
        state.idempotency.insert(
            ctx.key,
            ApplyRecord {
                body_hash: ctx.body_hash,
                head: new_head.clone(),
                t,
                recorded_index: log_index,
                tally: tally.clone(),
            },
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
        initial_branch,
        initial_head,
        initial_t,
        governance,
        created_at_millis,
    } = args;

    if state.ledgers.contains_key(&ledger_id) {
        return Response::AlreadyExists { ledger_id };
    }

    state.ledgers.insert(
        ledger_id.clone(),
        LedgerRecord {
            governance,
            created_at_millis,
            created_index: log_index,
            branches: vec![initial_branch.clone()],
        },
    );

    state.refs.insert(
        RefKey::new(&ledger_id, &initial_branch),
        RefEntry {
            head: initial_head,
            t: initial_t,
            last_advanced_at_millis: created_at_millis,
            last_advanced_index: log_index,
            // No index built yet; the first `AdvanceIndexHead` for
            // this branch will populate this.
            index: None,
        },
    );

    Response::Created { ledger_id }
}

fn delete_ledger(state: &mut NameServiceState, ledger_id: String) -> Response {
    if state.ledgers.remove(&ledger_id).is_none() {
        return Response::LedgerNotFound { ledger_id };
    }
    state.refs.retain(|key, _| key.ledger_id != ledger_id);
    Response::Deleted { ledger_id }
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
        Command::CreateLedger(CreateLedgerArgs {
            ledger_id: ledger_id.into(),
            initial_branch: "main".into(),
            initial_head: cid(0),
            initial_t: 0,
            governance: cid(0xAA),
            created_at_millis: 1_000,
        })
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
    fn create_ledger_registers_record_and_initial_ref() {
        let mut state = NameServiceState::new();
        let resp = apply(&mut state, create_ledger("test/db"), 1);
        assert_eq!(
            resp,
            Response::Created {
                ledger_id: "test/db".into()
            }
        );
        assert_eq!(state.ledgers.len(), 1);
        assert_eq!(state.refs.len(), 1);
        let ref_entry = state
            .refs
            .get(&RefKey::new("test/db", "main"))
            .expect("initial ref present");
        assert_eq!(ref_entry.t, 0);
        assert_eq!(ref_entry.last_advanced_index, 1);
    }

    #[test]
    fn create_ledger_idempotent_on_duplicate() {
        let mut state = NameServiceState::new();
        apply(&mut state, create_ledger("test/db"), 1);
        let resp = apply(&mut state, create_ledger("test/db"), 2);
        assert_eq!(
            resp,
            Response::AlreadyExists {
                ledger_id: "test/db".into()
            }
        );
        assert_eq!(state.ledgers.len(), 1);
    }

    #[test]
    fn advance_ref_succeeds_when_expected_prev_matches() {
        let mut state = NameServiceState::new();
        apply(&mut state, create_ledger("test/db"), 1);
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
        apply(&mut state, create_ledger("test/db"), 1);
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
        apply(&mut state, create_ledger("test/db"), 1);

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
        apply(&mut state, create_ledger("test/db"), 1);

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
    fn delete_ledger_removes_record_and_all_refs() {
        let mut state = NameServiceState::new();
        apply(&mut state, create_ledger("test/db"), 1);
        apply(
            &mut state,
            advance("test/db", "feature", None, cid(2), 5, None),
            2,
        );
        assert_eq!(state.refs.len(), 2);

        let resp = apply(
            &mut state,
            Command::DeleteLedger {
                ledger_id: "test/db".into(),
            },
            3,
        );
        assert_eq!(
            resp,
            Response::Deleted {
                ledger_id: "test/db".into()
            }
        );
        assert!(state.ledgers.is_empty());
        assert!(state.refs.is_empty());
    }

    #[test]
    fn delete_ledger_returns_not_found_for_missing() {
        let mut state = NameServiceState::new();
        let resp = apply(
            &mut state,
            Command::DeleteLedger {
                ledger_id: "missing".into(),
            },
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
        apply(&mut state, create_ledger("test/db"), 1);
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

        let bytes = state.to_snapshot().unwrap();
        let restored = NameServiceState::from_snapshot(&bytes).unwrap();
        assert_eq!(state, restored);
    }

    #[test]
    fn apply_then_snapshot_then_restore_then_apply_continues_correctly() {
        let mut state = NameServiceState::new();
        apply(&mut state, create_ledger("test/db"), 1);

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
        apply(&mut state, create_ledger("test/db"), 1);

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
        apply(&mut state, create_ledger("test/db"), 1);

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
        apply(&mut state, create_ledger("test/db"), 1);

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
        apply(&mut state, create_ledger("test/db"), 1);
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
}
