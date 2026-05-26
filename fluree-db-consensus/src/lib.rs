//! Transaction submission traits and consensus implementations for Fluree DB.
//!
//! This crate defines the abstraction by which transactions are submitted
//! and accepted into a ledger. Each implementation has its own trust model
//! and durability mechanism:
//!
//! - [`MonolithicConsensus`] — a single integrated unit handles every
//!   transaction; the local execution stream is the agreement. Used for
//!   development, testing, and deployments that do not need cross-node
//!   coordination.
//!
//! Future implementations (Raft for crash-fault tolerance, BFT for byzantine
//! tolerance) will live alongside, behind the same [`Submitter`] trait.
//!
//! Submission identity and status lookup are driven by optional
//! [`IdempotencyKey`]s. Callers who want idempotent retry or after-the-fact
//! status lookup generate a key (typically a ULID) and include it in their
//! [`TransactionRequest`]; submissions sharing a key collapse to a single
//! outcome. Callers who don't need those guarantees may omit the key.

pub mod monolithic;

pub use monolithic::{MonolithicConsensus, DEFAULT_IDEMPOTENCY_TTL};

use async_trait::async_trait;
use fluree_db_api::{
    CommitId, ConflictStrategy, GovernanceOptions, IndexingStatus, RevertSelection, TrackingOptions,
    TrackingTally,
};
use fluree_db_transact::{CommitOpts, CommitReceipt, TxnOpts};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::fmt;
use thiserror::Error;

/// Caller-provided identifier for a transaction submission.
///
/// Used both for idempotent retry (retries with the same key collapse to one
/// outcome) and for status lookup via [`SubmissionLookup`]. Callers typically
/// generate a ULID before submission so they can recover after a disconnect.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IdempotencyKey(String);

impl IdempotencyKey {
    pub fn new(key: impl Into<String>) -> Self {
        Self(key.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for IdempotencyKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for IdempotencyKey {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for IdempotencyKey {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

/// The transaction payload in its submitted form.
///
/// Each variant fixes both the parser path *and* the insert/upsert/update
/// semantics, so invalid combinations (e.g. Turtle-Update, TriG-Insert)
/// are unrepresentable at the type level. SPARQL UPDATE encodes its own
/// semantics in the query and so has no per-op variants.
pub enum TransactionBody {
    /// JSON-LD document staged as pure insert (no retractions).
    JsonLdInsert(JsonValue),
    /// JSON-LD document staged with upsert semantics
    /// (existing-value retraction per `(subject, predicate)`).
    JsonLdUpsert(JsonValue),
    /// JSON-LD document staged as an update (general retract + assert).
    JsonLdUpdate(JsonValue),
    /// Plain Turtle text (`text/turtle`) staged as pure insert.
    TurtleInsert(String),
    /// Plain Turtle text (`text/turtle`) staged with upsert semantics.
    TurtleUpsert(String),
    /// TriG text (`application/trig`) staged with upsert semantics —
    /// `GRAPH` blocks require the upsert path so no insert variant exists.
    TrigUpsert(String),
    /// SPARQL UPDATE query text; the lowered `Txn` carries its own
    /// insert/update semantics.
    Sparql(String),
}

/// Transaction submission payload.
///
/// Carries the transaction itself plus everything an implementation needs to
/// process it. Implementation-specific knobs (e.g., Raft propose timeout,
/// monolithic backpressure overrides) live on the implementation, not here.
///
/// `idempotency_key` is optional: callers who want idempotent retry or
/// after-the-fact status lookup provide one; callers who don't care can
/// omit it and forfeit those guarantees.
///
/// `tracking` enables fuel/time/policy accounting; when present, the
/// resulting [`TransactionReceipt`] carries a [`TrackingTally`].
///
/// `governance` carries the authentication / authorization / accounting
/// configuration for the transaction. The implementation builds the policy
/// context from it against the ledger state it executes against. Callers
/// assemble it from wherever the inputs live — a JSON-LD body's `opts`, or
/// request headers for SPARQL.
pub struct TransactionRequest {
    pub idempotency_key: Option<IdempotencyKey>,
    pub ledger_id: String,
    pub body: TransactionBody,
    pub txn_opts: TxnOpts,
    pub commit_opts: CommitOpts,
    pub tracking: Option<TrackingOptions>,
    pub governance: GovernanceOptions,
}

/// Receipt returned once a submission is durably accepted.
///
/// `idempotency_key` echoes whatever the caller provided in the request, or
/// `None` if the submission was anonymous. `tally` carries the fuel/time/
/// policy accounting when the request enabled tracking, `None` otherwise.
#[derive(Debug, Clone)]
pub struct TransactionReceipt {
    pub idempotency_key: Option<IdempotencyKey>,
    pub commit: CommitReceipt,
    pub tally: Option<TrackingTally>,
}

/// Revert submission payload.
///
/// Produces a single inverse commit on the given branch that retracts the
/// effects of the commits identified by `selection`. `strategy` controls
/// how conflicts between the reverted flakes and the branch head's current
/// facts are resolved.
pub struct RevertRequest {
    pub idempotency_key: Option<IdempotencyKey>,
    pub ledger_name: String,
    pub branch: String,
    pub selection: RevertSelection,
    pub strategy: ConflictStrategy,
}

/// Receipt returned once a revert is durably accepted.
#[derive(Debug, Clone)]
pub struct RevertReceipt {
    pub idempotency_key: Option<IdempotencyKey>,
    pub branch: String,
    pub reverted_commits: Vec<CommitId>,
    pub conflict_count: usize,
    pub strategy: ConflictStrategy,
    pub new_head_t: i64,
    pub new_head_id: CommitId,
}

/// Merge submission payload.
///
/// Replays `source_branch` onto `target_branch` (or the source's parent
/// branch if `target_branch` is `None`), producing either a fast-forward
/// update of the target's HEAD or a general merge commit. `strategy`
/// controls how conflicts between source and target flakes are resolved.
pub struct MergeRequest {
    pub idempotency_key: Option<IdempotencyKey>,
    pub ledger_name: String,
    pub source_branch: String,
    pub target_branch: Option<String>,
    pub strategy: ConflictStrategy,
}

/// Receipt returned once a merge is durably accepted.
#[derive(Debug, Clone)]
pub struct MergeReceipt {
    pub idempotency_key: Option<IdempotencyKey>,
    pub source: String,
    pub target: String,
    pub fast_forward: bool,
    pub new_head_t: i64,
    pub new_head_id: CommitId,
    pub commits_copied: usize,
    pub conflict_count: usize,
    pub strategy: ConflictStrategy,
}

/// Rebase submission payload.
///
/// Replays `branch`'s unique commits on top of its source branch's current
/// HEAD, resolving conflicts according to `strategy`.
pub struct RebaseRequest {
    pub idempotency_key: Option<IdempotencyKey>,
    pub ledger_name: String,
    pub branch: String,
    pub strategy: ConflictStrategy,
}

/// Receipt returned once a rebase is durably accepted.
#[derive(Debug, Clone)]
pub struct RebaseReceipt {
    pub idempotency_key: Option<IdempotencyKey>,
    pub branch: String,
    pub fast_forward: bool,
    pub replayed: usize,
    pub skipped: usize,
    pub conflicts: usize,
    pub failures: usize,
    pub total_commits: usize,
    pub source_head_t: i64,
    pub source_head_id: CommitId,
    pub strategy: ConflictStrategy,
}

/// Push submission payload.
///
/// Carries precomputed commit v2 blobs (oldest-first) to be validated,
/// stored, and appended to the target ledger's commit head. `blobs` is an
/// auxiliary map of any non-commit objects (e.g., `commit.txn`) the
/// commits reference, keyed by content ID or legacy address.
pub struct PushRequest {
    pub idempotency_key: Option<IdempotencyKey>,
    pub ledger_id: String,
    pub commits: Vec<Vec<u8>>,
    pub blobs: HashMap<String, Vec<u8>>,
    pub governance: GovernanceOptions,
}

/// Receipt returned once a push is durably accepted.
#[derive(Debug, Clone)]
pub struct PushReceipt {
    pub idempotency_key: Option<IdempotencyKey>,
    pub ledger: String,
    pub accepted: usize,
    pub head_t: i64,
    pub head_id: CommitId,
    pub indexing: IndexingStatus,
}

/// Receipt for any operation submitted through consensus.
///
/// Variants correspond one-to-one with [`Submitter`] trait methods. The
/// umbrella type lets [`SubmissionState`] and the idempotency cache stay
/// uniform across operation kinds without erasing per-op typing at the
/// trait methods themselves.
#[derive(Debug, Clone)]
pub enum OperationReceipt {
    Transaction(TransactionReceipt),
    Revert(RevertReceipt),
    Merge(MergeReceipt),
    Rebase(RebaseReceipt),
    Push(PushReceipt),
}

/// State of a previously-submitted operation, accessible by idempotency key.
#[derive(Debug, Clone)]
pub enum SubmissionState {
    /// No submission with this key is known.
    Unknown,
    /// Submission accepted, durability not yet acknowledged.
    InFlight,
    /// Submission durably accepted and committed.
    Committed(OperationReceipt),
    /// Submission attempted but failed.
    Failed(SubmissionError),
}

/// Errors returned from a submission attempt.
#[derive(Debug, Clone, Error)]
pub enum SubmissionError {
    /// The idempotency key was previously used for a transaction with a
    /// different body. Callers should treat this as a programming error —
    /// keys identify a specific submission and must not be reused with
    /// different content.
    #[error("idempotency key collision: key already used for a different transaction")]
    KeyCollision,

    /// A submission with this key is already in progress. Callers waiting
    /// for an idempotent retry should poll [`SubmissionLookup::status`]
    /// rather than re-submitting.
    #[error("submission with this key is already in progress")]
    AlreadyInFlight,

    /// The submission was processed and failed.
    ///
    /// `status` is the HTTP status code categorising the failure — `4xx`
    /// for a bad request (malformed transaction, policy denial, missing
    /// ledger), `5xx` for an internal failure. Carrying the status lets
    /// callers render an accurate response without coupling to any one
    /// implementation's error taxonomy.
    #[error("{message}")]
    Execution { status: u16, message: String },
}

/// Submit operations for processing.
///
/// Each method represents an operation kind — transactions, reverts, and
/// (later) merges and rebases — that requires durable acceptance through
/// the same consensus path. Implementations choose how acceptance is
/// achieved (local execution for monolithic, leader replication for Raft,
/// quorum voting for BFT) but the caller's contract is identical per
/// method: pass a request, await the future, get the per-op receipt when
/// durably accepted.
///
/// "Durably accepted" means the operation is persisted and visible to
/// subsequent reads on this same consensus instance. Cross-instance read
/// consistency (e.g., querying a follower right after writing on a leader)
/// is handled at the read path, not here.
///
/// Dropping the returned future does **not** cancel the underlying
/// submission. Once accepted internally, the work proceeds to completion
/// regardless. To learn the outcome after dropping, look up by idempotency
/// key via [`SubmissionLookup`].
#[async_trait]
pub trait Submitter: Send + Sync {
    /// Stage and commit a transaction.
    async fn transact(
        &self,
        request: TransactionRequest,
    ) -> Result<TransactionReceipt, SubmissionError>;

    /// Revert the effects of one or more commits on a branch as a single
    /// inverse commit.
    async fn revert(
        &self,
        request: RevertRequest,
    ) -> Result<RevertReceipt, SubmissionError>;

    /// Replay a source branch onto a target branch — fast-forward when the
    /// target hasn't diverged, otherwise a merge commit under the supplied
    /// conflict strategy.
    async fn merge(
        &self,
        request: MergeRequest,
    ) -> Result<MergeReceipt, SubmissionError>;

    /// Replay a branch's unique commits on top of its source branch's
    /// current HEAD.
    async fn rebase(
        &self,
        request: RebaseRequest,
    ) -> Result<RebaseReceipt, SubmissionError>;

    /// Ingest precomputed commit v2 blobs onto a ledger, advancing its
    /// commit head.
    async fn push(
        &self,
        request: PushRequest,
    ) -> Result<PushReceipt, SubmissionError>;
}

/// Look up the state of a previously-submitted transaction by its
/// idempotency key.
///
/// Pairs with [`Submitter`] for callers that need to discover the outcome
/// of a submission whose returned future was lost (timeout, disconnect,
/// process restart). Most implementations of [`Submitter`] also implement
/// this trait, but they are intentionally separable — a thin submission
/// proxy might implement only [`Submitter`] and delegate status lookup
/// elsewhere.
#[async_trait]
pub trait SubmissionLookup: Send + Sync {
    async fn status(&self, ledger_id: &str, key: &IdempotencyKey) -> SubmissionState;
}
