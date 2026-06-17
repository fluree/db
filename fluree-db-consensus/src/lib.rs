//! Transaction submission traits and consensus implementations for Fluree DB.
//!
//! This crate defines the abstraction by which transactions are submitted
//! and accepted into a ledger. Each implementation has its own trust model
//! and durability mechanism:
//!
//! - [`LocalCommitter`] — runs the parse → stage → policy → commit
//!   pipeline against a local [`Fluree`](fluree_db_api::Fluree) instance.
//!   The base over which other committers compose.
//! - [`CachingCommitter`] — wraps an inner committer with an
//!   in-memory idempotency cache (TTL-bounded retry collapse + status
//!   lookup via [`SubmissionLookup`]) and an admission-control
//!   semaphore that bounds in-flight submissions.
//!
//! Future implementations (Raft for crash-fault tolerance, BFT for
//! byzantine tolerance) will live alongside, behind the same
//! [`Committer`] trait, and compose the same way.
//!
//! Submission identity and status lookup are driven by optional
//! [`IdempotencyKey`]s. Callers who want idempotent retry or after-the-fact
//! status lookup generate a key (typically a ULID) and include it in their
//! [`TransactionRequest`]; submissions sharing a key collapse to a single
//! outcome. Callers who don't need those guarantees may omit the key.

pub mod caching;
pub mod local;
#[cfg(feature = "raft")]
pub mod raft;

pub use caching::{CachingCommitter, DEFAULT_IDEMPOTENCY_TTL};
pub use local::LocalCommitter;

// Trait re-exports for embedders that hold a type-erased committer.
// `SubmittingCommitter` is the combined surface AppState typically
// holds; `Committer` and `SubmissionLookup` are the constituents.
#[cfg(feature = "raft")]
pub use raft::{ClusterNode, Command, NodeId, Response, TypeConfig};

/// Re-exports from openraft so embedders can construct a
/// [`Raft<TypeConfig>`] handle without taking a direct openraft
/// dependency.
#[cfg(feature = "raft")]
pub use openraft::error::Fatal as RaftFatal;
#[cfg(feature = "raft")]
pub use openraft::{
    Config as RaftConfig, ConfigError as RaftConfigError, Raft, ServerState as RaftServerState,
};

use async_trait::async_trait;
use fluree_db_api::{
    CommitId, ConflictStrategy, GovernanceOptions, IndexingStatus, RevertSelection,
    TrackingOptions, TrackingTally,
};
use fluree_db_transact::{CommitOpts, CommitOptsRequest, CommitReceipt, TxnOpts};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::fmt;
use thiserror::Error;

/// Caller-provided identifier for a write submission.
///
/// Used for idempotent retry (retries with the same key collapse to one
/// outcome) and for after-the-fact status lookup. Callers typically
/// generate a ULID before submission so they can recover after a
/// disconnect.
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

/// Composite lookup key for idempotency caches: an [`IdempotencyKey`]
/// scoped to a particular ledger. Submissions on different ledgers
/// with the same key are independent.
///
/// Used by both the in-process [`CachingCommitter`] and the
/// replicated Raft state machine so the two layers agree on cache
/// identity without owning parallel definitions.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IdempotencyCacheKey {
    pub ledger_id: String,
    pub key: IdempotencyKey,
}

impl IdempotencyCacheKey {
    pub fn new(ledger_id: impl Into<String>, key: IdempotencyKey) -> Self {
        Self {
            ledger_id: ledger_id.into(),
            key,
        }
    }
}

/// The transaction payload in its submitted form.
///
/// Each variant fixes both the parser path *and* the insert/upsert/update
/// semantics, so invalid combinations (e.g. Turtle-Update, TriG-Insert)
/// are unrepresentable at the type level. SPARQL UPDATE encodes its own
/// semantics in the query and so has no per-op variants.
#[derive(Debug, Clone, Serialize, Deserialize)]
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

impl TransactionBody {
    /// Stable short name for this body's operation kind. Used in
    /// telemetry / correlation, so spelling matches across the variants
    /// that share semantics.
    pub fn operation_tag(&self) -> &'static str {
        match self {
            Self::JsonLdInsert(_) | Self::TurtleInsert(_) => "insert",
            Self::JsonLdUpsert(_) | Self::TurtleUpsert(_) | Self::TrigUpsert(_) => "upsert",
            Self::JsonLdUpdate(_) => "update",
            Self::Sparql(_) => "sparql-update",
        }
    }

    /// SHA-256 of this body's canonical bytes.
    ///
    /// Each variant tag is mixed into the digest so the same JSON
    /// bytes under different semantics (insert vs upsert vs update)
    /// hash to different values — two retries that disagree on
    /// operation kind collide correctly.
    ///
    /// Used both by the in-process caching layer (to detect
    /// "same key, different body" misuse) and by consensus-coordinated
    /// nameservices (to populate the [`IdempotencyContext`]'s
    /// `body_hash` field).
    pub fn body_hash(&self) -> [u8; 32] {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        match self {
            Self::JsonLdInsert(json) => {
                hasher.update(b"jsonld-insert");
                hasher.update(json.to_string().as_bytes());
            }
            Self::JsonLdUpsert(json) => {
                hasher.update(b"jsonld-upsert");
                hasher.update(json.to_string().as_bytes());
            }
            Self::JsonLdUpdate(json) => {
                hasher.update(b"jsonld-update");
                hasher.update(json.to_string().as_bytes());
            }
            Self::TurtleInsert(text) => {
                hasher.update(b"turtle-insert");
                hasher.update(text.as_bytes());
            }
            Self::TurtleUpsert(text) => {
                hasher.update(b"turtle-upsert");
                hasher.update(text.as_bytes());
            }
            Self::TrigUpsert(text) => {
                hasher.update(b"trig-upsert");
                hasher.update(text.as_bytes());
            }
            Self::Sparql(text) => {
                hasher.update(b"sparql");
                hasher.update(text.as_bytes());
            }
        }
        hasher.finalize().into()
    }
}

/// Serializable envelope a consensus-coordinated committer writes to
/// shared content-addressed storage before enqueueing work.
///
/// The CID of this blob is what travels through the Raft command queue
/// (as `EnqueueCommandArgs::request_cid`); the worker reads the blob
/// back to recover everything it needs to advance the head. Bundling
/// the per-request context here means the queue itself stays thin
/// (one CID + a body-kind discriminator) and we don't have to
/// replicate large opaque values through the Raft log.
///
/// One variant per `Committer` method.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueuedRequest {
    /// `transact` — body, opts, tracking, governance to stage and
    /// commit through the worker.
    Transact(QueuedTransact),
    /// `push` — raw commit-chain bytes the client supplied. The
    /// worker decodes via `Fluree::prepare_push`, advances the head
    /// to the chain's final commit.
    Push(QueuedPush),
    /// `revert` — selection + conflict strategy. The worker
    /// re-runs `Fluree::prepare_revert` and advances the head to
    /// the resulting inverse commit (or NoOp short-circuits when
    /// the conflict strategy drops every reverted flake).
    Revert(QueuedRevert),
    /// `merge` — source / target branches + conflict strategy.
    /// The worker re-runs `Fluree::prepare_merge` and advances the
    /// target branch's head.
    Merge(QueuedMerge),
    /// `rebase` — branch + conflict strategy. The worker re-runs
    /// `Fluree::prepare_rebase` and advances the branch's head.
    Rebase(QueuedRebase),
}

impl QueuedRequest {
    /// Encode the envelope for content-addressed storage. The leader
    /// writes these bytes to CAS; the resulting `ContentId` becomes
    /// the `request_cid` in `EnqueueCommandArgs`.
    ///
    /// JSON is used here (not postcard like state-machine snapshots)
    /// because the body and several option fields carry
    /// `serde_json::Value`, which postcard refuses to round-trip —
    /// its arbitrary-precision `Number` shape needs a self-describing
    /// format. The envelope is one-shot (one write, one read) so the
    /// size overhead is negligible compared to the body itself.
    pub fn to_bytes(&self) -> Result<Vec<u8>, QueuedRequestCodecError> {
        Ok(serde_json::to_vec(self)?)
    }

    /// Decode the envelope retrieved from CAS by `request_cid`. Used
    /// worker-side to recover the request context.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, QueuedRequestCodecError> {
        Ok(serde_json::from_slice(bytes)?)
    }
}

/// Transact-side envelope payload. Fields mirror the request-side
/// projection of [`TransactionRequest`]. Node-side concerns (signing
/// keys, in-flight upload handles) live on the runtime [`CommitOpts`]
/// and are rehydrated worker-side — they do not travel through CAS.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueuedTransact {
    pub body: TransactionBody,
    pub txn_opts: TxnOpts,
    pub commit_opts: CommitOptsRequest,
    pub tracking: Option<TrackingOptions>,
    pub governance: GovernanceOptions,
}

/// Push-side envelope payload.
///
/// Commits are content-addressed: the [`QueuedTransactor`] writes
/// each commit's bytes to the per-ledger content store before
/// enqueueing and records only the resulting [`ContentId`]s here.
/// That keeps the envelope small (no commit bytes inline) and lines
/// up with the design's "commits already in CAS when the worker
/// picks up the entry" assumption — the worker reads them back by
/// CID and reconstructs the [`fluree_db_api::PushCommitsRequest`]
/// the existing `prepare_push` pipeline expects.
///
/// Auxiliary `blobs` stay keyed by their client-supplied address
/// (which may be a CID or a legacy hex address) and travel as raw
/// bytes — the keys are reference targets the commits rely on, so
/// the server can't recompute them via `ContentId::new(...)`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueuedPush {
    pub commit_cids: Vec<CommitId>,
    pub blobs: HashMap<String, Vec<u8>>,
    pub governance: GovernanceOptions,
}

/// Revert-side envelope payload. Mirrors the fields of
/// [`RevertRequest`] the worker needs to re-run `prepare_revert`. The
/// branch + ledger come from the `EnqueueCommandArgs` shell so we
/// don't duplicate them in the envelope.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueuedRevert {
    pub selection: RevertSelection,
    pub strategy: ConflictStrategy,
}

/// Merge-side envelope payload. The queue entry rides on the target
/// branch (the one whose head the merge mutates); the source branch
/// is named here. When `target_branch` is `None`, the worker resolves
/// it the same way the legacy committer does — falling back to the
/// source's parent branch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueuedMerge {
    pub source_branch: String,
    pub target_branch: Option<String>,
    pub strategy: ConflictStrategy,
}

/// Rebase-side envelope payload. The queue entry rides on the branch
/// being rebased (the one whose head shifts to the source's HEAD plus
/// replayed commits).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueuedRebase {
    pub strategy: ConflictStrategy,
}

/// Error encoding or decoding a [`QueuedRequest`].
#[derive(Debug, Error)]
pub enum QueuedRequestCodecError {
    #[error("json codec: {0}")]
    Json(#[from] serde_json::Error),
}

/// Transaction submission payload.
///
/// Carries the transaction itself plus everything an implementation needs to
/// process it. Implementation-specific knobs (e.g., Raft propose timeout,
/// per-committer admission overrides) live on the implementation, not here.
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
/// Variants correspond one-to-one with [`Committer`] trait methods. The
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

    /// The consensus implementation has reached its in-flight operation
    /// cap and refused the submission without executing it. Callers
    /// should retry with backoff.
    #[error("committer overloaded; in-flight operation cap reached")]
    Overloaded,
}

/// Submit operations for processing.
///
/// Each method represents an operation kind — transactions, reverts,
/// merges, rebases, pushes — that requires durable acceptance.
/// Implementations choose how acceptance is achieved (direct local
/// execution, leader replication for Raft, quorum voting for BFT) but
/// the caller's contract is identical per method: pass a request, await
/// the future, get the per-op receipt when durably accepted.
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
pub trait Committer: Send + Sync {
    /// Stage and commit a transaction.
    async fn transact(
        &self,
        request: TransactionRequest,
    ) -> Result<TransactionReceipt, SubmissionError>;

    /// Revert the effects of one or more commits on a branch as a single
    /// inverse commit.
    async fn revert(&self, request: RevertRequest) -> Result<RevertReceipt, SubmissionError>;

    /// Replay a source branch onto a target branch — fast-forward when the
    /// target hasn't diverged, otherwise a merge commit under the supplied
    /// conflict strategy.
    async fn merge(&self, request: MergeRequest) -> Result<MergeReceipt, SubmissionError>;

    /// Replay a branch's unique commits on top of its source branch's
    /// current HEAD.
    async fn rebase(&self, request: RebaseRequest) -> Result<RebaseReceipt, SubmissionError>;

    /// Ingest precomputed commit v2 blobs onto a ledger, advancing its
    /// commit head.
    async fn push(&self, request: PushRequest) -> Result<PushReceipt, SubmissionError>;
}

/// Look up the state of a previously-submitted transaction by its
/// idempotency key.
///
/// Pairs with [`Committer`] for callers that need to discover the outcome
/// of a submission whose returned future was lost (timeout, disconnect,
/// process restart). Most implementations of [`Committer`] also implement
/// this trait, but they are intentionally separable — a thin submission
/// proxy might implement only [`Committer`] and delegate status lookup
/// elsewhere.
#[async_trait]
pub trait SubmissionLookup: Send + Sync {
    async fn status(&self, ledger_id: &str, key: &IdempotencyKey) -> SubmissionState;
}

/// Combined committer + lookup trait. Lets callers (notably
/// `fluree-db-server::AppState`) hold a single
/// `Arc<dyn SubmittingCommitter>` whose concrete type can swap
/// between [`LocalCommitter`] and the Raft-side
/// [`QueuedTransactor`](crate::raft::queued_transactor::QueuedTransactor)
/// at server-construction time.
///
/// Blanket-implemented for every type that already implements both
/// parent traits, so no manual impl is needed on
/// [`CachingCommitter`] / [`LocalCommitter`] / `QueuedTransactor`.
pub trait SubmittingCommitter: Committer + SubmissionLookup {}
impl<T> SubmittingCommitter for T where T: Committer + SubmissionLookup + ?Sized {}
