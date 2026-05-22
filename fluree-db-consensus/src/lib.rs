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
use fluree_db_api::{TrackingOptions, TrackingTally};
use fluree_db_transact::{CommitOpts, CommitReceipt, TxnOpts, TxnType};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
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
/// The variant fixes how the body is parsed and staged; [`TransactionRequest::txn_type`]
/// independently fixes the insert/update/upsert semantics. Not every
/// combination is valid — Turtle/TriG bodies do not support `Update`
/// (SPARQL UPDATE is the update path for RDF text).
pub enum TransactionBody {
    /// A JSON-LD transaction document.
    JsonLd(JsonValue),
    /// Turtle or TriG RDF text. The parser handles TriG `GRAPH` blocks.
    Turtle(String),
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
pub struct TransactionRequest {
    pub idempotency_key: Option<IdempotencyKey>,
    pub txn_type: TxnType,
    pub body: TransactionBody,
    pub txn_opts: TxnOpts,
    pub commit_opts: CommitOpts,
    pub tracking: Option<TrackingOptions>,
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

/// State of a previously-submitted transaction, accessible by idempotency key.
#[derive(Debug, Clone)]
pub enum SubmissionState {
    /// No submission with this key is known.
    Unknown,
    /// Submission accepted, durability not yet acknowledged.
    InFlight,
    /// Submission durably accepted and committed.
    Committed(TransactionReceipt),
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

    /// Implementation-defined submission failure. Surfaces transactor,
    /// storage, network, or consensus-protocol errors as a string so
    /// callers do not couple to any one implementation's error taxonomy.
    #[error("submission failed: {0}")]
    Submission(String),
}

/// Submit transactions for processing.
///
/// Implementations choose how acceptance is achieved — local execution for
/// monolithic, leader replication for Raft, quorum voting for BFT — but
/// the caller's contract is identical: pass a [`TransactionRequest`], await
/// the future, get a [`TransactionReceipt`] when durably accepted.
///
/// "Durably accepted" means the transaction is persisted and visible to
/// subsequent reads on this same consensus instance. Cross-instance read
/// consistency (e.g., querying a follower right after writing on a leader)
/// is handled at the read path, not here.
///
/// Dropping the returned future does **not** cancel the underlying submission.
/// Once accepted internally, the work proceeds to completion regardless. To
/// learn the outcome after dropping, look up by idempotency key via
/// [`SubmissionLookup`].
#[async_trait]
pub trait Submitter: Send + Sync {
    async fn submit(
        &self,
        ledger_id: &str,
        request: TransactionRequest,
    ) -> Result<TransactionReceipt, SubmissionError>;
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
