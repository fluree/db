//! Error types for the nameservice crate

use fluree_db_core::ledger_id::LedgerIdParseError;
use thiserror::Error;

/// Result type for nameservice operations
pub type Result<T> = std::result::Result<T, NameServiceError>;

/// Errors that can occur in nameservice operations
#[derive(Error, Debug)]
pub enum NameServiceError {
    /// Ledger not found
    #[error("Ledger not found: {0}")]
    NotFound(String),

    /// Invalid ID format (ledger_id or graph_source_id)
    #[error("Invalid ID format: {0}")]
    InvalidId(String),

    /// Storage/IO error
    #[error("Storage error: {0}")]
    Storage(String),

    /// Serialization error
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// IO error
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// The ledger has been retracted
    #[error("Ledger has been retracted: {0}")]
    Retracted(String),

    /// Ledger already exists (cannot create)
    #[error("Ledger already exists: {0}")]
    LedgerAlreadyExists(String),

    /// The underlying state machine rejected the propose with a
    /// terminal failure that retrying won't fix — typically a
    /// state-machine invariant the apply path surfaced, or a
    /// replicated apply path returning an unreachable response
    /// variant. Distinguished from [`Self::Storage`] so callers can
    /// route to a deterministic terminal handler (e.g. queue
    /// poisoning) instead of looping on the same propose forever.
    #[error("State machine rejected propose: {0}")]
    ApplyRejected(String),

    /// The replicated apply observed that the proposed work no
    /// longer applies to its target — the queue entry was popped
    /// by a racing worker or admin-cleared between stage and
    /// propose. Distinguished from [`Self::Storage`] so callers
    /// drop the local install and move on rather than retrying
    /// against a state that will never match again.
    #[error("Apply observed stale state: {0}")]
    ApplyStale(String),

    /// The propose arrived behind the current state of affairs —
    /// it was built on a view lagging the replicated state (its
    /// value doesn't advance the current head), or another attempt
    /// at the same work was already in flight. The target work is
    /// still pending, so callers should refresh their local view
    /// and rebuild the propose. Distinguished from
    /// [`Self::ApplyStale`] (the work is gone; drop it) and
    /// [`Self::Storage`] (transient; retry the same propose).
    #[error("Apply lagged behind current state: {0}")]
    ApplyLagged(String),

    /// A propose toward the replicated log ended without a
    /// determinable outcome — transport failure, lost response, or
    /// leader step-down between submission and reply. The work may
    /// have committed: callers must not treat this as "nothing
    /// happened" (in particular, must not release resources a
    /// committed outcome would reference) and recover by
    /// re-checking replicated state or rebuilding the propose.
    /// Distinguished from [`Self::Storage`] (backend error; nothing
    /// was submitted) and from the decided apply outcomes
    /// ([`Self::ApplyStale`], [`Self::ApplyLagged`],
    /// [`Self::ApplyRejected`]).
    #[error("Propose outcome unresolved: {0}")]
    ProposeUnresolved(String),
}

impl From<LedgerIdParseError> for NameServiceError {
    fn from(e: LedgerIdParseError) -> Self {
        Self::InvalidId(e.to_string())
    }
}

impl From<fluree_db_core::StorageExtError> for NameServiceError {
    fn from(e: fluree_db_core::StorageExtError) -> Self {
        Self::Storage(e.to_string())
    }
}

impl From<fluree_db_core::Error> for NameServiceError {
    fn from(e: fluree_db_core::Error) -> Self {
        Self::Storage(e.to_string())
    }
}

impl NameServiceError {
    /// Create a not found error
    pub fn not_found(id: impl Into<String>) -> Self {
        Self::NotFound(id.into())
    }

    /// Create an invalid ID format error
    pub fn invalid_id(msg: impl Into<String>) -> Self {
        Self::InvalidId(msg.into())
    }

    /// Create a storage error
    pub fn storage(msg: impl Into<String>) -> Self {
        Self::Storage(msg.into())
    }

    /// Create a ledger already exists error
    pub fn ledger_already_exists(id: impl Into<String>) -> Self {
        Self::LedgerAlreadyExists(id.into())
    }

    /// Create an [`Self::ApplyRejected`] error signaling a terminal
    /// state-machine apply failure. Use for variants the caller
    /// should treat as "give up, don't retry" rather than the
    /// generic transient [`Self::storage`] phrasing.
    pub fn apply_rejected(msg: impl Into<String>) -> Self {
        Self::ApplyRejected(msg.into())
    }

    /// Create an [`Self::ApplyStale`] error signaling that the
    /// proposed work no longer applies (queue front advanced past
    /// the proposed queue_id, or the queue was admin-cleared).
    /// Callers drop the local install and continue rather than
    /// retrying.
    pub fn apply_stale(msg: impl Into<String>) -> Self {
        Self::ApplyStale(msg.into())
    }

    /// Create an [`Self::ApplyLagged`] error signaling that the
    /// propose was built on a view lagging the replicated state and
    /// its target is still pending. Callers refresh their local
    /// view and rebuild the propose rather than dropping the work.
    pub fn apply_lagged(msg: impl Into<String>) -> Self {
        Self::ApplyLagged(msg.into())
    }

    /// Create a [`Self::ProposeUnresolved`] error signaling that a
    /// propose's outcome could not be determined and may have
    /// committed. Callers keep any resources the committed outcome
    /// would reference and re-check replicated state before acting.
    pub fn propose_unresolved(msg: impl Into<String>) -> Self {
        Self::ProposeUnresolved(msg.into())
    }
}
