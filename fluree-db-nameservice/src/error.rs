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
}
