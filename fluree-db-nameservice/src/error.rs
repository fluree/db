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
}
