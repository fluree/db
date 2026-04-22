//! Error types for nameservice sync operations

use thiserror::Error;

/// Errors from sync operations
#[derive(Debug, Error)]
pub enum SyncError {
    /// Network or HTTP error communicating with a remote
    #[error("Remote communication error: {0}")]
    Remote(String),

    /// Error from the local nameservice
    #[error("Nameservice error: {0}")]
    Nameservice(#[from] fluree_db_nameservice::NameServiceError),

    /// JSON serialization/deserialization error
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// Configuration error
    #[error("Configuration error: {0}")]
    Config(String),

    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Exhausted all origins / not found / auth failures
    #[error("All origins failed for CID {cid}: {details}")]
    FetchFailed { cid: String, details: String },

    /// Bytes returned but hash doesn't verify — always terminal, never retry
    #[error("Integrity verification failed for CID {0}")]
    IntegrityFailed(String),

    /// Pack protocol error (malformed stream, invalid frame, server-side error)
    #[error("Pack protocol error: {0}")]
    PackProtocol(String),

    /// Remote server does not support pack endpoint (404 or 406).
    /// Caller should fall back to paginated export.
    #[error("Pack endpoint not supported by server")]
    PackNotSupported,
}

impl From<reqwest::Error> for SyncError {
    fn from(e: reqwest::Error) -> Self {
        SyncError::Remote(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, SyncError>;
