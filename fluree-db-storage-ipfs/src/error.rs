//! Error types for IPFS storage backend.

use thiserror::Error;

/// Errors from the IPFS storage backend.
#[derive(Debug, Error)]
pub enum IpfsStorageError {
    #[error("IPFS node connection failed: {0}")]
    ConnectionFailed(String),

    #[error("IPFS block not found: {0}")]
    NotFound(String),

    #[error("IPFS RPC error: {0}")]
    Rpc(String),

    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("CID verification failed: expected {expected}, got {actual}")]
    CidMismatch { expected: String, actual: String },

    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, IpfsStorageError>;

impl From<IpfsStorageError> for fluree_db_core::error::Error {
    fn from(err: IpfsStorageError) -> Self {
        match err {
            IpfsStorageError::NotFound(msg) => fluree_db_core::error::Error::not_found(msg),
            other => fluree_db_core::error::Error::storage(other.to_string()),
        }
    }
}
