//! Error types for fluree-db-core

use thiserror::Error;

/// Result type alias using our Error
pub type Result<T> = std::result::Result<T, Error>;

/// Core error type
#[derive(Error, Debug)]
pub enum Error {
    /// Storage-related errors
    #[error("Storage error: {0}")]
    Storage(String),

    /// Resource not found
    #[error("Not found: {0}")]
    NotFound(String),

    /// Invalid address format
    #[error("Invalid address: {0}")]
    InvalidAddress(String),

    /// JSON parsing error (serde_json)
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// Invalid index type
    #[error("Invalid index: {0}")]
    InvalidIndex(String),

    /// Invalid range query
    #[error("Invalid range: {0}")]
    InvalidRange(String),

    /// Cache error
    #[error("Cache error: {0}")]
    Cache(String),

    /// I/O error
    #[error("I/O error: {0}")]
    Io(String),

    /// Invalid commit format
    #[error("Invalid commit: {0}")]
    InvalidCommit(String),

    /// Generic error with message
    #[error("{0}")]
    Other(String),

    /// Fuel limit exceeded during a tracked operation.
    #[error("{0}")]
    FuelExceeded(#[from] crate::tracking::FuelExceededError),
}

impl Error {
    /// Create a storage error
    pub fn storage(msg: impl Into<String>) -> Self {
        Error::Storage(msg.into())
    }

    /// Create a not found error
    pub fn not_found(msg: impl Into<String>) -> Self {
        Error::NotFound(msg.into())
    }

    /// Create an invalid address error
    pub fn invalid_address(msg: impl Into<String>) -> Self {
        Error::InvalidAddress(msg.into())
    }

    /// Create an invalid index error
    pub fn invalid_index(msg: impl Into<String>) -> Self {
        Error::InvalidIndex(msg.into())
    }

    /// Create an invalid commit error
    pub fn invalid_commit(msg: impl Into<String>) -> Self {
        Error::InvalidCommit(msg.into())
    }

    /// Create an invalid range error
    pub fn invalid_range(msg: impl Into<String>) -> Self {
        Error::InvalidRange(msg.into())
    }

    /// Create a cache error
    pub fn cache(msg: impl Into<String>) -> Self {
        Error::Cache(msg.into())
    }

    /// Create an I/O error
    pub fn io(msg: impl Into<String>) -> Self {
        Error::Io(msg.into())
    }

    /// Create a generic error
    pub fn other(msg: impl Into<String>) -> Self {
        Error::Other(msg.into())
    }
}
