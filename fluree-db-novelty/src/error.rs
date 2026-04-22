//! Error types for the novelty crate

use thiserror::Error;

/// Result type for novelty operations
pub type Result<T> = std::result::Result<T, NoveltyError>;

/// Errors that can occur in novelty operations
#[derive(Error, Debug)]
pub enum NoveltyError {
    /// Novelty overflow - too many flakes, trigger reindex
    #[error("Novelty overflow: {0}")]
    Overflow(String),

    /// Storage/IO error
    #[error("Storage error: {0}")]
    Storage(String),

    /// Commit not found
    #[error("Commit not found: {0}")]
    CommitNotFound(String),

    /// Invalid commit format
    #[error("Invalid commit format: {0}")]
    InvalidCommit(String),

    /// Unknown graph Sid — flake references a graph not in reverse_graph
    #[error("Invalid graph: {0}")]
    InvalidGraph(String),

    /// Serialization error
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// Core error wrapper
    #[error("Core error: {0}")]
    Core(#[from] fluree_db_core::Error),
}

impl NoveltyError {
    /// Create an overflow error
    pub fn overflow(msg: impl Into<String>) -> Self {
        Self::Overflow(msg.into())
    }

    /// Create a storage error
    pub fn storage(msg: impl Into<String>) -> Self {
        Self::Storage(msg.into())
    }

    /// Create a commit not found error
    pub fn commit_not_found(addr: impl Into<String>) -> Self {
        Self::CommitNotFound(addr.into())
    }

    /// Create an invalid commit error
    pub fn invalid_commit(msg: impl Into<String>) -> Self {
        Self::InvalidCommit(msg.into())
    }
}
