//! Error types for AWS storage backends

use thiserror::Error;

/// Errors from AWS storage operations
#[derive(Debug, Error)]
pub enum AwsStorageError {
    /// I/O or network error
    #[error("I/O error: {0}")]
    Io(String),

    /// Resource not found
    #[error("Not found: {0}")]
    NotFound(String),

    /// Precondition failed (CAS conflict - 412)
    #[error("Precondition failed (CAS conflict)")]
    PreconditionFailed,

    /// Unauthorized - invalid credentials
    #[error("Unauthorized: {0}")]
    Unauthorized(String),

    /// Forbidden - insufficient permissions
    #[error("Forbidden: {0}")]
    Forbidden(String),

    /// Throttled - rate limited
    #[error("Throttled: {0}")]
    Throttled(String),

    /// Invalid configuration
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    /// Missing region configuration
    #[error("Missing AWS region configuration")]
    MissingRegion,

    /// S3 SDK error
    #[cfg(feature = "s3")]
    #[error("S3 error: {0}")]
    S3(String),

    /// DynamoDB SDK error
    #[cfg(feature = "dynamodb")]
    #[error("DynamoDB error: {0}")]
    DynamoDB(String),

    /// Other error
    #[error("{0}")]
    Other(String),
}

impl AwsStorageError {
    pub fn io(msg: impl Into<String>) -> Self {
        Self::Io(msg.into())
    }

    pub fn not_found(msg: impl Into<String>) -> Self {
        Self::NotFound(msg.into())
    }

    pub fn invalid_config(msg: impl Into<String>) -> Self {
        Self::InvalidConfig(msg.into())
    }

    #[cfg(feature = "s3")]
    pub fn s3(msg: impl Into<String>) -> Self {
        Self::S3(msg.into())
    }

    #[cfg(feature = "dynamodb")]
    pub fn dynamodb(msg: impl Into<String>) -> Self {
        Self::DynamoDB(msg.into())
    }

    pub fn other(msg: impl Into<String>) -> Self {
        Self::Other(msg.into())
    }
}

/// Result type for AWS storage operations
pub type Result<T> = std::result::Result<T, AwsStorageError>;

// Convert to fluree_db_core errors
impl From<AwsStorageError> for fluree_db_core::error::Error {
    fn from(err: AwsStorageError) -> Self {
        match err {
            AwsStorageError::NotFound(msg) => fluree_db_core::error::Error::not_found(msg),
            AwsStorageError::Io(msg) => fluree_db_core::error::Error::io(msg),
            _ => fluree_db_core::error::Error::storage(err.to_string()),
        }
    }
}
