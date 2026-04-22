//! Error types for vector search operations.

use thiserror::Error;

/// Error type for vector index operations.
#[derive(Debug, Error)]
pub enum VectorError {
    /// Vector dimension mismatch
    #[error("Vector dimension mismatch: expected {expected}, got {actual}")]
    DimensionMismatch { expected: usize, actual: usize },

    /// Invalid distance metric
    #[error("Invalid distance metric: {0}")]
    InvalidMetric(String),

    /// Index operation failed
    #[error("Index operation failed: {0}")]
    IndexError(String),

    /// Serialization error
    #[error("Serialization error: {0}")]
    SerializeError(String),

    /// Point ID collision that couldn't be resolved
    #[error("Point ID collision could not be resolved for IRI: {0}")]
    PointIdCollision(String),

    /// usearch library error
    #[error("usearch error: {0}")]
    Usearch(String),

    /// IO error
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Postcard serialization error
    #[error("Postcard error: {0}")]
    Postcard(#[from] postcard::Error),

    /// Invalid snapshot format
    #[error("Invalid snapshot format: {0}")]
    InvalidFormat(String),

    /// Unsupported format version
    #[error("Unsupported snapshot version: {version} (max supported: {max_supported})")]
    UnsupportedVersion { version: u8, max_supported: u8 },
}

pub type Result<T> = std::result::Result<T, VectorError>;
