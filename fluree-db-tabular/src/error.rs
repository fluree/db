//! Error types for tabular operations.

use thiserror::Error;

/// Errors from tabular batch operations.
#[derive(Debug, Error)]
pub enum TabularError {
    /// Schema or structural error (column count mismatch, row count mismatch, etc.)
    #[error("Schema error: {0}")]
    Schema(String),
}

/// Result type for tabular operations.
pub type Result<T> = std::result::Result<T, TabularError>;
