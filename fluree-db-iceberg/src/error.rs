//! Error types for Iceberg operations.

use thiserror::Error;

/// Errors from Iceberg operations.
#[derive(Debug, Error)]
pub enum IcebergError {
    /// Configuration error
    #[error("Configuration error: {0}")]
    Config(String),

    /// Authentication error
    #[error("Authentication error: {0}")]
    Auth(String),

    /// Catalog error (REST API issues)
    #[error("Catalog error: {0}")]
    Catalog(String),

    /// HTTP/network error
    #[error("HTTP error: {0}")]
    Http(String),

    /// Metadata parsing error
    #[error("Metadata error: {0}")]
    Metadata(String),

    /// Storage/IO error
    #[error("Storage error: {0}")]
    Storage(String),

    /// Snapshot not found
    #[error("Snapshot not found: {0}")]
    SnapshotNotFound(String),

    /// Table not found
    #[error("Table not found: {0}")]
    TableNotFound(String),

    /// Credential error
    #[error("Credential error: {0}")]
    Credential(String),

    /// Manifest parsing error
    #[error("Manifest error: {0}")]
    Manifest(String),

    /// Scan planning error
    #[error("Scan error: {0}")]
    Scan(String),

    /// Unsupported file format
    #[error("Unsupported file format: {0}")]
    UnsupportedFormat(String),
}

impl IcebergError {
    pub fn config(msg: impl Into<String>) -> Self {
        Self::Config(msg.into())
    }

    pub fn auth(msg: impl Into<String>) -> Self {
        Self::Auth(msg.into())
    }

    pub fn catalog(msg: impl Into<String>) -> Self {
        Self::Catalog(msg.into())
    }

    pub fn metadata(msg: impl Into<String>) -> Self {
        Self::Metadata(msg.into())
    }

    pub fn storage(msg: impl Into<String>) -> Self {
        Self::Storage(msg.into())
    }

    pub fn credential(msg: impl Into<String>) -> Self {
        Self::Credential(msg.into())
    }

    pub fn manifest(msg: impl Into<String>) -> Self {
        Self::Manifest(msg.into())
    }

    pub fn scan(msg: impl Into<String>) -> Self {
        Self::Scan(msg.into())
    }

    pub fn unsupported_format(msg: impl Into<String>) -> Self {
        Self::UnsupportedFormat(msg.into())
    }
}

/// Result type for Iceberg operations.
pub type Result<T> = std::result::Result<T, IcebergError>;

// Integration with core errors
impl From<IcebergError> for fluree_db_core::error::Error {
    fn from(err: IcebergError) -> Self {
        match &err {
            IcebergError::TableNotFound(msg) => fluree_db_core::error::Error::not_found(msg),
            IcebergError::Storage(msg) => fluree_db_core::error::Error::storage(msg),
            // Auth and other errors map to generic "other" since core doesn't have unauthorized
            _ => fluree_db_core::error::Error::other(err.to_string()),
        }
    }
}

// Integration with tabular errors
impl From<fluree_db_tabular::TabularError> for IcebergError {
    fn from(err: fluree_db_tabular::TabularError) -> Self {
        match err {
            fluree_db_tabular::TabularError::Schema(msg) => IcebergError::Scan(msg),
        }
    }
}

// Integration with reqwest errors
impl From<reqwest::Error> for IcebergError {
    fn from(err: reqwest::Error) -> Self {
        if err.is_timeout() {
            IcebergError::Http(format!("Request timeout: {err}"))
        } else if err.is_connect() {
            IcebergError::Http(format!("Connection error: {err}"))
        } else {
            IcebergError::Http(format!("HTTP error: {err}"))
        }
    }
}
