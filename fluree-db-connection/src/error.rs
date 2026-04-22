//! Error types for fluree-db-connection

use thiserror::Error;

/// Result type alias using ConnectionError
pub type Result<T> = std::result::Result<T, ConnectionError>;

/// Connection-related errors
#[derive(Error, Debug)]
pub enum ConnectionError {
    /// Invalid configuration
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    /// Unsupported component type in configuration
    #[error("Unsupported component type: {type_iri}")]
    UnsupportedComponent { type_iri: String },

    /// JSON-LD processing error
    #[error("JSON-LD error: {0}")]
    JsonLd(String),

    /// Storage backend error
    #[error("Storage error: {0}")]
    Storage(String),

    /// Resource not found
    #[error("Not found: {0}")]
    NotFound(String),

    /// Core library error
    #[error(transparent)]
    Core(#[from] fluree_db_core::Error),
}

impl From<fluree_graph_json_ld::JsonLdError> for ConnectionError {
    fn from(err: fluree_graph_json_ld::JsonLdError) -> Self {
        ConnectionError::JsonLd(err.to_string())
    }
}

impl ConnectionError {
    /// Create an invalid config error
    pub fn invalid_config(msg: impl Into<String>) -> Self {
        ConnectionError::InvalidConfig(msg.into())
    }

    /// Create an unsupported component error
    pub fn unsupported_component(type_iri: impl Into<String>) -> Self {
        ConnectionError::UnsupportedComponent {
            type_iri: type_iri.into(),
        }
    }

    /// Create a storage error
    pub fn storage(msg: impl Into<String>) -> Self {
        ConnectionError::Storage(msg.into())
    }

    /// Create a not found error
    pub fn not_found(msg: impl Into<String>) -> Self {
        ConnectionError::NotFound(msg.into())
    }
}
