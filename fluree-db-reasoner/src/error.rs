//! OWL2-RL reasoning error types

use thiserror::Error;

/// Reasoning errors
#[derive(Debug, Error)]
pub enum ReasonerError {
    /// Core database error
    #[error("Core error: {0}")]
    Core(#[from] fluree_db_core::Error),

    /// Reasoning internal error
    #[error("Reasoner error: {0}")]
    Internal(String),

    /// Reasoning was capped by budget constraints
    #[error("Reasoning capped: {reason}")]
    Capped { reason: String },

    /// Invalid ontology configuration
    #[error("Invalid ontology: {0}")]
    InvalidOntology(String),
}

/// Result type for reasoning operations
pub type Result<T> = std::result::Result<T, ReasonerError>;
