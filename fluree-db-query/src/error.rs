//! Error types for query execution

use crate::binding::BatchError;
use crate::eval::{ArithmeticError, ComparisonError};
use thiserror::Error;

/// Query execution errors
#[derive(Error, Debug)]
pub enum QueryError {
    /// Error from fluree-db-core
    #[error("Core error: {0}")]
    Core(#[from] fluree_db_core::Error),

    /// Batch construction error
    #[error("Batch error: {0}")]
    Batch(#[from] BatchError),

    /// R2RML materialization error
    #[error("R2RML error: {0}")]
    R2rml(#[from] fluree_db_r2rml::R2rmlError),

    /// Operator not opened
    #[error("Operator not opened - call open() before next_batch()")]
    OperatorNotOpened,

    /// Operator already opened
    #[error("Operator already opened")]
    OperatorAlreadyOpened,

    /// Operator is closed
    #[error("Operator is closed")]
    OperatorClosed,

    /// Variable not found
    #[error("Variable not found: {0}")]
    VariableNotFound(String),

    /// Index selection failed
    #[error("No suitable index for query pattern")]
    NoSuitableIndex,

    /// Invalid query
    #[error("Invalid query: {0}")]
    InvalidQuery(String),

    /// Invalid filter expression
    #[error("Invalid filter: {0}")]
    InvalidFilter(String),

    /// Invalid expression (function/BIND evaluation error)
    #[error("Invalid expression: {0}")]
    InvalidExpression(String),

    /// Dictionary lookup or encoded-value decode failed during query execution
    #[error("Dictionary lookup failed: {0}")]
    DictionaryLookup(String),

    /// Resource limit exceeded
    #[error("Resource limit exceeded: {0}")]
    ResourceLimit(String),

    /// Fuel limit exceeded
    #[error(transparent)]
    FuelLimitExceeded(#[from] fluree_db_core::FuelExceededError),

    /// Internal error (should not happen in normal operation)
    #[error("Internal error: {0}")]
    Internal(String),

    /// Policy evaluation error
    #[error("Policy error: {0}")]
    Policy(String),

    /// Query mode not yet supported with binary indexes
    #[error("Unsupported mode: {0}")]
    UnsupportedMode(String),

    /// Requested time range not covered by binary index
    #[error("Time range not covered: requested t={requested_t} but base_t={base_t}")]
    TimeRangeNotCovered { requested_t: i64, base_t: i64 },

    /// Arithmetic error during expression evaluation
    #[error("Arithmetic error: {0}")]
    Arithmetic(#[from] ArithmeticError),

    /// Comparison error during expression evaluation
    #[error("Comparison error: {0}")]
    Comparison(#[from] ComparisonError),
}

impl QueryError {
    /// Create a dictionary lookup failure with debug context.
    pub fn dictionary_lookup(msg: impl Into<String>) -> Self {
        Self::DictionaryLookup(msg.into())
    }

    /// Returns true when an expression error should degrade to false/unbound
    /// under normal SPARQL evaluation instead of aborting the query.
    pub fn can_demote_in_expression(&self) -> bool {
        matches!(
            self,
            Self::InvalidFilter(_)
                | Self::InvalidExpression(_)
                | Self::Arithmetic(_)
                | Self::Comparison(_)
        )
    }

    /// Create an execution error (runtime configuration/environment issue).
    pub fn execution(msg: impl Into<String>) -> Self {
        Self::Internal(msg.into())
    }

    /// Convert an `io::Error` to a `QueryError`, preserving fuel-exhaustion
    /// errors (which `BinaryGraphView`/`BinaryCursor` smuggle through as
    /// `io::Error::other(FuelExceededError)`).
    pub fn from_io(context: &str, err: std::io::Error) -> Self {
        if let Some(fe) = err
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<fluree_db_core::FuelExceededError>())
        {
            return Self::FuelLimitExceeded(fe.clone());
        }
        Self::Internal(format!("{context}: {err}"))
    }
}

/// Result type for query operations
pub type Result<T> = std::result::Result<T, QueryError>;

#[cfg(test)]
mod tests {
    use super::QueryError;

    #[test]
    fn can_demote_expression_errors_only() {
        assert!(QueryError::InvalidFilter("bad regex".into()).can_demote_in_expression());
        assert!(QueryError::InvalidExpression("bad bind".into()).can_demote_in_expression());
        assert!(
            !QueryError::dictionary_lookup("missing string id".to_string())
                .can_demote_in_expression()
        );
        assert!(!QueryError::Internal("runtime failure".into()).can_demote_in_expression());
    }
}
