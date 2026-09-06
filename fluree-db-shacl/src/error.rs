//! SHACL error types

use fluree_db_core::Sid;
use thiserror::Error;

/// Result type for SHACL operations
pub type Result<T> = std::result::Result<T, ShaclError>;

/// SHACL validation and compilation errors
#[derive(Debug, Error)]
pub enum ShaclError {
    /// Shape compilation error
    #[error("Failed to compile shape {shape_id}: {message}")]
    CompilationError { shape_id: Sid, message: String },

    /// Invalid constraint specification
    #[error("Invalid constraint on shape {shape_id}: {message}")]
    InvalidConstraint { shape_id: Sid, message: String },

    /// Invalid regex pattern in sh:pattern
    #[error("Invalid regex pattern '{pattern}': {message}")]
    InvalidPattern { pattern: String, message: String },

    /// Query execution error during validation
    #[error("Query error during validation: {0}")]
    QueryError(#[from] fluree_db_query::QueryError),

    /// Core database error
    #[error("Database error: {0}")]
    CoreError(#[from] fluree_db_core::Error),

    /// Invalid or unsupported `sh:sparql` constraint query — raised as a
    /// validation *failure* (per SHACL, distinct from a violation) when the
    /// owning shape fires on a focus node.
    #[error("Invalid sh:sparql constraint {constraint}: {message}")]
    SparqlConstraint { constraint: Sid, message: String },

    /// Shape references unknown shape
    #[error("Shape {referrer} references unknown shape {referenced}")]
    UnknownShapeReference { referrer: Sid, referenced: Sid },

    /// Circular shape reference detected
    #[error("Circular shape reference detected involving {shape_id}")]
    CircularReference { shape_id: Sid },
}
