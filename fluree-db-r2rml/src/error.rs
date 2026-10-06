//! R2RML error types

use thiserror::Error;

/// R2RML-specific errors
#[derive(Debug, Error)]
pub enum R2rmlError {
    /// Error parsing R2RML mapping document
    #[error("Parse error: {0}")]
    Parse(String),

    /// Error extracting mapping from graph
    #[error("Extraction error: {0}")]
    Extraction(String),

    /// Missing required property in mapping
    #[error("Missing required property: {0}")]
    MissingProperty(String),

    /// Invalid property value
    #[error("Invalid value for {property}: {message}")]
    InvalidValue { property: String, message: String },

    /// Invalid template syntax
    #[error("Invalid template: {0}")]
    InvalidTemplate(String),

    /// Reference to non-existent TriplesMap
    #[error("Unknown TriplesMap: {0}")]
    UnknownTriplesMap(String),

    /// Two or more rr:TriplesMap definitions resolve to the same subject IRI
    #[error("Duplicate TriplesMap IRI: {0}")]
    DuplicateTriplesMap(String),

    /// Column not found in table
    #[error("Column not found: {column} in table {table}")]
    ColumnNotFound { column: String, table: String },

    /// Term materialization error
    #[error("Materialization error: {0}")]
    Materialization(String),

    /// Unsupported feature
    #[error("Unsupported feature: {0}")]
    Unsupported(String),

    /// A materialize (twin) build was refused because the source snapshots read
    /// during the build cannot be trusted — a table moved off its pinned
    /// `metadata_location` mid-build, or loadTable caching (hence snapshot pinning)
    /// is disabled. (id=3717339918: replaces a stringly `Result<(), String>`.)
    #[error("Build snapshot integrity: {0}")]
    BuildSnapshotIntegrity(String),
}

/// Result type for R2RML operations
pub type R2rmlResult<T> = Result<T, R2rmlError>;
