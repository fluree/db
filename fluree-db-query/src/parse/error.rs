//! Parse error types for JSON-LD query parsing

use thiserror::Error;

/// Errors that can occur during query parsing
#[derive(Error, Debug)]
pub enum ParseError {
    /// Required field is missing from the query
    #[error("Missing required field: {0}")]
    MissingField(&'static str),

    /// Invalid @context specification
    #[error("Invalid @context: {0}")]
    InvalidContext(String),

    /// Invalid variable syntax (must start with '?')
    #[error("Invalid variable syntax: '{0}' (must start with '?')")]
    InvalidVariable(String),

    /// Invalid where clause format
    #[error("Invalid where clause: {0}")]
    InvalidWhere(String),

    /// Invalid construct clause format
    #[error("Invalid construct clause: {0}")]
    InvalidConstruct(String),

    /// Invalid filter expression
    #[error("Invalid filter expression: {0}")]
    InvalidFilter(String),

    /// IRI could not be encoded (namespace not registered)
    #[error("IRI encoding failed for '{0}': namespace not registered")]
    UnknownNamespace(String),

    /// Invalid select clause format
    #[error("Invalid select clause: {0}")]
    InvalidSelect(String),

    // ========================================================================
    // Query modifier validation errors
    // ========================================================================
    /// Unknown aggregate function
    #[error("Unknown aggregate function: {0}")]
    UnknownAggregate(String),

    /// Invalid sort direction
    #[error("Invalid sort direction: '{0}' (expected 'asc' or 'desc')")]
    InvalidSortDirection(String),

    /// Invalid groupBy clause
    #[error("groupBy must be an array of variable strings")]
    InvalidGroupBy,

    /// Invalid orderBy clause
    #[error("orderBy must be an array of objects with 'var' field")]
    InvalidOrderBy,

    /// Invalid limit value
    #[error("limit must be a non-negative integer")]
    InvalidLimit,

    /// Invalid offset value
    #[error("offset must be a non-negative integer")]
    InvalidOffset,

    /// Invalid query option
    #[error("Invalid query option: {0}")]
    InvalidOption(String),

    /// Type coercion error (incompatible @value/@type combination)
    /// e.g., {"@value": 3, "@type": "xsd:string"} - number cannot be coerced to string
    #[error("Type coercion error: {0}")]
    TypeCoercion(String),

    /// JSON parsing error
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// JSON-LD processing error
    #[error("JSON-LD error: {0}")]
    JsonLd(String),
}

impl From<fluree_graph_json_ld::JsonLdError> for ParseError {
    fn from(err: fluree_graph_json_ld::JsonLdError) -> Self {
        ParseError::JsonLd(err.to_string())
    }
}

/// Result type for parse operations
pub type Result<T> = std::result::Result<T, ParseError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = ParseError::MissingField("select");
        assert_eq!(err.to_string(), "Missing required field: select");

        let err = ParseError::InvalidVariable("name".to_string());
        assert_eq!(
            err.to_string(),
            "Invalid variable syntax: 'name' (must start with '?')"
        );
    }
}
