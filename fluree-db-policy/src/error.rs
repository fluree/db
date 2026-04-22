//! Policy error types

use thiserror::Error;

/// Policy-related errors
#[derive(Debug, Error)]
pub enum PolicyError {
    /// Invalid policy query format
    #[error("Invalid policy query: {message}")]
    InvalidQuery { message: String },

    /// Invalid policy action
    #[error("Invalid policy action: {action}")]
    InvalidAction { action: String },

    /// Modify operation denied by policy
    #[error("{message}")]
    ModifyDenied { message: String },

    /// Policy parsing error
    #[error("Policy parse error: {message}")]
    ParseError { message: String },

    /// Internal policy error
    #[error("Policy error: {message}")]
    Internal { message: String },

    /// Error looking up subject classes
    #[error("Class lookup error: {message}")]
    ClassLookup { message: String },

    /// Error executing policy query
    #[error("Policy query execution error: {message}")]
    QueryExecution { message: String },
}

impl PolicyError {
    /// Create a modify denied error with custom message
    pub fn modify_denied(message: impl Into<String>) -> Self {
        Self::ModifyDenied {
            message: message.into(),
        }
    }

    /// Create an invalid query error
    pub fn invalid_query(message: impl Into<String>) -> Self {
        Self::InvalidQuery {
            message: message.into(),
        }
    }

    /// Create an invalid action error
    pub fn invalid_action(action: impl Into<String>) -> Self {
        Self::InvalidAction {
            action: action.into(),
        }
    }

    /// Create a parse error
    pub fn parse_error(message: impl Into<String>) -> Self {
        Self::ParseError {
            message: message.into(),
        }
    }

    /// Create an internal error
    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal {
            message: message.into(),
        }
    }
}

/// Result type alias for policy operations
pub type Result<T> = std::result::Result<T, PolicyError>;
