//! Error types for the query peer

use thiserror::Error;

/// Errors that can occur in the peer
#[derive(Debug, Error)]
pub enum PeerError {
    /// Configuration validation failed
    #[error("Configuration error: {0}")]
    Config(String),

    /// SSE connection or protocol error
    #[error("SSE error: {0}")]
    Sse(#[from] SseError),

    /// IO error (file operations, etc.)
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// Errors specific to SSE client operations
#[derive(Debug, Error)]
pub enum SseError {
    /// HTTP request failed
    #[error("HTTP request failed: {0}")]
    Request(#[from] reqwest::Error),

    /// HTTP response status error
    #[error("HTTP error: {0}")]
    HttpStatus(reqwest::StatusCode),

    /// Failed to parse SSE event data
    #[error("Failed to parse event data: {0}")]
    Parse(#[from] serde_json::Error),

    /// Unknown record type in event
    #[error("Unknown record type: {0}")]
    UnknownRecordType(String),

    /// Token loading error
    #[error("Failed to load token: {0}")]
    TokenLoad(std::io::Error),
}

impl SseError {
    /// Check if this is a fatal error that should not be retried
    ///
    /// Fatal errors include:
    /// - 401 Unauthorized (token is invalid/expired)
    /// - 403 Forbidden (not authorized)
    ///
    /// Transient errors (network issues, 5xx) should be retried with backoff.
    pub fn is_fatal(&self) -> bool {
        match self {
            SseError::HttpStatus(status) => {
                // 401 and 403 are fatal - retrying won't help without fixing the token
                status.as_u16() == 401 || status.as_u16() == 403
            }
            SseError::TokenLoad(_) => {
                // Can't load token - fatal until config is fixed
                true
            }
            _ => false,
        }
    }
}
