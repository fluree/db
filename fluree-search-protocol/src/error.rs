//! Search error types.

use serde::{Deserialize, Serialize};

/// Search error response.
///
/// Returned by the search service when a request cannot be completed.
/// The error includes a structured code for programmatic handling
/// and a human-readable message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchError {
    /// Protocol version.
    pub protocol_version: String,

    /// Request ID (echoed from request if provided).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,

    /// Error details.
    pub error: ErrorDetail,
}

/// Structured error details.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorDetail {
    /// Machine-readable error code.
    pub code: ErrorCode,

    /// Human-readable error message.
    pub message: String,

    /// Whether the request can be retried.
    #[serde(default)]
    pub retryable: bool,

    /// Additional error context (optional).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
}

/// Error codes for search operations.
///
/// These codes provide machine-readable error classification
/// for programmatic error handling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCode {
    /// Graph source not found.
    GraphSourceNotFound,

    /// Invalid request format or parameters.
    InvalidRequest,

    /// Protocol version not supported.
    UnsupportedProtocolVersion,

    /// General timeout (operation took too long).
    Timeout,

    /// Sync operation timed out waiting for index head.
    SyncTimeout,

    /// No snapshot exists for the requested `as_of_t`.
    NoSnapshotForAsOfT,

    /// Index has not been built yet.
    IndexNotBuilt,

    /// Authentication required but not provided.
    Unauthorized,

    /// Authentication provided but insufficient permissions.
    Forbidden,

    /// Internal server error.
    Internal,
}

impl SearchError {
    /// Create a new search error.
    pub fn new(
        protocol_version: impl Into<String>,
        request_id: Option<String>,
        code: ErrorCode,
        message: impl Into<String>,
    ) -> Self {
        Self {
            protocol_version: protocol_version.into(),
            request_id,
            error: ErrorDetail {
                code,
                message: message.into(),
                retryable: code.is_retryable(),
                details: None,
            },
        }
    }

    /// Add additional details to the error.
    pub fn with_details(mut self, details: serde_json::Value) -> Self {
        self.error.details = Some(details);
        self
    }
}

impl ErrorCode {
    /// Returns whether errors with this code are typically retryable.
    pub fn is_retryable(self) -> bool {
        matches!(
            self,
            ErrorCode::Timeout | ErrorCode::SyncTimeout | ErrorCode::Internal
        )
    }

    /// Returns the HTTP status code typically associated with this error.
    pub fn http_status(self) -> u16 {
        match self {
            ErrorCode::GraphSourceNotFound => 404,
            ErrorCode::InvalidRequest => 400,
            ErrorCode::UnsupportedProtocolVersion => 400,
            ErrorCode::Timeout => 504,
            ErrorCode::SyncTimeout => 504,
            ErrorCode::NoSnapshotForAsOfT => 404,
            ErrorCode::IndexNotBuilt => 404,
            ErrorCode::Unauthorized => 401,
            ErrorCode::Forbidden => 403,
            ErrorCode::Internal => 500,
        }
    }
}

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ErrorCode::GraphSourceNotFound => write!(f, "GRAPH_SOURCE_NOT_FOUND"),
            ErrorCode::InvalidRequest => write!(f, "INVALID_REQUEST"),
            ErrorCode::UnsupportedProtocolVersion => write!(f, "UNSUPPORTED_PROTOCOL_VERSION"),
            ErrorCode::Timeout => write!(f, "TIMEOUT"),
            ErrorCode::SyncTimeout => write!(f, "SYNC_TIMEOUT"),
            ErrorCode::NoSnapshotForAsOfT => write!(f, "NO_SNAPSHOT_FOR_AS_OF_T"),
            ErrorCode::IndexNotBuilt => write!(f, "INDEX_NOT_BUILT"),
            ErrorCode::Unauthorized => write!(f, "UNAUTHORIZED"),
            ErrorCode::Forbidden => write!(f, "FORBIDDEN"),
            ErrorCode::Internal => write!(f, "INTERNAL"),
        }
    }
}

impl std::error::Error for SearchError {}

impl std::fmt::Display for SearchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.error.code, self.error.message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_serialization() {
        let error = SearchError::new(
            "1.0",
            Some("req-123".to_string()),
            ErrorCode::GraphSourceNotFound,
            "Graph source 'test:main' not found",
        );

        let json = serde_json::to_string_pretty(&error).unwrap();
        assert!(json.contains("GRAPH_SOURCE_NOT_FOUND"));
        assert!(json.contains("Graph source"));

        let parsed: SearchError = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.error.code, ErrorCode::GraphSourceNotFound);
        assert!(!parsed.error.retryable);
    }

    #[test]
    fn test_error_with_details() {
        let error = SearchError::new("1.0", None, ErrorCode::InvalidRequest, "Invalid limit")
            .with_details(serde_json::json!({
                "field": "limit",
                "value": -1,
                "constraint": "must be positive"
            }));

        let json = serde_json::to_string(&error).unwrap();
        assert!(json.contains("must be positive"));
    }

    #[test]
    fn test_retryable_errors() {
        assert!(ErrorCode::Timeout.is_retryable());
        assert!(ErrorCode::SyncTimeout.is_retryable());
        assert!(ErrorCode::Internal.is_retryable());

        assert!(!ErrorCode::GraphSourceNotFound.is_retryable());
        assert!(!ErrorCode::InvalidRequest.is_retryable());
        assert!(!ErrorCode::Unauthorized.is_retryable());
    }

    #[test]
    fn test_http_status_codes() {
        assert_eq!(ErrorCode::GraphSourceNotFound.http_status(), 404);
        assert_eq!(ErrorCode::InvalidRequest.http_status(), 400);
        assert_eq!(ErrorCode::Timeout.http_status(), 504);
        assert_eq!(ErrorCode::Unauthorized.http_status(), 401);
        assert_eq!(ErrorCode::Internal.http_status(), 500);
    }

    #[test]
    fn test_error_code_display() {
        assert_eq!(
            ErrorCode::GraphSourceNotFound.to_string(),
            "GRAPH_SOURCE_NOT_FOUND"
        );
        assert_eq!(
            ErrorCode::NoSnapshotForAsOfT.to_string(),
            "NO_SNAPSHOT_FOR_AS_OF_T"
        );
    }
}
