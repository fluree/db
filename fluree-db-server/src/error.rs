//! Server error types with HTTP status code mapping

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use fluree_db_api::{ApiError, SparqlUpdateLowerError};
use fluree_db_nameservice::NameServiceError;
use fluree_db_query::parse::ParseError;
use serde::Serialize;
use thiserror::Error;

/// Server error type that wraps API errors and provides HTTP status mapping
#[derive(Error, Debug)]
pub enum ServerError {
    /// API layer error
    #[error("{0}")]
    Api(#[from] ApiError),

    /// Missing ledger alias in request
    #[error("Missing ledger alias: provide via path, header, or request body")]
    MissingLedger,

    /// JSON parsing error
    #[error("Invalid JSON: {0}")]
    Json(#[from] serde_json::Error),

    /// Generic bad request error
    #[error("Bad request: {0}")]
    BadRequest(String),

    /// Feature not implemented
    #[error("Not implemented: {0}")]
    NotImplemented(String),

    /// Invalid header value
    #[error("Invalid header value: {0}")]
    InvalidHeader(String),

    /// Unauthorized (Bearer token required/invalid)
    #[error("{0}")]
    Unauthorized(String),

    /// Not Found (404)
    #[error("{0}")]
    NotFound(String),

    /// Not Acceptable (406) - content negotiation failure
    #[error("{0}")]
    NotAcceptable(String),

    /// SPARQL UPDATE lowering error
    #[error("SPARQL UPDATE error: {0}")]
    SparqlUpdateLower(#[from] SparqlUpdateLowerError),
}

impl ServerError {
    /// Map error to error type IRI (compact form)
    pub fn error_type(&self) -> &'static str {
        use fluree_vocab::errors;

        match self {
            // API errors (explicit HTTP status passthrough)
            //
            // Map common statuses to stable error types so clients can branch on `@type`.
            ServerError::Api(ApiError::Http { status, .. }) => match status {
                401 => errors::UNAUTHORIZED,
                403 => errors::ACCESS_DENIED,
                409 => errors::COMMIT_CONFLICT,
                422 => errors::INVALID_TRANSACTION,
                _ => errors::INTERNAL,
            },

            // Not Found
            ServerError::Api(ApiError::NotFound(msg)) => {
                // Distinguish graph source not found from ledger not found
                if msg.contains("Graph source") || msg.contains("graph source") {
                    errors::GRAPH_SOURCE_NOT_FOUND
                } else {
                    errors::LEDGER_NOT_FOUND
                }
            }

            // Ledger management
            ServerError::Api(ApiError::LedgerExists(_)) => errors::LEDGER_EXISTS,

            // Index operations
            ServerError::Api(ApiError::IndexTimeout(_)) => errors::INDEX_TIMEOUT,
            ServerError::Api(ApiError::IndexingDisabled) => errors::INDEXING_DISABLED,
            ServerError::Api(ApiError::ReindexConflict { .. }) => errors::REINDEX_CONFLICT,

            // Parsing errors
            ServerError::Api(ApiError::Parse(ParseError::TypeCoercion(_))) => errors::TYPE_COERCION,
            ServerError::Api(ApiError::Parse(_)) => errors::JSONLD_PARSE,
            ServerError::Api(ApiError::Turtle(_)) => errors::TURTLE_PARSE,
            ServerError::Api(ApiError::Sparql { .. }) => errors::SPARQL_PARSE,
            ServerError::Api(ApiError::SparqlLower(_)) => errors::SPARQL_LOWER,
            ServerError::Json(_) => errors::JSON_PARSE,

            // Query/Transaction errors
            ServerError::Api(ApiError::Query(_)) => errors::INVALID_QUERY,
            ServerError::Api(ApiError::Batch(_)) => errors::INVALID_QUERY,
            ServerError::Api(ApiError::Transact(_)) => errors::INVALID_TRANSACTION,

            // API-level errors
            ServerError::MissingLedger => errors::MISSING_LEDGER,
            ServerError::BadRequest(_) => errors::BAD_REQUEST,
            ServerError::InvalidHeader(_) => errors::INVALID_HEADER,
            ServerError::NotImplemented(_) => errors::NOT_IMPLEMENTED,
            ServerError::Unauthorized(_) => errors::UNAUTHORIZED,
            ServerError::NotFound(_) => errors::NOT_FOUND,
            ServerError::NotAcceptable(_) => errors::NOT_ACCEPTABLE,
            ServerError::SparqlUpdateLower(_) => errors::SPARQL_LOWER,

            // Auth/Policy (requires credential feature)
            #[cfg(feature = "credential")]
            ServerError::Api(ApiError::Credential(_)) => errors::INVALID_CREDENTIAL,

            // System errors
            ServerError::Api(ApiError::Connection(_)) => errors::CONNECTION,
            ServerError::Api(ApiError::NameService(_)) => errors::NAMESERVICE,
            ServerError::Api(ApiError::Core(_)) => errors::INTERNAL,
            ServerError::Api(ApiError::Ledger(_)) => errors::INTERNAL,
            ServerError::Api(ApiError::Novelty(_)) => errors::INTERNAL,
            ServerError::Api(ApiError::Bm25Builder(_)) => errors::BM25,
            ServerError::Api(ApiError::Bm25Serialize(_)) => errors::BM25,
            ServerError::Api(ApiError::Internal(_)) => errors::INTERNAL,
            ServerError::Api(ApiError::Drop(_)) => errors::INTERNAL,
            ServerError::Api(ApiError::Json(_)) => errors::INTERNAL,
            ServerError::Api(ApiError::Config(_)) => errors::CONFIG,
            ServerError::Api(ApiError::Format(_)) => errors::FORMAT,

            // Catch any new ApiError variants as internal
            #[allow(unreachable_patterns)]
            ServerError::Api(_) => errors::INTERNAL,
        }
    }

    /// Map error to HTTP status code
    pub fn status_code(&self) -> StatusCode {
        match self {
            // Explicit HTTP status passthrough (e.g. credentialed tx/query tracked errors)
            ServerError::Api(ApiError::Http { status, .. }) => {
                StatusCode::from_u16(*status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
            }

            // 404 - Not Found
            ServerError::Api(ApiError::NotFound(_)) => StatusCode::NOT_FOUND,

            // 409 - Conflict
            ServerError::Api(ApiError::LedgerExists(_)) => StatusCode::CONFLICT,

            // 400 - Bad Request (client errors)
            ServerError::Api(ApiError::Parse(_)) => StatusCode::BAD_REQUEST,
            ServerError::Api(ApiError::Query(_)) => StatusCode::BAD_REQUEST,
            ServerError::Api(ApiError::Batch(_)) => StatusCode::BAD_REQUEST,
            ServerError::Api(ApiError::Transact(_)) => StatusCode::BAD_REQUEST,
            ServerError::Api(ApiError::Turtle(_)) => StatusCode::BAD_REQUEST,
            ServerError::Api(ApiError::Sparql { .. }) => StatusCode::BAD_REQUEST,
            ServerError::Api(ApiError::SparqlLower(_)) => StatusCode::BAD_REQUEST,
            ServerError::Api(ApiError::Config(_)) => StatusCode::BAD_REQUEST,
            ServerError::Api(ApiError::Format(_)) => StatusCode::BAD_REQUEST,
            ServerError::MissingLedger => StatusCode::BAD_REQUEST,
            ServerError::Json(_) => StatusCode::BAD_REQUEST,
            ServerError::BadRequest(_) => StatusCode::BAD_REQUEST,
            ServerError::InvalidHeader(_) => StatusCode::BAD_REQUEST,
            ServerError::SparqlUpdateLower(_) => StatusCode::BAD_REQUEST,

            // 501 - Not Implemented
            ServerError::NotImplemented(_) => StatusCode::NOT_IMPLEMENTED,

            // 401 - Unauthorized
            ServerError::Unauthorized(_) => StatusCode::UNAUTHORIZED,

            // 404 - Not Found (explicit, not from ApiError)
            ServerError::NotFound(_) => StatusCode::NOT_FOUND,

            // 406 - Not Acceptable (content negotiation failure)
            ServerError::NotAcceptable(_) => StatusCode::NOT_ACCEPTABLE,
            #[cfg(feature = "credential")]
            ServerError::Api(ApiError::Credential(_)) => StatusCode::UNAUTHORIZED,

            // 500 - Internal Server Error (server-side errors and catch-all)
            ServerError::Api(ApiError::Connection(_)) => StatusCode::INTERNAL_SERVER_ERROR,
            ServerError::Api(ApiError::NameService(_)) => StatusCode::INTERNAL_SERVER_ERROR,
            ServerError::Api(ApiError::Core(_)) => StatusCode::INTERNAL_SERVER_ERROR,
            ServerError::Api(ApiError::Ledger(_)) => StatusCode::INTERNAL_SERVER_ERROR,
            ServerError::Api(ApiError::Novelty(_)) => StatusCode::INTERNAL_SERVER_ERROR,
            ServerError::Api(ApiError::Bm25Builder(_)) => StatusCode::INTERNAL_SERVER_ERROR,
            ServerError::Api(ApiError::Bm25Serialize(_)) => StatusCode::INTERNAL_SERVER_ERROR,
            ServerError::Api(ApiError::Internal(_)) => StatusCode::INTERNAL_SERVER_ERROR,
            ServerError::Api(ApiError::Drop(_)) => StatusCode::INTERNAL_SERVER_ERROR,
            ServerError::Api(ApiError::Json(_)) => StatusCode::INTERNAL_SERVER_ERROR,

            // Catch any new ApiError variants as 500
            #[allow(unreachable_patterns)]
            ServerError::Api(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// Create a not implemented error
    pub fn not_implemented(feature: impl Into<String>) -> Self {
        ServerError::NotImplemented(feature.into())
    }

    /// Create a bad request error
    pub fn bad_request(msg: impl Into<String>) -> Self {
        ServerError::BadRequest(msg.into())
    }

    /// Create an invalid header error
    pub fn invalid_header(msg: impl Into<String>) -> Self {
        ServerError::InvalidHeader(msg.into())
    }

    /// Create an internal error (wraps ApiError::Internal)
    pub fn internal(msg: impl Into<String>) -> Self {
        ServerError::Api(ApiError::Internal(msg.into()))
    }

    /// Create an unauthorized error (401)
    pub fn unauthorized(msg: impl Into<String>) -> Self {
        ServerError::Unauthorized(msg.into())
    }

    /// Create a not found error (404)
    pub fn not_found(msg: impl Into<String>) -> Self {
        ServerError::NotFound(msg.into())
    }

    /// Create a not acceptable error (406)
    pub fn not_acceptable(msg: impl Into<String>) -> Self {
        ServerError::NotAcceptable(msg.into())
    }
}

impl From<NameServiceError> for ServerError {
    fn from(e: NameServiceError) -> Self {
        // NameServiceError variants map to ApiError which maps to ServerError
        ServerError::Api(ApiError::NameService(e))
    }
}

/// JSON error response body
#[derive(Serialize)]
pub struct ErrorResponse {
    /// Error message
    pub error: String,
    /// HTTP status code
    pub status: u16,
    /// Error type (compact IRI, e.g., "err:db/InvalidQuery")
    #[serde(rename = "@type")]
    pub error_type: String,
    /// Optional cause chain for nested errors
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cause: Option<Box<ErrorResponse>>,
}

impl IntoResponse for ServerError {
    fn into_response(self) -> Response {
        let status = self.status_code();
        let error_type = self.error_type();

        // Extract cause chain from underlying ApiError
        let cause = extract_cause(&self);

        let body = ErrorResponse {
            error: self.to_string(),
            status: status.as_u16(),
            error_type: error_type.to_string(),
            cause,
        };

        let json = serde_json::to_string(&body).unwrap_or_else(|_| {
            format!(
                r#"{{"error":"{}","status":{},"@type":"{}"}}"#,
                self,
                status.as_u16(),
                error_type
            )
        });

        (status, [("content-type", "application/json")], json).into_response()
    }
}

/// Extract cause chain from error (only for high-value cases)
fn extract_cause(error: &ServerError) -> Option<Box<ErrorResponse>> {
    use fluree_vocab::errors;

    match error {
        // High-value case 1: Transaction errors wrapping JSON parse errors
        ServerError::Api(ApiError::Transact(transact_err)) => {
            // Check if it's wrapping a JSON error
            if let Some(source) = std::error::Error::source(transact_err) {
                if let Some(json_err) = source.downcast_ref::<serde_json::Error>() {
                    return Some(Box::new(ErrorResponse {
                        error: json_err.to_string(),
                        status: 400,
                        error_type: errors::JSON_PARSE.to_string(),
                        cause: None,
                    }));
                }
                // Check for Query errors (WHERE clause failures)
                if let Some(query_err) = source.downcast_ref::<fluree_db_query::QueryError>() {
                    return Some(Box::new(ErrorResponse {
                        error: query_err.to_string(),
                        status: 400,
                        error_type: errors::QUERY_EXECUTION.to_string(),
                        cause: None,
                    }));
                }
            }
            None
        }

        // High-value case 2: Query errors wrapping storage failures
        ServerError::Api(ApiError::Query(query_err)) => {
            if let Some(source) = std::error::Error::source(query_err) {
                if let Some(core_err) = source.downcast_ref::<fluree_db_core::Error>() {
                    return Some(Box::new(ErrorResponse {
                        error: core_err.to_string(),
                        status: 500,
                        error_type: errors::STORAGE_READ.to_string(),
                        cause: None,
                    }));
                }
            }
            None
        }

        // High-value case 3: JSON parsing at API level
        ServerError::Json(json_err) => {
            // Already at the leaf, but show it as a structured error
            Some(Box::new(ErrorResponse {
                error: format!("at line {}, column {}", json_err.line(), json_err.column()),
                status: 400,
                error_type: errors::JSON_PARSE.to_string(),
                cause: None,
            }))
        }

        _ => None,
    }
}

/// Result type alias for server operations
pub type Result<T> = std::result::Result<T, ServerError>;
