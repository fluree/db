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

    /// Novelty backpressure (503 + `Retry-After`): the ledger's in-memory
    /// novelty is at `reindex_max_bytes` (or the transaction would cross it)
    /// and the indexer must drain it before new commits are accepted.
    /// Carries the pipeline's message for submissions whose typed
    /// `ApiError::Transact` variant was flattened at the consensus boundary;
    /// errors that still carry the variant map via the `ApiError` arms.
    #[error("{0}")]
    NoveltyBackpressure(String),

    /// The transaction's own delta meets or exceeds `reindex_max_bytes`
    /// (413 + `err:db/NoveltyDeltaTooLarge`, no `Retry-After`): no amount
    /// of indexer draining can ever admit it, so telling the client to
    /// retry (the 503 shape above) would wedge a pipeline on the oversized
    /// record forever. Same consensus-boundary role as
    /// `NoveltyBackpressure`.
    #[error("{0}")]
    NoveltyDeltaTooLarge(String),
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
                // The HTTP body-size cap (`read_limited_body`). The other
                // 413 — an oversized novelty delta — never takes this arm:
                // it stays typed end-to-end (`ServerError::NoveltyDeltaTooLarge`
                // / the `Transact` variant below) precisely so the two 413s
                // carry distinct codes for clients to branch on.
                413 => errors::PAYLOAD_TOO_LARGE,
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
            ServerError::Api(ApiError::NoveltyDeferred { .. }) => errors::NOVELTY_DEFERRED,
            ServerError::Api(ApiError::MaterializePartial { tally, .. }) => {
                if tally.failed > 0 {
                    errors::MATERIALIZE_PARTIAL
                } else {
                    errors::NOVELTY_DEFERRED
                }
            }
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
            ServerError::Api(ApiError::Cypher { .. }) => errors::CYPHER_PARSE,
            ServerError::Api(ApiError::CypherLower(_)) => errors::CYPHER_LOWER,
            ServerError::Api(ApiError::CypherUpdateLower(_)) => errors::CYPHER_LOWER,
            ServerError::Json(_) => errors::JSON_PARSE,

            // Query/Transaction errors
            ServerError::Api(ApiError::AwaitTNotReached { .. }) => errors::READ_AFTER_WRITE_TIMEOUT,
            ServerError::Api(ApiError::Query(fluree_db_query::QueryError::Cancelled {
                ..
            })) => errors::QUERY_CANCELLED,

            // Storage-permission / fail-closed errors (403). Raised directly on
            // the preview path or wrapped from the query engine on the scan
            // path; both surface the same distinct `@type` so clients can branch.
            // These MUST precede the generic `ApiError::Query(_)` arm below.
            ServerError::Api(
                ApiError::StorageAccessDenied { .. }
                | ApiError::Query(fluree_db_query::QueryError::StorageAccessDenied { .. }),
            ) => errors::STORAGE_ACCESS_DENIED,
            ServerError::Api(
                ApiError::CatalogCredentialsNotVended { .. }
                | ApiError::Query(fluree_db_query::QueryError::CatalogCredentialsNotVended {
                    ..
                }),
            ) => errors::CATALOG_CREDENTIALS_NOT_VENDED,

            // Virtual-dataset (R2RML) unsupported-pattern refusal: a distinct
            // `@type` so Solo's browse UI can gate on the condition instead of
            // matching prose. Stays HTTP 400 (well-formed request, unsupported on
            // this source) — unlike the 403/507 distinct-status precedents — via
            // the generic `Query(_)` arm in `status_code()`. MUST precede the
            // generic `ApiError::Query(_)` arm below.
            ServerError::Api(ApiError::Query(
                fluree_db_query::QueryError::R2rmlUnsupportedPattern { .. },
            )) => errors::R2RML_UNSUPPORTED_PATTERN,

            ServerError::Api(ApiError::Query(_)) => errors::INVALID_QUERY,
            ServerError::Api(ApiError::Batch(_)) => errors::INVALID_QUERY,
            // Optimistic-concurrency conflicts: a distinct, retryable class so
            // clients can branch on `@type` (and the 409 status below).
            ServerError::Api(ApiError::Transact(
                fluree_db_api::TransactError::CommitConflict { .. }
                | fluree_db_api::TransactError::PublishLostRace { .. }
                | fluree_db_api::TransactError::NamespaceConflict(_),
            )) => errors::COMMIT_CONFLICT,
            // Oversized single delta: `delta >= max` can never succeed by
            // drain, so it must not carry the retryable code below. MUST
            // precede the drainable novelty arm.
            ServerError::Api(ApiError::Transact(
                fluree_db_api::TransactError::NoveltyWouldExceed {
                    delta_bytes,
                    max_bytes,
                    ..
                },
            )) if delta_bytes >= max_bytes => errors::NOVELTY_DELTA_TOO_LARGE,
            ServerError::NoveltyDeltaTooLarge(_) => errors::NOVELTY_DELTA_TOO_LARGE,
            // Novelty backpressure: retryable capacity pressure, not an
            // invalid transaction. Both variants carry the same machine code
            // (the message distinguishes at-max from would-exceed) so clients
            // branch on one `@type` for "the indexer needs to drain".
            ServerError::Api(ApiError::Transact(
                fluree_db_api::TransactError::NoveltyAtMax
                | fluree_db_api::TransactError::NoveltyWouldExceed { .. },
            ))
            | ServerError::NoveltyBackpressure(_) => errors::NOVELTY_AT_MAX,
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

            // Cross-ledger model dependency failure (502). The variant
            // is preserved in ApiError::CrossLedger so structured
            // callers can branch on the specific failure; the `@type`
            // surfaced here is the umbrella IRI.
            ServerError::Api(ApiError::CrossLedger(_)) => errors::CROSS_LEDGER,

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
            // Retryable backpressure, NOT a fault. The 503 belonged here, in
            // ServerError's mapping, which is what the HTTP layer consults — putting it
            // only on `ApiError::status_code()` left it dead and a deferral surfaced as
            // a 500 `err:system/InternalError`, telling operators a normal capacity
            // condition was an internal error.
            ServerError::Api(ApiError::NoveltyDeferred { .. }) => StatusCode::SERVICE_UNAVAILABLE,
            // A partial fan-out window splits by WHY it is incomplete. Deferral-only is
            // the same retryable capacity condition as above; a target that actually
            // failed is a fault and must not be dressed up as backpressure, or a
            // permanently broken target would 503 forever and read as "just busy".
            ServerError::Api(ApiError::MaterializePartial { tally, .. }) => {
                if tally.failed > 0 {
                    StatusCode::INTERNAL_SERVER_ERROR
                } else {
                    StatusCode::SERVICE_UNAVAILABLE
                }
            }
            ServerError::Api(ApiError::NotFound(_)) => StatusCode::NOT_FOUND,

            // 409 - Conflict
            ServerError::Api(ApiError::LedgerExists(_)) => StatusCode::CONFLICT,
            // Optimistic-concurrency / namespace-allocation conflicts are
            // retryable: 409 lets clients distinguish "retry" from a 400 "bad
            // request". (After server-side reconcile-and-retry these only reach
            // the client when the bounded retry budget is exhausted.)
            ServerError::Api(ApiError::Transact(
                fluree_db_api::TransactError::CommitConflict { .. }
                | fluree_db_api::TransactError::PublishLostRace { .. }
                | fluree_db_api::TransactError::NamespaceConflict(_),
            )) => StatusCode::CONFLICT,

            // 413 - the transaction's own delta meets or exceeds
            // `reindex_max_bytes`: no drain can ever admit it, so a 503
            // would wedge the client retrying a request that can never
            // work. MUST precede the drainable 503 arm below.
            ServerError::Api(ApiError::Transact(
                fluree_db_api::TransactError::NoveltyWouldExceed {
                    delta_bytes,
                    max_bytes,
                    ..
                },
            )) if delta_bytes >= max_bytes => StatusCode::PAYLOAD_TOO_LARGE,
            ServerError::NoveltyDeltaTooLarge(_) => StatusCode::PAYLOAD_TOO_LARGE,

            // 503 - novelty backpressure. The same retryable capacity class
            // as `NoveltyDeferred` above: only the indexer draining clears
            // it. A 400 here tells well-behaved clients (retry 5xx, treat
            // 4xx as permanent) to drop the write. MUST precede the generic
            // `ApiError::Transact(_)` arm below.
            ServerError::Api(ApiError::Transact(
                fluree_db_api::TransactError::NoveltyAtMax
                | fluree_db_api::TransactError::NoveltyWouldExceed { .. },
            ))
            | ServerError::NoveltyBackpressure(_) => StatusCode::SERVICE_UNAVAILABLE,

            // 403 - Forbidden (storage-permission / fail-closed). Raised
            // directly (preview path) or wrapped from the query engine (scan
            // path). MUST precede the generic `ApiError::Query(_)` arm below.
            ServerError::Api(
                ApiError::StorageAccessDenied { .. }
                | ApiError::CatalogCredentialsNotVended { .. }
                | ApiError::Query(
                    fluree_db_query::QueryError::StorageAccessDenied { .. }
                    | fluree_db_query::QueryError::CatalogCredentialsNotVended { .. },
                ),
            ) => StatusCode::FORBIDDEN,

            // 400 - Bad Request (client errors)
            ServerError::Api(ApiError::Parse(_)) => StatusCode::BAD_REQUEST,
            ServerError::Api(ApiError::Query(fluree_db_query::QueryError::Cancelled {
                ..
            })) => StatusCode::REQUEST_TIMEOUT,
            ServerError::Api(ApiError::Query(_)) => StatusCode::BAD_REQUEST,
            ServerError::Api(ApiError::Batch(_)) => StatusCode::BAD_REQUEST,
            ServerError::Api(ApiError::Transact(_)) => StatusCode::BAD_REQUEST,
            ServerError::Api(ApiError::Turtle(_)) => StatusCode::BAD_REQUEST,
            ServerError::Api(ApiError::Sparql { .. }) => StatusCode::BAD_REQUEST,
            ServerError::Api(ApiError::SparqlLower(_)) => StatusCode::BAD_REQUEST,
            ServerError::Api(ApiError::Cypher { .. }) => StatusCode::BAD_REQUEST,
            ServerError::Api(ApiError::CypherLower(_)) => StatusCode::BAD_REQUEST,
            ServerError::Api(ApiError::CypherUpdateLower(_)) => StatusCode::BAD_REQUEST,
            ServerError::Api(ApiError::Config(_)) => StatusCode::BAD_REQUEST,
            ServerError::Api(ApiError::Format(_)) => StatusCode::BAD_REQUEST,
            ServerError::Api(ApiError::AwaitTNotReached { .. }) => StatusCode::REQUEST_TIMEOUT,
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

            // 502 - Bad Gateway. Cross-ledger model dependency failure
            // is conceptually an upstream-dependency error, not an
            // internal panic — operators can distinguish "your data
            // ledger is broken" (500) from "the model ledger this
            // data ledger depends on is broken" (502). The wrapped
            // CrossLedgerError variant is preserved in the JSON body
            // so callers can branch on the specific failure.
            ServerError::Api(ApiError::CrossLedger(_)) => StatusCode::BAD_GATEWAY,

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

        let mut response = (status, [("content-type", "application/json")], json).into_response();
        // Every 503 this server emits is retryable capacity pressure
        // (novelty at max, materialization deferred, committer overloaded),
        // and RFC 9110 says a 503 should tell the client when to come back.
        //
        // The value is a conservative jittered constant, not the smallest
        // non-zero one: many HTTP clients honor `Retry-After` IN PREFERENCE
        // to their own exponential backoff, so a static 1s would pin every
        // blocked client at 1 rps of rejected requests during exactly the
        // window the server is capacity-stressed. Drain time isn't
        // observable at the error site — which argues for a conservative
        // constant, and the per-response jitter de-choruses clients that
        // were refused in the same instant.
        if status == StatusCode::SERVICE_UNAVAILABLE {
            debug_assert!(
                RETRYABLE_503_TYPES.contains(&error_type),
                "503 with unclassified @type {error_type}: every 503 this server \
                 emits must be retryable capacity pressure (Retry-After attaches \
                 unconditionally) — classify the new source in RETRYABLE_503_TYPES \
                 or map it to a different status"
            );
            let secs: u32 = rand::Rng::gen_range(
                &mut rand::thread_rng(),
                RETRY_AFTER_SECS_MIN..=RETRY_AFTER_SECS_MAX,
            );
            response.headers_mut().insert(
                axum::http::header::RETRY_AFTER,
                axum::http::HeaderValue::from(secs),
            );
        }
        response
    }
}

/// Bounds for the jittered `Retry-After` on 503 responses (uniform integer
/// seconds, inclusive). See the rationale at the attachment site in
/// [`ServerError::into_response`].
pub(crate) const RETRY_AFTER_SECS_MIN: u32 = 3;
pub(crate) const RETRY_AFTER_SECS_MAX: u32 = 8;

/// The `@type` codes a 503 response is allowed to carry.
///
/// `Retry-After` attaches to every 503 unconditionally, so "every 503 this
/// server emits is retryable capacity pressure" is a load-bearing invariant:
/// a non-retryable condition mapped to 503 would tell clients to hammer a
/// request that can never succeed. The `debug_assert` in `into_response`
/// fails any test that produces a 503 whose `@type` is not classified here,
/// forcing a conscious decision for each new 503 source.
///
/// - `NOVELTY_AT_MAX`: novelty backpressure (stage-time at-max, or a
///   drainable commit-time would-exceed) — clears when the indexer drains.
/// - `NOVELTY_DEFERRED`: materialization deferred (including the
///   deferral-only `MaterializePartial` split) — same capacity class.
/// - `INTERNAL`: the status-passthrough hole — `SubmissionError::Overloaded`
///   (committer in-flight cap, retryable by its contract) and any tracked
///   error relayed via `ApiError::Http { status: 503 }` reach `error_type`'s
///   `Http` catch-all. If a new 503 lands here, give it a typed code instead
///   of relying on this entry.
pub(crate) const RETRYABLE_503_TYPES: [&str; 3] = [
    fluree_vocab::errors::NOVELTY_AT_MAX,
    fluree_vocab::errors::NOVELTY_DEFERRED,
    fluree_vocab::errors::INTERNAL,
];

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

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_vocab::errors;

    fn storage_denied_direct() -> ApiError {
        ApiError::StorageAccessDenied {
            bucket: "b".into(),
            key: "warehouse/t/data/f.parquet".into(),
            region: Some("us-east-2".into()),
            message: "service error: AccessDenied".into(),
        }
    }

    fn storage_denied_via_query() -> ApiError {
        ApiError::Query(fluree_db_query::QueryError::StorageAccessDenied {
            bucket: "b".into(),
            key: "warehouse/t/data/f.parquet".into(),
            region: Some("us-east-2".into()),
            message: "service error: AccessDenied".into(),
        })
    }

    fn not_vended_direct() -> ApiError {
        ApiError::CatalogCredentialsNotVended {
            catalog_uri: "https://catalog.example/v1".into(),
        }
    }

    fn not_vended_via_query() -> ApiError {
        ApiError::Query(fluree_db_query::QueryError::CatalogCredentialsNotVended {
            catalog_uri: "https://catalog.example/v1".into(),
        })
    }

    #[test]
    fn storage_access_denied_is_403_both_paths() {
        // Preview path (direct ApiError) and scan path (wrapped via Query) both
        // surface HTTP 403 + the distinct STORAGE_ACCESS_DENIED @type.
        for api in [storage_denied_direct(), storage_denied_via_query()] {
            let se = ServerError::Api(api);
            assert_eq!(se.status_code(), StatusCode::FORBIDDEN);
            assert_eq!(se.error_type(), errors::STORAGE_ACCESS_DENIED);
        }
    }

    #[test]
    fn credentials_not_vended_is_403_both_paths() {
        for api in [not_vended_direct(), not_vended_via_query()] {
            let se = ServerError::Api(api);
            assert_eq!(se.status_code(), StatusCode::FORBIDDEN);
            assert_eq!(se.error_type(), errors::CATALOG_CREDENTIALS_NOT_VENDED);
        }
    }

    #[test]
    fn error_body_json_shape_for_storage_access_denied() {
        // Exact body the solo code-first dispatch consumes: `@type` is the
        // stable dispatch key; `status` is 403; the structured fields
        // (bucket/key/region) are carried in the human-readable `error` string.
        let se = ServerError::Api(storage_denied_via_query());
        let body = ErrorResponse {
            error: se.to_string(),
            status: se.status_code().as_u16(),
            error_type: se.error_type().to_string(),
            cause: extract_cause(&se),
        };
        let json = serde_json::to_value(&body).unwrap();
        assert_eq!(json["status"], 403);
        assert_eq!(json["@type"], errors::STORAGE_ACCESS_DENIED);
        let msg = json["error"].as_str().unwrap();
        assert!(msg.contains("s3://b/warehouse/t/data/f.parquet"), "{msg}");
        assert!(msg.contains("region us-east-2"), "{msg}");
        // No cause chain for these leaf errors.
        assert!(json.get("cause").is_none());
    }

    #[test]
    fn error_body_json_shape_for_credentials_not_vended() {
        let se = ServerError::Api(not_vended_direct());
        let body = ErrorResponse {
            error: se.to_string(),
            status: se.status_code().as_u16(),
            error_type: se.error_type().to_string(),
            cause: extract_cause(&se),
        };
        let json = serde_json::to_value(&body).unwrap();
        assert_eq!(json["status"], 403);
        assert_eq!(json["@type"], errors::CATALOG_CREDENTIALS_NOT_VENDED);
        let msg = json["error"].as_str().unwrap();
        assert!(msg.contains("https://catalog.example/v1"), "{msg}");
        assert!(msg.contains("vended_credentials=false"), "{msg}");
    }

    #[test]
    fn r2rml_unsupported_pattern_is_400_with_distinct_type() {
        // Well-formed but unsupported ON THIS SOURCE: stays HTTP 400 (not a
        // distinct status like 403/507), but carries a distinct `@type` machine
        // code so the Solo browse UI can branch on it instead of matching prose.
        let se = ServerError::Api(ApiError::Query(
            fluree_db_query::QueryError::r2rml_unsupported_pattern(
                "graph source 'x' has 1 pattern(s) with a variable predicate and a bound term",
            ),
        ));
        assert_eq!(se.status_code(), StatusCode::BAD_REQUEST);
        assert_eq!(se.error_type(), errors::R2RML_UNSUPPORTED_PATTERN);
        let body = ErrorResponse {
            error: se.to_string(),
            status: se.status_code().as_u16(),
            error_type: se.error_type().to_string(),
            cause: extract_cause(&se),
        };
        let json = serde_json::to_value(&body).unwrap();
        assert_eq!(json["status"], 400);
        assert_eq!(json["@type"], errors::R2RML_UNSUPPORTED_PATTERN);
        // The human-readable message keeps the migration substring existing
        // prose-matchers rely on.
        assert!(
            json["error"]
                .as_str()
                .unwrap()
                .contains("cannot be converted to R2RML scans"),
            "{}",
            json["error"]
        );
    }

    use fluree_db_api::TargetTally;

    fn partial(ok: usize, deferred: usize, failed: usize) -> ServerError {
        ServerError::Api(ApiError::MaterializePartial {
            tally: TargetTally {
                ok,
                deferred,
                failed,
            },
            detail: "test".into(),
        })
    }

    /// A partial window's status must be decided by WHY it is incomplete.
    ///
    /// Deferral is capacity and clears itself, so 503 ("try again") is honest. A target
    /// that FAILED will not clear itself, and answering 503 would tell a caller to keep
    /// retrying a permanently broken ledger while reporting it as merely busy — the same
    /// mistake, in the other direction, as the 500 that a plain deferral used to return.
    #[test]
    fn partial_window_status_follows_the_reason_not_the_partialness() {
        assert_eq!(
            partial(21, 1, 0).status_code(),
            StatusCode::SERVICE_UNAVAILABLE,
            "deferral-only is retryable capacity pressure"
        );
        assert_eq!(
            partial(21, 0, 1).status_code(),
            StatusCode::INTERNAL_SERVER_ERROR,
            "a failed target is a fault and must not be reported as backpressure"
        );
        // A failure alongside deferrals still reports the failure: it is the outcome
        // needing attention, and burying it under the milder one is how a broken ledger
        // stayed invisible for 20 minutes across 208 windows.
        assert_eq!(
            partial(20, 1, 1).status_code(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    /// Novelty backpressure must answer 503 + `err:db/NoveltyAtMax` in every
    /// shape it reaches this layer: the raw `ApiError::Transact` variants
    /// (paths that never cross the consensus boundary) and the
    /// `ServerError::NoveltyBackpressure` reconstruction (paths where the
    /// variant was flattened to a `SubmissionError`). Before this mapping the
    /// condition fell through the `Transact(_)` catch-alls as 400
    /// InvalidTransaction, and a client that retries 5xx but treats 4xx as
    /// permanent dropped the write.
    #[test]
    fn novelty_backpressure_is_503_with_novelty_code_in_every_shape() {
        let shapes = [
            ServerError::Api(ApiError::Transact(
                fluree_db_api::TransactError::NoveltyAtMax,
            )),
            ServerError::Api(ApiError::Transact(
                fluree_db_api::TransactError::NoveltyWouldExceed {
                    current_bytes: 90,
                    delta_bytes: 20,
                    max_bytes: 100,
                },
            )),
            ServerError::NoveltyBackpressure("Novelty at maximum size, reindexing required".into()),
        ];
        for se in shapes {
            assert_eq!(se.status_code(), StatusCode::SERVICE_UNAVAILABLE, "{se}");
            assert_eq!(se.error_type(), errors::NOVELTY_AT_MAX, "{se}");
        }
    }

    /// Every 503 carries `Retry-After` (all of this server's 503s are
    /// retryable capacity pressure); non-503s must not. The value is
    /// jittered, so assert presence + the configured range, not an exact
    /// number — an exact pin would re-freeze the constant the jitter
    /// exists to avoid.
    #[test]
    fn service_unavailable_responses_carry_retry_after() {
        let resp = ServerError::Api(ApiError::Transact(
            fluree_db_api::TransactError::NoveltyAtMax,
        ))
        .into_response();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        let secs = retry_after_secs(&resp).expect("503 must carry Retry-After");
        assert!(
            (RETRY_AFTER_SECS_MIN..=RETRY_AFTER_SECS_MAX).contains(&secs),
            "Retry-After {secs} outside [{RETRY_AFTER_SECS_MIN}, {RETRY_AFTER_SECS_MAX}]"
        );

        let resp = ServerError::BadRequest("nope".into()).into_response();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        assert!(resp
            .headers()
            .get(axum::http::header::RETRY_AFTER)
            .is_none());
    }

    fn retry_after_secs(resp: &Response) -> Option<u32> {
        resp.headers()
            .get(axum::http::header::RETRY_AFTER)?
            .to_str()
            .ok()?
            .parse()
            .ok()
    }

    /// The "every 503 is retryable capacity" invariant, pinned against an
    /// explicit allowlist. Enumerates every 503-producing path in this
    /// crate's mapping; each must carry an `@type` from
    /// [`RETRYABLE_503_TYPES`] and a `Retry-After` in the jitter range. A
    /// future arm that maps a new condition to 503 fails this test (or the
    /// `debug_assert` in `into_response`, for paths this corpus misses)
    /// until the code is consciously classified as retryable capacity.
    #[test]
    fn every_503_source_is_classified_retryable_capacity() {
        // One entry per 503-producing path, with the code it must carry.
        let sources: [(ServerError, &str, &str); 6] = [
            (
                ServerError::Api(ApiError::NoveltyDeferred { remaining: 3 }),
                errors::NOVELTY_DEFERRED,
                "materialization deferred (capacity)",
            ),
            (
                partial(21, 1, 0),
                errors::NOVELTY_DEFERRED,
                "deferral-only partial window",
            ),
            (
                ServerError::Api(ApiError::Transact(
                    fluree_db_api::TransactError::NoveltyAtMax,
                )),
                errors::NOVELTY_AT_MAX,
                "stage-time novelty at max",
            ),
            (
                ServerError::Api(ApiError::Transact(
                    fluree_db_api::TransactError::NoveltyWouldExceed {
                        current_bytes: 90,
                        delta_bytes: 20,
                        max_bytes: 100,
                    },
                )),
                errors::NOVELTY_AT_MAX,
                "drainable commit-time would-exceed",
            ),
            (
                ServerError::NoveltyBackpressure("novelty at max (flattened)".into()),
                errors::NOVELTY_AT_MAX,
                "consensus-flattened novelty backpressure",
            ),
            (
                // `SubmissionError::Overloaded` reaches this shape via
                // `submission_error_to_server_error` — retryable by its
                // contract (in-flight cap), but its `@type` is the `Http`
                // catch-all. Classified consciously; a typed code should
                // replace this entry if the passthrough grows more cases.
                ServerError::Api(ApiError::http(
                    503,
                    "committer overloaded; in-flight operation cap reached",
                )),
                errors::INTERNAL,
                "committer-overload status passthrough",
            ),
        ];
        for (se, expected_type, why) in sources {
            assert_eq!(se.status_code(), StatusCode::SERVICE_UNAVAILABLE, "{why}");
            assert_eq!(se.error_type(), expected_type, "{why}");
            assert!(
                RETRYABLE_503_TYPES.contains(&expected_type),
                "{why}: {expected_type} missing from the allowlist"
            );
            let resp = se.into_response();
            let secs = retry_after_secs(&resp)
                .unwrap_or_else(|| panic!("{why}: 503 must carry Retry-After"));
            assert!(
                (RETRY_AFTER_SECS_MIN..=RETRY_AFTER_SECS_MAX).contains(&secs),
                "{why}: Retry-After {secs} outside range"
            );
        }
        // The allowlist itself is part of the contract: growing it is a
        // conscious act, recorded here.
        assert_eq!(RETRYABLE_503_TYPES.len(), 3);
    }

    /// The two 413s carry distinct codes for clients to branch on: the HTTP
    /// body-size cap (`read_limited_body`'s `ApiError::http(413, ..)`) is
    /// `err:db/PayloadTooLarge`, never the novelty code — and, like the
    /// novelty 413, it must not invite a retry.
    #[test]
    fn body_limit_413_carries_payload_too_large_not_the_novelty_code() {
        let se = ServerError::Api(ApiError::http(
            413,
            "request body exceeds the configured limit",
        ));
        assert_eq!(se.status_code(), StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(se.error_type(), errors::PAYLOAD_TOO_LARGE);
        assert_ne!(se.error_type(), errors::NOVELTY_DELTA_TOO_LARGE);
        let resp = se.into_response();
        assert!(resp
            .headers()
            .get(axum::http::header::RETRY_AFTER)
            .is_none());
    }

    /// The oversized-delta carve-out: a delta at or above
    /// `reindex_max_bytes` can never succeed by drain (the commit check is
    /// `current + delta >= max`, so even empty novelty refuses it), so it
    /// must NOT get the retryable 503 shape — it answers 413 +
    /// `err:db/NoveltyDeltaTooLarge` with NO `Retry-After`, in both the
    /// raw-variant and consensus-flattened shapes. The drainable/oversized
    /// boundary is exactly `delta >= max`.
    #[test]
    fn oversized_novelty_delta_is_413_with_distinct_code_and_no_retry_after() {
        let shapes = [
            ServerError::Api(ApiError::Transact(
                fluree_db_api::TransactError::NoveltyWouldExceed {
                    current_bytes: 0,
                    delta_bytes: 100,
                    max_bytes: 100,
                },
            )),
            ServerError::NoveltyDeltaTooLarge(
                "Transaction would exceed novelty limit: current=0, delta=100, max=100".into(),
            ),
        ];
        for se in shapes {
            assert_eq!(se.status_code(), StatusCode::PAYLOAD_TOO_LARGE, "{se}");
            assert_eq!(se.error_type(), errors::NOVELTY_DELTA_TOO_LARGE, "{se}");
            let resp = se.into_response();
            assert!(
                resp.headers()
                    .get(axum::http::header::RETRY_AFTER)
                    .is_none(),
                "413 must not invite a retry"
            );
        }
    }

    #[test]
    fn partial_window_error_code_matches_its_status() {
        assert_eq!(
            partial(21, 1, 0).error_type(),
            fluree_vocab::errors::NOVELTY_DEFERRED
        );
        assert_eq!(
            partial(21, 0, 1).error_type(),
            fluree_vocab::errors::MATERIALIZE_PARTIAL
        );
    }
}
