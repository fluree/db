//! Server error types with HTTP status code mapping

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use fluree_db_api::{ApiError, SparqlUpdateLowerError};
use fluree_db_nameservice::NameServiceError;
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

    /// Unsupported Media Type (415) - request body format not accepted
    #[error("{0}")]
    UnsupportedMediaType(String),

    /// SPARQL UPDATE lowering error
    #[error("SPARQL UPDATE error: {0}")]
    SparqlUpdateLower(#[from] SparqlUpdateLowerError),

    /// Novelty backpressure (503 + `Retry-After`): the ledger's in-memory
    /// novelty is at `reindex_max_bytes` (or the transaction would cross it)
    /// and the indexer must drain it before new commits are accepted.
    /// Carries the pipeline's message for submissions whose typed
    /// `ApiError::Transact` variant was flattened at the consensus boundary;
    /// errors that still carry the variant answer through `ApiError`'s table.
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

    /// The write would bring the ledger past the number of distinct
    /// datatypes its index can store (422 + `err:db/DatatypeLimitExceeded`,
    /// no `Retry-After`). Same consensus-boundary role as
    /// `NoveltyBackpressure`.
    #[error("{0}")]
    DatatypeLimitExceeded(String),

    /// Every commit attempt lost its race with other writers (503 +
    /// `err:db/CommitContention` + `Retry-After`). Same consensus-boundary
    /// role as `NoveltyBackpressure`.
    #[error("{0}")]
    CommitContention(String),
}

impl ServerError {
    /// Map error to error type IRI (compact form). An API error carries its
    /// own, beside its status ([`ApiError::error_type`]).
    pub fn error_type(&self) -> &'static str {
        use fluree_vocab::errors;

        match self {
            ServerError::Api(e) => e.error_type(),
            ServerError::MissingLedger => errors::MISSING_LEDGER,
            ServerError::Json(_) => errors::JSON_PARSE,
            ServerError::BadRequest(_) => errors::BAD_REQUEST,
            ServerError::InvalidHeader(_) => errors::INVALID_HEADER,
            ServerError::NotImplemented(_) => errors::NOT_IMPLEMENTED,
            ServerError::Unauthorized(_) => errors::UNAUTHORIZED,
            ServerError::NotFound(_) => errors::NOT_FOUND,
            ServerError::NotAcceptable(_) => errors::NOT_ACCEPTABLE,
            ServerError::UnsupportedMediaType(_) => errors::UNSUPPORTED_MEDIA_TYPE,
            ServerError::SparqlUpdateLower(_) => errors::SPARQL_LOWER,
            ServerError::NoveltyBackpressure(_) => errors::NOVELTY_AT_MAX,
            ServerError::NoveltyDeltaTooLarge(_) => errors::NOVELTY_DELTA_TOO_LARGE,
            ServerError::DatatypeLimitExceeded(_) => errors::DATATYPE_LIMIT_EXCEEDED,
            ServerError::CommitContention(_) => errors::COMMIT_CONTENTION,
        }
    }

    /// Map error to HTTP status code. An API error answers with the API's
    /// table ([`ApiError::status_code`]), the one every flattened form of it
    /// carries too.
    pub fn status_code(&self) -> StatusCode {
        match self {
            ServerError::Api(e) => {
                StatusCode::from_u16(e.status_code()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
            }
            ServerError::MissingLedger
            | ServerError::Json(_)
            | ServerError::BadRequest(_)
            | ServerError::InvalidHeader(_)
            | ServerError::SparqlUpdateLower(_) => StatusCode::BAD_REQUEST,
            ServerError::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            ServerError::NotFound(_) => StatusCode::NOT_FOUND,
            ServerError::NotAcceptable(_) => StatusCode::NOT_ACCEPTABLE,
            ServerError::UnsupportedMediaType(_) => StatusCode::UNSUPPORTED_MEDIA_TYPE,
            ServerError::NotImplemented(_) => StatusCode::NOT_IMPLEMENTED,
            ServerError::NoveltyDeltaTooLarge(_) => StatusCode::PAYLOAD_TOO_LARGE,
            ServerError::DatatypeLimitExceeded(_) => StatusCode::UNPROCESSABLE_ENTITY,
            ServerError::NoveltyBackpressure(_) | ServerError::CommitContention(_) => {
                StatusCode::SERVICE_UNAVAILABLE
            }
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

    /// Create an unsupported media type error (415)
    pub fn unsupported_media_type(msg: impl Into<String>) -> Self {
        ServerError::UnsupportedMediaType(msg.into())
    }
}

impl From<fluree_db_api::LedgerIdParseError> for ServerError {
    fn from(e: fluree_db_api::LedgerIdParseError) -> Self {
        ServerError::bad_request(e.to_string())
    }
}

/// The ledger id a request names, for authorizing it.
///
/// Accepts the full address grammar (`name[:branch][@t:..][#graph]`) and
/// returns the id the API will resolve it to, so a scope check and the
/// operation it guards cannot disagree about which ledger that is.
pub(crate) fn scope_id(raw: &str) -> std::result::Result<fluree_db_api::LedgerId, ServerError> {
    Ok(fluree_db_api::LedgerRef::parse(raw)?.into_id())
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
/// - `COMMIT_CONTENTION`: every commit attempt lost its race with other
///   writers — clears when they ease off.
/// - `INTERNAL`: the status-passthrough hole — `SubmissionError::Overloaded`
///   (committer in-flight cap, retryable by its contract) and any tracked
///   error relayed via `ApiError::Http { status: 503 }` reach `error_type`'s
///   `Http` catch-all. If a new 503 lands here, give it a typed code instead
///   of relying on this entry.
pub(crate) const RETRYABLE_503_TYPES: [&str; 4] = [
    fluree_vocab::errors::NOVELTY_AT_MAX,
    fluree_vocab::errors::NOVELTY_DEFERRED,
    fluree_vocab::errors::COMMIT_CONTENTION,
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

    #[test]
    fn policy_denied_write_is_forbidden_with_access_denied_type() {
        let denied = || fluree_db_api::PolicyError::modify_denied("write refused");
        for err in [
            ApiError::Policy(denied()),
            ApiError::Transact(fluree_db_api::TransactError::PolicyViolation(denied())),
        ] {
            let response = ServerError::Api(err).into_response();
            assert_eq!(response.status(), StatusCode::FORBIDDEN);
            assert_eq!(
                response.headers().get(axum::http::header::RETRY_AFTER),
                None
            );
        }
        let err = ServerError::Api(ApiError::Transact(
            fluree_db_api::TransactError::PolicyViolation(denied()),
        ));
        assert_eq!(err.error_type(), errors::ACCESS_DENIED);
    }

    /// An id refused below the edge is still the caller's input.
    #[test]
    fn an_invalid_ledger_id_is_a_bad_request_wherever_it_is_refused() {
        let parse = fluree_db_api::LedgerIdParseError::new("Invalid ledger id 'a@b'");
        for err in [
            ServerError::from(parse.clone()),
            ServerError::Api(ApiError::InvalidLedgerId(parse)),
            ServerError::Api(ApiError::InvalidBranch("a@b".into())),
            ServerError::Api(ApiError::NameService(NameServiceError::InvalidId(
                "a@b".into(),
            ))),
        ] {
            assert_eq!(err.status_code(), StatusCode::BAD_REQUEST, "{err:?}");
            assert_eq!(err.error_type(), errors::BAD_REQUEST, "{err:?}");
        }
    }

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

    /// A nameservice lookup that fails inside a query (a graph-source probe)
    /// is the backend's fault: typed and answered as the nameservice failure
    /// it is, a 500, not the generic query 400.
    #[test]
    fn a_failed_nameservice_lookup_in_a_query_is_a_500() {
        let se = ServerError::Api(ApiError::Query(fluree_db_query::QueryError::Nameservice(
            "storage unavailable".into(),
        )));
        assert_eq!(se.status_code(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(se.error_type(), errors::NAMESERVICE);
    }

    #[test]
    fn a_catalog_access_refusal_is_403_with_its_own_type() {
        let se = ServerError::Api(ApiError::Query(
            fluree_db_query::QueryError::CatalogAccessDenied {
                table: "main.sales.orders".into(),
                message: "User does not have SELECT (403 Forbidden)".into(),
            },
        ));
        assert_eq!(se.status_code(), StatusCode::FORBIDDEN);
        assert_eq!(se.error_type(), errors::CATALOG_ACCESS_DENIED);
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

    /// A failure to read the ledger config answers 500 with
    /// `err:storage/ReadFailure` and no `Retry-After`, in both of its forms:
    /// a transaction's staging lanes (`TransactError::ConfigUnavailable`,
    /// which the generic `Transact` arm would make a 400) and the API's own
    /// config reads (`ApiError::ConfigUnavailable`: the policy context and a
    /// query's config). The consensus-flattened form is in
    /// `submission_error_variants_are_exhaustively_classified`.
    #[test]
    fn a_config_read_failure_is_a_storage_read_failure() {
        for se in [
            ServerError::Api(ApiError::Transact(
                fluree_db_api::TransactError::ConfigUnavailable("read failed".into()),
            )),
            ServerError::Api(ApiError::ConfigUnavailable("read failed".into())),
        ] {
            assert_eq!(se.status_code(), StatusCode::INTERNAL_SERVER_ERROR, "{se}");
            assert_eq!(se.error_type(), errors::STORAGE_READ, "{se}");
            let resp = se.into_response();
            assert!(retry_after_secs(&resp).is_none());
        }
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
        let sources: [(ServerError, &str, &str); 8] = [
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
                ServerError::Api(ApiError::commit_contention(
                    16,
                    ApiError::Transact(fluree_db_api::TransactError::CommitConflict {
                        expected_t: 1,
                        head_t: 2,
                    }),
                )),
                errors::COMMIT_CONTENTION,
                "every commit attempt lost its race",
            ),
            (
                ServerError::CommitContention("commit contention (flattened)".into()),
                errors::COMMIT_CONTENTION,
                "consensus-flattened commit contention",
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
        assert_eq!(RETRYABLE_503_TYPES.len(), 4);
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

    /// A vector the index cannot store is a permanent refusal: 422 +
    /// `err:db/InvalidTransaction` with no `Retry-After`, as the typed error
    /// and as the committer flattens it (`Http { status }` from
    /// `ApiError::status_code`).
    #[test]
    fn invalid_vector_is_422_invalid_transaction_without_retry_after() {
        let typed = ApiError::Transact(fluree_db_api::TransactError::InvalidVector(
            "vector dims mismatch: expected 3, got 2".into(),
        ));
        let flattened = ApiError::Http {
            status: typed.status_code(),
            message: typed.to_string(),
        };
        for se in [ServerError::Api(typed), ServerError::Api(flattened)] {
            assert_eq!(se.status_code(), StatusCode::UNPROCESSABLE_ENTITY, "{se}");
            assert_eq!(se.error_type(), errors::INVALID_TRANSACTION, "{se}");
            let resp = se.into_response();
            assert!(resp
                .headers()
                .get(axum::http::header::RETRY_AFTER)
                .is_none());
        }
    }

    /// The datatype limit is a permanent refusal: 422 +
    /// `err:db/DatatypeLimitExceeded` with no `Retry-After`, in both the
    /// raw-variant and consensus-flattened shapes.
    #[test]
    fn datatype_limit_is_422_with_distinct_code_and_no_retry_after() {
        let shapes = [
            ServerError::Api(ApiError::Transact(
                fluree_db_api::TransactError::DatatypeLimitExceeded {
                    used: 16_369,
                    adding: 1,
                    max: 16_369,
                },
            )),
            ServerError::DatatypeLimitExceeded("datatype limit exceeded".into()),
        ];
        for se in shapes {
            assert_eq!(se.status_code(), StatusCode::UNPROCESSABLE_ENTITY, "{se}");
            assert_eq!(se.error_type(), errors::DATATYPE_LIMIT_EXCEEDED, "{se}");
            let resp = se.into_response();
            assert!(
                resp.headers()
                    .get(axum::http::header::RETRY_AFTER)
                    .is_none(),
                "422 must not invite a retry"
            );
        }
    }

    /// Merge, rebase and revert reach the HTTP layer through the committer,
    /// which flattens a typed error to `ApiError::Http { status }`; previews
    /// and sweeps return it typed. A branch error answers the same either way.
    #[test]
    fn branch_errors_answer_the_same_typed_or_flattened() {
        for (api, status, code) in [
            (
                ApiError::InvalidBranch("Branch main has no source branch".into()),
                StatusCode::BAD_REQUEST,
                errors::BAD_REQUEST,
            ),
            (
                ApiError::BranchConflict("another maintenance operation holds db:main".into()),
                StatusCode::CONFLICT,
                errors::COMMIT_CONFLICT,
            ),
        ] {
            let flattened = ServerError::Api(ApiError::http(api.status_code(), api.to_string()));
            for se in [ServerError::Api(api), flattened] {
                assert_eq!(se.status_code(), status, "{se}");
                assert_eq!(se.error_type(), code, "{se}");
            }
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
