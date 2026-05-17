//! Transaction endpoints: /fluree/update, /fluree/insert, /fluree/upsert
//!
//! Supports multiple content types:
//! - `application/json`: JSON-LD transaction format (update/insert/upsert)
//! - `application/sparql-update`: SPARQL UPDATE syntax (update only)
//! - `text/turtle`: Turtle RDF format (insert/upsert only)
//! - `application/trig`: TriG format with named graphs (upsert only)
//!
//! # Ledger Selection Priority
//!
//! For non-path endpoints (/fluree/update, etc.), ledger is resolved in this order:
//! 1. Path parameter (/:ledger/update, etc.)
//! 2. Query parameter (?ledger=mydb:main)
//! 3. Header (Fluree-Ledger: mydb:main)
//! 4. Body field ("ledger" or "from")
//!
//! # Turtle vs TriG Semantics
//!
//! - **Turtle on `/insert`**: Uses fast direct flake path. Pure insert semantics.
//! - **TriG on `/insert`**: Returns 400 error. Named graphs require upsert path.
//! - **Turtle/TriG on `/upsert`**: Uses upsert path with GRAPH block extraction for named graphs.

use crate::config::ServerRole;
use crate::error::{Result, ServerError};
use crate::extract::tracking_headers;
use crate::extract::{FlureeHeaders, MaybeCredential, MaybeDataBearer};
use crate::state::AppState;
use crate::telemetry::{
    create_request_span, extract_request_id, extract_trace_id, set_span_error_code,
};
use axum::extract::{Path, Request, State};
use axum::response::{IntoResponse, Response};
use axum::Json;
use fluree_db_api::{
    lower_sparql_update, parse_sparql, with_index_request_correlation, CommitOpts,
    IndexRequestCorrelation, NamespaceRegistry, SparqlQueryBody, TrackingOptions, TxnOpts, TxnType,
};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tracing::Instrument;

/// Query parameters for transaction endpoints
#[derive(Debug, Deserialize, Default)]
pub struct TransactQueryParams {
    /// Target ledger (format: name:branch)
    pub ledger: Option<String>,
}

/// Commit information in transaction response
#[derive(Serialize)]
pub struct CommitInfo {
    /// Commit content identifier (CID)
    pub hash: String,
}

/// Transaction response - expected server format
#[derive(Serialize)]
pub struct TransactResponse {
    /// Ledger identifier
    pub ledger_id: String,
    /// Transaction time (t value)
    pub t: i64,
    /// Transaction ID (SHA-256 hash of transaction data)
    #[serde(rename = "tx-id")]
    pub tx_id: String,
    /// Commit information
    pub commit: CommitInfo,
}

/// Compute transaction ID from request body (SHA-256 hash)
///
/// This matches the legacy derive-tx-id behavior which hashes the JSON-LD normalized data.
/// For simplicity we hash the raw JSON bytes - this is deterministic for the same input.
fn compute_tx_id(body: &JsonValue) -> String {
    let json_bytes = serde_json::to_vec(body).unwrap_or_default();
    let hash = Sha256::digest(&json_bytes);
    format!("fluree:tx:sha256:{}", hex::encode(hash))
}

/// Compute transaction ID from SPARQL UPDATE string
fn compute_tx_id_sparql(sparql: &str) -> String {
    let hash = Sha256::digest(sparql.as_bytes());
    format!("fluree:tx:sha256:{}", hex::encode(hash))
}

/// If the request was signed (credentialed), return the *original* signed envelope
/// to store for provenance (JWS string or VC JSON).
fn raw_txn_from_credential(credential: &MaybeCredential) -> Option<JsonValue> {
    let extracted = credential.credential.as_ref()?;
    let raw = extracted.raw_body.as_ref();

    // Prefer JSON if it parses, otherwise store as string.
    if let Ok(s) = std::str::from_utf8(raw) {
        let trimmed = s.trim();
        if trimmed.starts_with('{') || trimmed.starts_with('[') {
            if let Ok(json) = serde_json::from_str::<JsonValue>(trimmed) {
                return Some(json);
            }
        }
        return Some(JsonValue::String(trimmed.to_string()));
    }

    // Fallback for non-UTF8: store base64 string for auditability.
    use base64::Engine as _;
    let b64 = base64::engine::general_purpose::STANDARD.encode(raw);
    Some(JsonValue::String(format!("base64:{b64}")))
}

/// Extract query params from request URI before consuming the request
fn extract_query_params(request: &Request) -> TransactQueryParams {
    request
        .uri()
        .query()
        .and_then(|q| serde_urlencoded::from_str(q).ok())
        .unwrap_or_default()
}

/// Check if the credential contains a W3C SPARQL Protocol form-encoded update
/// (`Content-Type: application/x-www-form-urlencoded` with `update=<sparql>`).
///
/// If detected, rewrites the credential's body and flags so the rest of the
/// pipeline treats it as `application/sparql-update`.  This is required for
/// standard SPARQL benchmarking tools (e.g. BSBM test driver) that use the
/// form-encoded transport defined in the SPARQL 1.1 Protocol spec §2.2.
fn maybe_rewrite_form_encoded_update(credential: &mut MaybeCredential) {
    // Only act when none of the typed content-type flags are already set
    if credential.is_sparql_update
        || credential.is_sparql
        || credential.is_turtle
        || credential.is_trig
    {
        return;
    }

    // Check Content-Type header for form-urlencoded
    let is_form = credential
        .headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|ct| ct.contains("application/x-www-form-urlencoded"))
        .unwrap_or(false);

    if !is_form {
        return;
    }

    // Try to parse the body as form data and extract the `update` field
    let body_str = match std::str::from_utf8(&credential.body) {
        Ok(s) => s,
        Err(_) => return,
    };

    let parsed: Vec<(String, String)> = match serde_urlencoded::from_str(body_str) {
        Ok(p) => p,
        Err(_) => return,
    };

    if let Some((_, sparql)) = parsed.iter().find(|(k, _)| k == "update") {
        credential.body = axum::body::Bytes::from(sparql.clone());
        credential.is_sparql_update = true;
    }
}

/// Inject header-based tracking options into transaction body (modifies in place).
///
/// Mirrors the query-side `inject_headers_into_query` pattern: header values act
/// as defaults that do not override body-level opts.
fn inject_headers_into_txn(body: &mut JsonValue, headers: &FlureeHeaders) {
    if !headers.has_tracking() {
        return;
    }
    if let Some(obj) = body.as_object_mut() {
        let opts = obj
            .entry("opts")
            .or_insert_with(|| JsonValue::Object(serde_json::Map::new()));
        if let Some(opts_obj) = opts.as_object_mut() {
            headers.inject_into_opts(opts_obj);
        }
    }
}

/// Check if tracking options are present in transaction body (meta or max-fuel).
fn has_tracking_opts(body: &JsonValue) -> bool {
    let Some(opts) = body.get("opts") else {
        return false;
    };
    if let Some(meta) = opts.get("meta") {
        match meta {
            JsonValue::Bool(true) => return true,
            JsonValue::Object(obj) if !obj.is_empty() => return true,
            _ => {}
        }
    }
    opts.get("max-fuel").is_some()
        || opts.get("max_fuel").is_some()
        || opts.get("maxFuel").is_some()
}

/// Build `TrackingOptions` from the transaction body opts (after header injection).
fn tracking_options_from_body(body: &JsonValue) -> Option<TrackingOptions> {
    if !has_tracking_opts(body) {
        return None;
    }
    let opts = body.get("opts");
    let tracking = TrackingOptions::from_opts_value(opts);
    if tracking.any_enabled() {
        Some(tracking)
    } else {
        None
    }
}

/// Helper to extract ledger ID from request
///
/// Priority: path > query param > header > body.ledger > body.from
fn get_ledger_id(
    path_ledger: Option<&str>,
    query_params: &TransactQueryParams,
    headers: &FlureeHeaders,
    body: &JsonValue,
) -> Result<String> {
    // Priority: path > query param > header > body.ledger > body.from
    if let Some(ledger) = path_ledger {
        return Ok(ledger.to_string());
    }

    if let Some(ledger) = &query_params.ledger {
        return Ok(ledger.clone());
    }

    if let Some(ledger) = &headers.ledger {
        return Ok(ledger.clone());
    }

    if let Some(ledger) = body.get("ledger").and_then(|v| v.as_str()) {
        return Ok(ledger.to_string());
    }

    if let Some(from) = body.get("from").and_then(|v| v.as_str()) {
        return Ok(from.to_string());
    }

    Err(ServerError::MissingLedger)
}

// ============================================================================
// Data API Auth Helpers
// ============================================================================

/// Resolve the effective author identity for transactions.
///
/// Precedence:
/// 1) Signed request DID (credential)
/// 2) Bearer token identity (fluree.identity ?? sub)
fn effective_author(
    credential: &MaybeCredential,
    bearer: Option<&crate::extract::DataPrincipal>,
) -> Option<String> {
    credential
        .did()
        .map(std::string::ToString::to_string)
        .or_else(|| bearer.and_then(|p| p.identity.clone()))
}

/// Enforce write authorization for a ledger according to `data_auth.mode`.
///
/// Records `error_code` on the current span when access is denied.
fn enforce_write_access(
    state: &AppState,
    ledger: &str,
    bearer: Option<&crate::extract::DataPrincipal>,
    credential: &MaybeCredential,
) -> Result<()> {
    let data_auth = state.config.data_auth();

    // In Required mode: accept either signed requests OR bearer tokens.
    if data_auth.mode == crate::config::DataAuthMode::Required && !credential.is_signed() {
        let Some(p) = bearer else {
            set_span_error_code(&tracing::Span::current(), "error:Unauthorized");
            return Err(ServerError::unauthorized(
                "Authentication required (signed request or Bearer token)",
            ));
        };
        if !p.can_write(ledger) {
            set_span_error_code(&tracing::Span::current(), "error:Forbidden");
            // Avoid existence leak
            return Err(ServerError::not_found("Ledger not found"));
        }
        return Ok(());
    }

    // In Optional/None mode: if a bearer token is present, it still limits access.
    if !credential.is_signed() {
        if let Some(p) = bearer {
            if !p.can_write(ledger) {
                set_span_error_code(&tracing::Span::current(), "error:Forbidden");
                return Err(ServerError::not_found("Ledger not found"));
            }
        }
    }

    Ok(())
}

/// Execute an update transaction (WHERE/DELETE/INSERT)
///
/// POST /fluree/update
///
/// Executes a full transaction with insert, delete, and where clauses.
/// Supports signed requests (JWS/VC format).
/// In peer mode, forwards the request to the transaction server.
pub async fn update(
    State(state): State<Arc<AppState>>,
    MaybeDataBearer(bearer): MaybeDataBearer,
    request: Request,
) -> Response {
    // In peer mode, forward to transaction server
    if state.config.server_role == ServerRole::Peer {
        return forward_write_request(&state, request).await;
    }

    // Transaction mode: process locally
    update_local(state, bearer, request).await.into_response()
}

/// Local implementation of update (transaction mode only)
async fn update_local(
    state: Arc<AppState>,
    bearer: Option<crate::extract::DataPrincipal>,
    request: Request,
) -> Result<Response> {
    // Extract query params before consuming the request
    let query_params = extract_query_params(&request);
    // Extract headers
    let headers_result = FlureeHeaders::from_headers(request.headers());
    let headers = match headers_result {
        Ok(h) => h,
        Err(e) => return Err(e),
    };

    // Extract credential (consumes the request body)
    let mut credential = MaybeCredential::extract(request).await?;

    // W3C SPARQL Protocol: rewrite form-encoded `update=...` to sparql-update
    maybe_rewrite_form_encoded_update(&mut credential);

    // Create request span with correlation context
    let request_id = extract_request_id(&credential.headers, &state.telemetry_config);

    // Detect input format before span creation so otel.name is set at open time
    let input_format = if credential.is_sparql_update() {
        "sparql-update"
    } else if credential.is_trig() {
        "trig"
    } else if credential.is_turtle_or_trig() {
        "turtle"
    } else {
        "json-ld"
    };

    let span = create_request_span(
        "update",
        request_id.as_deref(),
        extract_trace_id(&credential.headers).as_deref(),
        None, // ledger ID determined later
        None, // tenant_id not yet supported
        Some(input_format),
    );
    async move {
        let span = tracing::Span::current();

        // Check if this is a SPARQL UPDATE request
        if credential.is_sparql_update() {
            tracing::info!(
                status = "start",
                format = "sparql-update",
                "SPARQL UPDATE request received"
            );
            return execute_sparql_update_request(
                &state,
                None,
                &query_params,
                &headers,
                &credential,
                &span,
                bearer.as_ref(),
            )
            .await;
        }

        // Update does not accept Turtle/TriG. Use /insert or /upsert.
        if credential.is_turtle_or_trig() {
            set_span_error_code(&span, "error:BadRequest");
            return Err(ServerError::bad_request(
                "Turtle/TriG is not supported on the update endpoint. \
                 Use /v1/fluree/insert for Turtle inserts, /v1/fluree/upsert for Turtle/TriG upserts, \
                 or send JSON-LD/SPARQL UPDATE to /v1/fluree/update.",
            ));
        }

        tracing::info!(status = "start", "transaction request received");

        let mut body_json = match credential.body_json() {
            Ok(json) => json,
            Err(e) => {
                set_span_error_code(&span, "error:BadRequest");
                tracing::warn!(error = %e, "invalid JSON in transaction body");
                return Err(e);
            }
        };

        let ledger_id = match get_ledger_id(None, &query_params, &headers, &body_json) {
            Ok(id) => {
                span.record("ledger_id", id.as_str());
                id
            }
            Err(e) => {
                set_span_error_code(&span, "error:BadRequest");
                tracing::warn!(error = %e, "missing ledger ID");
                return Err(e);
            }
        };

        enforce_write_access(&state, &ledger_id, bearer.as_ref(), &credential)?;
        let author = effective_author(&credential, bearer.as_ref());
        execute_transaction(
            &state,
            &ledger_id,
            TxnType::Update,
            &mut body_json,
            &credential,
            author.as_deref(),
            &headers,
        )
        .await
    }
    .instrument(span)
    .await
}

/// Execute an update transaction with ledger in path
///
/// POST /:ledger/update
/// Supports signed requests (JWS/VC format).
/// In peer mode, forwards the request to the transaction server.
pub async fn update_ledger(
    State(state): State<Arc<AppState>>,
    Path(ledger): Path<String>,
    MaybeDataBearer(bearer): MaybeDataBearer,
    request: Request,
) -> Response {
    // In peer mode, forward to transaction server
    if state.config.server_role == ServerRole::Peer {
        return forward_write_request(&state, request).await;
    }

    // Transaction mode: process locally
    update_ledger_local(state, ledger, bearer, request)
        .await
        .into_response()
}

/// Execute a transaction with ledger as greedy tail segment.
///
/// POST /fluree/update/<ledger...>
pub async fn update_ledger_tail(
    State(state): State<Arc<AppState>>,
    Path(ledger): Path<String>,
    MaybeDataBearer(bearer): MaybeDataBearer,
    request: Request,
) -> Response {
    update_ledger(State(state), Path(ledger), MaybeDataBearer(bearer), request).await
}

/// Local implementation of update_ledger
async fn update_ledger_local(
    state: Arc<AppState>,
    ledger: String,
    bearer: Option<crate::extract::DataPrincipal>,
    request: Request,
) -> Result<Response> {
    // Extract query params before consuming the request
    let query_params = extract_query_params(&request);

    let headers_result = FlureeHeaders::from_headers(request.headers());
    let headers = match headers_result {
        Ok(h) => h,
        Err(e) => return Err(e),
    };
    let mut credential = MaybeCredential::extract(request).await?;

    // W3C SPARQL Protocol: rewrite form-encoded `update=...` to sparql-update
    maybe_rewrite_form_encoded_update(&mut credential);

    let request_id = extract_request_id(&credential.headers, &state.telemetry_config);

    let input_format = if credential.is_sparql_update() {
        "sparql-update"
    } else if credential.is_trig() {
        "trig"
    } else if credential.is_turtle_or_trig() {
        "turtle"
    } else {
        "json-ld"
    };

    let span = create_request_span(
        "update",
        request_id.as_deref(),
        extract_trace_id(&credential.headers).as_deref(),
        Some(&ledger),
        None,
        Some(input_format),
    );
    async move {
        let span = tracing::Span::current();

        // Check if this is a SPARQL UPDATE request
        if credential.is_sparql_update() {
            tracing::info!(
                status = "start",
                format = "sparql-update",
                "SPARQL UPDATE request received"
            );
            return execute_sparql_update_request(
                &state,
                Some(&ledger),
                &query_params,
                &headers,
                &credential,
                &span,
                bearer.as_ref(),
            )
            .await;
        }

        // Update does not accept Turtle/TriG. Use /insert or /upsert.
        if credential.is_turtle_or_trig() {
            set_span_error_code(&span, "error:BadRequest");
            return Err(ServerError::bad_request(
                "Turtle/TriG is not supported on the update endpoint. \
                 Use /v1/fluree/insert for Turtle inserts, /v1/fluree/upsert for Turtle/TriG upserts, \
                 or send JSON-LD/SPARQL UPDATE to /v1/fluree/update.",
            ));
        }

        tracing::info!(status = "start", "ledger transaction request received");

        let mut body_json = match credential.body_json() {
            Ok(json) => json,
            Err(e) => {
                set_span_error_code(&span, "error:BadRequest");
                tracing::warn!(error = %e, "invalid JSON in transaction body");
                return Err(e);
            }
        };

        let ledger_id = match get_ledger_id(Some(&ledger), &query_params, &headers, &body_json) {
            Ok(id) => {
                span.record("ledger_id", id.as_str());
                id
            }
            Err(e) => {
                set_span_error_code(&span, "error:BadRequest");
                tracing::warn!(error = %e, "ledger ID mismatch");
                return Err(e);
            }
        };

        enforce_write_access(&state, &ledger_id, bearer.as_ref(), &credential)?;
        let author = effective_author(&credential, bearer.as_ref());
        execute_transaction(
            &state,
            &ledger_id,
            TxnType::Update,
            &mut body_json,
            &credential,
            author.as_deref(),
            &headers,
        )
        .await
    }
    .instrument(span)
    .await
}

/// Insert data
///
/// POST /fluree/insert
///
/// Convenience endpoint for insert-only transactions.
/// Supports signed requests (JWS/VC format).
/// In peer mode, forwards the request to the transaction server.
pub async fn insert(
    State(state): State<Arc<AppState>>,
    MaybeDataBearer(bearer): MaybeDataBearer,
    request: Request,
) -> Response {
    // In peer mode, forward to transaction server
    if state.config.server_role == ServerRole::Peer {
        return forward_write_request(&state, request).await;
    }

    insert_local(state, bearer, request).await.into_response()
}

/// Local implementation of insert
async fn insert_local(
    state: Arc<AppState>,
    bearer: Option<crate::extract::DataPrincipal>,
    request: Request,
) -> Result<Response> {
    // Extract query params before consuming the request
    let query_params = extract_query_params(&request);

    let headers_result = FlureeHeaders::from_headers(request.headers());
    let headers = match headers_result {
        Ok(h) => h,
        Err(e) => return Err(e),
    };
    let credential = MaybeCredential::extract(request).await?;

    let request_id = extract_request_id(&credential.headers, &state.telemetry_config);

    let input_format = if credential.is_trig() {
        "trig"
    } else if credential.is_turtle_or_trig() {
        "turtle"
    } else {
        "json-ld"
    };

    let span = create_request_span(
        "insert",
        request_id.as_deref(),
        extract_trace_id(&credential.headers).as_deref(),
        None,
        None,
        Some(input_format),
    );
    async move {
        let span = tracing::Span::current();

        // Check if this is a Turtle or TriG request
        if credential.is_turtle_or_trig() {
            let format = if credential.is_trig() {
                "trig"
            } else {
                "turtle"
            };
            tracing::info!(
                status = "start",
                format = format,
                "insert transaction requested"
            );

            let turtle = match credential.body_string() {
                Ok(s) => s,
                Err(e) => {
                    set_span_error_code(&span, "error:BadRequest");
                    tracing::warn!(error = %e, "invalid UTF-8 in Turtle/TriG body");
                    return Err(e);
                }
            };

            // For Turtle/TriG, ledger must come from query param or header
            let ledger_id = match query_params.ledger.as_ref().or(headers.ledger.as_ref()) {
                Some(ledger) => {
                    span.record("ledger_id", ledger.as_str());
                    ledger.clone()
                }
                None => {
                    set_span_error_code(&span, "error:BadRequest");
                    tracing::warn!("missing ledger ID for Turtle/TriG insert");
                    return Err(ServerError::MissingLedger);
                }
            };

            enforce_write_access(&state, &ledger_id, bearer.as_ref(), &credential)?;
            let author = effective_author(&credential, bearer.as_ref());
            return execute_turtle_transaction(
                &state,
                &ledger_id,
                TxnType::Insert,
                &turtle,
                &credential,
                author.as_deref(),
            )
            .await;
        }

        tracing::info!(status = "start", "insert transaction requested");

        let mut body_json = match credential.body_json() {
            Ok(json) => json,
            Err(e) => {
                set_span_error_code(&span, "error:BadRequest");
                tracing::warn!(error = %e, "invalid JSON in insert request body");
                return Err(e);
            }
        };

        let ledger_id = match get_ledger_id(None, &query_params, &headers, &body_json) {
            Ok(id) => {
                span.record("ledger_id", id.as_str());
                id
            }
            Err(e) => {
                set_span_error_code(&span, "error:BadRequest");
                tracing::warn!(error = %e, "missing ledger ID");
                return Err(e);
            }
        };

        enforce_write_access(&state, &ledger_id, bearer.as_ref(), &credential)?;
        let author = effective_author(&credential, bearer.as_ref());
        execute_transaction(
            &state,
            &ledger_id,
            TxnType::Insert,
            &mut body_json,
            &credential,
            author.as_deref(),
            &headers,
        )
        .await
    }
    .instrument(span)
    .await
}

/// Upsert data
///
/// POST /fluree/upsert
///
/// Convenience endpoint for upsert transactions (insert or update).
/// Supports signed requests (JWS/VC format).
/// In peer mode, forwards the request to the transaction server.
pub async fn upsert(
    State(state): State<Arc<AppState>>,
    MaybeDataBearer(bearer): MaybeDataBearer,
    request: Request,
) -> Response {
    // In peer mode, forward to transaction server
    if state.config.server_role == ServerRole::Peer {
        return forward_write_request(&state, request).await;
    }

    upsert_local(state, bearer, request).await.into_response()
}

/// Local implementation of upsert
async fn upsert_local(
    state: Arc<AppState>,
    bearer: Option<crate::extract::DataPrincipal>,
    request: Request,
) -> Result<Response> {
    // Extract query params before consuming the request
    let query_params = extract_query_params(&request);

    let headers_result = FlureeHeaders::from_headers(request.headers());
    let headers = match headers_result {
        Ok(h) => h,
        Err(e) => return Err(e),
    };
    let credential = MaybeCredential::extract(request).await?;

    let request_id = extract_request_id(&credential.headers, &state.telemetry_config);

    let input_format = if credential.is_trig() {
        "trig"
    } else if credential.is_turtle_or_trig() {
        "turtle"
    } else {
        "json-ld"
    };

    let span = create_request_span(
        "upsert",
        request_id.as_deref(),
        extract_trace_id(&credential.headers).as_deref(),
        None,
        None,
        Some(input_format),
    );
    async move {
        let span = tracing::Span::current();

        // Check if this is a Turtle or TriG request
        if credential.is_turtle_or_trig() {
            let format = if credential.is_trig() {
                "trig"
            } else {
                "turtle"
            };
            tracing::info!(
                status = "start",
                format = format,
                "upsert transaction requested"
            );

            let turtle = match credential.body_string() {
                Ok(s) => s,
                Err(e) => {
                    set_span_error_code(&span, "error:BadRequest");
                    tracing::warn!(error = %e, "invalid UTF-8 in Turtle/TriG body");
                    return Err(e);
                }
            };

            // For Turtle/TriG, ledger must come from query param or header
            let ledger_id = match query_params.ledger.as_ref().or(headers.ledger.as_ref()) {
                Some(ledger) => {
                    span.record("ledger_id", ledger.as_str());
                    ledger.clone()
                }
                None => {
                    set_span_error_code(&span, "error:BadRequest");
                    tracing::warn!("missing ledger ID for Turtle/TriG upsert");
                    return Err(ServerError::MissingLedger);
                }
            };

            enforce_write_access(&state, &ledger_id, bearer.as_ref(), &credential)?;
            let author = effective_author(&credential, bearer.as_ref());
            return execute_turtle_transaction(
                &state,
                &ledger_id,
                TxnType::Upsert,
                &turtle,
                &credential,
                author.as_deref(),
            )
            .await;
        }

        tracing::info!(status = "start", "upsert transaction requested");

        let mut body_json = match credential.body_json() {
            Ok(json) => json,
            Err(e) => {
                set_span_error_code(&span, "error:BadRequest");
                tracing::warn!(error = %e, "invalid JSON in upsert request body");
                return Err(e);
            }
        };

        let ledger_id = match get_ledger_id(None, &query_params, &headers, &body_json) {
            Ok(id) => {
                span.record("ledger_id", id.as_str());
                id
            }
            Err(e) => {
                set_span_error_code(&span, "error:BadRequest");
                tracing::warn!(error = %e, "missing ledger ID");
                return Err(e);
            }
        };

        enforce_write_access(&state, &ledger_id, bearer.as_ref(), &credential)?;
        let author = effective_author(&credential, bearer.as_ref());
        execute_transaction(
            &state,
            &ledger_id,
            TxnType::Upsert,
            &mut body_json,
            &credential,
            author.as_deref(),
            &headers,
        )
        .await
    }
    .instrument(span)
    .await
}

/// Insert data with ledger in path
///
/// POST /:ledger/insert
/// Supports signed requests (JWS/VC format).
/// In peer mode, forwards the request to the transaction server.
pub async fn insert_ledger(
    State(state): State<Arc<AppState>>,
    Path(ledger): Path<String>,
    MaybeDataBearer(bearer): MaybeDataBearer,
    request: Request,
) -> Response {
    // In peer mode, forward to transaction server
    if state.config.server_role == ServerRole::Peer {
        return forward_write_request(&state, request).await;
    }

    insert_ledger_local(state, ledger, bearer, request)
        .await
        .into_response()
}

/// Insert data with ledger as greedy tail segment.
///
/// POST /fluree/insert/<ledger...>
pub async fn insert_ledger_tail(
    State(state): State<Arc<AppState>>,
    Path(ledger): Path<String>,
    MaybeDataBearer(bearer): MaybeDataBearer,
    request: Request,
) -> Response {
    insert_ledger(State(state), Path(ledger), MaybeDataBearer(bearer), request).await
}

/// Local implementation of insert_ledger
async fn insert_ledger_local(
    state: Arc<AppState>,
    ledger: String,
    bearer: Option<crate::extract::DataPrincipal>,
    request: Request,
) -> Result<Response> {
    // Extract query params before consuming the request
    let query_params = extract_query_params(&request);

    let headers_result = FlureeHeaders::from_headers(request.headers());
    let headers = match headers_result {
        Ok(h) => h,
        Err(e) => return Err(e),
    };
    let credential = MaybeCredential::extract(request).await?;

    let request_id = extract_request_id(&credential.headers, &state.telemetry_config);

    let input_format = if credential.is_trig() {
        "trig"
    } else if credential.is_turtle_or_trig() {
        "turtle"
    } else {
        "json-ld"
    };

    let span = create_request_span(
        "insert",
        request_id.as_deref(),
        extract_trace_id(&credential.headers).as_deref(),
        Some(&ledger),
        None,
        Some(input_format),
    );
    async move {
        let span = tracing::Span::current();

        // Check if this is a Turtle or TriG request
        if credential.is_turtle_or_trig() {
            let format = if credential.is_trig() {
                "trig"
            } else {
                "turtle"
            };
            tracing::info!(
                status = "start",
                format = format,
                "ledger insert transaction requested"
            );

            let turtle = match credential.body_string() {
                Ok(s) => s,
                Err(e) => {
                    set_span_error_code(&span, "error:BadRequest");
                    tracing::warn!(error = %e, "invalid UTF-8 in Turtle/TriG body");
                    return Err(e);
                }
            };

            enforce_write_access(&state, &ledger, bearer.as_ref(), &credential)?;
            let author = effective_author(&credential, bearer.as_ref());
            return execute_turtle_transaction(
                &state,
                &ledger,
                TxnType::Insert,
                &turtle,
                &credential,
                author.as_deref(),
            )
            .await;
        }

        tracing::info!(status = "start", "ledger insert transaction requested");

        let mut body_json = match credential.body_json() {
            Ok(json) => json,
            Err(e) => {
                set_span_error_code(&span, "error:BadRequest");
                tracing::warn!(error = %e, "invalid JSON in insert request body");
                return Err(e);
            }
        };

        let ledger_id = match get_ledger_id(Some(&ledger), &query_params, &headers, &body_json) {
            Ok(id) => {
                span.record("ledger_id", id.as_str());
                id
            }
            Err(e) => {
                set_span_error_code(&span, "error:BadRequest");
                tracing::warn!(error = %e, "ledger ID mismatch");
                return Err(e);
            }
        };

        enforce_write_access(&state, &ledger_id, bearer.as_ref(), &credential)?;
        let author = effective_author(&credential, bearer.as_ref());
        execute_transaction(
            &state,
            &ledger_id,
            TxnType::Insert,
            &mut body_json,
            &credential,
            author.as_deref(),
            &headers,
        )
        .await
    }
    .instrument(span)
    .await
}

/// Upsert data with ledger in path
///
/// POST /:ledger/upsert
/// Supports signed requests (JWS/VC format).
/// In peer mode, forwards the request to the transaction server.
pub async fn upsert_ledger(
    State(state): State<Arc<AppState>>,
    Path(ledger): Path<String>,
    MaybeDataBearer(bearer): MaybeDataBearer,
    request: Request,
) -> Response {
    // In peer mode, forward to transaction server
    if state.config.server_role == ServerRole::Peer {
        return forward_write_request(&state, request).await;
    }

    upsert_ledger_local(state, ledger, bearer, request)
        .await
        .into_response()
}

/// Upsert data with ledger as greedy tail segment.
///
/// POST /fluree/upsert/<ledger...>
pub async fn upsert_ledger_tail(
    State(state): State<Arc<AppState>>,
    Path(ledger): Path<String>,
    MaybeDataBearer(bearer): MaybeDataBearer,
    request: Request,
) -> Response {
    upsert_ledger(State(state), Path(ledger), MaybeDataBearer(bearer), request).await
}

/// Local implementation of upsert_ledger
async fn upsert_ledger_local(
    state: Arc<AppState>,
    ledger: String,
    bearer: Option<crate::extract::DataPrincipal>,
    request: Request,
) -> Result<Response> {
    // Extract query params before consuming the request
    let query_params = extract_query_params(&request);

    let headers_result = FlureeHeaders::from_headers(request.headers());
    let headers = match headers_result {
        Ok(h) => h,
        Err(e) => return Err(e),
    };
    let credential = MaybeCredential::extract(request).await?;

    let request_id = extract_request_id(&credential.headers, &state.telemetry_config);

    let input_format = if credential.is_trig() {
        "trig"
    } else if credential.is_turtle_or_trig() {
        "turtle"
    } else {
        "json-ld"
    };

    let span = create_request_span(
        "upsert",
        request_id.as_deref(),
        extract_trace_id(&credential.headers).as_deref(),
        Some(&ledger),
        None,
        Some(input_format),
    );
    async move {
        let span = tracing::Span::current();

        // Check if this is a Turtle or TriG request
        if credential.is_turtle_or_trig() {
            let format = if credential.is_trig() {
                "trig"
            } else {
                "turtle"
            };
            tracing::info!(
                status = "start",
                format = format,
                "ledger upsert transaction requested"
            );

            let turtle = match credential.body_string() {
                Ok(s) => s,
                Err(e) => {
                    set_span_error_code(&span, "error:BadRequest");
                    tracing::warn!(error = %e, "invalid UTF-8 in Turtle/TriG body");
                    return Err(e);
                }
            };

            enforce_write_access(&state, &ledger, bearer.as_ref(), &credential)?;
            let author = effective_author(&credential, bearer.as_ref());
            return execute_turtle_transaction(
                &state,
                &ledger,
                TxnType::Upsert,
                &turtle,
                &credential,
                author.as_deref(),
            )
            .await;
        }

        tracing::info!(status = "start", "ledger upsert transaction requested");

        let mut body_json = match credential.body_json() {
            Ok(json) => json,
            Err(e) => {
                set_span_error_code(&span, "error:BadRequest");
                tracing::warn!(error = %e, "invalid JSON in upsert request body");
                return Err(e);
            }
        };

        let ledger_id = match get_ledger_id(Some(&ledger), &query_params, &headers, &body_json) {
            Ok(id) => {
                span.record("ledger_id", id.as_str());
                id
            }
            Err(e) => {
                set_span_error_code(&span, "error:BadRequest");
                tracing::warn!(error = %e, "ledger ID mismatch");
                return Err(e);
            }
        };

        enforce_write_access(&state, &ledger_id, bearer.as_ref(), &credential)?;
        let author = effective_author(&credential, bearer.as_ref());
        execute_transaction(
            &state,
            &ledger_id,
            TxnType::Upsert,
            &mut body_json,
            &credential,
            author.as_deref(),
            &headers,
        )
        .await
    }
    .instrument(span)
    .await
}

/// Execute a transaction with the given type.
///
/// When tracking headers are present (fluree-track-fuel, fluree-max-fuel, etc.),
/// tracking options are injected into the transaction body and the response
/// includes x-fdb-fuel / x-fdb-time headers.
async fn execute_transaction(
    state: &AppState,
    ledger_id: &str,
    txn_type: TxnType,
    body: &mut JsonValue,
    credential: &MaybeCredential,
    author: Option<&str>,
    headers: &FlureeHeaders,
) -> Result<Response> {
    // Inject header-based tracking options into body opts (header defaults, body overrides)
    inject_headers_into_txn(body, headers);

    // Apply bearer identity + server-default policy-class to opts, honoring the
    // root-identity impersonation semantic (see routes::policy_auth). After this
    // call, body.opts.identity / policy-class reflect the effective identity
    // used for policy enforcement.
    let default_policy_class = state.config.data_auth().default_policy_class.clone();
    crate::routes::policy_auth::apply_auth_identity_to_opts(
        state,
        ledger_id,
        body,
        author,
        default_policy_class.as_deref(),
    )
    .await;

    // Extract tracking options from body (after header injection)
    let tracking = tracking_options_from_body(body);

    // Parse QueryConnectionOptions from the finalized body opts. These drive
    // PolicyContext construction below.
    let qc_opts = fluree_db_api::QueryConnectionOptions::from_json(body).unwrap_or_default();

    // Create execution span
    let span =
        tracing::debug_span!("transact_execute", ledger_id = ledger_id, txn_type = ?txn_type);
    async move {
        let span = tracing::Span::current();

        // Compute tx-id from request body (before any modification)
        let tx_id = compute_tx_id(body);

        tracing::debug!(tx_id = %tx_id, "computed transaction ID");

        // Get cached ledger handle (loads if not cached)
        // Transaction execution is only in transaction mode (peers forward)
        let handle = match state.fluree.ledger_cached(ledger_id).await {
            Ok(handle) => handle,
            Err(e) => {
                let server_error = ServerError::Api(e);
                set_span_error_code(&span, "error:NotFound");
                tracing::error!(error = %server_error, "ledger not found");
                return Err(server_error);
            }
        };

        // Build a PolicyContext from the finalized opts if any policy inputs
        // are present. This covers:
        //   - unsigned bearer requests (identity now forced into opts)
        //   - impersonation requests (opts.identity from body/header)
        //   - explicit opts.policy / opts.policy-class on any request
        // Requests with no policy inputs still run under root (today's behavior).
        let policy_ctx = if qc_opts.has_any_policy_inputs() {
            let snap = handle.snapshot().await;
            match fluree_db_api::build_policy_context(
                &snap.snapshot,
                snap.novelty.as_ref(),
                Some(snap.novelty.as_ref()),
                snap.t,
                &qc_opts,
            )
            .await
            {
                Ok(ctx) => Some(ctx),
                Err(e) => {
                    let server_error = ServerError::Api(e);
                    set_span_error_code(&span, "error:PolicyBuildFailed");
                    tracing::error!(error = %server_error, "failed to build policy context");
                    return Err(server_error);
                }
            }
        } else {
            None
        };

        // Effective author: prefer the (possibly-impersonated) opts.identity
        // so the commit records who the transaction was executed AS. The
        // original bearer identity that authorized the request is captured in
        // the `policy impersonation: bearer=... target=... ledger=...` audit
        // log emitted by `apply_auth_identity_to_opts`. The two-source design
        // is deliberate: commits should remain attributable to the policy
        // subject responsible for the data change, while the audit trail
        // captures the operator who performed the action.
        let did = qc_opts
            .identity
            .clone()
            .or_else(|| author.map(String::from));

        // TxnOpts: unchanged by identity; commit provenance flows through CommitOpts.
        // Pick up `opts.shapes` from the body so inline SHACL shapes
        // reach the staging path. Other TxnOpts fields are not yet
        // surfaced over HTTP (branch/context/etc. come from headers
        // or query params); add them here if a use case lands.
        let mut txn_opts = TxnOpts::default();
        if let Some(shapes) = body.get("opts").and_then(|o| o.get("shapes")) {
            txn_opts.shapes = Some(shapes.clone());
        }

        // Build and execute the transaction via the builder API.
        // Hoisted above CommitOpts assembly so we can spawn the raw-txn upload
        // in parallel with the rest of the pipeline when the request is signed.
        let fluree = &state.fluree;

        // If the request was signed, ALWAYS store the original signed envelope for provenance.
        // (No opt-in needed; this is the primary reason to store txn payloads.)
        let mut commit_opts = match &did {
            Some(d) => CommitOpts::default().identity(d.clone()),
            None => CommitOpts::default(),
        };
        if let Some(raw_txn) = raw_txn_from_credential(credential) {
            let content_store = fluree.content_store(handle.ledger_id());
            commit_opts = commit_opts.with_raw_txn_spawned(content_store, raw_txn);
        }

        let builder = fluree.stage(&handle);
        let builder = match txn_type {
            TxnType::Insert => builder.insert(body),
            TxnType::Upsert => builder.upsert(body),
            TxnType::Update => builder.update(body),
        };
        let mut builder = builder.txn_opts(txn_opts).commit_opts(commit_opts);
        if let Some(config) = &state.index_config {
            builder = builder.index_config(config.clone());
        }
        if let Some(opts) = tracking {
            builder = builder.tracking(opts);
        }
        if let Some(ctx) = policy_ctx {
            builder = builder.policy(ctx);
        }

        let correlation = index_request_correlation(
            &credential.headers,
            extract_request_id(&credential.headers, &state.telemetry_config),
            match txn_type {
                TxnType::Insert => "insert",
                TxnType::Upsert => "upsert",
                TxnType::Update => "update",
            },
        );
        let result = match with_index_request_correlation(correlation, builder.execute()).await {
            Ok(result) => {
                tracing::info!(
                    status = "success",
                    commit_t = result.receipt.t,
                    commit_id = %result.receipt.commit_id,
                    "transaction committed"
                );
                result
            }
            Err(e) => {
                let server_error = ServerError::Api(e);
                set_span_error_code(&span, "error:InvalidTransaction");
                tracing::error!(error = %server_error, "transaction failed");
                return Err(server_error);
            }
        };

        let response_json = Json(TransactResponse {
            ledger_id: ledger_id.to_string(),
            t: result.receipt.t,
            tx_id,
            commit: CommitInfo {
                hash: result.receipt.commit_id.to_string(),
            },
        });

        // Return tracking headers when a tally is present
        match result.tally {
            Some(tally) => {
                let hdrs = tracking_headers(&tally);
                Ok((hdrs, response_json).into_response())
            }
            None => Ok(response_json.into_response()),
        }
    }
    .instrument(span)
    .await
}

// ===== Turtle/TriG execution =====

/// Compute transaction ID from Turtle/TriG string
fn compute_tx_id_turtle(turtle: &str) -> String {
    let hash = Sha256::digest(turtle.as_bytes());
    format!("fluree:tx:sha256:{}", hex::encode(hash))
}

fn index_request_correlation(
    headers: &axum::http::HeaderMap,
    request_id: Option<String>,
    operation: &'static str,
) -> IndexRequestCorrelation {
    IndexRequestCorrelation::new(request_id, extract_trace_id(headers), Some(operation))
}

/// Execute a Turtle/TriG transaction
///
/// This function handles both:
/// - text/turtle: Standard Turtle format (insert or upsert)
/// - application/trig: TriG format with GRAPH blocks for named graphs (upsert only)
///
/// # Insert vs Upsert Semantics
///
/// - **Insert with Turtle** (`text/turtle` on `/insert`): Uses direct flake parsing (fast path).
///   Pure insert - will fail if subjects already exist with conflicting data.
/// - **Insert with TriG** (`application/trig` on `/insert`): Not supported - returns 400.
///   Named graphs require the upsert path for GRAPH block extraction.
/// - **Upsert with Turtle/TriG** (`/upsert`): Uses `upsert_turtle` which handles GRAPH blocks
///   and supports named graph ingestion. For each (subject, predicate) pair, existing values
///   are retracted before new values are asserted.
async fn execute_turtle_transaction(
    state: &AppState,
    ledger_id: &str,
    txn_type: TxnType,
    turtle: &str,
    credential: &MaybeCredential,
    author: Option<&str>,
) -> Result<Response> {
    let is_trig = credential.is_trig();

    // Create execution span
    let format = if is_trig { "trig" } else { "turtle" };
    let span = tracing::debug_span!("transact_execute", ledger_id = ledger_id, txn_type = ?txn_type, format = format);
    async move {
        let span = tracing::Span::current();

        // TriG on /insert is not supported - named graphs require upsert path
        if is_trig && txn_type == TxnType::Insert {
            set_span_error_code(&span, "error:BadRequest");
            tracing::warn!("TriG format not supported on insert endpoint");
            return Err(ServerError::bad_request(
                "TriG format (application/trig) is not supported on the insert endpoint. \
                 Named graph ingestion requires the upsert endpoint (/upsert or /:ledger/upsert).",
            ));
        }

        // Compute tx-id from Turtle string
        let tx_id = compute_tx_id_turtle(turtle);

        tracing::debug!(tx_id = %tx_id, "computed transaction ID");

        // Get cached ledger handle (loads if not cached)
        let handle = match state.fluree.ledger_cached(ledger_id).await {
            Ok(handle) => handle,
            Err(e) => {
                let server_error = ServerError::Api(e);
                set_span_error_code(&span, "error:NotFound");
                tracing::error!(error = %server_error, "ledger not found");
                return Err(server_error);
            }
        };

        let did = author.map(String::from);

        let txn_opts = TxnOpts::default();

        // Build fluree handle first so we can spawn the raw-txn upload in
        // parallel with the rest of the pipeline.
        let fluree = &state.fluree;

        // If the request was signed, ALWAYS store the original signed envelope for provenance.
        let mut commit_opts = match &did {
            Some(d) => CommitOpts::default().identity(d.clone()),
            None => CommitOpts::default(),
        };
        if let Some(raw_txn) = raw_txn_from_credential(credential) {
            let content_store = fluree.content_store(handle.ledger_id());
            commit_opts = commit_opts.with_raw_txn_spawned(content_store, raw_txn);
        }

        let builder = fluree.stage(&handle);
        let builder = match txn_type {
            // Insert with plain Turtle: use fast direct flake path
            TxnType::Insert => builder.insert_turtle(turtle),
            // Upsert: use upsert_turtle which handles GRAPH blocks for named graphs
            TxnType::Upsert => builder.upsert_turtle(turtle),
            TxnType::Update => {
                // Update with Turtle is not supported - use SPARQL UPDATE instead
                set_span_error_code(&span, "error:BadRequest");
                return Err(ServerError::bad_request(
                    "Turtle format is not supported for update transactions. Use SPARQL UPDATE instead.",
                ));
            }
        };
        let mut builder = builder.txn_opts(txn_opts).commit_opts(commit_opts);
        if let Some(config) = &state.index_config {
            builder = builder.index_config(config.clone());
        }

        let correlation = index_request_correlation(
            &credential.headers,
            extract_request_id(&credential.headers, &state.telemetry_config),
            match txn_type {
                TxnType::Insert => "insert",
                TxnType::Upsert => "upsert",
                TxnType::Update => "update",
            },
        );
        let result = match with_index_request_correlation(correlation, builder.execute()).await {
            Ok(result) => {
                tracing::info!(
                    status = "success",
                    commit_t = result.receipt.t,
                    commit_id = %result.receipt.commit_id,
                    "Turtle/TriG transaction committed"
                );
                result
            }
            Err(e) => {
                let server_error = ServerError::Api(e);
                set_span_error_code(&span, "error:InvalidTransaction");
                tracing::error!(error = %server_error, "Turtle/TriG transaction failed");
                return Err(server_error);
            }
        };

        let response_json = Json(TransactResponse {
            ledger_id: ledger_id.to_string(),
            t: result.receipt.t,
            tx_id,
            commit: CommitInfo {
                hash: result.receipt.commit_id.to_string(),
            },
        });

        match result.tally {
            Some(tally) => {
                let hdrs = tracking_headers(&tally);
                Ok((hdrs, response_json).into_response())
            }
            None => Ok(response_json.into_response()),
        }
    }
    .instrument(span)
    .await
}

// ===== SPARQL UPDATE execution =====

/// Execute a SPARQL UPDATE request
///
/// This function:
/// 1. Parses the SPARQL UPDATE string
/// 2. Extracts the UpdateOperation from the AST
/// 3. Lowers to Txn IR using lower_sparql_update
/// 4. Executes via the stage/commit pipeline
async fn execute_sparql_update_request(
    state: &AppState,
    path_ledger: Option<&str>,
    query_params: &TransactQueryParams,
    headers: &FlureeHeaders,
    credential: &MaybeCredential,
    parent_span: &tracing::Span,
    bearer: Option<&crate::extract::DataPrincipal>,
) -> Result<Response> {
    // Extract SPARQL string from body
    let sparql = match credential.body_string() {
        Ok(s) => s,
        Err(e) => {
            set_span_error_code(parent_span, "error:BadRequest");
            tracing::warn!(error = %e, "invalid SPARQL UPDATE body");
            return Err(e);
        }
    };

    // Compute tx-id from SPARQL string
    let tx_id = compute_tx_id_sparql(&sparql);

    // Get ledger id from path, query param, or header (SPARQL UPDATE body doesn't contain ledger)
    let ledger_id = match path_ledger {
        Some(ledger) => ledger.to_string(),
        None => match query_params.ledger.as_ref().or(headers.ledger.as_ref()) {
            Some(ledger) => ledger.clone(),
            None => {
                set_span_error_code(parent_span, "error:BadRequest");
                tracing::warn!("missing ledger ID for SPARQL UPDATE");
                return Err(ServerError::MissingLedger);
            }
        },
    };

    parent_span.record("ledger_id", ledger_id.as_str());

    // Enforce write access for unsigned requests when bearer is present/required
    enforce_write_access(state, &ledger_id, bearer, credential)?;

    // Parse SPARQL
    let parse_output = parse_sparql(&sparql);
    if parse_output.has_errors() {
        let errors: Vec<String> = parse_output
            .diagnostics
            .iter()
            .filter(|d| d.is_error())
            .map(|d| d.message.clone())
            .collect();
        set_span_error_code(parent_span, "error:SparqlParse");
        tracing::warn!(errors = ?errors, "SPARQL UPDATE parse errors");
        return Err(ServerError::bad_request(format!(
            "SPARQL UPDATE parse error: {}",
            errors.join("; ")
        )));
    }

    let ast = match parse_output.ast {
        Some(ast) => ast,
        None => {
            set_span_error_code(parent_span, "error:SparqlParse");
            return Err(ServerError::bad_request("Failed to parse SPARQL UPDATE"));
        }
    };

    // Verify this is an UPDATE operation
    let update_op = match &ast.body {
        SparqlQueryBody::Update(op) => op,
        _ => {
            set_span_error_code(parent_span, "error:BadRequest");
            tracing::warn!("Expected SPARQL UPDATE, got query");
            return Err(ServerError::bad_request(
                "Expected SPARQL UPDATE operation, got query. Use the /query endpoint for SELECT/CONSTRUCT/ASK/DESCRIBE.",
            ));
        }
    };

    // Get ledger handle
    let handle = match state.fluree.ledger_cached(&ledger_id).await {
        Ok(handle) => handle,
        Err(e) => {
            let server_error = ServerError::Api(e);
            set_span_error_code(parent_span, "error:NotFound");
            tracing::error!(error = %server_error, "ledger not found");
            return Err(server_error);
        }
    };

    // Resolve the effective identity honoring the root-impersonation semantic.
    // For SPARQL UPDATE, impersonation is driven by the `fluree-identity` header
    // (there is no body-level opts block). Policy-class / policy / policy-values
    // headers are not yet plumbed for SPARQL UPDATE.
    let bearer_identity = effective_author(credential, bearer);
    let effective_identity = crate::routes::policy_auth::resolve_sparql_identity(
        state,
        &ledger_id,
        bearer_identity.as_deref(),
        headers.identity.as_deref(),
    )
    .await;

    let policy_values_map = match headers.policy_values_map() {
        Ok(v) => v,
        Err(e) => {
            set_span_error_code(parent_span, "error:BadRequest");
            tracing::warn!(error = %e, "invalid fluree-policy-values header");
            return Err(e);
        }
    };
    let qc_opts = fluree_db_api::QueryConnectionOptions {
        identity: effective_identity.clone(),
        policy_class: if headers.policy_class.is_empty() {
            None
        } else {
            Some(headers.policy_class.clone())
        },
        policy: headers.policy.clone(),
        policy_values: policy_values_map,
        default_allow: headers.default_allow,
        ..Default::default()
    };

    let fluree = &state.fluree;
    let correlation = index_request_correlation(
        &credential.headers,
        extract_request_id(&credential.headers, &state.telemetry_config),
        "sparql-update",
    );

    // Bounded retry around snapshot fetch + lowering + execute.
    //
    // `lower_sparql_update` allocates IRIs against a `NamespaceRegistry` built
    // from the *current* snapshot to assign Sids to template terms. Two
    // concurrent SPARQL UPDATEs racing on a fresh namespace can both pick the
    // same first-time code for *different* prefixes, then the second writer
    // hits `TransactError::NamespaceConflict` at staging because the staging
    // registry sees the first writer's commit. The fix: re-fetch the snapshot
    // and re-lower against the latest namespace allocations. Bounded to 3
    // attempts to avoid livelock; the conflict is rare (requires concurrent
    // writers AND first-time-namespace contention), so 1 retry is usually
    // enough.
    const MAX_NS_RETRIES: usize = 3;
    let mut last_error: Option<ServerError> = None;
    let mut result = None;
    for attempt in 1..=MAX_NS_RETRIES {
        let cached_state = handle.snapshot().await;
        let mut ns = NamespaceRegistry::from_db(&cached_state.snapshot);

        // Build PolicyContext from the resolved identity plus all header-supplied
        // policy fields. Rebuilt each attempt because it depends on the snapshot.
        let policy_ctx = if qc_opts.has_any_policy_inputs() {
            match fluree_db_api::build_policy_context(
                &cached_state.snapshot,
                cached_state.novelty.as_ref(),
                Some(cached_state.novelty.as_ref()),
                cached_state.t,
                &qc_opts,
            )
            .await
            {
                Ok(ctx) => Some(ctx),
                Err(e) => {
                    let server_error = ServerError::Api(e);
                    set_span_error_code(parent_span, "error:PolicyBuildFailed");
                    tracing::error!(error = %server_error, "failed to build policy context for SPARQL UPDATE");
                    return Err(server_error);
                }
            }
        } else {
            None
        };

        let txn = match lower_sparql_update(update_op, &ast.prologue, &mut ns, TxnOpts::default()) {
            Ok(txn) => txn,
            Err(e) => {
                set_span_error_code(parent_span, "error:SparqlLower");
                tracing::warn!(error = %e, "SPARQL UPDATE lowering failed");
                return Err(ServerError::SparqlUpdateLower(e));
            }
        };

        tracing::debug!(
            tx_id = %tx_id,
            attempt,
            txn_type = ?txn.txn_type,
            where_patterns = txn.where_patterns.len(),
            delete_templates = txn.delete_templates.len(),
            insert_templates = txn.insert_templates.len(),
            "SPARQL UPDATE lowered to Txn IR"
        );

        let mut builder = fluree.stage(&handle).txn(txn);
        let mut commit_opts = CommitOpts::default();
        if let Some(d) = &effective_identity {
            commit_opts = commit_opts.identity(d.clone());
        }
        // If the request was signed, ALWAYS store the original signed envelope
        // for provenance. Spawn the upload in parallel with the rest of the
        // pipeline. Re-spawning across retries is harmless: the content store
        // is content-addressed so duplicate puts are idempotent.
        if let Some(raw_txn) = raw_txn_from_credential(credential) {
            let content_store = fluree.content_store(handle.ledger_id());
            commit_opts = commit_opts.with_raw_txn_spawned(content_store, raw_txn);
        }
        builder = builder.commit_opts(commit_opts);
        if let Some(config) = &state.index_config {
            builder = builder.index_config(config.clone());
        }
        if let Some(ctx) = policy_ctx {
            builder = builder.policy(ctx);
        }

        match with_index_request_correlation(correlation.clone(), builder.execute()).await {
            Ok(r) => {
                tracing::info!(
                    status = "success",
                    attempt,
                    commit_t = r.receipt.t,
                    commit_id = %r.receipt.commit_id,
                    "SPARQL UPDATE committed"
                );
                result = Some(r);
                break;
            }
            Err(fluree_db_api::ApiError::Transact(
                fluree_db_api::TransactError::NamespaceConflict(msg),
            )) if attempt < MAX_NS_RETRIES => {
                tracing::warn!(
                    attempt,
                    max_attempts = MAX_NS_RETRIES,
                    %msg,
                    "SPARQL UPDATE namespace conflict; re-lowering against latest snapshot"
                );
                continue;
            }
            Err(e) => {
                let server_error = ServerError::Api(e);
                set_span_error_code(parent_span, "error:InvalidTransaction");
                tracing::error!(error = %server_error, attempt, "SPARQL UPDATE failed");
                last_error = Some(server_error);
                break;
            }
        }
    }
    let result = match result {
        Some(r) => r,
        None => {
            return Err(last_error.unwrap_or_else(|| {
                ServerError::internal("SPARQL UPDATE failed after retries with no captured error")
            }));
        }
    };

    let response_json = Json(TransactResponse {
        ledger_id,
        t: result.receipt.t,
        tx_id,
        commit: CommitInfo {
            hash: result.receipt.commit_id.to_string(),
        },
    });

    match result.tally {
        Some(tally) => {
            let hdrs = tracking_headers(&tally);
            Ok((hdrs, response_json).into_response())
        }
        None => Ok(response_json.into_response()),
    }
}

// ===== Peer mode forwarding =====

/// Forward a transaction request to the transaction server (peer mode)
async fn forward_write_request(state: &AppState, request: Request) -> Response {
    let client = match state.forwarding_client.as_ref() {
        Some(c) => c,
        None => {
            return ServerError::internal("Forwarding client not configured").into_response();
        }
    };

    tracing::debug!("Forwarding transaction request to transaction server");

    // Forward the request and return the response directly
    // This preserves the upstream status codes (including 502/504 for errors)
    match client.forward(request).await {
        Ok(response) => response,
        Err(e) => e.into_response(),
    }
}
