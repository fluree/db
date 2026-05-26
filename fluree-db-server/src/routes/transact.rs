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
use axum::http::{HeaderMap, HeaderValue};
use axum::response::{IntoResponse, Response};
use axum::Json;
use fluree_db_api::{
    with_index_request_correlation, ApiError, CommitOpts, Fluree, GovernanceOptions,
    IndexRequestCorrelation, LedgerHandle, PolicyStats, TrackingOptions, TrackingTally, TxnOpts,
    TxnType,
};
use fluree_db_consensus::{
    IdempotencyKey, SubmissionError, Submitter, TransactionBody, TransactionReceipt,
    TransactionRequest,
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
    /// Execution time when tracking was requested
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time: Option<String>,
    /// Fuel consumed when tracking was requested
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fuel: Option<f64>,
    /// Policy stats when policy tracking was requested
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy: Option<std::collections::HashMap<String, PolicyStats>>,
}

fn transact_response(
    ledger_id: String,
    t: i64,
    tx_id: String,
    commit_hash: String,
    tally: Option<&TrackingTally>,
) -> TransactResponse {
    TransactResponse {
        ledger_id,
        t,
        tx_id,
        commit: CommitInfo { hash: commit_hash },
        time: tally.and_then(|t| t.time.clone()),
        fuel: tally.and_then(|t| t.fuel),
        policy: tally.and_then(|t| t.policy.clone()),
    }
}

fn record_tracking_on_span(span: &tracing::Span, tally: &TrackingTally) {
    if let Some(ref time) = tally.time {
        span.record("tracker_time", time.as_str());
    }
    if let Some(fuel) = tally.fuel {
        span.record("tracker_fuel", fuel);
    }
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

/// Extract an [`IdempotencyKey`] from the `Idempotency-Key` request header.
///
/// Returns `None` when the header is absent or empty. Non-UTF-8 header values
/// are also treated as absent — the consensus layer can only key on strings.
pub(crate) fn extract_idempotency_key(headers: &HeaderMap) -> Option<IdempotencyKey> {
    headers
        .get("Idempotency-Key")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(IdempotencyKey::new)
}

/// Tracking options requested via headers, or `None` when none were requested.
fn tracking_from_headers(headers: &FlureeHeaders) -> Option<TrackingOptions> {
    headers
        .has_tracking()
        .then(|| headers.to_tracking_options())
}

/// Map a [`SubmissionError`] to the [`ServerError`] / HTTP status the rest
/// of the server expects.
///
/// `Execution` carries the status from the underlying transaction pipeline
/// (e.g. 422 for a validation failure, 404 for a missing ledger), so it is
/// passed through rather than collapsed to a 500.
pub(crate) fn submission_error_to_server_error(err: SubmissionError) -> ServerError {
    let status = match &err {
        SubmissionError::KeyCollision | SubmissionError::AlreadyInFlight => 409,
        SubmissionError::Overloaded => 503,
        SubmissionError::Execution { status, .. } => *status,
    };
    ServerError::Api(ApiError::http(status, err.to_string()))
}

/// Build a response from a consensus receipt, attaching the headers the
/// receipt implies.
///
/// Echoes the `Idempotency-Key` when the submission carried one (signalling
/// that the server tracked it for idempotent retry; absence signals it was
/// ignored), and attaches the tracking headers when a tally is present.
fn build_consensus_response(
    response_json: Json<TransactResponse>,
    receipt: &TransactionReceipt,
) -> Response {
    let mut headers = HeaderMap::new();
    if let Some(key) = &receipt.idempotency_key {
        if let Ok(value) = HeaderValue::from_str(key.as_str()) {
            headers.insert("Idempotency-Key", value);
        }
    }
    if let Some(tally) = &receipt.tally {
        headers.extend(tracking_headers(tally));
    }
    (headers, response_json).into_response()
}

/// The mutated body plus options extracted from it.
///
/// Returned by [`prepare_transaction_body`] and consumed downstream by both
/// the consensus and direct-builder paths.
struct PreparedTransaction {
    body: JsonValue,
    tracking: Option<TrackingOptions>,
    governance: GovernanceOptions,
}

/// Phase 1 of [`execute_transaction`]: prepare the JSON-LD request body.
///
/// Injects header-derived options, applies bearer-identity / policy-class
/// defaults to the body's opts (which the rest of the pipeline reads),
/// then extracts the tracking and policy options from the finalized body.
async fn prepare_transaction_body(
    state: &AppState,
    ledger_id: &str,
    mut body: JsonValue,
    headers: &FlureeHeaders,
    author: Option<&str>,
) -> PreparedTransaction {
    inject_headers_into_txn(&mut body, headers);

    let default_policy_class = state.config.data_auth().default_policy_class.clone();
    crate::routes::policy_auth::apply_auth_identity_to_opts(
        state,
        ledger_id,
        &mut body,
        author,
        default_policy_class.as_deref(),
    )
    .await;

    let tracking = tracking_options_from_body(&body);
    let governance = GovernanceOptions::from_json(&body).unwrap_or_default();

    PreparedTransaction {
        body,
        tracking,
        governance,
    }
}

/// Resolve the effective identity for a transaction.
///
/// Prefers the (possibly-impersonated) `opts.identity` so the commit records
/// who the transaction was executed AS; falls back to the bearer-derived
/// author. The original bearer identity that authorized the request is
/// captured separately in the impersonation audit log emitted by
/// `apply_auth_identity_to_opts` — commits stay attributable to the policy
/// subject responsible for the data change, while the audit trail captures
/// the operator who performed the action.
fn effective_did<'a>(
    governance: &'a GovernanceOptions,
    author: Option<&'a str>,
) -> Option<&'a str> {
    governance.identity.as_deref().or(author)
}

/// Build the [`CommitOpts`] for the transaction.
///
/// Encodes the effective identity (so the commit records its author) and,
/// for signed requests, spawns the raw-envelope upload in parallel so it
/// overlaps with the rest of the pipeline.
fn build_commit_opts(
    did: Option<&str>,
    credential: &MaybeCredential,
    fluree: &Fluree,
    handle: &LedgerHandle,
) -> CommitOpts {
    let mut commit_opts = match did {
        Some(d) => CommitOpts::default().identity(d.to_string()),
        None => CommitOpts::default(),
    };
    if let Some(raw_txn) = raw_txn_from_credential(credential) {
        let content_store = fluree.content_store(handle.id());
        commit_opts = commit_opts.with_raw_txn_spawned(content_store, raw_txn);
    }
    commit_opts
}

/// Submit a prepared transaction through monolithic consensus and shape the
/// HTTP response.
///
/// All upstream preparation (header injection, identity wiring, opts
/// assembly, `tx_id` derivation) happens in the caller. Request correlation
/// is attached around the submission so background index work the
/// transaction triggers stays attributable to the originating request.
async fn transact_via_consensus(
    state: &AppState,
    ledger_id: &str,
    request: TransactionRequest,
    tx_id: String,
    headers: &HeaderMap,
) -> Result<Response> {
    let correlation = IndexRequestCorrelation::new(
        extract_request_id(headers, &state.telemetry_config),
        extract_trace_id(headers),
        Some(request.body.operation_tag()),
    );

    let submission =
        with_index_request_correlation(correlation, state.consensus.transact(request)).await;
    let receipt = match submission {
        Ok(receipt) => {
            tracing::info!(
                status = "success",
                commit_t = receipt.commit.t,
                commit_id = %receipt.commit.commit_id,
                "transaction committed via consensus"
            );
            receipt
        }
        Err(err) => {
            set_span_error_code(&tracing::Span::current(), "error:InvalidTransaction");
            tracing::error!(error = %err, "transaction submission failed");
            return Err(submission_error_to_server_error(err));
        }
    };

    if let Some(tally) = &receipt.tally {
        record_tracking_on_span(&tracing::Span::current(), tally);
    }
    let response_json = Json(transact_response(
        ledger_id.to_string(),
        receipt.commit.t,
        tx_id,
        receipt.commit.commit_id.to_string(),
        receipt.tally.as_ref(),
    ));
    Ok(build_consensus_response(response_json, &receipt))
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

        let body_json = match credential.body_json() {
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
            body_json,
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

        let body_json = match credential.body_json() {
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
            body_json,
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
                &headers,
                author.as_deref(),
            )
            .await;
        }

        tracing::info!(status = "start", "insert transaction requested");

        let body_json = match credential.body_json() {
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
            body_json,
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
                &headers,
                author.as_deref(),
            )
            .await;
        }

        tracing::info!(status = "start", "upsert transaction requested");

        let body_json = match credential.body_json() {
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
            body_json,
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
                &headers,
                author.as_deref(),
            )
            .await;
        }

        tracing::info!(status = "start", "ledger insert transaction requested");

        let body_json = match credential.body_json() {
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
            body_json,
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
                &headers,
                author.as_deref(),
            )
            .await;
        }

        tracing::info!(status = "start", "ledger upsert transaction requested");

        let body_json = match credential.body_json() {
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
            body_json,
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
    body: JsonValue,
    credential: &MaybeCredential,
    author: Option<&str>,
    headers: &FlureeHeaders,
) -> Result<Response> {
    let idempotency_key = extract_idempotency_key(&credential.headers);
    let prepared_transaction =
        prepare_transaction_body(state, ledger_id, body, headers, author).await;

    let span = tracing::debug_span!(
        "transact_execute",
        ledger_id = ledger_id,
        txn_type = ?txn_type,
        tracker_time = tracing::field::Empty,
        tracker_fuel = tracing::field::Empty,
    );
    async move {
        let span = tracing::Span::current();

        let tx_id = compute_tx_id(&prepared_transaction.body);
        tracing::debug!(tx_id = %tx_id, "computed transaction ID");

        // Resolve the ledger handle up front so a missing ledger surfaces as
        // a 404 here, before submission. The handle is also the source of the
        // canonical ledger ID used to scope the raw-txn content store.
        let handle = match state.fluree.ledger_cached(ledger_id).await {
            Ok(handle) => handle,
            Err(e) => {
                let server_error = ServerError::Api(e);
                set_span_error_code(&span, "error:NotFound");
                tracing::error!(error = %server_error, "ledger not found");
                return Err(server_error);
            }
        };

        let did = effective_did(&prepared_transaction.governance, author);
        let commit_opts = build_commit_opts(did, credential, &state.fluree, &handle);

        // Every JSON-LD transaction goes through consensus. Policy context,
        // tracking, and execution are all handled by the submission layer;
        // policy is built there from the ledger state the transaction
        // actually stages against.
        let body = match txn_type {
            TxnType::Insert => TransactionBody::JsonLdInsert(prepared_transaction.body),
            TxnType::Upsert => TransactionBody::JsonLdUpsert(prepared_transaction.body),
            TxnType::Update => TransactionBody::JsonLdUpdate(prepared_transaction.body),
        };
        let request = TransactionRequest {
            idempotency_key,
            ledger_id: ledger_id.to_string(),
            body,
            txn_opts: TxnOpts::default(),
            commit_opts,
            tracking: prepared_transaction.tracking,
            governance: prepared_transaction.governance,
        };
        transact_via_consensus(state, ledger_id, request, tx_id, &credential.headers).await
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
    headers: &FlureeHeaders,
    author: Option<&str>,
) -> Result<Response> {
    let is_trig = credential.is_trig();

    // Create execution span
    let format = if is_trig { "trig" } else { "turtle" };
    let span = tracing::debug_span!(
        "transact_execute",
        ledger_id = ledger_id,
        txn_type = ?txn_type,
        format = format,
        tracker_time = tracing::field::Empty,
        tracker_fuel = tracing::field::Empty,
    );
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

        let commit_opts = build_commit_opts(author, credential, &state.fluree, &handle);

        // Turtle/TriG carries no body `opts`, so policy runs under root.
        // Tracking, however, is header-driven and applies to every format.
        let tracking = tracking_from_headers(headers);
        // (TriG, Insert) is rejected above and (Turtle/TriG, Update) is
        // unreachable here — none of the callers pass `TxnType::Update`
        // for a Turtle body, and the consensus `TransactionBody` has no
        // Turtle-Update variant.
        let body = match (is_trig, txn_type) {
            (false, TxnType::Insert) => TransactionBody::TurtleInsert(turtle.to_string()),
            (false, TxnType::Upsert) => TransactionBody::TurtleUpsert(turtle.to_string()),
            (true, TxnType::Upsert) => TransactionBody::TrigUpsert(turtle.to_string()),
            (true, TxnType::Insert) => unreachable!("rejected above"),
            (_, TxnType::Update) => unreachable!("Turtle callers never pass Update"),
        };
        let request = TransactionRequest {
            idempotency_key: extract_idempotency_key(&credential.headers),
            ledger_id: ledger_id.to_string(),
            body,
            txn_opts: TxnOpts::default(),
            commit_opts,
            tracking,
            governance: GovernanceOptions::default(),
        };
        transact_via_consensus(state, ledger_id, request, tx_id, &credential.headers).await
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

    // Resolve the ledger handle up front: a missing ledger surfaces as a 404
    // here, and the handle provides the canonical ledger ID for commit_opts.
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
    // For SPARQL UPDATE, impersonation is driven by the `fluree-identity`
    // header (there is no body-level opts block); the remaining policy inputs
    // come from the policy-class / policy / policy-values headers.
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
    let governance = GovernanceOptions {
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

    let commit_opts = build_commit_opts(
        effective_identity.as_deref(),
        credential,
        &state.fluree,
        &handle,
    );

    // The query is parsed and lowered inside the consensus layer, under the
    // ledger write lock — so namespace allocation shares the staging
    // registry and the namespace-conflict retry that pre-lowering required
    // is gone. Tracking is header-driven for SPARQL.
    let tracking = tracking_from_headers(headers);
    let request = TransactionRequest {
        idempotency_key: extract_idempotency_key(&credential.headers),
        ledger_id: ledger_id.clone(),
        body: TransactionBody::Sparql(sparql),
        txn_opts: TxnOpts::default(),
        commit_opts,
        tracking,
        governance,
    };
    transact_via_consensus(state, &ledger_id, request, tx_id, &credential.headers).await
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
