//! Ledger management endpoints: /fluree/create, /fluree/drop, /fluree/ledger-info

use crate::config::ServerRole;
use crate::error::{Result, ServerError};
use crate::extract::{FlureeHeaders, MaybeDataBearer};
use crate::state::AppState;
use crate::telemetry::{
    create_request_span, extract_request_id, extract_trace_id, set_span_error_code,
};
use axum::extract::{Path, Query, Request, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use fluree_db_api::wire::{ReindexRequest, ReindexResponse};
use fluree_db_api::{ApiError, BranchDropReport, DropMode, DropReport, DropStatus};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tracing::Instrument;

/// Commit information in create response
#[derive(Serialize)]
pub struct CommitInfo {
    /// Commit content identifier (CID), None for genesis (t=0, no commit exists)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commit_id: Option<String>,
    /// Commit hash (SHA-256) - empty for genesis
    pub hash: String,
}

/// Create ledger response - expected server format
#[derive(Serialize)]
pub struct CreateResponse {
    /// Ledger identifier
    pub ledger_id: String,
    /// Transaction time (t=0 for new empty ledger)
    pub t: i64,
    /// Transaction ID (SHA-256 hash of create request)
    #[serde(rename = "tx-id")]
    pub tx_id: String,
    /// Commit information
    pub commit: CommitInfo,
}

/// Compute transaction ID from request body (SHA-256 hash)
fn compute_tx_id(body: &JsonValue) -> String {
    let json_bytes = serde_json::to_vec(body).unwrap_or_default();
    let hash = Sha256::digest(&json_bytes);
    format!("fluree:tx:sha256:{}", hex::encode(hash))
}

/// Create a new ledger
///
/// POST /fluree/create
///
/// Creates a new empty ledger with genesis state.
/// To add data, use /fluree/insert, /fluree/upsert, or /fluree/update after creation.
///
/// Request body:
/// - `ledger`: Required ledger alias (e.g., "mydb" or "mydb:main")
///
/// Returns 201 Created on success, 409 Conflict if ledger already exists.
/// In peer mode, forwards the request to the transaction server.
pub async fn create(State(state): State<Arc<AppState>>, request: Request) -> Response {
    // In peer mode, forward to transaction server
    if state.config.server_role == ServerRole::Peer {
        return forward_write_request(&state, request).await;
    }

    create_local(state, request).await.into_response()
}

/// Local implementation of create
async fn create_local(state: Arc<AppState>, request: Request) -> Result<impl IntoResponse> {
    let headers_result = FlureeHeaders::from_headers(request.headers());
    let headers = match headers_result {
        Ok(h) => h,
        Err(e) => return Err(e),
    };

    // Read and parse body
    let body_bytes = axum::body::to_bytes(request.into_body(), 50 * 1024 * 1024)
        .await
        .map_err(|e| ServerError::bad_request(format!("Failed to read body: {e}")))?;
    let body: JsonValue = serde_json::from_slice(&body_bytes)
        .map_err(|e| ServerError::bad_request(format!("Invalid JSON: {e}")))?;

    // Create request span
    let request_id = extract_request_id(&headers.raw, &state.telemetry_config);
    let trace_id = extract_trace_id(&headers.raw);

    let span = create_request_span(
        "ledger:create",
        request_id.as_deref(),
        trace_id.as_deref(),
        None, // ledger alias determined later
        None,
        None, // no input format for ledger ops
    );
    async move {
        let span = tracing::Span::current();

        tracing::info!(status = "start", "ledger creation requested");

        // Extract ledger alias from body
        let alias = match body
            .get("ledger")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ServerError::bad_request("Missing required field: ledger"))
        {
            Ok(alias) => {
                span.record("ledger_id", alias);
                alias.to_string()
            }
            Err(e) => {
                set_span_error_code(&span, "error:BadRequest");
                tracing::warn!(error = %e, "missing ledger alias in create request");
                return Err(e);
            }
        };

        // Reject unexpected fields so callers don't silently lose data
        // (e.g. including `@graph` expecting initial data to be loaded).
        if let Some(obj) = body.as_object() {
            let unexpected: Vec<&str> = obj
                .keys()
                .filter(|k| k.as_str() != "ledger")
                .map(String::as_str)
                .collect();
            if !unexpected.is_empty() {
                let err = ServerError::bad_request(format!(
                    "Unexpected field(s) in create request: {}. \
                     POST /fluree/create only accepts the `ledger` field and creates an empty ledger. \
                     To add initial data, use POST /fluree/insert or /fluree/upsert after creation.",
                    unexpected.join(", ")
                ));
                set_span_error_code(&span, "error:BadRequest");
                tracing::warn!(
                    unexpected_fields = ?unexpected,
                    "rejected create request with unexpected fields"
                );
                return Err(err);
            }
        }

        // Compute tx-id from the request body
        let tx_id = compute_tx_id(&body);

        // Create the ledger (empty, t=0)
        // Ledger creation is only in transaction mode (peers forward)
        let ledger = match state.fluree.create_ledger(&alias).await {
            Ok(ledger) => ledger,
            Err(e) => {
                let server_error = ServerError::Api(e);
                set_span_error_code(&span, "error:AlreadyExists");
                tracing::error!(error = %server_error, "ledger creation failed");
                return Err(server_error);
            }
        };
        let ledger_id = ledger.ledger_id().to_string();

        let response = CreateResponse {
            ledger_id: ledger_id.clone(),
            t: 0,
            tx_id,
            commit: CommitInfo {
                commit_id: None, // genesis has no commit
                hash: String::new(),
            },
        };

        tracing::info!(status = "success", "ledger created");
        Ok((StatusCode::CREATED, Json(response)))
    }
    .instrument(span)
    .await
}

/// Drop ledger request body
#[derive(Deserialize)]
pub struct DropRequest {
    /// Ledger alias
    pub ledger: String,
    /// Hard drop (delete files) - default false
    #[serde(default)]
    pub hard: bool,
}

/// Drop ledger response
#[derive(Serialize)]
pub struct DropResponse {
    /// Ledger identifier
    pub ledger_id: String,
    /// Drop status
    pub status: String,
    /// Files deleted (hard mode only)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub files_deleted: Option<usize>,
    /// Warnings (if any)
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

impl From<DropReport> for DropResponse {
    fn from(report: DropReport) -> Self {
        let status = match report.status {
            DropStatus::Dropped => "dropped",
            DropStatus::AlreadyRetracted => "already_retracted",
            DropStatus::NotFound => "not_found",
        };

        let files_deleted = if report.artifacts_deleted > 0 {
            Some(report.artifacts_deleted)
        } else {
            None
        };

        DropResponse {
            ledger_id: report.ledger_id,
            status: status.to_string(),
            files_deleted,
            warnings: report.warnings,
        }
    }
}

/// Drop a ledger
///
/// POST /fluree/drop
///
/// Retracts a ledger from the nameservice.
/// With `hard: true`, also deletes all storage artifacts.
/// In peer mode, forwards the request to the transaction server.
pub async fn drop(State(state): State<Arc<AppState>>, request: Request) -> Response {
    // In peer mode, forward to transaction server
    if state.config.server_role == ServerRole::Peer {
        return forward_write_request(&state, request).await;
    }

    drop_local(state, request).await.into_response()
}

/// Local implementation of drop
async fn drop_local(state: Arc<AppState>, request: Request) -> Result<Json<DropResponse>> {
    let headers_result = FlureeHeaders::from_headers(request.headers());
    let headers = match headers_result {
        Ok(h) => h,
        Err(e) => return Err(e),
    };

    // Read and parse body
    let body_bytes = axum::body::to_bytes(request.into_body(), 50 * 1024 * 1024)
        .await
        .map_err(|e| ServerError::bad_request(format!("Failed to read body: {e}")))?;
    let req: DropRequest = serde_json::from_slice(&body_bytes)
        .map_err(|e| ServerError::bad_request(format!("Invalid JSON: {e}")))?;

    // Create request span
    let request_id = extract_request_id(&headers.raw, &state.telemetry_config);
    let trace_id = extract_trace_id(&headers.raw);

    let span = create_request_span(
        "ledger:drop",
        request_id.as_deref(),
        trace_id.as_deref(),
        Some(&req.ledger),
        None,
        None,
    );
    async move {
        let span = tracing::Span::current();

        tracing::info!(
            status = "start",
            hard_drop = req.hard,
            "ledger drop requested"
        );

        let mode = if req.hard {
            DropMode::Hard
        } else {
            DropMode::Soft
        };

        // Ledger drop is only in transaction mode (peers forward).
        // Try ledger first, then fall back to graph source if not found.
        let report = match state.fluree.drop_ledger(&req.ledger, mode).await {
            Ok(report) => report,
            Err(e) => {
                let server_error = ServerError::Api(e);
                set_span_error_code(&span, "error:NotFound");
                tracing::error!(error = %server_error, "ledger drop failed");
                return Err(server_error);
            }
        };

        // If ledger not found, try graph source drop
        if matches!(report.status, DropStatus::NotFound) {
            let gs_report = match state
                .fluree
                .drop_graph_source(&req.ledger, None, mode)
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    set_span_error_code(&span, "error:NotFound");
                    tracing::error!(error = %e, "graph source drop also failed");
                    return Err(ServerError::Api(e));
                }
            };

            let gs_status = match gs_report.status {
                DropStatus::Dropped => "dropped",
                DropStatus::AlreadyRetracted => "already_retracted",
                DropStatus::NotFound => "not_found",
            };

            tracing::info!(
                status = "success",
                drop_status = gs_status,
                "graph source dropped"
            );
            return Ok(Json(DropResponse {
                ledger_id: format!("{}:{}", gs_report.name, gs_report.branch),
                status: gs_status.to_string(),
                files_deleted: None,
                warnings: gs_report.warnings,
            }));
        }

        tracing::info!(status = "success", drop_status = ?report.status, "ledger dropped");
        Ok(Json(DropResponse::from(report)))
    }
    .instrument(span)
    .await
}

// =============================================================================
// Reindex
// =============================================================================

/// Full reindex from commit history.
///
/// POST /fluree/reindex
///
/// Rebuilds the binary index for a ledger from scratch using the server's
/// configured indexer settings. In peer mode, forwards to the transaction
/// server.
pub async fn reindex(State(state): State<Arc<AppState>>, request: Request) -> Response {
    if state.config.server_role == ServerRole::Peer {
        return forward_write_request(&state, request).await;
    }

    reindex_local(state, request).await.into_response()
}

async fn reindex_local(state: Arc<AppState>, request: Request) -> Result<Json<ReindexResponse>> {
    let headers = FlureeHeaders::from_headers(request.headers())?;

    let body_bytes = axum::body::to_bytes(request.into_body(), 50 * 1024 * 1024)
        .await
        .map_err(|e| ServerError::bad_request(format!("Failed to read body: {e}")))?;
    let req: ReindexRequest = serde_json::from_slice(&body_bytes)
        .map_err(|e| ServerError::bad_request(format!("Invalid JSON: {e}")))?;

    let request_id = extract_request_id(&headers.raw, &state.telemetry_config);
    let trace_id = extract_trace_id(&headers.raw);

    let span = create_request_span(
        "ledger:reindex",
        request_id.as_deref(),
        trace_id.as_deref(),
        Some(&req.ledger),
        None,
        None,
    );
    async move {
        let span = tracing::Span::current();
        tracing::info!(status = "start", ledger = %req.ledger, "ledger reindex requested");

        let result = match state
            .fluree
            .reindex(&req.ledger, fluree_db_api::ReindexOptions::default())
            .await
        {
            Ok(r) => r,
            Err(e) => {
                let server_error = ServerError::Api(e);
                set_span_error_code(&span, "error:ReindexFailed");
                tracing::error!(error = %server_error, "ledger reindex failed");
                return Err(server_error);
            }
        };

        tracing::info!(
            status = "success",
            index_t = result.index_t,
            "ledger reindex complete"
        );
        Ok(Json(ReindexResponse::from(result)))
    }
    .instrument(span)
    .await
}

// =============================================================================
// List ledgers + graph sources
// =============================================================================

/// Entry in the ledger/graph-source list response
#[derive(Serialize)]
pub struct ListEntry {
    pub name: String,
    pub branch: String,
    #[serde(rename = "type")]
    pub entry_type: String,
    pub t: i64,
}

/// List all ledgers and graph sources
///
/// GET /fluree/ledgers
pub async fn list_ledgers(State(state): State<Arc<AppState>>) -> Result<Json<Vec<ListEntry>>> {
    let ledger_records = state
        .fluree
        .nameservice()
        .all_records()
        .await
        .map_err(|e| ServerError::internal(format!("Failed to list ledgers: {e}")))?;

    let gs_records = state
        .fluree
        .nameservice()
        .all_graph_source_records()
        .await
        .map_err(|e| ServerError::internal(format!("Failed to list graph sources: {e}")))?;

    let mut entries = Vec::new();

    for r in &ledger_records {
        if r.retracted {
            continue;
        }
        entries.push(ListEntry {
            name: r.name.clone(),
            branch: r.branch.clone(),
            entry_type: "Ledger".to_string(),
            t: r.commit_t,
        });
    }

    for gs in &gs_records {
        if gs.retracted {
            continue;
        }
        entries.push(ListEntry {
            name: gs.name.clone(),
            branch: gs.branch.clone(),
            entry_type: format_source_type(&gs.source_type),
            t: gs.index_t,
        });
    }

    Ok(Json(entries))
}

fn format_source_type(st: &fluree_db_nameservice::GraphSourceType) -> String {
    match st {
        fluree_db_nameservice::GraphSourceType::Bm25 => "BM25".to_string(),
        fluree_db_nameservice::GraphSourceType::Vector => "Vector".to_string(),
        fluree_db_nameservice::GraphSourceType::Geo => "Geo".to_string(),
        fluree_db_nameservice::GraphSourceType::R2rml => "R2RML".to_string(),
        fluree_db_nameservice::GraphSourceType::Iceberg => "Iceberg".to_string(),
        fluree_db_nameservice::GraphSourceType::Unknown(s) => format!("Unknown({s})"),
    }
}

/// Build a JSON representation of a graph source record for the info endpoint.
fn graph_source_info_json(gs: &fluree_db_nameservice::GraphSourceRecord) -> JsonValue {
    let mut obj = serde_json::json!({
        "name": gs.name,
        "branch": gs.branch,
        "type": format_source_type(&gs.source_type),
        "graph_source_id": gs.graph_source_id,
        "retracted": gs.retracted,
        "index_t": gs.index_t,
    });

    if let Some(ref id) = gs.index_id {
        obj["index_id"] = serde_json::Value::String(id.to_string());
    }

    if !gs.dependencies.is_empty() {
        obj["dependencies"] = serde_json::json!(gs.dependencies);
    }

    // Include parsed config if non-empty
    if !gs.config.is_empty() && gs.config != "{}" {
        if let Ok(parsed) = serde_json::from_str::<JsonValue>(&gs.config) {
            obj["config"] = parsed;
        }
    }

    obj
}

// =============================================================================
// Info
// =============================================================================

/// Ledger info response (simplified, used in proxy storage mode fallback)
#[derive(Serialize)]
pub struct LedgerInfoResponse {
    /// Ledger identifier
    pub ledger_id: String,
    /// Current transaction time
    pub t: i64,
    /// Head commit ContentId (storage-agnostic identity), if known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commit_head_id: Option<fluree_db_core::ContentId>,
    /// Head index ContentId (storage-agnostic identity), if known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_head_id: Option<fluree_db_core::ContentId>,
}

/// Get ledger information
///
/// GET /fluree/ledger-info?ledger=<alias>
/// or with fluree-ledger header
///
/// In non-proxy mode (transaction server or peer with shared storage), returns
/// comprehensive ledger metadata including commit info, namespace codes, and
/// statistics (properties, classes with counts and hierarchy).
///
/// In proxy storage mode, returns simplified nameservice-only response since
/// the peer doesn't have local ledger state to compute stats from.
pub async fn info(
    State(state): State<Arc<AppState>>,
    headers: FlureeHeaders,
    bearer: MaybeDataBearer,
    query: axum::extract::Query<LedgerInfoQuery>,
) -> Result<Response> {
    // Create request span
    let request_id = extract_request_id(&headers.raw, &state.telemetry_config);
    let trace_id = extract_trace_id(&headers.raw);

    let span = create_request_span(
        "ledger:info",
        request_id.as_deref(),
        trace_id.as_deref(),
        None, // ledger alias determined later
        None,
        None,
    );
    async move {
        let span = tracing::Span::current();

        tracing::info!(status = "start", "ledger info requested");

        // Get ledger alias from query param or header
        let alias = match query
            .ledger
            .as_ref()
            .or(headers.ledger.as_ref())
            .ok_or(ServerError::MissingLedger)
        {
            Ok(alias) => {
                span.record("ledger_id", alias.as_str());
                alias
            }
            Err(e) => {
                set_span_error_code(&span, "error:BadRequest");
                tracing::warn!(error = %e, "missing ledger alias in info request");
                return Err(e);
            }
        };

        // Enforce data auth (ledger-info is a read operation; Bearer token only)
        let data_auth = state.config.data_auth();
        if data_auth.mode == crate::config::DataAuthMode::Required && bearer.0.is_none() {
            set_span_error_code(&span, "error:Unauthorized");
            return Err(ServerError::unauthorized("Bearer token required"));
        }
        if let Some(p) = bearer.0.as_ref() {
            if !p.can_read(alias) {
                set_span_error_code(&span, "error:Forbidden");
                // Avoid existence leak
                return Err(ServerError::not_found("Ledger not found"));
            }
        }

        // In proxy storage mode, return simplified nameservice-only response
        // (peer doesn't have local ledger state to compute full stats)
        if state.config.is_proxy_storage_mode() {
            return info_simplified(&state, alias, &span).await;
        }

        // Non-proxy mode: load ledger and return comprehensive info.
        // If ledger is not found, fall back to graph source lookup.
        let ledger_state = match super::query::load_ledger_for_query(&state, alias, &span).await {
            Ok(ls) => ls,
            Err(ServerError::Api(ref e)) if e.is_not_found() => {
                // Try graph source lookup
                if let Ok(Some(gs)) = state.fluree.nameservice().lookup_graph_source(alias).await {
                    tracing::info!(status = "success", "graph source info retrieved");
                    return Ok(Json(graph_source_info_json(&gs)).into_response());
                }
                set_span_error_code(&span, "error:NotFound");
                return Err(ServerError::Api(ApiError::NotFound(alias.to_string())));
            }
            Err(e) => return Err(e),
        };

        let t = ledger_state.snapshot.t;

        // Build comprehensive ledger info
        //
        // By default we return the full novelty-aware ledger-info payload. Query
        // params can opt into lighter/index-derived variants explicitly.
        let graph_selector = match query.graph.as_deref() {
            Some(name) => fluree_db_api::ledger_info::GraphSelector::ByName(name.to_string()),
            None => fluree_db_api::ledger_info::GraphSelector::Default,
        };
        let mut opts = fluree_db_api::ledger_info::LedgerInfoOptions {
            graph: graph_selector,
            ..Default::default()
        };
        if let Some(enabled) = query.realtime_property_details {
            opts.realtime_property_details = enabled;
        }
        if let Some(enabled) = query.include_property_datatypes {
            opts.include_property_datatypes = enabled;
        }
        if let Some(enabled) = query.include_property_estimates {
            opts.include_property_estimates = enabled;
        }

        let admin_storage = state
            .fluree
            .backend()
            .admin_storage_cloned()
            .ok_or_else(|| {
                ServerError::internal("ledger_info requires a managed storage backend")
            })?;
        let mut info = fluree_db_api::ledger_info::build_ledger_info_with_options(
            &ledger_state,
            &admin_storage,
            None,
            opts,
        )
        .await
        .map_err(|e| {
            set_span_error_code(&span, "error:InternalError");
            tracing::error!(error = %e, "failed to build ledger info");
            ServerError::internal(format!("Failed to build ledger info: {e}"))
        })?;

        if let Some(obj) = info.as_object_mut() {
            obj.insert(
                "ledger_id".to_string(),
                serde_json::Value::String(alias.to_string()),
            );
            obj.insert("t".to_string(), serde_json::Value::Number(t.into()));
        }

        tracing::info!(status = "success", "ledger info retrieved");
        Ok(Json(info).into_response())
    }
    .instrument(span)
    .await
}

/// Get ledger information with ledger as greedy tail segment.
///
/// GET /fluree/info/<ledger...>
pub async fn info_ledger_tail(
    State(state): State<Arc<AppState>>,
    Path(ledger): Path<String>,
    headers: FlureeHeaders,
    bearer: MaybeDataBearer,
    Query(mut query): Query<LedgerInfoQuery>,
) -> Result<Response> {
    query.ledger = Some(ledger);
    info(State(state), headers, bearer, axum::extract::Query(query)).await
}

/// Simplified ledger info for proxy storage mode (nameservice lookup only).
/// Falls back to graph source lookup if ledger is not found.
async fn info_simplified(state: &AppState, alias: &str, span: &tracing::Span) -> Result<Response> {
    // Lookup ledger in nameservice
    match state.fluree.nameservice().lookup(alias).await {
        Ok(Some(record)) => {
            tracing::info!(
                status = "success",
                commit_t = record.commit_t,
                "ledger info retrieved (simplified)"
            );
            return Ok(Json(LedgerInfoResponse {
                ledger_id: record.ledger_id.clone(),
                t: record.commit_t,
                commit_head_id: record.commit_head_id.clone(),
                index_head_id: record.index_head_id.clone(),
            })
            .into_response());
        }
        Ok(None) => { /* fall through to graph source lookup */ }
        Err(e) => {
            let server_error = ServerError::Api(ApiError::NameService(e));
            set_span_error_code(span, "error:InternalError");
            tracing::error!(error = %server_error, "nameservice lookup failed");
            return Err(server_error);
        }
    }

    // Try graph source lookup
    if let Ok(Some(gs)) = state.fluree.nameservice().lookup_graph_source(alias).await {
        tracing::info!(status = "success", "graph source info retrieved");
        return Ok(Json(graph_source_info_json(&gs)).into_response());
    }

    let server_error = ServerError::Api(ApiError::NotFound(alias.to_string()));
    set_span_error_code(span, "error:NotFound");
    tracing::warn!(error = %server_error, "not found as ledger or graph source");
    Err(server_error)
}

/// Query parameters for ledger-info
#[derive(Deserialize)]
pub struct LedgerInfoQuery {
    pub ledger: Option<String>,
    /// When false, use the lighter fast novelty-aware stats path instead of the
    /// default full lookup-backed ledger-info stats path.
    pub realtime_property_details: Option<bool>,
    /// When true, include `datatypes` under `stats.properties[*]`.
    pub include_property_datatypes: Option<bool>,
    /// When true, include index-derived NDV/selectivity estimates.
    pub include_property_estimates: Option<bool>,
    /// Which graph to scope stats to (e.g., “default”, “txn-meta”, or a graph IRI).
    /// Defaults to “default” (g_id = 0).
    pub graph: Option<String>,
}

/// Ledger exists response
#[derive(Serialize)]
pub struct ExistsResponse {
    /// Ledger identifier (echoed back)
    pub ledger_id: String,
    /// Whether the ledger exists
    pub exists: bool,
}

/// Check if a ledger exists
///
/// GET /fluree/exists?ledger=<alias>
/// or with fluree-ledger header
///
/// Returns a simple boolean response indicating whether the ledger
/// is registered in the nameservice. This is a lightweight check
/// that does not load the ledger data.
pub async fn exists(
    State(state): State<Arc<AppState>>,
    headers: FlureeHeaders,
    bearer: MaybeDataBearer,
    query: axum::extract::Query<LedgerInfoQuery>,
) -> Result<Json<ExistsResponse>> {
    // Create request span
    let request_id = extract_request_id(&headers.raw, &state.telemetry_config);
    let trace_id = extract_trace_id(&headers.raw);

    let span = create_request_span(
        "ledger:exists",
        request_id.as_deref(),
        trace_id.as_deref(),
        None,
        None,
        None,
    );
    async move {
        let span = tracing::Span::current();

        tracing::info!(status = "start", "ledger exists check requested");

        // Get ledger alias from query param or header
        let alias = match query
            .ledger
            .as_ref()
            .or(headers.ledger.as_ref())
            .ok_or(ServerError::MissingLedger)
        {
            Ok(alias) => {
                span.record("ledger_id", alias.as_str());
                alias.clone()
            }
            Err(e) => {
                set_span_error_code(&span, "error:BadRequest");
                tracing::warn!(error = %e, "missing ledger alias in exists request");
                return Err(e);
            }
        };

        // Enforce data auth (exists is a read operation; Bearer token only)
        let data_auth = state.config.data_auth();
        if data_auth.mode == crate::config::DataAuthMode::Required && bearer.0.is_none() {
            set_span_error_code(&span, "error:Unauthorized");
            return Err(ServerError::unauthorized("Bearer token required"));
        }
        if let Some(p) = bearer.0.as_ref() {
            if !p.can_read(&alias) {
                set_span_error_code(&span, "error:Forbidden");
                // Avoid existence leak
                return Err(ServerError::not_found("Ledger not found"));
            }
        }

        // Check if ledger exists via nameservice lookup
        let exists = match state.fluree.ledger_exists(&alias).await {
            Ok(exists) => exists,
            Err(e) => {
                let server_error = ServerError::Api(e);
                set_span_error_code(&span, "error:InternalError");
                tracing::error!(error = %server_error, "ledger exists check failed");
                return Err(server_error);
            }
        };

        tracing::info!(
            status = "success",
            exists = exists,
            "ledger exists check completed"
        );
        Ok(Json(ExistsResponse {
            ledger_id: alias,
            exists,
        }))
    }
    .instrument(span)
    .await
}

/// Check ledger existence with ledger as greedy tail segment.
///
/// GET /fluree/exists/<ledger...>
pub async fn exists_ledger_tail(
    State(state): State<Arc<AppState>>,
    Path(ledger): Path<String>,
    headers: FlureeHeaders,
    bearer: MaybeDataBearer,
    Query(mut query): Query<LedgerInfoQuery>,
) -> Result<Json<ExistsResponse>> {
    query.ledger = Some(ledger);
    exists(State(state), headers, bearer, axum::extract::Query(query)).await
}

// ── Branch endpoints ──────────────────────────────────────────────────────

/// Branch info in list response
#[derive(Serialize)]
pub struct BranchInfo {
    /// Branch name (e.g., "main", "feature-x")
    pub branch: String,
    /// Full ledger:branch identifier
    pub ledger_id: String,
    /// Current transaction time on this branch
    pub t: i64,
    /// Source branch this was created from, if any
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
}

/// Create branch request body
#[derive(Deserialize)]
pub struct CreateBranchRequest {
    /// Ledger name (e.g., "mydb")
    pub ledger: String,
    /// New branch name (e.g., "feature-x")
    pub branch: String,
    /// Source branch to create from (defaults to "main")
    #[serde(default)]
    pub source: Option<String>,
    /// Optional commit reference to branch at.
    ///
    /// Accepts `"t:N"` for a transaction number or a hex digest / full CID
    /// for prefix resolution. When omitted, the branch starts at the source
    /// branch's current HEAD.
    #[serde(default)]
    pub at: Option<String>,
}

/// Create branch response
#[derive(Serialize)]
pub struct CreateBranchResponse {
    /// Full ledger:branch identifier
    pub ledger_id: String,
    /// Branch name
    pub branch: String,
    /// Source branch this was created from
    pub source: String,
    /// Transaction time at branch point
    pub t: i64,
}

/// Create a new branch
///
/// POST /fluree/branch
///
/// Request body:
/// - `ledger`: Ledger name (e.g., "mydb")
/// - `branch`: New branch name (e.g., "feature-x")
/// - `source`: Source branch (optional, defaults to "main")
///
/// Returns 201 Created on success, 409 Conflict if branch already exists,
/// 404 Not Found if source branch does not exist.
pub async fn create_branch(State(state): State<Arc<AppState>>, request: Request) -> Response {
    if state.config.server_role == ServerRole::Peer {
        return forward_write_request(&state, request).await;
    }

    create_branch_local(state, request).await.into_response()
}

async fn create_branch_local(state: Arc<AppState>, request: Request) -> Result<impl IntoResponse> {
    let (parts, body) = request.into_parts();
    let headers = FlureeHeaders::from_headers(&parts.headers)?;

    let body_bytes = axum::body::to_bytes(body, 50 * 1024 * 1024)
        .await
        .map_err(|e| ServerError::bad_request(format!("Failed to read body: {e}")))?;
    let req: CreateBranchRequest = serde_json::from_slice(&body_bytes)
        .map_err(|e| ServerError::bad_request(format!("Invalid JSON: {e}")))?;

    let source = req.source.unwrap_or_else(|| "main".to_string());
    let ledger = req.ledger;
    let branch = req.branch;

    let at_commit = match req.at.as_deref() {
        Some(s) => Some(
            fluree_db_api::CommitRef::parse(s)
                .map_err(|e| ServerError::bad_request(e.to_string()))?,
        ),
        None => None,
    };

    let request_id = extract_request_id(&headers.raw, &state.telemetry_config);
    let trace_id = extract_trace_id(&headers.raw);
    let span = create_request_span(
        "branch:create",
        request_id.as_deref(),
        trace_id.as_deref(),
        Some(&ledger),
        None,
        None,
    );

    async move {
        let span = tracing::Span::current();

        tracing::info!(
            status = "start",
            branch = %branch,
            source = %source,
            "branch creation requested"
        );

        let record = match state
            .fluree
            .create_branch(&ledger, &branch, Some(&source), at_commit)
            .await
        {
            Ok(record) => record,
            Err(e) => {
                let server_error = ServerError::Api(e);
                set_span_error_code(&span, "error:BranchCreateFailed");
                tracing::error!(error = %server_error, "branch creation failed");
                return Err(server_error);
            }
        };

        let response = CreateBranchResponse {
            ledger_id: record.ledger_id.clone(),
            branch: record.branch.clone(),
            source: record.source_branch.unwrap_or_default(),
            t: record.commit_t,
        };

        tracing::info!(status = "success", "branch created");
        Ok((StatusCode::CREATED, Json(response)))
    }
    .instrument(span)
    .await
}

/// List branches for a ledger
///
/// GET /fluree/branch/*ledger
///
/// Returns all non-retracted branches for the specified ledger.
pub async fn list_branches(
    State(state): State<Arc<AppState>>,
    Path(ledger): Path<String>,
    headers: FlureeHeaders,
    bearer: MaybeDataBearer,
) -> Result<Json<Vec<BranchInfo>>> {
    let request_id = extract_request_id(&headers.raw, &state.telemetry_config);
    let trace_id = extract_trace_id(&headers.raw);

    let span = create_request_span(
        "branch:list",
        request_id.as_deref(),
        trace_id.as_deref(),
        Some(&ledger),
        None,
        None,
    );
    async move {
        let span = tracing::Span::current();

        // Enforce data auth (list-branches is a read operation; Bearer token only)
        let data_auth = state.config.data_auth();
        if data_auth.mode == crate::config::DataAuthMode::Required && bearer.0.is_none() {
            set_span_error_code(&span, "error:Unauthorized");
            return Err(ServerError::unauthorized("Bearer token required"));
        }
        if let Some(p) = bearer.0.as_ref() {
            if !p.can_read(&ledger) {
                set_span_error_code(&span, "error:Forbidden");
                return Err(ServerError::not_found("Ledger not found"));
            }
        }

        let records = state
            .fluree
            .list_branches(&ledger)
            .await
            .map_err(ServerError::Api)?;

        let branches = records
            .into_iter()
            .map(|r| BranchInfo {
                branch: r.branch,
                ledger_id: r.ledger_id,
                t: r.commit_t,
                source: r.source_branch,
            })
            .collect();

        Ok(Json(branches))
    }
    .instrument(span)
    .await
}

// =============================================================================
// Drop Branch
// =============================================================================

/// Drop branch request body
#[derive(Deserialize)]
pub struct DropBranchRequest {
    /// Ledger name (e.g., "mydb")
    pub ledger: String,
    /// Branch name to drop (e.g., "feature-x")
    pub branch: String,
}

/// Drop branch response
#[derive(Serialize)]
pub struct DropBranchResponse {
    /// Full ledger:branch identifier
    pub ledger_id: String,
    /// Drop status
    pub status: String,
    /// Whether the drop was deferred (branch has children)
    pub deferred: bool,
    /// Files deleted
    #[serde(skip_serializing_if = "Option::is_none")]
    pub files_deleted: Option<usize>,
    /// Ancestor branches that were cascade-dropped
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub cascaded: Vec<String>,
    /// Warnings (if any)
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

impl From<BranchDropReport> for DropBranchResponse {
    fn from(report: BranchDropReport) -> Self {
        let status = match report.status {
            DropStatus::Dropped => "dropped",
            DropStatus::AlreadyRetracted => "already_retracted",
            DropStatus::NotFound => "not_found",
        };

        let files_deleted = if report.artifacts_deleted > 0 {
            Some(report.artifacts_deleted)
        } else {
            None
        };

        DropBranchResponse {
            ledger_id: report.ledger_id,
            status: status.to_string(),
            deferred: report.deferred,
            files_deleted,
            cascaded: report.cascaded,
            warnings: report.warnings,
        }
    }
}

/// Drop a branch
///
/// POST /fluree/drop-branch
///
/// Request body:
/// - `ledger`: Ledger name (e.g., "mydb")
/// - `branch`: Branch name to drop (e.g., "feature-x")
///
/// Cannot drop the "main" branch.
pub async fn drop_branch(State(state): State<Arc<AppState>>, request: Request) -> Response {
    if state.config.server_role == ServerRole::Peer {
        return forward_write_request(&state, request).await;
    }

    drop_branch_local(state, request).await.into_response()
}

async fn drop_branch_local(
    state: Arc<AppState>,
    request: Request,
) -> Result<Json<DropBranchResponse>> {
    let headers_result = FlureeHeaders::from_headers(request.headers());
    let headers = match headers_result {
        Ok(h) => h,
        Err(e) => return Err(e),
    };

    let body_bytes = axum::body::to_bytes(request.into_body(), 50 * 1024 * 1024)
        .await
        .map_err(|e| ServerError::bad_request(format!("Failed to read body: {e}")))?;
    let req: DropBranchRequest = serde_json::from_slice(&body_bytes)
        .map_err(|e| ServerError::bad_request(format!("Invalid JSON: {e}")))?;

    let request_id = extract_request_id(&headers.raw, &state.telemetry_config);
    let trace_id = extract_trace_id(&headers.raw);

    let span = create_request_span(
        "branch:drop",
        request_id.as_deref(),
        trace_id.as_deref(),
        Some(&req.ledger),
        None,
        None,
    );
    async move {
        let span = tracing::Span::current();

        tracing::info!(
            status = "start",
            branch = %req.branch,
            "branch drop requested"
        );

        let report = match state.fluree.drop_branch(&req.ledger, &req.branch).await {
            Ok(report) => report,
            Err(e) => {
                let server_error = ServerError::Api(e);
                set_span_error_code(&span, "error:BranchDropFailed");
                tracing::error!(error = %server_error, "branch drop failed");
                return Err(server_error);
            }
        };

        tracing::info!(
            status = "success",
            deferred = report.deferred,
            "branch dropped"
        );
        Ok(Json(DropBranchResponse::from(report)))
    }
    .instrument(span)
    .await
}

// =============================================================================
// Rebase Branch
// =============================================================================

/// Rebase branch request body
#[derive(Deserialize)]
pub struct RebaseBranchRequest {
    /// Ledger name (e.g., "mydb")
    pub ledger: String,
    /// Branch name to rebase (e.g., "feature-x")
    pub branch: String,
    /// Conflict resolution strategy (optional, defaults to "take-both")
    #[serde(default)]
    pub strategy: Option<String>,
}

/// Rebase branch response
#[derive(Serialize)]
pub struct RebaseBranchResponse {
    /// Full ledger:branch identifier
    pub ledger_id: String,
    /// Branch name
    pub branch: String,
    /// Whether this was a fast-forward (no unique branch commits)
    pub fast_forward: bool,
    /// Number of commits replayed
    pub replayed: usize,
    /// Number of commits skipped
    pub skipped: usize,
    /// Number of conflicts detected
    pub conflicts: usize,
    /// Number of failures
    pub failures: usize,
    /// Total commits considered
    pub total_commits: usize,
    /// Source branch HEAD t after rebase
    pub source_head_t: i64,
}

/// Rebase a branch onto its source branch's current HEAD
///
/// POST /fluree/rebase
///
/// Request body:
/// - `ledger`: Ledger name (e.g., "mydb")
/// - `branch`: Branch name to rebase (e.g., "feature-x")
/// - `strategy`: Conflict strategy (optional: "take-both", "abort", "take-source", "take-branch", "skip")
///
/// Cannot rebase the "main" branch.
pub async fn rebase(State(state): State<Arc<AppState>>, request: Request) -> Response {
    if state.config.server_role == ServerRole::Peer {
        return forward_write_request(&state, request).await;
    }

    rebase_local(state, request).await.into_response()
}

async fn rebase_local(state: Arc<AppState>, request: Request) -> Result<impl IntoResponse> {
    let (parts, body) = request.into_parts();
    let headers = FlureeHeaders::from_headers(&parts.headers)?;

    let body_bytes = axum::body::to_bytes(body, 50 * 1024 * 1024)
        .await
        .map_err(|e| ServerError::bad_request(format!("Failed to read body: {e}")))?;
    let req: RebaseBranchRequest = serde_json::from_slice(&body_bytes)
        .map_err(|e| ServerError::bad_request(format!("Invalid JSON: {e}")))?;

    let strategy = match req.strategy.as_deref() {
        Some(s) => fluree_db_api::ConflictStrategy::from_str_name(s)
            .ok_or_else(|| ServerError::bad_request(format!("Unknown conflict strategy: {s}")))?,
        None => fluree_db_api::ConflictStrategy::default(),
    };

    let request_id = extract_request_id(&headers.raw, &state.telemetry_config);
    let trace_id = extract_trace_id(&headers.raw);
    let span = create_request_span(
        "branch:rebase",
        request_id.as_deref(),
        trace_id.as_deref(),
        Some(&req.ledger),
        None,
        None,
    );

    async move {
        let span = tracing::Span::current();

        tracing::info!(
            status = "start",
            branch = %req.branch,
            strategy = strategy.as_str(),
            "branch rebase requested"
        );

        let report = match state
            .fluree
            .rebase_branch(&req.ledger, &req.branch, strategy)
            .await
        {
            Ok(report) => report,
            Err(e) => {
                let server_error = ServerError::Api(e);
                set_span_error_code(&span, "error:BranchRebaseFailed");
                tracing::error!(error = %server_error, "branch rebase failed");
                return Err(server_error);
            }
        };

        let ledger_id = fluree_db_core::ledger_id::format_ledger_id(&req.ledger, &req.branch);
        let response = RebaseBranchResponse {
            ledger_id,
            branch: req.branch,
            fast_forward: report.fast_forward,
            replayed: report.replayed,
            skipped: report.skipped,
            conflicts: report.conflicts.len(),
            failures: report.failures.len(),
            total_commits: report.total_commits,
            source_head_t: report.source_head_t,
        };

        tracing::info!(
            status = "success",
            fast_forward = report.fast_forward,
            replayed = report.replayed,
            "branch rebased"
        );
        Ok((StatusCode::OK, Json(response)))
    }
    .instrument(span)
    .await
}

// ============================================================================
// Merge
// ============================================================================

/// Merge branch request body
#[derive(Deserialize)]
pub struct MergeBranchRequest {
    /// Ledger name (e.g., "mydb")
    pub ledger: String,
    /// Source branch to merge from (e.g., "feature-x")
    pub source: String,
    /// Target branch to merge into (defaults to the source's parent branch)
    #[serde(default)]
    pub target: Option<String>,
    /// Conflict resolution strategy (optional, defaults to "take-both")
    #[serde(default)]
    pub strategy: Option<String>,
}

/// Merge branch response
#[derive(Serialize)]
pub struct MergeBranchResponse {
    /// Full ledger:branch identifier of the target
    pub ledger_id: String,
    /// Target branch name
    pub target: String,
    /// Source branch name
    pub source: String,
    /// Whether this was a fast-forward merge
    pub fast_forward: bool,
    /// New commit HEAD t of the target
    pub new_head_t: i64,
    /// Number of commit blobs copied to the target namespace
    pub commits_copied: usize,
    /// Number of conflicts detected
    pub conflict_count: usize,
    /// Conflict strategy used (None for fast-forward)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub strategy: Option<String>,
}

/// Merge a source branch into a target branch
///
/// POST /fluree/merge
///
/// Request body:
/// - `ledger`: Ledger name (e.g., "mydb")
/// - `source`: Source branch to merge from (e.g., "feature-x")
/// - `target`: Target branch to merge into (optional, defaults to source's parent)
pub async fn merge(State(state): State<Arc<AppState>>, request: Request) -> Response {
    if state.config.server_role == ServerRole::Peer {
        return forward_write_request(&state, request).await;
    }

    merge_local(state, request).await.into_response()
}

async fn merge_local(state: Arc<AppState>, request: Request) -> Result<impl IntoResponse> {
    let (parts, body) = request.into_parts();
    let headers = FlureeHeaders::from_headers(&parts.headers)?;

    let body_bytes = axum::body::to_bytes(body, 50 * 1024 * 1024)
        .await
        .map_err(|e| ServerError::bad_request(format!("Failed to read body: {e}")))?;
    let req: MergeBranchRequest = serde_json::from_slice(&body_bytes)
        .map_err(|e| ServerError::bad_request(format!("Invalid JSON: {e}")))?;

    let request_id = extract_request_id(&headers.raw, &state.telemetry_config);
    let trace_id = extract_trace_id(&headers.raw);
    let span = create_request_span(
        "branch:merge",
        request_id.as_deref(),
        trace_id.as_deref(),
        Some(&req.ledger),
        None,
        None,
    );

    async move {
        let span = tracing::Span::current();

        let strategy = match req.strategy.as_deref() {
            Some(s) => fluree_db_api::ConflictStrategy::from_str_name(s).ok_or_else(|| {
                ServerError::bad_request(format!("Unknown conflict strategy: {s}"))
            })?,
            None => fluree_db_api::ConflictStrategy::default(),
        };

        tracing::info!(
            status = "start",
            source = %req.source,
            target = ?req.target,
            strategy = strategy.as_str(),
            "branch merge requested"
        );

        let report = match state
            .fluree
            .merge_branch(&req.ledger, &req.source, req.target.as_deref(), strategy)
            .await
        {
            Ok(report) => report,
            Err(e) => {
                let server_error = ServerError::Api(e);
                set_span_error_code(&span, "error:BranchMergeFailed");
                tracing::error!(error = %server_error, "branch merge failed");
                return Err(server_error);
            }
        };

        let ledger_id = fluree_db_core::ledger_id::format_ledger_id(&req.ledger, &report.target);
        let response = MergeBranchResponse {
            ledger_id,
            target: report.target,
            source: report.source,
            fast_forward: report.fast_forward,
            new_head_t: report.new_head_t,
            commits_copied: report.commits_copied,
            conflict_count: report.conflict_count,
            strategy: report.strategy,
        };

        tracing::info!(
            status = "success",
            fast_forward = report.fast_forward,
            "branch merged"
        );
        Ok((StatusCode::OK, Json(response)))
    }
    .instrument(span)
    .await
}

// ============================================================================
// Revert
// ============================================================================

/// Revert request body.
///
/// Exactly one of `commit`, `commits`, or `range` must be supplied; the server
/// rejects requests that omit all three or supply more than one.
///
/// Each commit-reference field accepts the same string forms parsed by
/// [`fluree_db_api::CommitRef::parse`]: `t:N`, a hex digest prefix, or a full
/// commit ID.
#[derive(Deserialize)]
pub struct RevertRequest {
    /// Ledger name (e.g., "mydb").
    pub ledger: String,
    /// Branch the revert commit will be written to (e.g., "main").
    pub branch: String,
    /// Conflict resolution strategy. Defaults to `abort` so callers must
    /// explicitly opt in to automatic resolution. Accepted values:
    /// `abort`, `take-source`, `take-branch`.
    #[serde(default)]
    pub strategy: Option<String>,
    /// Single commit to revert. Mutually exclusive with `commits`/`range`.
    #[serde(default)]
    pub commit: Option<String>,
    /// Set of commits to revert (cherry-pick style). Mutually exclusive with
    /// `commit`/`range`.
    #[serde(default)]
    pub commits: Option<Vec<String>>,
    /// Git-style range `from..to` (`from` exclusive, `to` inclusive). Mutually
    /// exclusive with `commit`/`commits`.
    #[serde(default)]
    pub range: Option<RevertRangeBody>,
}

#[derive(Deserialize)]
pub struct RevertRangeBody {
    pub from: String,
    pub to: String,
}

#[derive(Serialize)]
pub struct RevertResponse {
    /// Full ledger:branch identifier the revert was written to.
    pub ledger_id: String,
    /// Branch the revert was written to.
    pub branch: String,
    /// Commit IDs reverted (newest-first, the order applied).
    pub reverted_commits: Vec<fluree_db_api::ContentId>,
    /// Number of `(s, p, g)` keys that conflicted before resolution.
    pub conflict_count: usize,
    /// Conflict-resolution strategy applied.
    pub strategy: String,
    /// `t` of the freshly written revert commit.
    pub new_head_t: i64,
    /// Commit ID of the freshly written revert commit.
    pub new_head_id: fluree_db_api::ContentId,
}

/// Revert one or more commits on a branch.
///
/// `POST /fluree/revert`
///
/// See [`RevertRequest`] for the body shape.
pub async fn revert(State(state): State<Arc<AppState>>, request: Request) -> Response {
    if state.config.server_role == ServerRole::Peer {
        return forward_write_request(&state, request).await;
    }

    revert_local(state, request).await.into_response()
}

async fn revert_local(state: Arc<AppState>, request: Request) -> Result<impl IntoResponse> {
    let (parts, body) = request.into_parts();
    let headers = FlureeHeaders::from_headers(&parts.headers)?;

    let body_bytes = axum::body::to_bytes(body, 50 * 1024 * 1024)
        .await
        .map_err(|e| ServerError::bad_request(format!("Failed to read body: {e}")))?;
    let req: RevertRequest = serde_json::from_slice(&body_bytes)
        .map_err(|e| ServerError::bad_request(format!("Invalid JSON: {e}")))?;

    let request_id = extract_request_id(&headers.raw, &state.telemetry_config);
    let trace_id = extract_trace_id(&headers.raw);
    let span = create_request_span(
        "branch:revert",
        request_id.as_deref(),
        trace_id.as_deref(),
        Some(&req.ledger),
        None,
        None,
    );

    async move {
        let span = tracing::Span::current();

        let strategy = match req.strategy.as_deref() {
            Some(s) => fluree_db_api::ConflictStrategy::from_str_name(s).ok_or_else(|| {
                ServerError::bad_request(format!("Unknown conflict strategy: {s}"))
            })?,
            None => fluree_db_api::ConflictStrategy::Abort,
        };

        let provided = [
            req.commit.is_some(),
            req.commits.is_some(),
            req.range.is_some(),
        ]
        .iter()
        .filter(|p| **p)
        .count();
        if provided != 1 {
            return Err(ServerError::bad_request(
                "Exactly one of `commit`, `commits`, or `range` must be provided".to_string(),
            ));
        }

        let report = if let Some(commit) = req.commit {
            let commit_ref = parse_commit_ref(&commit)?;
            tracing::info!(
                status = "start",
                branch = %req.branch,
                strategy = strategy.as_str(),
                "branch revert (single) requested"
            );
            state
                .fluree
                .revert_commit(&req.ledger, &req.branch, commit_ref, strategy)
                .await
        } else if let Some(commits) = req.commits {
            let parsed: std::result::Result<Vec<_>, _> =
                commits.iter().map(|s| parse_commit_ref(s)).collect();
            let parsed = parsed?;
            tracing::info!(
                status = "start",
                branch = %req.branch,
                count = parsed.len(),
                strategy = strategy.as_str(),
                "branch revert (set) requested"
            );
            state
                .fluree
                .revert_commits(&req.ledger, &req.branch, parsed, strategy)
                .await
        } else if let Some(range) = req.range {
            let from = parse_commit_ref(&range.from)?;
            let to = parse_commit_ref(&range.to)?;
            tracing::info!(
                status = "start",
                branch = %req.branch,
                strategy = strategy.as_str(),
                "branch revert (range) requested"
            );
            state
                .fluree
                .revert_range(&req.ledger, &req.branch, from, to, strategy)
                .await
        } else {
            unreachable!("validated above");
        };

        let report = match report {
            Ok(report) => report,
            Err(e) => {
                let server_error = ServerError::Api(e);
                set_span_error_code(&span, "error:BranchRevertFailed");
                tracing::error!(error = %server_error, "branch revert failed");
                return Err(server_error);
            }
        };

        let ledger_id = fluree_db_core::ledger_id::format_ledger_id(&req.ledger, &report.branch);
        let response = RevertResponse {
            ledger_id,
            branch: report.branch,
            reverted_commits: report.reverted_commits,
            conflict_count: report.conflict_count,
            strategy: report.strategy,
            new_head_t: report.new_head_t,
            new_head_id: report.new_head_id,
        };

        tracing::info!(
            status = "success",
            new_head_t = response.new_head_t,
            "branch revert succeeded"
        );
        Ok((StatusCode::OK, Json(response)))
    }
    .instrument(span)
    .await
}

fn parse_commit_ref(s: &str) -> Result<fluree_db_api::CommitRef> {
    fluree_db_api::CommitRef::parse(s)
        .map_err(|e| ServerError::bad_request(format!("Invalid commit reference {s:?}: {e}")))
}

// ============================================================================
// Merge Preview (read-only)
// ============================================================================

/// Hard cap on `max_commits` — clamps the per-side commit list returned to
/// the client regardless of what they request. 10x the recommended default.
///
/// **What this protects:** response body size and the per-commit
/// `load_commit_by_id` reads (one full commit blob loaded per summary
/// returned).
///
/// **What this does NOT protect:** the underlying divergence walk. The
/// `count` field on each side reflects the full unbounded divergence —
/// computed by walking every commit envelope between HEAD and the common
/// ancestor — so a request against branches diverged by N commits costs N
/// envelope reads regardless of the cap. If you need to reject huge
/// divergences, add an operational guard before invoking the walk
/// (e.g., refuse when ancestor.t < target.t - SOME_LIMIT).
const PREVIEW_HARD_MAX_COMMITS: usize = 5_000;

/// Hard cap on `max_conflict_keys`. 25x the recommended default.
///
/// **What this protects:** the size of `conflicts.keys` in the response.
///
/// **What this does NOT protect:** the conflict computation. When
/// `include_conflicts=true`, both `compute_delta_keys` walks scan the full
/// per-side delta regardless of cap. Clients that need a fast preview
/// should pass `include_conflicts=false`.
const PREVIEW_HARD_MAX_CONFLICT_KEYS: usize = 5_000;

/// Query parameters for [`merge_preview`].
#[derive(Deserialize)]
pub struct MergePreviewQuery {
    /// Source branch.
    pub source: String,
    /// Target branch (optional; defaults to the source's parent).
    #[serde(default)]
    pub target: Option<String>,
    /// Cap on per-side commit list. Defaults to 500.
    #[serde(default)]
    pub max_commits: Option<usize>,
    /// Cap on conflict keys returned. Defaults to 200.
    #[serde(default)]
    pub max_conflict_keys: Option<usize>,
    /// Skip the conflict computation when only counts are needed. Defaults to true.
    #[serde(default)]
    pub include_conflicts: Option<bool>,
    /// Include source/target flake values for returned conflict keys.
    #[serde(default)]
    pub include_conflict_details: Option<bool>,
    /// Strategy used for conflict resolution labels. Defaults to take-both.
    #[serde(default)]
    pub strategy: Option<String>,
}

/// Read-only branch merge preview.
///
/// GET /fluree/merge-preview/*ledger?source=&target=&max_commits=&max_conflict_keys=&include_conflicts=
///
/// Returns a JSON [`fluree_db_api::MergePreview`] with ahead/behind commit
/// summaries, conflict keys, and fast-forward eligibility — without mutating
/// any ledger state.
pub async fn merge_preview(
    State(state): State<Arc<AppState>>,
    Path(ledger): Path<String>,
    Query(params): Query<MergePreviewQuery>,
    headers: FlureeHeaders,
    bearer: MaybeDataBearer,
) -> Result<Json<fluree_db_api::MergePreview>> {
    let request_id = extract_request_id(&headers.raw, &state.telemetry_config);
    let trace_id = extract_trace_id(&headers.raw);

    let span = create_request_span(
        "branch:merge-preview",
        request_id.as_deref(),
        trace_id.as_deref(),
        Some(&ledger),
        None,
        None,
    );
    async move {
        let span = tracing::Span::current();

        // Same auth pattern as list_branches: Bearer required when configured.
        let data_auth = state.config.data_auth();
        if data_auth.mode == crate::config::DataAuthMode::Required && bearer.0.is_none() {
            set_span_error_code(&span, "error:Unauthorized");
            return Err(ServerError::unauthorized("Bearer token required"));
        }
        if let Some(p) = bearer.0.as_ref() {
            if !p.can_read(&ledger) {
                set_span_error_code(&span, "error:Forbidden");
                return Err(ServerError::not_found("Ledger not found"));
            }
        }

        // Start from the API's default (which carries the recommended caps)
        // and override only fields the caller supplied. Caller values are
        // additionally clamped to the server-side hard maximums so a
        // request like `max_commits=10000000` cannot blow out the response
        // body or force unbounded `load_commit_by_id` reads. The
        // *divergence walk itself* (envelope BFS via `collect_dag_cids`)
        // is unaffected — see the `MERGE_PREVIEW_HARD_MAX_*` constant
        // comments above. Contract documented in
        // docs/cli/server-integration.md (§Merge Preview Contract, rule 10).
        let mut opts = fluree_db_api::MergePreviewOpts::default();
        if let Some(n) = params.max_commits {
            opts.max_commits = Some(n.min(PREVIEW_HARD_MAX_COMMITS));
        }
        if let Some(n) = params.max_conflict_keys {
            opts.max_conflict_keys = Some(n.min(PREVIEW_HARD_MAX_CONFLICT_KEYS));
        }
        if let Some(b) = params.include_conflicts {
            opts.include_conflicts = b;
        }
        if let Some(b) = params.include_conflict_details {
            opts.include_conflict_details = b;
        }
        if opts.include_conflict_details && !opts.include_conflicts {
            return Err(ServerError::bad_request(
                "include_conflict_details requires include_conflicts=true",
            ));
        }
        if let Some(s) = params.strategy.as_deref() {
            opts.conflict_strategy =
                fluree_db_api::ConflictStrategy::parse_canonical(s).map_err(|_| {
                    ServerError::bad_request(format!("Unknown merge preview strategy: {s}"))
                })?;
            if opts.conflict_strategy == fluree_db_api::ConflictStrategy::Skip {
                return Err(ServerError::bad_request(
                    "Skip strategy is not supported for merge preview",
                ));
            }
        }
        if opts.conflict_strategy == fluree_db_api::ConflictStrategy::Abort
            && !opts.include_conflicts
        {
            return Err(ServerError::bad_request(
                "strategy=abort requires include_conflicts=true for mergeable preview",
            ));
        }

        let preview = state
            .fluree
            .merge_preview_with(&ledger, &params.source, params.target.as_deref(), opts)
            .await
            .map_err(ServerError::Api)?;

        Ok(Json(preview))
    }
    .instrument(span)
    .await
}

// ============================================================================
// Revert Preview (read-only)
// ============================================================================

/// Query parameters for [`revert_preview`].
///
/// Exactly one of `commit`, `commits`, or (`from`, `to`) must be supplied.
/// `commits` is comma-separated to keep the URL-encoded shape compact for
/// modest sets; clients with many CIDs should call the mutating endpoint
/// (`POST /revert`) which accepts a JSON array.
#[derive(Deserialize)]
pub struct RevertPreviewQuery {
    /// Branch the revert would be applied to.
    pub branch: String,
    /// Single commit reference (mutually exclusive with `commits`/`from`/`to`).
    #[serde(default)]
    pub commit: Option<String>,
    /// Comma-separated list of commit references (mutually exclusive with
    /// `commit`/`from`/`to`).
    #[serde(default)]
    pub commits: Option<String>,
    /// Range start, exclusive. Requires `to`.
    #[serde(default)]
    pub from: Option<String>,
    /// Range end, inclusive. Requires `from`.
    #[serde(default)]
    pub to: Option<String>,
    /// Cap on returned commit list. Defaults to 500.
    #[serde(default)]
    pub max_commits: Option<usize>,
    /// Cap on returned conflict keys. Defaults to 200.
    #[serde(default)]
    pub max_conflict_keys: Option<usize>,
    /// Skip conflict computation when only counts are needed. Defaults to true.
    #[serde(default)]
    pub include_conflicts: Option<bool>,
    /// Strategy used for the `revertable` verdict. Defaults to `abort`.
    #[serde(default)]
    pub strategy: Option<String>,
}

/// Read-only revert preview.
///
/// `GET /fluree/revert-preview/*ledger?branch=&commit=&commits=&from=&to=&strategy=&max_commits=&max_conflict_keys=&include_conflicts=`
///
/// Returns a [`fluree_db_api::RevertPreview`] describing what a revert with
/// the given selection would do — without writing a commit.
pub async fn revert_preview(
    State(state): State<Arc<AppState>>,
    Path(ledger): Path<String>,
    Query(params): Query<RevertPreviewQuery>,
    headers: FlureeHeaders,
    bearer: MaybeDataBearer,
) -> Result<Json<fluree_db_api::RevertPreview>> {
    let request_id = extract_request_id(&headers.raw, &state.telemetry_config);
    let trace_id = extract_trace_id(&headers.raw);
    let span = create_request_span(
        "branch:revert-preview",
        request_id.as_deref(),
        trace_id.as_deref(),
        Some(&ledger),
        None,
        None,
    );
    async move {
        let span = tracing::Span::current();

        // Same data-auth pattern as merge-preview.
        let data_auth = state.config.data_auth();
        if data_auth.mode == crate::config::DataAuthMode::Required && bearer.0.is_none() {
            set_span_error_code(&span, "error:Unauthorized");
            return Err(ServerError::unauthorized("Bearer token required"));
        }
        if let Some(p) = bearer.0.as_ref() {
            if !p.can_read(&ledger) {
                set_span_error_code(&span, "error:Forbidden");
                return Err(ServerError::not_found("Ledger not found"));
            }
        }

        let mut opts = fluree_db_api::RevertPreviewOpts::default();
        if let Some(n) = params.max_commits {
            opts.max_commits = Some(n.min(PREVIEW_HARD_MAX_COMMITS));
        }
        if let Some(n) = params.max_conflict_keys {
            opts.max_conflict_keys = Some(n.min(PREVIEW_HARD_MAX_CONFLICT_KEYS));
        }
        if let Some(b) = params.include_conflicts {
            opts.include_conflicts = b;
        }
        if let Some(s) = params.strategy.as_deref() {
            opts.conflict_strategy =
                fluree_db_api::ConflictStrategy::parse_canonical(s).map_err(|_| {
                    ServerError::bad_request(format!("Unknown revert preview strategy: {s}"))
                })?;
        }

        // Normalize the range pair: both supplied ⇒ Some(pair); both absent
        // ⇒ None; partial ⇒ error. Doing this first lets the count check
        // below treat range as a single boolean and lets the dispatch
        // destructure without further unwrapping.
        let range = match (params.from, params.to) {
            (Some(f), Some(t)) => Some((f, t)),
            (None, None) => None,
            _ => {
                return Err(ServerError::bad_request(
                    "`from` and `to` must be supplied together".to_string(),
                ));
            }
        };

        let supplied = [
            params.commit.is_some(),
            params.commits.is_some(),
            range.is_some(),
        ]
        .iter()
        .filter(|p| **p)
        .count();
        if supplied != 1 {
            return Err(ServerError::bad_request(
                "Exactly one of `commit`, `commits`, or `from`+`to` must be provided".to_string(),
            ));
        }

        let preview = if let Some(commit) = params.commit {
            let commit_ref = parse_commit_ref(&commit)?;
            state
                .fluree
                .revert_commit_preview_with(&ledger, &params.branch, commit_ref, opts)
                .await
                .map_err(ServerError::Api)?
        } else if let Some(commits_csv) = params.commits {
            let parsed: std::result::Result<Vec<_>, _> = commits_csv
                .split(',')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .map(parse_commit_ref)
                .collect();
            let parsed = parsed?;
            if parsed.is_empty() {
                return Err(ServerError::bad_request(
                    "`commits` must contain at least one commit reference".to_string(),
                ));
            }
            state
                .fluree
                .revert_commits_preview_with(&ledger, &params.branch, parsed, opts)
                .await
                .map_err(ServerError::Api)?
        } else {
            let (from, to) = range.expect("count check guarantees range is Some here");
            let from = parse_commit_ref(&from)?;
            let to = parse_commit_ref(&to)?;
            state
                .fluree
                .revert_range_preview_with(&ledger, &params.branch, from, to, opts)
                .await
                .map_err(ServerError::Api)?
        };

        Ok(Json(preview))
    }
    .instrument(span)
    .await
}

/// Forward a transaction request to the transaction server (peer mode)
pub(super) async fn forward_write_request(state: &AppState, request: Request) -> Response {
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
