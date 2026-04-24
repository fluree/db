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
            .create_branch(&ledger, &branch, Some(&source), None)
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
