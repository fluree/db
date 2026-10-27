//! Iceberg graph source endpoints: POST /v1/fluree/iceberg/map

use crate::config::ServerRole;
use crate::error::{Result, ServerError};
use crate::extract::FlureeHeaders;
use crate::state::AppState;
use crate::telemetry::{create_request_span, extract_request_id, extract_trace_id};
use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::Instrument;

use super::iceberg_ssrf::guard_connection;
use super::ledger::forward_write_request;
use fluree_db_api::SecretRules;

/// Request body for `POST /v1/fluree/iceberg/map`
#[derive(Deserialize)]
pub struct IcebergMapRequest {
    /// Graph source name
    pub name: String,
    /// Table identifier (namespace.table)
    pub table: Option<String>,
    /// Inline R2RML mapping content (required): a source is queried through
    /// its mapping, which also names its tables. Optional here only so a
    /// request without it is refused by [`required_mapping`], in words, rather
    /// than by the JSON parser.
    pub r2rml: Option<String>,
    /// R2RML mapping media type
    pub r2rml_type: Option<String>,
    /// Branch name
    pub branch: Option<String>,
    /// Tombstone/delete convention: source column inspected to classify a row
    /// as a delete during materialization. Omit to disable retraction.
    pub delete_column: Option<String>,
    /// Column values that mark a row as a delete. A `null` element matches a NULL
    /// `delete_column` value (null-payload tombstone), e.g. `["d", "delete"]`,
    /// `[null]`, or `["d", null]`. Required when `delete_column` is set.
    #[serde(default)]
    pub delete_values: Vec<Option<String>>,
    /// Ordering column for latest-by-key materialization (e.g. `event_timestamp`).
    pub order_by: Option<String>,
    /// Model ledger (`name:branch`) whose default graph supplies the source's
    /// view policies and class/property hierarchy.
    pub model: Option<String>,
    /// `default-allow` for governed requests that match no policy (`true`
    /// keeps the source readable under authentication without a model).
    pub default_allow: Option<bool>,
    /// The connection, as every Iceberg route takes it: see
    /// [`ConnectionFields`].
    #[serde(flatten)]
    pub connection: ConnectionFields,
}

/// The rest of an Iceberg request: its connection, given either as the
/// structured `catalog` / `io` objects (exactly the stored config's shape) or
/// as the legacy flat fields (`mode`, `catalog_uri`, `auth_bearer_env`, …).
/// [`fluree_db_api::iceberg_connection_from_json`] reads it, and refuses an
/// unknown field, the two forms mixed, and auth given two ways or half given.
pub type ConnectionFields = serde_json::Map<String, serde_json::Value>;

/// Response for `POST /v1/fluree/iceberg/map`
#[derive(Serialize)]
pub struct IcebergMapResponse {
    pub graph_source_id: String,
    pub table_identifier: String,
    pub catalog_uri: String,
    pub connection_tested: bool,
    pub mapping_source: String,
    pub triples_map_count: usize,
    pub table_count: usize,
    pub table_names: Vec<String>,
    pub mapping_validated: bool,
    /// Warnings about the `model` reference (policies a virtual source cannot
    /// evaluate).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub model_warnings: Vec<String>,
}

/// Map an Iceberg table as a graph source
///
/// POST /v1/fluree/iceberg/map
pub async fn iceberg_map(State(state): State<Arc<AppState>>, request: Request) -> Response {
    if state.config.server_role == ServerRole::Peer {
        return forward_write_request(&state, request).await;
    }

    iceberg_map_local(state, request).await.into_response()
}

async fn iceberg_map_local(state: Arc<AppState>, request: Request) -> Result<impl IntoResponse> {
    let headers_result = FlureeHeaders::from_headers(request.headers());
    let headers = match headers_result {
        Ok(h) => h,
        Err(e) => return Err(e),
    };

    let body_bytes = axum::body::to_bytes(request.into_body(), 50 * 1024 * 1024)
        .await
        .map_err(|e| ServerError::bad_request(format!("Failed to read body: {e}")))?;
    let req: IcebergMapRequest = serde_json::from_slice(&body_bytes)
        .map_err(|e| ServerError::bad_request(format!("Invalid JSON: {e}")))?;

    let request_id = extract_request_id(&headers.raw, &state.telemetry_config);
    let trace_id = extract_trace_id(&headers.raw);

    let span = create_request_span(
        "iceberg:map",
        request_id.as_deref(),
        trace_id.as_deref(),
        Some(&req.name),
        None,
        None,
    );
    async move {
        tracing::info!(status = "start", name = %req.name, "iceberg map requested");

        let mapping = required_mapping(&req)?;
        let fluree = &state.fluree;
        let iceberg_config = build_iceberg_config(&req, &SecretRules::server(fluree))?;

        let config = fluree_db_api::R2rmlCreateConfig {
            iceberg: iceberg_config,
            mapping: fluree_db_api::R2rmlMappingInput::Content(mapping.to_string()),
            mapping_media_type: req.r2rml_type.clone(),
        };
        let result = fluree
            .create_r2rml_graph_source(config)
            .await
            .map_err(ServerError::Api)?;
        let response = IcebergMapResponse {
            model_warnings: result.model_warnings,
            graph_source_id: result.graph_source_id,
            table_identifier: result.table_identifier,
            catalog_uri: result.catalog_uri,
            connection_tested: result.connection_tested,
            mapping_source: result.mapping_source,
            triples_map_count: result.triples_map_count,
            table_count: result.table_count,
            table_names: result.table_names,
            mapping_validated: result.mapping_validated,
        };

        tracing::info!(
            status = "success",
            graph_source_id = %response.graph_source_id,
            "iceberg graph source mapped"
        );
        Ok((StatusCode::CREATED, Json(response)))
    }
    .instrument(span)
    .await
}

/// Request body for `POST /v1/fluree/iceberg/materialize`
#[derive(Deserialize)]
pub struct IcebergMaterializeRequest {
    /// Source graph source id (the R2RML/Iceberg source to read).
    pub source: String,
    /// Target native ledger to materialize into (created if absent).
    pub target: String,
    /// Force a full re-read, ignoring the watermark persisted in the target
    /// ledger. Default `false`: resolve the watermark and refresh incrementally
    /// (full only on the first run or a non-incremental-safe window).
    #[serde(default)]
    pub force_full: bool,
}

/// Response for `POST /v1/fluree/iceberg/materialize`
#[derive(Serialize)]
pub struct IcebergMaterializeResponse {
    pub source: String,
    pub target: String,
    /// The watermark this pass started from (previously-materialized snapshot).
    pub from_snapshot_id: Option<i64>,
    /// The source snapshot now materialized (the persisted watermark).
    pub to_snapshot_id: Option<i64>,
    /// Whether an incremental (added-files-only) scan was used.
    pub incremental: bool,
    /// Whether anything was committed (false on a no-delta poll).
    pub committed: bool,
    pub rows_read: usize,
    pub subjects_upserted: usize,
    pub subjects_retracted: usize,
}

/// Materialize an R2RML / Iceberg graph source into a native ledger.
///
/// POST /v1/fluree/iceberg/materialize
pub async fn iceberg_materialize(State(state): State<Arc<AppState>>, request: Request) -> Response {
    if state.config.server_role == ServerRole::Peer {
        return forward_write_request(&state, request).await;
    }

    iceberg_materialize_local(state, request)
        .await
        .into_response()
}

async fn iceberg_materialize_local(
    state: Arc<AppState>,
    request: Request,
) -> Result<impl IntoResponse> {
    let headers = FlureeHeaders::from_headers(request.headers())?;

    let body_bytes = axum::body::to_bytes(request.into_body(), 50 * 1024 * 1024)
        .await
        .map_err(|e| ServerError::bad_request(format!("Failed to read body: {e}")))?;
    let req: IcebergMaterializeRequest = serde_json::from_slice(&body_bytes)
        .map_err(|e| ServerError::bad_request(format!("Invalid JSON: {e}")))?;

    let request_id = extract_request_id(&headers.raw, &state.telemetry_config);
    let trace_id = extract_trace_id(&headers.raw);

    let span = create_request_span(
        "iceberg:materialize",
        request_id.as_deref(),
        trace_id.as_deref(),
        Some(&req.source),
        None,
        None,
    );
    async move {
        tracing::info!(
            status = "start",
            source = %req.source,
            target = %req.target,
            force_full = req.force_full,
            "iceberg materialize requested"
        );

        let result = state
            .fluree
            .materialize_r2rml_graph_source(&req.source, &req.target, req.force_full)
            .await
            .map_err(ServerError::Api)?;

        let response = IcebergMaterializeResponse {
            source: req.source.clone(),
            target: req.target.clone(),
            from_snapshot_id: result.from_snapshot_id,
            to_snapshot_id: result.to_snapshot_id,
            incremental: result.incremental,
            committed: result.committed,
            rows_read: result.rows_read,
            subjects_upserted: result.subjects_upserted,
            subjects_retracted: result.subjects_retracted,
        };

        tracing::info!(
            status = "success",
            source = %response.source,
            target = %response.target,
            to_snapshot_id = ?response.to_snapshot_id,
            incremental = response.incremental,
            committed = response.committed,
            rows_read = response.rows_read,
            subjects_upserted = response.subjects_upserted,
            subjects_retracted = response.subjects_retracted,
            "iceberg materialize complete"
        );
        Ok((StatusCode::OK, Json(response)))
    }
    .instrument(span)
    .await
}

/// Request body for `POST /v1/fluree/iceberg/track` and `/untrack`.
#[derive(Deserialize)]
pub struct IcebergTrackRequest {
    /// Source graph source id to track.
    pub source: String,
    /// Target native ledger to keep materialized.
    pub target: String,
    /// How often the worker re-syncs this job, in seconds. Omit to use the
    /// worker's default (30s). Ignored by `/untrack`. Must be > 0.
    #[serde(default)]
    pub poll_interval_secs: Option<u64>,
    /// Block the response until the opportunistic first sync finishes.
    ///
    /// Defaults to FALSE: registration is durable the moment it is persisted, and
    /// the worker polls regardless, so the first sync is a latency optimisation
    /// rather than part of the operation. Waiting for it made `track` take as long
    /// as a full materialize — on a fresh volume, bootstrap registers 17 sources
    /// and each one blocked on a FULL read, serialising 17 backfills inside pod
    /// startup.
    ///
    /// Set true when a caller genuinely wants the first sync's numbers in the
    /// response (tests, one-off manual runs on a small source).
    #[serde(default)]
    pub wait_for_first_sync: bool,
}

/// Response for `POST /v1/fluree/iceberg/track`.
#[derive(Serialize)]
pub struct IcebergTrackResponse {
    pub source: String,
    pub target: String,
    /// Whether the worker is now tracking this pair.
    pub tracked: bool,
    /// The effective poll interval for this job, in seconds.
    pub poll_interval_secs: u64,
    /// Number of jobs the worker is tracking.
    pub tracked_jobs: usize,
    /// What happened to the opportunistic first sync: `scheduled` (left to the
    /// tracking worker's next tick), `completed`, or `failed`.
    ///
    /// Separate from `tracked` because the two are genuinely independent, and
    /// conflating them was a real defect: a first sync that lost a commit race made
    /// the whole call return an error even though the job was registered AND
    /// persisted beforehand. Operators reasonably read `track ERROR` as "tracking
    /// did not happen" when it had.
    pub first_sync: &'static str,
    /// Numbers from the first sync — only present when `wait_for_first_sync` was set
    /// and it succeeded. `null` otherwise, which existing consumers already tolerate.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub initial: Option<IcebergMaterializeResponse>,
    /// Why the first sync failed, when it did. Never fails the call.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_sync_error: Option<String>,
}

/// Register a `source → target` materialization tracking job and run an
/// immediate first sync. The worker then keeps the target fresh on its poll
/// interval (incremental when safe).
///
/// POST /v1/fluree/iceberg/track
pub async fn iceberg_track(State(state): State<Arc<AppState>>, request: Request) -> Response {
    if state.config.server_role == ServerRole::Peer {
        return forward_write_request(&state, request).await;
    }
    iceberg_track_local(state, request).await.into_response()
}

async fn iceberg_track_local(state: Arc<AppState>, request: Request) -> Result<impl IntoResponse> {
    let _headers = FlureeHeaders::from_headers(request.headers())?;
    let body_bytes = axum::body::to_bytes(request.into_body(), 1024 * 1024)
        .await
        .map_err(|e| ServerError::bad_request(format!("Failed to read body: {e}")))?;
    let req: IcebergTrackRequest = serde_json::from_slice(&body_bytes)
        .map_err(|e| ServerError::bad_request(format!("Invalid JSON: {e}")))?;

    if req.poll_interval_secs == Some(0) {
        return Err(ServerError::bad_request(
            "poll_interval_secs must be greater than 0",
        ));
    }

    let worker = state.materialize_worker.as_ref().ok_or_else(|| {
        ServerError::bad_request(
            "materialization tracking worker is not running on this node (peer role, or \
             indexing disabled — materialization needs a local indexer to drain novelty)",
        )
    })?;

    let interval = worker.track(
        &req.source,
        &req.target,
        req.poll_interval_secs.map(std::time::Duration::from_secs),
    );

    // Persist the job so a restart restores it instead of silently stopping
    // materialization until a client re-issues this call. Written after the
    // in-memory registration and before the first sync, so the durable record
    // and the running worker never disagree in the direction that loses work.
    state
        .fluree
        .persist_materialize_job(&fluree_db_api::PersistedMaterializeJob {
            source: req.source.clone(),
            target: req.target.clone(),
            poll_interval_secs: interval.as_secs(),
        })
        .await
        .map_err(ServerError::Api)?;

    // The opportunistic first sync — populate the target without waiting a poll cycle.
    //
    // It is NOT part of registration, and the code above is what makes that true: the
    // job is in the worker AND persisted before we get here. So this must neither
    // block the response nor be able to fail it. Previously it did both:
    //
    //   * `?` on its error made `track` report failure for a job that was already
    //     registered and durable. A first sync losing a commit race produced
    //     `track ERROR: Commit conflict ...`, which reads as "tracking did not happen".
    //   * awaiting it made `track` cost a full materialize. On a fresh volume nothing
    //     is tracked, so bootstrap registers all 17 sources and each blocked on a FULL
    //     read — 17 backfills serialised inside pod startup.
    let tracked_jobs = worker.tracked_jobs().len();
    let (first_sync, initial, first_sync_error) = if req.wait_for_first_sync {
        match state
            .fluree
            .materialize_r2rml_graph_source(&req.source, &req.target, false)
            .await
        {
            Ok(result) => (
                "completed",
                Some(IcebergMaterializeResponse {
                    source: req.source.clone(),
                    target: req.target.clone(),
                    from_snapshot_id: result.from_snapshot_id,
                    to_snapshot_id: result.to_snapshot_id,
                    incremental: result.incremental,
                    committed: result.committed,
                    rows_read: result.rows_read,
                    subjects_upserted: result.subjects_upserted,
                    subjects_retracted: result.subjects_retracted,
                }),
                None,
            ),
            // Reported, never fatal: the job is tracked and the worker will retry.
            Err(e) => {
                tracing::warn!(
                    source = %req.source,
                    target = %req.target,
                    error = %e,
                    "iceberg/track: first sync failed; the job IS tracked and the worker \
                     will retry on its next poll"
                );
                ("failed", None, Some(e.to_string()))
            }
        }
    } else {
        // DELIBERATELY NOT SPAWNED. Leave it to the tracking worker's next tick.
        //
        // Spawning looked like the obvious way to keep the latency benefit without
        // blocking, and it is a trap: the worker polls jobs through a SERIAL outer loop,
        // but spawned tasks bypass that entirely. Registering 17 sources therefore
        // launched 17 unbounded concurrent materializes — and the per-window accumulator
        // is the one memory term nothing currently bounds (~10 GiB for a single
        // ~800k-row window). Measured locally: 873 % CPU, i.e. ~9 cores, on what should
        // be background work.
        //
        // The synchronous version was accidentally providing the serialisation. Removing
        // the await must not also remove the brake.
        //
        // Cost of leaving it to the worker: up to one poll interval before data appears
        // (30 s default, and the caller can pass poll_interval_secs to shorten it). That
        // is a small price for keeping first-sync concurrency equal to the worker's,
        // which is the only place it is bounded.
        ("scheduled", None, None)
    };

    let response = IcebergTrackResponse {
        source: req.source,
        target: req.target,
        tracked: true,
        poll_interval_secs: interval.as_secs(),
        tracked_jobs,
        first_sync,
        initial,
        first_sync_error,
    };
    Ok((StatusCode::OK, Json(response)))
}

/// Stop tracking a `source → target` pair (leaves already-materialized data).
///
/// POST /v1/fluree/iceberg/untrack
pub async fn iceberg_untrack(State(state): State<Arc<AppState>>, request: Request) -> Response {
    if state.config.server_role == ServerRole::Peer {
        return forward_write_request(&state, request).await;
    }
    iceberg_untrack_local(state, request).await.into_response()
}

async fn iceberg_untrack_local(
    state: Arc<AppState>,
    request: Request,
) -> Result<impl IntoResponse> {
    let _headers = FlureeHeaders::from_headers(request.headers())?;
    let body_bytes = axum::body::to_bytes(request.into_body(), 1024 * 1024)
        .await
        .map_err(|e| ServerError::bad_request(format!("Failed to read body: {e}")))?;
    let req: IcebergTrackRequest = serde_json::from_slice(&body_bytes)
        .map_err(|e| ServerError::bad_request(format!("Invalid JSON: {e}")))?;

    let worker = state.materialize_worker.as_ref().ok_or_else(|| {
        ServerError::bad_request("materialization tracking worker is not running on this node")
    })?;
    let removed = worker.untrack(&req.source, &req.target);

    // Durable too, or a restart would resurrect the job.
    state
        .fluree
        .forget_materialize_job(&req.source, &req.target)
        .await
        .map_err(ServerError::Api)?;

    Ok((
        StatusCode::OK,
        Json(serde_json::json!({
            "source": req.source,
            "target": req.target,
            "removed": removed,
            "tracked_jobs": worker.tracked_jobs().len(),
        })),
    ))
}

/// Tracking-worker status: tracked jobs + cumulative stats.
///
/// GET /v1/fluree/iceberg/tracking
pub async fn iceberg_tracking_status(State(state): State<Arc<AppState>>) -> Response {
    let Some(worker) = state.materialize_worker.as_ref() else {
        return (
            StatusCode::OK,
            Json(serde_json::json!({ "running": false, "jobs": [] })),
        )
            .into_response();
    };
    let jobs: Vec<_> = worker
        .job_infos()
        .into_iter()
        .map(|j| {
            serde_json::json!({
                "source": j.source,
                "target": j.target,
                "poll_interval_secs": j.poll_interval_secs,
            })
        })
        .collect();
    let stats = worker.stats();
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "running": true,
            "jobs": jobs,
            "stats": {
                "polls": stats.polls,
                "syncs_committed": stats.syncs_committed,
                "syncs_noop": stats.syncs_noop,
                // Surfaced separately from failures on purpose: a non-zero rate here
                // means the INDEXER is the bottleneck, which is a capacity signal, not
                // a fault. Counting deferrals as failures is how 1,050 of them once
                // presented as an outage and hid the real cause.
                "syncs_deferred": stats.syncs_deferred,
                "syncs_failed": stats.syncs_failed,
                // Polls where the targets did not land uniformly. For a fan-out job this
                // is the normal steady state, not an anomaly.
                "syncs_partial": stats.syncs_partial,

                // READ THESE for progress. The counters above are per POLL, and a poll
                // fans out to many independent target ledgers, so they cannot express
                // partial progress: a window with 21 of 22 targets committed scored zero
                // commits and one deferral, and was consequently diagnosed as a stall.
                "targets_committed": stats.targets_committed,
                "targets_deferred": stats.targets_deferred,
                "targets_failed": stats.targets_failed,

                "tracked_jobs": stats.tracked_jobs,
            }
        })),
    )
        .into_response()
}

/// The request's R2RML mapping, which every Iceberg source needs: its queries
/// are answered from it. A missing or blank one is a 400 that says so.
fn required_mapping(req: &IcebergMapRequest) -> Result<&str> {
    req.r2rml
        .as_deref()
        .filter(|mapping| !mapping.trim().is_empty())
        .ok_or_else(|| {
            ServerError::bad_request(
                "an R2RML mapping is required: send `r2rml`, the mapping's Turtle content",
            )
        })
}

/// The map request's create config: its connection through the one codec
/// (then the SSRF guard on the URLs it names), and its own settings.
fn build_iceberg_config(
    req: &IcebergMapRequest,
    rules: &SecretRules,
) -> Result<fluree_db_api::IcebergCreateConfig> {
    let mut config = fluree_db_api::iceberg_create_from_json(
        &req.name,
        req.table.as_deref(),
        req.connection.clone(),
        rules,
    )
    .map_err(input_error)?;
    guard_connection(&config.connection)?;

    if let Some(ref branch) = req.branch {
        config = config.with_branch(branch);
    }
    if let Some(ref column) = req.delete_column {
        let convention = fluree_db_api::DeleteConvention {
            column: column.clone(),
            deleted_values: req.delete_values.clone(),
        };
        convention
            .validate()
            .map_err(|e| ServerError::bad_request(format!("invalid delete convention: {e}")))?;
        config = config.with_delete_convention(convention);
    }
    if let Some(ref order_by) = req.order_by {
        config = config.with_order_by(order_by);
    }
    if let Some(ref model) = req.model {
        config = config.with_model(model);
    }
    if let Some(allow) = req.default_allow {
        config = config.with_default_allow(allow);
    }

    Ok(config)
}

/// A refused graph-source input: a typed 400 naming the field.
pub(crate) fn input_error(e: fluree_db_api::GraphSourceInputError) -> ServerError {
    ServerError::Api(e.into())
}

// =============================================================================
// Read-only catalog browse + metadata preview (metadata-only, no graph source
// created). POST-with-read-semantics: the connection carries a secret in the
// body, so these are POSTs, but they mutate nothing.
// =============================================================================

/// A read-only route's connection: through the one codec, then the SSRF
/// guard on the URLs it names.
fn build_connection(
    state: &AppState,
    fields: ConnectionFields,
) -> Result<fluree_db_api::IcebergConnectionConfig> {
    build_connection_with(fields, &SecretRules::server(&state.fluree))
}

fn build_connection_with(
    fields: ConnectionFields,
    rules: &SecretRules,
) -> Result<fluree_db_api::IcebergConnectionConfig> {
    let conn = fluree_db_api::iceberg_connection_from_json(fields, rules).map_err(input_error)?;
    guard_connection(&conn)?;
    Ok(conn)
}

/// Read the request span, parse the JSON body into `T`.
async fn parse_iceberg_body<T: serde::de::DeserializeOwned>(request: Request) -> Result<T> {
    let body_bytes = axum::body::to_bytes(request.into_body(), 50 * 1024 * 1024)
        .await
        .map_err(|e| ServerError::bad_request(format!("Failed to read body: {e}")))?;
    serde_json::from_slice(&body_bytes)
        .map_err(|e| ServerError::bad_request(format!("Invalid JSON: {e}")))
}

/// Request body for `POST /v1/fluree/iceberg/catalog/browse`
#[derive(Deserialize)]
pub struct IcebergBrowseRequest {
    #[serde(flatten)]
    pub connection: ConnectionFields,
    /// Browse depth: "namespaces" or "tables" (default "tables")
    pub depth: Option<String>,
}

/// Browse an Iceberg catalog (namespaces + tables). Read-only.
///
/// POST /v1/fluree/iceberg/catalog/browse
pub async fn iceberg_catalog_browse(
    State(state): State<Arc<AppState>>,
    request: Request,
) -> Response {
    iceberg_catalog_browse_local(state, request)
        .await
        .into_response()
}

async fn iceberg_catalog_browse_local(
    state: Arc<AppState>,
    request: Request,
) -> Result<impl IntoResponse> {
    use fluree_db_api::BrowseDepth;

    let headers = FlureeHeaders::from_headers(request.headers())?;
    let request_id = extract_request_id(&headers.raw, &state.telemetry_config);
    let trace_id = extract_trace_id(&headers.raw);
    let req: IcebergBrowseRequest = parse_iceberg_body(request).await?;

    let span = create_request_span(
        "iceberg:catalog:browse",
        request_id.as_deref(),
        trace_id.as_deref(),
        None,
        None,
        None,
    );
    async move {
        let conn = build_connection(&state, req.connection)?;
        let depth = match req.depth.as_deref().map(str::to_lowercase).as_deref() {
            Some("namespaces") => BrowseDepth::Namespaces,
            None | Some("tables") => BrowseDepth::Tables,
            Some(other) => {
                return Err(ServerError::bad_request(format!(
                    "unknown depth '{other}'. Use 'namespaces' or 'tables'."
                )));
            }
        };

        let browse = state
            .fluree
            .browse_iceberg_catalog(conn, depth)
            .await
            .map_err(ServerError::Api)?;

        tracing::info!(
            status = "success",
            namespaces = browse.namespaces.len(),
            tables = browse.tables.len(),
            "iceberg catalog browsed"
        );
        Ok((StatusCode::OK, Json(browse)))
    }
    .instrument(span)
    .await
}

/// Request body for `POST /v1/fluree/iceberg/catalog/preview`
#[derive(Deserialize)]
pub struct IcebergPreviewRequest {
    #[serde(flatten)]
    pub connection: ConnectionFields,
    /// Table namespace (e.g. "DW")
    pub namespace: String,
    /// Table name (e.g. "DIM_STORE")
    pub name: String,
    /// Stats tier: "schema" (Tier-A) or "stats" (Tier-A + Tier-B). Default "schema".
    /// Tier-A reads no manifest; when the snapshot summary omits the row and
    /// file totals it reads the snapshot's manifest list for them, once per
    /// snapshot.
    pub tier: Option<String>,
}

/// Preview an Iceberg table's schema (+ optional per-column stats). Read-only.
///
/// POST /v1/fluree/iceberg/catalog/preview
pub async fn iceberg_catalog_preview(
    State(state): State<Arc<AppState>>,
    request: Request,
) -> Response {
    iceberg_catalog_preview_local(state, request)
        .await
        .into_response()
}

async fn iceberg_catalog_preview_local(
    state: Arc<AppState>,
    request: Request,
) -> Result<impl IntoResponse> {
    use fluree_db_api::{StatsTier, TableIdentifier};

    let headers = FlureeHeaders::from_headers(request.headers())?;
    let request_id = extract_request_id(&headers.raw, &state.telemetry_config);
    let trace_id = extract_trace_id(&headers.raw);
    let req: IcebergPreviewRequest = parse_iceberg_body(request).await?;

    let span = create_request_span(
        "iceberg:catalog:preview",
        request_id.as_deref(),
        trace_id.as_deref(),
        Some(&format!("{}.{}", req.namespace, req.name)),
        None,
        None,
    );
    async move {
        let conn = build_connection(&state, req.connection)?;
        let tier = match req.tier.as_deref().map(str::to_lowercase).as_deref() {
            None | Some("schema") => StatsTier::Schema,
            Some("stats") => StatsTier::Stats,
            Some(other) => {
                return Err(ServerError::bad_request(format!(
                    "unknown tier '{other}'. Use 'schema' or 'stats'."
                )));
            }
        };
        let table = TableIdentifier::new(&req.namespace, &req.name);

        let preview = state
            .fluree
            .preview_iceberg_table(conn, table, tier)
            .await
            .map_err(ServerError::Api)?;

        tracing::info!(
            status = "success",
            table = %format!("{}.{}", req.namespace, req.name),
            columns = preview.schema.columns.len(),
            "iceberg table previewed"
        );
        Ok((StatusCode::OK, Json(preview)))
    }
    .instrument(span)
    .await
}

/// Request body for `POST /v1/fluree/iceberg/catalog/verify`
#[derive(Deserialize)]
pub struct IcebergVerifyRequest {
    #[serde(flatten)]
    pub connection: ConnectionFields,
    /// Table identifier (`"NAMESPACE.NAME"`, byte-for-byte catalog casing).
    pub table: String,
}

/// Verify that the connection's resolved credentials can READ a table's storage
/// (the onboarding "Test" probe). Read-only: creates no graph source, writes
/// nothing; it goes through the engine's own credential + storage path and proves
/// both the `metadata/` and `data/` S3 prefixes are readable. A table that cannot
/// be read answers `200` with `readable: false` and the reason in `error`.
///
/// POST /v1/fluree/iceberg/catalog/verify
pub async fn iceberg_catalog_verify(
    State(state): State<Arc<AppState>>,
    request: Request,
) -> Response {
    iceberg_catalog_verify_local(state, request)
        .await
        .into_response()
}

async fn iceberg_catalog_verify_local(
    state: Arc<AppState>,
    request: Request,
) -> Result<impl IntoResponse> {
    let headers = FlureeHeaders::from_headers(request.headers())?;
    let request_id = extract_request_id(&headers.raw, &state.telemetry_config);
    let trace_id = extract_trace_id(&headers.raw);
    let req: IcebergVerifyRequest = parse_iceberg_body(request).await?;

    let span = create_request_span(
        "iceberg:catalog:verify",
        request_id.as_deref(),
        trace_id.as_deref(),
        Some(&req.table),
        None,
        None,
    );
    async move {
        let conn = build_connection(&state, req.connection)?;

        let report = state
            .fluree
            .verify_iceberg_storage_access(conn, &req.table)
            .await
            .map_err(ServerError::Api)?;

        tracing::info!(
            status = "success",
            table = %req.table,
            readable = report.readable,
            credential_source = report.credential_source,
            data_file_count = report.data_file_count,
            data_probe_skipped = report.data_probe_skipped,
            "iceberg storage access verified"
        );
        Ok((StatusCode::OK, Json(report)))
    }
    .instrument(span)
    .await
}

// =============================================================================
// Deterministic R2RML generation (metadata-only; creates no graph source).
// =============================================================================

/// One `per_table_overrides` entry (its `{namespace, name}` key + values). JSON
/// object keys must be strings, so overrides ride the wire as a list rather than
/// a struct-keyed map.
#[derive(Deserialize)]
pub struct TableOverrideEntry {
    /// Table namespace (e.g. "DW").
    pub namespace: String,
    /// Table name (e.g. "DIM_STORE").
    pub name: String,
    /// Replaces identifier_field_ids with a SINGLE-column subject key (kept for
    /// backward compatibility). Always earns a SubjectKeyUnverified diagnostic.
    #[serde(default)]
    pub primary_key: Option<String>,
    /// Replaces identifier_field_ids with a COMPOSITE (one-or-more-column) subject
    /// key. Takes precedence over `primary_key` when both are present.
    #[serde(default)]
    pub subject_key: Option<Vec<String>>,
    /// Per-table subject-key strategy: `auto` (always emit) or `identifier`
    /// (strict). `null` inherits the request-level `options.subject_strategy`.
    #[serde(default)]
    pub subject_strategy: Option<fluree_db_api::SubjectStrategy>,
    /// Overrides the derived class name / subject slug for the table.
    #[serde(default)]
    pub class_name: Option<String>,
}

/// Request body for `POST /v1/fluree/iceberg/r2rml/generate`.
#[derive(Deserialize)]
pub struct IcebergGenerateRequest {
    #[serde(flatten)]
    pub connection: ConnectionFields,
    /// Tables to map, in output order: `[{ "namespace": .., "name": .. }]`.
    pub tables: Vec<fluree_db_api::TableIdentifier>,
    /// The SINGLE base namespace all IRIs derive from.
    pub base_namespace: String,
    /// Per-table subject-key / class-name overrides.
    #[serde(default)]
    pub per_table_overrides: Vec<TableOverrideEntry>,
    /// Emit knobs (xsd_long_as_integer / emit_fk_joins / keep_fk_keys_as_literals).
    #[serde(default)]
    pub options: fluree_db_api::GenerateOptions,
    /// RESERVED for PR-4 (target-model IRI rewrite); accepted and ignored.
    #[serde(default)]
    pub target_model_ledger_id: Option<String>,
}

/// Deterministically generate an R2RML mapping over Iceberg tables. Read-only
/// (metadata-only; creates no graph source).
///
/// POST /v1/fluree/iceberg/r2rml/generate
pub async fn iceberg_r2rml_generate(
    State(state): State<Arc<AppState>>,
    request: Request,
) -> Response {
    iceberg_r2rml_generate_local(state, request)
        .await
        .into_response()
}

async fn iceberg_r2rml_generate_local(
    state: Arc<AppState>,
    request: Request,
) -> Result<impl IntoResponse> {
    use fluree_db_api::{GenerateR2rmlRequest, TableIdentifier, TableOverride};

    let headers = FlureeHeaders::from_headers(request.headers())?;
    let request_id = extract_request_id(&headers.raw, &state.telemetry_config);
    let trace_id = extract_trace_id(&headers.raw);
    let req: IcebergGenerateRequest = parse_iceberg_body(request).await?;

    let span = create_request_span(
        "iceberg:r2rml:generate",
        request_id.as_deref(),
        trace_id.as_deref(),
        None,
        None,
        None,
    );
    async move {
        if req.tables.is_empty() {
            return Err(ServerError::bad_request(
                "at least one table is required for generate",
            ));
        }
        let connection = build_connection(&state, req.connection)?;
        let per_table_overrides = req
            .per_table_overrides
            .into_iter()
            .map(|e| {
                // A composite `subject_key` wins; else the single `primary_key`
                // is lifted into a one-element column list.
                let primary_key = e.subject_key.or_else(|| e.primary_key.map(|pk| vec![pk]));
                (
                    TableIdentifier::new(e.namespace, e.name),
                    TableOverride {
                        primary_key,
                        class_name: e.class_name,
                        subject_strategy: e.subject_strategy,
                    },
                )
            })
            .collect();

        let api_req = GenerateR2rmlRequest {
            connection,
            tables: req.tables,
            base_namespace: req.base_namespace,
            per_table_overrides,
            options: req.options,
            target_model_ledger_id: req.target_model_ledger_id,
        };

        let response = state
            .fluree
            .generate_r2rml(api_req)
            .await
            .map_err(ServerError::Api)?;

        tracing::info!(
            status = "success",
            tables = response.structured.table_mappings.len(),
            diagnostics = response.diagnostics.len(),
            "iceberg r2rml generated"
        );
        Ok((StatusCode::OK, Json(response)))
    }
    .instrument(span)
    .await
}

/// Request body for `POST /v1/fluree/iceberg/r2rml/validate`
#[derive(Deserialize)]
pub struct IcebergValidateRequest {
    #[serde(flatten)]
    pub connection: ConnectionFields,
    /// R2RML mapping to validate, in Turtle format.
    pub r2rml: String,
    /// Optional Iceberg snapshot id to validate against. The metadata preview
    /// resolves each table's current snapshot, so this is recorded, not enforced.
    pub snapshot: Option<i64>,
}

/// Validate an R2RML mapping against a live catalog (compile + cross-check).
/// Read-only: creates no graph source, writes nothing.
///
/// POST /v1/fluree/iceberg/r2rml/validate
pub async fn iceberg_r2rml_validate(
    State(state): State<Arc<AppState>>,
    request: Request,
) -> Response {
    iceberg_r2rml_validate_local(state, request)
        .await
        .into_response()
}

async fn iceberg_r2rml_validate_local(
    state: Arc<AppState>,
    request: Request,
) -> Result<impl IntoResponse> {
    let headers = FlureeHeaders::from_headers(request.headers())?;
    let request_id = extract_request_id(&headers.raw, &state.telemetry_config);
    let trace_id = extract_trace_id(&headers.raw);
    let req: IcebergValidateRequest = parse_iceberg_body(request).await?;

    let span = create_request_span(
        "iceberg:r2rml:validate",
        request_id.as_deref(),
        trace_id.as_deref(),
        None,
        None,
        None,
    );
    async move {
        let conn = build_connection(&state, req.connection)?;

        let response = state
            .fluree
            .validate_r2rml(conn, req.r2rml, req.snapshot)
            .await
            .map_err(ServerError::Api)?;

        tracing::info!(
            status = "success",
            compiled_ok = response.compiled_ok,
            triples_maps = response.triples_map_count,
            diagnostics = response.diagnostics.len(),
            "iceberg r2rml validated"
        );
        Ok((StatusCode::OK, Json(response)))
    }
    .instrument(span)
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_api::{GraphSourceInputError, InputErrorKind};

    fn rules(listed: &str) -> SecretRules {
        SecretRules::listed(listed, false)
    }

    fn map_request(extra: serde_json::Value) -> IcebergMapRequest {
        let mut body = serde_json::json!({
            "name": "gs", "mode": "rest",
            "catalog_uri": "https://catalog.example.com", "table": "ns.tbl",
            "r2rml": "@prefix rr: <http://www.w3.org/ns/r2rml#> .",
        });
        body.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        serde_json::from_value(body).unwrap()
    }

    fn stored(config: fluree_db_api::IcebergCreateConfig) -> serde_json::Value {
        serde_json::to_value(config.to_iceberg_gs_config()).unwrap()
    }

    fn stored_auth(config: fluree_db_api::IcebergCreateConfig) -> serde_json::Value {
        stored(config)["catalog"]["auth"].clone()
    }

    /// The typed refusal inside a route error.
    fn input(e: ServerError) -> GraphSourceInputError {
        match e {
            ServerError::Api(fluree_db_api::ApiError::GraphSourceInput(e)) => e,
            other => panic!("not a graph-source input refusal: {other}"),
        }
    }

    fn connection(body: serde_json::Value) -> Result<fluree_db_api::IcebergConnectionConfig> {
        build_connection_with(body.as_object().unwrap().clone(), &rules("CATALOG_TOKEN"))
    }

    #[test]
    fn request_deserializes_oauth2_scope_and_reaches_auth_config() {
        // Omit client_id (Horizon case); provide token_url + secret + scope.
        let req = map_request(serde_json::json!({
            "oauth2_token_url": "https://catalog.example.com/v1/oauth/tokens",
            "oauth2_client_secret": "pat",
            "oauth2_scope": "session:role:ICEBERG_READER",
            "oauth2_audience": "polaris"
        }));
        let auth = stored_auth(build_iceberg_config(&req, &rules("")).unwrap());
        assert_eq!(auth["type"], "oauth2_client_credentials");
        assert_eq!(auth["client_id"], ""); // defaulted to empty
        assert_eq!(auth["scope"], "session:role:ICEBERG_READER");
        assert_eq!(auth["audience"], "polaris");
    }

    #[test]
    fn a_listed_variable_is_stored_as_its_name() {
        let req = map_request(serde_json::json!({"auth_bearer_env": "CATALOG_TOKEN"}));
        let auth = stored_auth(build_iceberg_config(&req, &rules("OTHER, CATALOG_TOKEN")).unwrap());
        assert_eq!(auth["type"], "bearer");
        assert_eq!(auth["token"]["env_var"], "CATALOG_TOKEN");

        let req = map_request(serde_json::json!({
            "oauth2_token_url": "https://catalog.example.com/token",
            "oauth2_client_id": "app",
            "oauth2_client_secret_env": "CATALOG_SECRET",
        }));
        let auth = stored_auth(build_iceberg_config(&req, &rules("CATALOG_SECRET")).unwrap());
        assert_eq!(auth["type"], "oauth2_client_credentials");
        assert_eq!(auth["client_secret"]["env_var"], "CATALOG_SECRET");
    }

    #[test]
    fn a_variable_the_operator_did_not_list_is_refused() {
        for allowed in ["", "CATALOG_TOKEN_2", "XCATALOG_TOKEN,CATALOG"] {
            for (field, extra) in [
                ("auth_bearer_env", serde_json::json!({})),
                (
                    "oauth2_client_secret_env",
                    serde_json::json!({"oauth2_token_url": "https://catalog.example.com/token"}),
                ),
            ] {
                let mut extra = extra;
                extra[field] = "CATALOG_TOKEN".into();
                let err = build_iceberg_config(&map_request(extra), &rules(allowed))
                    .err()
                    .unwrap_or_else(|| panic!("{field} accepted with allowlist {allowed:?}"));
                assert!(
                    err.to_string().contains(fluree_db_api::SECRET_ENV_VARS_ENV),
                    "{err}"
                );
            }
        }
    }

    #[test]
    fn a_secret_given_both_ways_is_refused() {
        let req = map_request(serde_json::json!({
            "auth_bearer": "t", "auth_bearer_env": "CATALOG_TOKEN",
        }));
        assert!(build_iceberg_config(&req, &rules("CATALOG_TOKEN")).is_err());
    }

    #[test]
    fn browse_and_preview_hold_a_named_variable_to_the_same_list() {
        let req = |allowed: &str| {
            build_connection_with(
                serde_json::json!({
                    "mode": "rest", "catalog_uri": "https://catalog.example.com",
                    "auth_bearer_env": "CATALOG_TOKEN",
                })
                .as_object()
                .unwrap()
                .clone(),
                &rules(allowed),
            )
        };
        assert!(req("CATALOG_TOKEN").is_ok());
        assert!(req("").is_err());
    }

    #[test]
    fn a_token_url_without_a_secret_is_refused_not_dropped() {
        // It used to store `{"type": "none"}` and authenticate nothing.
        let req = map_request(serde_json::json!({
            "oauth2_token_url": "https://catalog.example.com/v1/oauth/tokens"
        }));
        let e = input(build_iceberg_config(&req, &rules("")).unwrap_err());
        assert_eq!(e.field, "oauth2_token_url");
        assert!(matches!(e.kind, InputErrorKind::Incomplete { .. }), "{e}");
    }

    #[test]
    fn google_auth_reaches_every_read_only_route() {
        // `auth_google_metadata` used to work on `iceberg/map` only: the
        // read-only routes' request type had no such field and ignored it,
        // so browse and preview ran with no auth at all.
        let flat = serde_json::json!({
            "mode": "rest", "catalog_uri": "https://biglake.googleapis.com/iceberg/v1/restcatalog",
            "auth_google_metadata": true,
        });
        let structured = serde_json::json!({"catalog": {"type": "rest",
            "uri": "https://biglake.googleapis.com/iceberg/v1/restcatalog",
            "auth": {"type": "google", "credentials": {"source": "metadata"},
                "quota_project": "lake"}}});
        for body in [flat, structured] {
            let browse: IcebergBrowseRequest = serde_json::from_value(body.clone()).unwrap();
            let mut preview = body.clone();
            preview["namespace"] = "ns".into();
            preview["name"] = "t".into();
            let preview: IcebergPreviewRequest = serde_json::from_value(preview).unwrap();
            let mut verify = body.clone();
            verify["table"] = "ns.t".into();
            let verify: IcebergVerifyRequest = serde_json::from_value(verify).unwrap();
            for fields in [browse.connection, preview.connection, verify.connection] {
                let conn = build_connection_with(fields, &rules("")).unwrap();
                let fluree_db_api::CatalogMode::Rest(rest) = &conn.catalog_mode else {
                    panic!("rest");
                };
                assert!(
                    matches!(
                        rest.auth,
                        fluree_db_api::IcebergAuthConfig::GoogleMetadata { .. }
                            | fluree_db_api::IcebergAuthConfig::Google { .. }
                    ),
                    "auth dropped: {:?}",
                    rest.auth
                );
            }
        }
    }

    #[test]
    fn an_unknown_field_is_refused_on_every_route() {
        let body = serde_json::json!({
            "mode": "rest", "catalog_uri": "https://catalog.example.com",
            "auth_bearer_tokn": "t",
        });
        let browse: IcebergBrowseRequest = serde_json::from_value(body.clone()).unwrap();
        let e = input(build_connection_with(browse.connection, &rules("")).unwrap_err());
        assert_eq!(
            (e.field.as_str(), &e.kind),
            ("auth_bearer_tokn", &InputErrorKind::Unknown)
        );
        let mut map = body;
        map["name"] = "gs".into();
        map["table"] = "ns.t".into();
        let req: IcebergMapRequest = serde_json::from_value(map).unwrap();
        let e = input(build_iceberg_config(&req, &rules("")).unwrap_err());
        assert_eq!(e.field, "auth_bearer_tokn");
    }

    #[test]
    fn the_map_route_stores_the_same_config_from_either_form() {
        let flat = map_request(serde_json::json!({
            "warehouse": "wh", "auth_bearer_env": "CATALOG_TOKEN", "s3_region": "us-east-2",
        }));
        let structured: IcebergMapRequest = serde_json::from_value(serde_json::json!({
            "name": "gs", "table": "ns.tbl",
            "catalog": {"type": "rest", "uri": "https://catalog.example.com", "warehouse": "wh",
                "auth": {"type": "bearer", "token": {"env_var": "CATALOG_TOKEN"}}},
            "io": {"s3_region": "us-east-2"},
        }))
        .unwrap();
        assert_eq!(
            stored(build_iceberg_config(&flat, &rules("CATALOG_TOKEN")).unwrap()),
            stored(build_iceberg_config(&structured, &rules("CATALOG_TOKEN")).unwrap())
        );
    }

    #[test]
    fn browse_request_flattens_connection_and_builds_config() {
        // The flattened connection fields must deserialize alongside `depth`,
        // and build a REST connection carrying the OAuth2 scope.
        let body = serde_json::json!({
            "mode": "rest",
            "catalog_uri": "https://catalog.example.com",
            "warehouse": "wh1",
            "oauth2_token_url": "https://catalog.example.com/v1/oauth/tokens",
            "oauth2_client_secret": "pat",
            "oauth2_scope": "session:role:ICEBERG_READER",
            "depth": "namespaces"
        });
        let req: IcebergBrowseRequest = serde_json::from_value(body).unwrap();
        assert_eq!(req.depth.as_deref(), Some("namespaces"));

        let create = fluree_db_api::IcebergCreateConfig::from_connection(
            "gs",
            build_connection_with(req.connection, &rules("")).unwrap(),
            "ns.tbl",
        );
        assert!(create.is_rest());
        let v = stored(create);
        assert_eq!(v["catalog"]["warehouse"], "wh1");
        assert_eq!(v["catalog"]["auth"]["type"], "oauth2_client_credentials");
        assert_eq!(v["catalog"]["auth"]["scope"], "session:role:ICEBERG_READER");
    }

    #[test]
    fn every_catalog_mode_builds_its_connection() {
        let direct = connection(serde_json::json!({
            "mode": "direct", "table_location": "s3://bucket/warehouse/ns/table"
        }))
        .unwrap();
        assert!(direct.is_direct());
        let glue = connection(serde_json::json!({
            "mode": "glue", "region": "us-east-1", "catalog_id": "123456789012"
        }))
        .unwrap();
        assert!(glue.is_glue());
        let s3tables = connection(serde_json::json!({
            "mode": "s3tables", "region": "us-east-1",
            "table_bucket_arn": "arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket"
        }))
        .unwrap();
        assert!(s3tables.is_s3tables());
        // s3tables mode without an ARN is a 400, mirroring the map route.
        assert!(connection(serde_json::json!({ "mode": "s3tables" })).is_err());
    }

    #[test]
    fn rest_only_fields_are_refused_by_other_modes() {
        // Catalog auth or a warehouse sent with another mode would be ignored;
        // refuse each one, on both request shapes.
        let fields = [
            "auth_bearer",
            "auth_bearer_env",
            "oauth2_token_url",
            "oauth2_client_id",
            "oauth2_client_secret",
            "oauth2_client_secret_env",
            "oauth2_scope",
            "oauth2_audience",
            "auth_google_metadata",
            "auth_google_scopes",
            "warehouse",
        ];
        let given = |field: &str| match field {
            "auth_google_metadata" => serde_json::json!(true),
            _ => serde_json::json!("x"),
        };

        for field in fields {
            let mut body = serde_json::json!({
                "name": "orders",
                "mode": "glue",
                "r2rml": "@prefix rr: <http://www.w3.org/ns/r2rml#> ."
            });
            body[field] = given(field);
            let req: IcebergMapRequest = serde_json::from_value(body).unwrap();
            let err = build_iceberg_config(&req, &rules("x"))
                .unwrap_err()
                .to_string();
            assert!(
                err.contains(&format!("{field} applies to rest mode only")),
                "{field}: {err}"
            );

            let mut body =
                serde_json::json!({ "mode": "direct", "table_location": "s3://b/w/ns/t" });
            body[field] = given(field);
            let err = connection(body).unwrap_err().to_string();
            assert!(
                err.contains(&format!("{field} applies to rest mode only")),
                "{field}: {err}"
            );
        }
    }

    #[test]
    fn map_request_glue_mode_is_mapping_driven() {
        // Like rest, a glue source whose tables come from its R2RML mapping needs
        // no `table` of its own; each rr:tableName is a Glue `<database>.<table>`.
        let body = serde_json::json!({
            "name": "orders",
            "mode": "glue",
            "region": "us-east-1",
            "r2rml": "@prefix rr: <http://www.w3.org/ns/r2rml#> ."
        });
        let req: IcebergMapRequest = serde_json::from_value(body).unwrap();
        let config = build_iceberg_config(&req, &rules("")).unwrap();
        assert!(config.connection.is_glue());
        config.validate().unwrap();

        // A request without a mapping, or with a blank one, is refused in
        // words: the mapping is what a source's queries are answered from.
        for body in [
            serde_json::json!({ "name": "orders", "mode": "glue", "table": "db.t" }),
            serde_json::json!({ "name": "orders", "mode": "glue", "table": "db.t", "r2rml": "  " }),
        ] {
            let req: IcebergMapRequest = serde_json::from_value(body).unwrap();
            let err = required_mapping(&req).unwrap_err().to_string();
            assert!(
                err.contains("an R2RML mapping is required: send `r2rml`"),
                "{err}"
            );
        }
    }

    #[test]
    fn a_structured_connection_is_held_to_the_ssrf_guard() {
        let e = connection(serde_json::json!({
            "catalog": {"type": "rest", "uri": "http://169.254.169.254/latest"}
        }))
        .unwrap_err();
        assert!(e.to_string().contains("SSRF guard"), "{e}");
    }

    #[test]
    fn generate_request_flattens_connection_tables_overrides_and_options() {
        // The flattened connection fields deserialize alongside the generate
        // body: tables, base_namespace, an overrides list, options, and the
        // reserved target_model_ledger_id.
        let body = serde_json::json!({
            "mode": "rest",
            "catalog_uri": "https://catalog.example.com",
            "warehouse": "wh1",
            "oauth2_token_url": "https://catalog.example.com/v1/oauth/tokens",
            "oauth2_client_secret": "pat",
            "oauth2_scope": "session:role:ICEBERG_READER",
            "tables": [
                {"namespace": "DW", "name": "DIM_GEOGRAPHY"},
                {"namespace": "DW", "name": "DIM_SUPPLIER"}
            ],
            "base_namespace": "http://ns.fluree.dev/edw#",
            "per_table_overrides": [
                {"namespace": "DW", "name": "DIM_SUPPLIER", "primary_key": "ALT_KEY"}
            ],
            "options": {"xsd_long_as_integer": false},
            "target_model_ledger_id": "model:main"
        });
        let req: IcebergGenerateRequest = serde_json::from_value(body).unwrap();

        assert_eq!(req.tables.len(), 2);
        assert_eq!(req.tables[0].namespace, "DW");
        assert_eq!(req.tables[0].name, "DIM_GEOGRAPHY");
        assert_eq!(req.base_namespace, "http://ns.fluree.dev/edw#");

        assert_eq!(req.per_table_overrides.len(), 1);
        assert_eq!(req.per_table_overrides[0].name, "DIM_SUPPLIER");
        assert_eq!(
            req.per_table_overrides[0].primary_key.as_deref(),
            Some("ALT_KEY")
        );
        assert!(req.per_table_overrides[0].class_name.is_none());

        // Explicit `false` overrides the default; the other knobs default `true`.
        assert!(!req.options.xsd_long_as_integer);
        assert!(req.options.emit_fk_joins);
        assert!(req.options.keep_fk_keys_as_literals);

        assert_eq!(req.target_model_ledger_id.as_deref(), Some("model:main"));

        // The flattened connection builds a REST connection carrying the scope.
        let conn = build_connection_with(req.connection, &rules("")).unwrap();
        assert!(conn.is_rest());
    }

    #[test]
    fn verify_request_flattens_connection_and_table() {
        let body = serde_json::json!({
            "mode": "rest",
            "catalog_uri": "https://catalog.example.com",
            "warehouse": "wh1",
            "oauth2_token_url": "https://catalog.example.com/v1/oauth/tokens",
            "oauth2_client_secret": "pat",
            "oauth2_scope": "session:role:ICEBERG_READER",
            "table": "DW.DIM_STORE"
        });
        let req: IcebergVerifyRequest = serde_json::from_value(body).unwrap();
        assert_eq!(req.table, "DW.DIM_STORE");
        let conn = build_connection_with(req.connection, &rules("")).unwrap();
        assert!(conn.is_rest());
    }

    #[test]
    fn generate_request_defaults_overrides_and_options() {
        // Overrides and options are optional; options default to all-`true`.
        let body = serde_json::json!({
            "mode": "rest",
            "catalog_uri": "https://catalog.example.com",
            "tables": [{"namespace": "DW", "name": "DIM_DATE"}],
            "base_namespace": "http://ns.fluree.dev/edw#"
        });
        let req: IcebergGenerateRequest = serde_json::from_value(body).unwrap();
        assert!(req.per_table_overrides.is_empty());
        assert!(req.options.xsd_long_as_integer);
        assert!(req.options.emit_fk_joins);
        assert!(req.options.keep_fk_keys_as_literals);
        assert!(req.target_model_ledger_id.is_none());
    }
}
