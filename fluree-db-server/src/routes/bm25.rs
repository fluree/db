//! BM25 full-text index management endpoints: POST /v1/fluree/bm25/create and
//! POST /v1/fluree/bm25/sync
//!
//! A BM25 index is a Fluree *graph source*, so only creation and sync need
//! family-specific routes — listing, inspection, and drop are served by the
//! graph-source fallbacks in [`super::ledger`] (`/ledgers`, `/info`, `/drop`),
//! the same division the Iceberg family uses.

use crate::config::ServerRole;
use crate::error::{Result, ServerError};
use crate::extract::FlureeHeaders;
use crate::state::AppState;
use crate::telemetry::{create_request_span, extract_request_id, extract_trace_id};
use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use fluree_db_api::Bm25CreateConfig;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::Instrument;

use super::ledger::forward_write_request;

/// Request body for `POST /v1/fluree/bm25/create`
#[derive(Deserialize)]
pub struct Bm25CreateRequest {
    /// Graph-source name for the index (no `:`). The resulting alias is
    /// `<name>:<branch>`.
    pub name: String,
    /// Source ledger alias to index (e.g. `"docs:main"`).
    pub ledger: String,
    /// Branch for the index graph source. Defaults to `"main"`.
    pub branch: Option<String>,
    /// Indexing query (FQL / JSON-LD) selecting the documents and the text
    /// properties to index. Must select `@id`.
    pub query: serde_json::Value,
    /// BM25 k1 (term-frequency saturation). Defaults to 1.2.
    pub k1: Option<f64>,
    /// BM25 b (document-length normalization, 0..=1). Defaults to 0.75.
    pub b: Option<f64>,
}

/// Response for `POST /v1/fluree/bm25/create`
#[derive(Serialize)]
pub struct Bm25CreateResponse {
    pub graph_source_id: String,
    pub doc_count: usize,
    pub term_count: usize,
    pub index_t: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_id: Option<String>,
}

/// Create a BM25 full-text search index over a ledger
///
/// POST /v1/fluree/bm25/create
pub async fn bm25_create(State(state): State<Arc<AppState>>, request: Request) -> Response {
    if state.config.server_role == ServerRole::Peer {
        return forward_write_request(&state, request).await;
    }

    bm25_create_local(state, request).await.into_response()
}

async fn bm25_create_local(state: Arc<AppState>, request: Request) -> Result<impl IntoResponse> {
    let headers = FlureeHeaders::from_headers(request.headers())?;

    let body_bytes = axum::body::to_bytes(request.into_body(), 50 * 1024 * 1024)
        .await
        .map_err(|e| ServerError::bad_request(format!("Failed to read body: {e}")))?;
    let req: Bm25CreateRequest = serde_json::from_slice(&body_bytes)
        .map_err(|e| ServerError::bad_request(format!("Invalid JSON: {e}")))?;

    let request_id = extract_request_id(&headers.raw, &state.telemetry_config);
    let trace_id = extract_trace_id(&headers.raw);

    let span = create_request_span(
        "bm25:create",
        request_id.as_deref(),
        trace_id.as_deref(),
        Some(&req.name),
        None,
        None,
    );
    async move {
        tracing::info!(
            status = "start",
            name = %req.name,
            ledger = %req.ledger,
            "bm25 index create requested"
        );

        let config = build_bm25_config(req);
        let source_ledger = config.ledger.clone();
        let result = state
            .fluree
            .create_full_text_index(config)
            .await
            .map_err(|e| {
                if e.is_not_found() {
                    ServerError::not_found(format!("Ledger not found: {source_ledger}"))
                } else {
                    ServerError::Api(e)
                }
            })?;

        let response = Bm25CreateResponse {
            graph_source_id: result.graph_source_id,
            doc_count: result.doc_count,
            term_count: result.term_count,
            index_t: result.index_t,
            index_id: result.index_id.map(|id| id.to_string()),
        };

        tracing::info!(
            status = "success",
            graph_source_id = %response.graph_source_id,
            doc_count = response.doc_count,
            index_t = response.index_t,
            "bm25 index created"
        );
        Ok((StatusCode::CREATED, Json(response)))
    }
    .instrument(span)
    .await
}

fn build_bm25_config(req: Bm25CreateRequest) -> Bm25CreateConfig {
    let Bm25CreateRequest {
        name,
        ledger,
        branch,
        query,
        k1,
        b,
    } = req;

    let mut config = Bm25CreateConfig::new(name, ledger, query);
    if let Some(branch) = branch {
        config = config.with_branch(branch);
    }
    if let Some(k1) = k1 {
        config = config.with_k1(k1);
    }
    if let Some(b) = b {
        config = config.with_b(b);
    }
    config
}

/// Request body for `POST /v1/fluree/bm25/sync`
#[derive(Deserialize)]
pub struct Bm25SyncRequest {
    /// Index graph-source alias to sync (e.g. `"docsearch:main"`).
    pub index: String,
}

/// Query parameters for `POST /v1/fluree/bm25/sync`
#[derive(Deserialize)]
struct Bm25SyncParams {
    /// Source-ledger `t` to sync through. Absent syncs through the source's
    /// current head.
    t: Option<i64>,
}

/// Response for `POST /v1/fluree/bm25/sync`
#[derive(Serialize)]
pub struct Bm25SyncResponse {
    pub graph_source_id: String,
    pub upserted: usize,
    pub removed: usize,
    pub affected_subjects: usize,
    pub old_watermark: i64,
    pub new_watermark: i64,
    pub was_full_resync: bool,
}

/// Sync a BM25 full-text index up to its source ledger's state
///
/// POST /v1/fluree/bm25/sync
/// POST /v1/fluree/bm25/sync?t=<t>
pub async fn bm25_sync(State(state): State<Arc<AppState>>, request: Request) -> Response {
    if state.config.server_role == ServerRole::Peer {
        return forward_write_request(&state, request).await;
    }

    bm25_sync_local(state, request).await.into_response()
}

async fn bm25_sync_local(state: Arc<AppState>, request: Request) -> Result<impl IntoResponse> {
    let headers = FlureeHeaders::from_headers(request.headers())?;
    let target_t = sync_target_t(&request)?;

    let body_bytes = axum::body::to_bytes(request.into_body(), 50 * 1024 * 1024)
        .await
        .map_err(|e| ServerError::bad_request(format!("Failed to read body: {e}")))?;
    let req: Bm25SyncRequest = serde_json::from_slice(&body_bytes)
        .map_err(|e| ServerError::bad_request(format!("Invalid JSON: {e}")))?;

    let request_id = extract_request_id(&headers.raw, &state.telemetry_config);
    let trace_id = extract_trace_id(&headers.raw);

    let span = create_request_span(
        "bm25:sync",
        request_id.as_deref(),
        trace_id.as_deref(),
        Some(&req.index),
        None,
        None,
    );
    async move {
        tracing::info!(
            status = "start",
            index = %req.index,
            target_t = ?target_t,
            "bm25 index sync requested"
        );

        // `timeout_ms` is accepted but ignored by the API today, so the pinned
        // path passes `None` rather than inventing a surface for it here.
        let result = match target_t {
            Some(t) => state.fluree.sync_bm25_index_to(&req.index, t, None).await,
            None => state.fluree.sync_bm25_index(&req.index).await,
        }
        .map_err(ServerError::Api)?;

        let response = Bm25SyncResponse {
            graph_source_id: result.graph_source_id,
            upserted: result.upserted,
            removed: result.removed,
            affected_subjects: result.affected_subjects,
            old_watermark: result.old_watermark,
            new_watermark: result.new_watermark,
            was_full_resync: result.was_full_resync,
        };

        tracing::info!(
            status = "success",
            graph_source_id = %response.graph_source_id,
            upserted = response.upserted,
            removed = response.removed,
            new_watermark = response.new_watermark,
            "bm25 index synced"
        );
        Ok(Json(response))
    }
    .instrument(span)
    .await
}

/// Read the `t` query parameter, if present.
///
/// Deliberately surfaces a deserialization failure as a 400 instead of
/// following the `.ok().unwrap_or_default()` idiom used elsewhere for optional
/// params: silently discarding an unparseable `t` would sync through the
/// source's head, which is the opposite of the pinned sync the caller asked
/// for.
///
/// A ledger's first commit is `t = 1`, so `t < 1` names no commit. The pinned
/// sync path does not reject it — it rebuilds at that `t` and reports success
/// having moved the index's watermark backwards to a value no commit can
/// restore — so it is refused here.
fn sync_target_t(request: &Request) -> Result<Option<i64>> {
    let Some(query) = request.uri().query() else {
        return Ok(None);
    };

    let params: Bm25SyncParams = serde_urlencoded::from_str(query)
        .map_err(|e| ServerError::bad_request(format!("Invalid query parameter: {e}")))?;

    match params.t {
        Some(t) if t < 1 => Err(ServerError::bad_request(format!(
            "t must be a positive commit number, got {t}"
        ))),
        target_t => Ok(target_t),
    }
}
