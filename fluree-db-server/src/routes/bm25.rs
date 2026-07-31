//! BM25 full-text index management endpoints: POST /v1/fluree/bm25/create
//!
//! A BM25 index is a Fluree *graph source*, so only creation needs a
//! family-specific route — listing, inspection, and drop are served by the
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
