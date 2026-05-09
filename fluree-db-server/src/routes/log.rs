//! Commit log endpoint: `GET /v1/fluree/log/*ledger`.
//!
//! Returns lightweight per-commit summaries (newest-first by `t`) for use by
//! `fluree log` and similar history views. Unlike `/commits`, this endpoint
//! uses normal data-read auth — it does not return raw commit blobs.

use crate::config::ServerRole;
use crate::error::{Result, ServerError};
use crate::extract::{FlureeHeaders, MaybeDataBearer};
use crate::state::AppState;
use crate::telemetry::{
    create_request_span, extract_request_id, extract_trace_id, set_span_error_code,
};
use axum::extract::{Path, Query, Request, State};
use axum::response::{IntoResponse, Response};
use axum::Json;
use fluree_db_api::CommitSummary;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::Instrument;

/// Hard cap on the number of summaries returned in one response, regardless
/// of the client's `limit` parameter. Mirrors the merge-preview hard cap.
const LOG_HARD_MAX_COMMITS: usize = 5_000;

/// Default cap when the client omits `limit`.
const LOG_DEFAULT_LIMIT: usize = 100;

#[derive(Deserialize)]
pub struct LogQuery {
    /// Maximum summaries to return (newest-first). Server clamps to a hard max.
    pub limit: Option<usize>,
}

#[derive(Serialize)]
pub struct LogResponse {
    pub ledger_id: String,
    pub commits: Vec<CommitSummary>,
    /// Total commits in the chain, regardless of `limit`. `truncated == count >
    /// commits.len()`.
    pub count: usize,
    pub truncated: bool,
}

/// `GET /v1/fluree/log/<ledger...>?limit=N`
pub async fn log_ledger_tail(
    State(state): State<Arc<AppState>>,
    Path(ledger): Path<String>,
    headers: FlureeHeaders,
    bearer: MaybeDataBearer,
    Query(query): Query<LogQuery>,
    request: Request,
) -> Response {
    if state.config.server_role == ServerRole::Peer {
        let client = match state.forwarding_client.as_ref() {
            Some(c) => c,
            None => {
                return ServerError::internal("Forwarding client not configured").into_response()
            }
        };
        return match client.forward(request).await {
            Ok(resp) => resp,
            Err(e) => e.into_response(),
        };
    }

    log_local(state, ledger, headers, bearer, query)
        .await
        .into_response()
}

async fn log_local(
    state: Arc<AppState>,
    ledger: String,
    headers: FlureeHeaders,
    bearer: MaybeDataBearer,
    query: LogQuery,
) -> Result<Response> {
    let request_id = extract_request_id(&headers.raw, &state.telemetry_config);
    let trace_id = extract_trace_id(&headers.raw);

    let span = create_request_span(
        "commit:log",
        request_id.as_deref(),
        trace_id.as_deref(),
        Some(&ledger),
        None,
        None,
    );
    async move {
        let span = tracing::Span::current();
        tracing::info!(status = "start", "commit log requested");

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

        let limit = query
            .limit
            .unwrap_or(LOG_DEFAULT_LIMIT)
            .min(LOG_HARD_MAX_COMMITS);

        let (commits, count) = state
            .fluree
            .commit_log(&ledger, Some(limit))
            .await
            .map_err(ServerError::Api)?;

        let truncated = count > commits.len();
        tracing::info!(
            status = "success",
            count,
            returned = commits.len(),
            "commit log complete"
        );
        Ok(Json(LogResponse {
            ledger_id: ledger,
            commits,
            count,
            truncated,
        })
        .into_response())
    }
    .instrument(span)
    .await
}
