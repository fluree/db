//! Commit show endpoint: `GET /v1/fluree/show/*ledger`.
//!
//! Returns a decoded commit with resolved IRIs — the server-side equivalent
//! of `fluree show`. Flakes are filtered by the caller's policy identity,
//! matching the same auth semantics as the query endpoints.

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
use serde::Deserialize;
use std::sync::Arc;
use tracing::Instrument;

/// Query parameters for commit show.
#[derive(Deserialize)]
pub struct CommitShowQuery {
    /// Commit identifier: "t:<N>" for transaction number, hex prefix, or full CID.
    pub commit: Option<String>,
    /// Ledger alias (alternative to path param).
    pub ledger: Option<String>,
}

/// Show a decoded commit (ledger in path tail).
///
/// `GET /v1/fluree/show/<ledger...>?commit=<ref>`
pub async fn show_ledger_tail(
    State(state): State<Arc<AppState>>,
    Path(ledger): Path<String>,
    headers: FlureeHeaders,
    bearer: MaybeDataBearer,
    Query(mut query): Query<CommitShowQuery>,
    request: Request,
) -> Response {
    // In peer mode, forward to transactor.
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

    query.ledger = Some(ledger);
    show_local(state, headers, bearer, query)
        .await
        .into_response()
}

async fn show_local(
    state: Arc<AppState>,
    headers: FlureeHeaders,
    bearer: MaybeDataBearer,
    query: CommitShowQuery,
) -> Result<Response> {
    let request_id = extract_request_id(&headers.raw, &state.telemetry_config);
    let trace_id = extract_trace_id(&headers.raw);

    let span = create_request_span(
        "commit:show",
        request_id.as_deref(),
        trace_id.as_deref(),
        None,
        None,
        None,
    );
    async move {
        let span = tracing::Span::current();

        tracing::info!(status = "start", "commit show requested");

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
                return Err(e);
            }
        };

        // Get commit identifier
        let commit_ref = match query.commit.as_deref() {
            Some(c) if !c.is_empty() => c,
            _ => {
                set_span_error_code(&span, "error:BadRequest");
                return Err(ServerError::bad_request(
                    "Missing required query parameter: commit",
                ));
            }
        };

        // Enforce data auth (same pattern as query endpoints)
        let data_auth = state.config.data_auth();
        if data_auth.mode == crate::config::DataAuthMode::Required && bearer.0.is_none() {
            set_span_error_code(&span, "error:Unauthorized");
            return Err(ServerError::unauthorized("Bearer token required"));
        }
        if let Some(p) = bearer.0.as_ref() {
            if !p.can_read(&alias) {
                set_span_error_code(&span, "error:Forbidden");
                return Err(ServerError::not_found("Ledger not found"));
            }
        }

        // Extract identity and policy class for flake-level filtering.
        // Identity comes from the bearer token; policy_class from server
        // config. Unlike the query endpoints, show does not accept
        // per-request policy overrides (no signed body or header injection).
        let identity = bearer.0.as_ref().and_then(|p| p.identity.clone());
        let policy_class = data_auth.default_policy_class.as_deref();

        // Proxy storage mode cannot decode commits (no local index).
        if state.config.is_proxy_storage_mode() {
            set_span_error_code(&span, "error:NotImplemented");
            return Err(ServerError::NotImplemented(
                "Commit show is not available in proxy storage mode".to_string(),
            ));
        }

        let fluree = &state.fluree;

        // Parse commit ref: "t:N" → by transaction number, otherwise by prefix/CID
        let detail = if let Some(t_str) = commit_ref.strip_prefix("t:") {
            let t: i64 = t_str.parse().map_err(|_| {
                ServerError::bad_request(format!("Invalid transaction number: '{t_str}'"))
            })?;
            fluree
                .graph(&alias)
                .commit_t(t)
                .identity(identity.as_deref())
                .policy_class(policy_class)
                .execute()
                .await
                .map_err(ServerError::Api)?
        } else {
            fluree
                .graph(&alias)
                .commit_prefix(commit_ref)
                .identity(identity.as_deref())
                .policy_class(policy_class)
                .execute()
                .await
                .map_err(ServerError::Api)?
        };

        tracing::info!(status = "success", t = detail.t, "commit show complete");
        Ok(Json(detail).into_response())
    }
    .instrument(span)
    .await
}
