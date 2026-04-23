//! Commit export endpoint: `GET /v1/fluree/commits/*ledger`.
//!
//! Returns paginated commit blobs using address-cursor pagination.
//! Each page walks backward via `parents` — O(limit) per page.
//!
//! Requires `fluree.storage.*` permissions (replication-grade, not `ledger.read`).

use crate::config::ServerRole;
use crate::error::{Result, ServerError};
use crate::extract::StorageProxyBearer;
use crate::state::AppState;
use axum::extract::{Path, Query, Request, State};
use axum::response::{IntoResponse, Response};
use fluree_db_api::ExportCommitsRequest;
use std::sync::Arc;

/// Export commits from a ledger (ledger in path tail).
///
/// `GET /v1/fluree/commits/<ledger...>?cursor=<addr>&limit=100`
pub async fn commits_ledger_tail(
    State(state): State<Arc<AppState>>,
    Path(ledger): Path<String>,
    // Option so we can reach peer-mode forwarding even without a valid token.
    // Auth is enforced in commits_ledger_local for non-peer mode.
    principal: Option<StorageProxyBearer>,
    Query(params): Query<ExportCommitsRequest>,
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

    commits_ledger_local(state, ledger, principal, params)
        .await
        .into_response()
}

async fn commits_ledger_local(
    state: Arc<AppState>,
    ledger: String,
    principal: Option<StorageProxyBearer>,
    params: ExportCommitsRequest,
) -> Result<axum::Json<fluree_db_api::ExportCommitsResponse>> {
    // Enforce replication-grade permissions (fluree.storage.*).
    // StorageProxyBearer returns None when storage proxy is disabled or token is
    // missing/invalid; we map these to appropriate errors.
    let StorageProxyBearer(principal) = principal.ok_or_else(|| {
        if state.config.storage_proxy().enabled {
            ServerError::unauthorized("Bearer token with storage permissions required")
        } else {
            ServerError::not_found("Storage proxy not enabled")
        }
    })?;

    if !principal.is_authorized_for_ledger(&ledger) {
        return Err(ServerError::not_found("Ledger not found"));
    }

    // Load cached ledger handle.
    let handle = state
        .fluree
        .ledger_cached(&ledger)
        .await
        .map_err(ServerError::Api)?;

    let fluree = &state.fluree;
    let resp = fluree
        .export_commit_range(&handle, &params)
        .await
        .map_err(ServerError::Api)?;

    Ok(axum::Json(resp))
}
