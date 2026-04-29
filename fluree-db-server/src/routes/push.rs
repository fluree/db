//! Push commit ingestion endpoint: `POST /v1/fluree/push/*ledger`.
//!
//! Accepts precomputed commit v2 bytes from a client, validates them against the
//! current ledger state (strict sequencing + retraction invariant + policy + SHACL),
//! stores the commit blobs, and advances commit head via CAS.

use crate::config::ServerRole;
use crate::error::{Result, ServerError};
use crate::extract::{FlureeHeaders, MaybeDataBearer};
use crate::state::AppState;
use axum::extract::{Path, Request, State};
use axum::response::{IntoResponse, Response};
use fluree_db_api::{PushCommitsRequest, PushCommitsResponse, QueryConnectionOptions};
use std::sync::Arc;

/// Push commits to a ledger (ledger in path tail).
///
/// `POST /v1/fluree/push/<ledger...>`
pub async fn push_ledger_tail(
    State(state): State<Arc<AppState>>,
    Path(ledger): Path<String>,
    headers: FlureeHeaders,
    MaybeDataBearer(bearer): MaybeDataBearer,
    request: Request,
) -> Response {
    // In peer mode, forward to transaction server.
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

    push_ledger_local(state, ledger, headers, bearer, request)
        .await
        .into_response()
}

async fn push_ledger_local(
    state: Arc<AppState>,
    ledger: String,
    headers: FlureeHeaders,
    bearer: Option<crate::extract::DataPrincipal>,
    request: Request,
) -> Result<axum::Json<PushCommitsResponse>> {
    // Enforce data auth rules (Bearer token only for push in this first cut).
    let data_auth = state.config.data_auth();
    if data_auth.mode == crate::config::DataAuthMode::Required && bearer.is_none() {
        return Err(ServerError::unauthorized("Bearer token required"));
    }

    // Enforce bearer ledger scope (avoid existence leak).
    if let Some(p) = bearer.as_ref() {
        if !p.can_write(&ledger) {
            return Err(ServerError::not_found("Ledger not found"));
        }
    }

    // Load cached ledger handle.
    let handle = state
        .fluree
        .ledger_cached(&ledger)
        .await
        .map_err(ServerError::Api)?;

    // Build policy options.
    //
    let mut opts = QueryConnectionOptions {
        // Identity is non-spoofable: derived from bearer token (fluree.identity ?? sub).
        identity: bearer.as_ref().and_then(|p| p.identity.clone()),
        // Allow client-provided inline policy and policy-values headers.
        policy: headers.policy.clone(),
        policy_values: headers.policy_values_map()?,
        ..Default::default()
    };

    // Force server default policy-class if configured (non-spoofable).
    if let Some(pc) = data_auth.default_policy_class.as_ref() {
        opts.policy_class = Some(vec![pc.clone()]);
    } else if !headers.policy_class.is_empty() {
        opts.policy_class = Some(headers.policy_class.clone());
    }

    // Index config: server-level override if present, else canonical default.
    let index_config_owned = state
        .index_config
        .clone()
        .unwrap_or_else(fluree_db_api::server_defaults::default_index_config);
    let index_config = &index_config_owned;

    // Parse JSON body.
    let bytes = axum::body::to_bytes(request.into_body(), 50 * 1024 * 1024)
        .await
        .map_err(|e| ServerError::bad_request(format!("failed to read request body: {e}")))?;
    let body: PushCommitsRequest = serde_json::from_slice(&bytes)?;

    let fluree = &state.fluree;
    let resp = fluree
        .push_commits_with_handle(&handle, body, &opts, index_config)
        .await
        .map_err(ServerError::Api)?;

    Ok(axum::Json(resp))
}
