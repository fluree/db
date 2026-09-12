//! Push commit ingestion endpoint: `POST /v1/fluree/push/*ledger`.
//!
//! Accepts precomputed commit v2 bytes from a client, validates them against the
//! current ledger state (strict sequencing + retraction invariant + policy + SHACL),
//! stores the commit blobs, and advances commit head via CAS.

use crate::config::ServerRole;
use crate::error::{Result, ServerError};
use crate::extract::{FlureeHeaders, MaybeDataBearer};
use crate::routes::transact::{extract_idempotency_key, submission_error_to_server_error};
use crate::state::AppState;
use axum::extract::{Path, Request, State};
use axum::response::{IntoResponse, Response};
use fluree_db_api::{GovernanceOptions, PushCommitsRequest, PushCommitsResponse, PushedHead};
use fluree_db_consensus::PushRequest;
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

    // Push has no signed body credential; bind its verified bearer exactly as
    // ordinary writes do, then resolve the complete header selection.
    let headers =
        crate::routes::policy_auth::bind_authorization(&state, headers, bearer.as_ref(), None)?;
    let governance = if bearer.is_some() {
        crate::routes::policy_auth::bound_governance(headers.identity.as_deref(), &headers)?
    } else {
        // Preserve the anonymous push defaults, including the server class.
        // No bearer means no verified identity, so `f:IdentityRestricted`
        // override control denies this request, as it should.
        GovernanceOptions {
            policy_class: data_auth.default_policy_class.map(|c| vec![c]).or_else(|| {
                (!headers.policy_class.is_empty()).then(|| headers.policy_class.clone())
            }),
            policy: headers.policy.clone(),
            policy_values: headers.policy_values_map()?,
            ..Default::default()
        }
    };

    let idempotency_key = extract_idempotency_key(&headers.raw)?;

    let bytes = axum::body::to_bytes(request.into_body(), 50 * 1024 * 1024)
        .await
        .map_err(|e| ServerError::bad_request(format!("failed to read request body: {e}")))?;
    let parsed: PushCommitsRequest = serde_json::from_slice(&bytes)?;

    let req = PushRequest {
        idempotency_key,
        ledger_id: ledger,
        commits: parsed.commits.into_iter().map(|b| b.0).collect(),
        blobs: parsed.blobs.into_iter().map(|(k, v)| (k, v.0)).collect(),
        governance,
    };

    let receipt = state
        .committer
        .push(req)
        .await
        .map_err(submission_error_to_server_error)?;

    Ok(axum::Json(PushCommitsResponse {
        ledger: receipt.ledger,
        accepted: receipt.accepted,
        head: PushedHead {
            t: receipt.head_t,
            commit_id: receipt.head_id,
        },
        indexing: receipt.indexing,
    }))
}
