//! Push commit ingestion endpoints: `POST /v1/fluree/push/*ledger` and
//! `POST /v1/fluree/push-merges/*ledger`.
//!
//! Accepts precomputed commit v2 bytes from a client, validates them against the
//! current ledger state (strict sequencing + retraction invariant + policy + SHACL),
//! stores the commit blobs, and advances commit head via CAS.
//!
//! A push whose commits include a merge also carries the commits that merge
//! brought in, and it goes to `push-merges`. A server predating that endpoint
//! answers it with 404 rather than storing the merge commits without their
//! parents, which is what it would do with the extra field on `push`. The
//! `push` endpoint refuses such a body for the same reason: a client that
//! sends one there cannot tell those two servers apart.

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

/// Whether the endpoint a request arrived on takes the commits a merge
/// brought in.
#[derive(Clone, Copy, PartialEq, Eq)]
enum MergedCommits {
    Accepted,
    Refused,
}

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
    push_tail(
        state,
        ledger,
        headers,
        bearer,
        request,
        MergedCommits::Refused,
    )
    .await
}

/// Push commits to a ledger, including the commits its merges brought in.
///
/// `POST /v1/fluree/push-merges/<ledger...>`
pub async fn push_merges_ledger_tail(
    State(state): State<Arc<AppState>>,
    Path(ledger): Path<String>,
    headers: FlureeHeaders,
    MaybeDataBearer(bearer): MaybeDataBearer,
    request: Request,
) -> Response {
    push_tail(
        state,
        ledger,
        headers,
        bearer,
        request,
        MergedCommits::Accepted,
    )
    .await
}

async fn push_tail(
    state: Arc<AppState>,
    ledger: String,
    headers: FlureeHeaders,
    bearer: Option<crate::extract::DataPrincipal>,
    request: Request,
    merged: MergedCommits,
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

    push_ledger_local(state, ledger, headers, bearer, request, merged)
        .await
        .into_response()
}

async fn push_ledger_local(
    state: Arc<AppState>,
    ledger: String,
    headers: FlureeHeaders,
    bearer: Option<crate::extract::DataPrincipal>,
    request: Request,
    merged: MergedCommits,
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

    if merged == MergedCommits::Refused && !parsed.merged_commits.is_empty() {
        return Err(ServerError::bad_request(format!(
            "this push carries the commits its merges brought in; send it to \
             POST /v1/fluree/push-merges/{ledger}"
        )));
    }

    let req = PushRequest {
        idempotency_key,
        ledger_id: ledger,
        commits: parsed.commits.into_iter().map(|b| b.0).collect(),
        blobs: parsed.blobs.into_iter().map(|(k, v)| (k, v.0)).collect(),
        merged_commits: parsed.merged_commits.into_iter().map(|b| b.0).collect(),
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
