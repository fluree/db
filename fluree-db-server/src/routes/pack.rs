//! Pack stream endpoint: `POST /v1/fluree/pack/*ledger`.
//!
//! Streams binary pack frames containing commit blobs and (optionally) index
//! artifacts for efficient clone/pull. Uses `application/x-fluree-pack` content
//! type and natural backpressure via an `mpsc` channel.
//!
//! Auth: requires `StorageProxyBearer` with `fluree.storage.*` scope (same as
//! commit export and storage proxy endpoints).

use crate::config::ServerRole;
use crate::error::{Result, ServerError};
use crate::extract::StorageProxyBearer;
use crate::state::AppState;
use axum::body::Body;
use axum::extract::{Path, Request, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use fluree_db_api::pack::{stream_pack, validate_pack_request};
use fluree_db_core::pack::PackRequest;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::debug;

/// Content type for pack responses.
const PACK_CONTENT_TYPE: &str = "application/x-fluree-pack";

/// Pack endpoint (ledger in path tail).
///
/// `POST /v1/fluree/pack/<ledger...>`
pub async fn pack_ledger_tail(
    State(state): State<Arc<AppState>>,
    Path(ledger): Path<String>,
    principal: Option<StorageProxyBearer>,
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

    pack_ledger_local(state, ledger, principal, request)
        .await
        .into_response()
}

async fn pack_ledger_local(
    state: Arc<AppState>,
    ledger: String,
    principal: Option<StorageProxyBearer>,
    request: Request,
) -> Result<Response> {
    // Enforce replication-grade permissions (fluree.storage.*).
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

    // Read and validate request body.
    let body_bytes = axum::body::to_bytes(request.into_body(), 1024 * 1024)
        .await
        .map_err(|e| ServerError::bad_request(format!("failed to read request body: {e}")))?;
    let pack_request: PackRequest = serde_json::from_slice(&body_bytes)?;

    if let Err(msg) = validate_pack_request(&pack_request) {
        return Err(ServerError::bad_request(msg));
    }

    // Load cached ledger handle.
    let handle = state
        .fluree
        .ledger_cached(&ledger)
        .await
        .map_err(ServerError::Api)?;

    debug!(
        ledger = %ledger,
        want_count = pack_request.want.len(),
        have_count = pack_request.have.len(),
        include_indexes = pack_request.include_indexes,
        include_txns = pack_request.include_txns,
        "pack: starting stream"
    );

    // Create channel for backpressure-aware streaming.
    let (tx, rx) = mpsc::channel(64);

    // Spawn producer task.
    let fluree = state.fluree.clone();
    tokio::spawn(async move {
        stream_pack(&fluree, &handle, &pack_request, tx).await;
    });

    // Convert receiver into a body stream via futures::stream::unfold.
    let stream = futures::stream::unfold(rx, |mut rx| async move {
        let item = rx.recv().await?;
        let mapped = match item {
            Ok(bytes) => Ok(Bytes::from(bytes)),
            Err(e) => Err(std::io::Error::other(e.to_string())),
        };
        Some((mapped, rx))
    });

    let body = Body::from_stream(stream);
    let response = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, PACK_CONTENT_TYPE)
        .header(header::TRANSFER_ENCODING, "chunked")
        .body(body)
        .expect("response builder cannot fail");

    Ok(response)
}
