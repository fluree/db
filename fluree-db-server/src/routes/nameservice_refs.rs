//! Nameservice ref endpoints for remote sync
//!
//! These endpoints allow remote clients to push ref updates, initialize ledgers,
//! and fetch snapshots of all nameservice records. They are the server-side
//! counterpart to the `fluree-db-nameservice-sync` crate's client operations.
//!
//! # Endpoints
//! - `POST /fluree/nameservice/refs/:alias/commit` — CAS push for commit head
//! - `POST /fluree/nameservice/refs/:alias/index` — CAS push for index head
//! - `POST /fluree/nameservice/refs/:alias/init` — Create ledger if absent
//! - `GET /fluree/nameservice/snapshot` — Full snapshot of all records
//!
//! # Authorization
//! All endpoints require a Bearer token with storage proxy permissions.
//! Per-alias endpoints check `principal.is_authorized_for_ledger()`.
//! Snapshot filters results to the principal's authorized scope.
//!
//! # Server Role
//! These endpoints are only available on transaction servers (file-backed mode).
//! Peer-mode instances return 404 since write operations require a read-write nameservice.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use fluree_db_nameservice::{CasResult, NameServiceError, RefKind, RefValue};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::error::ServerError;
use crate::extract::StorageProxyBearer;
use crate::state::AppState;

// ============================================================================
// Request/Response Types
// ============================================================================

/// Request body for CAS push operations
#[derive(Debug, Deserialize)]
pub struct PushRefRequest {
    /// Expected current ref value (null for initial creation)
    pub expected: Option<RefValue>,
    /// New ref value to set
    pub new: RefValue,
}

/// Response for successful push
#[derive(Debug, Serialize)]
pub struct PushRefResponse {
    pub status: String,
    /// The ref value after the operation
    #[serde(rename = "ref", skip_serializing_if = "Option::is_none")]
    pub ref_value: Option<RefValue>,
    /// Actual value on conflict
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actual: Option<RefValue>,
}

/// Response for init operation
#[derive(Debug, Serialize)]
pub struct InitResponse {
    pub created: bool,
}

/// Response for snapshot operation
#[derive(Debug, Serialize)]
pub struct SnapshotResponse {
    pub ledgers: Vec<fluree_db_nameservice::NsRecord>,
    pub graph_sources: Vec<fluree_db_nameservice::GraphSourceRecord>,
}

// ============================================================================
// Guards
// ============================================================================

/// Ensure we are running in file-backed (transaction server) mode.
/// Returns 404 in proxy/peer mode since write operations require a read-write nameservice.
fn require_read_write_mode(state: &AppState) -> Result<(), ServerError> {
    if state.fluree.nameservice_mode().is_read_only() {
        return Err(ServerError::not_found(
            "Nameservice sync endpoints are not available in peer mode",
        ));
    }
    Ok(())
}

// ============================================================================
// Handlers
// ============================================================================

/// POST /fluree/nameservice/refs/:alias/commit
///
/// Compare-and-set push for the commit head ref.
/// Returns 200 on success, 409 on CAS conflict.
pub async fn push_commit_head(
    State(state): State<Arc<AppState>>,
    Path(alias): Path<String>,
    StorageProxyBearer(principal): StorageProxyBearer,
    Json(body): Json<PushRefRequest>,
) -> Result<Response, ServerError> {
    require_read_write_mode(&state)?;

    if !principal.is_authorized_for_ledger(&alias) {
        return Err(ServerError::not_found("Ledger not found"));
    }

    push_ref_inner(&state, &alias, RefKind::CommitHead, body).await
}

/// POST /fluree/nameservice/refs/:alias/index
///
/// Compare-and-set push for the index head ref.
/// Returns 200 on success, 409 on CAS conflict.
pub async fn push_index_head(
    State(state): State<Arc<AppState>>,
    Path(alias): Path<String>,
    StorageProxyBearer(principal): StorageProxyBearer,
    Json(body): Json<PushRefRequest>,
) -> Result<Response, ServerError> {
    require_read_write_mode(&state)?;

    if !principal.is_authorized_for_ledger(&alias) {
        return Err(ServerError::not_found("Ledger not found"));
    }

    push_ref_inner(&state, &alias, RefKind::IndexHead, body).await
}

/// Shared implementation for push_commit_head and push_index_head
async fn push_ref_inner(
    state: &AppState,
    alias: &str,
    kind: RefKind,
    body: PushRefRequest,
) -> Result<Response, ServerError> {
    let ns = state.fluree.nameservice_mode().publisher().ok_or_else(|| {
        ServerError::internal("Write operations require a read-write nameservice")
    })?;

    let result = ns
        .compare_and_set_ref(alias, kind, body.expected.as_ref(), &body.new)
        .await
        .map_err(|e| ServerError::internal(format!("CAS operation failed: {e}")))?;

    match result {
        CasResult::Updated => {
            let resp = PushRefResponse {
                status: "updated".to_string(),
                ref_value: Some(body.new),
                actual: None,
            };
            Ok((StatusCode::OK, Json(resp)).into_response())
        }
        CasResult::Conflict { actual } => {
            let resp = PushRefResponse {
                status: "conflict".to_string(),
                ref_value: None,
                actual,
            };
            Ok((StatusCode::CONFLICT, Json(resp)).into_response())
        }
    }
}

/// POST /fluree/nameservice/refs/:alias/init
///
/// Initialize a ledger on the nameservice (create-if-absent).
/// Returns `{ "created": true }` if new, `{ "created": false }` if already existed.
pub async fn init_ledger(
    State(state): State<Arc<AppState>>,
    Path(alias): Path<String>,
    StorageProxyBearer(principal): StorageProxyBearer,
) -> Result<Json<InitResponse>, ServerError> {
    require_read_write_mode(&state)?;

    if !principal.is_authorized_for_ledger(&alias) {
        return Err(ServerError::not_found("Ledger not found"));
    }

    let ns = state.fluree.nameservice_mode().publisher().ok_or_else(|| {
        ServerError::internal("Write operations require a read-write nameservice")
    })?;

    match ns.publish_ledger_init(&alias).await {
        Ok(()) => Ok(Json(InitResponse { created: true })),
        Err(NameServiceError::LedgerAlreadyExists(_)) => Ok(Json(InitResponse { created: false })),
        Err(e) => Err(ServerError::internal(format!("Init failed: {e}"))),
    }
}

/// GET /fluree/nameservice/snapshot
///
/// Returns a full snapshot of all ledger and graph source records.
/// Results are filtered to the principal's authorized scope:
/// - `storage_all`: returns all records
/// - Otherwise: filters ledgers to `storage_ledgers`, excludes graph sources
pub async fn snapshot(
    State(state): State<Arc<AppState>>,
    StorageProxyBearer(principal): StorageProxyBearer,
) -> Result<Json<SnapshotResponse>, ServerError> {
    require_read_write_mode(&state)?;

    let fluree = &state.fluree;
    let ns = fluree.nameservice();

    let all_ledgers = ns
        .all_records()
        .await
        .map_err(|e| ServerError::internal(format!("Failed to list ledgers: {e}")))?;

    if principal.storage_all {
        // Full access: return everything
        let graph_sources = ns
            .all_graph_source_records()
            .await
            .map_err(|e| ServerError::internal(format!("Failed to list graph sources: {e}")))?;

        Ok(Json(SnapshotResponse {
            ledgers: all_ledgers,
            graph_sources,
        }))
    } else {
        // Scoped access: filter ledgers to authorized set, no graph sources
        let ledgers = all_ledgers
            .into_iter()
            .filter(|r| principal.is_authorized_for_ledger(&r.ledger_id))
            .collect();

        Ok(Json(SnapshotResponse {
            ledgers,
            graph_sources: vec![],
        }))
    }
}
