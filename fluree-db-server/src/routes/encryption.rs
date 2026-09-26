//! Encryption key rotation endpoints.
//!
//! `GET /encryption` and `GET /encryption/rotate/status` are reads any node
//! answers from the storage-resident progress record. The rotate, pause,
//! cancel and verify endpoints are admin-gated writes that run where the
//! indexer runs: under Raft, on the leader (followers forward), so one node
//! holds the sweep and a leadership change hands it over.

use crate::error::{Result, ServerError};
use crate::state::AppState;
use axum::extract::State;
use axum::Json;
use fluree_db_api::key_rotation::{KeyRotationOptions, KeyRotationProgress, KeyRotationStatus};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// The identity a sweep records as its holder: the Raft node id when
/// clustered, else host and pid.
#[cfg_attr(not(feature = "raft"), allow(unused_variables))]
pub(crate) fn rotation_holder(state: &AppState) -> String {
    #[cfg(feature = "raft")]
    if let Some(id) = state.config.raft_node_id {
        return format!("node-{id}");
    }
    format!(
        "{}:{}",
        std::env::var("HOSTNAME").unwrap_or_else(|_| "server".to_string()),
        std::process::id()
    )
}

#[derive(Debug, Serialize)]
pub struct EncryptionResponse {
    /// Whether the storage encrypts at rest.
    pub encrypted: bool,
    /// Ids of the keys held, current first. Never key material.
    pub key_ids: Vec<u32>,
    pub current_key_id: Option<u32>,
}

/// `GET /encryption` — the keys this node holds, by id. Asks the storage
/// directly rather than inferring "not encrypted" from an error.
pub async fn encryption(State(state): State<Arc<AppState>>) -> Result<Json<EncryptionResponse>> {
    Ok(Json(match state.fluree.encryption_key_ids() {
        Some((key_ids, current_key_id)) => EncryptionResponse {
            encrypted: true,
            key_ids,
            current_key_id: Some(current_key_id),
        },
        None => EncryptionResponse {
            encrypted: false,
            key_ids: Vec::new(),
            current_key_id: None,
        },
    }))
}

/// `GET /encryption/rotate/status` — the progress record plus what this
/// node knows about the sweep.
pub async fn rotate_status(State(state): State<Arc<AppState>>) -> Result<Json<KeyRotationStatus>> {
    let status = state
        .fluree
        .key_rotation_status()
        .await
        .map_err(ServerError::Api)?;
    Ok(Json(status))
}

#[derive(Debug, Deserialize)]
pub struct RotateRequest {
    /// The key being retired; every blob on it is rewritten.
    pub retire_key_id: u32,
    #[serde(default)]
    pub dry_run: bool,
    /// Limit the sweep to one ledger, by name or branch-qualified id.
    #[serde(default)]
    pub ledger: Option<String>,
    /// Throttle on rewritten plaintext bytes per second.
    #[serde(default)]
    pub max_bytes_per_sec: Option<u64>,
}

/// `POST /encryption/rotate` — start, or resume, a rotation.
pub async fn rotate(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RotateRequest>,
) -> Result<Json<KeyRotationProgress>> {
    let holder = rotation_holder(&state);
    tracing::info!(
        retire_key_id = req.retire_key_id,
        dry_run = req.dry_run,
        ledger = ?req.ledger,
        holder = %holder,
        "key rotation requested"
    );
    let progress = state
        .fluree
        .start_key_rotation(KeyRotationOptions {
            retire_key_id: req.retire_key_id,
            dry_run: req.dry_run,
            ledger: req.ledger,
            max_bytes_per_sec: req.max_bytes_per_sec,
            holder,
        })
        .await
        .map_err(ServerError::Api)?;
    Ok(Json(progress))
}

#[derive(Debug, Serialize)]
pub struct SignalResponse {
    pub ok: bool,
}

/// `POST /encryption/rotate/pause` — stop after the next blob, resumable.
pub async fn rotate_pause(State(state): State<Arc<AppState>>) -> Result<Json<SignalResponse>> {
    state
        .fluree
        .pause_key_rotation()
        .map_err(ServerError::Api)?;
    Ok(Json(SignalResponse { ok: true }))
}

/// `POST /encryption/rotate/cancel` — stop after the next blob; the next
/// start begins over.
pub async fn rotate_cancel(State(state): State<Arc<AppState>>) -> Result<Json<SignalResponse>> {
    state
        .fluree
        .cancel_key_rotation()
        .map_err(ServerError::Api)?;
    Ok(Json(SignalResponse { ok: true }))
}

#[derive(Debug, Deserialize)]
pub struct VerifyRequest {
    pub retire_key_id: u32,
}

/// `POST /encryption/rotate/verify` — count blobs still on the retiring
/// key across the whole store and stamp the record. Only a zero count
/// licenses removing the key from configuration.
pub async fn rotate_verify(
    State(state): State<Arc<AppState>>,
    Json(req): Json<VerifyRequest>,
) -> Result<Json<KeyRotationProgress>> {
    let progress = state
        .fluree
        .verify_key_rotation(req.retire_key_id)
        .await
        .map_err(ServerError::Api)?;
    Ok(Json(progress))
}
