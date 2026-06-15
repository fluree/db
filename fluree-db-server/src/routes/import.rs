//! Ledger restore endpoint: `POST /v1/fluree/import/*ledger`.
//!
//! Accepts a `.flpack` archive request body and creates a **new** ledger under
//! the path name by streaming the archive frames into storage — commits, txn
//! blobs, and (when present) prebuilt index artifacts — then finalizing the
//! commit/index heads from the embedded nameservice manifest. This is the
//! wholesale-restore counterpart of the `POST /pack/*ledger` export endpoint:
//! the archive is trusted byte-for-byte (every frame is SHA-256 verified) and
//! not replayed, so the restored ledger is immediately queryable.
//!
//! The new name is independent of whatever the source ledger was called, so
//! the same archive can be restored under a different name.
//!
//! Auth: lives in the admin-protected route group (same bracket as
//! create/drop). The body carries prebuilt index artifacts the server did not
//! produce, so this is an admin-grade operation.

use crate::config::ServerRole;
use crate::error::{Result, ServerError};
use crate::import_jobs::{ImportJob, ImportStatus};
use crate::state::AppState;
use axum::extract::{Path, Query, Request, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response};
use futures::TryStreamExt;
use serde::Deserialize;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio_util::io::StreamReader;
use tracing::debug;

/// Import endpoint (ledger in path tail).
///
/// `POST /v1/fluree/import/<ledger...>`
pub async fn import_ledger_tail(
    State(state): State<Arc<AppState>>,
    Path(ledger): Path<String>,
    request: Request,
) -> Response {
    // In peer mode, forward to the transactor — restore is a write.
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

    import_ledger_local(state, ledger, request)
        .await
        .into_response()
}

async fn import_ledger_local(
    state: Arc<AppState>,
    ledger: String,
    request: Request,
) -> Result<Response> {
    debug!(ledger = %ledger, "import: starting .flpack restore");

    // Adapt the request body into an `AsyncRead` so `restore_ledger` decodes
    // the archive frame-by-frame instead of buffering it whole — production
    // archives can be many gigabytes.
    let body_stream = request
        .into_body()
        .into_data_stream()
        .map_err(std::io::Error::other);
    let mut reader = StreamReader::new(body_stream);

    // `restore_ledger` creates the ledger itself and errors if the name is
    // already taken (mapped to 409 via the ApiError → response conversion,
    // same as `POST /create`). On any mid-stream failure it rolls back the
    // half-created ledger before returning.
    let result = state
        .fluree
        .restore_ledger(&ledger, &mut reader)
        .await
        .map_err(ServerError::Api)?;

    debug!(
        ledger = %result.ledger_id,
        commits = result.commits,
        txn_blobs = result.txn_blobs,
        index_artifacts = result.index_artifacts,
        "import: restore complete"
    );

    Ok((StatusCode::CREATED, Json(result)).into_response())
}

// ============================================================================
// Negotiated upload flow (reference implementation)
//
// For clients that cannot send a large body to `POST /import` (e.g. behind a
// payload-capped gateway): mint an upload slot, the client PUTs the archive
// out-of-band, then notifies `complete`, and the server restores from the
// staged archive asynchronously. The client polls status to a terminal state.
//
// This reference backend stages uploads to the local filesystem and points the
// upload URL back at the server's own blob endpoint. A production server would
// instead mint a presigned PUT to real object storage; the CLI handshake is
// identical either way.
// ============================================================================

#[derive(Deserialize)]
struct MintRequest {
    /// Target ledger name (path is fixed; the ledger rides in the body so it
    /// doesn't collide with the greedy `*ledger` tail on `POST /import`).
    ledger: String,
    /// Optional archive size hint (bytes).
    #[serde(default)]
    #[allow(dead_code)]
    size: Option<u64>,
}

#[derive(Deserialize)]
pub(crate) struct BlobQuery {
    token: String,
}

/// Forward a request to the transactor. Callers check `server_role == Peer`
/// before calling, so this only runs in peer mode.
async fn forward_to_transactor(state: &Arc<AppState>, request: Request) -> Response {
    match state.forwarding_client.as_ref() {
        Some(c) => match c.forward(request).await {
            Ok(resp) => resp,
            Err(e) => e.into_response(),
        },
        None => ServerError::internal("Forwarding client not configured").into_response(),
    }
}

/// Reject when the negotiated upload flow is not enabled on this server.
fn ensure_presign_enabled(state: &AppState) -> Result<()> {
    if state.config.import_presign_enabled {
        Ok(())
    } else {
        Err(ServerError::not_found("Negotiated upload import is not enabled"))
    }
}

/// `POST /v1/fluree/import-upload` — mint an upload slot for a `.flpack`.
///
/// Body: `{ "ledger": "<name>", "size"?: <bytes> }`. Returns an `import_id` and
/// an `upload` descriptor the client PUTs the archive to. Admin-protected.
pub async fn mint_upload(State(state): State<Arc<AppState>>, request: Request) -> Response {
    if state.config.server_role == ServerRole::Peer {
        return forward_to_transactor(&state, request).await;
    }
    mint_upload_local(state, request).await.into_response()
}

async fn mint_upload_local(state: Arc<AppState>, request: Request) -> Result<Response> {
    ensure_presign_enabled(&state)?;

    let body = axum::body::to_bytes(request.into_body(), 64 * 1024)
        .await
        .map_err(|e| ServerError::bad_request(format!("failed to read body: {e}")))?;
    let req: MintRequest = serde_json::from_slice(&body)
        .map_err(|e| ServerError::bad_request(format!("invalid mint request: {e}")))?;

    // Opaque, unguessable identifiers. `import_id` keys the job; `token` is the
    // capability embedded in the upload URL (models a presigned signature).
    let import_id = format!("imp_{:016x}", rand::random::<u64>());
    let token = format!("{:032x}", rand::random::<u128>());

    // Stage under the configured dir (or the system temp dir).
    let staging_dir = state
        .config
        .import_staging_dir
        .clone()
        .unwrap_or_else(std::env::temp_dir)
        .join("fluree-import-staging");
    tokio::fs::create_dir_all(&staging_dir)
        .await
        .map_err(|e| ServerError::internal(format!("failed to create staging dir: {e}")))?;
    let staged_path = staging_dir.join(format!("{import_id}.flpack"));

    state.import_jobs.insert(
        import_id.clone(),
        ImportJob {
            ledger_id: req.ledger.clone(),
            token: token.clone(),
            staged_path,
            status: ImportStatus::AwaitingUpload,
            result: None,
            error: None,
            created_at: Instant::now(),
        },
    );

    // Reference backend: the upload URL points back at this server's own blob
    // endpoint (relative — the client resolves it against the origin it is
    // already talking to). A production backend returns an absolute presigned
    // object-store URL here instead.
    let expires_at_unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() + 3600)
        .unwrap_or(0);

    let response = serde_json::json!({
        "import_id": import_id,
        "ledger": req.ledger,
        "upload": {
            "method": "PUT",
            "url": format!("/v1/fluree/import-upload/{import_id}/blob?token={token}"),
            "headers": { "Content-Type": "application/x-fluree-pack" },
            "expires_at_unix": expires_at_unix,
        },
    });
    Ok((StatusCode::OK, Json(response)).into_response())
}

/// `PUT /v1/fluree/import-upload/:import_id/blob?token=…` — stage the archive.
///
/// Authorized by the capability `token` in the query string (not admin auth) —
/// the URL is the capability, mirroring a presigned object-store PUT.
pub async fn put_blob(
    State(state): State<Arc<AppState>>,
    Path(import_id): Path<String>,
    Query(q): Query<BlobQuery>,
    request: Request,
) -> Response {
    if state.config.server_role == ServerRole::Peer {
        return forward_to_transactor(&state, request).await;
    }
    put_blob_local(state, import_id, q.token, request)
        .await
        .into_response()
}

async fn put_blob_local(
    state: Arc<AppState>,
    import_id: String,
    token: String,
    request: Request,
) -> Result<Response> {
    ensure_presign_enabled(&state)?;

    let (expected_token, staged_path, status) = state
        .import_jobs
        .upload_target(&import_id)
        .ok_or_else(|| ServerError::not_found("unknown import_id"))?;

    // Constant-time-ish token check (length + equality). A mismatch is a 404 to
    // avoid confirming the import_id exists.
    if token != expected_token {
        return Err(ServerError::not_found("unknown import_id"));
    }
    if status != ImportStatus::AwaitingUpload {
        return Err(ServerError::bad_request(
            "upload slot is no longer awaiting an upload",
        ));
    }

    // Stream the body straight to the staged file — never buffer the archive.
    let mut file = tokio::fs::File::create(&staged_path)
        .await
        .map_err(|e| ServerError::internal(format!("failed to open staging file: {e}")))?;
    let body_stream = request
        .into_body()
        .into_data_stream()
        .map_err(std::io::Error::other);
    let mut reader = StreamReader::new(body_stream);
    tokio::io::copy(&mut reader, &mut file)
        .await
        .map_err(|e| ServerError::bad_request(format!("failed to stage upload: {e}")))?;

    Ok((StatusCode::OK, Json(serde_json::json!({ "status": "uploaded" }))).into_response())
}

/// `POST /v1/fluree/import-upload/:import_id/complete` — begin the restore.
///
/// Transitions the job to `running` and restores from the staged archive on a
/// background task; the client polls the status endpoint. Admin-protected.
pub async fn complete_upload(
    State(state): State<Arc<AppState>>,
    Path(import_id): Path<String>,
    request: Request,
) -> Response {
    if state.config.server_role == ServerRole::Peer {
        return forward_to_transactor(&state, request).await;
    }
    complete_upload_local(state, import_id).await.into_response()
}

async fn complete_upload_local(state: Arc<AppState>, import_id: String) -> Result<Response> {
    ensure_presign_enabled(&state)?;

    let (ledger_id, staged_path, status) = state
        .import_jobs
        .completion_target(&import_id)
        .ok_or_else(|| ServerError::not_found("unknown import_id"))?;

    match status {
        ImportStatus::AwaitingUpload => {}
        ImportStatus::Running => {
            return Err(ServerError::bad_request("import is already running"));
        }
        ImportStatus::Succeeded | ImportStatus::Failed => {
            return Err(ServerError::bad_request("import has already completed"));
        }
    }
    if !tokio::fs::try_exists(&staged_path).await.unwrap_or(false) {
        return Err(ServerError::bad_request(
            "no archive was uploaded for this import_id",
        ));
    }

    state.import_jobs.set_status(&import_id, ImportStatus::Running);

    // Restore on a background task so a large restore is not bounded by the
    // request lifetime; the client polls status to a terminal state.
    let bg_state = Arc::clone(&state);
    let bg_import_id = import_id.clone();
    tokio::spawn(async move {
        let outcome = async {
            let mut file = tokio::fs::File::open(&staged_path)
                .await
                .map_err(|e| format!("failed to open staged archive: {e}"))?;
            bg_state
                .fluree
                .restore_ledger(&ledger_id, &mut file)
                .await
                .map_err(|e| e.to_string())
        }
        .await;

        match outcome {
            Ok(result) => {
                let json = serde_json::to_value(&result).unwrap_or_else(|_| serde_json::json!({}));
                bg_state.import_jobs.set_succeeded(&bg_import_id, json);
            }
            Err(msg) => {
                bg_state.import_jobs.set_failed(&bg_import_id, msg);
            }
        }
        // Best-effort cleanup of the staged archive.
        let _ = tokio::fs::remove_file(&staged_path).await;
    });

    let response = serde_json::json!({ "import_id": import_id, "status": "running" });
    Ok((StatusCode::ACCEPTED, Json(response)).into_response())
}

/// `GET /v1/fluree/import-upload/:import_id` — poll import status.
///
/// Returns `{ status, result?, error? }`. Admin-protected.
pub async fn import_status(
    State(state): State<Arc<AppState>>,
    Path(import_id): Path<String>,
    request: Request,
) -> Response {
    if state.config.server_role == ServerRole::Peer {
        return forward_to_transactor(&state, request).await;
    }
    import_status_local(state, import_id).await.into_response()
}

async fn import_status_local(state: Arc<AppState>, import_id: String) -> Result<Response> {
    ensure_presign_enabled(&state)?;

    let (status, result, error) = state
        .import_jobs
        .status_snapshot(&import_id)
        .ok_or_else(|| ServerError::not_found("unknown import_id"))?;

    let mut body = serde_json::json!({
        "import_id": import_id,
        "status": status.as_str(),
    });
    if let Some(result) = result {
        body["result"] = result;
    }
    if let Some(error) = error {
        body["error"] = serde_json::Value::String(error);
    }
    Ok((StatusCode::OK, Json(body)).into_response())
}
