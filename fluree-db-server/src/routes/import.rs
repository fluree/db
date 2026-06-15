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
use crate::import_jobs::{ImportJob, ImportStatus, MultipartPlan};
use crate::state::AppState;
use axum::extract::{Path, Query, Request, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response};
use futures::TryStreamExt;
use serde::Deserialize;
use std::path::{Path as FsPath, PathBuf};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio_util::io::StreamReader;
use tracing::debug;

/// S3's hard ceiling on parts per multipart upload. The server raises the part
/// size when an archive would otherwise need more parts than this. This is the
/// real correctness constraint; the 5 MiB-per-part S3 minimum is left to the
/// configured `import_multipart_part_size_bytes` (default 256 MiB), which sits
/// comfortably above it.
const MAX_MULTIPART_PARTS: u64 = 10_000;

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
    /// Archive size hint (bytes). Drives the single-PUT vs multipart choice:
    /// an archive at or above the configured threshold is minted as multipart.
    #[serde(default)]
    size: Option<u64>,
}

#[derive(Deserialize)]
pub(crate) struct BlobQuery {
    token: String,
}

/// One completed part reported by the client on `complete` (multipart).
#[derive(Deserialize)]
struct CompletedPart {
    part_number: u32,
    /// Object-store ETag for the part. The reference backend doesn't re-verify
    /// it (restore re-hashes every frame anyway); a real backend passes it to
    /// `CompleteMultipartUpload`.
    #[serde(default)]
    #[allow(dead_code)]
    etag: Option<String>,
}

/// Optional body on `POST …/complete`. Empty for single-PUT uploads; carries
/// the part list for multipart uploads.
#[derive(Deserialize, Default)]
struct CompleteRequest {
    #[serde(default)]
    parts: Vec<CompletedPart>,
}

/// Path a multipart part is staged at: a sibling of the assembled archive,
/// e.g. `<staging>/imp_abc.flpack` → `<staging>/imp_abc.part.00001`.
fn part_staging_path(staged_path: &FsPath, part_number: u32) -> PathBuf {
    let stem = staged_path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("import");
    let parent = staged_path.parent().unwrap_or(FsPath::new("."));
    parent.join(format!("{stem}.part.{part_number:05}"))
}

/// Choose a part size and count for an archive of `size` bytes, starting from
/// the configured target and raising it so the count never exceeds S3's
/// 10,000-part ceiling. Returns `(part_size, num_parts)`, both ≥ 1.
fn plan_multipart(size: u64, configured_part_size: u64) -> (u64, u32) {
    let mut part_size = configured_part_size.max(1);
    if size.div_ceil(part_size) > MAX_MULTIPART_PARTS {
        // Raise to the smallest MiB-aligned size that fits under the ceiling.
        let needed = size.div_ceil(MAX_MULTIPART_PARTS);
        part_size = needed.div_ceil(1024 * 1024) * 1024 * 1024;
    }
    let num_parts = size.div_ceil(part_size).max(1) as u32;
    (part_size, num_parts)
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
        Err(ServerError::not_found(
            "Negotiated upload import is not enabled",
        ))
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

    let expires_at_unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() + 3600)
        .unwrap_or(0);

    // An archive at or above the multipart threshold is uploaded in parts: a
    // single S3 PUT caps at 5 GiB, so anything larger MUST go multipart.
    let multipart = req
        .size
        .filter(|&sz| sz >= state.config.import_multipart_threshold_bytes)
        .map(|sz| plan_multipart(sz, state.config.import_multipart_part_size_bytes));

    let response = if let Some((part_size, num_parts)) = multipart {
        // Reference backend: one upload URL per part, each pointing back at this
        // server's own part-sink endpoint. A production backend mints one
        // presigned `UploadPart` URL per part against its object store instead.
        let parts: Vec<serde_json::Value> = (1..=num_parts)
            .map(|part_number| {
                serde_json::json!({
                    "part_number": part_number,
                    "url": format!(
                        "/v1/fluree/import-upload/{import_id}/part/{part_number}?token={token}"
                    ),
                    "headers": { "Content-Type": "application/x-fluree-pack" },
                })
            })
            .collect();

        // The reference `upload_id` is just the import_id; a real backend
        // carries the object-store UploadId here.
        state.import_jobs.insert(
            import_id.clone(),
            ImportJob {
                ledger_id: req.ledger.clone(),
                token: token.clone(),
                staged_path,
                multipart: Some(MultipartPlan {
                    upload_id: import_id.clone(),
                    part_size,
                    num_parts,
                }),
                status: ImportStatus::AwaitingUpload,
                result: None,
                error: None,
                created_at: Instant::now(),
            },
        );

        serde_json::json!({
            "import_id": import_id,
            "ledger": req.ledger,
            "multipart": {
                "upload_id": import_id,
                "part_size_bytes": part_size,
                "parts": parts,
                "expires_at_unix": expires_at_unix,
            },
        })
    } else {
        state.import_jobs.insert(
            import_id.clone(),
            ImportJob {
                ledger_id: req.ledger.clone(),
                token: token.clone(),
                staged_path,
                multipart: None,
                status: ImportStatus::AwaitingUpload,
                result: None,
                error: None,
                created_at: Instant::now(),
            },
        );

        // Reference backend: the upload URL points back at this server's own
        // blob endpoint (relative — the client resolves it against the origin
        // it is already talking to). A production backend returns an absolute
        // presigned object-store URL here instead.
        serde_json::json!({
            "import_id": import_id,
            "ledger": req.ledger,
            "upload": {
                "method": "PUT",
                "url": format!("/v1/fluree/import-upload/{import_id}/blob?token={token}"),
                "headers": { "Content-Type": "application/x-fluree-pack" },
                "expires_at_unix": expires_at_unix,
            },
        })
    };
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

    let (expected_token, staged_path, status, multipart) = state
        .import_jobs
        .upload_target(&import_id)
        .ok_or_else(|| ServerError::not_found("unknown import_id"))?;

    // Constant-time-ish token check (length + equality). A mismatch is a 404 to
    // avoid confirming the import_id exists.
    if token != expected_token {
        return Err(ServerError::not_found("unknown import_id"));
    }
    if multipart.is_some() {
        return Err(ServerError::bad_request(
            "this import was minted for multipart upload; PUT parts to the part URLs instead",
        ));
    }
    if status != ImportStatus::AwaitingUpload {
        return Err(ServerError::bad_request(
            "upload slot is no longer awaiting an upload",
        ));
    }

    // Stream the body straight to the staged file — never buffer the archive.
    stream_body_to_file(request, &staged_path)
        .await
        .map_err(|e| ServerError::bad_request(format!("failed to stage upload: {e}")))?;

    Ok((
        StatusCode::OK,
        Json(serde_json::json!({ "status": "uploaded" })),
    )
        .into_response())
}

/// `PUT /v1/fluree/import-upload/:import_id/part/:part_number?token=…` — stage
/// one multipart part. Token-authorized via the URL (not admin auth), mirroring
/// a presigned `UploadPart`.
pub async fn put_part(
    State(state): State<Arc<AppState>>,
    Path((import_id, part_number)): Path<(String, u32)>,
    Query(q): Query<BlobQuery>,
    request: Request,
) -> Response {
    if state.config.server_role == ServerRole::Peer {
        return forward_to_transactor(&state, request).await;
    }
    put_part_local(state, import_id, part_number, q.token, request)
        .await
        .into_response()
}

async fn put_part_local(
    state: Arc<AppState>,
    import_id: String,
    part_number: u32,
    token: String,
    request: Request,
) -> Result<Response> {
    ensure_presign_enabled(&state)?;

    let (expected_token, staged_path, status, multipart) = state
        .import_jobs
        .upload_target(&import_id)
        .ok_or_else(|| ServerError::not_found("unknown import_id"))?;

    if token != expected_token {
        return Err(ServerError::not_found("unknown import_id"));
    }
    let Some(plan) = multipart else {
        return Err(ServerError::bad_request(
            "this import was not minted for multipart upload",
        ));
    };
    if part_number < 1 || part_number > plan.num_parts {
        return Err(ServerError::bad_request(format!(
            "part_number {part_number} out of range 1..={}",
            plan.num_parts
        )));
    }
    if status != ImportStatus::AwaitingUpload {
        return Err(ServerError::bad_request(
            "upload slot is no longer awaiting an upload",
        ));
    }

    // Stream the part straight to its sibling file, hashing as we go so the
    // response carries an ETag the client echoes back on `complete`.
    let part_path = part_staging_path(&staged_path, part_number);
    let etag = stream_body_to_file_hashed(request, &part_path)
        .await
        .map_err(|e| ServerError::bad_request(format!("failed to stage part: {e}")))?;

    Ok((
        StatusCode::OK,
        [(axum::http::header::ETAG, format!("\"{etag}\""))],
        Json(serde_json::json!({ "status": "uploaded", "part_number": part_number })),
    )
        .into_response())
}

/// Stream a request body to `path`, never buffering the whole body.
async fn stream_body_to_file(request: Request, path: &FsPath) -> std::io::Result<()> {
    let mut file = tokio::fs::File::create(path).await?;
    let body_stream = request
        .into_body()
        .into_data_stream()
        .map_err(std::io::Error::other);
    let mut reader = StreamReader::new(body_stream);
    tokio::io::copy(&mut reader, &mut file).await?;
    Ok(())
}

/// Like [`stream_body_to_file`] but returns the SHA-256 hex of the bytes
/// written — the reference backend's stand-in for an object-store part ETag.
async fn stream_body_to_file_hashed(request: Request, path: &FsPath) -> std::io::Result<String> {
    use sha2::{Digest, Sha256};
    use tokio::io::AsyncWriteExt as _;

    let mut file = tokio::fs::File::create(path).await?;
    let mut hasher = Sha256::new();
    let mut stream = request.into_body().into_data_stream();
    while let Some(chunk) = stream.try_next().await.map_err(std::io::Error::other)? {
        hasher.update(&chunk);
        file.write_all(&chunk).await?;
    }
    file.flush().await?;
    Ok(hex::encode(hasher.finalize()))
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
    complete_upload_local(state, import_id, request)
        .await
        .into_response()
}

async fn complete_upload_local(
    state: Arc<AppState>,
    import_id: String,
    request: Request,
) -> Result<Response> {
    ensure_presign_enabled(&state)?;

    let (ledger_id, staged_path, status, multipart) = state
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

    // Parse the (optional) part list. Empty body is fine for single-PUT.
    let body = axum::body::to_bytes(request.into_body(), 4 * 1024 * 1024)
        .await
        .map_err(|e| ServerError::bad_request(format!("failed to read body: {e}")))?;
    let complete_req: CompleteRequest = if body.is_empty() {
        CompleteRequest::default()
    } else {
        serde_json::from_slice(&body)
            .map_err(|e| ServerError::bad_request(format!("invalid complete request: {e}")))?
    };

    // For multipart, every part must be staged before we assemble. The blob
    // (single-PUT) case just checks the staged archive exists.
    if let Some(ref plan) = multipart {
        let reported: std::collections::BTreeSet<u32> =
            complete_req.parts.iter().map(|p| p.part_number).collect();
        let expected: std::collections::BTreeSet<u32> = (1..=plan.num_parts).collect();
        if reported != expected {
            return Err(ServerError::bad_request(format!(
                "complete must list exactly parts 1..={} (got {} distinct)",
                plan.num_parts,
                reported.len()
            )));
        }
        for part_number in 1..=plan.num_parts {
            let part_path = part_staging_path(&staged_path, part_number);
            if !tokio::fs::try_exists(&part_path).await.unwrap_or(false) {
                return Err(ServerError::bad_request(format!(
                    "part {part_number} was never uploaded"
                )));
            }
        }
    } else if !tokio::fs::try_exists(&staged_path).await.unwrap_or(false) {
        return Err(ServerError::bad_request(
            "no archive was uploaded for this import_id",
        ));
    }

    state
        .import_jobs
        .set_status(&import_id, ImportStatus::Running);

    // Assemble (multipart) + restore on a background task so a large restore is
    // not bounded by the request lifetime; the client polls to a terminal state.
    let bg_state = Arc::clone(&state);
    let bg_import_id = import_id.clone();
    let num_parts = multipart.as_ref().map(|p| p.num_parts);
    tokio::spawn(async move {
        let outcome = async {
            // Concatenate parts in order into the assembled archive. A real
            // object-store backend would instead call CompleteMultipartUpload
            // with the reported ETags; here the parts are local files.
            if let Some(num_parts) = num_parts {
                assemble_parts(&staged_path, num_parts)
                    .await
                    .map_err(|e| format!("failed to assemble multipart upload: {e}"))?;
            }
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
        // Best-effort cleanup of the staged archive (parts are removed by
        // `assemble_parts` as they are consumed).
        let _ = tokio::fs::remove_file(&staged_path).await;
    });

    let response = serde_json::json!({ "import_id": import_id, "status": "running" });
    Ok((StatusCode::ACCEPTED, Json(response)).into_response())
}

/// Concatenate staged parts `1..=num_parts` into `staged_path` (in order), then
/// remove each part file. Streams part-by-part — never holds a whole part, let
/// alone the whole archive, in memory.
async fn assemble_parts(staged_path: &FsPath, num_parts: u32) -> std::io::Result<()> {
    let mut out = tokio::fs::File::create(staged_path).await?;
    for part_number in 1..=num_parts {
        let part_path = part_staging_path(staged_path, part_number);
        let mut part = tokio::fs::File::open(&part_path).await?;
        tokio::io::copy(&mut part, &mut out).await?;
        drop(part);
        let _ = tokio::fs::remove_file(&part_path).await;
    }
    use tokio::io::AsyncWriteExt as _;
    out.flush().await?;
    Ok(())
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
