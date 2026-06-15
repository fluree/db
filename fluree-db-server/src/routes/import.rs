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
use crate::state::AppState;
use axum::extract::{Path, Request, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response};
use futures::TryStreamExt;
use std::sync::Arc;
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
