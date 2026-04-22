//! Stub endpoints for features not yet implemented

use crate::error::{Result, ServerError};
use axum::extract::Path;
use axum::Json;
use serde_json::Value as JsonValue;

/// WebSocket subscription endpoint (stub)
///
/// GET /fluree/subscribe
///
/// Not yet implemented - returns 501.
pub async fn subscribe() -> Result<Json<JsonValue>> {
    Err(ServerError::not_implemented(
        "WebSocket subscriptions not yet implemented. Use polling with /fluree/query instead.",
    ))
}

/// Remote resource fetch endpoint (stub)
///
/// GET/POST /fluree/remote/:path
///
/// Not yet implemented - returns 501.
pub async fn remote(Path(path): Path<String>) -> Result<Json<JsonValue>> {
    Err(ServerError::not_implemented(format!(
        "Remote resource fetch not yet implemented: {path}"
    )))
}
