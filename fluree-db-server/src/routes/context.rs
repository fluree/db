//! Default context management endpoints: GET/PUT /v1/fluree/context/*ledger

use crate::config::ServerRole;
use crate::error::{Result, ServerError};
use crate::extract::{FlureeHeaders, MaybeDataBearer};
use crate::state::AppState;
use crate::telemetry::{create_request_span, extract_request_id, extract_trace_id};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use std::sync::Arc;
use tracing::Instrument;

/// GET /v1/fluree/context/*ledger
///
/// Returns the default JSON-LD context for a ledger.
/// Returns 200 with the context object, or 200 with `null` if no context is set.
pub async fn get_context(
    State(state): State<Arc<AppState>>,
    Path(ledger): Path<String>,
    headers: FlureeHeaders,
    bearer: MaybeDataBearer,
) -> Result<Response> {
    let request_id = extract_request_id(&headers.raw, &state.telemetry_config);
    let trace_id = extract_trace_id(&headers.raw);

    let span = create_request_span(
        "context:get",
        request_id.as_deref(),
        trace_id.as_deref(),
        Some(&ledger),
        None,
        None,
    );

    async move {
        // Enforce data auth (read operation)
        let data_auth = state.config.data_auth();
        if data_auth.mode == crate::config::DataAuthMode::Required && bearer.0.is_none() {
            return Err(ServerError::unauthorized("Bearer token required"));
        }
        if let Some(p) = bearer.0.as_ref() {
            if !p.can_read(&ledger) {
                return Err(ServerError::not_found("Ledger not found"));
            }
        }

        let ctx = state
            .fluree
            .get_default_context(&ledger)
            .await
            .map_err(ServerError::Api)?;

        Ok(Json(serde_json::json!({ "@context": ctx })).into_response())
    }
    .instrument(span)
    .await
}

/// PUT /v1/fluree/context/*ledger
///
/// Replace the default JSON-LD context for a ledger.
///
/// Request body: JSON object representing the context (prefix → IRI mappings).
/// Returns 200 on success, 405 in peer mode, 409 on CAS conflict after retries,
/// 404 if ledger not found.
pub async fn set_context(
    State(state): State<Arc<AppState>>,
    Path(ledger): Path<String>,
    headers: FlureeHeaders,
    bearer: MaybeDataBearer,
    Json(body): Json<serde_json::Value>,
) -> Result<Response> {
    // In peer mode, reject writes — context updates go through the tx server
    if state.config.server_role == ServerRole::Peer {
        return Ok((
            StatusCode::METHOD_NOT_ALLOWED,
            Json(serde_json::json!({
                "error": "context updates are not available in peer mode"
            })),
        )
            .into_response());
    }

    let request_id = extract_request_id(&headers.raw, &state.telemetry_config);
    let trace_id = extract_trace_id(&headers.raw);

    let span = create_request_span(
        "context:set",
        request_id.as_deref(),
        trace_id.as_deref(),
        Some(&ledger),
        None,
        None,
    );

    async move {
        // Enforce data auth (write operation)
        let data_auth = state.config.data_auth();
        if data_auth.mode == crate::config::DataAuthMode::Required && bearer.0.is_none() {
            return Err(ServerError::unauthorized("Bearer token required"));
        }
        if let Some(p) = bearer.0.as_ref() {
            if !p.can_write(&ledger) {
                return Err(ServerError::not_found("Ledger not found"));
            }
        }

        // Extract the context value: accept either { "@context": {...} } or bare {...}
        let context = if let Some(ctx) = body.get("@context") {
            ctx.clone()
        } else {
            body
        };

        // Validate that the context is an object (prefix → IRI map)
        if !context.is_object() {
            return Err(ServerError::BadRequest(
                "context must be a JSON object mapping prefixes to IRIs".to_string(),
            ));
        }

        let f = &state.fluree;
        match f
            .set_default_context(&ledger, &context)
            .await
            .map_err(ServerError::Api)?
        {
            fluree_db_api::SetContextResult::Updated => Ok((
                StatusCode::OK,
                Json(serde_json::json!({ "status": "updated" })),
            )
                .into_response()),
            fluree_db_api::SetContextResult::Conflict => Ok((
                StatusCode::CONFLICT,
                Json(serde_json::json!({
                    "error": "Concurrent update conflict. Please retry."
                })),
            )
                .into_response()),
        }
    }
    .instrument(span)
    .await
}
