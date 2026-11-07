//! Reasoned-head administration: register a reasoning program for
//! persistence on a branch, list a branch's heads, retire one, and refresh
//! them now rather than wait for the background worker.
//!
//! Every route here publishes through the nameservice (or reads what it
//! publishes), so all are admin-gated and run where the ledger's writes do.

use crate::config::ServerRole;
use crate::error::{Result, ServerError};
use crate::state::AppState;
use axum::extract::{Request, State};
use axum::response::{IntoResponse, Response};
use axum::Json;
use fluree_db_api::{ReasonedHeads, RefreshOutcome};
use fluree_db_core::reasoned_head::HeadId;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::sync::Arc;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RegisterRequest {
    pub ledger: String,
    /// The reasoning keys a query would carry, plus an optional `graph`.
    pub program: JsonValue,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RegisterResponse {
    pub ledger: String,
    pub head_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HeadRequest {
    pub ledger: String,
    pub head_id: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RefreshResult {
    pub head_id: String,
    #[serde(flatten)]
    pub outcome: RefreshOutcome,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RefreshResponse {
    pub ledger: String,
    pub results: Vec<RefreshResult>,
}

async fn body<T: serde::de::DeserializeOwned>(request: Request) -> Result<T> {
    let bytes = axum::body::to_bytes(request.into_body(), 10 * 1024 * 1024)
        .await
        .map_err(|e| ServerError::bad_request(format!("Failed to read body: {e}")))?;
    serde_json::from_slice(&bytes)
        .map_err(|e| ServerError::bad_request(format!("Invalid JSON: {e}")))
}

fn head_id(raw: &str) -> Result<HeadId> {
    HeadId::parse(raw).map_err(ServerError::bad_request)
}

macro_rules! peer_forwards {
    ($state:expr, $request:expr) => {
        if $state.config.server_role == ServerRole::Peer {
            return super::ledger::forward_write_request(&$state, $request).await;
        }
    };
}

/// `POST /reasoned-heads/register`: `{"ledger", "program"}` → the head id.
pub async fn register(State(state): State<Arc<AppState>>, request: Request) -> Response {
    peer_forwards!(state, request);
    async move {
        let req: RegisterRequest = body(request).await?;
        let head = state
            .fluree
            .register_reasoned_head(&req.ledger, &req.program)
            .await
            .map_err(ServerError::Api)?;
        Ok::<_, ServerError>(Json(RegisterResponse {
            ledger: req.ledger,
            head_id: head.to_string(),
        }))
    }
    .await
    .into_response()
}

/// `POST /reasoned-heads/list`: `{"ledger"}` → the branch's heads document.
pub async fn list(State(state): State<Arc<AppState>>, request: Request) -> Response {
    peer_forwards!(state, request);
    async move {
        let req: HeadRequest = body(request).await?;
        let doc: ReasonedHeads = state
            .fluree
            .reasoned_heads(&req.ledger)
            .await
            .map_err(ServerError::Api)?;
        Ok::<_, ServerError>(Json(doc))
    }
    .await
    .into_response()
}

/// `POST /reasoned-heads/retire`: `{"ledger", "headId"}`.
pub async fn retire(State(state): State<Arc<AppState>>, request: Request) -> Response {
    peer_forwards!(state, request);
    async move {
        let req: HeadRequest = body(request).await?;
        let raw = req
            .head_id
            .as_deref()
            .ok_or_else(|| ServerError::bad_request("headId is required"))?;
        let head = head_id(raw)?;
        state
            .fluree
            .retire_reasoned_head(&req.ledger, &head)
            .await
            .map_err(ServerError::Api)?;
        Ok::<_, ServerError>(Json(RegisterResponse {
            ledger: req.ledger,
            head_id: head.to_string(),
        }))
    }
    .await
    .into_response()
}

/// `POST /reasoned-heads/refresh`: `{"ledger", "headId"?}` → what each
/// refresh did. Without `headId`, every active head of the branch.
pub async fn refresh(State(state): State<Arc<AppState>>, request: Request) -> Response {
    peer_forwards!(state, request);
    async move {
        let req: HeadRequest = body(request).await?;
        let results = match req.head_id.as_deref() {
            Some(raw) => {
                let head = head_id(raw)?;
                let outcome = state
                    .fluree
                    .refresh_reasoned_head(&req.ledger, &head)
                    .await
                    .map_err(ServerError::Api)?;
                vec![(head, outcome)]
            }
            None => state
                .fluree
                .refresh_reasoned_heads(&req.ledger)
                .await
                .map_err(ServerError::Api)?,
        };
        Ok::<_, ServerError>(Json(RefreshResponse {
            ledger: req.ledger,
            results: results
                .into_iter()
                .map(|(head, outcome)| RefreshResult {
                    head_id: head.to_string(),
                    outcome,
                })
                .collect(),
        }))
    }
    .await
    .into_response()
}
