//! Reasoning-index administration: register a reasoning program for
//! persistence on a branch, list a branch's indexes, retire one, and refresh
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
use fluree_db_api::{ReasoningIndexes, RefreshOutcome};
use fluree_db_core::reasoning_index::ReasoningIndexId;
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
    pub index_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexRequest {
    pub ledger: String,
    pub index_id: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RefreshResult {
    pub index_id: String,
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

fn index_id(raw: &str) -> Result<ReasoningIndexId> {
    ReasoningIndexId::parse(raw).map_err(ServerError::bad_request)
}

macro_rules! peer_forwards {
    ($state:expr, $request:expr) => {
        if $state.config.server_role == ServerRole::Peer {
            return super::ledger::forward_write_request(&$state, $request).await;
        }
    };
}

/// `POST /reasoning-indexes/register`: `{"ledger", "program"}` → the index id.
pub async fn register(State(state): State<Arc<AppState>>, request: Request) -> Response {
    peer_forwards!(state, request);
    async move {
        let req: RegisterRequest = body(request).await?;
        let index = state
            .fluree
            .register_reasoning_index(&req.ledger, &req.program)
            .await
            .map_err(ServerError::Api)?;
        Ok::<_, ServerError>(Json(RegisterResponse {
            ledger: req.ledger,
            index_id: index.to_string(),
        }))
    }
    .await
    .into_response()
}

/// `POST /reasoning-indexes/list`: `{"ledger"}` → the branch's reasoning-indexes document.
pub async fn list(State(state): State<Arc<AppState>>, request: Request) -> Response {
    peer_forwards!(state, request);
    async move {
        let req: IndexRequest = body(request).await?;
        let doc: ReasoningIndexes = state
            .fluree
            .reasoning_indexes(&req.ledger)
            .await
            .map_err(ServerError::Api)?;
        Ok::<_, ServerError>(Json(doc))
    }
    .await
    .into_response()
}

/// `POST /reasoning-indexes/retire`: `{"ledger", "indexId"}`.
pub async fn retire(State(state): State<Arc<AppState>>, request: Request) -> Response {
    peer_forwards!(state, request);
    async move {
        let req: IndexRequest = body(request).await?;
        let raw = req
            .index_id
            .as_deref()
            .ok_or_else(|| ServerError::bad_request("indexId is required"))?;
        let index = index_id(raw)?;
        state
            .fluree
            .retire_reasoning_index(&req.ledger, &index)
            .await
            .map_err(ServerError::Api)?;
        Ok::<_, ServerError>(Json(RegisterResponse {
            ledger: req.ledger,
            index_id: index.to_string(),
        }))
    }
    .await
    .into_response()
}

/// `POST /reasoning-indexes/refresh`: `{"ledger", "indexId"?}` → what each
/// refresh did. Without `indexId`, every active index of the branch.
pub async fn refresh(State(state): State<Arc<AppState>>, request: Request) -> Response {
    peer_forwards!(state, request);
    async move {
        let req: IndexRequest = body(request).await?;
        let results = match req.index_id.as_deref() {
            Some(raw) => {
                let index = index_id(raw)?;
                let outcome = state
                    .fluree
                    .refresh_reasoning_index(&req.ledger, &index)
                    .await
                    .map_err(ServerError::Api)?;
                vec![(index, outcome)]
            }
            None => state
                .fluree
                .refresh_reasoning_indexes(&req.ledger)
                .await
                .map_err(ServerError::Api)?,
        };
        Ok::<_, ServerError>(Json(RefreshResponse {
            ledger: req.ledger,
            results: results
                .into_iter()
                .map(|(index, outcome)| RefreshResult {
                    index_id: index.to_string(),
                    outcome,
                })
                .collect(),
        }))
    }
    .await
    .into_response()
}
