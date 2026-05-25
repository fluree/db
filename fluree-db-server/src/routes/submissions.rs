//! Status lookup for previously-submitted transactions.
//!
//! Exposes [`SubmissionLookup::status`] over HTTP. Clients that supplied an
//! `Idempotency-Key` when submitting a transaction can later query this
//! endpoint to discover the outcome of that submission — useful when the
//! original response was lost (timeout, disconnect, process restart).

use crate::state::AppState;
use axum::{
    extract::{Path, State},
    Json,
};
use fluree_db_consensus::{
    IdempotencyKey, OperationReceipt, RevertReceipt, SubmissionLookup, SubmissionState,
    TransactionReceipt,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// URL params for the submission-status endpoint.
///
/// Route shape: `/submissions/:key/*ledger`. The ledger ID may itself contain
/// `/` (e.g., `mydb/main`) so it must come last as a greedy capture.
#[derive(Deserialize)]
pub struct SubmissionStatusParams {
    pub key: String,
    pub ledger: String,
}

/// JSON response shape for a submission status query.
#[derive(Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum SubmissionStateResponse {
    Unknown,
    InFlight,
    Committed { status: OperationStatusResponse },
    Failed { error: String },
}

/// Polymorphic status response — discriminated by the operation kind that
/// produced it, so callers can recover the per-op fields after the fact.
#[derive(Serialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum OperationStatusResponse {
    Transaction(TransactionStatusResponse),
    Revert(RevertStatusResponse),
}

#[derive(Serialize)]
pub struct TransactionStatusResponse {
    pub idempotency_key: Option<String>,
    pub commit_id: String,
    pub t: i64,
    pub flake_count: usize,
}

#[derive(Serialize)]
pub struct RevertStatusResponse {
    pub idempotency_key: Option<String>,
    pub branch: String,
    pub reverted_commits: Vec<String>,
    pub conflict_count: usize,
    pub strategy: String,
    pub new_head_t: i64,
    pub new_head_id: String,
}

pub async fn submission_status(
    State(state): State<Arc<AppState>>,
    Path(params): Path<SubmissionStatusParams>,
) -> Json<SubmissionStateResponse> {
    let key = IdempotencyKey::new(params.key);
    let lookup_state = state.consensus.status(&params.ledger, &key).await;
    Json(SubmissionStateResponse::from(lookup_state))
}

impl From<SubmissionState> for SubmissionStateResponse {
    fn from(state: SubmissionState) -> Self {
        match state {
            SubmissionState::Unknown => Self::Unknown,
            SubmissionState::InFlight => Self::InFlight,
            SubmissionState::Committed(receipt) => Self::Committed {
                status: receipt.into(),
            },
            SubmissionState::Failed(err) => Self::Failed {
                error: err.to_string(),
            },
        }
    }
}

impl From<OperationReceipt> for OperationStatusResponse {
    fn from(receipt: OperationReceipt) -> Self {
        match receipt {
            OperationReceipt::Transaction(r) => Self::Transaction(r.into()),
            OperationReceipt::Revert(r) => Self::Revert(r.into()),
        }
    }
}

impl From<TransactionReceipt> for TransactionStatusResponse {
    fn from(receipt: TransactionReceipt) -> Self {
        Self {
            idempotency_key: receipt.idempotency_key.map(|k| k.as_str().to_string()),
            commit_id: receipt.commit.commit_id.to_string(),
            t: receipt.commit.t,
            flake_count: receipt.commit.flake_count,
        }
    }
}

impl From<RevertReceipt> for RevertStatusResponse {
    fn from(receipt: RevertReceipt) -> Self {
        Self {
            idempotency_key: receipt.idempotency_key.map(|k| k.as_str().to_string()),
            branch: receipt.branch,
            reverted_commits: receipt
                .reverted_commits
                .into_iter()
                .map(|id| id.to_string())
                .collect(),
            conflict_count: receipt.conflict_count,
            strategy: receipt.strategy.as_str().to_string(),
            new_head_t: receipt.new_head_t,
            new_head_id: receipt.new_head_id.to_string(),
        }
    }
}
