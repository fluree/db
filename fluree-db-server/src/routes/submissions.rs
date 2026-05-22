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
use fluree_db_consensus::{IdempotencyKey, SubmissionLookup, SubmissionState, TransactionReceipt};
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
    Committed { receipt: TransactionReceiptResponse },
    Failed { error: String },
}

#[derive(Serialize)]
pub struct TransactionReceiptResponse {
    pub idempotency_key: Option<String>,
    pub commit_id: String,
    pub t: i64,
    pub flake_count: usize,
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
                receipt: receipt.into(),
            },
            SubmissionState::Failed(err) => Self::Failed {
                error: err.to_string(),
            },
        }
    }
}

impl From<TransactionReceipt> for TransactionReceiptResponse {
    fn from(receipt: TransactionReceipt) -> Self {
        Self {
            idempotency_key: receipt.idempotency_key.map(|k| k.as_str().to_string()),
            commit_id: receipt.commit.commit_id.to_string(),
            t: receipt.commit.t,
            flake_count: receipt.commit.flake_count,
        }
    }
}
