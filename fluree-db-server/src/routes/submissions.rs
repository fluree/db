//! Status lookup for previously-submitted transactions.
//!
//! Exposes [`SubmissionLookup::status`] over HTTP. Clients that supplied an
//! `Idempotency-Key` when submitting a transaction can later query this
//! endpoint to discover the outcome of that submission — useful when the
//! original response was lost (timeout, disconnect, process restart).

use crate::config::DataAuthMode;
use crate::error::{Result, ServerError};
use crate::extract::MaybeDataBearer;
use crate::state::AppState;
use axum::{
    extract::{Path, State},
    response::{IntoResponse, Response},
    Json,
};
use fluree_db_consensus::{
    BodyKind, IdempotencyKey, MergeReceipt, OperationReceipt, PushReceipt, RebaseReceipt,
    RevertReceipt, SubmissionState, TransactionReceipt,
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
///
/// The `Committed` variant always carries the canonical kit
/// (`commit_id`, `t`, `kind`, `idempotency_key`) and an optional
/// `detail` block with the full per-op response. `detail` is `null`
/// when the in-process receipt cache no longer holds the typed
/// receipt — typically after a leader transition, a process
/// restart, or moka TTL eviction. The commit identity above is
/// already authoritative; clients that want full per-op fields can
/// chase them through the commit-log endpoint.
#[derive(Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum SubmissionStateResponse {
    Unknown,
    InFlight,
    Committed {
        idempotency_key: Option<String>,
        /// Op kind — `"transact"`, `"push"`, `"revert"`, `"merge"`,
        /// or `"rebase"`. The seven transact body shapes (JSON-LD
        /// insert/upsert/update, Turtle insert/upsert, TriG upsert,
        /// SPARQL) collapse to `"transact"` so clients don't have to
        /// branch on body format.
        kind: &'static str,
        commit_id: String,
        t: i64,
        /// Full per-op detail when the originating node still has
        /// the typed receipt cached. `null` after leader transition
        /// / restart / cache eviction.
        #[serde(skip_serializing_if = "Option::is_none")]
        detail: Option<OperationDetailResponse>,
    },
    Failed {
        error: String,
    },
}

/// Per-op detail block, present on `Committed` when the typed
/// receipt was recoverable from the in-process cache. Discriminated
/// on `operation` matching the kit-level `kind`, but carrying the
/// richer field set.
#[derive(Serialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum OperationDetailResponse {
    Transaction(TransactionStatusResponse),
    Revert(RevertStatusResponse),
    Merge(MergeStatusResponse),
    Rebase(RebaseStatusResponse),
    Push(PushStatusResponse),
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

#[derive(Serialize)]
pub struct MergeStatusResponse {
    pub idempotency_key: Option<String>,
    pub source: String,
    pub target: String,
    pub fast_forward: bool,
    pub new_head_t: i64,
    pub new_head_id: String,
    pub commits_copied: usize,
    pub conflict_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub strategy: Option<String>,
}

#[derive(Serialize)]
pub struct RebaseStatusResponse {
    pub idempotency_key: Option<String>,
    pub branch: String,
    pub fast_forward: bool,
    pub replayed: usize,
    pub skipped: usize,
    pub conflicts: usize,
    pub failures: usize,
    pub total_commits: usize,
    pub source_head_t: i64,
    pub source_head_id: String,
    pub strategy: String,
}

#[derive(Serialize)]
pub struct PushStatusResponse {
    pub idempotency_key: Option<String>,
    pub ledger: String,
    pub accepted: usize,
    pub head_t: i64,
    pub head_id: String,
}

pub async fn submission_status(
    State(state): State<Arc<AppState>>,
    bearer: MaybeDataBearer,
    Path(params): Path<SubmissionStatusParams>,
) -> Result<Response> {
    // Read-side data-auth gate mirroring `routes/ledger.rs::info`: the
    // response carries commit metadata (commit ids, `t`, reverted commits,
    // …) so it has to clear the same scope check the rest of the data API
    // honors. Without this, anyone holding or guessing an idempotency key
    // could probe submission outcomes on any ledger, and a cache-hit would
    // confirm both the ledger's existence and the operation's effect.
    let data_auth = state.config.data_auth();
    if data_auth.mode == DataAuthMode::Required && bearer.0.is_none() {
        return Err(ServerError::unauthorized("Bearer token required"));
    }
    if let Some(principal) = bearer.0.as_ref() {
        if !principal.can_read(&params.ledger) {
            // Match the existence-leak avoidance in `info`: out-of-scope and
            // missing-ledger return the same 404 so a caller can't use the
            // response to distinguish them.
            return Err(ServerError::not_found("Ledger not found"));
        }
    }

    // Validate the URL-borne key through the same constructor that gates
    // header-borne keys, so an over-long path segment is rejected at the
    // boundary before it can be hashed into a cache lookup.
    let key = IdempotencyKey::new(params.key)
        .map_err(|e| ServerError::BadRequest(format!("invalid idempotency key: {e}")))?;
    let lookup_state = state.committer.status(&params.ledger, &key).await;
    Ok(Json(SubmissionStateResponse::from(lookup_state)).into_response())
}

impl From<SubmissionState> for SubmissionStateResponse {
    fn from(state: SubmissionState) -> Self {
        match state {
            SubmissionState::Unknown => Self::Unknown,
            SubmissionState::InFlight => Self::InFlight,
            SubmissionState::Committed {
                idempotency_key,
                kind,
                commit_id,
                t,
                tally: _,
                receipt,
            } => Self::Committed {
                idempotency_key: idempotency_key.map(|k| k.as_str().to_string()),
                kind: body_kind_tag(kind),
                commit_id: commit_id.to_string(),
                t,
                detail: receipt.map(OperationDetailResponse::from),
            },
            SubmissionState::Failed(err) => Self::Failed {
                error: err.to_string(),
            },
        }
    }
}

impl From<OperationReceipt> for OperationDetailResponse {
    fn from(receipt: OperationReceipt) -> Self {
        match receipt {
            OperationReceipt::Transaction(r) => Self::Transaction(r.into()),
            OperationReceipt::Revert(r) => Self::Revert(r.into()),
            OperationReceipt::Merge(r) => Self::Merge(r.into()),
            OperationReceipt::Rebase(r) => Self::Rebase(r.into()),
            OperationReceipt::Push(r) => Self::Push(r.into()),
        }
    }
}

fn body_kind_tag(kind: BodyKind) -> &'static str {
    match kind {
        // The seven transact body shapes share one public tag —
        // clients branch on `kind` without having to know JSON-LD
        // vs Turtle vs SPARQL.
        BodyKind::JsonLdInsert
        | BodyKind::JsonLdUpsert
        | BodyKind::JsonLdUpdate
        | BodyKind::TurtleInsert
        | BodyKind::TurtleUpsert
        | BodyKind::TrigUpsert
        | BodyKind::Sparql => "transact",
        BodyKind::Pushed => "push",
        BodyKind::Revert => "revert",
        BodyKind::Merge => "merge",
        BodyKind::Rebase => "rebase",
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

impl From<MergeReceipt> for MergeStatusResponse {
    fn from(receipt: MergeReceipt) -> Self {
        // Fast-forward merges don't apply a conflict strategy — omit the
        // field to match the immediate `POST /merge` response shape.
        let strategy = (!receipt.fast_forward).then(|| receipt.strategy.as_str().to_string());
        Self {
            idempotency_key: receipt.idempotency_key.map(|k| k.as_str().to_string()),
            source: receipt.source,
            target: receipt.target,
            fast_forward: receipt.fast_forward,
            new_head_t: receipt.new_head_t,
            new_head_id: receipt.new_head_id.to_string(),
            commits_copied: receipt.commits_copied,
            conflict_count: receipt.conflict_count,
            strategy,
        }
    }
}

impl From<RebaseReceipt> for RebaseStatusResponse {
    fn from(receipt: RebaseReceipt) -> Self {
        Self {
            idempotency_key: receipt.idempotency_key.map(|k| k.as_str().to_string()),
            branch: receipt.branch,
            fast_forward: receipt.fast_forward,
            replayed: receipt.replayed,
            skipped: receipt.skipped,
            conflicts: receipt.conflicts,
            failures: receipt.failures,
            total_commits: receipt.total_commits,
            source_head_t: receipt.source_head_t,
            source_head_id: receipt.source_head_id.to_string(),
            strategy: receipt.strategy.as_str().to_string(),
        }
    }
}

impl From<PushReceipt> for PushStatusResponse {
    fn from(receipt: PushReceipt) -> Self {
        Self {
            idempotency_key: receipt.idempotency_key.map(|k| k.as_str().to_string()),
            ledger: receipt.ledger,
            accepted: receipt.accepted,
            head_t: receipt.head_t,
            head_id: receipt.head_id.to_string(),
        }
    }
}
