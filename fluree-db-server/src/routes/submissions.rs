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
    BodyKind, CommittedSubmission, IdempotencyKey, MergeReceipt, OperationReceipt, PushReceipt,
    RebaseReceipt, RevertReceipt, SubmissionState, TransactionReceipt,
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
    Committed(Box<CommittedSubmissionResponse>),
    Failed { error: String },
}

/// Payload of [`SubmissionStateResponse::Committed`]. Lifted into a
/// struct so the enclosing enum's variant footprint stays a single
/// pointer wide — the wire shape (flat fields plus optional
/// `detail` / `status` blocks) is preserved via the enclosing enum's
/// `#[serde]` tag attribute and this struct's flat layout.
///
/// `detail` and `status` carry the same payload; `status` is a
/// back-compat alias for v1 clients that read the previous
/// `{ "state": "committed", "status": {...} }` shape, before the
/// canonical kit (`commit_id`, `t`, `kind`, `idempotency_key`) was
/// hoisted to top-level fields. Both are omitted when the typed
/// receipt is not recoverable (leader transition / restart / moka
/// eviction). Plan to drop `status` in the next major version.
#[derive(Serialize)]
pub struct CommittedSubmissionResponse {
    pub idempotency_key: Option<String>,
    /// Op kind — `"transact"`, `"push"`, `"revert"`, `"merge"`,
    /// or `"rebase"`. The seven transact body shapes (JSON-LD
    /// insert/upsert/update, Turtle insert/upsert, TriG upsert,
    /// SPARQL) collapse to `"transact"` so clients don't have to
    /// branch on body format.
    pub kind: &'static str,
    pub commit_id: String,
    pub t: i64,
    /// Full per-op detail when the originating node still has the
    /// typed receipt cached. `null` after leader transition /
    /// restart / cache eviction.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<OperationDetailResponse>,
    /// **Deprecated** — back-compat alias of [`Self::detail`] for v1
    /// clients written against the prior `{ status: {...} }` shape.
    /// Carries the same payload. Will be removed in the next major
    /// version; migrate readers to `detail`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<OperationDetailResponse>,
}

/// Per-op detail block, present on `Committed` when the typed
/// receipt was recoverable from the in-process cache. Discriminated
/// on `operation` matching the kit-level `kind`, but carrying the
/// richer field set.
#[derive(Clone, Serialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum OperationDetailResponse {
    Transaction(TransactionStatusResponse),
    Revert(RevertStatusResponse),
    Merge(MergeStatusResponse),
    Rebase(RebaseStatusResponse),
    Push(PushStatusResponse),
}

#[derive(Clone, Serialize)]
pub struct TransactionStatusResponse {
    pub idempotency_key: Option<String>,
    pub commit_id: String,
    pub t: i64,
    pub flake_count: usize,
}

#[derive(Clone, Serialize)]
pub struct RevertStatusResponse {
    pub idempotency_key: Option<String>,
    pub branch: String,
    pub reverted_commits: Vec<String>,
    pub conflict_count: usize,
    pub strategy: String,
    pub new_head_t: i64,
    pub new_head_id: String,
}

#[derive(Clone, Serialize)]
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

#[derive(Clone, Serialize)]
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

#[derive(Clone, Serialize)]
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
            SubmissionState::Committed(committed) => {
                let CommittedSubmission {
                    idempotency_key,
                    kind,
                    commit_id,
                    t,
                    tally: _,
                    receipt,
                } = *committed;
                // Emit `detail` and the back-compat `status` alias
                // with identical payloads — both omitted when the
                // typed receipt was not recoverable. The double-
                // serialize cost is one `OperationDetailResponse`
                // clone per cache hit; trivial vs the round-trip
                // it serves.
                let detail = receipt.map(|r| OperationDetailResponse::from(*r));
                Self::Committed(Box::new(CommittedSubmissionResponse {
                    idempotency_key: idempotency_key.map(|k| k.as_str().to_string()),
                    kind: body_kind_tag(kind),
                    commit_id: commit_id.to_string(),
                    t,
                    status: detail.clone(),
                    detail,
                }))
            }
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

#[cfg(test)]
mod tests {
    //! Wire-format pinning. The shape evolved past the original v1
    //! contract (`{ state: "committed", status: {...} }`), so the
    //! response now emits the new flat fields **and** the legacy
    //! `status` alias side-by-side. These tests pin both shapes so a
    //! refactor of the response struct can't silently re-break v1
    //! clients.
    use super::*;
    use fluree_db_api::{CommitId, CommitReceipt, ContentKind};
    use fluree_db_consensus::CommittedSubmission;

    fn fake_commit_id() -> CommitId {
        CommitId::new(ContentKind::Commit, &[7u8; 16])
    }

    fn cached_transact_state() -> SubmissionState {
        let commit_id = fake_commit_id();
        SubmissionState::Committed(Box::new(CommittedSubmission {
            idempotency_key: Some(IdempotencyKey::new("client-key-42").expect("fits cap")),
            kind: BodyKind::JsonLdInsert,
            commit_id: commit_id.clone(),
            t: 42,
            tally: None,
            receipt: Some(Box::new(OperationReceipt::Transaction(TransactionReceipt {
                idempotency_key: Some(IdempotencyKey::new("client-key-42").expect("fits cap")),
                commit: CommitReceipt {
                    commit_id,
                    t: 42,
                    flake_count: 3,
                },
                tally: None,
            }))),
        }))
    }

    #[test]
    fn cached_committed_emits_both_new_flat_fields_and_legacy_status() {
        let response = SubmissionStateResponse::from(cached_transact_state());
        let json = serde_json::to_value(&response).expect("serialize");

        // New v2 surface: top-level canonical kit.
        assert_eq!(json["state"], "committed");
        assert_eq!(json["idempotency_key"], "client-key-42");
        assert_eq!(json["kind"], "transact");
        assert_eq!(json["t"], 42);
        assert!(json["commit_id"].is_string());

        // New v2 surface: `detail` block.
        assert_eq!(json["detail"]["operation"], "transaction");
        assert_eq!(json["detail"]["flake_count"], 3);

        // v1 back-compat: `status` alias carries the same payload.
        assert_eq!(json["status"]["operation"], "transaction");
        assert_eq!(json["status"]["flake_count"], 3);
        assert_eq!(json["status"], json["detail"]);
    }

    #[test]
    fn degraded_committed_omits_both_status_and_detail() {
        // Post-leader-transition / restart / TTL eviction: the typed
        // receipt is gone but the canonical kit remains. Both
        // `detail` and `status` must be absent (not `null`) so v1
        // clients reading `response.status.operation` get a clean
        // "key missing" rather than a null-deref.
        let state = SubmissionState::Committed(Box::new(CommittedSubmission {
            idempotency_key: Some(IdempotencyKey::new("client-key-42").expect("fits cap")),
            kind: BodyKind::JsonLdInsert,
            commit_id: fake_commit_id(),
            t: 42,
            tally: None,
            receipt: None,
        }));
        let response = SubmissionStateResponse::from(state);
        let json = serde_json::to_value(&response).expect("serialize");

        assert_eq!(json["state"], "committed");
        assert_eq!(json["t"], 42);
        assert!(json.get("detail").is_none(), "detail must be omitted");
        assert!(json.get("status").is_none(), "status must be omitted");
    }

    #[test]
    fn unknown_and_in_flight_have_no_extra_fields() {
        let unknown = serde_json::to_value(SubmissionStateResponse::from(SubmissionState::Unknown))
            .expect("serialize");
        assert_eq!(unknown, serde_json::json!({ "state": "unknown" }));

        let in_flight = serde_json::to_value(SubmissionStateResponse::from(
            SubmissionState::InFlight,
        ))
        .expect("serialize");
        assert_eq!(in_flight, serde_json::json!({ "state": "in_flight" }));
    }
}
