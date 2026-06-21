//! [`Committer`] implementation that drives the per-operation execution
//! pipeline against a local [`Fluree`] instance.
//!
//! Translates each typed request into the corresponding `Fluree::*`
//! call and lifts the result back into the umbrella receipt type.
//! Holds no admission, idempotency, or replication state.
//!
//! [`Committer`]: crate::Committer

use crate::{
    Committer, IdempotencyKey, MergeReceipt, MergeRequest, PushReceipt, PushRequest, RebaseReceipt,
    RebaseRequest, RevertReceipt, RevertRequest, RevertSelection, SubmissionError,
    SubmissionLookup, SubmissionState, TransactionBody, TransactionReceipt, TransactionRequest,
};
use async_trait::async_trait;
use fluree_db_api::{
    ApiError, Base64Bytes, Fluree, GovernanceOptions, LedgerHandle, LedgerManager, PolicyContext,
    PushCommitsRequest, RefreshOpts, TransactError,
};
use fluree_db_ledger::IndexConfig;
use std::sync::Arc;

/// Upper bound on stage + commit attempts when a retryable
/// inter-writer conflict surfaces. Mirrors the prior monolithic
/// retry budget; sized high enough that a contended ledger with
/// several concurrent writers still converges within one HTTP
/// timeout, low enough to bound the worst-case latency added by a
/// pathological writer.
const MAX_TXN_RETRIES: usize = 16;

/// Conflicts that heal by reconciling the cached writer state to
/// the durable nameservice head and re-staging against the fresh
/// state.
///
/// Unreachable for a single instance with no external writers — the
/// per-ledger write lock plus atomic cache replace after publish
/// keep `verify_sequencing` consistent, the head-record CAS
/// uncontended, and namespace allocations process-serialized. They
/// become reachable when something outside this committer's lock
/// writes to the same durable backend (multiple processes sharing a
/// nameservice + storage, out-of-band cache invalidation).
///
/// `tx_builder` deliberately excludes `NamespaceConflict` from its
/// internal retry — re-staging would consume the already-built
/// `stage_result`. The consensus layer (this loop) preserves the
/// request body across attempts, so it's the right altitude.
fn is_retryable_txn_conflict(e: &ApiError) -> bool {
    matches!(
        e,
        ApiError::Transact(
            TransactError::CommitConflict { .. }
                | TransactError::PublishLostRace { .. }
                | TransactError::NamespaceConflict(_)
        )
    )
}

/// Per-operation execution path against a local [`Fluree`] instance.
///
/// Translates each typed request into the corresponding `Fluree::*`
/// call and lifts the result back into the umbrella receipt type.
pub struct LocalCommitter {
    fluree: Arc<Fluree>,
    index_config: IndexConfig,
}

impl LocalCommitter {
    pub fn new(fluree: Arc<Fluree>, index_config: IndexConfig) -> Self {
        Self {
            fluree,
            index_config,
        }
    }

    fn ledger_manager(&self) -> Result<&Arc<LedgerManager>, SubmissionError> {
        self.fluree
            .ledger_manager()
            .ok_or_else(|| SubmissionError::Execution {
                status: 500,
                message: "LedgerManager is not configured on the Fluree instance".into(),
            })
    }
}

#[async_trait]
impl Committer for LocalCommitter {
    async fn transact(
        &self,
        request: TransactionRequest,
    ) -> Result<TransactionReceipt, SubmissionError> {
        let TransactionRequest {
            idempotency_key,
            ledger_id,
            body,
            txn_opts,
            commit_opts,
            tracking,
            governance,
        } = request;

        let ledger_handle = self
            .ledger_manager()?
            .get_or_load(&ledger_id)
            .await
            .map_err(execution_failure)?;

        // Bounded reconcile-and-retry around stage + commit. See
        // [`is_retryable_txn_conflict`] for the conflict variants that
        // heal via `refresh()` + re-stage. Each attempt rebuilds the
        // policy context (against the post-refresh snapshot) and the
        // builder from scratch — the body is borrowed, so the
        // per-iteration cost is one extra clone of `TxnOpts` /
        // `CommitOpts` / `TrackingOptions`.
        let mut last_error: Option<ApiError> = None;
        for attempt in 1..=MAX_TXN_RETRIES {
            let policy_ctx = build_policy_context(&ledger_handle, &governance).await?;

            // The builder API holds the ledger write lock and replaces the cached
            // state internally for the duration of stage + commit — no manual
            // lock/clone/replace dance is needed here. Each body variant fixes
            // both the parser path and the insert/upsert/update semantics.
            let staged = self.fluree.stage(&ledger_handle);
            let staged = match &body {
                TransactionBody::JsonLdInsert(json) => staged.insert(json),
                TransactionBody::JsonLdUpsert(json) => staged.upsert(json),
                TransactionBody::JsonLdUpdate(json) => staged.update(json),
                TransactionBody::TurtleInsert(text) => staged.insert_turtle(text.as_str()),
                TransactionBody::TurtleUpsert(text) | TransactionBody::TrigUpsert(text) => {
                    staged.upsert_turtle(text.as_str())
                }
                TransactionBody::Sparql(query) => staged.sparql_update(query.as_str()),
            };
            let mut builder = staged
                .txn_opts(txn_opts.clone())
                .commit_opts(commit_opts.clone())
                .index_config(self.index_config.clone());
            if let Some(tracking) = tracking.clone() {
                builder = builder.tracking(tracking);
            }
            if let Some(policy) = policy_ctx {
                builder = builder.policy(policy);
            }

            match builder.execute().await {
                Ok(result) => {
                    return Ok(TransactionReceipt {
                        idempotency_key,
                        commit: result.receipt,
                        tally: result.tally,
                    });
                }
                Err(e) if attempt < MAX_TXN_RETRIES && is_retryable_txn_conflict(&e) => {
                    tracing::warn!(
                        attempt,
                        max_attempts = MAX_TXN_RETRIES,
                        error = %e,
                        "transaction commit conflict; reconciling cached state and retrying"
                    );
                    if let Err(refresh_err) = self
                        .fluree
                        .refresh(&ledger_id, RefreshOpts::default())
                        .await
                    {
                        tracing::warn!(
                            attempt,
                            error = %refresh_err,
                            "refresh during commit-conflict retry failed; retrying anyway"
                        );
                    }
                    last_error = Some(e);
                    continue;
                }
                Err(e) => return Err(execution_failure(e)),
            }
        }

        Err(execution_failure(last_error.unwrap_or_else(|| {
            ApiError::internal("transaction failed after retries with no captured error")
        })))
    }

    async fn revert(&self, request: RevertRequest) -> Result<RevertReceipt, SubmissionError> {
        let RevertRequest {
            idempotency_key,
            ledger_name,
            branch,
            selection,
            strategy,
            ..
        } = request;

        let result = match selection {
            RevertSelection::Commits(commits) => {
                self.fluree
                    .revert_commits(&ledger_name, &branch, commits.into_vec(), strategy)
                    .await
            }
            RevertSelection::Range { from, to } => {
                self.fluree
                    .revert_range(&ledger_name, &branch, from, to, strategy)
                    .await
            }
        };

        let outcome = result.map_err(execution_failure)?;
        Ok(RevertReceipt {
            idempotency_key,
            branch,
            reverted_commits: outcome.reverted_commits,
            conflict_count: outcome.conflict_count,
            strategy,
            new_head_t: outcome.new_head_t,
            new_head_id: outcome.new_head_id,
        })
    }

    async fn merge(&self, request: MergeRequest) -> Result<MergeReceipt, SubmissionError> {
        let MergeRequest {
            idempotency_key,
            ledger_name,
            source_branch,
            target_branch,
            strategy,
            ..
        } = request;

        let report = self
            .fluree
            .merge_branch(
                &ledger_name,
                &source_branch,
                target_branch.as_deref(),
                strategy,
            )
            .await
            .map_err(execution_failure)?;

        Ok(MergeReceipt {
            idempotency_key,
            source: report.source,
            target: report.target,
            fast_forward: report.fast_forward,
            new_head_t: report.new_head_t,
            new_head_id: report.new_head_id,
            commits_copied: report.commits_copied,
            conflict_count: report.conflict_count,
            strategy,
        })
    }

    async fn rebase(&self, request: RebaseRequest) -> Result<RebaseReceipt, SubmissionError> {
        let RebaseRequest {
            idempotency_key,
            ledger_name,
            branch,
            strategy,
            ..
        } = request;

        let report = self
            .fluree
            .rebase_branch(&ledger_name, &branch, strategy)
            .await
            .map_err(execution_failure)?;

        Ok(RebaseReceipt {
            idempotency_key,
            branch,
            fast_forward: report.fast_forward,
            replayed: report.replayed,
            skipped: report.skipped,
            conflicts: report.conflicts.len(),
            failures: report.failures.len(),
            total_commits: report.total_commits,
            source_head_t: report.source_head_t,
            source_head_id: report.source_head_id,
            strategy,
        })
    }

    async fn push(&self, request: PushRequest) -> Result<PushReceipt, SubmissionError> {
        let PushRequest {
            idempotency_key,
            ledger_id,
            commits,
            blobs,
            governance,
        } = request;

        let payload = PushCommitsRequest {
            commits: commits.into_iter().map(Base64Bytes).collect(),
            blobs: blobs
                .into_iter()
                .map(|(k, v)| (k, Base64Bytes(v)))
                .collect(),
        };

        let response = self
            .fluree
            .push_commits(&ledger_id, payload, &governance, &self.index_config)
            .await
            .map_err(execution_failure)?;

        Ok(PushReceipt {
            idempotency_key,
            ledger: response.ledger,
            accepted: response.accepted,
            head_t: response.head.t,
            head_id: response.head.commit_id,
            indexing: response.indexing,
        })
    }
}

/// `LocalCommitter` doesn't maintain its own idempotency state — the
/// caching is wrapped around it by [`CachingCommitter`]. So the
/// status lookup here is always `Unknown`: the wrapping layer
/// consults its moka cache first, and only falls through here when
/// the cache misses. For the Raft path the inner committer is
/// [`QueuedTransactor`](crate::raft::queued_transactor::QueuedTransactor),
/// which surfaces a [`SubmissionState::Committed`] from replicated
/// idempotency state — see its `SubmissionLookup` impl.
#[async_trait]
impl SubmissionLookup for LocalCommitter {
    async fn status(&self, _ledger_id: &str, _key: &IdempotencyKey) -> SubmissionState {
        SubmissionState::Unknown
    }
}

/// Map a transaction-pipeline error into a [`SubmissionError`], preserving
/// the HTTP status so the caller can render an accurate response.
pub(crate) fn execution_failure(err: ApiError) -> SubmissionError {
    SubmissionError::Execution {
        status: err.status_code(),
        message: err.to_string(),
    }
}

/// Build a [`PolicyContext`] from the request's policy inputs.
///
/// Returns `Ok(None)` when there are no policy inputs — the transaction
/// runs under root. The context is built from a snapshot of the ledger
/// this node is about to stage against, so policy enforcement reflects
/// the same state the transaction commits onto. Building it here, rather
/// than having the caller pre-build and pass a context, keeps the policy
/// context bound to the executing node's state — the shape a replicated
/// implementation needs.
pub(crate) async fn build_policy_context(
    ledger_handle: &LedgerHandle,
    governance: &GovernanceOptions,
) -> Result<Option<PolicyContext>, SubmissionError> {
    if !governance.has_any_policy_inputs() {
        return Ok(None);
    }

    let snap = ledger_handle.snapshot().await;
    fluree_db_api::build_policy_context(
        &snap.snapshot,
        snap.novelty.as_ref(),
        Some(snap.novelty.as_ref()),
        snap.t,
        governance,
    )
    .await
    .map(Some)
    .map_err(execution_failure)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retryable_includes_the_three_inter_writer_conflicts() {
        assert!(is_retryable_txn_conflict(&ApiError::Transact(
            TransactError::CommitConflict {
                expected_t: 5,
                head_t: 6,
            },
        )));
        assert!(is_retryable_txn_conflict(&ApiError::Transact(
            TransactError::PublishLostRace {
                ledger_id: "db:main".into(),
                attempted_t: 5,
                attempted_commit_id: "a".into(),
                published_t: 5,
                published_commit_id: "b".into(),
            },
        )));
        assert!(is_retryable_txn_conflict(&ApiError::Transact(
            TransactError::NamespaceConflict("ex".into()),
        )));
    }

    #[test]
    fn retryable_excludes_non_inter_writer_conflicts() {
        // EmptyTransaction is a client-side error — retrying with the
        // same body produces the same result. Must NOT loop.
        assert!(!is_retryable_txn_conflict(&ApiError::Transact(
            TransactError::EmptyTransaction,
        )));
        // CommitIdMismatch indicates the head was rewritten under us in
        // a way refresh can't reconcile (snapshot taken at the wrong
        // moment); a retry would hit the same mismatch.
        assert!(!is_retryable_txn_conflict(&ApiError::Transact(
            TransactError::CommitIdMismatch {
                expected: "a".into(),
                found: "b".into(),
            },
        )));
    }
}
