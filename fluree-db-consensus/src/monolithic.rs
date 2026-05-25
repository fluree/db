//! Monolithic consensus implementation.
//!
//! A single integrated unit handles receiving, ordering, and executing every
//! transaction. No replication, no quorum — the local execution stream *is*
//! the agreement. Suitable for development, testing, and deployments that
//! do not need cross-node coordination.

use crate::{
    IdempotencyKey, MergeReceipt, MergeRequest, OperationReceipt, RevertReceipt, RevertRequest,
    RevertSelection, SubmissionError, SubmissionLookup, SubmissionState, Submitter,
    TransactionBody, TransactionReceipt, TransactionRequest,
};
use async_trait::async_trait;
use fluree_db_api::{
    ApiError, CommitRef, Fluree, GovernanceOptions, LedgerHandle, LedgerManager, PolicyContext,
};
use fluree_db_ledger::IndexConfig;
use fluree_db_transact::TxnType;
use moka::future::Cache;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::Duration;

/// Map a transaction-pipeline error into a [`SubmissionError`], preserving
/// the HTTP status so the caller can render an accurate response.
fn execution_failure(err: ApiError) -> SubmissionError {
    SubmissionError::Execution {
        status: err.status_code(),
        message: err.to_string(),
    }
}

/// Build a [`PolicyContext`] from the request's policy inputs.
///
/// Returns `Ok(None)` when there are no policy inputs — the transaction runs
/// under root. The context is built from a snapshot of the ledger this node
/// is about to stage against, so policy enforcement reflects the same state
/// the transaction commits onto. Building it here, rather than having the
/// caller pre-build and pass a context, keeps the policy context bound to
/// the executing node's state — the shape a replicated implementation needs.
async fn build_policy_context(
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

/// Default TTL for idempotency cache entries (1 hour).
///
/// After this duration, a previously-recorded submission state is forgotten;
/// the same idempotency key may then be reused for a new submission with any
/// body, and status lookups for the expired key return [`SubmissionState::Unknown`].
pub const DEFAULT_IDEMPOTENCY_TTL: Duration = Duration::from_secs(3600);

/// Upper bound on idempotency cache entries, so sustained keyed traffic
/// can't grow the cache without limit between TTL evictions.
const IDEMPOTENCY_CACHE_CAPACITY: u64 = 100_000;

/// Composite cache key: `(ledger_id, idempotency_key)`. Submissions on
/// different ledgers with the same key are independent.
type SubmissionCacheKey = (String, IdempotencyKey);

/// Cached state for a submission plus the hash of the body it carried.
/// The hash enables detecting the misuse case where the same idempotency
/// key is reused with a different transaction body.
#[derive(Clone)]
struct CachedSubmission {
    state: SubmissionState,
    body_hash: [u8; 32],
}

/// Monolithic consensus over the local Fluree transaction infrastructure.
///
/// Resolves the target ledger via the [`LedgerManager`] on the supplied
/// [`Fluree`] instance, takes the write lock, runs stage + commit through
/// the existing transactor, and replaces the cached ledger state with the
/// post-commit state.
///
/// Idempotency is tracked in an in-memory TTL cache. The cache is not
/// persisted across restarts; that is acceptable here because a process
/// restart loses any in-flight submissions anyway.
pub struct MonolithicConsensus {
    fluree: Arc<Fluree>,
    index_config: IndexConfig,
    cache: Cache<SubmissionCacheKey, CachedSubmission>,
}

impl MonolithicConsensus {
    /// Construct with the default 1-hour idempotency TTL.
    pub fn new(fluree: Arc<Fluree>, index_config: IndexConfig) -> Self {
        Self::with_ttl(fluree, index_config, DEFAULT_IDEMPOTENCY_TTL)
    }

    /// Construct with a caller-specified idempotency TTL.
    pub fn with_ttl(fluree: Arc<Fluree>, index_config: IndexConfig, ttl: Duration) -> Self {
        let cache = Cache::builder()
            .time_to_live(ttl)
            .max_capacity(IDEMPOTENCY_CACHE_CAPACITY)
            .build();
        Self {
            fluree,
            index_config,
            cache,
        }
    }

    fn hash_request_body(request: &TransactionRequest) -> [u8; 32] {
        let mut hasher = Sha256::new();
        // The format tag keeps a JSON-LD body and a Turtle body that happen
        // to stringify alike from hashing to the same value.
        match &request.body {
            TransactionBody::JsonLd(json) => {
                hasher.update(b"jsonld");
                hasher.update(json.to_string().as_bytes());
            }
            TransactionBody::Turtle(text) => {
                hasher.update(b"turtle");
                hasher.update(text.as_bytes());
            }
            TransactionBody::Sparql(text) => {
                hasher.update(b"sparql");
                hasher.update(text.as_bytes());
            }
        }
        let txn_type_tag: &[u8] = match request.txn_type {
            TxnType::Insert => b"insert",
            TxnType::Upsert => b"upsert",
            TxnType::Update => b"update",
        };
        hasher.update(txn_type_tag);
        hasher.finalize().into()
    }

    fn ledger_manager(&self) -> Result<&Arc<LedgerManager>, SubmissionError> {
        self.fluree
            .ledger_manager()
            .ok_or_else(|| SubmissionError::Execution {
                status: 500,
                message: "LedgerManager is not configured on the Fluree instance".into(),
            })
    }

    async fn execute_transaction(
        &self,
        ledger_id: &str,
        request: TransactionRequest,
    ) -> Result<TransactionReceipt, SubmissionError> {
        let TransactionRequest {
            idempotency_key,
            txn_type,
            body,
            txn_opts,
            commit_opts,
            tracking,
            governance,
        } = request;

        let ledger_handle = self
            .ledger_manager()?
            .get_or_load(ledger_id)
            .await
            .map_err(execution_failure)?;

        let policy_ctx = build_policy_context(&ledger_handle, &governance).await?;

        // The builder API holds the ledger write lock and replaces the cached
        // state internally for the duration of stage + commit — no manual
        // lock/clone/replace dance is needed here. The (body, txn_type) pair
        // selects the staging operation; Turtle/TriG has no update form.
        let staged = self.fluree.stage(&ledger_handle);
        let staged = match (&body, txn_type) {
            (TransactionBody::JsonLd(json), TxnType::Insert) => staged.insert(json),
            (TransactionBody::JsonLd(json), TxnType::Upsert) => staged.upsert(json),
            (TransactionBody::JsonLd(json), TxnType::Update) => staged.update(json),
            (TransactionBody::Turtle(text), TxnType::Insert) => staged.insert_turtle(text.as_str()),
            (TransactionBody::Turtle(text), TxnType::Upsert) => staged.upsert_turtle(text.as_str()),
            (TransactionBody::Turtle(_), TxnType::Update) => {
                return Err(SubmissionError::Execution {
                    status: 400,
                    message: "Turtle/TriG is not supported for update transactions".into(),
                });
            }
            // SPARQL UPDATE carries its own insert/update semantics in the
            // query, so `txn_type` is ignored for this body.
            (TransactionBody::Sparql(query), _) => staged.sparql_update(query.as_str()),
        };
        let mut builder = staged
            .txn_opts(txn_opts)
            .commit_opts(commit_opts)
            .index_config(self.index_config.clone());
        if let Some(tracking) = tracking {
            builder = builder.tracking(tracking);
        }
        if let Some(policy) = policy_ctx {
            builder = builder.policy(policy);
        }

        let result = builder.execute().await.map_err(execution_failure)?;

        Ok(TransactionReceipt {
            idempotency_key,
            commit: result.receipt,
            tally: result.tally,
        })
    }

    /// Atomically claim an idempotency slot in the cache.
    ///
    /// Returns `Ok(None)` when the caller wins the claim and must execute
    /// the operation. Returns `Ok(Some(receipt))` when an earlier
    /// submission with the same key and body already completed. Returns
    /// `Err(KeyCollision)` for a mismatched body or `Err(AlreadyInFlight)`
    /// when another caller's execution is still running.
    async fn try_claim_slot(
        &self,
        cache_key: SubmissionCacheKey,
        body_hash: [u8; 32],
    ) -> Result<Option<OperationReceipt>, SubmissionError> {
        // `or_insert_with_if` writes a fresh `InFlight` marker when the key
        // is absent, or replaces a prior failed attempt for the same body —
        // failures are re-attemptable. Concurrent submissions for the same
        // key see `is_fresh() == false` and collapse onto the existing
        // submission; only the caller that wins the claim goes on to execute.
        let claim = self
            .cache
            .entry(cache_key)
            .or_insert_with_if(
                std::future::ready(CachedSubmission {
                    state: SubmissionState::InFlight,
                    body_hash,
                }),
                |existing| {
                    matches!(existing.state, SubmissionState::Failed(_))
                        && existing.body_hash == body_hash
                },
            )
            .await;

        if claim.is_fresh() {
            return Ok(None);
        }

        let existing = claim.into_value();
        if existing.body_hash != body_hash {
            return Err(SubmissionError::KeyCollision);
        }
        match existing.state {
            SubmissionState::Committed(receipt) => Ok(Some(receipt)),
            _ => Err(SubmissionError::AlreadyInFlight),
        }
    }

    /// Record the outcome of a freshly-executed claim back into the cache.
    ///
    /// `wrap` lifts the per-operation receipt into the umbrella
    /// [`OperationReceipt`] so the cache stays uniform across operation
    /// kinds.
    async fn record_outcome<R, F>(
        &self,
        cache_key: SubmissionCacheKey,
        body_hash: [u8; 32],
        outcome: &Result<R, SubmissionError>,
        wrap: F,
    ) where
        R: Clone,
        F: FnOnce(R) -> OperationReceipt,
    {
        let final_state = match outcome {
            Ok(receipt) => SubmissionState::Committed(wrap(receipt.clone())),
            Err(err) => SubmissionState::Failed(err.clone()),
        };
        self.cache
            .insert(
                cache_key,
                CachedSubmission {
                    state: final_state,
                    body_hash,
                },
            )
            .await;
    }

    fn hash_revert_body(request: &RevertRequest) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(request.ledger_name.as_bytes());
        hasher.update([0u8]);
        hasher.update(request.branch.as_bytes());
        hasher.update([0u8]);
        match &request.selection {
            RevertSelection::Commits(commits) => {
                hasher.update([0u8]);
                hasher.update((commits.len() as u64).to_le_bytes());
                for commit in commits.iter() {
                    hash_commit_ref(&mut hasher, commit);
                }
            }
            RevertSelection::Range { from, to } => {
                hasher.update([1u8]);
                hash_commit_ref(&mut hasher, from);
                hash_commit_ref(&mut hasher, to);
            }
        }
        hasher.update([request.strategy as u8]);
        hasher.finalize().into()
    }

    async fn execute_revert(
        &self,
        request: RevertRequest,
    ) -> Result<RevertReceipt, SubmissionError> {
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

    fn hash_merge_body(request: &MergeRequest) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(request.ledger_name.as_bytes());
        hasher.update([0u8]);
        hasher.update(request.source_branch.as_bytes());
        hasher.update([0u8]);
        match &request.target_branch {
            Some(target) => {
                hasher.update([1u8]);
                hasher.update(target.as_bytes());
                hasher.update([0u8]);
            }
            None => hasher.update([0u8]),
        }
        hasher.update([request.strategy as u8]);
        hasher.finalize().into()
    }

    async fn execute_merge(
        &self,
        request: MergeRequest,
    ) -> Result<MergeReceipt, SubmissionError> {
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
}

fn hash_commit_ref(hasher: &mut Sha256, commit: &CommitRef) {
    match commit {
        CommitRef::Exact(id) => {
            hasher.update([0u8]);
            hasher.update(id.to_bytes());
        }
        CommitRef::Prefix(prefix) => {
            hasher.update([1u8]);
            hasher.update(prefix.as_bytes());
            hasher.update([0u8]);
        }
        CommitRef::T(t) => {
            hasher.update([2u8]);
            hasher.update(t.to_le_bytes());
        }
    }
}

#[async_trait]
impl Submitter for MonolithicConsensus {
    async fn transact(
        &self,
        ledger_id: &str,
        request: TransactionRequest,
    ) -> Result<TransactionReceipt, SubmissionError> {
        // Anonymous submissions skip the idempotency cache entirely:
        // no key means no retry-collapse and no later status lookup.
        let Some(idempotency_key) = request.idempotency_key.clone() else {
            return self.execute_transaction(ledger_id, request).await;
        };

        let cache_key = (ledger_id.to_string(), idempotency_key);
        let body_hash = Self::hash_request_body(&request);

        if let Some(receipt) = self.try_claim_slot(cache_key.clone(), body_hash).await? {
            return match receipt {
                OperationReceipt::Transaction(receipt) => Ok(receipt),
                OperationReceipt::Revert(_) | OperationReceipt::Merge(_) => {
                    Err(SubmissionError::KeyCollision)
                }
            };
        }

        let outcome = self.execute_transaction(ledger_id, request).await;
        self.record_outcome(cache_key, body_hash, &outcome, OperationReceipt::Transaction)
            .await;
        outcome
    }

    async fn revert(
        &self,
        request: RevertRequest,
    ) -> Result<RevertReceipt, SubmissionError> {
        let Some(idempotency_key) = request.idempotency_key.clone() else {
            return self.execute_revert(request).await;
        };

        // Cache key uses the same `ledger:branch` form as `transact` so a
        // single status-lookup endpoint works uniformly across op kinds.
        let ledger_id = fluree_db_api::format_ledger_id(&request.ledger_name, &request.branch);
        let cache_key = (ledger_id, idempotency_key);
        let body_hash = Self::hash_revert_body(&request);

        if let Some(receipt) = self.try_claim_slot(cache_key.clone(), body_hash).await? {
            return match receipt {
                OperationReceipt::Revert(receipt) => Ok(receipt),
                OperationReceipt::Transaction(_) | OperationReceipt::Merge(_) => {
                    Err(SubmissionError::KeyCollision)
                }
            };
        }

        let outcome = self.execute_revert(request).await;
        self.record_outcome(cache_key, body_hash, &outcome, OperationReceipt::Revert)
            .await;
        outcome
    }

    async fn merge(
        &self,
        request: MergeRequest,
    ) -> Result<MergeReceipt, SubmissionError> {
        let Some(idempotency_key) = request.idempotency_key.clone() else {
            return self.execute_merge(request).await;
        };

        // Namespace the cache by `ledger:source_branch`. The source branch
        // uniquely identifies the merge from the client's perspective and is
        // always known up front — no need to pre-resolve the target.
        let ledger_id =
            fluree_db_api::format_ledger_id(&request.ledger_name, &request.source_branch);
        let cache_key = (ledger_id, idempotency_key);
        let body_hash = Self::hash_merge_body(&request);

        if let Some(receipt) = self.try_claim_slot(cache_key.clone(), body_hash).await? {
            return match receipt {
                OperationReceipt::Merge(receipt) => Ok(receipt),
                OperationReceipt::Transaction(_) | OperationReceipt::Revert(_) => {
                    Err(SubmissionError::KeyCollision)
                }
            };
        }

        let outcome = self.execute_merge(request).await;
        self.record_outcome(cache_key, body_hash, &outcome, OperationReceipt::Merge)
            .await;
        outcome
    }
}

#[async_trait]
impl SubmissionLookup for MonolithicConsensus {
    async fn status(&self, ledger_id: &str, key: &IdempotencyKey) -> SubmissionState {
        let cache_key = (ledger_id.to_string(), key.clone());
        match self.cache.get(&cache_key).await {
            Some(entry) => entry.state,
            None => SubmissionState::Unknown,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_api::{CommitId, CommitRef, ConflictStrategy, FlureeBuilder, TrackingOptions};
    use fluree_db_transact::{CommitOpts, TxnOpts};
    use serde_json::{json, Value as JsonValue};

    /// Build a Fluree + `MonolithicConsensus` + initialized ledger.
    ///
    /// Each test gets its own in-memory Fluree, so tests don't share state
    /// and the same `ledger_id` is safe to reuse across tests.
    async fn setup() -> (Arc<fluree_db_api::Fluree>, MonolithicConsensus, String) {
        let fluree = Arc::new(FlureeBuilder::memory().build_memory());
        let ledger_id = "test/consensus:main".to_string();
        fluree
            .create_ledger(&ledger_id)
            .await
            .expect("create ledger");
        let index_config = fluree_db_ledger::IndexConfig {
            reindex_min_bytes: 1024 * 1024,
            reindex_max_bytes: 1024 * 1024 * 100,
        };
        let consensus = MonolithicConsensus::new(Arc::clone(&fluree), index_config);
        (fluree, consensus, ledger_id)
    }

    fn sample_insert(name: &str) -> JsonValue {
        json!({
            "@context": {"ex": "http://example.org/"},
            "@graph": [{
                "@id": format!("ex:{name}"),
                "ex:name": name
            }]
        })
    }

    fn request(key: Option<&str>, body: JsonValue) -> TransactionRequest {
        TransactionRequest {
            idempotency_key: key.map(IdempotencyKey::new),
            txn_type: TxnType::Insert,
            body: TransactionBody::JsonLd(body),
            txn_opts: TxnOpts::default(),
            commit_opts: CommitOpts::default(),
            tracking: None,
            governance: GovernanceOptions::default(),
        }
    }

    #[tokio::test]
    async fn anonymous_submission_returns_receipt() {
        let (_fluree, consensus, ledger_id) = setup().await;

        let receipt = consensus
            .transact(&ledger_id, request(None, sample_insert("alice")))
            .await
            .expect("submission to succeed");

        assert!(receipt.idempotency_key.is_none());
        assert!(receipt.commit.flake_count > 0);
    }

    #[tokio::test]
    async fn keyed_submission_is_visible_via_status_lookup() {
        let (_fluree, consensus, ledger_id) = setup().await;
        let key = IdempotencyKey::new("01J5XAMPLE001");

        let receipt = consensus
            .transact(
                &ledger_id,
                request(Some(key.as_str()), sample_insert("alice")),
            )
            .await
            .expect("submission to succeed");
        assert_eq!(receipt.idempotency_key.as_ref(), Some(&key));

        match consensus.status(&ledger_id, &key).await {
            SubmissionState::Committed(OperationReceipt::Transaction(stored)) => {
                assert_eq!(stored.commit.t, receipt.commit.t);
                assert_eq!(stored.commit.commit_id, receipt.commit.commit_id);
            }
            other => panic!("expected Committed(Transaction), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn status_returns_unknown_for_unseen_key() {
        let (_fluree, consensus, ledger_id) = setup().await;
        let key = IdempotencyKey::new("01J5UNKNOWN");

        assert!(matches!(
            consensus.status(&ledger_id, &key).await,
            SubmissionState::Unknown
        ));
    }

    #[tokio::test]
    async fn idempotent_retry_returns_cached_receipt() {
        let (_fluree, consensus, ledger_id) = setup().await;
        let key = IdempotencyKey::new("01J5RETRY001");
        let body = sample_insert("alice");

        let first = consensus
            .transact(&ledger_id, request(Some(key.as_str()), body.clone()))
            .await
            .expect("first submission to succeed");

        let second = consensus
            .transact(&ledger_id, request(Some(key.as_str()), body))
            .await
            .expect("retry with same body should return cached receipt");

        // Same receipt — the second call should NOT have re-executed.
        // If it had, the new transaction would advance `t` past the first.
        assert_eq!(first.commit.t, second.commit.t);
        assert_eq!(first.commit.commit_id, second.commit.commit_id);
    }

    #[tokio::test]
    async fn key_collision_with_different_body_errors() {
        let (_fluree, consensus, ledger_id) = setup().await;
        let key = IdempotencyKey::new("01J5COLLIDE001");

        consensus
            .transact(
                &ledger_id,
                request(Some(key.as_str()), sample_insert("alice")),
            )
            .await
            .expect("first submission to succeed");

        let err = consensus
            .transact(
                &ledger_id,
                request(Some(key.as_str()), sample_insert("bob")),
            )
            .await
            .expect_err("second submission with different body should fail");

        assert!(
            matches!(err, SubmissionError::KeyCollision),
            "expected KeyCollision, got {err:?}"
        );
    }

    #[tokio::test]
    async fn anonymous_submissions_do_not_populate_cache() {
        let (_fluree, consensus, ledger_id) = setup().await;

        consensus
            .transact(&ledger_id, request(None, sample_insert("alice")))
            .await
            .expect("anonymous submission");

        // A fresh keyed submission with any body should succeed — no anonymous
        // entry should sit in the cache to clash with it.
        let key = IdempotencyKey::new("01J5FRESH001");
        consensus
            .transact(
                &ledger_id,
                request(Some(key.as_str()), sample_insert("bob")),
            )
            .await
            .expect("fresh keyed submission should succeed after anonymous");
    }

    #[tokio::test]
    async fn upsert_routes_through_consensus() {
        let (_fluree, consensus, ledger_id) = setup().await;

        let mut req = request(None, sample_insert("alice"));
        req.txn_type = TxnType::Upsert;

        let receipt = consensus
            .transact(&ledger_id, req)
            .await
            .expect("upsert to succeed");
        assert!(receipt.commit.flake_count > 0);
    }

    #[tokio::test]
    async fn tracking_enabled_submission_carries_tally() {
        let (_fluree, consensus, ledger_id) = setup().await;

        let mut req = request(None, sample_insert("alice"));
        req.tracking = Some(TrackingOptions {
            track_time: true,
            track_fuel: true,
            track_policy: false,
            max_fuel: None,
        });

        let receipt = consensus
            .transact(&ledger_id, req)
            .await
            .expect("tracked submission to succeed");
        assert!(
            receipt.tally.is_some(),
            "a tracking-enabled submission should carry a tally"
        );
    }

    #[tokio::test]
    async fn policy_default_allow_permits_transaction() {
        let (_fluree, consensus, ledger_id) = setup().await;

        // `default-allow: true` is a policy input — it triggers policy-context
        // construction inside the consensus layer — and it permits the write.
        let mut req = request(None, sample_insert("alice"));
        req.governance = GovernanceOptions {
            default_allow: true,
            ..Default::default()
        };

        let receipt = consensus
            .transact(&ledger_id, req)
            .await
            .expect("policy-permitted transaction to succeed");
        assert!(receipt.commit.flake_count > 0);
    }

    #[tokio::test]
    async fn view_only_policy_blocks_transaction() {
        let (_fluree, consensus, ledger_id) = setup().await;

        // A view-only policy grants `f:view` but never `f:modify`; with
        // `default-allow: false` the write has no grant, so the consensus
        // layer's policy enforcement must reject it.
        let body = json!({
            "@context": {"ex": "http://example.org/"},
            "insert": {"@id": "ex:john", "ex:name": "John"}
        });
        let mut req = request(None, body);
        req.txn_type = TxnType::Update;
        req.governance = GovernanceOptions {
            policy: Some(json!([{
                "@id": "ex:viewOnly",
                "f:action": [{"@id": "f:view"}],
                "f:allow": true
            }])),
            default_allow: false,
            ..Default::default()
        };

        let err = consensus
            .transact(&ledger_id, req)
            .await
            .expect_err("view-only policy should block the write");
        // A policy denial is a client error — it must carry a 4xx status.
        match err {
            SubmissionError::Execution { status, .. } => {
                assert!((400..500).contains(&status), "expected 4xx, got {status}");
            }
            other => panic!("expected Execution, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn failed_submission_is_recorded_as_failed() {
        let (_fluree, consensus, _ledger_id) = setup().await;
        let key = IdempotencyKey::new("01J5FAILED001");
        let missing = "test/missing-ledger:main";

        let err = consensus
            .transact(missing, request(Some(key.as_str()), sample_insert("alice")))
            .await
            .expect_err("submission to a missing ledger should fail");
        assert!(
            matches!(err, SubmissionError::Execution { .. }),
            "expected an execution failure, got {err:?}"
        );

        match consensus.status(missing, &key).await {
            SubmissionState::Failed(_) => {}
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn turtle_insert_routes_through_consensus() {
        let (_fluree, consensus, ledger_id) = setup().await;

        let turtle = r#"@prefix ex: <http://example.org/> .
ex:alice ex:name "Alice" ."#;
        let req = TransactionRequest {
            idempotency_key: None,
            txn_type: TxnType::Insert,
            body: TransactionBody::Turtle(turtle.to_string()),
            txn_opts: TxnOpts::default(),
            commit_opts: CommitOpts::default(),
            tracking: None,
            governance: GovernanceOptions::default(),
        };

        let receipt = consensus
            .transact(&ledger_id, req)
            .await
            .expect("turtle insert to succeed");
        assert!(receipt.commit.flake_count > 0);
    }

    #[tokio::test]
    async fn turtle_update_is_rejected() {
        let (_fluree, consensus, ledger_id) = setup().await;

        let req = TransactionRequest {
            idempotency_key: None,
            txn_type: TxnType::Update,
            body: TransactionBody::Turtle(
                r#"@prefix ex: <http://example.org/> .
ex:alice ex:name "Alice" ."#
                    .to_string(),
            ),
            txn_opts: TxnOpts::default(),
            commit_opts: CommitOpts::default(),
            tracking: None,
            governance: GovernanceOptions::default(),
        };

        let err = consensus
            .transact(&ledger_id, req)
            .await
            .expect_err("Turtle is not a valid update body");
        match err {
            SubmissionError::Execution { status, .. } => {
                assert_eq!(status, 400, "Turtle-update rejection should be 400");
            }
            other => panic!("expected Execution, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn sparql_update_routes_through_consensus() {
        let (_fluree, consensus, ledger_id) = setup().await;

        let req = TransactionRequest {
            idempotency_key: None,
            txn_type: TxnType::Update,
            body: TransactionBody::Sparql(
                r#"INSERT DATA { <http://example.org/alice> <http://example.org/name> "Alice" . }"#
                    .to_string(),
            ),
            txn_opts: TxnOpts::default(),
            commit_opts: CommitOpts::default(),
            tracking: None,
            governance: GovernanceOptions::default(),
        };

        let receipt = consensus
            .transact(&ledger_id, req)
            .await
            .expect("SPARQL UPDATE to succeed");
        assert!(receipt.commit.flake_count > 0);
    }

    #[tokio::test]
    async fn sparql_parse_error_is_rejected() {
        let (_fluree, consensus, ledger_id) = setup().await;

        let req = TransactionRequest {
            idempotency_key: None,
            txn_type: TxnType::Update,
            body: TransactionBody::Sparql("INSERT DATA { this is not valid sparql".to_string()),
            txn_opts: TxnOpts::default(),
            commit_opts: CommitOpts::default(),
            tracking: None,
            governance: GovernanceOptions::default(),
        };

        let err = consensus
            .transact(&ledger_id, req)
            .await
            .expect_err("malformed SPARQL should be rejected");
        match err {
            SubmissionError::Execution { status, .. } => {
                assert_eq!(status, 400, "SPARQL parse error should be 400");
            }
            other => panic!("expected Execution, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn failed_submission_can_be_retried() {
        let (fluree, consensus, _ledger_id) = setup().await;
        let key = IdempotencyKey::new("01J5RETRYAFTERFAIL");
        let ledger = "test/created-later:main";
        let body = sample_insert("alice");

        // First attempt fails — the ledger does not exist yet.
        consensus
            .transact(ledger, request(Some(key.as_str()), body.clone()))
            .await
            .expect_err("first attempt should fail before the ledger exists");

        // Create the ledger, then retry with the same key + body. A cached
        // `Failed` entry must not block the retry.
        fluree.create_ledger(ledger).await.expect("create ledger");
        let receipt = consensus
            .transact(ledger, request(Some(key.as_str()), body))
            .await
            .expect("retry after the ledger exists should succeed");
        assert!(receipt.commit.flake_count > 0);
    }

    /// Submit two inserts against the test ledger and return the second
    /// commit's ID — the first call seeds the genesis commit (which cannot
    /// be reverted) and the second produces the revertable commit every
    /// revert test needs.
    async fn seed_commit(consensus: &MonolithicConsensus, ledger_id: &str, name: &str) -> CommitId {
        consensus
            .transact(ledger_id, request(None, sample_insert("__genesis__")))
            .await
            .expect("genesis transaction to succeed");
        let receipt = consensus
            .transact(ledger_id, request(None, sample_insert(name)))
            .await
            .expect("seed transaction to succeed");
        receipt.commit.commit_id
    }

    fn revert_request(
        key: Option<&str>,
        commit: CommitId,
        strategy: ConflictStrategy,
    ) -> RevertRequest {
        RevertRequest {
            idempotency_key: key.map(IdempotencyKey::new),
            ledger_name: "test/consensus".to_string(),
            branch: "main".to_string(),
            selection: RevertSelection::single(CommitRef::Exact(commit)),
            strategy,
        }
    }

    #[tokio::test]
    async fn anonymous_revert_returns_receipt() {
        let (_fluree, consensus, ledger_id) = setup().await;
        let commit = seed_commit(&consensus, &ledger_id, "alice").await;

        let receipt = consensus
            .revert(revert_request(None, commit, ConflictStrategy::Abort))
            .await
            .expect("revert to succeed");

        assert!(receipt.idempotency_key.is_none());
        assert_eq!(receipt.reverted_commits.len(), 1);
    }

    #[tokio::test]
    async fn idempotent_revert_returns_cached_receipt() {
        let (_fluree, consensus, ledger_id) = setup().await;
        let commit = seed_commit(&consensus, &ledger_id, "alice").await;
        let key = "01J5REVERTRETRY";

        let first = consensus
            .revert(revert_request(
                Some(key),
                commit.clone(),
                ConflictStrategy::Abort,
            ))
            .await
            .expect("first revert to succeed");

        let second = consensus
            .revert(revert_request(Some(key), commit, ConflictStrategy::Abort))
            .await
            .expect("retry with same body should return cached receipt");

        // Same receipt — the second call must not have re-executed.
        // A second revert would advance `new_head_t` past the first.
        assert_eq!(first.new_head_t, second.new_head_t);
        assert_eq!(first.new_head_id, second.new_head_id);
    }

    #[tokio::test]
    async fn keyed_revert_is_visible_via_status_lookup() {
        let (_fluree, consensus, ledger_id) = setup().await;
        let commit = seed_commit(&consensus, &ledger_id, "alice").await;
        let key = IdempotencyKey::new("01J5REVERTSTATUS");

        let receipt = consensus
            .revert(revert_request(
                Some(key.as_str()),
                commit,
                ConflictStrategy::Abort,
            ))
            .await
            .expect("revert to succeed");

        match consensus.status(&ledger_id, &key).await {
            SubmissionState::Committed(OperationReceipt::Revert(stored)) => {
                assert_eq!(stored.new_head_t, receipt.new_head_t);
                assert_eq!(stored.new_head_id, receipt.new_head_id);
            }
            other => panic!("expected Committed(Revert), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn revert_key_collides_with_transaction_key() {
        let (_fluree, consensus, ledger_id) = setup().await;
        let key = "01J5MIXEDKEY";
        // Seed a non-genesis commit so the revert target is valid.
        let commit = seed_commit(&consensus, &ledger_id, "alice").await;

        // A keyed transaction claims the key on this ledger:branch.
        consensus
            .transact(&ledger_id, request(Some(key), sample_insert("carol")))
            .await
            .expect("keyed transaction to succeed");

        // A revert reusing the same key must collide — the cached entry is
        // a `Transaction` receipt, not a `Revert` one, so the bodies cannot
        // match.
        let err = consensus
            .revert(revert_request(Some(key), commit, ConflictStrategy::Abort))
            .await
            .expect_err("revert with a transaction's key should collide");
        assert!(
            matches!(err, SubmissionError::KeyCollision),
            "expected KeyCollision, got {err:?}"
        );
    }

    /// Build a Fluree + consensus + a parent branch with one commit + a child
    /// `feature` branch with one additional commit — the minimum setup a
    /// merge test needs.
    async fn setup_with_feature_branch() -> (Arc<fluree_db_api::Fluree>, MonolithicConsensus) {
        let (fluree, consensus, parent_id) = setup().await;
        // Genesis commit on `main` so the branch has a head to fork from.
        consensus
            .transact(&parent_id, request(None, sample_insert("__genesis__")))
            .await
            .expect("seed commit to succeed");
        fluree
            .create_branch("test/consensus", "feature", Some("main"), None)
            .await
            .expect("create feature branch");
        // One commit on `feature` so the merge has something to apply.
        consensus
            .transact(
                "test/consensus:feature",
                request(None, sample_insert("alice")),
            )
            .await
            .expect("commit on feature to succeed");
        (fluree, consensus)
    }

    fn merge_request(key: Option<&str>) -> MergeRequest {
        MergeRequest {
            idempotency_key: key.map(IdempotencyKey::new),
            ledger_name: "test/consensus".to_string(),
            source_branch: "feature".to_string(),
            target_branch: Some("main".to_string()),
            strategy: ConflictStrategy::default(),
        }
    }

    #[tokio::test]
    async fn anonymous_merge_returns_receipt() {
        let (_fluree, consensus) = setup_with_feature_branch().await;

        let receipt = consensus
            .merge(merge_request(None))
            .await
            .expect("merge to succeed");

        assert!(receipt.idempotency_key.is_none());
        assert_eq!(receipt.source, "feature");
        assert_eq!(receipt.target, "main");
        // `main` hasn't advanced since `feature` branched, so the merge
        // resolves to a fast-forward.
        assert!(receipt.fast_forward);
    }

    #[tokio::test]
    async fn idempotent_merge_returns_cached_receipt() {
        let (_fluree, consensus) = setup_with_feature_branch().await;
        let key = "01J5MERGERETRY";

        let first = consensus
            .merge(merge_request(Some(key)))
            .await
            .expect("first merge to succeed");

        let second = consensus
            .merge(merge_request(Some(key)))
            .await
            .expect("retry with same body should return cached receipt");

        // Same receipt — the second call must not have re-executed.
        // A second merge attempt against the already-merged target would
        // change the head or fail; either way the t/id would differ.
        assert_eq!(first.new_head_t, second.new_head_t);
        assert_eq!(first.new_head_id, second.new_head_id);
    }

    #[tokio::test]
    async fn keyed_merge_is_visible_via_status_lookup() {
        let (_fluree, consensus) = setup_with_feature_branch().await;
        let key = IdempotencyKey::new("01J5MERGESTATUS");

        let receipt = consensus
            .merge(merge_request(Some(key.as_str())))
            .await
            .expect("merge to succeed");

        // Status namespacing for merge is `ledger:source_branch`.
        let cache_ledger_id = fluree_db_api::format_ledger_id("test/consensus", "feature");
        match consensus.status(&cache_ledger_id, &key).await {
            SubmissionState::Committed(OperationReceipt::Merge(stored)) => {
                assert_eq!(stored.new_head_t, receipt.new_head_t);
                assert_eq!(stored.new_head_id, receipt.new_head_id);
                assert_eq!(stored.fast_forward, receipt.fast_forward);
            }
            other => panic!("expected Committed(Merge), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn merge_key_collides_with_transaction_key() {
        let (_fluree, consensus) = setup_with_feature_branch().await;
        let key = "01J5MIXEDMERGEKEY";

        // A keyed transaction on `ledger:feature` claims the cache slot.
        consensus
            .transact(
                "test/consensus:feature",
                request(Some(key), sample_insert("dave")),
            )
            .await
            .expect("keyed transaction to succeed");

        let err = consensus
            .merge(merge_request(Some(key)))
            .await
            .expect_err("merge with a transaction's key should collide");
        assert!(
            matches!(err, SubmissionError::KeyCollision),
            "expected KeyCollision, got {err:?}"
        );
    }
}
