//! Monolithic consensus implementation.
//!
//! A single integrated unit handles receiving, ordering, and executing every
//! transaction. No replication, no quorum — the local execution stream *is*
//! the agreement. Suitable for development, testing, and deployments that
//! do not need cross-node coordination.

use crate::{
    IdempotencyKey, SubmissionError, SubmissionLookup, SubmissionState, Submitter,
    TransactionReceipt, TransactionRequest,
};
use async_trait::async_trait;
use fluree_db_api::{Fluree, LedgerHandle, LedgerManager, PolicyContext, QueryConnectionOptions};
use fluree_db_ledger::IndexConfig;
use fluree_db_transact::TxnType;
use moka::future::Cache;
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::Duration;

/// Build a [`PolicyContext`] from the ledger state when the transaction body
/// carries policy inputs (identity / policy class in its `opts`).
///
/// Returns `Ok(None)` when there are no policy inputs — the transaction runs
/// under root. The context is built from a snapshot of the ledger this node
/// is about to stage against, so policy enforcement reflects the same state
/// the transaction commits onto. Building it here, rather than having the
/// caller pre-build and pass it, keeps the policy context bound to the
/// executing node's state — the shape a replicated implementation needs.
async fn build_policy_context(
    ledger_handle: &LedgerHandle,
    txn_json: &JsonValue,
) -> Result<Option<PolicyContext>, SubmissionError> {
    let qc_opts = QueryConnectionOptions::from_json(txn_json).unwrap_or_default();
    if !qc_opts.has_any_policy_inputs() {
        return Ok(None);
    }

    let snap = ledger_handle.snapshot().await;
    fluree_db_api::build_policy_context(
        &snap.snapshot,
        snap.novelty.as_ref(),
        Some(snap.novelty.as_ref()),
        snap.t,
        &qc_opts,
    )
    .await
    .map(Some)
    .map_err(|e| SubmissionError::Submission(format!("policy context: {e}")))
}

/// Default TTL for idempotency cache entries (1 hour).
///
/// After this duration, a previously-recorded submission state is forgotten;
/// the same idempotency key may then be reused for a new submission with any
/// body, and status lookups for the expired key return [`SubmissionState::Unknown`].
pub const DEFAULT_IDEMPOTENCY_TTL: Duration = Duration::from_secs(3600);

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
        let cache = Cache::builder().time_to_live(ttl).build();
        Self {
            fluree,
            index_config,
            cache,
        }
    }

    fn hash_request_body(request: &TransactionRequest) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(request.txn_json.to_string().as_bytes());
        hasher.update(format!("{:?}", request.txn_type).as_bytes());
        hasher.finalize().into()
    }

    fn ledger_manager(&self) -> Result<&Arc<LedgerManager>, SubmissionError> {
        self.fluree.ledger_manager().ok_or_else(|| {
            SubmissionError::Submission(
                "LedgerManager is not configured on the Fluree instance".into(),
            )
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
            txn_json,
            txn_opts,
            commit_opts,
            tracking,
        } = request;

        let ledger_handle = self
            .ledger_manager()?
            .get_or_load(ledger_id)
            .await
            .map_err(|e| SubmissionError::Submission(format!("ledger load: {e}")))?;

        let policy_ctx = build_policy_context(&ledger_handle, &txn_json).await?;

        // The builder API holds the ledger write lock and replaces the cached
        // state internally for the duration of stage + commit — no manual
        // lock/clone/replace dance is needed here.
        let builder = self.fluree.stage(&ledger_handle);
        let builder = match txn_type {
            TxnType::Insert => builder.insert(&txn_json),
            TxnType::Upsert => builder.upsert(&txn_json),
            TxnType::Update => builder.update(&txn_json),
        };
        let mut builder = builder
            .txn_opts(txn_opts)
            .commit_opts(commit_opts)
            .index_config(self.index_config.clone());
        if let Some(tracking) = tracking {
            builder = builder.tracking(tracking);
        }
        if let Some(policy) = policy_ctx {
            builder = builder.policy(policy);
        }

        let result = builder
            .execute()
            .await
            .map_err(|e| SubmissionError::Submission(format!("transact: {e}")))?;

        Ok(TransactionReceipt {
            idempotency_key,
            commit: result.receipt,
            tally: result.tally,
        })
    }
}

#[async_trait]
impl Submitter for MonolithicConsensus {
    async fn submit(
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

        if let Some(existing) = self.cache.get(&cache_key).await {
            if existing.body_hash != body_hash {
                return Err(SubmissionError::KeyCollision);
            }
            match existing.state {
                SubmissionState::Committed(receipt) => return Ok(receipt),
                SubmissionState::InFlight => return Err(SubmissionError::AlreadyInFlight),
                SubmissionState::Failed(_) | SubmissionState::Unknown => {
                    // Allow re-attempt of a previously-failed submission with
                    // the same body. Falls through to the normal submission path.
                }
            }
        }

        self.cache
            .insert(
                cache_key.clone(),
                CachedSubmission {
                    state: SubmissionState::InFlight,
                    body_hash,
                },
            )
            .await;

        let outcome = self.execute_transaction(ledger_id, request).await;

        let final_state = match &outcome {
            Ok(receipt) => SubmissionState::Committed(receipt.clone()),
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
    use fluree_db_api::FlureeBuilder;
    use fluree_db_transact::{CommitOpts, TxnOpts};
    use serde_json::json;

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
            txn_json: body,
            txn_opts: TxnOpts::default(),
            commit_opts: CommitOpts::default(),
            tracking: None,
        }
    }

    #[tokio::test]
    async fn anonymous_submission_returns_receipt() {
        let (_fluree, consensus, ledger_id) = setup().await;

        let receipt = consensus
            .submit(&ledger_id, request(None, sample_insert("alice")))
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
            .submit(
                &ledger_id,
                request(Some(key.as_str()), sample_insert("alice")),
            )
            .await
            .expect("submission to succeed");
        assert_eq!(receipt.idempotency_key.as_ref(), Some(&key));

        match consensus.status(&ledger_id, &key).await {
            SubmissionState::Committed(stored) => {
                assert_eq!(stored.commit.t, receipt.commit.t);
                assert_eq!(stored.commit.commit_id, receipt.commit.commit_id);
            }
            other => panic!("expected Committed, got {other:?}"),
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
            .submit(&ledger_id, request(Some(key.as_str()), body.clone()))
            .await
            .expect("first submission to succeed");

        let second = consensus
            .submit(&ledger_id, request(Some(key.as_str()), body))
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
            .submit(
                &ledger_id,
                request(Some(key.as_str()), sample_insert("alice")),
            )
            .await
            .expect("first submission to succeed");

        let err = consensus
            .submit(&ledger_id, request(Some(key.as_str()), sample_insert("bob")))
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
            .submit(&ledger_id, request(None, sample_insert("alice")))
            .await
            .expect("anonymous submission");

        // A fresh keyed submission with any body should succeed — no anonymous
        // entry should sit in the cache to clash with it.
        let key = IdempotencyKey::new("01J5FRESH001");
        consensus
            .submit(&ledger_id, request(Some(key.as_str()), sample_insert("bob")))
            .await
            .expect("fresh keyed submission should succeed after anonymous");
    }
}
