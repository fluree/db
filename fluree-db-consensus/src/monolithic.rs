//! Monolithic consensus implementation.
//!
//! A single integrated unit handles receiving, ordering, and executing every
//! transaction. No replication, no quorum — the local execution stream *is*
//! the agreement. Suitable for development, testing, and deployments that
//! do not need cross-node coordination.

use crate::{
    SubmissionError, IdempotencyKey, SubmissionLookup, SubmissionState, Submitter,
    TransactionReceipt, TransactionRequest,
};
use async_trait::async_trait;
use fluree_db_api::{Fluree, LedgerManager};
use fluree_db_ledger::IndexConfig;
use moka::future::Cache;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::Duration;

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
        let ledger_handle = self
            .ledger_manager()?
            .get_or_load(ledger_id)
            .await
            .map_err(|e| SubmissionError::Submission(format!("ledger load: {e}")))?;

        let mut write_guard = ledger_handle.lock_for_write().await;
        let ledger_state = write_guard.clone_state();

        let result = self
            .fluree
            .transact(
                ledger_state,
                request.txn_type,
                &request.txn_json,
                request.txn_opts,
                request.commit_opts,
                &self.index_config,
            )
            .await
            .map_err(|e| SubmissionError::Submission(format!("transact: {e}")))?;

        ledger_handle
            .sync_binary_store_from_state(&result.ledger)
            .await;
        write_guard.replace(result.ledger);

        Ok(TransactionReceipt {
            idempotency_key: request.idempotency_key,
            commit: result.receipt,
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
