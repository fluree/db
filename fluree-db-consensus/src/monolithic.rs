//! Monolithic consensus implementation.
//!
//! A single integrated unit handles receiving, ordering, and executing every
//! transaction. No replication, no quorum — the local execution stream *is*
//! the agreement. Suitable for development, testing, and deployments that
//! do not need cross-node coordination.

use crate::{
    IdempotencyKey, SubmissionError, SubmissionLookup, SubmissionState, Submitter, TransactionBody,
    TransactionReceipt, TransactionRequest,
};
use async_trait::async_trait;
use fluree_db_api::{
    ApiError, Fluree, GovernanceOptions, LedgerHandle, LedgerManager, PolicyContext,
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
                // A previously-failed submission is re-attemptable with the
                // same key — failures may be transient.
                SubmissionState::Failed(_) | SubmissionState::Unknown => {}
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
    use fluree_db_api::{FlureeBuilder, TrackingOptions};
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
            .submit(
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
            .submit(&ledger_id, request(None, sample_insert("alice")))
            .await
            .expect("anonymous submission");

        // A fresh keyed submission with any body should succeed — no anonymous
        // entry should sit in the cache to clash with it.
        let key = IdempotencyKey::new("01J5FRESH001");
        consensus
            .submit(
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
            .submit(&ledger_id, req)
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
            .submit(&ledger_id, req)
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
            .submit(&ledger_id, req)
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
            .submit(&ledger_id, req)
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
            .submit(missing, request(Some(key.as_str()), sample_insert("alice")))
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
            .submit(&ledger_id, req)
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
            .submit(&ledger_id, req)
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
            .submit(&ledger_id, req)
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
            .submit(&ledger_id, req)
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
            .submit(ledger, request(Some(key.as_str()), body.clone()))
            .await
            .expect_err("first attempt should fail before the ledger exists");

        // Create the ledger, then retry with the same key + body. A cached
        // `Failed` entry must not block the retry.
        fluree.create_ledger(ledger).await.expect("create ledger");
        let receipt = consensus
            .submit(ledger, request(Some(key.as_str()), body))
            .await
            .expect("retry after the ledger exists should succeed");
        assert!(receipt.commit.flake_count > 0);
    }
}
