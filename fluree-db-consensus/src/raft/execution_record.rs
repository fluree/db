//! Execution records — durable, content-store-resident artifacts
//! that survive a leader crash mid-flight, enabling retry-without-
//! re-stage so the cluster neither pays a second compute charge nor
//! loses the original fuel tally.
//!
//! ## Why
//!
//! Without this layer, a leader that crashes after writing the commit
//! blob but before reaching Raft quorum forces a retry to choose
//! between re-staging (wasting work) and reusing the blob (losing the
//! fuel bill — a DOS hole). With the record, the new leader finds the
//! same staging output *and* the same fuel tally, charges once,
//! advances the ref.
//!
//! ## Lookup
//!
//! Records are addressed by a deterministic hash of
//! `(body_hash, idempotency_cache_key)` so any node on any retry can
//! locate the record without consulting an index. Anonymous
//! submissions (no idempotency key) don't get records — they can't
//! be deduplicated anyway.
//!
//! ## Lifecycle
//!
//! Records are written via the raw [`StorageWrite::write_bytes`] API
//! rather than CAS-verified — their identity is their lookup key,
//! not their content hash. Cleanup is handled by a periodic sweep
//! (see [`sweep_stale_records`]) that deletes anything older than
//! `idempotency_ttl + grace`. No Raft log traffic for releases.

use crate::raft::state_machine::RecordedTally;
use crate::IdempotencyCacheKey;
use fluree_db_api::{ledger_id_prefix_for_path, ContentId, Storage};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

/// Outcome of the leader's staging work for one keyed submission.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExecutionStatus {
    /// Staging produced a valid commit blob.
    Applied { commit_cid: ContentId, head_t: i64 },
    /// Staging consumed fuel but failed (validation, policy, etc.).
    /// Retries finding this status bill the same fuel and surface
    /// the same failure rather than re-attempting.
    Failed { reason: String },
}

/// Typed reference to an execution record by its lookup key.
///
/// Travels through Raft on `AdvanceRefArgs::release` so every node
/// learns which records can be released after the advance applies.
/// Each node reconstructs the full storage address via
/// [`exec_address`] using its local storage method.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionRecordRef {
    pub key: IdempotencyCacheKey,
    pub body_hash: [u8; 32],
}

impl ExecutionRecordRef {
    pub fn new(key: IdempotencyCacheKey, body_hash: [u8; 32]) -> Self {
        Self { key, body_hash }
    }
}

/// Durable record of one keyed submission's staging work.
///
/// Written by the leader after staging but before proposing the
/// resulting `AdvanceRef` through Raft. Survives a leader crash, so
/// a retry handled by a different leader can skip re-staging and
/// bill the original fuel.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionRecord {
    /// SHA-256 of the request body. Lets a retry verify the record
    /// is for the same body before reusing it.
    pub body_hash: [u8; 32],
    /// Pointer to the body in the content store.
    pub body_ref: ContentId,
    /// Idempotency cache key the submission carried.
    pub idempotency_key: IdempotencyCacheKey,
    /// What happened during staging.
    pub status: ExecutionStatus,
    /// Fuel / tracking accounting. Load-bearing for DOS mitigation —
    /// retries finding this record charge this fuel rather than
    /// recomputing.
    pub tally: RecordedTally,
    /// Leader's wall-clock at recording, milliseconds since the Unix
    /// epoch. Source of truth for the sweep's staleness check.
    pub recorded_at_millis: u64,
}

/// Errors from execution-record operations.
#[derive(Debug, Error)]
pub enum ExecutionRecordError {
    #[error("postcard error: {0}")]
    Postcard(#[from] postcard::Error),
    #[error("storage error: {0}")]
    Storage(String),
}

/// Compute the deterministic address where the execution record for
/// `(body_hash, key)` lives in storage exposed via `method`.
///
/// The path digest is `SHA-256(body_hash || ledger_id || ':' || key)`,
/// so given the same body and idempotency key the address is fixed —
/// a retry locates the record without an index.
pub fn exec_address(method: &str, body_hash: &[u8; 32], key: &IdempotencyCacheKey) -> String {
    let mut hasher = Sha256::new();
    hasher.update(body_hash);
    hasher.update(key.ledger_id.as_bytes());
    hasher.update(b":");
    hasher.update(key.key.as_str().as_bytes());
    let digest = hasher.finalize();
    let hex_digest = hex::encode(digest);

    let ledger_prefix = ledger_id_prefix_for_path(&key.ledger_id);
    format!("fluree:{method}://{ledger_prefix}/exec/{hex_digest}.exec")
}

/// Listing prefix for all execution records under a given ledger.
/// Used by [`sweep_stale_records`].
pub fn exec_prefix(method: &str, ledger_id: &str) -> String {
    let ledger_prefix = ledger_id_prefix_for_path(ledger_id);
    format!("fluree:{method}://{ledger_prefix}/exec/")
}

/// Write an execution record to storage at the address derived from
/// `(record.body_hash, record.idempotency_key)`.
pub async fn write_execution_record<S: Storage>(
    storage: &S,
    record: &ExecutionRecord,
) -> Result<(), ExecutionRecordError> {
    let address = exec_address(
        storage.storage_method(),
        &record.body_hash,
        &record.idempotency_key,
    );
    let bytes = postcard::to_allocvec(record)?;
    storage
        .write_bytes(&address, &bytes)
        .await
        .map_err(|e| ExecutionRecordError::Storage(e.to_string()))
}

/// Read the execution record for `(body_hash, key)`, returning
/// `Ok(None)` when no record exists at the derived address.
pub async fn read_execution_record_by_key<S: Storage>(
    storage: &S,
    body_hash: &[u8; 32],
    key: &IdempotencyCacheKey,
) -> Result<Option<ExecutionRecord>, ExecutionRecordError> {
    let address = exec_address(storage.storage_method(), body_hash, key);
    let exists = storage
        .exists(&address)
        .await
        .map_err(|e| ExecutionRecordError::Storage(e.to_string()))?;
    if !exists {
        return Ok(None);
    }
    let bytes = storage
        .read_bytes(&address)
        .await
        .map_err(|e| ExecutionRecordError::Storage(e.to_string()))?;
    let record = postcard::from_bytes(&bytes)?;
    Ok(Some(record))
}

/// Sweep stale execution records for `ledger_id` in `storage`.
///
/// Deletes any record whose `recorded_at_millis` is strictly less
/// than `threshold_millis`. The threshold should be calculated as
/// `now - (idempotency_ttl + grace)` so we only release records past
/// the point where the idempotency cache could still have their
/// entry. Returns the number of records deleted.
///
/// Records whose bytes can't be read or parsed are skipped — they're
/// either being written concurrently (will be picked up next sweep)
/// or corrupt (caller should investigate, but the sweep doesn't
/// block on them).
pub async fn sweep_stale_records<S: Storage>(
    storage: &S,
    ledger_id: &str,
    threshold_millis: u64,
) -> Result<usize, ExecutionRecordError> {
    let prefix = exec_prefix(storage.storage_method(), ledger_id);
    let addresses = storage
        .list_prefix(&prefix)
        .await
        .map_err(|e| ExecutionRecordError::Storage(e.to_string()))?;

    let mut deleted = 0;
    for address in addresses {
        let Ok(bytes) = storage.read_bytes(&address).await else {
            continue;
        };
        let Ok(record) = postcard::from_bytes::<ExecutionRecord>(&bytes) else {
            continue;
        };
        if record.recorded_at_millis < threshold_millis && storage.delete(&address).await.is_ok() {
            deleted += 1;
        }
    }
    Ok(deleted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::IdempotencyKey;
    use fluree_db_api::{ContentKind, MemoryStorage};

    fn cid(seed: u8) -> ContentId {
        ContentId::new(ContentKind::Commit, &[seed])
    }

    fn key(ledger: &str, k: &str) -> IdempotencyCacheKey {
        IdempotencyCacheKey::new(ledger, IdempotencyKey::new(k))
    }

    fn record(seed: u8, recorded_at: u64) -> ExecutionRecord {
        ExecutionRecord {
            body_hash: [seed; 32],
            body_ref: cid(seed),
            idempotency_key: key("test/db", &format!("k{seed}")),
            status: ExecutionStatus::Applied {
                commit_cid: cid(seed.wrapping_add(1)),
                head_t: i64::from(seed),
            },
            tally: RecordedTally {
                time: Some("1ms".to_string()),
                fuel: Some(42.0),
                policy: None,
            },
            recorded_at_millis: recorded_at,
        }
    }

    #[test]
    fn serialization_round_trip() {
        let r = record(7, 1000);
        let bytes = postcard::to_allocvec(&r).unwrap();
        let r2: ExecutionRecord = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(r.body_hash, r2.body_hash);
        assert_eq!(r.body_ref, r2.body_ref);
        assert_eq!(r.idempotency_key, r2.idempotency_key);
        assert_eq!(r.recorded_at_millis, r2.recorded_at_millis);
    }

    #[test]
    fn exec_address_is_deterministic() {
        let k = key("test/db", "k1");
        let body_hash = [7u8; 32];
        let a = exec_address("memory", &body_hash, &k);
        let b = exec_address("memory", &body_hash, &k);
        assert_eq!(a, b);
    }

    #[test]
    fn exec_address_differs_by_body_hash() {
        let k = key("test/db", "k1");
        assert_ne!(
            exec_address("memory", &[1u8; 32], &k),
            exec_address("memory", &[2u8; 32], &k),
        );
    }

    #[test]
    fn exec_address_differs_by_idempotency_key() {
        let body_hash = [7u8; 32];
        assert_ne!(
            exec_address("memory", &body_hash, &key("test/db", "k1")),
            exec_address("memory", &body_hash, &key("test/db", "k2")),
        );
    }

    #[test]
    fn exec_address_differs_by_ledger() {
        let body_hash = [7u8; 32];
        assert_ne!(
            exec_address("memory", &body_hash, &key("test/db1", "k1")),
            exec_address("memory", &body_hash, &key("test/db2", "k1")),
        );
    }

    #[tokio::test]
    async fn write_and_read_round_trip() {
        let storage = MemoryStorage::new();
        let r = record(7, 1000);
        write_execution_record(&storage, &r).await.unwrap();
        let read = read_execution_record_by_key(&storage, &r.body_hash, &r.idempotency_key)
            .await
            .unwrap()
            .expect("record present");
        assert_eq!(read.body_hash, r.body_hash);
        assert_eq!(read.recorded_at_millis, r.recorded_at_millis);
        match read.status {
            ExecutionStatus::Applied { head_t, .. } => assert_eq!(head_t, 7),
            ExecutionStatus::Failed { .. } => panic!("expected Applied"),
        }
    }

    #[tokio::test]
    async fn read_missing_returns_none() {
        let storage = MemoryStorage::new();
        let result = read_execution_record_by_key(&storage, &[0u8; 32], &key("test/db", "missing"))
            .await
            .unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn sweep_removes_stale_and_keeps_fresh() {
        let storage = MemoryStorage::new();
        write_execution_record(&storage, &record(1, 100))
            .await
            .unwrap();
        write_execution_record(&storage, &record(2, 200))
            .await
            .unwrap();
        write_execution_record(&storage, &record(3, 500))
            .await
            .unwrap();

        let deleted = sweep_stale_records(&storage, "test/db", 300).await.unwrap();
        assert_eq!(deleted, 2);

        assert!(
            read_execution_record_by_key(&storage, &[3u8; 32], &key("test/db", "k3"))
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            read_execution_record_by_key(&storage, &[1u8; 32], &key("test/db", "k1"))
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            read_execution_record_by_key(&storage, &[2u8; 32], &key("test/db", "k2"))
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn sweep_no_op_when_all_fresh() {
        let storage = MemoryStorage::new();
        write_execution_record(&storage, &record(1, 1000))
            .await
            .unwrap();
        write_execution_record(&storage, &record(2, 2000))
            .await
            .unwrap();

        let deleted = sweep_stale_records(&storage, "test/db", 500).await.unwrap();
        assert_eq!(deleted, 0);
        assert!(
            read_execution_record_by_key(&storage, &[1u8; 32], &key("test/db", "k1"))
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn sweep_scoped_to_target_ledger() {
        let storage = MemoryStorage::new();

        let mut r1 = record(1, 100);
        r1.idempotency_key = key("test/db1", "k1");
        write_execution_record(&storage, &r1).await.unwrap();

        let mut r2 = record(2, 100);
        r2.idempotency_key = key("test/db1", "k2");
        write_execution_record(&storage, &r2).await.unwrap();

        let mut r3 = record(3, 100);
        r3.idempotency_key = key("test/db2", "k1");
        write_execution_record(&storage, &r3).await.unwrap();

        let deleted = sweep_stale_records(&storage, "test/db1", 500)
            .await
            .unwrap();
        assert_eq!(deleted, 2);

        // db2's record untouched.
        let db2 = read_execution_record_by_key(&storage, &[3u8; 32], &key("test/db2", "k1"))
            .await
            .unwrap();
        assert!(db2.is_some());
    }
}
