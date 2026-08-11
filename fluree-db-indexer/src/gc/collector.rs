//! Garbage collection implementation
//!
//! Provides the `clean_garbage` function that walks the prev-index chain,
//! identifies gc-eligible indexes, and releases obsolete CAS artifacts.
//!
//! # GC semantics
//!
//! The garbage record in root N contains addresses of nodes that were replaced
//! when creating root N from root N-1. When we GC:
//! 1. Use root N's garbage manifest to delete nodes from root N-1
//! 2. Delete root N-1's garbage manifest
//! 3. Delete root N-1 itself (truncating the chain)
//!
//! This means GC operates on pairs: (newer root with manifest, older root to delete).
//!
//! Only branch-local artifacts are released here; dictionary blobs are shared
//! across a ledger's branches and are deferred to the storage sweep. See
//! [`partition_branch_local`] and the module docs on [`crate::gc`].

use super::{parse_garbage_record, CleanGarbageConfig, CleanGarbageResult};
use super::{DEFAULT_MAX_OLD_INDEXES, DEFAULT_MIN_TIME_GARBAGE_MINS};
use crate::error::Result;
use fluree_db_binary_index::IndexRoot;
use fluree_db_core::storage::ContentStore;
use fluree_db_core::ContentId;
use futures::stream::StreamExt;
use std::path::Path;

/// Concurrent storage releases per garbage manifest. Releases are independent
/// round trips, so a serial pass over a large manifest is almost entirely
/// latency.
const RELEASE_CONCURRENCY: usize = 32;

/// Entry in the prev-index chain.
pub(crate) struct IndexChainEntry {
    /// Transaction time of this index.
    pub(crate) t: i64,
    /// CID of this root blob.
    pub(crate) root_id: ContentId,
    /// CID of this root's garbage manifest (if any).
    pub(crate) garbage_id: Option<ContentId>,
    /// The decoded index root (already fetched during chain walk).
    pub(crate) root: IndexRoot,
}

/// Decode an index root blob (FIR6) and extract the GC-relevant fields.
///
/// Returns `(index_t, prev_index_id, garbage_id, decoded_root)`.
fn parse_chain_fields(
    bytes: &[u8],
) -> Result<(i64, Option<ContentId>, Option<ContentId>, IndexRoot)> {
    let root = IndexRoot::decode(bytes)
        .map_err(|e| crate::error::IndexerError::Serialization(format!("index root FIR6: {e}")))?;
    let prev_id = root.prev_index.as_ref().map(|p| p.id.clone());
    let garbage_id = root.garbage.as_ref().map(|g| g.id.clone());
    Ok((root.index_t, prev_id, garbage_id, root))
}

/// Split a garbage manifest's items into the branch-local CIDs this collector
/// may release, returning them with a count of the shared ones it skipped.
///
/// Dictionary blobs live in the ledger-wide `@shared/dicts/` namespace, so a
/// branch's manifest can name one that a sibling branch's index still
/// references — content addressing makes identical dictionary content produce
/// an identical CID, and a branch created from another starts out sharing it.
/// This collector walks one branch's chain and holds no exclusion over the
/// others, so it cannot establish that a shared blob is unreferenced.
///
/// Those blobs are left for the storage sweep, which unions the reachable set
/// across every branch while holding them all excluded. See
/// [`plan_sweep`](super::plan_sweep).
fn partition_branch_local(garbage: &[String]) -> (Vec<ContentId>, usize) {
    let mut branch_local = Vec::with_capacity(garbage.len());
    let mut shared = 0;

    for item in garbage {
        match item.parse::<ContentId>() {
            Ok(cid) if is_shared_across_branches(&cid) => shared += 1,
            Ok(cid) => branch_local.push(cid),
            Err(e) => tracing::warn!(
                item,
                error = %e,
                "Skipping unrecognized garbage item (not a valid CID)"
            ),
        }
    }

    (branch_local, shared)
}

/// Whether `id` addresses a blob in the ledger-wide namespace shared by every
/// branch, rather than one scoped to a single branch.
fn is_shared_across_branches(id: &ContentId) -> bool {
    id.codec() == fluree_db_core::CODEC_FLUREE_DICT_BLOB
}

/// Release every node listed in the garbage manifest at `garbage_id`.
///
/// Returns the number of nodes released, or `None` when the chain walk should
/// stop: an unreadable or unparseable manifest, or one still inside the
/// retention window. Manifests are consulted oldest-first, so one that is too
/// recent guarantees every later one is newer still.
async fn release_manifest_nodes(
    store: &dyn ContentStore,
    garbage_id: &ContentId,
    manifest_t: i64,
    cache_dir: Option<&Path>,
    now_ms: i64,
    min_age_ms: i64,
) -> Option<usize> {
    let bytes = match get_cached_or_remote(store, garbage_id, cache_dir).await {
        Ok(bytes) => bytes,
        Err(e) => {
            tracing::debug!(
                t = manifest_t,
                error = %e,
                "Failed to load garbage record (may already be released), stopping GC"
            );
            return None;
        }
    };

    let record = match parse_garbage_record(&bytes) {
        Ok(record) => record,
        Err(e) => {
            tracing::debug!(
                t = manifest_t,
                error = %e,
                "Failed to parse garbage record, stopping GC"
            );
            return None;
        }
    };

    // A record with no timestamp predates the `created_at_ms` field, so it was
    // written at least a release ago — past any retention window. Treating the
    // absent timestamp as "too recent" would stop the walk at that record on
    // every pass, pinning every version newer than it.
    if record.created_at_ms != 0 && now_ms - record.created_at_ms < min_age_ms {
        tracing::debug!(
            t = manifest_t,
            age_mins = (now_ms - record.created_at_ms) / 60000,
            "Garbage record too recent, stopping GC"
        );
        return None;
    }

    let release_started = std::time::Instant::now();
    tracing::debug!(
        t = manifest_t,
        %garbage_id,
        items = record.garbage.len(),
        "GC releasing garbage record items"
    );

    let (cids, shared): (Vec<ContentId>, usize) = partition_branch_local(&record.garbage);

    // Each release is an independent storage round trip. Stepping over absent
    // manifests and untimestamped records means the first pass after this fix
    // drains a backlog those two conditions had pinned, so a serial loop here
    // is almost entirely latency — held against a GC semaphore permit.
    let released = futures::stream::iter(cids)
        .map(|cid| async move {
            match store.release(&cid).await {
                Ok(()) => 1,
                Err(e) => {
                    tracing::debug!(
                        %cid,
                        error = %e,
                        "Failed to release garbage node (may already be released)"
                    );
                    0
                }
            }
        })
        .buffer_unordered(RELEASE_CONCURRENCY)
        .fold(0usize, |acc, n| async move { acc + n })
        .await;

    tracing::debug!(
        t = manifest_t,
        %garbage_id,
        released,
        elapsed_ms = release_started.elapsed().as_millis() as u64,
        deferred_to_sweep = shared,
        "GC garbage record item release pass complete"
    );

    Some(released)
}

/// Get current timestamp in milliseconds
fn current_timestamp_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Clean garbage from old index versions.
///
/// This function implements the expected GC semantics:
///
/// 1. Walks the prev-index chain to collect all index versions
/// 2. Retains `current + max_old_indexes` versions (e.g., max_old_indexes=5 keeps 6 total)
/// 3. For gc-eligible indexes, uses the newer root's garbage manifest to release nodes
/// 4. Releases the older root and its garbage manifest (truncating the chain)
///
/// # Retention Policy
///
/// Both thresholds must be satisfied for GC to occur:
/// - `max_old_indexes`: Maximum old index versions to keep (default: 5)
///   With max_old_indexes=5, we keep current + 5 old = 6 total
/// - `min_time_garbage_mins`: Minimum age before an index can be GC'd (default: 30)
///
/// Age is determined by the garbage record's `created_at_ms` field. A record
/// with no timestamp predates the field and is treated as past the window.
///
/// # Roots with no garbage manifest
///
/// A root written before the manifest write became unconditional can carry
/// none. The nodes it replaced cannot be named, so they are left in storage
/// for the sweep to reclaim, and the walk continues rather than stopping — one
/// such root would otherwise pin every version newer than it forever.
///
/// # Safety
///
/// This function is idempotent - running it multiple times is safe.
/// Chain walking is tolerant of missing roots (stops gracefully).
/// Already-released nodes are skipped without error.
pub async fn clean_garbage(
    store: &dyn ContentStore,
    current_root_id: &ContentId,
    config: CleanGarbageConfig,
) -> Result<CleanGarbageResult> {
    let max_old_indexes = config.max_old_indexes.unwrap_or(DEFAULT_MAX_OLD_INDEXES) as usize;
    let min_age_mins = config
        .min_time_garbage_mins
        .unwrap_or(DEFAULT_MIN_TIME_GARBAGE_MINS);
    let min_age_ms = min_age_mins as i64 * 60 * 1000;
    let now_ms = current_timestamp_ms();
    let started = std::time::Instant::now();

    // 1. Walk prev_index chain to collect all index versions (tolerant of missing roots)
    let index_chain = walk_prev_index_chain_cs_cached(
        store,
        current_root_id,
        config.artifact_cache_dir.as_deref(),
    )
    .await?;
    tracing::debug!(
        root_id = %current_root_id,
        chain_len = index_chain.len(),
        max_old_indexes,
        min_age_mins,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "GC prev-index chain walk complete"
    );

    // Retention: keep current + max_old_indexes
    // With max_old_indexes=5, keep_count=6 (indices 0..5)
    let keep_count = 1 + max_old_indexes;

    if index_chain.len() <= keep_count {
        // Not enough indexes to trigger GC
        return Ok(CleanGarbageResult::default());
    }

    // 2. Process ALL gc-eligible entries from oldest to newest.
    //
    // Chain is newest-first. Indices 0..keep_count are retained.
    // Indices keep_count..len are gc-eligible.
    //
    // For each gc-eligible entry at index i, the manifest at index i-1 (the
    // newer entry) lists nodes from entry i that were replaced. We use that
    // manifest to release those nodes, then release the entry's own garbage
    // manifest and root.
    //
    // Oldest-first processing (reversed range) is crash-safe: if interrupted,
    // remaining gc-eligible entries are still reachable via prev_index chain
    // from the retained set. Newest-first would truncate the chain at the
    // retention boundary, orphaning everything beyond.
    //
    // A read or retention failure breaks (not continues) because skipping an
    // entry and releasing a newer one would orphan the skipped entry and
    // everything older than it. A *missing* manifest is different: see the
    // `None` arm below.

    let mut deleted_count = 0;
    let mut indexes_cleaned = 0;
    let mut unnameable_indexes = 0;

    for i in (keep_count..index_chain.len()).rev() {
        let manifest_entry = &index_chain[i - 1];
        let entry_to_delete = &index_chain[i];

        // Manifest from the newer entry lists nodes from entry_to_delete
        // that were replaced when manifest_entry was built.
        match &manifest_entry.garbage_id {
            Some(garbage_id) => {
                match release_manifest_nodes(
                    store,
                    garbage_id,
                    manifest_entry.t,
                    config.artifact_cache_dir.as_deref(),
                    now_ms,
                    min_age_ms,
                )
                .await
                {
                    Some(released) => deleted_count += released,
                    None => break,
                }
            }
            // Roots written before the manifest write became unconditional can
            // carry none, and the nodes they replaced cannot be named from
            // here. Those nodes are left for the storage sweep rather than
            // stalling this walk permanently — a single such root would
            // otherwise pin every version newer than it. Releasing the
            // superseded root is still safe: the walk runs oldest-first, so
            // nothing older than it remains to orphan.
            None => {
                tracing::warn!(
                    t = manifest_entry.t,
                    superseded_t = entry_to_delete.t,
                    "index root has no garbage manifest; releasing the superseded root \
                     and leaving its replaced nodes for the storage sweep"
                );
                unnameable_indexes += 1;
            }
        }

        // Release entry_to_delete's own garbage manifest
        if let Some(ref old_garbage_id) = entry_to_delete.garbage_id {
            if let Err(e) = store.release(old_garbage_id).await {
                tracing::debug!(
                    cid = %old_garbage_id,
                    error = %e,
                    "Failed to release old garbage manifest (may already be released)"
                );
            }
        }

        // Release the old db-root
        if let Err(e) = store.release(&entry_to_delete.root_id).await {
            tracing::debug!(
                cid = %entry_to_delete.root_id,
                error = %e,
                "Failed to release old db-root (may already be released)"
            );
        } else {
            indexes_cleaned += 1;
        }
    }

    if indexes_cleaned > 0 || deleted_count > 0 {
        tracing::info!(
            indexes_cleaned = indexes_cleaned,
            nodes_deleted = deleted_count,
            unnameable_indexes = unnameable_indexes,
            retained_count = keep_count,
            "Garbage collection complete"
        );
    }

    Ok(CleanGarbageResult {
        indexes_cleaned,
        nodes_deleted: deleted_count,
    })
}

/// Walk the prev-index chain using `ContentStore::get` (CID-based).
///
/// Returns entries in order from newest to oldest.
///
/// **Tolerant behavior**: if a prev_index link points at a root that no longer
/// exists — the normal result of a prior GC truncating the chain — the walk
/// stops gracefully there rather than returning an error, which is what makes
/// GC idempotent.
///
/// A root that *does* exist but cannot be read propagates the error instead.
/// Returning a short chain would be indistinguishable from a genuinely short
/// one, and callers that decide which artifacts are unreferenced would treat
/// everything past the unreadable root as garbage.
pub(crate) async fn walk_prev_index_chain_cs(
    store: &dyn ContentStore,
    current_root_id: &ContentId,
) -> Result<Vec<IndexChainEntry>> {
    walk_prev_index_chain_cs_cached(store, current_root_id, None).await
}

async fn get_cached_or_remote(
    store: &dyn ContentStore,
    id: &ContentId,
    cache_dir: Option<&Path>,
) -> Result<Vec<u8>> {
    match cache_dir {
        Some(cache_dir) => Ok(
            fluree_db_binary_index::read::artifact_cache::fetch_cached_bytes_cid(
                store, id, cache_dir,
            )
            .await
            .map_err(|e| crate::error::IndexerError::StorageRead(e.to_string()))?,
        ),
        None => Ok(store.get(id).await?),
    }
}

pub(crate) async fn walk_prev_index_chain_cs_cached(
    store: &dyn ContentStore,
    current_root_id: &ContentId,
    cache_dir: Option<&Path>,
) -> Result<Vec<IndexChainEntry>> {
    let mut chain = Vec::new();
    let mut current_id = current_root_id.clone();

    loop {
        let read_started = std::time::Instant::now();
        let bytes = match get_cached_or_remote(store, &current_id, cache_dir).await {
            Ok(b) => b,
            Err(e) => {
                if chain.is_empty() {
                    return Err(e);
                }
                // A *released* root is the normal end of a walk: GC truncates
                // the chain from the oldest end, leaving the retained
                // boundary's prev_index dangling. Any other read failure is
                // not an ending — it is a root whose contents could not be
                // seen, and a caller deciding what is unreferenced would treat
                // everything beyond it as garbage. Distinguish by existence,
                // since the disk-cache path stringifies the error and loses
                // its kind. If existence cannot be established either, treat
                // the root as present and propagate.
                if store.has(&current_id).await.unwrap_or(true) {
                    return Err(e);
                }
                tracing::debug!(
                    root_id = %current_id,
                    "prev_index released by prior GC, chain ends here"
                );
                break;
            }
        };
        tracing::trace!(
            root_id = %current_id,
            bytes = bytes.len(),
            elapsed_ms = read_started.elapsed().as_millis() as u64,
            from_cache_enabled = cache_dir.is_some(),
            "GC loaded prev-index root"
        );

        let (t, prev_index_id, garbage_id, root) = parse_chain_fields(&bytes)?;

        let next_id = prev_index_id;
        chain.push(IndexChainEntry {
            t,
            root_id: current_id,
            garbage_id,
            root,
        });

        match next_id {
            Some(id) => current_id = id,
            None => break,
        }
    }

    Ok(chain)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::build::root_assembly::{encode_and_write_root_v6, Fir6Inputs};
    use crate::build::types::{UploadedDicts, UploadedIndexes};
    use crate::IndexStats;
    use fluree_db_binary_index::{
        BinaryGarbageRef, BinaryPrevIndexRef, DictPackRefs, DictRefs, DictTreeRefs, IndexRoot,
    };
    use fluree_db_core::prelude::*;
    use fluree_db_core::storage::content_store_for;
    use std::collections::{BTreeMap, HashMap};

    const LEDGER: &str = "test:main";

    /// Build a content store from MemoryStorage for testing.
    fn test_store(storage: &MemoryStorage) -> impl ContentStore + '_ {
        content_store_for(storage.clone(), LEDGER)
    }

    /// Build a minimal FIR6 root with the given t, prev_index, and garbage.
    fn minimal_fir6(
        t: i64,
        prev_index: Option<BinaryPrevIndexRef>,
        garbage: Option<BinaryGarbageRef>,
    ) -> Vec<u8> {
        crate::gc::test_support::minimal_fir6_for(
            LEDGER,
            t,
            prev_index,
            garbage,
            ContentId::new(ContentKind::IndexLeaf, b"dummy"),
        )
    }

    /// Helper: create a CID and its derived memory-storage address.
    fn cid_and_addr(kind: ContentKind, data: &[u8]) -> (ContentId, String) {
        crate::gc::test_support::cid_and_addr_for(LEDGER, kind, data)
    }

    #[test]
    fn test_current_timestamp_ms() {
        let ts = current_timestamp_ms();
        // Should be a reasonable timestamp (after year 2020)
        assert!(ts > 1_577_836_800_000); // Jan 1, 2020 in ms
    }

    #[test]
    fn test_parse_chain_fields_v3_cid() {
        // FIR6 root with prev_index and garbage set.
        let (prev_cid, _) = cid_and_addr(ContentKind::IndexRoot, b"prev");
        let (garb_cid, _) = cid_and_addr(ContentKind::GarbageRecord, b"garb");

        let bytes = minimal_fir6(
            5,
            Some(BinaryPrevIndexRef {
                t: 4,
                id: prev_cid.clone(),
            }),
            Some(BinaryGarbageRef {
                id: garb_cid.clone(),
            }),
        );
        let (t, prev, garbage, _) = parse_chain_fields(&bytes).unwrap();
        assert_eq!(t, 5);
        assert_eq!(prev, Some(prev_cid));
        assert_eq!(garbage, Some(garb_cid));
    }

    #[test]
    fn test_parse_chain_fields_minimal() {
        // FIR6 root without prev_index or garbage.
        let bytes = minimal_fir6(1, None, None);
        let (t, prev, garbage, _) = parse_chain_fields(&bytes).unwrap();
        assert_eq!(t, 1);
        assert_eq!(prev, None);
        assert_eq!(garbage, None);
    }

    #[tokio::test]
    async fn test_walk_empty_chain() {
        let storage = MemoryStorage::new();
        let (root_cid, root_addr) = cid_and_addr(ContentKind::IndexRoot, b"root1");

        let root_bytes = minimal_fir6(1, None, None);
        storage.write_bytes(&root_addr, &root_bytes).await.unwrap();

        let store = test_store(&storage);
        let chain = walk_prev_index_chain_cs(&store, &root_cid).await.unwrap();
        assert_eq!(chain.len(), 1);
        assert_eq!(chain[0].t, 1);
        assert_eq!(chain[0].root_id, root_cid);

        // Also verify clean_garbage with this chain (not enough to GC)
        let config = CleanGarbageConfig {
            max_old_indexes: Some(5),
            min_time_garbage_mins: Some(0),
            ..Default::default()
        };
        let result = clean_garbage(&store, &root_cid, config).await.unwrap();
        assert_eq!(result.indexes_cleaned, 0);
        assert_eq!(result.nodes_deleted, 0);
    }

    #[tokio::test]
    async fn test_walk_chain_fir6_format() {
        // Test chain walking with FIR6-encoded roots
        let storage = MemoryStorage::new();

        let (cid1, addr1) = cid_and_addr(ContentKind::IndexRoot, b"root1");
        let (cid2, addr2) = cid_and_addr(ContentKind::IndexRoot, b"root2");
        let (_, addr3) = cid_and_addr(ContentKind::IndexRoot, b"root3");

        let root1 = minimal_fir6(1, None, None);
        let root2 = minimal_fir6(
            2,
            Some(BinaryPrevIndexRef {
                t: 1,
                id: cid1.clone(),
            }),
            None,
        );
        let root3 = minimal_fir6(
            3,
            Some(BinaryPrevIndexRef {
                t: 2,
                id: cid2.clone(),
            }),
            None,
        );

        storage.write_bytes(&addr1, &root1).await.unwrap();
        storage.write_bytes(&addr2, &root2).await.unwrap();
        storage.write_bytes(&addr3, &root3).await.unwrap();

        let store = test_store(&storage);
        let (cid3, _) = cid_and_addr(ContentKind::IndexRoot, b"root3");
        let chain = walk_prev_index_chain_cs(&store, &cid3).await.unwrap();
        assert_eq!(chain.len(), 3);
        assert_eq!(chain[0].t, 3);
        assert_eq!(chain[1].t, 2);
        assert_eq!(chain[2].t, 1);
    }

    #[tokio::test]
    async fn test_walk_chain_tolerant_of_missing_prev() {
        let storage = MemoryStorage::new();

        let (missing_cid, _) = cid_and_addr(ContentKind::IndexRoot, b"missing");
        let (_, addr2) = cid_and_addr(ContentKind::IndexRoot, b"root2");

        let root2 = minimal_fir6(
            2,
            Some(BinaryPrevIndexRef {
                t: 1,
                id: missing_cid,
            }),
            None,
        );
        storage.write_bytes(&addr2, &root2).await.unwrap();

        let store = test_store(&storage);
        let (cid2, _) = cid_and_addr(ContentKind::IndexRoot, b"root2");
        let chain = walk_prev_index_chain_cs(&store, &cid2).await.unwrap();
        assert_eq!(chain.len(), 1);
        assert_eq!(chain[0].t, 2);
    }

    #[tokio::test]
    async fn test_clean_garbage_semantics() {
        // Test GC with FIR6-encoded roots and garbage items.
        let storage = MemoryStorage::new();

        let (cid1, addr1) = cid_and_addr(ContentKind::IndexRoot, b"root1");
        let (cid2, addr2) = cid_and_addr(ContentKind::IndexRoot, b"root2");
        let (cid3, addr3) = cid_and_addr(ContentKind::IndexRoot, b"root3");
        let (garb_cid1, garb_addr1) = cid_and_addr(ContentKind::GarbageRecord, b"garb1");
        let (garb_cid2, garb_addr2) = cid_and_addr(ContentKind::GarbageRecord, b"garb2");
        let (old_leaf_cid, old_leaf_addr) = cid_and_addr(ContentKind::IndexLeaf, b"old_leaf");

        let old_ts = current_timestamp_ms() - (60 * 60 * 1000);

        // t=1: oldest, has its own garbage manifest
        let root1 = minimal_fir6(
            1,
            None,
            Some(BinaryGarbageRef {
                id: garb_cid1.clone(),
            }),
        );

        // t=2: points to t=1, has garbage manifest (nodes replaced from t=1->t=2)
        let root2 = minimal_fir6(
            2,
            Some(BinaryPrevIndexRef {
                t: 1,
                id: cid1.clone(),
            }),
            Some(BinaryGarbageRef {
                id: garb_cid2.clone(),
            }),
        );

        // t=3: current, points to t=2
        let root3 = minimal_fir6(
            3,
            Some(BinaryPrevIndexRef {
                t: 2,
                id: cid2.clone(),
            }),
            None,
        );

        // Garbage record at t=2: CID strings of nodes replaced from t=1
        let garbage2 = format!(
            r#"{{"ledger_id": "{LEDGER}", "t": 2, "garbage": ["{old_leaf_cid}"], "created_at_ms": {old_ts}}}"#
        );

        // Garbage record at t=1 (empty, will be deleted with t=1)
        let garbage1 =
            format!(r#"{{"ledger_id": "{LEDGER}", "t": 1, "garbage": [], "created_at_ms": 0}}"#);

        storage.write_bytes(&addr1, &root1).await.unwrap();
        storage.write_bytes(&addr2, &root2).await.unwrap();
        storage.write_bytes(&addr3, &root3).await.unwrap();
        storage
            .write_bytes(&garb_addr2, garbage2.as_bytes())
            .await
            .unwrap();
        storage
            .write_bytes(&garb_addr1, garbage1.as_bytes())
            .await
            .unwrap();
        storage
            .write_bytes(&old_leaf_addr, b"old leaf data")
            .await
            .unwrap();

        let config = CleanGarbageConfig {
            max_old_indexes: Some(1),
            min_time_garbage_mins: Some(30),
            ..Default::default()
        };

        let store = test_store(&storage);
        let result = clean_garbage(&store, &cid3, config).await.unwrap();

        // Should clean 1 index (t=1) and delete 1 node (old_leaf)
        assert_eq!(result.indexes_cleaned, 1);
        assert_eq!(result.nodes_deleted, 1);

        // Old leaf deleted via CID->address resolution
        assert!(!store.has(&old_leaf_cid).await.unwrap());
        // t=1 root deleted
        assert!(!store.has(&cid1).await.unwrap());
        // t=1 garbage manifest deleted
        assert!(!store.has(&garb_cid1).await.unwrap());
        // t=2 and t=3 retained
        assert!(store.has(&cid2).await.unwrap());
        assert!(store.has(&cid3).await.unwrap());
        // t=2 garbage manifest retained
        assert!(store.has(&garb_cid2).await.unwrap());
    }

    #[tokio::test]
    async fn test_clean_garbage_respects_time_threshold() {
        let storage = MemoryStorage::new();

        let (cid1, addr1) = cid_and_addr(ContentKind::IndexRoot, b"root1");
        let (cid2, addr2) = cid_and_addr(ContentKind::IndexRoot, b"root2");
        let (cid3, addr3) = cid_and_addr(ContentKind::IndexRoot, b"root3");
        let (garb_cid2, garb_addr2) = cid_and_addr(ContentKind::GarbageRecord, b"garb2");

        // Recent timestamp (5 mins ago) -- NOT old enough
        let recent_ts = current_timestamp_ms() - (5 * 60 * 1000);

        let root1 = minimal_fir6(1, None, None);
        let root2 = minimal_fir6(
            2,
            Some(BinaryPrevIndexRef {
                t: 1,
                id: cid1.clone(),
            }),
            Some(BinaryGarbageRef {
                id: garb_cid2.clone(),
            }),
        );
        let root3 = minimal_fir6(
            3,
            Some(BinaryPrevIndexRef {
                t: 2,
                id: cid2.clone(),
            }),
            None,
        );

        let garbage2 = format!(
            r#"{{"ledger_id": "{LEDGER}", "t": 2, "garbage": ["old"], "created_at_ms": {recent_ts}}}"#
        );

        storage.write_bytes(&addr1, &root1).await.unwrap();
        storage.write_bytes(&addr2, &root2).await.unwrap();
        storage.write_bytes(&addr3, &root3).await.unwrap();
        storage
            .write_bytes(&garb_addr2, garbage2.as_bytes())
            .await
            .unwrap();

        let config = CleanGarbageConfig {
            max_old_indexes: Some(1),
            min_time_garbage_mins: Some(30),
            ..Default::default()
        };

        let store = test_store(&storage);
        let result = clean_garbage(&store, &cid3, config).await.unwrap();

        // Nothing cleaned -- garbage too recent
        assert_eq!(result.indexes_cleaned, 0);
        assert_eq!(result.nodes_deleted, 0);

        // All roots still exist
        assert!(store.has(&cid1).await.unwrap());
        assert!(store.has(&cid2).await.unwrap());
        assert!(store.has(&cid3).await.unwrap());
    }

    #[tokio::test]
    async fn test_clean_garbage_idempotent() {
        let storage = MemoryStorage::new();

        let (cid1, addr1) = cid_and_addr(ContentKind::IndexRoot, b"root1");
        let (cid2, addr2) = cid_and_addr(ContentKind::IndexRoot, b"root2");
        let (cid3, addr3) = cid_and_addr(ContentKind::IndexRoot, b"root3");
        let (garb_cid2, garb_addr2) = cid_and_addr(ContentKind::GarbageRecord, b"garb2");
        let (old_cid, old_addr) = cid_and_addr(ContentKind::IndexLeaf, b"old");

        let old_ts = current_timestamp_ms() - (60 * 60 * 1000);

        let root1 = minimal_fir6(1, None, None);
        let root2 = minimal_fir6(
            2,
            Some(BinaryPrevIndexRef {
                t: 1,
                id: cid1.clone(),
            }),
            Some(BinaryGarbageRef {
                id: garb_cid2.clone(),
            }),
        );
        let root3 = minimal_fir6(
            3,
            Some(BinaryPrevIndexRef {
                t: 2,
                id: cid2.clone(),
            }),
            None,
        );

        let garbage2 = format!(
            r#"{{"ledger_id": "{LEDGER}", "t": 2, "garbage": ["{old_cid}"], "created_at_ms": {old_ts}}}"#
        );

        storage.write_bytes(&addr1, &root1).await.unwrap();
        storage.write_bytes(&addr2, &root2).await.unwrap();
        storage.write_bytes(&addr3, &root3).await.unwrap();
        storage
            .write_bytes(&garb_addr2, garbage2.as_bytes())
            .await
            .unwrap();
        storage.write_bytes(&old_addr, b"old data").await.unwrap();

        let config = CleanGarbageConfig {
            max_old_indexes: Some(1),
            min_time_garbage_mins: Some(30),
            ..Default::default()
        };

        let store = test_store(&storage);

        // First GC run
        let result1 = clean_garbage(&store, &cid3, config.clone()).await.unwrap();
        assert_eq!(result1.indexes_cleaned, 1);
        assert!(!store.has(&cid1).await.unwrap());

        // Second GC run -- idempotent (chain is now t=3->t=2, only 2 entries <= keep=2)
        let result2 = clean_garbage(&store, &cid3, config).await.unwrap();
        assert_eq!(result2.indexes_cleaned, 0);
        assert_eq!(result2.nodes_deleted, 0);

        // t=2 and t=3 still exist
        assert!(store.has(&cid2).await.unwrap());
        assert!(store.has(&cid3).await.unwrap());
    }

    #[tokio::test]
    async fn test_clean_garbage_multi_delete() {
        // Chain: t=5->t=4->t=3->t=2->t=1, max_old_indexes=1, keep=2 (t=5, t=4)
        let storage = MemoryStorage::new();

        let (cid1, addr1) = cid_and_addr(ContentKind::IndexRoot, b"root1");
        let (cid2, addr2) = cid_and_addr(ContentKind::IndexRoot, b"root2");
        let (cid3, addr3) = cid_and_addr(ContentKind::IndexRoot, b"root3");
        let (cid4, addr4) = cid_and_addr(ContentKind::IndexRoot, b"root4");
        let (cid5, addr5) = cid_and_addr(ContentKind::IndexRoot, b"root5");
        let (garb_cid2, garb_addr2) = cid_and_addr(ContentKind::GarbageRecord, b"garb2");
        let (garb_cid3, garb_addr3) = cid_and_addr(ContentKind::GarbageRecord, b"garb3");
        let (garb_cid4, garb_addr4) = cid_and_addr(ContentKind::GarbageRecord, b"garb4");
        let (n1_cid, n1_addr) = cid_and_addr(ContentKind::IndexLeaf, b"node1");
        let (n2_cid, n2_addr) = cid_and_addr(ContentKind::IndexLeaf, b"node2");
        let (n3_cid, n3_addr) = cid_and_addr(ContentKind::IndexLeaf, b"node3");

        let old_ts = current_timestamp_ms() - (60 * 60 * 1000);

        let root1 = minimal_fir6(1, None, None);
        let root2 = minimal_fir6(
            2,
            Some(BinaryPrevIndexRef {
                t: 1,
                id: cid1.clone(),
            }),
            Some(BinaryGarbageRef {
                id: garb_cid2.clone(),
            }),
        );
        let root3 = minimal_fir6(
            3,
            Some(BinaryPrevIndexRef {
                t: 2,
                id: cid2.clone(),
            }),
            Some(BinaryGarbageRef {
                id: garb_cid3.clone(),
            }),
        );
        let root4 = minimal_fir6(
            4,
            Some(BinaryPrevIndexRef {
                t: 3,
                id: cid3.clone(),
            }),
            Some(BinaryGarbageRef {
                id: garb_cid4.clone(),
            }),
        );
        let root5 = minimal_fir6(
            5,
            Some(BinaryPrevIndexRef {
                t: 4,
                id: cid4.clone(),
            }),
            None,
        );

        let garbage2 = format!(
            r#"{{"ledger_id": "{LEDGER}", "t": 2, "garbage": ["{n1_cid}"], "created_at_ms": {old_ts}}}"#
        );
        let garbage3 = format!(
            r#"{{"ledger_id": "{LEDGER}", "t": 3, "garbage": ["{n2_cid}"], "created_at_ms": {old_ts}}}"#
        );
        let garbage4 = format!(
            r#"{{"ledger_id": "{LEDGER}", "t": 4, "garbage": ["{n3_cid}"], "created_at_ms": {old_ts}}}"#
        );

        storage.write_bytes(&addr1, &root1).await.unwrap();
        storage.write_bytes(&addr2, &root2).await.unwrap();
        storage.write_bytes(&addr3, &root3).await.unwrap();
        storage.write_bytes(&addr4, &root4).await.unwrap();
        storage.write_bytes(&addr5, &root5).await.unwrap();
        storage
            .write_bytes(&garb_addr2, garbage2.as_bytes())
            .await
            .unwrap();
        storage
            .write_bytes(&garb_addr3, garbage3.as_bytes())
            .await
            .unwrap();
        storage
            .write_bytes(&garb_addr4, garbage4.as_bytes())
            .await
            .unwrap();
        storage.write_bytes(&n1_addr, b"n1").await.unwrap();
        storage.write_bytes(&n2_addr, b"n2").await.unwrap();
        storage.write_bytes(&n3_addr, b"n3").await.unwrap();

        let config = CleanGarbageConfig {
            max_old_indexes: Some(1),
            min_time_garbage_mins: Some(30),
            ..Default::default()
        };

        let store = test_store(&storage);
        let result = clean_garbage(&store, &cid5, config).await.unwrap();

        assert_eq!(result.indexes_cleaned, 3);
        assert_eq!(result.nodes_deleted, 3);

        // GC-eligible roots deleted
        assert!(!store.has(&cid1).await.unwrap());
        assert!(!store.has(&cid2).await.unwrap());
        assert!(!store.has(&cid3).await.unwrap());
        // Nodes deleted
        assert!(!store.has(&n1_cid).await.unwrap());
        assert!(!store.has(&n2_cid).await.unwrap());
        assert!(!store.has(&n3_cid).await.unwrap());
        // Retained
        assert!(store.has(&cid4).await.unwrap());
        assert!(store.has(&cid5).await.unwrap());
        assert!(store.has(&garb_cid4).await.unwrap());
    }

    /// End-to-end: simulates an incremental index update that replaces leaf,
    /// branch, and dict CIDs, publishes a new root with garbage manifest,
    /// then verifies clean_garbage deletes exactly those replaced artifacts
    /// after the retention period.
    #[tokio::test]
    async fn test_incremental_gc_deletes_replaced_artifacts() {
        let storage = MemoryStorage::new();

        // --- Artifacts from the ORIGINAL (base) index at t=5 ---
        // These are the CAS blobs that get replaced during incremental update.
        let (old_leaf_spot_0, old_leaf_spot_0_addr) =
            cid_and_addr(ContentKind::IndexLeaf, b"spot-leaf-0-old");
        let (old_leaf_spot_1, old_leaf_spot_1_addr) =
            cid_and_addr(ContentKind::IndexLeaf, b"spot-leaf-1-old");
        let (old_branch_g1, old_branch_g1_addr) =
            cid_and_addr(ContentKind::IndexBranch, b"branch-g1-old");
        let (old_rev_branch, old_rev_branch_addr) =
            cid_and_addr(ContentKind::IndexLeaf, b"subj-rev-branch-old");
        let (old_rev_leaf, old_rev_leaf_addr) =
            cid_and_addr(ContentKind::IndexLeaf, b"subj-rev-leaf-old");

        // Write old artifacts to storage (they exist in CAS)
        storage
            .write_bytes(&old_leaf_spot_0_addr, b"old spot leaf 0")
            .await
            .unwrap();
        storage
            .write_bytes(&old_leaf_spot_1_addr, b"old spot leaf 1")
            .await
            .unwrap();
        storage
            .write_bytes(&old_branch_g1_addr, b"old g1 branch")
            .await
            .unwrap();
        storage
            .write_bytes(&old_rev_branch_addr, b"old rev branch")
            .await
            .unwrap();
        storage
            .write_bytes(&old_rev_leaf_addr, b"old rev leaf")
            .await
            .unwrap();

        // --- Base root at t=5 (the index before incremental update) ---
        let (base_root_cid, base_root_addr) = cid_and_addr(ContentKind::IndexRoot, b"root-t5");
        let base_root_bytes = minimal_fir6(5, None, None);
        storage
            .write_bytes(&base_root_addr, &base_root_bytes)
            .await
            .unwrap();

        // --- Incremental update produces new root at t=10 ---
        // The pipeline accumulated these replaced CIDs:
        let replaced_cids = [
            old_leaf_spot_0.clone(),
            old_leaf_spot_1.clone(),
            old_branch_g1.clone(),
            old_rev_branch.clone(),
            old_rev_leaf.clone(),
        ];

        // Write garbage manifest (as the pipeline does via write_garbage_record)
        let old_ts = current_timestamp_ms() - (60 * 60 * 1000); // 1 hour ago
        let garbage_items: Vec<String> = replaced_cids
            .iter()
            .map(std::string::ToString::to_string)
            .collect();
        let garbage_json = format!(
            r#"{{"ledger_id": "{}", "t": 10, "garbage": [{}], "created_at_ms": {}}}"#,
            LEDGER,
            garbage_items
                .iter()
                .map(|s| format!("\"{s}\""))
                .collect::<Vec<_>>()
                .join(","),
            old_ts
        );
        let (garb_cid, garb_addr) = cid_and_addr(ContentKind::GarbageRecord, b"garb-t10");
        storage
            .write_bytes(&garb_addr, garbage_json.as_bytes())
            .await
            .unwrap();

        // New root at t=10: prev_index → base root, garbage → manifest
        let (new_root_cid, new_root_addr) = cid_and_addr(ContentKind::IndexRoot, b"root-t10");
        let new_root_bytes = minimal_fir6(
            10,
            Some(BinaryPrevIndexRef {
                t: 5,
                id: base_root_cid.clone(),
            }),
            Some(BinaryGarbageRef {
                id: garb_cid.clone(),
            }),
        );
        storage
            .write_bytes(&new_root_addr, &new_root_bytes)
            .await
            .unwrap();

        let store = test_store(&storage);

        // --- Before GC: all artifacts exist ---
        assert!(store.has(&old_leaf_spot_0).await.unwrap());
        assert!(store.has(&old_leaf_spot_1).await.unwrap());
        assert!(store.has(&old_branch_g1).await.unwrap());
        assert!(store.has(&old_rev_branch).await.unwrap());
        assert!(store.has(&old_rev_leaf).await.unwrap());
        assert!(store.has(&base_root_cid).await.unwrap());

        // --- Run GC: max_old_indexes=0 means only keep current ---
        let config = CleanGarbageConfig {
            max_old_indexes: Some(0),
            min_time_garbage_mins: Some(30),
            ..Default::default()
        };
        let result = clean_garbage(&store, &new_root_cid, config).await.unwrap();

        // Should delete 1 old index (t=5) and 5 replaced artifacts
        assert_eq!(result.indexes_cleaned, 1);
        assert_eq!(result.nodes_deleted, 5);

        // --- All replaced artifacts deleted ---
        assert!(!store.has(&old_leaf_spot_0).await.unwrap());
        assert!(!store.has(&old_leaf_spot_1).await.unwrap());
        assert!(!store.has(&old_branch_g1).await.unwrap());
        assert!(!store.has(&old_rev_branch).await.unwrap());
        assert!(!store.has(&old_rev_leaf).await.unwrap());

        // --- Old root deleted ---
        assert!(!store.has(&base_root_cid).await.unwrap());

        // --- Current root + its garbage manifest retained ---
        assert!(store.has(&new_root_cid).await.unwrap());
        assert!(store.has(&garb_cid).await.unwrap());
    }

    /// The minimal `Fir6Inputs` a rebuild publishes: no dict packs, no graph
    /// data, and `prev_index` as the version this root supersedes.
    fn minimal_fir6_inputs(t: i64, prev_index: Option<BinaryPrevIndexRef>) -> Fir6Inputs {
        let dummy_cid = ContentId::new(ContentKind::IndexLeaf, b"dummy");
        let dummy_tree = DictTreeRefs {
            branch: dummy_cid,
            leaves: Vec::new(),
        };
        Fir6Inputs {
            ledger_id: LEDGER.to_string(),
            index_t: t,
            namespace_codes: BTreeMap::new(),
            commit_derived_ns: HashMap::new(),
            ns_split_mode: fluree_db_core::ns_encoding::NsSplitMode::default(),
            predicate_sids: Vec::new(),
            uploaded_dicts: UploadedDicts {
                dict_refs: DictRefs {
                    forward_packs: DictPackRefs {
                        string_fwd_packs: Vec::new(),
                        subject_fwd_ns_packs: Vec::new(),
                    },
                    subject_reverse: dummy_tree.clone(),
                    string_reverse: dummy_tree,
                },
                subject_id_encoding: fluree_db_core::SubjectIdEncoding::Narrow,
                subject_watermarks: Vec::new(),
                string_watermark: 0,
                graph_iris: Vec::new(),
                datatype_iris: Vec::new(),
                language_tags: Vec::new(),
                numbig: BTreeMap::new(),
                vectors: BTreeMap::new(),
            },
            v3_uploaded: UploadedIndexes {
                default_graph_orders: Vec::new(),
                named_graphs: Vec::new(),
            },
            graph_arenas: Vec::new(),
            datatype_iris: Vec::new(),
            language_tags: Vec::new(),
            total_commit_size: 0,
            total_asserts: 0,
            total_retracts: 0,
            db_stats: None,
            db_schema: None,
            sketch_ref: None,
            attachment_events: None,
            prev_index,
        }
    }

    /// Roots in a synthetic chain that deviate from what the current write
    /// path produces, identified by `t`.
    #[derive(Default)]
    struct LegacyRoots {
        /// Published with no garbage manifest at all.
        no_manifest: &'static [i64],
        /// Manifest written before the `created_at_ms` field existed.
        no_timestamp: &'static [i64],
    }

    /// Write a linked chain of index roots `t=1..=len`. Each root at `t > 1`
    /// carries a garbage manifest listing the leaf blob its predecessor
    /// referenced, aged past any retention threshold, except where `legacy`
    /// says otherwise.
    ///
    /// Returns the root CIDs and the superseded leaf CIDs, each indexed by
    /// `t - 1`.
    async fn write_linked_chain(
        storage: &MemoryStorage,
        len: i64,
        legacy: LegacyRoots,
    ) -> (Vec<ContentId>, Vec<ContentId>) {
        let aged_ts = current_timestamp_ms() - (60 * 60 * 1000);
        let mut root_cids: Vec<ContentId> = Vec::new();
        let mut leaf_cids: Vec<ContentId> = Vec::new();

        for t in 1..=len {
            let (leaf_cid, leaf_addr) =
                cid_and_addr(ContentKind::IndexLeaf, format!("leaf-{t}").as_bytes());
            storage
                .write_bytes(&leaf_addr, b"superseded leaf")
                .await
                .unwrap();
            leaf_cids.push(leaf_cid);

            let garbage = if t > 1 && !legacy.no_manifest.contains(&t) {
                let (garb_cid, garb_addr) =
                    cid_and_addr(ContentKind::GarbageRecord, format!("garb-{t}").as_bytes());
                let superseded = &leaf_cids[(t - 2) as usize];
                let created_at_ms = if legacy.no_timestamp.contains(&t) {
                    0
                } else {
                    aged_ts
                };
                let json = format!(
                    r#"{{"ledger_id": "{LEDGER}", "t": {t}, "garbage": ["{superseded}"], "created_at_ms": {created_at_ms}}}"#
                );
                storage
                    .write_bytes(&garb_addr, json.as_bytes())
                    .await
                    .unwrap();
                Some(BinaryGarbageRef { id: garb_cid })
            } else {
                None
            };

            let prev_index = root_cids.last().map(|id| BinaryPrevIndexRef {
                t: t - 1,
                id: id.clone(),
            });
            let (root_cid, root_addr) =
                cid_and_addr(ContentKind::IndexRoot, format!("root-{t}").as_bytes());
            storage
                .write_bytes(&root_addr, &minimal_fir6(t, prev_index, garbage))
                .await
                .unwrap();
            root_cids.push(root_cid);
        }

        (root_cids, leaf_cids)
    }

    /// A rebuild publishes through `encode_and_write_root_v6` with no
    /// `GarbageContext`, so the prior index head reaches the new root only via
    /// `Fir6Inputs::prev_index`. Dropping it severs the prev-index chain: a
    /// later chain walk stops at this root and every earlier version is
    /// orphaned beyond GC's reach (#1548).
    #[tokio::test]
    async fn rebuild_root_links_prior_index_chain() {
        let storage = MemoryStorage::new();
        let store = test_store(&storage);
        let (prior_head, _) = cid_and_addr(ContentKind::IndexRoot, b"prior-head");

        let result = encode_and_write_root_v6(
            &store,
            minimal_fir6_inputs(
                9,
                Some(BinaryPrevIndexRef {
                    t: 8,
                    id: prior_head.clone(),
                }),
            ),
            IndexStats::default(),
        )
        .await
        .unwrap();

        let published = IndexRoot::decode(&store.get(&result.root_id).await.unwrap()).unwrap();
        assert_eq!(
            published.prev_index.map(|p| p.id),
            Some(prior_head),
            "a rebuild root must link the prior index head; without it the chain \
             walk terminates here and every earlier root is unreachable"
        );
    }

    /// A rebuild supersedes the entire prior index, so its published root must
    /// carry a garbage manifest naming what it replaced. Without one the root
    /// becomes an absorbing barrier as soon as the chain grows past it (#1548).
    #[tokio::test]
    async fn rebuild_root_carries_garbage_manifest() {
        let storage = MemoryStorage::new();
        let (root_cids, _) = write_linked_chain(&storage, 2, LegacyRoots::default()).await;
        let store = test_store(&storage);

        let result = encode_and_write_root_v6(
            &store,
            minimal_fir6_inputs(
                3,
                Some(BinaryPrevIndexRef {
                    t: 2,
                    id: root_cids[1].clone(),
                }),
            ),
            IndexStats::default(),
        )
        .await
        .unwrap();

        let published = IndexRoot::decode(&store.get(&result.root_id).await.unwrap()).unwrap();
        let garbage_id = published
            .garbage
            .expect("rebuild root must carry a garbage manifest")
            .id;
        let record = parse_garbage_record(&store.get(&garbage_id).await.unwrap())
            .expect("manifest must be readable");
        assert_eq!(record.t, 3, "manifest belongs to the root that wrote it");
    }

    /// When the prior root cannot be expanded, what it superseded is unknown.
    /// The manifest must then be absent rather than empty: an empty one claims
    /// the rebuild replaced nothing, which would let GC release the prior root
    /// while leaving behind every blob it referenced (#1548).
    #[tokio::test]
    async fn rebuild_omits_manifest_when_prior_root_unreadable() {
        let storage = MemoryStorage::new();
        let store = test_store(&storage);
        // Never written to storage, so its reachable set cannot be computed.
        let (unreadable, _) = cid_and_addr(ContentKind::IndexRoot, b"absent-prior-root");

        let result = encode_and_write_root_v6(
            &store,
            minimal_fir6_inputs(
                3,
                Some(BinaryPrevIndexRef {
                    t: 2,
                    id: unreadable.clone(),
                }),
            ),
            IndexStats::default(),
        )
        .await
        .unwrap();

        let published = IndexRoot::decode(&store.get(&result.root_id).await.unwrap()).unwrap();
        assert!(
            published.garbage.is_none(),
            "an undeterminable garbage set must not be recorded as an empty one"
        );
        assert_eq!(
            published.prev_index.map(|p| p.id),
            Some(unreadable),
            "the chain link is still published so the walk stays connected"
        );
    }

    /// Reindex over an existing chain, then GC. The consolidated root
    /// supersedes the whole prior chain, so everything past the retention
    /// window must become collectable (#1548).
    #[tokio::test]
    async fn reindex_publish_leaves_superseded_roots_collectable() {
        let storage = MemoryStorage::new();
        let (root_cids, leaf_cids) = write_linked_chain(&storage, 3, LegacyRoots::default()).await;
        let store = test_store(&storage);

        let reindexed = encode_and_write_root_v6(
            &store,
            minimal_fir6_inputs(
                4,
                Some(BinaryPrevIndexRef {
                    t: 3,
                    id: root_cids[2].clone(),
                }),
            ),
            IndexStats::default(),
        )
        .await
        .unwrap();

        let config = CleanGarbageConfig {
            max_old_indexes: Some(1),
            min_time_garbage_mins: Some(30),
            ..Default::default()
        };
        let result = clean_garbage(&store, &reindexed.root_id, config)
            .await
            .unwrap();

        assert_eq!(
            result.indexes_cleaned, 2,
            "t=1 and t=2 are past the retention window and must be released"
        );
        assert!(
            !store.has(&root_cids[0]).await.unwrap(),
            "t=1 root released"
        );
        assert!(
            !store.has(&root_cids[1]).await.unwrap(),
            "t=2 root released"
        );
        assert!(
            !store.has(&leaf_cids[0]).await.unwrap(),
            "t=1 leaf released"
        );
        assert!(
            !store.has(&leaf_cids[1]).await.unwrap(),
            "t=2 leaf released"
        );
        assert!(store.has(&root_cids[2]).await.unwrap(), "t=3 retained");
        assert!(store.has(&reindexed.root_id).await.unwrap(), "current root");
    }

    /// Retention promises `current + max_old_indexes` versions. A root
    /// published without a garbage manifest stops the collector's oldest-first
    /// walk, so every version newer than the gap is retained forever and the
    /// chain grows without bound (#1548).
    #[tokio::test]
    async fn gc_bounds_retained_chain_despite_manifest_gap() {
        let storage = MemoryStorage::new();
        let (root_cids, _) = write_linked_chain(
            &storage,
            6,
            LegacyRoots {
                no_manifest: &[4],
                ..Default::default()
            },
        )
        .await;
        let store = test_store(&storage);

        let max_old_indexes = 1;
        let config = CleanGarbageConfig {
            max_old_indexes: Some(max_old_indexes),
            min_time_garbage_mins: Some(30),
            ..Default::default()
        };
        clean_garbage(&store, &root_cids[5], config).await.unwrap();

        let mut retained = Vec::new();
        for (i, cid) in root_cids.iter().enumerate() {
            if store.has(cid).await.unwrap() {
                retained.push(i + 1);
            }
        }

        assert!(
            retained.len() <= 1 + max_old_indexes as usize,
            "retention keeps current + {max_old_indexes} old roots, but t={retained:?} survive"
        );
    }

    /// A branch's manifest can name a dictionary blob a sibling branch still
    /// references — dictionaries live in the ledger-wide `@shared/dicts/`
    /// namespace, and content addressing makes identical content share a CID.
    /// The collector walks one branch and holds no exclusion over the others,
    /// so it must leave those for the sweep rather than release them (#1548).
    #[tokio::test]
    async fn shared_dictionary_blobs_are_left_for_the_sweep() {
        use fluree_db_core::content_kind::DictKind;

        let storage = MemoryStorage::new();
        let aged_ts = current_timestamp_ms() - (60 * 60 * 1000);

        let (root1, addr1) = cid_and_addr(ContentKind::IndexRoot, b"root1");
        let (root2, addr2) = cid_and_addr(ContentKind::IndexRoot, b"root2");
        let (root3, addr3) = cid_and_addr(ContentKind::IndexRoot, b"root3");
        let (garb2, garb2_addr) = cid_and_addr(ContentKind::GarbageRecord, b"garb2");

        // One branch-local leaf and one shared dictionary, both superseded.
        let (leaf, leaf_addr) = cid_and_addr(ContentKind::IndexLeaf, b"superseded-leaf");
        let (dict, dict_addr) = cid_and_addr(
            ContentKind::DictBlob {
                dict: DictKind::SubjectReverse,
            },
            b"superseded-dict",
        );
        storage.write_bytes(&leaf_addr, b"leaf").await.unwrap();
        storage.write_bytes(&dict_addr, b"dict").await.unwrap();

        storage
            .write_bytes(&addr1, &minimal_fir6(1, None, None))
            .await
            .unwrap();
        storage
            .write_bytes(
                &addr2,
                &minimal_fir6(
                    2,
                    Some(BinaryPrevIndexRef {
                        t: 1,
                        id: root1.clone(),
                    }),
                    Some(BinaryGarbageRef { id: garb2.clone() }),
                ),
            )
            .await
            .unwrap();
        storage
            .write_bytes(
                &addr3,
                &minimal_fir6(
                    3,
                    Some(BinaryPrevIndexRef {
                        t: 2,
                        id: root2.clone(),
                    }),
                    None,
                ),
            )
            .await
            .unwrap();
        storage
            .write_bytes(
                &garb2_addr,
                format!(
                    r#"{{"ledger_id": "{LEDGER}", "t": 2, "garbage": ["{leaf}", "{dict}"], "created_at_ms": {aged_ts}}}"#
                )
                .as_bytes(),
            )
            .await
            .unwrap();

        let store = test_store(&storage);
        let config = CleanGarbageConfig {
            max_old_indexes: Some(1),
            min_time_garbage_mins: Some(30),
            ..Default::default()
        };
        let result = clean_garbage(&store, &root3, config).await.unwrap();

        assert_eq!(
            result.nodes_deleted, 1,
            "only the branch-local leaf is released"
        );
        assert!(!store.has(&leaf).await.unwrap(), "the leaf is reclaimed");
        assert!(
            store.has(&dict).await.unwrap(),
            "the shared dictionary survives; a sibling branch may still reference it"
        );
    }

    /// A manifest with no `created_at_ms` predates the field, so its nodes are
    /// long past any retention window. Reading the absent timestamp as "too
    /// recent" stopped the walk at that record on every pass, pinning every
    /// version newer than it (#1548).
    #[tokio::test]
    async fn manifest_without_timestamp_does_not_stop_gc() {
        let storage = MemoryStorage::new();
        // t=2's manifest predates the field; t=3's and t=4's are current.
        let (root_cids, leaf_cids) = write_linked_chain(
            &storage,
            4,
            LegacyRoots {
                no_timestamp: &[2],
                ..Default::default()
            },
        )
        .await;
        let store = test_store(&storage);

        let config = CleanGarbageConfig {
            max_old_indexes: Some(1),
            min_time_garbage_mins: Some(30),
            ..Default::default()
        };
        let result = clean_garbage(&store, &root_cids[3], config).await.unwrap();

        assert_eq!(
            result.indexes_cleaned, 2,
            "t=1 and t=2 are past retention and must be released"
        );
        assert!(
            !store.has(&leaf_cids[0]).await.unwrap(),
            "the pre-timestamp manifest still names t=1's superseded leaf"
        );
        assert!(
            !store.has(&leaf_cids[1]).await.unwrap(),
            "the walk continues past it to t=2's manifest"
        );
    }

    /// Stepping over a manifest gap strands exactly the nodes that gap would
    /// have named — one build's worth — and nothing more. The storage sweep
    /// reclaims them; every other superseded node is still released here.
    #[tokio::test]
    async fn manifest_gap_strands_only_its_own_step() {
        let storage = MemoryStorage::new();
        // The manifest at t=4 would have named t=3's superseded leaf.
        let (root_cids, leaf_cids) = write_linked_chain(
            &storage,
            6,
            LegacyRoots {
                no_manifest: &[4],
                ..Default::default()
            },
        )
        .await;
        let store = test_store(&storage);

        let config = CleanGarbageConfig {
            max_old_indexes: Some(1),
            min_time_garbage_mins: Some(30),
            ..Default::default()
        };
        let result = clean_garbage(&store, &root_cids[5], config).await.unwrap();

        assert_eq!(
            result.indexes_cleaned, 4,
            "the gap must not stop the walk: t=1..4 are all past retention"
        );
        assert_eq!(
            result.nodes_deleted, 3,
            "every superseded node except the gap step's is released"
        );

        assert!(
            store.has(&leaf_cids[2]).await.unwrap(),
            "t=3's superseded leaf is unnameable and waits for the sweep"
        );
        for (t, leaf) in [(1, &leaf_cids[0]), (2, &leaf_cids[1]), (4, &leaf_cids[3])] {
            assert!(
                !store.has(leaf).await.unwrap(),
                "t={t}'s superseded leaf is named by a manifest and must be released"
            );
        }
    }
}
