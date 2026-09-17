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
//! A pass plans first and releases second: every eligible manifest is read
//! oldest-first, then the nodes they name go out in batches, then the
//! superseded manifests and roots, oldest-first in bounded chunks. See
//! [`clean_garbage`] for why that order is crash-safe.
//!
//! Dictionary blobs are shared across a ledger's branches and are released
//! only as the pass's [`SharedBlobPolicy`] allows. See [`partition_nodes`]
//! and the module docs on [`crate::gc`].

use super::{parse_garbage_record, CleanGarbageConfig, CleanGarbageResult, SharedBlobPolicy};
use super::{DEFAULT_MAX_OLD_INDEXES, DEFAULT_MIN_TIME_GARBAGE_MINS};
use crate::error::Result;
use fluree_db_binary_index::IndexRoot;
use fluree_db_core::storage::ContentStore;
use fluree_db_core::ContentId;
use futures::stream::StreamExt;
use std::collections::HashSet;
use std::path::Path;

/// CIDs per batch release. Matches the S3 `DeleteObjects` maximum, so on an
/// object store a batch is one request.
const RELEASE_BATCH: usize = 1000;

/// Batches in flight at once. Small on purpose: the S3 backend caps requests
/// process-wide, and a pass that took most of that cap starved every reader
/// for as long as it ran.
const RELEASE_CONCURRENCY: usize = 4;

/// Superseded roots, with their manifests, released per chunk. Bounds what a
/// crash mid-chunk can strand; see the ordering note in [`clean_garbage`].
const ROOT_RELEASE_CHUNK: usize = 64;

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

/// How the nodes a pass's manifests name split under its policy.
struct NodePartition {
    /// Everything the pass releases: branch-local artifacts plus the shared
    /// blobs the policy admits. Sorted and deduplicated.
    release: Vec<ContentId>,
    /// Shared blobs in `release`.
    shared_released: usize,
    /// Shared blobs the manifests named that stay in storage.
    shared_deferred: usize,
    /// Items a manifest named as garbage that a root this pass still
    /// retains references directly. See [`retained_refs`] for why a CID a
    /// manifest named can be live again.
    resurrected: usize,
}

/// Every CAS id directly referenced by a root this pass retains —
/// `index_chain[..keep_count]`, newest-first, so the roots that survive the
/// pass and everything a query against any of them can still read.
///
/// A manifest names a CID as garbage relative to the *one* root it replaced
/// it in. Content addressing does not know that: two builds that happen to
/// produce byte-identical output get the same CID, whatever the manifests in
/// between said about it. Reverse-dictionary leaves hit this routinely under
/// a monotonic key pattern (ULID, UUIDv7, sequential ids) — a leaf that
/// receives one new entry per build is re-hashed and its old CID garbaged
/// every build, and the half a later split keeps is byte-identical to one of
/// those earlier states, reviving a CID an already-consumed manifest named.
/// Checked directly against `all_cas_ids()` rather than expanded, since a
/// resurrected CID this pass must not delete is exactly one still directly
/// reachable from a surviving root — nothing behind a named-graph or
/// annotation branch manifest changes that.
fn retained_refs(
    index_chain: &[IndexChainEntry],
    keep_count: usize,
) -> std::collections::HashSet<ContentId> {
    index_chain[..keep_count.min(index_chain.len())]
        .iter()
        .flat_map(|entry| entry.root.all_cas_ids())
        .collect()
}

/// Split manifest items into what this pass releases and what it leaves.
///
/// A CID [`retained_refs`] reports live is skipped regardless of policy —
/// see its doc for why a garbage-named CID can still be needed. Otherwise,
/// branch-local artifacts are always released. A dictionary blob is released
/// under [`SharedBlobPolicy::Release`] unless a sibling branch still reaches
/// it, and never under [`SharedBlobPolicy::Defer`]. See the module docs on
/// [`crate::gc`] for why the sibling set decides.
fn partition_nodes(
    items: &[String],
    policy: &SharedBlobPolicy,
    retained: &std::collections::HashSet<ContentId>,
) -> NodePartition {
    let mut out = NodePartition {
        release: Vec::with_capacity(items.len()),
        shared_released: 0,
        shared_deferred: 0,
        resurrected: 0,
    };
    for item in items {
        let cid = match item.parse::<ContentId>() {
            Ok(cid) => cid,
            Err(e) => {
                tracing::warn!(
                    item,
                    error = %e,
                    "Skipping unrecognized garbage item (not a valid CID)"
                );
                continue;
            }
        };
        if retained.contains(&cid) {
            out.resurrected += 1;
            tracing::debug!(
                %cid,
                "garbage-named CID is still referenced by a retained root; not releasing"
            );
            continue;
        }
        if !is_shared_across_branches(&cid) {
            out.release.push(cid);
            continue;
        }
        match policy {
            SharedBlobPolicy::Defer => out.shared_deferred += 1,
            SharedBlobPolicy::Release {
                referenced_elsewhere,
            } => {
                if referenced_elsewhere.contains(&cid) {
                    out.shared_deferred += 1;
                } else {
                    out.shared_released += 1;
                    out.release.push(cid);
                }
            }
        }
    }
    out.release.sort();
    out.release.dedup();
    out
}

/// Whether `id` addresses a blob in the ledger-wide namespace shared by every
/// branch, rather than one scoped to a single branch.
pub(crate) fn is_shared_across_branches(id: &ContentId) -> bool {
    id.codec() == fluree_db_core::CODEC_FLUREE_DICT_BLOB
}

/// The items the garbage manifest at `garbage_id` names.
///
/// Returns `None` when the chain walk should stop: an unreadable or
/// unparseable manifest, or one still inside the retention window.
/// Manifests are consulted oldest-first, so one that is too recent
/// guarantees every later one is newer still.
async fn load_manifest_nodes(
    store: &dyn ContentStore,
    garbage_id: &ContentId,
    manifest_t: i64,
    cache_dir: Option<&Path>,
    now_ms: i64,
    min_age_ms: i64,
) -> Option<Vec<String>> {
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

    tracing::debug!(
        t = manifest_t,
        %garbage_id,
        items = record.garbage.len(),
        "GC manifest loaded"
    );
    Some(record.garbage)
}

/// Release `ids` in batches, returning the ones that failed.
///
/// A failure is logged and skipped: the blob stays in storage, the manifest
/// that named it is released with its root regardless, and the sweep is
/// what reclaims it later. The per-item loop this replaced behaved the same
/// way.
async fn release_batched(
    store: &dyn ContentStore,
    ids: &[ContentId],
    what: &'static str,
) -> Vec<(ContentId, fluree_db_core::Error)> {
    // Futures built up front rather than in a stream closure: a closure over
    // the chunk borrow trips the compiler's higher-ranked lifetime check, and
    // the sweep's branch walks take the same shape for the same reason.
    let batches: Vec<_> = ids
        .chunks(RELEASE_BATCH)
        .map(|chunk| store.release_many(chunk))
        .collect();
    let failures: Vec<(ContentId, fluree_db_core::Error)> = futures::stream::iter(batches)
        .buffer_unordered(RELEASE_CONCURRENCY)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .flatten()
        .collect();
    for (cid, error) in &failures {
        tracing::debug!(
            %cid,
            error = %error,
            what,
            "Failed to release (may already be released)"
        );
    }
    failures
}

/// Get current timestamp in milliseconds
fn current_timestamp_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// A superseded version the pass has decided to release.
struct PlannedRelease {
    root_id: ContentId,
    garbage_id: Option<ContentId>,
    past_ceiling: bool,
}

/// What a pass has decided to release, before anything is released.
///
/// Planning reads a snapshot of the chain and takes as long as the chain is
/// deep; [`release`](Self::release) is the only step that can lose data, and
/// it is short. Splitting them lets a caller keep index builds out of the
/// release step alone — see [`release`](Self::release) for why that matters.
pub struct GarbagePlan {
    /// The head the chain snapshot was walked from.
    snapshot_head: ContentId,
    planned: Vec<PlannedRelease>,
    /// Every item the eligible manifests name.
    named: Vec<String>,
    /// [`retained_refs`] over the snapshot.
    retained: HashSet<ContentId>,
    unnameable_indexes: usize,
    keep_count: usize,
    cache_dir: Option<std::path::PathBuf>,
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
/// An optional `hard_max_old_indexes` ceiling overrides the age check for
/// versions past it. See [`CleanGarbageConfig::hard_max_old_indexes`] for what
/// that costs a query still reading one of those versions.
///
/// # Roots with no garbage manifest
///
/// A root written before the manifest write became unconditional can carry
/// none. The nodes it replaced cannot be named, so they are left in storage
/// for the sweep to reclaim, and the walk continues rather than stopping — one
/// such root would otherwise pin every version newer than it forever.
///
/// # Shared dictionary blobs
///
/// Released as [`CleanGarbageConfig::shared_blobs`] allows; see
/// [`partition_nodes`].
///
/// # Ordering and crash safety
///
/// The pass releases in three steps: the nodes every eligible manifest names,
/// then the superseded manifests and roots oldest-first in chunks of
/// [`ROOT_RELEASE_CHUNK`]. A crash after the nodes but before the roots leaves
/// the chain and its manifests intact, so the next pass re-reads them and
/// re-releases, which is idempotent. Releasing roots oldest-first keeps every
/// still-eligible version reachable from the retained set; newest-first would
/// cut the chain at the retention boundary and orphan everything beyond. The
/// backend chooses the order within a chunk, so a crash mid-chunk can leave a
/// newer root gone while an older one stays, stranding that root and its
/// manifest for the sweep: at most a chunk's worth of small blobs, whose
/// nodes were already released.
///
/// # Concurrent builds
///
/// This plans and releases back to back against one snapshot, which is only
/// sound where nothing can publish to the ledger meanwhile. The worker and
/// the API instead call [`plan_garbage`] and release through
/// [`release_garbage_plan`] with builds held off; see
/// [`GarbagePlan::release`].
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
    match plan_garbage(store, current_root_id, &config).await? {
        Some(plan) => {
            plan.release(store, current_root_id, &config.shared_blobs)
                .await
        }
        None => Ok(CleanGarbageResult::default()),
    }
}

/// Steps 1-2 of [`clean_garbage`]: walk the chain and decide what goes.
/// Releases nothing. `None` when the pass has nothing to release.
///
/// `config.shared_blobs` is not read here; the policy is an argument to
/// [`GarbagePlan::release`], so a caller can compute it as late as possible.
pub async fn plan_garbage(
    store: &dyn ContentStore,
    current_root_id: &ContentId,
    config: &CleanGarbageConfig,
) -> Result<Option<GarbagePlan>> {
    let max_old_indexes = config.max_old_indexes.unwrap_or(DEFAULT_MAX_OLD_INDEXES) as usize;
    let min_age_mins = config
        .min_time_garbage_mins
        .unwrap_or(DEFAULT_MIN_TIME_GARBAGE_MINS);
    // Chain positions at or beyond this are collected regardless of record age;
    // `None` means the age guard is never overridden. The chain is newest-first,
    // so a larger position is an older version and the ceiling cuts the oldest
    // tail. See `CleanGarbageConfig::hard_max_old_indexes` for the reader cost.
    //
    // Deliberately not clamped up to `max_old_indexes`: the retention target is
    // enforced by the loop bound below, which starts at `keep_count`, so no
    // value here can reach a version the retention promise covers.
    let hard_keep = config.hard_max_old_indexes.map(|v| 1 + v as usize);
    let min_age_ms = min_age_mins as i64 * 60 * 1000;
    let now_ms = current_timestamp_ms();
    let started = std::time::Instant::now();
    let cache_dir = config.artifact_cache_dir.as_deref();

    // 1. Walk prev_index chain to collect all index versions (tolerant of missing roots)
    let index_chain = walk_prev_index_chain_cs_cached(store, current_root_id, cache_dir).await?;
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
        return Ok(None);
    }

    // 2. Plan: consult every gc-eligible entry from oldest to newest.
    //
    // Chain is newest-first. Indices 0..keep_count are retained.
    // Indices keep_count..len are gc-eligible.
    //
    // For each gc-eligible entry at index i, the manifest at index i-1 (the
    // newer entry) lists nodes from entry i that were replaced. Its items go
    // into the release set, and entry i's own manifest and root are queued.
    //
    // A read or retention failure breaks (not continues) because skipping an
    // entry and releasing a newer one would orphan the skipped entry and
    // everything older than it. A *missing* manifest is different: see the
    // `None` arm below.
    let mut planned: Vec<PlannedRelease> = Vec::new();
    let mut named: Vec<String> = Vec::new();
    let mut unnameable_indexes = 0;

    for i in (keep_count..index_chain.len()).rev() {
        let manifest_entry = &index_chain[i - 1];
        let entry_to_delete = &index_chain[i];

        // The age guard is a delay, not a bound: at a fast publish rate the
        // versions inside the window outnumber `max_old_indexes` without limit.
        // Past an operator-set ceiling the versions go regardless of age, which
        // can release artifacts a query that started against one of them is
        // still reading. That is the ceiling's documented cost, so each override
        // is counted and reported in the result rather than left at debug level.
        //
        // Expressed as a zero age floor rather than as a skip of the check, so
        // that the OTHER reasons `load_manifest_nodes` stops the walk — an
        // unreadable or unparseable manifest — still stop it. Only the age
        // reason is overridden.
        let past_ceiling = matches!(hard_keep, Some(hk) if i >= hk);
        let age_floor_ms = if past_ceiling {
            tracing::debug!(
                t = manifest_entry.t,
                chain_position = i,
                hard_keep,
                "Past retention ceiling, collecting regardless of age"
            );
            0
        } else {
            min_age_ms
        };

        match &manifest_entry.garbage_id {
            Some(garbage_id) => {
                match load_manifest_nodes(
                    store,
                    garbage_id,
                    manifest_entry.t,
                    cache_dir,
                    now_ms,
                    age_floor_ms,
                )
                .await
                {
                    Some(items) => named.extend(items),
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

        planned.push(PlannedRelease {
            root_id: entry_to_delete.root_id.clone(),
            garbage_id: entry_to_delete.garbage_id.clone(),
            past_ceiling,
        });
    }

    if planned.is_empty() {
        return Ok(None);
    }

    Ok(Some(GarbagePlan {
        snapshot_head: current_root_id.clone(),
        planned,
        named,
        retained: retained_refs(&index_chain, keep_count),
        unnameable_indexes,
        keep_count,
        cache_dir: config.artifact_cache_dir.clone(),
    }))
}

impl GarbagePlan {
    /// Steps 3-4 of [`clean_garbage`]: release the planned nodes, then the
    /// superseded manifests and roots.
    ///
    /// `head` is the branch's index head **now** and `shared_blobs` the policy
    /// **now**, both of which may be newer than the snapshot the plan was made
    /// from. A build that published since can have revived a CID the plan
    /// names (see [`retained_refs`]), so every root from `head` back to the
    /// snapshot is added to the retained set before anything is released.
    ///
    /// That closes the window only if no build can publish, or upload a blob
    /// it is about to publish, between the caller reading `head` and this
    /// returning. The indexer worker guarantees it with a release window
    /// over every branch of the ledger; a caller without one has the
    /// single-process caveat `MaintenanceGuard` documents.
    pub async fn release(
        mut self,
        store: &dyn ContentStore,
        head: &ContentId,
        shared_blobs: &SharedBlobPolicy,
    ) -> Result<CleanGarbageResult> {
        let published_since = self.retain_published_since(store, head).await?;
        let Self {
            planned,
            named,
            retained,
            unnameable_indexes,
            keep_count,
            ..
        } = self;

        // 3. Release the nodes the manifests named, protecting anything a
        // retained root still references directly (see `retained_refs`).
        let partition = partition_nodes(&named, shared_blobs, &retained);
        let release_started = std::time::Instant::now();
        let node_failures = release_batched(store, &partition.release, "garbage node").await;
        let shared_failed = node_failures
            .iter()
            .filter(|(id, _)| is_shared_across_branches(id))
            .count();
        let deleted_count = partition.release.len() - node_failures.len();
        tracing::debug!(
            versions = planned.len(),
            released = deleted_count,
            shared_released = partition.shared_released - shared_failed,
            shared_deferred = partition.shared_deferred,
            resurrected = partition.resurrected,
            retained_refs = retained.len(),
            published_since,
            elapsed_ms = release_started.elapsed().as_millis() as u64,
            "GC garbage node release complete"
        );

        // 4. Release the superseded manifests and roots, oldest-first.
        let mut indexes_cleaned = 0;
        let mut age_guard_overridden = 0;
        for chunk in planned.chunks(ROOT_RELEASE_CHUNK) {
            let mut ids = Vec::with_capacity(chunk.len() * 2);
            for entry in chunk {
                if let Some(garbage_id) = &entry.garbage_id {
                    ids.push(garbage_id.clone());
                }
                ids.push(entry.root_id.clone());
            }
            let failures = store.release_many(&ids).await;
            for (cid, error) in &failures {
                tracing::debug!(
                    %cid,
                    error = %error,
                    "Failed to release old db-root or manifest (may already be released)"
                );
            }
            let failed: HashSet<&ContentId> = failures.iter().map(|(id, _)| id).collect();
            for entry in chunk {
                if !failed.contains(&entry.root_id) {
                    indexes_cleaned += 1;
                    if entry.past_ceiling {
                        age_guard_overridden += 1;
                    }
                }
            }
        }

        let shared_released = partition.shared_released - shared_failed;
        if indexes_cleaned > 0 || deleted_count > 0 || partition.resurrected > 0 {
            tracing::info!(
                indexes_cleaned = indexes_cleaned,
                nodes_deleted = deleted_count,
                shared_released,
                shared_deferred = partition.shared_deferred,
                resurrected = partition.resurrected,
                unnameable_indexes = unnameable_indexes,
                age_guard_overridden = age_guard_overridden,
                retained_count = keep_count,
                "Garbage collection complete"
            );
        }

        Ok(CleanGarbageResult {
            indexes_cleaned,
            nodes_deleted: deleted_count,
            resurrected: partition.resurrected,
            age_guard_overridden,
            shared_released,
            shared_deferred: partition.shared_deferred,
        })
    }

    /// Add to the retained set every root from `head` back to the snapshot
    /// head, returning how many there were. A chain that never reaches the
    /// snapshot — a reindex replaced it — is added whole.
    async fn retain_published_since(
        &mut self,
        store: &dyn ContentStore,
        head: &ContentId,
    ) -> Result<usize> {
        let mut published_since = 0;
        if *head == self.snapshot_head {
            return Ok(published_since);
        }
        let mut walk = PrevIndexChainWalk::new(store, head, self.cache_dir.as_deref());
        while let Some(entry) = walk.next_entry().await? {
            if entry.root_id == self.snapshot_head {
                break;
            }
            self.retained.extend(entry.root.all_cas_ids());
            published_since += 1;
        }
        Ok(published_since)
    }
}

/// Release `plan` from the ledger's state as of now. Where an indexer worker
/// exists the caller holds the `ReleaseWindow` that makes "now" hold still;
/// see `IndexerHandle::open_release_window`.
///
/// Both inputs the plan's snapshot can have aged out of are re-read: this
/// branch's head, and the listing the sibling check walks. Neither may be
/// cached — a build seconds old, on this branch or on a fork seconds old,
/// can already reference a blob the plan names.
///
/// A ledger that is gone, or has no index head, releases nothing. A listing
/// that cannot be taken defers every shared blob rather than guess.
pub async fn release_garbage_plan(
    plan: GarbagePlan,
    backend: &fluree_db_core::StorageBackend,
    nameservice: &(impl fluree_db_nameservice::NameServiceLookup + ?Sized),
    ledger_id: &str,
    cache_dir: Option<&Path>,
) -> Result<CleanGarbageResult> {
    let head = nameservice
        .lookup(ledger_id)
        .await
        .map_err(|e| crate::error::IndexerError::NameService(e.to_string()))?
        .and_then(|record| record.index_head_id);
    let Some(head) = head else {
        tracing::debug!(ledger_id, "ledger has no index head; releasing nothing");
        return Ok(CleanGarbageResult::default());
    };
    let shared_blobs = match nameservice.all_records().await {
        Ok(records) => super::shared_blob_policy_for(backend, &records, ledger_id, cache_dir).await,
        Err(e) => {
            tracing::warn!(
                ledger_id,
                error = %e,
                "could not list branches for the collector's sibling check; deferring shared blobs"
            );
            SharedBlobPolicy::Defer
        }
    };
    let store = backend.content_store(ledger_id);
    plan.release(store.as_ref(), &head, &shared_blobs).await
}

/// Collect the whole prev-index chain, newest root first, reading storage
/// directly.
///
/// See [`PrevIndexChainWalk::next_entry`] for how the walk ends, and prefer
/// the walk itself where each root can be consumed and dropped.
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
    let mut walk = PrevIndexChainWalk::new(store, current_root_id, cache_dir);
    let mut chain = Vec::new();

    while let Some(entry) = walk.next_entry().await? {
        chain.push(entry);
    }

    Ok(chain)
}

/// A prev-index chain walk in progress, newest root first.
///
/// Yields one root at a time. A caller that reduces each root to a summary —
/// the storage sweep's reachable CID set, say — then holds one decoded root
/// rather than the whole chain, which on a long chain is the difference
/// between a bounded working set and one that grows with the ledger's index
/// history.
pub(crate) struct PrevIndexChainWalk<'a> {
    store: &'a dyn ContentStore,
    cache_dir: Option<&'a Path>,
    /// The root to read next, or `None` once the chain has ended.
    next_id: Option<ContentId>,
    /// Whether any root has been yielded, which decides how an unreadable
    /// root is interpreted.
    yielded: bool,
}

impl<'a> PrevIndexChainWalk<'a> {
    pub(crate) fn new(
        store: &'a dyn ContentStore,
        head_id: &ContentId,
        cache_dir: Option<&'a Path>,
    ) -> Self {
        Self {
            store,
            cache_dir,
            next_id: Some(head_id.clone()),
            yielded: false,
        }
    }

    /// The next root in the chain, or `None` at its end.
    ///
    /// **Tolerant behavior**: if a prev_index link points at a root that no
    /// longer exists — the normal result of a prior GC truncating the chain —
    /// the walk ends gracefully rather than returning an error, which is what
    /// makes GC idempotent. A root that *does* exist but cannot be read
    /// propagates the error instead: a short chain would be
    /// indistinguishable from a genuinely short one, and callers deciding
    /// which artifacts are unreferenced would treat everything past the
    /// unreadable root as garbage.
    ///
    /// A `cache_dir` weakens that ending. A cached copy of a released root
    /// reads back, so the walk never learns storage has dropped it and
    /// continues into a chain the collector already truncated. Callers that
    /// must not act on a released root have to establish existence
    /// themselves — see `gc::sweep::chain_cas_ids`, which reads a failed
    /// expansion plus an absent root as the ending this walk missed.
    pub(crate) async fn next_entry(&mut self) -> Result<Option<IndexChainEntry>> {
        let Some(current_id) = self.next_id.take() else {
            return Ok(None);
        };

        let read_started = std::time::Instant::now();
        let bytes = match get_cached_or_remote(self.store, &current_id, self.cache_dir).await {
            Ok(bytes) => bytes,
            Err(e) => return self.end_of_chain_or_error(&current_id, e).await,
        };
        tracing::trace!(
            root_id = %current_id,
            bytes = bytes.len(),
            elapsed_ms = read_started.elapsed().as_millis() as u64,
            from_cache_enabled = self.cache_dir.is_some(),
            "GC loaded prev-index root"
        );

        let (t, prev_index_id, garbage_id, root) = parse_chain_fields(&bytes)?;
        self.next_id = prev_index_id;
        self.yielded = true;

        Ok(Some(IndexChainEntry {
            t,
            root_id: current_id,
            garbage_id,
            root,
        }))
    }

    /// Whether a root that would not read ends the chain or fails the walk.
    ///
    /// A *released* root is the normal end: GC truncates from the oldest end,
    /// leaving the retained boundary's prev_index dangling. Distinguish by
    /// existence, since the disk-cache path stringifies the error and loses
    /// its kind. If existence cannot be established either, treat the root as
    /// present and propagate. The head is never an ending — a walk that could
    /// not read its first root saw nothing at all.
    async fn end_of_chain_or_error(
        &self,
        root_id: &ContentId,
        error: crate::error::IndexerError,
    ) -> Result<Option<IndexChainEntry>> {
        if !self.yielded || self.store.has(root_id).await.unwrap_or(true) {
            return Err(error);
        }

        tracing::debug!(
            root_id = %root_id,
            "prev_index released by prior GC, chain ends here"
        );

        Ok(None)
    }
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

    /// Like [`minimal_fir6`], but with the given CID (rather than a fixed
    /// dummy) as the dict tree branch, so `all_cas_ids()` reports it — what
    /// resurrection tests need to make a root "reference" a specific CID.
    fn minimal_fir6_with_dict(
        t: i64,
        prev_index: Option<BinaryPrevIndexRef>,
        garbage: Option<BinaryGarbageRef>,
        dict_branch: ContentId,
    ) -> Vec<u8> {
        crate::gc::test_support::minimal_fir6_for(LEDGER, t, prev_index, garbage, dict_branch)
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

    /// A manifest naming a dictionary blob releases it only as the policy
    /// allows: never under `Defer`, always with no siblings, and not while a
    /// sibling still reaches it. The branch-local leaf beside it goes
    /// regardless.
    /// A garbage-named CID that reappears on a RETAINED root must not be
    /// released, even though it is unambiguously named as garbage by an
    /// older manifest. This is the shape a reverse-dict leaf split produces
    /// under a monotonic key pattern: a leaf re-hashed every build has its
    /// old CID garbaged every build, and the half a later split keeps can be
    /// byte-identical to one of those earlier states — the same CID an
    /// already-consumed manifest named, now live again via a root this pass
    /// keeps. See `retained_refs`.
    #[tokio::test]
    async fn a_garbage_named_cid_still_live_on_a_retained_root_is_not_released() {
        let dict_kind = ContentKind::DictBlob {
            dict: fluree_db_core::DictKind::Graphs,
        };
        let (resurrected, resurrected_addr) = cid_and_addr(dict_kind, b"leaf state N");
        let storage = MemoryStorage::new();
        storage
            .write_bytes(&resurrected_addr, b"leaf state N")
            .await
            .unwrap();

        // t=1: the leaf's first appearance, referenced directly as this
        // root's dict tree branch (all_cas_ids() includes tree.branch).
        let (cid1, addr1) = cid_and_addr(ContentKind::IndexRoot, b"root1");
        let root1 = minimal_fir6_with_dict(1, None, None, resurrected.clone());

        // t=2: the leaf gets touched (re-hashed), garbaging the t=1 CID —
        // exactly what happens to a growing leaf every build.
        let (cid2, addr2) = cid_and_addr(ContentKind::IndexRoot, b"root2");
        let (garb_cid2, garb_addr2) = cid_and_addr(ContentKind::GarbageRecord, b"garb2");
        let (touched, touched_addr) = cid_and_addr(dict_kind, b"leaf state N+1");
        let old_ts = current_timestamp_ms() - (60 * 60 * 1000);
        let root2 = minimal_fir6_with_dict(
            2,
            Some(BinaryPrevIndexRef {
                t: 1,
                id: cid1.clone(),
            }),
            Some(BinaryGarbageRef {
                id: garb_cid2.clone(),
            }),
            touched.clone(),
        );
        let garbage2 = format!(
            r#"{{"ledger_id": "{LEDGER}", "t": 2, "garbage": ["{resurrected}"], "created_at_ms": {old_ts}}}"#
        );

        // t=3: RETAINED (inside keep_count). The leaf resurrects: content
        // identical to t=1's state, so it gets the same CID again.
        let (cid3, addr3) = cid_and_addr(ContentKind::IndexRoot, b"root3");
        let root3 = minimal_fir6_with_dict(
            3,
            Some(BinaryPrevIndexRef {
                t: 2,
                id: cid2.clone(),
            }),
            None,
            resurrected.clone(),
        );

        // t=4: head, also retained.
        let (cid4, addr4) = cid_and_addr(ContentKind::IndexRoot, b"root4");
        let root4 = minimal_fir6_with_dict(
            4,
            Some(BinaryPrevIndexRef {
                t: 3,
                id: cid3.clone(),
            }),
            None,
            resurrected.clone(),
        );

        for (addr, bytes) in [
            (&addr1, root1.as_slice()),
            (&addr2, root2.as_slice()),
            (&addr3, root3.as_slice()),
            (&addr4, root4.as_slice()),
            (&garb_addr2, garbage2.as_bytes()),
            (&touched_addr, b"leaf state N+1".as_slice()),
        ] {
            storage.write_bytes(addr, bytes).await.unwrap();
        }

        // max_old_indexes=1 -> keep_count=2 -> retained {t=4, t=3}, t=1 and
        // t=2 are gc-eligible. Single branch, so the shared-blob policy
        // would otherwise release everything a manifest names.
        let config = CleanGarbageConfig {
            max_old_indexes: Some(1),
            min_time_garbage_mins: Some(30),
            shared_blobs: SharedBlobPolicy::Release {
                referenced_elsewhere: std::collections::HashSet::new(),
            },
            ..Default::default()
        };
        let store = test_store(&storage);
        let result = clean_garbage(&store, &cid4, config).await.unwrap();

        assert_eq!(result.indexes_cleaned, 2, "t=1 and t=2 both go");
        assert_eq!(
            result.resurrected, 1,
            "the manifest names the resurrected CID exactly once"
        );
        assert_eq!(
            result.shared_released, 0,
            "the resurrected CID must not count as released"
        );
        assert!(
            store.has(&resurrected).await.unwrap(),
            "root t=3 and t=4 still reference this CID; it must survive"
        );
        assert!(!store.has(&cid1).await.unwrap());
        assert!(!store.has(&cid2).await.unwrap());
        assert!(store.has(&cid3).await.unwrap());
        assert!(store.has(&cid4).await.unwrap());
    }

    /// The same revival, by a build that publishes *after* the pass planned.
    /// No root in the plan's snapshot references the CID, so only the walk
    /// from the head as of the release back to the snapshot can protect it.
    #[tokio::test]
    async fn a_cid_revived_by_a_root_published_after_the_plan_is_not_released() {
        let dict_kind = ContentKind::DictBlob {
            dict: fluree_db_core::DictKind::Graphs,
        };
        let (revived, revived_addr) = cid_and_addr(dict_kind, b"leaf state N");
        let (touched, touched_addr) = cid_and_addr(dict_kind, b"leaf state N+1");
        let old_ts = current_timestamp_ms() - (60 * 60 * 1000);
        let prev = |t: i64, id: &ContentId| Some(BinaryPrevIndexRef { t, id: id.clone() });

        let (cid1, addr1) = cid_and_addr(ContentKind::IndexRoot, b"root1");
        let (cid2, addr2) = cid_and_addr(ContentKind::IndexRoot, b"root2");
        let (cid3, addr3) = cid_and_addr(ContentKind::IndexRoot, b"root3");
        let (cid4, addr4) = cid_and_addr(ContentKind::IndexRoot, b"root4");
        let (garb_cid2, garb_addr2) = cid_and_addr(ContentKind::GarbageRecord, b"garb2");
        let root1 = minimal_fir6_with_dict(1, None, None, revived.clone());
        let root2 = minimal_fir6_with_dict(
            2,
            prev(1, &cid1),
            Some(BinaryGarbageRef {
                id: garb_cid2.clone(),
            }),
            touched.clone(),
        );
        let root3 = minimal_fir6_with_dict(3, prev(2, &cid2), None, touched.clone());
        // Published after the plan: the leaf splits back to its t=1 bytes.
        let root4 = minimal_fir6_with_dict(4, prev(3, &cid3), None, revived.clone());
        let garbage2 = format!(
            r#"{{"ledger_id": "{LEDGER}", "t": 2, "garbage": ["{revived}"], "created_at_ms": {old_ts}}}"#
        );

        let storage = MemoryStorage::new();
        for (addr, bytes) in [
            (&addr1, root1.as_slice()),
            (&addr2, root2.as_slice()),
            (&addr3, root3.as_slice()),
            (&garb_addr2, garbage2.as_bytes()),
            (&revived_addr, b"leaf state N".as_slice()),
            (&touched_addr, b"leaf state N+1".as_slice()),
        ] {
            storage.write_bytes(addr, bytes).await.unwrap();
        }

        let config = CleanGarbageConfig {
            max_old_indexes: Some(1),
            min_time_garbage_mins: Some(30),
            ..Default::default()
        };
        let store = test_store(&storage);
        let plan = plan_garbage(&store, &cid3, &config)
            .await
            .unwrap()
            .expect("t=1 is past retention");

        storage.write_bytes(&addr4, &root4).await.unwrap();

        let release_all = SharedBlobPolicy::Release {
            referenced_elsewhere: std::collections::HashSet::new(),
        };
        let result = plan.release(&store, &cid4, &release_all).await.unwrap();

        assert_eq!(result.indexes_cleaned, 1, "t=1 goes");
        assert_eq!(result.resurrected, 1);
        assert_eq!(result.shared_released, 0);
        assert!(
            store.has(&revived).await.unwrap(),
            "the head published after the plan references this CID; it must survive"
        );
        assert!(!store.has(&cid1).await.unwrap());
    }

    #[tokio::test]
    async fn shared_blobs_follow_the_policy() {
        let dict_kind = ContentKind::DictBlob {
            dict: fluree_db_core::DictKind::Graphs,
        };
        let (dict_cid, dict_addr) = cid_and_addr(dict_kind, b"shared dict");
        let cases: Vec<(SharedBlobPolicy, bool)> = vec![
            (SharedBlobPolicy::Defer, false),
            (
                SharedBlobPolicy::Release {
                    referenced_elsewhere: std::collections::HashSet::new(),
                },
                true,
            ),
            (
                SharedBlobPolicy::Release {
                    referenced_elsewhere: std::collections::HashSet::from([dict_cid.clone()]),
                },
                false,
            ),
        ];

        for (policy, expect_released) in cases {
            let storage = MemoryStorage::new();
            let (cid1, addr1) = cid_and_addr(ContentKind::IndexRoot, b"root1");
            let (cid2, addr2) = cid_and_addr(ContentKind::IndexRoot, b"root2");
            let (cid3, addr3) = cid_and_addr(ContentKind::IndexRoot, b"root3");
            let (garb_cid2, garb_addr2) = cid_and_addr(ContentKind::GarbageRecord, b"garb2");
            let (leaf_cid, leaf_addr) = cid_and_addr(ContentKind::IndexLeaf, b"old leaf");
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
                r#"{{"ledger_id": "{LEDGER}", "t": 2, "garbage": ["{leaf_cid}", "{dict_cid}"], "created_at_ms": {old_ts}}}"#
            );
            for (addr, bytes) in [
                (&addr1, root1.as_slice()),
                (&addr2, root2.as_slice()),
                (&addr3, root3.as_slice()),
                (&garb_addr2, garbage2.as_bytes()),
                (&leaf_addr, b"old leaf".as_slice()),
                (&dict_addr, b"shared dict".as_slice()),
            ] {
                storage.write_bytes(addr, bytes).await.unwrap();
            }

            let config = CleanGarbageConfig {
                max_old_indexes: Some(1),
                min_time_garbage_mins: Some(30),
                shared_blobs: policy.clone(),
                ..Default::default()
            };
            let store = test_store(&storage);
            let result = clean_garbage(&store, &cid3, config).await.unwrap();

            assert_eq!(result.indexes_cleaned, 1, "{policy:?}");
            assert!(
                !store.has(&leaf_cid).await.unwrap(),
                "branch-local leaf is released under every policy: {policy:?}"
            );
            assert_eq!(
                store.has(&dict_cid).await.unwrap(),
                !expect_released,
                "dict blob presence under {policy:?}"
            );
            assert_eq!(result.shared_released, usize::from(expect_released));
            assert_eq!(result.shared_deferred, usize::from(!expect_released));
            assert_eq!(result.nodes_deleted, 1 + usize::from(expect_released));
        }
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

    /// The hard ceiling overrides the age guard.
    ///
    /// Same fixture as `test_clean_garbage_respects_time_threshold` — a garbage
    /// record only 5 minutes old against a 30-minute guard — but with the ceiling
    /// set low enough that the oldest version is past it. Without this, a ledger
    /// publishing faster than the guard accumulates versions without bound, since
    /// `max_old_indexes` is ANDed with the age check and so bounds nothing.
    ///
    /// Note the sibling test above pins the complement: with no ceiling set,
    /// which is the default, this same chain is left alone, so the guard governs
    /// everything unless an operator opts in.
    #[tokio::test]
    async fn test_hard_ceiling_collects_despite_recent_garbage() {
        let storage = MemoryStorage::new();

        let (cid1, addr1) = cid_and_addr(ContentKind::IndexRoot, b"hard_root1");
        let (cid2, addr2) = cid_and_addr(ContentKind::IndexRoot, b"hard_root2");
        let (cid3, addr3) = cid_and_addr(ContentKind::IndexRoot, b"hard_root3");
        let (garb_cid2, garb_addr2) = cid_and_addr(ContentKind::GarbageRecord, b"hard_garb2");

        // 5 minutes old: well inside the 30-minute guard.
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
            // keep_count = 2, hard_keep = 2, so the oldest entry (chain index 2)
            // is at the ceiling and collected despite the recent record.
            hard_max_old_indexes: Some(1),
            ..Default::default()
        };

        let store = test_store(&storage);
        let result = clean_garbage(&store, &cid3, config).await.unwrap();

        assert_eq!(
            result.indexes_cleaned, 1,
            "the ceiling must override the age guard"
        );
        assert_eq!(
            result.age_guard_overridden, 1,
            "the override must be reported, not silent"
        );

        // The oldest root is gone; the retained newest ones survive.
        assert!(!store.has(&cid1).await.unwrap(), "oldest root collected");
        assert!(store.has(&cid2).await.unwrap());
        assert!(store.has(&cid3).await.unwrap());
    }

    /// Directory prefix under which this store writes index roots, taken from a
    /// real derived address so it cannot drift from the write path.
    ///
    /// An earlier version of the harness below listed
    /// `"test:main/main/index/roots/"`, which never matched the stored
    /// `fluree:memory://test/main/index/roots/…` addresses: every generation
    /// counted zero roots and every bound held vacuously, with GC deleted
    /// outright. The harness now refuses a zero count.
    fn roots_prefix() -> String {
        let (_, addr) = cid_and_addr(ContentKind::IndexRoot, b"prefix-probe");
        let dir_end = addr.rfind('/').expect("root address has a directory") + 1;
        addr[..dir_end].to_string()
    }

    /// Publish `generations` index versions, running GC after each exactly as
    /// the orchestrator does, and return how many root objects the store holds
    /// after every generation.
    ///
    /// Each generation's garbage record is stamped `garbage_ts()` and names one
    /// superseded leaf, which is what a real build records.
    async fn publish_then_gc(
        generations: usize,
        garbage_ts: impl Fn() -> i64,
        config: CleanGarbageConfig,
    ) -> Vec<usize> {
        let storage = MemoryStorage::new();
        let store = test_store(&storage);
        let prefix = roots_prefix();

        let mut prev: Option<(i64, ContentId)> = None;
        let mut counts = Vec::with_capacity(generations);

        for gen in 1..=generations {
            let t = gen as i64;
            let (root_cid, root_addr) =
                cid_and_addr(ContentKind::IndexRoot, format!("root{gen}").as_bytes());

            let garbage_ref = if prev.is_some() {
                let (g_cid, g_addr) =
                    cid_and_addr(ContentKind::GarbageRecord, format!("garb{gen}").as_bytes());
                let (dead_cid, dead_addr) =
                    cid_and_addr(ContentKind::IndexLeaf, format!("leaf{gen}").as_bytes());
                storage
                    .write_bytes(&dead_addr, b"superseded")
                    .await
                    .unwrap();
                let ts = garbage_ts();
                let body = format!(
                    r#"{{"ledger_id": "{LEDGER}", "t": {t}, "garbage": ["{dead_cid}"], "created_at_ms": {ts}}}"#
                );
                storage.write_bytes(&g_addr, body.as_bytes()).await.unwrap();
                Some(BinaryGarbageRef { id: g_cid })
            } else {
                None
            };

            let bytes = minimal_fir6(
                t,
                prev.as_ref().map(|(pt, pid)| BinaryPrevIndexRef {
                    t: *pt,
                    id: pid.clone(),
                }),
                garbage_ref,
            );
            storage.write_bytes(&root_addr, &bytes).await.unwrap();

            clean_garbage(&store, &root_cid, config.clone())
                .await
                .unwrap();

            let roots = storage.list_prefix(&prefix).await.unwrap().len();
            // The current root is always present, so zero means the harness is
            // not observing the store at all — see `roots_prefix`.
            assert!(
                roots >= 1,
                "generation {gen}: no roots observed under {prefix}"
            );
            counts.push(roots);
            prev = Some((t, root_cid));
        }
        counts
    }

    fn every_fifth(counts: &[usize]) -> Vec<(usize, usize)> {
        counts
            .iter()
            .enumerate()
            .filter(|(i, _)| i % 5 == 4)
            .map(|(i, c)| (i + 1, *c))
            .collect()
    }

    /// Baseline: with no age guard in play, repeated publish-then-GC settles at
    /// exactly `1 + max_old_indexes` roots. This passes on `main` too — it pins
    /// the truncation loop, not the ceiling — and gives the two tests below a
    /// known-good shape to differ from. Deleting GC fails it at 40 roots.
    #[tokio::test]
    async fn repeated_publish_then_gc_keeps_root_count_bounded() {
        const GENERATIONS: usize = 40;
        const MAX_OLD: u32 = 2;
        // A day old: past any age guard.
        let old_ts = current_timestamp_ms() - (24 * 60 * 60 * 1000);

        let counts = publish_then_gc(
            GENERATIONS,
            || old_ts,
            CleanGarbageConfig {
                max_old_indexes: Some(MAX_OLD),
                min_time_garbage_mins: Some(0),
                ..Default::default()
            },
        )
        .await;

        let keep_count = 1 + MAX_OLD as usize;
        assert_eq!(
            *counts.last().unwrap(),
            keep_count,
            "roots should settle at 1 + max_old_indexes; every 5th generation: {:?}",
            every_fifth(&counts)
        );
        assert!(
            counts.iter().all(|&c| c <= keep_count),
            "roots must never exceed the retention target once GC runs; \
             every 5th generation: {:?}",
            every_fifth(&counts)
        );
    }

    /// Production's regime — a live 30-minute age guard and fresh garbage
    /// records — with no ceiling set, which is the default. Every version is
    /// inside the guard, so GC must leave all of them alone. That is the
    /// reader-safety property the guard exists for: a query that started
    /// against any of these versions can still read it. The default
    /// configuration must never trade that away, so this fails if a default
    /// ceiling is ever derived again.
    ///
    /// Deleting GC leaves this green, deliberately: it pins the absence of an
    /// override, and GC doing nothing is exactly the required behaviour.
    #[tokio::test]
    async fn repeated_publish_under_a_live_age_guard_retains_everything_by_default() {
        const GENERATIONS: usize = 40;
        const MAX_OLD: u32 = 2;

        let counts = publish_then_gc(
            GENERATIONS,
            current_timestamp_ms,
            CleanGarbageConfig {
                max_old_indexes: Some(MAX_OLD),
                min_time_garbage_mins: Some(30),
                hard_max_old_indexes: None,
                ..Default::default()
            },
        )
        .await;

        assert_eq!(
            *counts.last().unwrap(),
            GENERATIONS,
            "without a ceiling the age guard must hold every version; \
             every 5th generation: {:?}",
            every_fifth(&counts)
        );
    }

    /// The same regime with a ceiling set: past it the age guard is overridden,
    /// so roots stay at exactly `1 + hard_max_old_indexes` instead of growing
    /// with every publish. This is the defect the ceiling exists for,
    /// reproduced. Removing the override — which is what `main` does — fails it
    /// with all 40 roots retained; deleting GC fails it the same way.
    #[tokio::test]
    async fn repeated_publish_then_gc_bounded_with_a_live_age_guard() {
        const GENERATIONS: usize = 40;
        const MAX_OLD: u32 = 2;
        const HARD_MAX_OLD: u32 = 8;

        let counts = publish_then_gc(
            GENERATIONS,
            current_timestamp_ms,
            CleanGarbageConfig {
                max_old_indexes: Some(MAX_OLD),
                min_time_garbage_mins: Some(30),
                hard_max_old_indexes: Some(HARD_MAX_OLD),
                ..Default::default()
            },
        )
        .await;

        let hard_keep = 1 + HARD_MAX_OLD as usize;
        assert_eq!(
            *counts.last().unwrap(),
            hard_keep,
            "roots must be pinned at the ceiling under a live age guard; \
             every 5th generation: {:?}",
            every_fifth(&counts)
        );
        assert!(
            counts.iter().all(|&c| c <= hard_keep),
            "roots must never exceed the ceiling; every 5th generation: {:?}",
            every_fifth(&counts)
        );
    }

    /// **The retention promise outranks the ceiling.** Even at the most hostile
    /// setting — `hard_max_old_indexes: Some(0)`, i.e. "override the age guard
    /// everywhere" — the newest `1 + max_old_indexes` versions must survive.
    ///
    /// The previous version of this test was VACUOUS and is worth recording as a
    /// warning. It used `max_old_indexes: Some(5)` against a 3-entry chain, so
    /// `index_chain.len() <= keep_count` returned early and the retention loop
    /// never ran at all. It asserted `indexes_cleaned == 0` and passed because
    /// nothing was attempted, not because anything was protected — it stayed
    /// green with the ceiling logic removed entirely.
    ///
    /// This version makes the chain LONGER than `keep_count`, so the loop runs
    /// and both halves are observable: the two eligible versions are collected
    /// despite fresh garbage records (the ceiling working), and the two retained
    /// ones survive (the retention promise holding). Lowering the loop's start
    /// from `keep_count` to `0` fails this.
    #[tokio::test]
    async fn test_hard_ceiling_never_collects_inside_retention_target() {
        let storage = MemoryStorage::new();

        // Chain newest-first once walked: [root4, root3, root2, root1].
        // max_old_indexes = 1 -> keep_count = 2, so root4/root3 are retained and
        // root2/root1 are gc-eligible.
        let (cid1, addr1) = cid_and_addr(ContentKind::IndexRoot, b"floor_root1");
        let (cid2, addr2) = cid_and_addr(ContentKind::IndexRoot, b"floor_root2");
        let (cid3, addr3) = cid_and_addr(ContentKind::IndexRoot, b"floor_root3");
        let (cid4, addr4) = cid_and_addr(ContentKind::IndexRoot, b"floor_root4");
        let (garb_cid2, garb_addr2) = cid_and_addr(ContentKind::GarbageRecord, b"floor_garb2");
        let (garb_cid3, garb_addr3) = cid_and_addr(ContentKind::GarbageRecord, b"floor_garb3");

        // Every record is 5 minutes old against a 30-minute guard, so nothing
        // here is collectable unless the ceiling overrides the age check.
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
            None,
        );
        let garbage2 = format!(
            r#"{{"ledger_id": "{LEDGER}", "t": 2, "garbage": [], "created_at_ms": {recent_ts}}}"#
        );
        let garbage3 = format!(
            r#"{{"ledger_id": "{LEDGER}", "t": 3, "garbage": [], "created_at_ms": {recent_ts}}}"#
        );

        storage.write_bytes(&addr1, &root1).await.unwrap();
        storage.write_bytes(&addr2, &root2).await.unwrap();
        storage.write_bytes(&addr3, &root3).await.unwrap();
        storage.write_bytes(&addr4, &root4).await.unwrap();
        storage
            .write_bytes(&garb_addr2, garbage2.as_bytes())
            .await
            .unwrap();
        storage
            .write_bytes(&garb_addr3, garbage3.as_bytes())
            .await
            .unwrap();

        let config = CleanGarbageConfig {
            max_old_indexes: Some(1),
            min_time_garbage_mins: Some(30),
            // The most hostile setting: override the age guard everywhere.
            hard_max_old_indexes: Some(0),
            ..Default::default()
        };

        let store = test_store(&storage);
        clean_garbage(&store, &cid4, config).await.unwrap();

        // Retained: the newest keep_count = 2. These must survive whatever the
        // ceiling says — this is the half a wrong loop bound would break.
        assert!(
            store.has(&cid4).await.unwrap(),
            "the current root must never be collected"
        );
        assert!(
            store.has(&cid3).await.unwrap(),
            "the retention target promises max_old_indexes=1 old version; the \
             ceiling must not reach inside it"
        );
        // Eligible: collected despite garbage records well inside the age guard,
        // which is the ceiling doing its job. Without it both would survive.
        assert!(
            !store.has(&cid2).await.unwrap(),
            "past the ceiling, age must not protect an eligible version"
        );
        assert!(
            !store.has(&cid1).await.unwrap(),
            "past the ceiling, age must not protect an eligible version"
        );
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
            saw_list_meta: false,
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
