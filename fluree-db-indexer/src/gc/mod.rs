//! # Garbage Collection
//!
//! Garbage collection for content-addressed storage (CID-based).
//!
//! During index building, CAS artifacts (dicts, branches, leaves) that are no
//! longer referenced by the new root are recorded in a garbage manifest.
//! The GC collector walks the `prev_index` chain, identifies gc-eligible roots,
//! and deletes their obsolete artifacts.
//!
//! ## Design
//!
//! 1. **During build**: Compute `old_root.all_cas_ids() \ new_root.all_cas_ids()`
//! 2. **After build**: Write a garbage record with the obsolete CID strings
//! 3. **Cleanup**: after each publish, and on the worker's periodic tick for
//!    every ledger whose chain may exceed retention, walk the prev-index
//!    chain, identify eligible garbage, and release CAS artifacts in batches
//!    via `ContentStore::release_many`
//!
//! ## Shared dictionary blobs
//!
//! Roots, leaves, branch manifests and sidecars are branch-local, so a
//! manifest naming one settles it: the collector releases it. Dictionary
//! blobs live in the ledger-wide `@shared/dicts/` namespace. A branch forked
//! from another starts out referencing the source's dictionaries rather than
//! a copy, so the source branch's manifest can name a blob the fork still
//! reads.
//!
//! The collector therefore releases a shared blob only under a
//! [`SharedBlobPolicy::Release`] whose `referenced_elsewhere` set — every
//! dictionary blob any *other* branch of the ledger still reaches through its
//! own chain, see [`shared_refs_of_branches`] — does not contain it. A blob a
//! sibling still reaches is deferred; when that sibling replaces it, its own
//! manifest names it, and the sibling's pass releases it if nothing else
//! reaches it by then. Whichever branch drops a blob last deletes it. A ledger
//! with one branch has nothing elsewhere and releases every blob its
//! manifests name.
//!
//! Two passes on branches of one ledger must not overlap: each could see the
//! other's not-yet-released old root still referencing a blob, both would
//! defer it, and both would consume the manifests that named it. The worker
//! serialises passes per ledger name for that reason.
//!
//! Which branches are siblings comes from a listing that may be minutes old,
//! because `all_records()` is O(ledgers) on every nameservice backend, plus
//! every branch this process has built since, which is the only way a fork
//! the listing predates can reference more than its source did. Each
//! sibling's *head* is never cached: it is one consistent `lookup`, made as
//! the pass releases. See [`current_sibling_heads`].
//!
//! Blobs named by manifests the collector has already consumed, and anything
//! orphaned off the chain, are reachable only by [`plan_sweep`], which
//! enumerates storage rather than walking chains.
//!
//! ## A manifest's "garbage" can still be live
//!
//! A manifest names a CID relative to the one build that replaced it.
//! Content addressing does not know that: a later build that happens to
//! produce byte-identical output gets the *same* CID, whatever an earlier
//! manifest said about it. Reverse-dictionary leaves hit this routinely
//! under a monotonic key pattern (ULID, UUIDv7, sequential ids, timestamp
//! suffixes): a leaf receiving one new entry per build is re-hashed and its
//! old CID garbaged every build, and the half a later split keeps is
//! byte-identical to one of the leaf's own earlier states — reviving a CID
//! an already-consumed manifest named.
//!
//! Before releasing anything a manifest names, `retained_refs` (in
//! `gc::collector`) checks it against `all_cas_ids()` of every root this
//! pass retains — every root a query or a future build can still read —
//! and skips it if any of them still reference it directly. "Retains" means
//! every root the pass leaves in the chain, which under a live age guard is
//! more than the retention count.
//!
//! A build can revive a CID at any moment, so a snapshot is not enough: a
//! pass is split into [`plan_garbage`], which reads a snapshot and releases
//! nothing, and [`GarbagePlan::release`], which the worker runs inside a
//! release window — no branch of the ledger building, the one in flight
//! waited out — after re-reading this branch's head and every sibling's
//! ([`release_garbage_plan`]). Roots published since the snapshot
//! join the retained set, and the sibling refs are as of the window. A
//! branch drop releases inside the same window. What is left is the
//! single-process caveat [`plan_sweep`] and `MaintenanceGuard` carry: a
//! second process indexing the same storage is not excluded.
//!
//! ## Garbage Record Format
//!
//! Garbage records are CAS-written JSON containing sorted/deduped CID strings
//! (base32-lower multibase). Each record includes a `created_at_ms` wall-clock
//! timestamp for time-based retention checks. Because of the timestamp, records
//! are indexer-specific (not deterministic across concurrent indexers), but this
//! is harmless since only one indexer wins the publish race.
//!
//! The collector releases CID strings via `ContentStore::release`.
//!
//! ## Time-Based Retention
//!
//! GC respects two thresholds:
//! - `max_old_indexes`: Maximum number of old index versions to keep (default: 5)
//! - `min_time_garbage_mins`: Minimum age before an index can be GC'd (default: 30)
//!
//! Both thresholds must be satisfied for GC to occur, so the slower of the two
//! wins. Under a sustained publish rate that is the age guard: a ledger
//! publishing twice a minute holds ~60 versions inside a 30-minute guard, and
//! `max_old_indexes = 5` then bounds nothing. Real retention becomes "however
//! many versions fit in `min_time_garbage_mins`", which grows with publish rate
//! and per-version size.
//!
//! Observed on a deployment: 79 retained versions against a target of 5, with
//! `objects/history` at 14 GiB for a ledger whose live index was 260 MiB and
//! whose entire commit log was 417 MiB — i.e. ~34x the dataset, all of it
//! age-guarded garbage.
//!
//! ## Version Ceiling
//!
//! An optional third threshold overrides the age guard:
//! - `hard_max_old_indexes`: past this many old versions, collect regardless of age
//!
//! It is off by default. The age guard is what keeps a query that started
//! against an older index version from having that version's artifacts
//! released underneath it; overriding the guard removes artifacts such a query
//! may still need. Enabling the ceiling is therefore an operator decision that
//! trades reader safety for a bound on the chain. Once set, the guard still
//! governs everything inside the ceiling.
//!
//! A ceiling bounds the number of retained versions, not their bytes. What a
//! retained version costs varies by orders of magnitude between ledgers (one
//! deployment held ~7.7 GiB per version on one ledger and ~3.3 GiB on another),
//! so size the ceiling from observed per-version bytes rather than treating it
//! as a disk limit.

pub(crate) mod collector;
mod record;
mod siblings;
mod sweep;
#[cfg(test)]
pub(crate) mod test_support;

pub use collector::{clean_garbage, plan_garbage, release_garbage_plan, GarbagePlan};
pub use record::GarbageRecord;
pub use siblings::{
    current_sibling_heads, shared_blob_policy_for, shared_refs_of_branches, siblings_of,
};
pub use sweep::{execute_sweep, plan_sweep, BranchIndexHead, SweepPlan, SweepResult};

use crate::error::Result;
use fluree_db_core::{ContentId, ContentKind, ContentStore};
use std::collections::HashSet;
use std::path::PathBuf;

/// What the collector does with a manifest entry that names a blob in the
/// ledger-wide `@shared/dicts/` namespace. See the module docs.
#[derive(Debug, Clone, Default)]
pub enum SharedBlobPolicy {
    /// Leave every shared blob in storage for the sweep.
    ///
    /// The choice when nothing has established what the ledger's other
    /// branches reference, which is the safe direction: a deferral costs
    /// disk until a sweep, a wrong release costs a branch its dictionary.
    #[default]
    Defer,
    /// Release shared blobs, except any in `referenced_elsewhere`.
    ///
    /// `referenced_elsewhere` is every dictionary blob some *other* branch of
    /// the same ledger still reaches through its own index chain, retracted
    /// branches included, as [`shared_refs_of_branches`] computes it. A
    /// ledger with one branch passes an empty set.
    Release {
        referenced_elsewhere: HashSet<ContentId>,
    },
}

/// Default maximum number of old indexes to retain
pub const DEFAULT_MAX_OLD_INDEXES: u32 = 5;

/// Default minimum age (in minutes) before an index can be garbage collected
pub const DEFAULT_MIN_TIME_GARBAGE_MINS: u32 = 30;

/// Configuration for garbage collection
#[derive(Debug, Clone, Default)]
pub struct CleanGarbageConfig {
    /// Maximum number of old indexes to keep (None = default 5)
    ///
    /// With max_old_indexes=5, we keep current + 5 old = 6 total index versions.
    pub max_old_indexes: Option<u32>,
    /// Minimum age in minutes before GC (None = default 30)
    ///
    /// Garbage records must be at least this old before their nodes can be deleted.
    pub min_time_garbage_mins: Option<u32>,
    /// Ceiling on retained old index versions, past which
    /// `min_time_garbage_mins` is overridden and versions are collected
    /// regardless of age. `None` (the default) sets no ceiling: the age guard
    /// is always honoured.
    ///
    /// Exists because `max_old_indexes` and `min_time_garbage_mins` are ANDed, so
    /// under a fast publish rate the age guard always wins and the count target
    /// bounds nothing (see the module docs).
    ///
    /// Setting it trades reader safety for that bound. The guard is what keeps a
    /// query that started against an older version from having that version's
    /// artifacts released while it is still reading them; past the ceiling those
    /// artifacts go regardless, and such a query fails or reads a torn version.
    /// Set it well above the number of versions published during the longest
    /// query the ledger serves. It bounds versions, not bytes: per-version size
    /// varies widely between ledgers, so derive it from observed per-version
    /// disk use.
    pub hard_max_old_indexes: Option<u32>,
    /// Optional disk artifact cache for root and garbage-record reads.
    pub artifact_cache_dir: Option<PathBuf>,
    /// What to do with dictionary blobs the manifests name. Defaults to
    /// [`SharedBlobPolicy::Defer`].
    pub shared_blobs: SharedBlobPolicy,
}

/// Result of garbage collection
#[derive(Debug, Clone, Default)]
pub struct CleanGarbageResult {
    /// Versions collected past `hard_max_old_indexes`, where the age guard was
    /// not applied. Non-zero means a query that was still reading one of them
    /// may have lost artifacts it needed.
    pub age_guard_overridden: usize,
    /// Number of old index versions cleaned up
    pub indexes_cleaned: usize,
    /// Number of nodes deleted
    pub nodes_deleted: usize,
    /// Dictionary blobs released, counted within `nodes_deleted`.
    pub shared_released: usize,
    /// Dictionary blobs the manifests named but this pass left in storage:
    /// every one under [`SharedBlobPolicy::Defer`], or those a sibling
    /// branch still reaches under [`SharedBlobPolicy::Release`].
    pub shared_deferred: usize,
    /// Items a manifest named as garbage that a root this pass retains
    /// still references directly, so they were left in storage regardless
    /// of policy. Non-zero on a healthy pass — see `retained_refs` in
    /// `gc::collector` for why a garbage-named CID can be live again. What
    /// it does not cover: a build publishing a new root *during* this pass,
    /// between its chain snapshot and the release call, can still lose a
    /// CID this check would have protected. Single-process deployments
    /// only, same caveat as the storage sweep and `MaintenanceGuard`.
    pub resurrected: usize,
}

/// Write a garbage record to storage.
///
/// An empty `garbage_cid_strings` is valid and records that the root replaced
/// nothing. Callers write the record unconditionally: the collector stops its
/// chain walk at the first root with no manifest, so an omitted record and an
/// empty one are not equivalent.
///
/// The CID strings are sorted and deduplicated before writing.
/// Includes a wall-clock `created_at_ms` timestamp for time-based GC retention.
///
/// Returns the `ContentId` of the written garbage record.
pub async fn write_garbage_record(
    content_store: &dyn ContentStore,
    ledger_id: &str,
    t: i64,
    garbage_cid_strings: Vec<String>,
) -> Result<ContentId> {
    let mut garbage_cid_strings = garbage_cid_strings;

    // Sort and dedupe for determinism
    garbage_cid_strings.sort();
    garbage_cid_strings.dedup();

    let record = GarbageRecord {
        ledger_id: ledger_id.to_string(),
        t,
        garbage: garbage_cid_strings,
        created_at_ms: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0),
    };

    let bytes = serde_json::to_vec(&record)?;
    let cid = content_store
        .put(ContentKind::GarbageRecord, &bytes)
        .await?;

    Ok(cid)
}

/// Parse a garbage record from raw bytes.
pub fn parse_garbage_record(bytes: &[u8]) -> Result<GarbageRecord> {
    let record: GarbageRecord = serde_json::from_slice(bytes)?;
    Ok(record)
}
