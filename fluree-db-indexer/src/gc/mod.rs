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
//! 3. **On-demand cleanup**: Walk the prev-index chain, identify eligible garbage,
//!    and release CAS artifacts via `ContentStore::release`
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
//! Both thresholds must be satisfied for GC to occur — which means the SLOWER of
//! the two wins, and under a fast publish rate that is always the age guard. A
//! ledger publishing twice a minute accumulates ~60 versions inside a 30-minute
//! guard, so `max_old_indexes = 5` is not a bound on anything: real retention is
//! "however many versions fit in `min_time_garbage_mins`", which is unbounded in
//! bytes because it scales with publish rate and per-version size.
//!
//! Observed on a deployment: 79 retained versions against a target of 5, with
//! `objects/history` at 14 GiB for a ledger whose live index was 260 MiB and
//! whose entire commit log was 417 MiB — i.e. ~34x the dataset, all of it
//! age-guarded garbage.
//!
//! ## Hard Ceiling
//!
//! So there is a third threshold that can override the age guard:
//! - `hard_max_old_indexes`: past this many old versions, collect regardless of age
//!
//! A time window cannot bound bytes; only a count can. The guard still applies up
//! to the ceiling, so the common case is unchanged and only genuinely runaway
//! chains trade delay for disk.

pub(crate) mod collector;
mod record;

pub use collector::clean_garbage;
pub use record::GarbageRecord;

use crate::error::Result;
use fluree_db_core::{ContentId, ContentKind, ContentStore};
use std::path::PathBuf;

/// Default maximum number of old indexes to retain
pub const DEFAULT_MAX_OLD_INDEXES: u32 = 5;

/// Default minimum age (in minutes) before an index can be garbage collected
pub const DEFAULT_MIN_TIME_GARBAGE_MINS: u32 = 30;

/// Default multiple of `max_old_indexes` past which the age guard is overridden.
///
/// See [`CleanGarbageConfig::hard_max_old_indexes`]. With the defaults this is
/// `5 * 4 = 20` old versions retained before the ceiling engages, so the age
/// guard still governs everything inside 4x the configured target.
pub const DEFAULT_HARD_MAX_MULTIPLE: u32 = 4;

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
    /// Hard ceiling on retained old index versions, past which
    /// `min_time_garbage_mins` is overridden and versions are collected
    /// regardless of age. `None` = `max_old_indexes * DEFAULT_HARD_MAX_MULTIPLE`.
    ///
    /// Exists because `max_old_indexes` and `min_time_garbage_mins` are ANDed, so
    /// the age guard always wins under a fast publish rate and the count target
    /// bounds nothing (see the module docs). A count is the only thing that can
    /// bound bytes.
    ///
    /// The trade-off is explicit: the guard exists so a concurrent query reading
    /// an older index version does not have its artifacts deleted underneath it,
    /// and overriding it can fail such a query. That is recoverable — a retry
    /// re-reads at the current version — whereas an exhausted disk stops the
    /// writer entirely and, because GC needs to write to make progress, cannot
    /// recover on its own. Set high enough that only genuinely runaway chains
    /// reach it.
    pub hard_max_old_indexes: Option<u32>,
    /// Optional disk artifact cache for root and garbage-record reads.
    pub artifact_cache_dir: Option<PathBuf>,
}

/// Result of garbage collection
#[derive(Debug, Clone, Default)]
pub struct CleanGarbageResult {
    /// Number of old index versions cleaned up
    pub indexes_cleaned: usize,
    /// Number of nodes deleted
    pub nodes_deleted: usize,
}

/// Write a garbage record to storage.
///
/// The caller must ensure `garbage_cid_strings` is non-empty; this function
/// does not handle the empty case (callers guard with `if !cids.is_empty()`).
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
