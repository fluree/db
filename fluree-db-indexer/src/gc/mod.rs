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
//! Both thresholds must be satisfied for GC to occur.

pub(crate) mod collector;
mod record;

pub use collector::clean_garbage;
pub use record::GarbageRecord;

use crate::error::Result;
use fluree_db_core::{ContentId, ContentKind, ContentStore};

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
