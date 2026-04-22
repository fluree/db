//! Compatibility wrapper for novelty-aware stats.

use crate::runtime_stats::assemble_fast_stats;
use crate::Novelty;
use fluree_db_core::{IndexStats, LedgerSnapshot};

/// Compute current stats by merging indexed stats with novelty updates.
///
/// This retains the historical sync API while delegating to the shared
/// fast novelty assembler used by runtime callers.
pub fn current_stats(indexed: &IndexStats, novelty: &Novelty) -> IndexStats {
    // Compatibility path: use a throwaway genesis snapshot so `indexed_t()` is treated
    // as zero and the full novelty window is merged, matching the historical
    // "current stats = indexed base + all novelty" behavior.
    let snapshot = LedgerSnapshot::genesis("stats:compat");
    assemble_fast_stats(indexed, &snapshot, novelty, i64::MAX, None)
}
