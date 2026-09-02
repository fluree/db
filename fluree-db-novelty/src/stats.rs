//! Compatibility wrapper for novelty-aware stats.

use crate::runtime_stats::assemble_fast_stats;
use crate::Novelty;
use fluree_db_core::{IndexStats, LedgerSnapshot};

/// Compute current stats by merging indexed stats with novelty updates.
///
/// This retains the historical sync API while delegating to the shared
/// fast novelty assembler used by runtime callers.
///
/// **Estimate-grade.** The throwaway genesis snapshot below carries no range
/// provider, so this cannot reconcile novelty assertions against the base
/// index: a fact asserted again after it was indexed is counted twice, and a
/// retraction of a fact that was never there subtracts one (#1391). It also
/// attributes every novelty flake to graph 0, since that snapshot has no
/// graph registry. Callers that render counts to users should merge through
/// [`crate::assemble_fast_stats_with`] /
/// [`crate::assemble_full_stats_with`] with
/// [`crate::NoveltyMerge::Reconciled`] and the ledger's real snapshot —
/// `ledger_info` and the Cypher catalog shims do.
pub fn current_stats(indexed: &IndexStats, novelty: &Novelty) -> IndexStats {
    // Compatibility path: use a throwaway genesis snapshot so `indexed_t()` is treated
    // as zero and the full novelty window is merged, matching the historical
    // "current stats = indexed base + all novelty" behavior.
    let snapshot = LedgerSnapshot::genesis("stats:compat");
    assemble_fast_stats(indexed, &snapshot, novelty, i64::MAX, None)
}
