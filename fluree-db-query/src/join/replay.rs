//! Bounded, query-local replay of an independent triple scan.
//!
//! Record while the first scan streams; only replay after EOF. Overflow or a
//! heap-backed binding abandons the cache, leaving the original scan untouched.

use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use fluree_db_core::QueryCancellation;

const MAX_ROWS: usize = 8192;
const MAX_BYTES: usize = 4 * 1024 * 1024;

pub(super) struct ScanReplay {
    batches: Vec<Batch>,
    rows: usize,
    bytes: usize,
    complete: bool,
    cancellation: QueryCancellation,
}

impl ScanReplay {
    pub(super) fn new(ctx: &ExecutionContext<'_>) -> Self {
        Self {
            batches: Vec::new(),
            rows: 0,
            bytes: 0,
            complete: false,
            cancellation: ctx.cancellation.clone(),
        }
    }

    /// False means the caller must drop the cache and keep streaming normally.
    pub(super) fn record(&mut self, batch: &Batch) -> bool {
        let bytes = batch
            .len()
            .saturating_mul(batch.schema().len())
            .saturating_mul(std::mem::size_of::<Binding>())
            // Allow for the outer Vec's spare capacity as it grows.
            .saturating_add(2 * std::mem::size_of::<Batch>())
            .saturating_add(batch.schema().len() * std::mem::size_of::<Vec<Binding>>());
        let budget = self
            .cancellation
            .memory_limit()
            .unwrap_or_else(crate::context::query_memory_budget_bytes);
        if self.rows.saturating_add(batch.len()) > MAX_ROWS
            || self.bytes.saturating_add(bytes) > MAX_BYTES
            || (budget != 0 && self.cancellation.allocated_bytes().saturating_add(bytes) > budget)
        {
            return false;
        }
        // Keep the memory ceiling meaningful even for very large strings,
        // vectors and decimals. Those scans retain the ordinary per-row path.
        for col in 0..batch.schema().len() {
            for row in 0..batch.len() {
                if !matches!(
                    batch.get_by_col(row, col),
                    Binding::EncodedSid { .. }
                        | Binding::EncodedPid { .. }
                        | Binding::EncodedLit { .. }
                        | Binding::Unbound
                        | Binding::Poisoned
                ) {
                    return false;
                }
            }
        }
        self.batches.push(batch.clone());
        self.rows += batch.len();
        self.bytes += bytes;
        self.cancellation.record_alloc(bytes);
        true
    }

    pub(super) fn finish(&mut self) {
        self.complete = true;
    }

    pub(super) fn is_complete(&self) -> bool {
        self.complete
    }

    pub(super) fn batch(&self, index: usize) -> Option<Batch> {
        self.batches.get(index).cloned()
    }
}

impl Drop for ScanReplay {
    fn drop(&mut self) {
        self.cancellation.release(self.bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::var_registry::{VarId, VarRegistry};
    use fluree_db_core::LedgerSnapshot;
    use std::sync::Arc;

    fn batch(rows: usize) -> Batch {
        Batch::new(
            Arc::from([VarId(0)]),
            vec![vec![Binding::EncodedPid { p_id: 17 }; rows]],
        )
        .unwrap()
    }

    #[test]
    fn replay_requires_eof_and_releases_memory_on_drop() {
        let snapshot = LedgerSnapshot::genesis("replay:main");
        let vars = VarRegistry::new();
        let ctx =
            ExecutionContext::new(&snapshot, &vars).with_cancellation(QueryCancellation::new());
        let mut replay = ScanReplay::new(&ctx);
        assert!(replay.record(&batch(2)));
        assert!(replay.record(&batch(3)));
        assert!(!replay.is_complete());
        assert!(ctx.mem_used() > 0);
        replay.finish();
        assert!(replay.is_complete());
        assert_eq!(replay.batch(0).unwrap().len(), 2);
        assert_eq!(replay.batch(1).unwrap().len(), 3);
        assert!(replay.batch(2).is_none());
        drop(replay);
        assert_eq!(ctx.mem_used(), 0);
    }

    #[test]
    fn capacity_and_memory_budget_decline_without_retaining_new_rows() {
        let snapshot = LedgerSnapshot::genesis("replay:main");
        let vars = VarRegistry::new();
        let ctx =
            ExecutionContext::new(&snapshot, &vars).with_cancellation(QueryCancellation::new());
        let mut replay = ScanReplay::new(&ctx);
        assert!(replay.record(&batch(MAX_ROWS)));
        let used = ctx.mem_used();
        assert!(!replay.record(&batch(1)));
        assert_eq!(ctx.mem_used(), used);
        drop(replay);
        assert_eq!(ctx.mem_used(), 0);
        ctx.cancellation.set_memory_limit(1);
        assert!(!ScanReplay::new(&ctx).record(&batch(1)));
        assert_eq!(ctx.mem_used(), 0);
    }

    #[test]
    fn empty_schema_multiplicity_and_materialized_binding_guard() {
        let snapshot = LedgerSnapshot::genesis("replay:main");
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);
        let mut replay = ScanReplay::new(&ctx);
        assert!(replay.record(&Batch::empty_schema_with_len(7)));
        replay.finish();
        assert_eq!(replay.batch(0).unwrap().len(), 7);
        let materialized = Batch::single_row(
            Arc::from([VarId(0)]),
            vec![Binding::Iri(Arc::from("http://example.org/large"))],
        )
        .unwrap();
        assert!(!ScanReplay::new(&ctx).record(&materialized));
        let mut empty = ScanReplay::new(&ctx);
        empty.finish();
        assert!(empty.is_complete());
        assert!(empty.batch(0).is_none());
    }
}
