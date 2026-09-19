//! One version's data files, kept so later scans plan from memory.
//!
//! Replaying a log costs a read per commit since the last checkpoint, plus the
//! checkpoint. A kept listing turns a repeat scan of the same version into no
//! log I/O at all, and a scan of a later version into a read of only the
//! commits in between.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::scan::ScanMetadata;
use delta_kernel::{DeltaResult, EngineData};

const DEFAULT_BUDGET_MB: usize = 256;

/// Bytes of listings held across every table in the process.
static HELD: AtomicUsize = AtomicUsize::new(0);

/// `FLUREE_DELTA_LOG_CACHE_MB`; `0` keeps nothing between queries.
pub(crate) fn budget() -> usize {
    std::env::var("FLUREE_DELTA_LOG_CACHE_MB")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(DEFAULT_BUDGET_MB)
        .saturating_mul(1024 * 1024)
}

/// Every live data file of one version: an unfiltered scan's file rows with
/// their statistics, so any later predicate can still skip against them.
pub(crate) struct Listing {
    batches: Vec<RecordBatch>,
    bytes: usize,
}

impl Listing {
    /// Collect an unfiltered scan's metadata, or `None` once it outgrows what
    /// the process-wide budget has left.
    pub(crate) fn collect(
        metadata: impl Iterator<Item = DeltaResult<ScanMetadata>>,
    ) -> DeltaResult<Option<Arc<Self>>> {
        let room = budget().saturating_sub(HELD.load(Ordering::Relaxed));
        let mut batches = Vec::new();
        let mut bytes = 0usize;
        for item in metadata {
            let live = item?.scan_files.apply_selection_vector()?;
            let batch: RecordBatch = (*ArrowEngineData::try_from_engine_data(live)?).into();
            bytes = bytes.saturating_add(batch.get_array_memory_size());
            if bytes > room {
                return Ok(None);
            }
            batches.push(batch);
        }
        // Concurrent collectors may overshoot the budget by one listing each.
        HELD.fetch_add(bytes, Ordering::Relaxed);
        Ok(Some(Arc::new(Self { batches, bytes })))
    }

    pub(crate) fn data(&self) -> Vec<Box<dyn EngineData>> {
        self.batches
            .iter()
            .map(|batch| Box::new(ArrowEngineData::new(batch.clone())) as Box<dyn EngineData>)
            .collect()
    }
}

impl Drop for Listing {
    fn drop(&mut self) {
        HELD.fetch_sub(self.bytes, Ordering::Relaxed);
    }
}
