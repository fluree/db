//! Window sizing for lanes that gather input and answer it with one probe.
//!
//! The batched nested-loop join, OPTIONAL seed coalescing, the property-join
//! driver, and a correlated R2RML scan's child each pull a window of input
//! rows, answer the window with one batched probe or scan, then emit. A wide window amortizes the
//! probe; a narrow one lets an outer `LIMIT` stop after little work.
//! [`FlushSchedule`] opens narrow when a lane asks for it and widens each
//! later window geometrically back to the lane's cap, so a lane that keeps
//! pulling pays for only a few extra flushes.

use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::operator::BoxedOperator;
use crate::var_registry::VarId;
use std::sync::Arc;

/// Minimum first window for probes whose setup should be amortized. Lanes
/// bounded by their input subjects can start smaller with `ramped_from`.
pub(crate) const MIN_FLUSH: usize = 1024;
/// Growth between consecutive windows.
pub(crate) const FLUSH_GROWTH: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FlushSchedule {
    size: usize,
    cap: usize,
}

impl FlushSchedule {
    /// Every window at `cap`.
    pub(crate) fn fixed(cap: usize) -> Self {
        Self { size: cap, cap }
    }

    /// First window at [`MIN_FLUSH`]. For lanes whose probe costs the same in
    /// total however the input is split, so ramping is free on a full drain.
    pub(crate) fn ramped(cap: usize) -> Self {
        Self {
            size: MIN_FLUSH.min(cap),
            cap,
        }
    }

    /// Start at the requested size for lanes whose probes are bounded by
    /// their input subjects, then grow geometrically if more rows are needed.
    pub(crate) fn ramped_from(initial: usize, cap: usize) -> Self {
        Self {
            size: initial.max(1).min(cap),
            cap,
        }
    }

    /// First window sized to a `LIMIT` row budget (see
    /// [`Operator::set_row_budget`](crate::operator::Operator::set_row_budget)).
    pub(crate) fn budgeted(budget: usize, cap: usize) -> Self {
        Self {
            size: budget.clamp(MIN_FLUSH.min(cap), cap),
            cap,
        }
    }

    pub(crate) fn size(&self) -> usize {
        self.size
    }

    /// Widen the next window; call after each flush.
    pub(crate) fn advance(&mut self) {
        self.size = self.size.saturating_mul(FLUSH_GROWTH).min(self.cap);
    }

    /// Size the next window for a row budget from the yield so far: `seen`
    /// inputs produced `yielded` of the `wanted` rows. The window covers the
    /// remaining rows at that rate twice over, never past the geometric step,
    /// which also applies until something has matched. A fixed step alone can
    /// overshoot the rows still needed nearly eightfold on a selective input.
    pub(crate) fn advance_toward(&mut self, wanted: usize, seen: usize, yielded: usize) {
        let step = self.size.saturating_mul(FLUSH_GROWTH).min(self.cap);
        self.size = match wanted.checked_sub(yielded) {
            Some(remaining) if remaining > 0 && yielded > 0 => remaining
                .saturating_mul(seen)
                .div_ceil(yielded)
                .saturating_mul(2)
                .clamp(1, step),
            _ => step,
        };
    }
}

/// Pull rows from `source` into one batch until it holds `window` rows or the
/// source runs dry, starting with the rows an earlier window left in
/// `pending`. All of a source's batches share one schema, so this is a
/// column-wise append, and a batch whose schema differs from the first is an
/// `Internal` error. With `exact`, the batch that crosses `window` is split
/// and its rest kept in `pending`; otherwise it is taken whole. `None` only
/// when the source is exhausted with zero rows.
pub(crate) async fn pull_window(
    source: &mut BoxedOperator,
    pending: &mut Option<(Batch, usize)>,
    window: usize,
    exact: bool,
    ctx: &ExecutionContext<'_>,
) -> Result<Option<Batch>> {
    let mut schema: Option<Arc<[VarId]>> = None;
    let mut columns: Vec<Vec<Binding>> = Vec::new();
    let mut rows = 0usize;
    while rows < window {
        let (batch, start) = match pending.take() {
            Some(pending) => pending,
            None => match source.next_batch(ctx).await? {
                Some(batch) => (batch, 0),
                None => break,
            },
        };
        let end = if exact {
            batch.len().min(start + window - rows)
        } else {
            batch.len()
        };
        match &schema {
            None => {
                schema = Some(batch.schema_arc());
                columns = (0..batch.schema().len()).map(|_| Vec::new()).collect();
            }
            // Columns are appended by index, so a batch in another variable
            // order would mix columns silently; refuse it instead, as the SQL
            // lane does.
            Some(first) if batch.schema() != first.as_ref() => {
                return Err(QueryError::Internal(format!(
                    "a source changed its batch schema mid-stream: first {:?}, then {:?}",
                    first.as_ref(),
                    batch.schema()
                )));
            }
            Some(_) => {}
        }
        for (c, col) in columns.iter_mut().enumerate() {
            let column = batch.column_by_idx(c).expect("column index within schema");
            col.extend_from_slice(&column[start..end]);
        }
        rows += end - start;
        if end < batch.len() {
            *pending = Some((batch, end));
        }
    }
    let Some(schema) = schema.filter(|_| rows > 0) else {
        return Ok(None);
    };
    // `from_parts` keeps the row count of a 0-column source.
    Ok(Some(Batch::from_parts(schema, columns, rows)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sizes(mut schedule: FlushSchedule, n: usize) -> Vec<usize> {
        (0..n)
            .map(|_| {
                let size = schedule.size();
                schedule.advance();
                size
            })
            .collect()
    }

    #[test]
    fn fixed_never_changes() {
        assert_eq!(sizes(FlushSchedule::fixed(100_000), 3), [100_000; 3]);
    }

    #[test]
    fn ramped_grows_geometrically_to_cap() {
        assert_eq!(
            sizes(FlushSchedule::ramped(100_000), 5),
            [1024, 8192, 65_536, 100_000, 100_000]
        );
    }

    #[test]
    fn budgeted_clamps_first_window() {
        assert_eq!(FlushSchedule::budgeted(10, 100_000).size(), MIN_FLUSH);
        assert_eq!(FlushSchedule::budgeted(5000, 100_000).size(), 5000);
        assert_eq!(FlushSchedule::budgeted(usize::MAX, 100_000).size(), 100_000);
    }

    #[test]
    fn subject_windows_start_at_the_goal_and_keep_growing() {
        assert_eq!(
            sizes(FlushSchedule::ramped_from(10, 100_000), 6),
            [10, 80, 640, 5120, 40_960, 100_000]
        );
        assert_eq!(FlushSchedule::ramped_from(0, 100_000).size(), 1);
        assert_eq!(
            FlushSchedule::ramped_from(usize::MAX, 100_000).size(),
            100_000
        );
    }

    #[test]
    fn a_known_yield_sizes_the_window_to_the_rows_still_wanted() {
        let mut schedule = FlushSchedule::ramped_from(5120, 100_000);
        // 6 of 10 rows from 5,850 inputs: 4 more need ~3,900, doubled.
        schedule.advance_toward(10, 5850, 6);
        assert_eq!(schedule.size(), 7800);

        // The estimate never exceeds the geometric step.
        let mut schedule = FlushSchedule::ramped_from(10, 100_000);
        schedule.advance_toward(1000, 10, 1);
        assert_eq!(schedule.size(), 80);
    }

    #[test]
    fn an_unknown_or_met_yield_takes_the_geometric_step() {
        for (wanted, yielded) in [(10, 0), (10, 10), (10, 12)] {
            let mut schedule = FlushSchedule::ramped_from(10, 100_000);
            schedule.advance_toward(wanted, 90, yielded);
            assert_eq!(schedule.size(), 80, "wanted {wanted}, yielded {yielded}");
        }
        let mut schedule = FlushSchedule::ramped_from(10, 100_000);
        schedule.advance_toward(usize::MAX, usize::MAX, 1);
        assert_eq!(schedule.size(), 80);
    }

    /// Columns are appended by index, so a second batch in another variable
    /// order would land its values under the wrong variables.
    #[tokio::test]
    async fn a_window_refuses_a_batch_in_another_schema() {
        use crate::seed::BatchReplayOperator;
        use crate::var_registry::VarRegistry;

        let snapshot = fluree_db_core::LedgerSnapshot::genesis("test:main");
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);
        let ab: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1)].into_boxed_slice());
        let ba: Arc<[VarId]> = Arc::from(vec![VarId(1), VarId(0)].into_boxed_slice());
        let one_row = |schema: &Arc<[VarId]>| {
            Batch::new(
                Arc::clone(schema),
                vec![
                    vec![Binding::iri("http://ex/a")],
                    vec![Binding::iri("http://ex/b")],
                ],
            )
            .unwrap()
        };
        let mut source: BoxedOperator = Box::new(BatchReplayOperator::new(
            Arc::clone(&ab),
            vec![one_row(&ab), one_row(&ba)],
        ));
        source.open(&ctx).await.unwrap();
        let err = pull_window(&mut source, &mut None, 10, false, &ctx)
            .await
            .expect_err("a second schema must not be appended by index");
        assert!(matches!(err, QueryError::Internal(_)), "{err:?}");
    }

    #[test]
    fn cap_below_min_flush_wins() {
        assert_eq!(FlushSchedule::ramped(100).size(), 100);
        assert_eq!(FlushSchedule::budgeted(10, 100).size(), 100);
    }
}
