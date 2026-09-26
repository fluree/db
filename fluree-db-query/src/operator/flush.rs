//! Window sizing for lanes that gather input and answer it with one probe.
//!
//! The batched nested-loop join, OPTIONAL seed coalescing, and the
//! property-join driver each pull a window of input rows, answer the window
//! with one batched probe or scan, then emit. A wide window amortizes the
//! probe; a narrow one lets an outer `LIMIT` stop after little work.
//! [`FlushSchedule`] opens narrow when a lane asks for it and widens each
//! later window geometrically back to the lane's cap, so a lane that keeps
//! pulling pays for only a few extra flushes.

/// Narrowest first window: per-flush setup isn't worth paying for fewer rows,
/// even under `LIMIT 1`.
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
    fn cap_below_min_flush_wins() {
        assert_eq!(FlushSchedule::ramped(100).size(), 100);
        assert_eq!(FlushSchedule::budgeted(10, 100).size(), 100);
    }
}
