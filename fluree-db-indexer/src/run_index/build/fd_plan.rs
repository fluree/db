//! Static per-phase file-descriptor apportionment for the V3 build pipeline.
//!
//! `build_indexes_from_commits` runs its FD-heavy phases strictly
//! sequentially — class-membership scatter → SPOT merge → secondary run
//! generation → secondary order merges — so each phase may plan against
//! (nearly) the whole [`FdBudget`] and no runtime permit tracking is needed.
//! The only concurrent consumers are the dictionary upload running on the
//! tokio side (a handful of transient handles, covered by the budget's
//! reserve) and the up-to-three parallel order merges (handled by dividing
//! the merge share by that concurrency).
//!
//! Degradation reference (reserve = `min(soft/4, 64)`, `A = soft - reserve`):
//!
//! | soft limit          | A      | scatter | SPOT fan-in | workers | merge fan-in (3 orders) |
//! |---------------------|--------|---------|-------------|---------|-------------------------|
//! | 256 (raise failed)  | 192    | 184     | 184         | 24      | 60                      |
//! | 1024 (AWS Lambda)   | 960    | 256     | 952         | 120     | 316                     |
//! | 10240 (raised macOS)| 10 176 | 256     | 10 168      | 1272    | 3388                    |
//!
//! Every value floors well above zero (see [`FdBudget::available`]'s floor of
//! 32), so even a degenerate limit produces a plan that makes progress and
//! lets the kernel — not an overflow — report genuinely impossible limits.

use fluree_db_core::fd_limit::FdBudget;

/// Per-phase file-descriptor allowances for one `build_indexes_from_commits`
/// invocation. All values are counts of *simultaneously open* descriptors the
/// phase may plan for; transient open-read-close handles ride on the reserve.
#[derive(Clone, Copy, Debug)]
pub struct FdPlan {
    /// Max simultaneously open class-membership bucket writers (≤ 256; the
    /// scatter multiplexes its 256 logical buckets through a pool this size).
    pub scatter_pool: usize,
    /// Max sorted-commit readers the SPOT merge may hold open at once; chunk
    /// counts beyond this trigger the hierarchical (group-merge) fallback.
    pub spot_fan_in: usize,
    /// Cap on secondary-run-generation workers (each holds ~6-8 descriptors:
    /// commit reader + three order run writers + background-flush transients).
    pub worker_cap: usize,
    /// Max run files one order's k-way merge may hold open at once; run
    /// counts beyond this trigger the cascaded multi-pass merge. Sized for
    /// up to three order merges running concurrently.
    pub merge_fan_in_per_order: usize,
}

/// Compute the per-phase FD plan for a build with `worker_count` workers.
pub fn plan_fd_usage(budget: FdBudget, worker_count: usize) -> FdPlan {
    let a = budget.available();
    // Secondary-order merges run up to min(worker_count, 3) at a time
    // (three buildable secondary orders; see build_all_indexes).
    let order_concurrency = worker_count.clamp(1, 3);
    FdPlan {
        scatter_pool: a.saturating_sub(8).clamp(16, 256),
        spot_fan_in: a.saturating_sub(8).max(16),
        worker_cap: (a / 8).max(1),
        merge_fan_in_per_order: (a / order_concurrency).saturating_sub(4).max(8),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_matches_degradation_table() {
        // S=256 with a failed raise: the historical worst case must still plan.
        let p = plan_fd_usage(FdBudget::from_soft(256), 8);
        assert_eq!(p.scatter_pool, 184);
        assert_eq!(p.spot_fan_in, 184);
        assert_eq!(p.worker_cap, 24);
        assert_eq!(p.merge_fan_in_per_order, 60);

        // S=1024 (AWS Lambda's fixed hard limit).
        let p = plan_fd_usage(FdBudget::from_soft(1024), 8);
        assert_eq!(p.scatter_pool, 256);
        assert_eq!(p.spot_fan_in, 952);
        assert_eq!(p.worker_cap, 120);
        assert_eq!(p.merge_fan_in_per_order, 316);

        // S=10240 (macOS after a successful raise).
        let p = plan_fd_usage(FdBudget::from_soft(10_240), 16);
        assert_eq!(p.scatter_pool, 256);
        assert_eq!(p.spot_fan_in, 10_168);
        assert_eq!(p.worker_cap, 1272);
        assert_eq!(p.merge_fan_in_per_order, 3388);
    }

    #[test]
    fn plan_survives_degenerate_budgets() {
        let p = plan_fd_usage(FdBudget::from_soft(8), 1);
        assert!(p.scatter_pool >= 16);
        assert!(p.spot_fan_in >= 16);
        assert!(p.worker_cap >= 1);
        assert!(p.merge_fan_in_per_order >= 8);
    }

    #[test]
    fn unlimited_budget_never_constrains() {
        let p = plan_fd_usage(FdBudget::unlimited(), 32);
        assert_eq!(p.scatter_pool, 256);
        assert!(p.spot_fan_in > 1 << 40);
        assert!(p.merge_fan_in_per_order > 1 << 40);
    }

    #[test]
    fn single_worker_gets_full_merge_share() {
        let one = plan_fd_usage(FdBudget::from_soft(256), 1);
        let three = plan_fd_usage(FdBudget::from_soft(256), 3);
        assert!(one.merge_fan_in_per_order > three.merge_fan_in_per_order);
    }
}
