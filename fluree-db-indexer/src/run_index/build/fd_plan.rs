//! Static per-phase file-descriptor apportionment for the V3 build pipeline.
//!
//! `build_indexes_from_commits` runs its FD-heavy phases strictly
//! sequentially — class-membership scatter → SPOT merge → secondary run
//! generation → secondary order merges — so each phase may plan against
//! (nearly) the whole [`FdBudget`] and no runtime permit tracking is needed.
//! The only concurrent consumers are the dictionary upload running on the
//! tokio side (covered by the budget's reserve — see [`FdBudget`]) and the
//! parallel order merges (handled by dividing the merge share by that
//! concurrency).
//!
//! Degradation reference (reserve = `min(max(soft/4, 32), 64)`,
//! `A = soft − reserve`, three concurrent orders):
//!
//! | soft limit          | A      | scatter | SPOT fan-in | workers | merge fan-in/order |
//! |---------------------|--------|---------|-------------|---------|--------------------|
//! | 96 (regression test)| 64     | 56      | 32          | 8       | 17                 |
//! | 256 (raise failed)  | 192    | 184     | 96          | 24      | 60                 |
//! | 1024 (AWS Lambda)   | 960    | 256     | 480         | 120     | 316                |
//! | 10240 (raised macOS)| 10 176 | 256     | 5088        | 1272    | 3388               |
//!
//! Every value floors well above zero (see [`FdBudget::available`]'s floor of
//! 32), so even a degenerate limit produces a plan that makes progress and
//! lets the kernel — not an overflow — report genuinely impossible limits.

use fluree_db_core::fd_limit::FdBudget;

/// Per-phase file-descriptor allowances for one index build. All values are
/// counts of *simultaneously open* descriptors the phase may plan for;
/// transient open-read-close handles ride on the reserve.
#[derive(Clone, Copy, Debug)]
pub struct FdPlan {
    /// Max simultaneously open class-membership bucket writers (≤ 256; the
    /// scatter multiplexes its 256 logical buckets through a pool this size).
    pub scatter_pool: usize,
    /// Max sorted-commit readers the SPOT merge may hold open at once; chunk
    /// counts beyond this trigger the hierarchical (group-merge) fallback.
    pub spot_fan_in: usize,
    /// Cap on secondary-run-generation workers. Each worker's true peak is 4
    /// descriptors (1 sorted-commit reader + up to 3 concurrent run-flush
    /// files — `RunWriter` joins the previous flush thread before spawning
    /// the next, so flushes cannot pile up); the `/8` divisor in the formula
    /// deliberately budgets 2× that for headroom.
    pub worker_cap: usize,
    /// Max run files one order's k-way merge may hold open at once; run
    /// counts beyond this trigger the cascaded multi-pass merge. Sized as
    /// `A / concurrent_orders − 4`, where the −4 covers each in-flight
    /// cascade pass's single output writer (plus slack).
    pub merge_fan_in_per_order: usize,
}

/// Compute the per-phase FD plan for a build with `worker_count` workers.
///
/// `concurrent_orders` is the number of order merges `build_all_indexes` may
/// run at once for this pipeline: 3 on the import path (SPOT is built
/// separately, before the secondary orders), 4 on the remapped-rebuild path
/// (all four orders build together).
pub fn plan_fd_usage(budget: FdBudget, worker_count: usize, concurrent_orders: usize) -> FdPlan {
    let a = budget.available();
    let order_concurrency = worker_count.clamp(1, concurrent_orders.max(1));
    FdPlan {
        scatter_pool: a.saturating_sub(8).clamp(16, 256),
        // Half of A, not A-minus-slack: the SPOT phase could safely claim
        // nearly everything (phases are sequential), but the reserve is an
        // estimate — object-store connection pools and OTEL sockets draw
        // from it invisibly — and a 50% share keeps real margin at the cost
        // of at most one extra grouping pass when chunk counts land between
        // A/2 and A.
        spot_fan_in: (a / 2).max(16),
        worker_cap: (a / 8).max(1),
        merge_fan_in_per_order: (a / order_concurrency).saturating_sub(4).max(8),
    }
}

/// Order-merge concurrency on the import path: PSOT/POST/OPST build in
/// parallel after SPOT has already completed on its own.
pub const IMPORT_CONCURRENT_ORDERS: usize = 3;

/// Order-merge concurrency on the remapped-rebuild path: all four orders
/// (including SPOT) build through `build_all_indexes` together.
pub const REBUILD_CONCURRENT_ORDERS: usize = 4;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_matches_degradation_table() {
        // S=96: the shipped end-to-end regression limit.
        let p = plan_fd_usage(FdBudget::from_soft(96), 8, IMPORT_CONCURRENT_ORDERS);
        assert_eq!(p.scatter_pool, 56);
        assert_eq!(p.spot_fan_in, 32);
        assert_eq!(p.worker_cap, 8);
        assert_eq!(p.merge_fan_in_per_order, 17);

        // S=256 with a failed raise: the historical worst case must still plan.
        let p = plan_fd_usage(FdBudget::from_soft(256), 8, IMPORT_CONCURRENT_ORDERS);
        assert_eq!(p.scatter_pool, 184);
        assert_eq!(p.spot_fan_in, 96);
        assert_eq!(p.worker_cap, 24);
        assert_eq!(p.merge_fan_in_per_order, 60);

        // S=1024 (AWS Lambda's fixed hard limit).
        let p = plan_fd_usage(FdBudget::from_soft(1024), 8, IMPORT_CONCURRENT_ORDERS);
        assert_eq!(p.scatter_pool, 256);
        assert_eq!(p.spot_fan_in, 480);
        assert_eq!(p.worker_cap, 120);
        assert_eq!(p.merge_fan_in_per_order, 316);

        // S=10240 (macOS after a successful raise).
        let p = plan_fd_usage(FdBudget::from_soft(10_240), 16, IMPORT_CONCURRENT_ORDERS);
        assert_eq!(p.scatter_pool, 256);
        assert_eq!(p.spot_fan_in, 5088);
        assert_eq!(p.worker_cap, 1272);
        assert_eq!(p.merge_fan_in_per_order, 3388);
    }

    #[test]
    fn rebuild_divides_by_four_concurrent_orders() {
        // The remapped-rebuild path runs all four orders concurrently, so its
        // per-order share must be smaller than the import path's at the same
        // budget and worker count.
        let import = plan_fd_usage(FdBudget::from_soft(256), 8, IMPORT_CONCURRENT_ORDERS);
        let rebuild = plan_fd_usage(FdBudget::from_soft(256), 8, REBUILD_CONCURRENT_ORDERS);
        assert_eq!(rebuild.merge_fan_in_per_order, 192 / 4 - 4);
        assert!(rebuild.merge_fan_in_per_order < import.merge_fan_in_per_order);
        // 4 × (fan_in + 1 cascade writer) must fit in A.
        assert!(4 * (rebuild.merge_fan_in_per_order + 1) <= 192);
    }

    #[test]
    fn plan_survives_degenerate_budgets() {
        let p = plan_fd_usage(FdBudget::from_soft(8), 1, IMPORT_CONCURRENT_ORDERS);
        assert!(p.scatter_pool >= 16);
        assert!(p.spot_fan_in >= 16);
        assert!(p.worker_cap >= 1);
        assert!(p.merge_fan_in_per_order >= 8);
    }

    #[test]
    fn unlimited_budget_never_constrains() {
        let p = plan_fd_usage(FdBudget::unlimited(), 32, IMPORT_CONCURRENT_ORDERS);
        assert_eq!(p.scatter_pool, 256);
        assert!(p.spot_fan_in > 1 << 40);
        assert!(p.merge_fan_in_per_order > 1 << 40);
    }

    #[test]
    fn single_worker_gets_full_merge_share() {
        let one = plan_fd_usage(FdBudget::from_soft(256), 1, IMPORT_CONCURRENT_ORDERS);
        let three = plan_fd_usage(FdBudget::from_soft(256), 3, IMPORT_CONCURRENT_ORDERS);
        assert!(one.merge_fan_in_per_order > three.merge_fan_in_per_order);
    }
}
