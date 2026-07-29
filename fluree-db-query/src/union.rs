//! UNION operator - executes branches with correlated input
//!
//! Implements SPARQL UNION semantics:
//! - For each input row, execute each branch with that row as a seed
//! - Concatenate results from all branches
//! - Normalize output batches to a unified schema (padding missing vars with Unbound)
//!
//! Correlation is essential: each branch must see the bindings from the current
//! input solution (row).

use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::Result;
use crate::execute::build_where_operators_seeded;
use crate::ir::Pattern;
use crate::operator::{compute_trimmed_vars, BoxedOperator, Operator, OperatorState};
use crate::seed::SeedOperator;
use crate::temporal_mode::PlanningContext;
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_core::StatsView;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::Arc;

/// UNION operator - executes branches for each input row (correlated).
pub struct UnionOperator {
    /// Child operator providing input solutions
    child: BoxedOperator,
    /// Branch patterns (each branch is its own pattern list)
    branches: Vec<Vec<Pattern>>,
    /// Unified schema across child + all branch patterns
    unified_schema: Arc<[VarId]>,
    /// Effective output schema (trimmed if `downstream_vars` is set, otherwise same as `schema`)
    effective_schema: Arc<[VarId]>,
    /// Operator state
    state: OperatorState,
    /// Buffered output batches produced from processing input rows
    output_buffer: VecDeque<Batch>,
    /// Current input batch being processed
    current_input_batch: Option<Batch>,
    /// Current row index in the input batch
    current_input_row: usize,
    /// True once the child is exhausted; pending output may still need draining.
    input_exhausted: bool,
    /// Optional stats for selectivity-based pattern reordering in branches
    stats: Option<Arc<StatsView>>,
    /// Planning context captured at planner-time. Used when building the
    /// per-row branch operator trees so they inherit the same temporal mode.
    planning: PlanningContext,
    /// Debug counters for low-noise batch fragmentation summaries.
    input_batches_seen: usize,
    input_rows_seen: usize,
    branch_execs: usize,
    output_batches_buffered: usize,
    output_rows_buffered: usize,
    max_input_batch_len: usize,
    max_output_batch_len: usize,
    output_batches_emitted: usize,
    output_rows_emitted: usize,
    max_emitted_batch_len: usize,
    pending_output_rows: usize,
    /// F17: advisory row budget from a top-of-tree `LIMIT`, forwarded to *each*
    /// branch operator tree (any single branch may supply all `budget` rows, so
    /// the whole budget is passed, not split). Set before `open()` via
    /// `set_row_budget`; `None` = unbudgeted (full branch drain).
    row_budget: Option<usize>,
    /// F17 secondary lever: set once the buffered output meets `row_budget`, so
    /// the union stops building further branches and pulling further input rows
    /// (the consuming LIMIT will not take more). Reset in `open`.
    budget_met: bool,
}

impl UnionOperator {
    /// Create a new correlated UNION operator.
    ///
    /// # Arguments
    ///
    /// * `child` - Input solutions operator
    /// * `branches` - Branch pattern lists (at least one required)
    /// * `stats` - Optional stats for selectivity-based pattern reordering in branches
    pub fn new(
        child: BoxedOperator,
        branches: Vec<Vec<Pattern>>,
        stats: Option<Arc<StatsView>>,
        planning: PlanningContext,
    ) -> Self {
        assert!(!branches.is_empty(), "UNION requires at least one branch");

        // Build unified schema: start with child schema (preserve order),
        // then add any vars referenced/introduced in branch patterns.
        let mut unified_vars: Vec<VarId> = child.schema().to_vec();
        let mut seen: HashSet<VarId> = unified_vars.iter().copied().collect();

        for branch in &branches {
            extend_schema_from_patterns(&mut unified_vars, &mut seen, branch);
        }

        let unified_schema: Arc<[VarId]> = Arc::from(unified_vars.into_boxed_slice());
        let effective_schema = unified_schema.clone();

        Self {
            child,
            branches,
            unified_schema,
            effective_schema,
            state: OperatorState::Created,
            output_buffer: VecDeque::new(),
            current_input_batch: None,
            current_input_row: 0,
            input_exhausted: false,
            stats,
            planning,
            input_batches_seen: 0,
            input_rows_seen: 0,
            branch_execs: 0,
            output_batches_buffered: 0,
            output_rows_buffered: 0,
            max_input_batch_len: 0,
            max_output_batch_len: 0,
            output_batches_emitted: 0,
            output_rows_emitted: 0,
            max_emitted_batch_len: 0,
            pending_output_rows: 0,
            row_budget: None,
            budget_met: false,
        }
    }

    /// Trim the output schema to only the required downstream variables.
    ///
    /// Variables not in `downstream_vars` are excluded from the output schema,
    /// avoiding unnecessary Unbound padding in `normalize_batch` and carrying
    /// fewer columns through the rest of the pipeline.
    pub fn with_out_schema(mut self, downstream_vars: Option<&[VarId]>) -> Self {
        if let Some(trimmed) = compute_trimmed_vars(&self.unified_schema, downstream_vars) {
            self.effective_schema = trimmed;
        }
        self
    }

    /// Normalize a batch to the effective schema (pad missing vars with Unbound).
    fn normalize_batch(&self, batch: Batch) -> Result<Batch> {
        // Variable-free solutions (e.g. an all-bound existence branch like
        // `<s> a <C>`) carry rows but no columns. `Batch::new` infers the row
        // count from the first column and so reports len=0 for a zero-column
        // batch, which would silently drop those existence rows. Preserve the
        // count explicitly — mirrors join.rs / optional.rs / seed.rs, which use
        // `empty_schema_with_len` for the same reason.
        if self.effective_schema.is_empty() {
            return Ok(Batch::empty_schema_with_len(batch.len()));
        }

        if batch.is_empty() {
            return Ok(Batch::empty(self.effective_schema.clone())?);
        }

        // Map each effective output var to its source column (if present) or Unbound padding
        let columns: Vec<Vec<Binding>> = self
            .effective_schema
            .iter()
            .map(|&var| {
                batch
                    .schema()
                    .iter()
                    .position(|&v| v == var)
                    .and_then(|src_idx| batch.column_by_idx(src_idx))
                    .map(<[Binding]>::to_vec)
                    .unwrap_or_else(|| vec![Binding::Unbound; batch.len()])
            })
            .collect();

        Ok(Batch::new(self.effective_schema.clone(), columns)?)
    }

    fn take_output_batch(&mut self, batch_size: usize) -> Result<Option<Batch>> {
        if self.output_buffer.is_empty() {
            return Ok(None);
        }

        let mut output_columns: Vec<Vec<Binding>> = self
            .effective_schema
            .iter()
            .map(|_| Vec::with_capacity(batch_size))
            .collect();
        let mut rows_added = 0usize;

        while rows_added < batch_size {
            let Some(batch) = self.output_buffer.pop_front() else {
                break;
            };

            let (schema, mut columns, batch_len) = batch.into_parts();
            debug_assert_eq!(&*schema, &*self.effective_schema);

            let rows_to_take = (batch_size - rows_added).min(batch_len);
            for (dest, source) in output_columns.iter_mut().zip(columns.iter_mut()) {
                dest.extend(source.drain(..rows_to_take));
            }
            rows_added += rows_to_take;
            self.pending_output_rows -= rows_to_take;

            if rows_to_take < batch_len {
                let remainder = Batch::from_parts(schema, columns, batch_len - rows_to_take)?;
                self.output_buffer.push_front(remainder);
                break;
            }
        }

        if rows_added == 0 {
            return Ok(None);
        }

        let batch = Batch::from_parts(self.effective_schema.clone(), output_columns, rows_added)?;
        self.output_batches_emitted += 1;
        self.output_rows_emitted += batch.len();
        self.max_emitted_batch_len = self.max_emitted_batch_len.max(batch.len());
        Ok(Some(batch))
    }
}

#[async_trait]
impl Operator for UnionOperator {
    fn plan_children(&self) -> Vec<crate::plan_node::PlanChild<'_>> {
        vec![crate::plan_node::PlanChild::child(self.child.as_ref())]
    }
    fn schema(&self) -> &[VarId] {
        &self.effective_schema
    }

    fn set_row_budget(&mut self, budget: usize) {
        // F17: forward the LIMIT budget to each branch (applied per branch build
        // in `next_batch`). Sound because the consuming LIMIT truncates the
        // branch concatenation to `budget`, so a branch stopping at `budget` rows
        // can never drop a row the LIMIT would keep. Switch-gated for OFF-parity.
        if crate::r2rml::union_budget_enabled() {
            self.row_budget = Some(budget);
        }
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        self.child.open(ctx).await?;
        self.state = OperatorState::Open;
        self.output_buffer.clear();
        self.current_input_batch = None;
        self.current_input_row = 0;
        self.input_exhausted = false;
        self.budget_met = false;
        self.input_batches_seen = 0;
        self.input_rows_seen = 0;
        self.branch_execs = 0;
        self.output_batches_buffered = 0;
        self.output_rows_buffered = 0;
        self.max_input_batch_len = 0;
        self.max_output_batch_len = 0;
        self.output_batches_emitted = 0;
        self.output_rows_emitted = 0;
        self.max_emitted_batch_len = 0;
        self.pending_output_rows = 0;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if self.state != OperatorState::Open {
            return Ok(None);
        }

        loop {
            if self.pending_output_rows >= ctx.batch_size
                || ((self.input_exhausted || self.budget_met) && self.pending_output_rows > 0)
            {
                let batch = self
                    .take_output_batch(ctx.batch_size)?
                    .expect("pending_output_rows tracks buffered union rows");
                return Ok(Some(batch));
            }

            // Stop once the child is drained OR the row budget is met (F17 lever):
            // in both cases there is nothing more to produce, only the buffer to
            // drain (handled above).
            if self.input_exhausted || self.budget_met {
                self.state = OperatorState::Exhausted;
                return Ok(None);
            }

            // Ensure we have an input batch to process.
            if self.current_input_batch.is_none()
                || self.current_input_row
                    >= self
                        .current_input_batch
                        .as_ref()
                        .map(super::binding::Batch::len)
                        .unwrap_or(0)
            {
                // Fetch next non-empty batch from child.
                let next = match self.child.next_batch(ctx).await? {
                    Some(b) if !b.is_empty() => b,
                    Some(_) => continue,
                    None => {
                        self.input_exhausted = true;
                        continue;
                    }
                };
                self.input_batches_seen += 1;
                self.max_input_batch_len = self.max_input_batch_len.max(next.len());
                self.current_input_batch = Some(next);
                self.current_input_row = 0;
            }

            // Process one input row: execute all branches with this row as seed.
            let input_batch = self.current_input_batch.as_ref().unwrap().clone();
            let row_idx = self.current_input_row;
            self.current_input_row += 1;
            self.input_rows_seen += 1;

            // Pass effective schema as required vars so branches trim internally
            let branch_downstream_vars: Option<&[VarId]> =
                if self.effective_schema.len() < self.unified_schema.len() {
                    Some(&self.effective_schema)
                } else {
                    None
                };

            for branch_patterns in &self.branches {
                self.branch_execs += 1;
                let seed = SeedOperator::from_batch_row(&input_batch, row_idx);
                let mut branch_op = build_where_operators_seeded(
                    Some(Box::new(seed)),
                    branch_patterns,
                    self.stats.clone(),
                    branch_downstream_vars,
                    &self.planning,
                )?;

                // F17: give each freshly-built branch tree the LIMIT budget so
                // its scan caps its materialize window at `budget` rows instead
                // of draining the whole table. Re-applied per branch (each branch
                // independently gets the full budget); no cross-branch decrement.
                if let Some(b) = self.row_budget {
                    branch_op.set_row_budget(b);
                }
                branch_op.open(ctx).await?;
                while let Some(batch) = branch_op.next_batch(ctx).await? {
                    ctx.check_cancelled()?;
                    if batch.is_empty() {
                        continue;
                    }
                    let normalized = self.normalize_batch(batch)?;
                    self.output_batches_buffered += 1;
                    self.output_rows_buffered += normalized.len();
                    self.max_output_batch_len = self.max_output_batch_len.max(normalized.len());
                    self.pending_output_rows += normalized.len();
                    self.output_buffer.push_back(normalized);
                    ctx.check_cancelled()?;
                }
                branch_op.close();

                // F17 secondary lever: once the buffered output meets the row
                // budget, stop building further branches (and pulling further
                // input rows) — the consuming LIMIT will not take more, so it is
                // pure waste. SOUND on two counts: (1) a budget reaches the union
                // ONLY when every operator between it and the LIMIT is
                // row/order-preserving (Sort, Distinct, GroupAggregate, … ABSORB
                // the budget), so the consumer wants an arbitrary k-subset
                // (rows_only) and any `budget` buffered rows are a valid answer;
                // (2) the check fires only when the budget is actually MET — an
                // under-filled branch (fewer than `budget` rows so far) does NOT
                // trip it, so a rare branch-1 still lets branch-2 run and add its
                // rows. Whole-query correctness holds for a correlated union too:
                // once `budget` rows exist, no later input row is needed either.
                //
                // COUPLING (read before "simplifying" the budget path): count (1)
                // rests entirely on the forward/absorb classification documented at
                // `Operator::set_row_budget` — a budget only reaches this union under
                // row/order-preserving operators. An operator author who FORWARDS a
                // budget through an order-SENSITIVE operator would silently break
                // count (1) here, not the forwarding site. That classification is the
                // load-bearing contract; it lives in the trait doc, so this lever
                // stays sound only as long as the doc's absorb list does.
                //
                // One operator is neither pure-forward nor pure-absorb in that
                // taxonomy: `GraphOperator` ABSORBS w.r.t. its own `self.child` but
                // threads the budget into the per-parent-batch CORRELATED INNER
                // subplan (`graph.rs`), a route the absorb-list framing above does
                // not name. A union inside a `GRAPH` scope therefore receives a
                // budget by that path. It stays sound because each parent batch
                // rebuilds the inner tree with a FRESH budget and every inner row
                // flows up to the same downstream LIMIT — so the buffered rows are
                // still a valid k-subset — but that is a second, subtler argument
                // than count (1) above, so verify it there when touching `graph.rs`.
                if self
                    .row_budget
                    .is_some_and(|b| self.pending_output_rows >= b)
                {
                    self.budget_met = true;
                    break;
                }
            }
        }
    }

    fn close(&mut self) {
        self.child.close();
        self.output_buffer.clear();
        self.state = OperatorState::Closed;
        self.pending_output_rows = 0;
    }

    fn estimated_rows(&self) -> Option<usize> {
        // Upper bound: child rows * number of branches.
        self.child
            .estimated_rows()
            .map(|r| r.saturating_mul(self.branches.len()))
    }
}

fn extend_schema_from_patterns(
    schema: &mut Vec<VarId>,
    seen: &mut HashSet<VarId>,
    patterns: &[Pattern],
) {
    for p in patterns {
        for v in p.produced_vars() {
            if seen.insert(v) {
                schema.push(v);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binding::Binding;
    use crate::context::ExecutionContext;
    use crate::seed::EmptyOperator;
    use crate::values::ValuesOperator;
    use crate::var_registry::VarRegistry;
    use fluree_db_core::FlakeValue;
    use fluree_db_core::Sid;
    use std::sync::Arc;

    #[test]
    fn test_union_operator_schema_computation() {
        // Child schema has ?s, branches introduce ?n and ?e.
        let child_schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let child: BoxedOperator = Box::new(TestEmptyWithSchema {
            schema: child_schema,
        });

        let branches = vec![
            vec![Pattern::Triple(crate::ir::triple::TriplePattern::new(
                crate::ir::triple::Ref::Var(VarId(0)),
                crate::ir::triple::Ref::Sid(Sid::new(100, "name")),
                crate::ir::triple::Term::Var(VarId(1)),
            ))],
            vec![Pattern::Triple(crate::ir::triple::TriplePattern::new(
                crate::ir::triple::Ref::Var(VarId(0)),
                crate::ir::triple::Ref::Sid(Sid::new(100, "email")),
                crate::ir::triple::Term::Var(VarId(2)),
            ))],
        ];

        let op = UnionOperator::new(
            child,
            branches,
            None,
            crate::temporal_mode::PlanningContext::current(),
        );
        assert_eq!(op.schema(), &[VarId(0), VarId(1), VarId(2)]);
    }

    #[test]
    fn test_union_operator_allows_position_0_via_empty_seed_child() {
        // UNION at position 0 should still be able to run using an EmptyOperator child.
        // Here we only validate it constructs; runtime behavior is covered by execute.rs integration tests.
        let empty = EmptyOperator::new();
        let child: BoxedOperator = Box::new(empty);
        let branches = vec![vec![], vec![]];
        let op = UnionOperator::new(
            child,
            branches,
            None,
            crate::temporal_mode::PlanningContext::current(),
        );
        assert_eq!(op.schema().len(), 0);
    }

    #[test]
    fn test_union_with_out_schema_trims_schema() {
        // Unified schema: [?s(0), ?n(1), ?e(2)]
        // Required vars: [?s(0), ?e(2)]
        // Expected effective schema: [?s(0), ?e(2)] (preserves unified order)
        let child_schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let child: BoxedOperator = Box::new(TestEmptyWithSchema {
            schema: child_schema,
        });

        let branches = vec![
            vec![Pattern::Triple(crate::ir::triple::TriplePattern::new(
                crate::ir::triple::Ref::Var(VarId(0)),
                crate::ir::triple::Ref::Sid(Sid::new(100, "name")),
                crate::ir::triple::Term::Var(VarId(1)),
            ))],
            vec![Pattern::Triple(crate::ir::triple::TriplePattern::new(
                crate::ir::triple::Ref::Var(VarId(0)),
                crate::ir::triple::Ref::Sid(Sid::new(100, "email")),
                crate::ir::triple::Term::Var(VarId(2)),
            ))],
        ];

        let op = UnionOperator::new(
            child,
            branches,
            None,
            crate::temporal_mode::PlanningContext::current(),
        )
        .with_out_schema(Some(&[VarId(0), VarId(2)]));

        assert_eq!(op.schema(), &[VarId(0), VarId(2)]);
    }

    #[test]
    fn test_union_with_out_schema_none_preserves_full_schema() {
        let child_schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let child: BoxedOperator = Box::new(TestEmptyWithSchema {
            schema: child_schema,
        });

        let branches = vec![vec![Pattern::Triple(
            crate::ir::triple::TriplePattern::new(
                crate::ir::triple::Ref::Var(VarId(0)),
                crate::ir::triple::Ref::Sid(Sid::new(100, "name")),
                crate::ir::triple::Term::Var(VarId(1)),
            ),
        )]];

        let op = UnionOperator::new(
            child,
            branches,
            None,
            crate::temporal_mode::PlanningContext::current(),
        )
        .with_out_schema(None);

        assert_eq!(op.schema(), &[VarId(0), VarId(1)]);
    }

    #[tokio::test]
    async fn test_union_coalesces_fragmented_branch_output() {
        let snapshot = fluree_db_core::LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let mut ctx = ExecutionContext::new(&snapshot, &vars);
        ctx.batch_size = 4;

        let child: BoxedOperator = Box::new(ValuesOperator::new(
            Box::new(EmptyOperator::new()),
            vec![VarId(0)],
            vec![
                vec![Binding::lit(FlakeValue::Long(1), Sid::new(2, "long"))],
                vec![Binding::lit(FlakeValue::Long(2), Sid::new(2, "long"))],
                vec![Binding::lit(FlakeValue::Long(3), Sid::new(2, "long"))],
            ],
        ));

        let branches = vec![
            vec![Pattern::Values {
                vars: vec![VarId(1)],
                rows: vec![
                    vec![Binding::lit(FlakeValue::Long(10), Sid::new(2, "long"))],
                    vec![Binding::lit(FlakeValue::Long(20), Sid::new(2, "long"))],
                ],
            }],
            vec![Pattern::Values {
                vars: vec![VarId(1)],
                rows: vec![vec![Binding::lit(
                    FlakeValue::Long(30),
                    Sid::new(2, "long"),
                )]],
            }],
        ];

        let mut op = UnionOperator::new(
            child,
            branches,
            None,
            crate::temporal_mode::PlanningContext::current(),
        );
        op.open(&ctx).await.unwrap();

        let batch1 = op.next_batch(&ctx).await.unwrap().unwrap();
        let batch2 = op.next_batch(&ctx).await.unwrap().unwrap();
        let batch3 = op.next_batch(&ctx).await.unwrap().unwrap();
        let batch4 = op.next_batch(&ctx).await.unwrap();

        assert_eq!(batch1.len(), 4);
        assert_eq!(batch2.len(), 4);
        assert_eq!(batch3.len(), 1);
        assert!(batch4.is_none());
    }

    // Helper struct for testing
    struct TestEmptyWithSchema {
        schema: Arc<[VarId]>,
    }

    #[async_trait]
    impl Operator for TestEmptyWithSchema {
        fn schema(&self) -> &[VarId] {
            &self.schema
        }

        async fn open(&mut self, _ctx: &ExecutionContext<'_>) -> Result<()> {
            Ok(())
        }

        async fn next_batch(&mut self, _ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
            Ok(None)
        }

        fn close(&mut self) {}
    }

    // ---- F17: UNION / BIND row-budget forwarding ----
    //
    // These cover the forwarding CONTRACT hermetically: BIND forwards a budget to
    // its child (the reclassification that unblocks q029), and the absorb boundary
    // (Sort / Distinct receive a budget but do NOT forward it — so an ORDER BY /
    // DISTINCT above a union means the union is never budgeted and declines for
    // free). The union stores a forwarded budget and re-applies it per branch.
    //
    // The per-branch SCAN early-stop, the real q029 ON==OFF byte-identical result,
    // and nested-union recursion are covered by the live q029 gate — only the R2RML
    // scan honors `row_budget`, and union branches are pattern-built with no
    // hermetic mock-scan injection point, so scan early-stop cannot be observed
    // without Iceberg.

    struct BudgetRecorder {
        received: Arc<std::sync::Mutex<Option<usize>>>,
        schema: Arc<[VarId]>,
        state: OperatorState,
    }

    #[async_trait]
    impl Operator for BudgetRecorder {
        fn schema(&self) -> &[VarId] {
            &self.schema
        }
        async fn open(&mut self, _ctx: &ExecutionContext<'_>) -> Result<()> {
            self.state = OperatorState::Open;
            Ok(())
        }
        async fn next_batch(&mut self, _ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
            Ok(None)
        }
        fn close(&mut self) {
            self.state = OperatorState::Closed;
        }
        fn set_row_budget(&mut self, budget: usize) {
            *self.received.lock().unwrap() = Some(budget);
        }
    }

    fn budget_recorder(received: &Arc<std::sync::Mutex<Option<usize>>>) -> BudgetRecorder {
        BudgetRecorder {
            received: Arc::clone(received),
            schema: Arc::from(vec![VarId(0)].into_boxed_slice()),
            state: OperatorState::Created,
        }
    }

    #[test]
    fn bind_forwards_row_budget_to_child() {
        use crate::bind::BindOperator;
        use crate::ir::expression::Expression;
        let received = Arc::new(std::sync::Mutex::new(None));
        let mut bind = BindOperator::new(
            Box::new(budget_recorder(&received)),
            VarId(1),
            Expression::Const(FlakeValue::Long(42)),
            vec![],
        );
        bind.set_row_budget(7);
        // BIND is 1:1 / order-preserving → forwards the budget to the scan below.
        assert_eq!(*received.lock().unwrap(), Some(7));
    }

    #[test]
    fn sort_absorbs_row_budget() {
        use crate::sort::{SortOperator, SortSpec};
        let received = Arc::new(std::sync::Mutex::new(None));
        let mut sort = SortOperator::new(
            Box::new(budget_recorder(&received)),
            vec![SortSpec::asc(VarId(0))],
        );
        sort.set_row_budget(7);
        // ORDER BY must rank every row → absorbs; a union under it is never budgeted.
        assert_eq!(*received.lock().unwrap(), None);
    }

    #[test]
    fn distinct_absorbs_row_budget() {
        use crate::distinct::DistinctOperator;
        let received = Arc::new(std::sync::Mutex::new(None));
        let mut distinct = DistinctOperator::new(Box::new(budget_recorder(&received)));
        distinct.set_row_budget(7);
        // DISTINCT may need > k raw rows to yield k unique → absorbs.
        assert_eq!(*received.lock().unwrap(), None);
    }

    #[test]
    fn union_stores_forwarded_row_budget() {
        let child_schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let child: BoxedOperator = Box::new(TestEmptyWithSchema {
            schema: child_schema,
        });
        let mut op = UnionOperator::new(
            child,
            vec![vec![], vec![]],
            None,
            crate::temporal_mode::PlanningContext::current(),
        );
        op.set_row_budget(9);
        // Stored (switch default-on); re-applied to each branch build in next_batch.
        assert_eq!(op.row_budget, Some(9));
    }

    #[tokio::test]
    async fn union_budget_preserves_limit_result() {
        use crate::limit::LimitOperator;
        let snapshot = fluree_db_core::LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);

        // One seed row; branches emit {10,20} and {30} → a 3-row union.
        let child: BoxedOperator = Box::new(ValuesOperator::new(
            Box::new(EmptyOperator::new()),
            vec![VarId(0)],
            vec![vec![Binding::lit(FlakeValue::Long(1), Sid::new(2, "long"))]],
        ));
        let branches = vec![
            vec![Pattern::Values {
                vars: vec![VarId(1)],
                rows: vec![
                    vec![Binding::lit(FlakeValue::Long(10), Sid::new(2, "long"))],
                    vec![Binding::lit(FlakeValue::Long(20), Sid::new(2, "long"))],
                ],
            }],
            vec![Pattern::Values {
                vars: vec![VarId(1)],
                rows: vec![vec![Binding::lit(
                    FlakeValue::Long(30),
                    Sid::new(2, "long"),
                )]],
            }],
        ];
        let union = UnionOperator::new(
            child,
            branches,
            None,
            crate::temporal_mode::PlanningContext::current(),
        );
        // LIMIT 2 forwards budget = 2 into the union (→ each branch build). The
        // result stays the correct first 2 rows: the F17 forwarding path preserves
        // correctness (scan early-stop is the live q029 gate).
        let mut limit = LimitOperator::new(Box::new(union), 2);
        limit.open(&ctx).await.unwrap();
        let mut total = 0;
        while let Some(b) = limit.next_batch(&ctx).await.unwrap() {
            total += b.len();
        }
        assert_eq!(total, 2);
    }

    // ---- F17 secondary lever: budget-met branch/input skip ----
    //
    // Build a 2-branch union over a single unit-seed input row, with Values
    // branches of known cardinality, and assert the branch BUILD count
    // (`branch_execs`) — the observable for "branch-2 was skipped". Values
    // branches ignore the forwarded budget, so branch-1 produces its full row
    // set and the lever's `pending >= budget` check is what gates branch-2.

    fn unit_seed_child() -> BoxedOperator {
        Box::new(ValuesOperator::new(
            Box::new(EmptyOperator::new()),
            vec![VarId(0)],
            vec![vec![Binding::lit(FlakeValue::Long(1), Sid::new(2, "long"))]],
        ))
    }

    fn two_values_branches(b1: &[i64], b2: &[i64]) -> Vec<Vec<Pattern>> {
        let mk = |vals: &[i64]| Pattern::Values {
            vars: vec![VarId(1)],
            rows: vals
                .iter()
                .map(|v| vec![Binding::lit(FlakeValue::Long(*v), Sid::new(2, "long"))])
                .collect(),
        };
        vec![vec![mk(b1)], vec![mk(b2)]]
    }

    async fn run_union(mut op: UnionOperator, ctx: &ExecutionContext<'_>) -> (usize, usize) {
        op.open(ctx).await.unwrap();
        let mut total = 0;
        while let Some(b) = op.next_batch(ctx).await.unwrap() {
            total += b.len();
        }
        (op.branch_execs, total)
    }

    #[tokio::test]
    async fn lever_budget_met_skips_later_branch() {
        let snapshot = fluree_db_core::LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);
        // branch-1 yields 2 rows == budget 2 → branch-2 must never be built.
        let branches = two_values_branches(&[10, 20], &[30]);
        let mut op = UnionOperator::new(
            unit_seed_child(),
            branches,
            None,
            crate::temporal_mode::PlanningContext::current(),
        );
        op.set_row_budget(2);
        let (branch_execs, total) = run_union(op, &ctx).await;
        assert_eq!(
            branch_execs, 1,
            "branch-2 must be skipped once branch-1 fills the budget"
        );
        assert_eq!(total, 2);
    }

    #[tokio::test]
    async fn lever_underfilled_branch_still_runs_later_branch() {
        let snapshot = fluree_db_core::LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);
        // branch-1 yields 2 < budget 3 → branch-2 IS built and drained (total 3).
        let branches = two_values_branches(&[10, 20], &[30]);
        let mut op = UnionOperator::new(
            unit_seed_child(),
            branches,
            None,
            crate::temporal_mode::PlanningContext::current(),
        );
        op.set_row_budget(3);
        let (branch_execs, total) = run_union(op, &ctx).await;
        assert_eq!(
            branch_execs, 2,
            "an under-filled branch-1 must not skip branch-2"
        );
        assert_eq!(total, 3);
    }

    #[tokio::test]
    async fn lever_absent_budget_runs_all_branches() {
        let snapshot = fluree_db_core::LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);
        // No budget set → the lever never trips; both branches always run.
        let branches = two_values_branches(&[10, 20], &[30]);
        let op = UnionOperator::new(
            unit_seed_child(),
            branches,
            None,
            crate::temporal_mode::PlanningContext::current(),
        );
        let (branch_execs, total) = run_union(op, &ctx).await;
        assert_eq!(branch_execs, 2, "without a budget both branches always run");
        assert_eq!(total, 3);
    }
}
