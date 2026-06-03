//! Subquery operator - executes nested queries and merges results
//!
//! Implements correlated subquery semantics where:
//! - For each parent solution, the subquery is executed
//! - Shared variables between parent and subquery are correlated
//! - Subquery results are merged with the parent solution
//!
//! # Syntax
//!
//! ```json
//! ["query", {
//!   "select": ["?s", "?age"],
//!   "where": {"@id": "?s", "schema:age": "?age"}
//! }]
//! ```
//!
//! # Correlation Semantics
//!
//! Variables shared between parent and subquery are used for correlation:
//! - If `?s` is bound in the parent, the subquery filters to only those `?s` values
//! - Results are merged back to the parent solution

use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::execute::build_where_operators_seeded;
use crate::ir::SubqueryPattern;
use crate::operator::{
    compute_trimmed_vars, effective_schema, trim_batch, BoxedOperator, Operator, OperatorState,
};
use crate::seed::{EmptyOperator, SeedOperator};
use crate::temporal_mode::PlanningContext;
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_core::StatsView;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Subquery operator - executes nested queries and merges results
pub struct SubqueryOperator {
    /// Child operator providing parent solutions
    child: BoxedOperator,
    /// The subquery pattern to execute
    subquery: SubqueryPattern,
    /// Output schema (parent schema + new subquery variables)
    in_schema: Arc<[VarId]>,
    /// Variables used for correlation (appear in BOTH parent schema and subquery patterns)
    correlation_vars: Vec<VarId>,
    /// New variables introduced by the subquery select list (not present in parent schema)
    new_vars: Vec<VarId>,
    /// Index of variables in the subquery select row (VarId -> position)
    select_index: HashMap<VarId, usize>,
    /// Operator state
    state: OperatorState,
    /// Buffered results
    result_buffer: Vec<Vec<Binding>>,
    /// Current position in result buffer
    buffer_pos: usize,
    /// Optional stats for selectivity-based pattern reordering in subquery
    stats: Option<Arc<StatsView>>,
    /// Planning context captured at planner-time for the subquery subplan.
    planning: PlanningContext,
    /// Variables required by downstream operators; if set, output is trimmed.
    out_schema: Option<Arc<[VarId]>>,
    /// Cached result rows for an UNCORRELATED subquery (`correlation_vars`
    /// empty). Such a subquery shares no variables with the parent, so its seed
    /// is always the empty solution (see `execute_subquery_for_row`) and it
    /// yields the identical result for every parent row. We execute it once,
    /// cache the rows here, and broadcast them to every parent row (and across
    /// batches) instead of rebuilding and re-running the subquery operator tree
    /// per row. Mirrors `FilterOperator::pre_resolve_uncorrelated` for EXISTS.
    uncorrelated_cache: Option<Vec<Vec<Binding>>>,
}

impl SubqueryOperator {
    /// Create a new subquery operator
    pub fn new(
        child: BoxedOperator,
        subquery: SubqueryPattern,
        stats: Option<Arc<StatsView>>,
        planning: PlanningContext,
    ) -> Self {
        let parent_schema: HashSet<VarId> = child.schema().iter().copied().collect();
        let subquery_select_vars: HashSet<VarId> = subquery.select.iter().copied().collect();

        // Correlation vars: variables in BOTH the parent schema AND the subquery
        // SELECT list.  Per SPARQL semantics, the subquery's scope boundary is
        // defined by its SELECT — variables not SELECTed are invisible from the
        // parent, even if referenced internally (e.g., in FILTERs).
        let correlation_vars: Vec<VarId> = child
            .schema()
            .iter()
            .copied()
            .filter(|v| subquery_select_vars.contains(v))
            .collect();

        // New vars are subquery *selected* vars that are not in parent schema, preserving select order.
        let new_vars: Vec<VarId> = subquery
            .select
            .iter()
            .copied()
            .filter(|v| !parent_schema.contains(v))
            .collect();

        // Build select index for row merging
        let select_index: HashMap<VarId, usize> = subquery
            .select
            .iter()
            .enumerate()
            .map(|(i, v)| (*v, i))
            .collect();

        // Output schema = parent schema + new vars from subquery
        let mut schema_vec: Vec<VarId> = child.schema().to_vec();
        schema_vec.extend(&new_vars);
        let schema = Arc::from(schema_vec.into_boxed_slice());

        Self {
            child,
            subquery,
            in_schema: schema,
            correlation_vars,
            new_vars,
            select_index,
            state: OperatorState::Created,
            result_buffer: Vec::new(),
            buffer_pos: 0,
            stats,
            planning,
            out_schema: None,
            uncorrelated_cache: None,
        }
    }

    /// Trim output to only the specified downstream variables.
    pub fn with_out_schema(mut self, downstream_vars: Option<&[VarId]>) -> Self {
        self.out_schema = compute_trimmed_vars(&self.in_schema, downstream_vars);
        self
    }
}

#[async_trait]
impl Operator for SubqueryOperator {
    fn schema(&self) -> &[VarId] {
        effective_schema(&self.out_schema, &self.in_schema)
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        if !self.state.can_open() {
            if self.state.is_closed() {
                return Err(QueryError::OperatorClosed);
            }
            return Err(QueryError::OperatorAlreadyOpened);
        }

        self.child.open(ctx).await?;
        self.state = OperatorState::Open;
        self.uncorrelated_cache = None;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if !self.state.can_next() {
            if self.state == OperatorState::Created {
                return Err(QueryError::OperatorNotOpened);
            }
            return Ok(None);
        }

        // If buffer has results, return them
        if self.buffer_pos < self.result_buffer.len() {
            return self.drain_buffer().await;
        }

        // Get next batch from child
        let Some(parent_batch) = self.child.next_batch(ctx).await? else {
            self.state = OperatorState::Exhausted;
            return Ok(None);
        };

        // Process each parent row
        self.result_buffer.clear();
        self.buffer_pos = 0;

        let uncorrelated = self.correlation_vars.is_empty();
        for row_idx in 0..parent_batch.len() {
            // Execute the subquery for this parent row.
            //
            // UNCORRELATED subqueries (no variables shared with the parent)
            // produce the identical result for every parent row — their seed is
            // always the empty solution. Execute once, cache, and reuse for all
            // rows and all subsequent batches. This collapses an
            // O(parent_rows) re-execution — each rebuilding and re-running the
            // full subquery operator tree — into a single run. The clone is of
            // the (typically single-row) cached result, not of any scan work.
            let subquery_results = if uncorrelated {
                if self.uncorrelated_cache.is_none() {
                    let rows = self
                        .execute_subquery_for_row(ctx, &parent_batch, row_idx)
                        .await?;
                    self.uncorrelated_cache = Some(rows);
                }
                self.uncorrelated_cache.as_ref().unwrap().clone()
            } else {
                self.execute_subquery_for_row(ctx, &parent_batch, row_idx)
                    .await?
            };

            // Merge results with parent row
            for subquery_row in subquery_results {
                let mut merged_row = Vec::with_capacity(self.in_schema.len());

                // Copy parent bindings
                for var in self.child.schema() {
                    let binding = parent_batch
                        .get(row_idx, *var)
                        .cloned()
                        .unwrap_or(Binding::Unbound);
                    merged_row.push(binding);
                }

                // Fill in any subquery-selected vars that already exist in the parent schema,
                // but are currently Unbound/Poisoned in the parent row (non-clobbering merge).
                for (parent_idx, var) in self.child.schema().iter().enumerate() {
                    if matches!(merged_row[parent_idx], Binding::Unbound | Binding::Poisoned) {
                        if let Some(&sel_idx) = self.select_index.get(var) {
                            if let Some(val) = subquery_row.get(sel_idx) {
                                if !matches!(val, Binding::Unbound | Binding::Poisoned) {
                                    merged_row[parent_idx] = val.clone();
                                }
                            }
                        }
                    }
                }

                // Append new vars introduced by the subquery select list, preserving select order.
                for var in &self.new_vars {
                    let binding = self
                        .select_index
                        .get(var)
                        .and_then(|&idx| subquery_row.get(idx))
                        .cloned()
                        .unwrap_or(Binding::Unbound);
                    merged_row.push(binding);
                }

                self.result_buffer.push(merged_row);
            }
        }

        self.drain_buffer().await
    }

    fn close(&mut self) {
        self.child.close();
        self.result_buffer.clear();
        self.state = OperatorState::Closed;
    }

    fn estimated_rows(&self) -> Option<usize> {
        // Subqueries can multiply rows; hard to estimate
        None
    }
}

impl SubqueryOperator {
    /// Drain buffered results into a batch
    async fn drain_buffer(&mut self) -> Result<Option<Batch>> {
        if self.buffer_pos >= self.result_buffer.len() {
            return Ok(None);
        }

        // Build batch from buffer
        let num_cols = self.in_schema.len();
        let mut columns: Vec<Vec<Binding>> = (0..num_cols).map(|_| Vec::new()).collect();

        for row in &self.result_buffer[self.buffer_pos..] {
            for (col_idx, binding) in row.iter().enumerate() {
                if col_idx < columns.len() {
                    columns[col_idx].push(binding.clone());
                }
            }
        }

        self.buffer_pos = self.result_buffer.len();

        if columns.is_empty() || columns[0].is_empty() {
            Ok(None)
        } else {
            let batch = Batch::new(self.in_schema.clone(), columns)?;
            Ok(trim_batch(&self.out_schema, batch))
        }
    }

    /// Execute subquery for a single parent row
    async fn execute_subquery_for_row(
        &self,
        ctx: &ExecutionContext<'_>,
        parent_batch: &Batch,
        row_idx: usize,
    ) -> Result<Vec<Vec<Binding>>> {
        // Build seed from parent row (for correlated execution)
        // Include correlation vars (present in both parent schema and subquery patterns).
        let seed_schema: Vec<VarId> = self.correlation_vars.clone();
        let seed_row: Vec<Binding> = self
            .correlation_vars
            .iter()
            .map(|var| {
                parent_batch
                    .get(row_idx, *var)
                    .cloned()
                    .unwrap_or(Binding::Unbound)
            })
            .collect();

        let seed: BoxedOperator = if seed_schema.is_empty() {
            Box::new(EmptyOperator::new())
        } else {
            let schema = Arc::from(seed_schema.into_boxed_slice());
            Box::new(SeedOperator::from_row(schema, seed_row))
        };

        // Build full operator tree for subquery patterns (supports filters, optionals, union, etc.)
        let where_op: BoxedOperator = build_where_operators_seeded(
            Some(seed),
            &self.subquery.patterns,
            self.stats.clone(),
            None,
            &self.planning,
        )?;

        // Apply the shared solution-modifier tail — GROUP BY + aggregation,
        // HAVING, post-aggregation binds, expression/aggregate ORDER-BY binds,
        // sort-var validation, ORDER BY (sort *before* project, with safe top-k),
        // PROJECT, DISTINCT, OFFSET, LIMIT — so a subquery inherits identical
        // modifier semantics to a top-level SELECT (same code path).
        //
        // Projection trimming (`variable_deps`) and the streaming-group
        // partition hint are skipped: the subquery runs once per correlated
        // parent row, and its full select list flows back into the merge.
        let select_vars: Option<&[VarId]> =
            (!self.subquery.select.is_empty()).then_some(self.subquery.select.as_slice());
        let mut operator = crate::execute::operator_tree::apply_solution_modifiers(
            where_op,
            self.subquery.grouping.as_ref(),
            &self.subquery.order_binds,
            &self.subquery.ordering,
            select_vars,
            self.subquery.distinct,
            self.subquery.offset,
            self.subquery.limit,
            false,
            None,
        )?;

        // Execute and collect results
        operator.open(ctx).await?;
        let mut results = Vec::new();

        while let Some(batch) = operator.next_batch(ctx).await? {
            for sub_row_idx in 0..batch.len() {
                // Extract bindings for subquery SELECT variables (in order)
                let row: Vec<Binding> = self
                    .subquery
                    .select
                    .iter()
                    .map(|var| {
                        batch
                            .get(sub_row_idx, *var)
                            .cloned()
                            .unwrap_or(Binding::Unbound)
                    })
                    .collect();
                results.push(row);
            }
        }

        operator.close();
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binding::Binding;
    use crate::ir::SubqueryPattern;
    use crate::seed::SeedOperator;
    use crate::var_registry::VarId;

    /// Verifies that correlation uses SELECT vars, not internal pattern vars.
    ///
    /// Scenario: parent schema has [?s, ?name], subquery SELECT is [?s, ?age],
    /// but subquery patterns also reference ?internal (not SELECTed).
    /// Correlation should be [?s] only — ?internal must NOT appear in
    /// correlation_vars even if it were somehow in the parent schema.
    #[test]
    fn correlation_uses_select_vars_not_pattern_vars() {
        let v_s = VarId(0);
        let v_name = VarId(1);
        let v_age = VarId(2);
        let v_internal = VarId(3);

        // Parent provides [?s, ?name]
        let parent_schema: Arc<[VarId]> = Arc::from(vec![v_s, v_name]);
        let child = SeedOperator::from_row(parent_schema, vec![Binding::Unbound, Binding::Unbound]);

        // Subquery SELECT [?s, ?age]; patterns also reference ?internal
        let subquery = SubqueryPattern::new(
            vec![v_s, v_age],
            vec![], // patterns don't matter for this structural test
        );

        let op = SubqueryOperator::new(
            Box::new(child),
            subquery,
            None,
            crate::temporal_mode::PlanningContext::current(),
        );

        // ?s is in both parent schema and subquery SELECT → correlated
        assert_eq!(op.correlation_vars, vec![v_s]);

        // ?age is new (in subquery SELECT but not parent schema)
        assert_eq!(op.new_vars, vec![v_age]);

        // ?name is NOT in subquery SELECT → not correlated, not new
        assert!(!op.correlation_vars.contains(&v_name));

        // ?internal is NOT in subquery SELECT → never appears
        assert!(!op.correlation_vars.contains(&v_internal));
    }
}
