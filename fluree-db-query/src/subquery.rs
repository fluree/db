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
use crate::group_aggregate::{binding_to_group_key_normalized, GroupKeyOwned};
use crate::ir::{Pattern, SubqueryPattern};
use crate::object_binding::{equality_norm, EqualityNorm};
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
    /// The subset of `correlation_vars` used as the hash-join key in join-mode:
    /// the variables the subquery PRODUCES itself (bound by a top-level required
    /// triple / property path). Seeding such a variable per parent row only
    /// filters the subquery's output to that value, so it is equivalent to
    /// evaluating the subquery once and joining on the variable. A correlation
    /// variable that the subquery does NOT bind (e.g. a `GROUP BY` key never
    /// constrained in the body — the BSBM BI-5 quirk) is a pass-through: it is
    /// omitted from the key and flows from the parent, matching SPARQL join
    /// semantics (the subquery's unbound value joins with the parent's bound
    /// value). Empty when there is no correlation (broadcast).
    join_keys: Vec<VarId>,
    /// Whether the subquery is evaluated ONCE and hash-joined (on `join_keys`)
    /// rather than re-executed per parent row. Requires no inner `LIMIT`/`OFFSET`
    /// and that every non-key correlation variable is an unreferenced
    /// pass-through. Gated by a parent-cardinality check so we only materialize
    /// when it beats per-row seeding; an uncorrelated subquery always
    /// materializes. Mirrors `SemijoinOperator`'s hash probe.
    join_mode: bool,
    /// Lazily materialized subquery result (built once when `join_mode`): all
    /// result rows plus a hash index from the correlation-variable values to the
    /// rows carrying them. Reused across every parent row and batch.
    materialized: Option<MaterializedSubquery>,
    /// Store for normalizing decoded bindings to encoded form on both join
    /// sides, so mixed-representation rows key identically. `None` outside
    /// single-ledger binary execution.
    norm: Option<EqualityNorm>,
}

/// A once-evaluated subquery result, indexed for hash-join probing.
struct MaterializedSubquery {
    /// All subquery result rows (projected to the subquery SELECT list).
    rows: Vec<Vec<Binding>>,
    /// Correlation-variable values -> indices into `rows`. An empty key vector
    /// (no correlation) maps every row under a single bucket (broadcast).
    index: HashMap<Vec<GroupKeyOwned>, Vec<usize>>,
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

        // Partition correlation vars: a JOIN KEY is one the subquery binds in
        // every solution itself (a top-level required triple / property path);
        // seeding it only filters the output, so it can be a hash key.
        let produced = self_produced_vars(&subquery.patterns);
        let join_keys: Vec<VarId> = correlation_vars
            .iter()
            .copied()
            .filter(|v| produced.contains(v))
            .collect();

        // Eligible for evaluate-once + hash-join when there is no inner slice and
        // every NON-key correlation var is an unreferenced pass-through (it
        // appears only in the SELECT, never constraining the body). Omitting such
        // a var from the join key matches SPARQL join semantics. A correlation
        // var that is referenced but not produced is a genuine per-row input.
        let body_referenced = referenced_vars_set(&subquery.patterns);
        let pass_through_ok = correlation_vars
            .iter()
            .all(|v| produced.contains(v) || !body_referenced.contains(v));
        let eligible = subquery.limit.is_none() && subquery.offset.is_none() && pass_through_ok;

        // Cardinality guard: evaluating once + hash-join removes the per-row
        // operator-rebuild overhead, which pays off when the parent drives many
        // rows; for a small parent, per-row seeding (with its pruning seed) can
        // be cheaper, so fall back to it (the pre-existing behavior — this guard
        // never regresses below it). An uncorrelated subquery has no shared key
        // and MUST be evaluated once regardless (per-row recomputes identically).
        let join_mode = eligible
            && (correlation_vars.is_empty()
                || child
                    .estimated_rows()
                    .is_none_or(|n| n >= SUBQUERY_MATERIALIZE_MIN_PARENT_ROWS));

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
            join_keys,
            join_mode,
            materialized: None,
            norm: None,
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
    fn plan_details(&self) -> serde_json::Map<String, serde_json::Value> {
        let mut m = serde_json::Map::new();
        m.insert("join-mode".into(), self.join_mode.into());
        if !self.correlation_vars.is_empty() {
            m.insert(
                "correlation-vars".into(),
                serde_json::Value::Array(
                    self.correlation_vars
                        .iter()
                        .map(|v| serde_json::Value::String(format!("?v{}", v.0)))
                        .collect(),
                ),
            );
        }
        m
    }

    /// The subquery's inner operator tree is built lazily at runtime (it is not
    /// held as a field), so the default `plan_children` walk can't reach it.
    /// Rebuild it here — build-only, no `open()`/exec — from the stored IR +
    /// stats + planning, and attach it under a `SubqueryBody` node so the inner
    /// joins (where BSBM-BI time lives) are visible. The first child is the outer
    /// input the subquery correlates against.
    fn describe(&self) -> crate::plan_node::PlanNode {
        use crate::plan_node::{PlanEdge, PlanEdgeRel, PlanNode};

        let mut children = vec![PlanEdge {
            rel: PlanEdgeRel::Child,
            node: self.child.describe(),
        }];

        let body = match self.build_inner_plan_for_explain() {
            Ok(inner) => PlanNode {
                op: "SubqueryBody".into(),
                est_rows: None,
                details: serde_json::Map::new(),
                children: vec![PlanEdge {
                    rel: PlanEdgeRel::Child,
                    node: inner.describe(),
                }],
            },
            Err(e) => PlanNode::leaf(format!("SubqueryBody <error: {e}>"), None),
        };
        children.push(PlanEdge {
            rel: PlanEdgeRel::Child,
            node: body,
        });

        PlanNode {
            op: self.op_name(),
            est_rows: self.estimated_rows(),
            details: self.plan_details(),
            children,
        }
    }

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
        self.materialized = None;
        if self.norm.is_none() {
            self.norm = equality_norm(ctx);
        }
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

        for row_idx in 0..parent_batch.len() {
            // Produce this parent row's matching subquery rows.
            //
            // JOIN MODE: evaluate the subquery ONCE (empty seed), index it by
            // the correlation-variable values, and for each parent row take only
            // the subquery rows whose join key matches — equivalent to seeding
            // per row but without rebuilding/re-running the subquery N times.
            // An empty correlation set indexes every row under one bucket, so
            // the match is a broadcast (the uncorrelated case).
            //
            // Otherwise (a genuine per-row correlation, or an inner slice that
            // makes seeding result-sensitive), fall back to per-row seeding.
            let subquery_results: Vec<Vec<Binding>> = if self.join_mode {
                if self.materialized.is_none() {
                    let m = self.materialize(ctx).await?;
                    self.materialized = Some(m);
                }
                let mat = self.materialized.as_ref().unwrap();
                let parent_key: Vec<GroupKeyOwned> = self
                    .join_keys
                    .iter()
                    .map(|v| {
                        parent_batch
                            .get(row_idx, *v)
                            .map(|b| {
                                let (store, gv) = EqualityNorm::parts(&self.norm);
                                binding_to_group_key_normalized(b, store, gv)
                            })
                            .unwrap_or(GroupKeyOwned::Absent)
                    })
                    .collect();
                match mat.index.get(&parent_key) {
                    Some(idxs) => idxs.iter().map(|&i| mat.rows[i].clone()).collect(),
                    None => Vec::new(),
                }
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
        // The subquery's OWN output estimate seeds the downstream object→subject
        // hash join's driving estimate so a `(message HAS_CREATOR friend)` probe
        // is costed against the ~producer size, not 1 — but ONLY for shapes whose
        // output is reliably bounded (scalar aggregate, anchored `WITH DISTINCT`
        // producer, or explicit LIMIT). For an arbitrary subquery the estimate is
        // just body cardinality, which is fine for join ordering but too
        // unreliable to perturb the hash-join cost model, so we keep the
        // conservative `None` the operator returned before.
        if !crate::planner::subquery_output_estimate_is_bounded(&self.subquery) {
            return None;
        }
        Some(
            crate::planner::estimate_subquery_output(&self.subquery, self.stats.as_deref()).round()
                as usize,
        )
    }
}

impl SubqueryOperator {
    /// Drain buffered results into a batch
    async fn drain_buffer(&mut self) -> Result<Option<Batch>> {
        if self.buffer_pos >= self.result_buffer.len() {
            return Ok(None);
        }

        // Number of rows about to be drained — needed to size an empty-schema
        // batch, where there are no columns to infer the row count from.
        let drained = self.result_buffer.len() - self.buffer_pos;

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

        // A variable-free subquery (e.g. `{ SELECT * WHERE { :a :p "1" } }`)
        // produces an empty schema; a match is still one empty-binding solution
        // per row. Emit an empty-schema batch with the row count rather than
        // collapsing to zero rows (out_schema is also empty here, so there is
        // nothing to trim).
        if num_cols == 0 {
            return Ok(Some(Batch::empty_schema_with_len(drained)));
        }

        if columns[0].is_empty() {
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

        self.run_subquery_with_seed(ctx, seed).await
    }

    /// Evaluate the subquery ONCE with an empty seed and index its result rows
    /// by their correlation-variable values, for hash-join probing in join-mode.
    /// An empty correlation set produces a single bucket (broadcast).
    async fn materialize(&self, ctx: &ExecutionContext<'_>) -> Result<MaterializedSubquery> {
        let rows = self
            .run_subquery_with_seed(ctx, Box::new(EmptyOperator::new()))
            .await?;
        let mut index: HashMap<Vec<GroupKeyOwned>, Vec<usize>> = HashMap::new();
        for (i, row) in rows.iter().enumerate() {
            let key: Vec<GroupKeyOwned> = self
                .join_keys
                .iter()
                .map(|v| {
                    self.select_index
                        .get(v)
                        .and_then(|&si| row.get(si))
                        .map(|b| {
                            let (store, gv) = EqualityNorm::parts(&self.norm);
                            binding_to_group_key_normalized(b, store, gv)
                        })
                        .unwrap_or(GroupKeyOwned::Absent)
                })
                .collect();
            index.entry(key).or_default().push(i);
        }
        Ok(MaterializedSubquery { rows, index })
    }

    /// Build the subquery's inner operator tree for `EXPLAIN` (build-only — no
    /// `open()`/exec). Mirrors [`run_subquery_with_seed`](Self::run_subquery_with_seed)'s
    /// construction with no execution.
    ///
    /// The seed must match the path that actually runs, because the seed's schema
    /// is the inner `reorder_patterns`' initial bound set AND the child every nested
    /// subquery sees (a `SeedOperator`'s 1-row estimate flips their cardinality-guard
    /// `join_mode` to per-row). `join_mode` evaluates the body ONCE with an empty
    /// seed (`materialize`); per-row seeds the correlation vars
    /// (`execute_subquery_for_row`). An uncorrelated subquery has no seed either way.
    fn build_inner_plan_for_explain(&self) -> Result<BoxedOperator> {
        let seed: BoxedOperator = if self.join_mode || self.correlation_vars.is_empty() {
            Box::new(EmptyOperator::new())
        } else {
            let seed_schema: Arc<[VarId]> =
                Arc::from(self.correlation_vars.clone().into_boxed_slice());
            let seed_row = vec![Binding::Unbound; self.correlation_vars.len()];
            Box::new(SeedOperator::from_row(seed_schema, seed_row))
        };

        let where_op = build_where_operators_seeded(
            Some(seed),
            &self.subquery.patterns,
            self.stats.clone(),
            None,
            &self.planning,
        )?;

        let select_vars: Option<&[VarId]> =
            (!self.subquery.select.is_empty()).then_some(self.subquery.select.as_slice());
        crate::execute::operator_tree::apply_solution_modifiers(
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
            &self.planning,
        )
    }

    /// Build and run the subquery's operator tree from `seed`, returning rows
    /// projected to the subquery SELECT list. Shared by the per-row (correlated)
    /// and once (join-mode) execution paths.
    async fn run_subquery_with_seed(
        &self,
        ctx: &ExecutionContext<'_>,
        seed: BoxedOperator,
    ) -> Result<Vec<Vec<Binding>>> {
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
        // partition hint are skipped: the subquery's full select list flows
        // back into the merge.
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
            &self.planning,
        )?;

        // Execute and collect results
        operator.open(ctx).await?;
        let mut results = Vec::new();

        while let Some(batch) = operator.next_batch(ctx).await? {
            ctx.check_cancelled()?;
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
            ctx.check_cancelled()?;
        }

        operator.close();
        Ok(results)
    }
}

/// Minimum estimated parent (driving) rows above which evaluating a self-keyed
/// subquery once + hash-join beats per-row seeding. Below this the per-row path
/// (with its pruning seed) can be cheaper, so it is kept — the pre-existing
/// behavior, which this never regresses below. An unknown parent size defaults
/// to materialize: the per-row operator-rebuild overhead, paid once per parent
/// row, is the larger risk.
const SUBQUERY_MATERIALIZE_MIN_PARENT_ROWS: usize = 8;

/// Variables bound in *every* solution of `patterns` — produced by a top-level
/// required pattern (triple / property path), or exported by a slice-free nested
/// sub-SELECT whose own body always-binds them. NOT vars bound only inside a
/// `UNION` branch or `OPTIONAL` (conditional), nor a sub-SELECT's pass-throughs.
/// These are the only correlation variables safe to use as evaluate-once
/// hash-join keys.
fn self_produced_vars(patterns: &[Pattern]) -> HashSet<VarId> {
    let mut produced: HashSet<VarId> = HashSet::new();
    for p in patterns {
        match p {
            Pattern::Triple(_) | Pattern::PropertyPath(_) => {
                produced.extend(p.produced_vars());
            }
            // A nested sub-SELECT binds the SELECT vars its own body always
            // binds, so those are always-bound here too. A slice would make
            // per-row seeding of such a var result-sensitive, so skip it then.
            Pattern::Subquery(sq) if sq.limit.is_none() && sq.offset.is_none() => {
                let inner = self_produced_vars(&sq.patterns);
                produced.extend(sq.select.iter().copied().filter(|v| inner.contains(v)));
            }
            _ => {}
        }
    }
    produced
}

/// All variables referenced anywhere in `patterns` (filters, binds, unions,
/// optionals, nested subquery bodies). A correlation variable that is referenced
/// but not produced is a genuine per-row input, not an omittable pass-through.
fn referenced_vars_set(patterns: &[Pattern]) -> HashSet<VarId> {
    patterns.iter().flat_map(Pattern::referenced_vars).collect()
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
