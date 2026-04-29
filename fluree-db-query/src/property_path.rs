//! Property path operator - transitive graph traversal
//!
//! Implements property path semantics for transitive predicates:
//! - `+` (one-or-more): Traverse at least one hop
//! - `*` (zero-or-more): Traverse zero or more hops (includes starting node)
//!
//! # Key semantics
//!
//! - BFS traversal with visited set for cycle detection
//! - Ref-only traversal: Only follows edges where object is a Sid (IRI reference)
//! - Short-circuits on cycles to prevent infinite loops
//! - Safety bound on max visited nodes to prevent runaway closure enumeration
//!
//! # Correlated execution modes
//!
//! When both subject and object are variables, the operator requires correlated
//! execution where at least one variable is bound by the upstream child operator:
//!
//! | Subject | Object | Behavior |
//! |---------|--------|----------|
//! | Bound   | Unbound | Forward traversal from subject, bind reachable to object |
//! | Unbound | Bound   | Backward traversal to object, bind sources to subject |
//! | Bound   | Bound   | Reachability filter: keep row only if path exists |
//! | Unbound | Unbound | **Error**: requires at least one bound variable |
//!
//! The "both unbound" case is intentionally an error to prevent accidental
//! full-closure enumeration which can be extremely expensive. If full closure
//! is needed, the query should explicitly bind one side first.

use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::ir::{PathModifier, PropertyPathPattern};
use crate::operator::{
    compute_trimmed_vars, effective_schema, trim_batch, BoxedOperator, Operator, OperatorState,
};
use crate::triple::Ref;
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_core::{
    range_with_overlay, FlakeValue, IndexType, RangeMatch, RangeOptions, RangeTest, Sid,
};
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;

/// Default maximum number of nodes to visit during traversal
/// This prevents runaway closure enumeration for both-variable patterns
pub const DEFAULT_MAX_VISITED: usize = 10_000;

/// Property path operator - transitive graph traversal
///
/// Supports two execution modes:
/// 1. **Unseeded mode** (no child): For constant-bound subject/object or both-variable closure
/// 2. **Correlated mode** (with child): For each input row, read bound var and traverse
///
/// The execution mode is determined at runtime based on the pattern and child bindings.
pub struct PropertyPathOperator {
    /// Optional child operator providing input solutions
    child: Option<BoxedOperator>,
    /// Property path pattern to execute
    pattern: PropertyPathPattern,
    /// Output schema (variables from subject and object)
    in_schema: Arc<[VarId]>,
    /// Operator state
    state: OperatorState,
    /// Safety bound for maximum visited nodes
    max_visited: usize,
    /// Results buffer for unseeded mode
    results_buffer: Option<Vec<(Sid, Sid)>>,
    /// Results buffer index
    results_idx: usize,
    /// Current child batch being processed (correlated mode)
    current_child_batch: Option<Batch>,
    /// Current row index in child batch
    current_child_row: usize,
    /// Variables required by downstream operators; if set, output is trimmed.
    out_schema: Option<Arc<[VarId]>>,
}

impl PropertyPathOperator {
    /// Create a new property path operator
    ///
    /// # Arguments
    ///
    /// * `child` - Optional input solutions operator (for correlated execution)
    /// * `pattern` - Property path pattern to execute
    /// * `max_visited` - Maximum nodes to visit (safety bound)
    pub fn new(
        child: Option<BoxedOperator>,
        pattern: PropertyPathPattern,
        max_visited: usize,
    ) -> Self {
        // Build schema from pattern variables
        let mut schema_vec = Vec::with_capacity(2);
        if let Ref::Var(v) = &pattern.subject {
            schema_vec.push(*v);
        }
        if let Ref::Var(v) = &pattern.object {
            schema_vec.push(*v);
        }

        // If we have a child, extend schema with child's variables
        let schema: Arc<[VarId]> = if let Some(ref child) = child {
            let mut full_schema: Vec<VarId> = child.schema().to_vec();
            let seen: HashSet<VarId> = full_schema.iter().copied().collect();
            for v in schema_vec {
                if !seen.contains(&v) {
                    full_schema.push(v);
                }
            }
            Arc::from(full_schema.into_boxed_slice())
        } else {
            Arc::from(schema_vec.into_boxed_slice())
        };

        Self {
            child,
            pattern,
            in_schema: schema,
            state: OperatorState::Created,
            max_visited,
            results_buffer: None,
            results_idx: 0,
            current_child_batch: None,
            current_child_row: 0,
            out_schema: None,
        }
    }

    /// Create with default max_visited
    pub fn with_defaults(child: Option<BoxedOperator>, pattern: PropertyPathPattern) -> Self {
        Self::new(child, pattern, DEFAULT_MAX_VISITED)
    }

    /// Trim output to only the specified downstream variables.
    pub fn with_out_schema(mut self, downstream_vars: Option<&[VarId]>) -> Self {
        self.out_schema = compute_trimmed_vars(&self.in_schema, downstream_vars);
        self
    }

    /// Traverse forward from a starting node (subject bound)
    ///
    /// Uses SPOT index: (subject=start, predicate=path_pred)
    /// Returns all reachable nodes via the transitive predicate.
    async fn traverse_forward(&self, ctx: &ExecutionContext<'_>, start: &Sid) -> Result<Vec<Sid>> {
        let mut visited: HashSet<Sid> = HashSet::new();
        let mut queue: VecDeque<Sid> = VecDeque::new();
        let mut results: Vec<Sid> = Vec::new();
        let mut added_start_via_cycle = false;

        // ZeroOrMore includes starting node
        if self.pattern.modifier == PathModifier::ZeroOrMore {
            results.push(start.clone());
            visited.insert(start.clone());
        }

        queue.push_back(start.clone());
        if self.pattern.modifier == PathModifier::OneOrMore {
            visited.insert(start.clone());
        }

        while let Some(current) = queue.pop_front() {
            // Safety bound check
            if visited.len() >= self.max_visited {
                return Err(QueryError::ResourceLimit(format!(
                    "Property path exceeded max visited nodes ({})",
                    self.max_visited
                )));
            }

            // SPOT index: (subject=current, predicate=path_pred)
            let range_match = RangeMatch::new()
                .with_subject(current.clone())
                .with_predicate(self.pattern.predicate.clone());

            // In dataset mode, property paths must run against a single active graph.
            // There is no meaningful dataset-wide `to_t` for multi-ledger datasets.
            let (db, overlay, to_t) = ctx.require_single_graph()?;

            let opts = RangeOptions::new().with_to_t(to_t);

            let flakes = range_with_overlay(
                db,
                ctx.binary_g_id,
                overlay,
                IndexType::Spot,
                RangeTest::Eq,
                range_match,
                opts,
            )
            .await?;

            for flake in flakes {
                // REF-ONLY: Only traverse Sid objects, skip literals
                if let FlakeValue::Ref(obj_sid) = flake.o {
                    // OneOrMore should still be able to emit `start` if a non-zero-length path
                    // returns to it (cycle), even though `visited` contains `start`.
                    if self.pattern.modifier == PathModifier::OneOrMore
                        && !added_start_via_cycle
                        && &obj_sid == start
                    {
                        results.push(obj_sid.clone());
                        added_start_via_cycle = true;
                        continue;
                    }
                    if visited.insert(obj_sid.clone()) {
                        results.push(obj_sid.clone());
                        queue.push_back(obj_sid);
                    }
                }
            }
        }

        Ok(results)
    }

    /// Traverse backward to a target node (object bound)
    ///
    /// Uses POST index: (predicate=path_pred, object=target)
    /// Returns all nodes that can reach the target via the transitive predicate.
    async fn traverse_backward(
        &self,
        ctx: &ExecutionContext<'_>,
        target: &Sid,
    ) -> Result<Vec<Sid>> {
        let mut visited: HashSet<Sid> = HashSet::new();
        let mut queue: VecDeque<Sid> = VecDeque::new();
        let mut results: Vec<Sid> = Vec::new();
        let mut added_target_via_cycle = false;

        // ZeroOrMore includes target node as a source
        if self.pattern.modifier == PathModifier::ZeroOrMore {
            results.push(target.clone());
            visited.insert(target.clone());
        }

        queue.push_back(target.clone());
        if self.pattern.modifier == PathModifier::OneOrMore {
            visited.insert(target.clone());
        }

        while let Some(current) = queue.pop_front() {
            // Safety bound check
            if visited.len() >= self.max_visited {
                return Err(QueryError::ResourceLimit(format!(
                    "Property path exceeded max visited nodes ({})",
                    self.max_visited
                )));
            }

            // POST index: (predicate=path_pred, object=current)
            let range_match = RangeMatch::new()
                .with_predicate(self.pattern.predicate.clone())
                .with_object(FlakeValue::Ref(current.clone()));

            let (db, overlay, to_t) = ctx.require_single_graph()?;

            let opts = RangeOptions::new().with_to_t(to_t);

            let flakes = range_with_overlay(
                db,
                ctx.binary_g_id,
                overlay,
                IndexType::Post,
                RangeTest::Eq,
                range_match,
                opts,
            )
            .await?;

            for flake in flakes {
                // Subject is always a Sid
                if self.pattern.modifier == PathModifier::OneOrMore
                    && !added_target_via_cycle
                    && &flake.s == target
                {
                    results.push(flake.s.clone());
                    added_target_via_cycle = true;
                    continue;
                }
                if visited.insert(flake.s.clone()) {
                    results.push(flake.s.clone());
                    queue.push_back(flake.s);
                }
            }
        }

        Ok(results)
    }

    /// Compute full transitive closure for a predicate (both vars unbound).
    ///
    /// Returns pairs (start, reachable) consistent with modifier semantics.
    async fn compute_closure(&self, ctx: &ExecutionContext<'_>) -> Result<Vec<(Sid, Sid)>> {
        // Pull all edges with this predicate using PSOT (predicate-indexed).
        let range_match = RangeMatch::predicate(self.pattern.predicate.clone());
        let (db, overlay, to_t) = ctx.require_single_graph()?;
        let opts = RangeOptions::new().with_to_t(to_t);
        let flakes = range_with_overlay(
            db,
            ctx.binary_g_id,
            overlay,
            IndexType::Psot,
            RangeTest::Eq,
            range_match,
            opts,
        )
        .await?;

        // Build adjacency (Sid -> Vec<Sid>)
        let mut adj: std::collections::HashMap<Sid, Vec<Sid>> = std::collections::HashMap::new();
        let mut nodes: HashSet<Sid> = HashSet::new();
        for flake in flakes {
            if let FlakeValue::Ref(o) = flake.o {
                nodes.insert(flake.s.clone());
                nodes.insert(o.clone());
                adj.entry(flake.s).or_default().push(o);
            }
        }

        let mut out: Vec<(Sid, Sid)> = Vec::new();
        for start in &nodes {
            // BFS from start using adjacency only (no DB calls).
            let mut visited: HashSet<Sid> = HashSet::new();
            let mut queue: VecDeque<Sid> = VecDeque::new();

            if self.pattern.modifier == PathModifier::ZeroOrMore {
                out.push((start.clone(), start.clone()));
                visited.insert(start.clone());
            }

            queue.push_back(start.clone());
            if self.pattern.modifier == PathModifier::OneOrMore {
                // Mark visited for cycle detection; still allow emitting (start,start) once via a cycle.
                visited.insert(start.clone());
            }

            let mut added_self_via_cycle = false;
            while let Some(cur) = queue.pop_front() {
                if visited.len() >= self.max_visited {
                    return Err(QueryError::ResourceLimit(format!(
                        "Property path exceeded max visited nodes ({})",
                        self.max_visited
                    )));
                }
                if let Some(nexts) = adj.get(&cur) {
                    for n in nexts {
                        if self.pattern.modifier == PathModifier::OneOrMore
                            && !added_self_via_cycle
                            && n == start
                        {
                            out.push((start.clone(), start.clone()));
                            added_self_via_cycle = true;
                            continue;
                        }
                        if visited.insert(n.clone()) {
                            out.push((start.clone(), n.clone()));
                            queue.push_back(n.clone());
                        }
                    }
                }
            }
        }

        Ok(out)
    }

    /// Check if a path exists between two nodes
    ///
    /// Used for reachability filter when both subject and object are bound.
    async fn path_exists(
        &self,
        ctx: &ExecutionContext<'_>,
        start: &Sid,
        target: &Sid,
    ) -> Result<bool> {
        // Use the same traversal semantics as forward execution and check membership.
        //
        // This avoids duplicating BFS logic and ensures cycle/self reachability behavior
        // matches the variable-binding mode.
        let reachable = self.traverse_forward(ctx, start).await?;
        Ok(reachable.iter().any(|sid| sid == target))
    }

    /// Execute unseeded mode (no child operator)
    ///
    /// This is called once during open() to compute all results.
    async fn execute_unseeded(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        let results = match (&self.pattern.subject, &self.pattern.object) {
            (Ref::Sid(subj), Ref::Var(_)) => {
                // Subject constant, object variable -> forward traversal
                let reachable = self.traverse_forward(ctx, subj).await?;
                reachable
                    .into_iter()
                    .map(|obj| (subj.clone(), obj))
                    .collect()
            }
            (Ref::Var(_), Ref::Sid(obj)) => {
                // Subject variable, object constant -> backward traversal
                let sources = self.traverse_backward(ctx, obj).await?;
                sources
                    .into_iter()
                    .map(|subj| (subj, obj.clone()))
                    .collect()
            }
            (Ref::Var(_), Ref::Var(_)) => self.compute_closure(ctx).await?,
            (Ref::Sid(_), Ref::Sid(_)) => {
                // Both constants: reachability check (0/1 rows). We use a dummy pair to indicate 1 row.
                let subj = match &self.pattern.subject {
                    Ref::Sid(s) => s,
                    _ => unreachable!(),
                };
                let obj = match &self.pattern.object {
                    Ref::Sid(s) => s,
                    _ => unreachable!(),
                };
                if self.path_exists(ctx, subj, obj).await? {
                    vec![(subj.clone(), obj.clone())]
                } else {
                    vec![]
                }
            }
            _ => {
                return Err(QueryError::InvalidQuery(
                    "Property path subject/object must be Var or Sid".to_string(),
                ));
            }
        };

        self.results_buffer = Some(results);
        self.results_idx = 0;
        Ok(())
    }

    /// Process a single child row in correlated mode
    async fn process_correlated_row(
        &self,
        ctx: &ExecutionContext<'_>,
        child_batch: &Batch,
        row_idx: usize,
    ) -> Result<Vec<Vec<Binding>>> {
        // Determine what's bound from child
        let subj_var = match &self.pattern.subject {
            Ref::Var(v) => Some(*v),
            _ => None,
        };
        let obj_var = match &self.pattern.object {
            Ref::Var(v) => Some(*v),
            _ => None,
        };

        // Get bound values from child
        let subj_binding = subj_var.and_then(|v| child_batch.column(v).map(|col| &col[row_idx]));
        let obj_binding = obj_var.and_then(|v| child_batch.column(v).map(|col| &col[row_idx]));

        // Resolve the active graph (property paths require a single active graph).
        let (db_for_encode, _overlay, _to_t) = ctx.require_single_graph()?;

        // Extract SIDs from constants or child bindings (if bound).
        //
        // Note: lowering may produce `Ref::Iri` constants to support cross-ledger joins.
        // For property paths we must traverse SIDs, so we opportunistically encode IRIs
        // against the selected active graph's namespace table.
        let resolve_sid = |term: &Ref, binding: Option<&Binding>| -> Option<Sid> {
            match term {
                Ref::Sid(s) => Some(s.clone()),
                Ref::Iri(iri) => db_for_encode.encode_iri(iri),
                Ref::Var(_) => binding.and_then(|b| match b {
                    Binding::Sid { sid: s, .. } => Some(s.clone()),
                    Binding::IriMatch { iri, .. } => db_for_encode.encode_iri(iri),
                    Binding::Iri(iri) => db_for_encode.encode_iri(iri),
                    _ => None,
                }),
            }
        };

        let subj_sid = resolve_sid(&self.pattern.subject, subj_binding);
        let obj_sid = resolve_sid(&self.pattern.object, obj_binding);

        // Determine execution mode based on what's bound
        match (subj_sid, obj_sid) {
            (Some(start), None) => {
                // Subject bound, object unbound -> forward traversal
                let reachable = self.traverse_forward(ctx, &start).await?;

                // Build output rows
                let mut rows = Vec::with_capacity(reachable.len());
                for obj in reachable {
                    let mut row: Vec<Binding> = Vec::with_capacity(self.in_schema.len());
                    // Copy child bindings
                    for var in self.in_schema.iter() {
                        if let Some(col) = child_batch.column(*var) {
                            row.push(col[row_idx].clone());
                        } else if Some(*var) == obj_var {
                            row.push(Binding::sid(obj.clone()));
                        } else if Some(*var) == subj_var {
                            row.push(Binding::sid(start.clone()));
                        } else {
                            row.push(Binding::Unbound);
                        }
                    }
                    rows.push(row);
                }
                Ok(rows)
            }
            (None, Some(target)) => {
                // Object bound, subject unbound -> backward traversal
                let sources = self.traverse_backward(ctx, &target).await?;

                let mut rows = Vec::with_capacity(sources.len());
                for subj in sources {
                    let mut row: Vec<Binding> = Vec::with_capacity(self.in_schema.len());
                    for var in self.in_schema.iter() {
                        if let Some(col) = child_batch.column(*var) {
                            row.push(col[row_idx].clone());
                        } else if Some(*var) == subj_var {
                            row.push(Binding::sid(subj.clone()));
                        } else if Some(*var) == obj_var {
                            row.push(Binding::sid(target.clone()));
                        } else {
                            row.push(Binding::Unbound);
                        }
                    }
                    rows.push(row);
                }
                Ok(rows)
            }
            (Some(start), Some(target)) => {
                // Both bound -> reachability filter
                let exists = self.path_exists(ctx, &start, &target).await?;
                if exists {
                    // Keep this row unchanged
                    let mut row: Vec<Binding> = Vec::with_capacity(self.in_schema.len());
                    for var in self.in_schema.iter() {
                        if let Some(col) = child_batch.column(*var) {
                            row.push(col[row_idx].clone());
                        } else {
                            row.push(Binding::Unbound);
                        }
                    }
                    Ok(vec![row])
                } else {
                    // Filter out this row
                    Ok(vec![])
                }
            }
            (None, None) => {
                // Neither bound in correlated mode: treat as a bounded full closure for this predicate.
                // This commonly happens when the only child is the seed operator.
                let pairs = self.compute_closure(ctx).await?;
                let mut rows = Vec::with_capacity(pairs.len());
                for (subj, obj) in pairs {
                    let mut row: Vec<Binding> = Vec::with_capacity(self.in_schema.len());
                    for var in self.in_schema.iter() {
                        if let Some(col) = child_batch.column(*var) {
                            row.push(col[row_idx].clone());
                        } else if Some(*var) == subj_var {
                            row.push(Binding::sid(subj.clone()));
                        } else if Some(*var) == obj_var {
                            row.push(Binding::sid(obj.clone()));
                        } else {
                            row.push(Binding::Unbound);
                        }
                    }
                    rows.push(row);
                }
                Ok(rows)
            }
        }
    }
}

#[async_trait]
impl Operator for PropertyPathOperator {
    fn schema(&self) -> &[VarId] {
        effective_schema(&self.out_schema, &self.in_schema)
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        if let Some(child) = &mut self.child {
            // Correlated mode: open child
            child.open(ctx).await?;
        } else {
            // Unseeded mode: execute traversal now
            self.execute_unseeded(ctx).await?;
        }
        self.state = OperatorState::Open;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if self.state != OperatorState::Open {
            return Ok(None);
        }

        if self.child.is_none() {
            // Unseeded mode: return buffered results
            let results = self
                .results_buffer
                .as_ref()
                .ok_or_else(|| QueryError::OperatorNotOpened)?;

            if self.results_idx >= results.len() {
                self.state = OperatorState::Exhausted;
                return Ok(None);
            }

            // Build a batch from results
            const BATCH_SIZE: usize = 1024;
            let end = std::cmp::min(self.results_idx + BATCH_SIZE, results.len());
            let batch_results = &results[self.results_idx..end];
            self.results_idx = end;

            if batch_results.is_empty() {
                self.state = OperatorState::Exhausted;
                return Ok(None);
            }

            // Build columns
            let mut columns: Vec<Vec<Binding>> = self
                .in_schema
                .iter()
                .map(|_| Vec::with_capacity(batch_results.len()))
                .collect();

            let subj_var = match &self.pattern.subject {
                Ref::Var(v) => Some(*v),
                _ => None,
            };
            let obj_var = match &self.pattern.object {
                Ref::Var(v) => Some(*v),
                _ => None,
            };

            for (subj, obj) in batch_results {
                for (col_idx, var) in self.in_schema.iter().enumerate() {
                    if Some(*var) == subj_var {
                        columns[col_idx].push(Binding::sid(subj.clone()));
                    } else if Some(*var) == obj_var {
                        columns[col_idx].push(Binding::sid(obj.clone()));
                    } else {
                        columns[col_idx].push(Binding::Unbound);
                    }
                }
            }

            let batch = Batch::new(self.in_schema.clone(), columns)?;
            return Ok(trim_batch(&self.out_schema, batch));
        }

        // Correlated mode: process child rows
        loop {
            // Get next child batch if needed
            if self.current_child_batch.is_none() {
                let child = self.child.as_mut().unwrap();
                match child.next_batch(ctx).await? {
                    Some(batch) if !batch.is_empty() => {
                        self.current_child_batch = Some(batch);
                        self.current_child_row = 0;
                    }
                    Some(_) => continue, // Empty batch, try next
                    None => {
                        self.state = OperatorState::Exhausted;
                        return Ok(None);
                    }
                }
            }

            let child_batch = self.current_child_batch.as_ref().unwrap();

            // Process rows from current child batch
            let mut all_rows: Vec<Vec<Binding>> = Vec::new();

            while self.current_child_row < child_batch.len() {
                let rows = self
                    .process_correlated_row(ctx, child_batch, self.current_child_row)
                    .await?;
                self.current_child_row += 1;
                all_rows.extend(rows);

                // Return batch if we have enough rows
                if all_rows.len() >= 1024 {
                    break;
                }
            }

            // Clear current batch if exhausted
            if self.current_child_row >= child_batch.len() {
                self.current_child_batch = None;
            }

            if !all_rows.is_empty() {
                // Build batch from rows
                let mut columns: Vec<Vec<Binding>> = self
                    .in_schema
                    .iter()
                    .map(|_| Vec::with_capacity(all_rows.len()))
                    .collect();

                for row in all_rows {
                    for (col_idx, binding) in row.into_iter().enumerate() {
                        columns[col_idx].push(binding);
                    }
                }

                let batch = Batch::new(self.in_schema.clone(), columns)?;
                return Ok(trim_batch(&self.out_schema, batch));
            }

            // No rows from this batch, try next
        }
    }

    fn close(&mut self) {
        if let Some(ref mut child) = self.child {
            child.close();
        }
        self.results_buffer = None;
        self.current_child_batch = None;
        self.state = OperatorState::Closed;
    }

    fn estimated_rows(&self) -> Option<usize> {
        // Hard to estimate for transitive traversal
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::triple::Ref;
    use fluree_db_core::Sid;

    fn make_pattern(subj: Ref, pred: Sid, modifier: PathModifier, obj: Ref) -> PropertyPathPattern {
        PropertyPathPattern::new(subj, pred, modifier, obj)
    }

    #[test]
    fn test_property_path_schema_forward() {
        // Subject constant, object variable -> schema has only object var
        let pattern = make_pattern(
            Ref::Sid(Sid::new(1, "alice")),
            Sid::new(2, "knows"),
            PathModifier::OneOrMore,
            Ref::Var(VarId(0)),
        );

        let op: PropertyPathOperator = PropertyPathOperator::with_defaults(None, pattern);

        assert_eq!(op.schema(), &[VarId(0)]);
    }

    #[test]
    fn test_property_path_schema_backward() {
        // Subject variable, object constant -> schema has only subject var
        let pattern = make_pattern(
            Ref::Var(VarId(0)),
            Sid::new(2, "knows"),
            PathModifier::ZeroOrMore,
            Ref::Sid(Sid::new(1, "bob")),
        );

        let op: PropertyPathOperator = PropertyPathOperator::with_defaults(None, pattern);

        assert_eq!(op.schema(), &[VarId(0)]);
    }

    #[test]
    fn test_property_path_schema_both_vars() {
        // Both subject and object are variables
        let pattern = make_pattern(
            Ref::Var(VarId(0)),
            Sid::new(2, "knows"),
            PathModifier::OneOrMore,
            Ref::Var(VarId(1)),
        );

        let op: PropertyPathOperator = PropertyPathOperator::with_defaults(None, pattern);

        assert_eq!(op.schema(), &[VarId(0), VarId(1)]);
    }

    #[test]
    fn test_property_path_max_visited_configurable() {
        let pattern = make_pattern(
            Ref::Sid(Sid::new(1, "alice")),
            Sid::new(2, "knows"),
            PathModifier::OneOrMore,
            Ref::Var(VarId(0)),
        );

        let op: PropertyPathOperator = PropertyPathOperator::new(None, pattern, 100);

        assert_eq!(op.max_visited, 100);
    }
}
