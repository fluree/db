//! EXISTS and NOT EXISTS operators - subquery existence filters
//!
//! Implements SPARQL EXISTS/NOT EXISTS semantics:
//! - For each input row, execute the EXISTS patterns seeded with current row bindings
//! - EXISTS: keep rows where at least one match is found
//! - NOT EXISTS: keep rows where no match is found
//!
//! Key semantics:
//! - EXISTS/NOT EXISTS executes with **current bindings** (outer scope available/correlated)
//! - Only checks existence (any result = true)
//! - Does NOT add new variables to output solution
//! - Short-circuits on first match for efficiency

use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::Result;
use crate::execute::build_where_operators_seeded;
use crate::ir::Pattern;
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::seed::{EmptyOperator, SeedOperator};
use crate::temporal_mode::PlanningContext;
use crate::var_registry::VarId;
use async_trait::async_trait;
use fluree_db_core::StatsView;
use std::collections::HashSet;
use std::sync::Arc;

/// EXISTS operator - keeps rows where subquery has at least one match
///
/// For each input row, executes the EXISTS patterns seeded with that row's bindings.
/// If any result is produced, the input row is kept; otherwise it's filtered out.
///
/// For NOT EXISTS, the `negated` field is true and the logic is inverted.
pub struct ExistsOperator {
    /// Child operator providing input solutions
    child: BoxedOperator,
    /// EXISTS/NOT EXISTS patterns to execute
    exists_patterns: Vec<Pattern>,
    /// If true, this is NOT EXISTS (invert the check)
    negated: bool,
    /// Output schema (same as child - EXISTS doesn't add variables)
    schema: Arc<[VarId]>,
    /// Operator state
    state: OperatorState,
    /// True if the exists subquery is uncorrelated (shares no vars with input schema)
    uncorrelated: bool,
    /// Cached existence result for uncorrelated EXISTS/NOT EXISTS (computed once in open)
    uncorrelated_has_match: Option<bool>,
    /// Optional stats for nested query optimization (Arc for cheap cloning in nested operators)
    stats: Option<Arc<StatsView>>,
    /// Planning context captured at planner-time. Used for the EXISTS subplan.
    planning: PlanningContext,
}

impl ExistsOperator {
    /// Create a new EXISTS operator
    ///
    /// # Arguments
    ///
    /// * `child` - Input solutions operator
    /// * `exists_patterns` - Patterns to execute for existence check
    /// * `negated` - If true, this is NOT EXISTS (keep rows where NO match found)
    /// * `stats` - Optional stats for nested query optimization (Arc for cheap cloning)
    pub fn new(
        child: BoxedOperator,
        exists_patterns: Vec<Pattern>,
        negated: bool,
        stats: Option<Arc<StatsView>>,
        planning: PlanningContext,
    ) -> Self {
        let schema: Arc<[VarId]> = Arc::from(child.schema().to_vec().into_boxed_slice());

        // Detect correlation: if no vars in patterns intersect the input schema,
        // EXISTS/NOT EXISTS reduces to a global boolean (evaluate once).
        let input_vars: HashSet<VarId> = schema.iter().copied().collect();
        let pattern_vars: HashSet<VarId> = exists_patterns
            .iter()
            .flat_map(super::ir::Pattern::variables)
            .collect();
        let uncorrelated = pattern_vars.is_disjoint(&input_vars);

        Self {
            child,
            exists_patterns,
            negated,
            schema,
            state: OperatorState::Created,
            uncorrelated,
            uncorrelated_has_match: None,
            stats,
            planning,
        }
    }

    /// Create a NOT EXISTS operator (convenience constructor)
    pub fn not_exists(
        child: BoxedOperator,
        patterns: Vec<Pattern>,
        stats: Option<Arc<StatsView>>,
        planning: PlanningContext,
    ) -> Self {
        Self::new(child, patterns, true, stats, planning)
    }

    /// Check if the EXISTS subquery produces any results for a given input row
    async fn has_match(
        &self,
        ctx: &ExecutionContext<'_>,
        input_batch: &Batch,
        row_idx: usize,
    ) -> Result<bool> {
        if self.uncorrelated {
            // Safe because we fill this in open() before next_batch can be called.
            return Ok(self.uncorrelated_has_match.unwrap_or(false));
        }

        // Seed with current row bindings (correlated subquery)
        let seed = SeedOperator::from_batch_row(input_batch, row_idx);
        let mut exists_op = build_where_operators_seeded(
            Some(Box::new(seed)),
            &self.exists_patterns,
            self.stats.clone(),
            None,
            &self.planning,
        )?;

        exists_op.open(ctx).await?;

        // Short-circuit: check if any result is produced
        let has_result = loop {
            match exists_op.next_batch(ctx).await? {
                Some(batch) if !batch.is_empty() => break true,
                Some(_) => continue, // Empty batch, try next
                None => break false, // No results
            }
        };

        exists_op.close();
        Ok(has_result)
    }
}

#[async_trait]
impl Operator for ExistsOperator {
    fn schema(&self) -> &[VarId] {
        &self.schema
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        if self.uncorrelated {
            // Evaluate once with an empty seed (fresh scope).
            #[allow(clippy::box_default)]
            let seed: BoxedOperator = Box::new(EmptyOperator::new());
            let mut exists_op = build_where_operators_seeded(
                Some(seed),
                &self.exists_patterns,
                self.stats.clone(),
                None,
                &self.planning,
            )?;
            exists_op.open(ctx).await?;
            let has_result = loop {
                match exists_op.next_batch(ctx).await? {
                    Some(batch) if !batch.is_empty() => break true,
                    Some(_) => continue,
                    None => break false,
                }
            };
            exists_op.close();
            self.uncorrelated_has_match = Some(has_result);
        }

        self.child.open(ctx).await?;
        self.state = OperatorState::Open;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if self.state != OperatorState::Open {
            return Ok(None);
        }

        loop {
            // Get next batch from child
            let input_batch = match self.child.next_batch(ctx).await? {
                Some(b) if !b.is_empty() => b,
                Some(_) => continue, // Skip empty batches
                None => {
                    self.state = OperatorState::Exhausted;
                    return Ok(None);
                }
            };

            // For each input row, check if it should be kept
            let mut keep_rows: Vec<bool> = Vec::with_capacity(input_batch.len());

            for row_idx in 0..input_batch.len() {
                let has_match = self.has_match(ctx, &input_batch, row_idx).await?;

                // EXISTS: keep if match found
                // NOT EXISTS: keep if NO match found
                let keep = if self.negated { !has_match } else { has_match };
                keep_rows.push(keep);
            }

            // Build output batch with only kept rows
            let kept_count = keep_rows.iter().filter(|&&k| k).count();
            if kept_count == 0 {
                // All rows filtered out, try next input batch
                continue;
            }

            if kept_count == input_batch.len() {
                // All rows kept, return unchanged
                return Ok(Some(input_batch));
            }

            // Build filtered batch
            let mut columns: Vec<Vec<Binding>> = (0..self.schema.len())
                .map(|_| Vec::with_capacity(kept_count))
                .collect();

            for (row_idx, keep) in keep_rows.iter().enumerate() {
                if *keep {
                    for (col_idx, var) in self.schema.iter().enumerate() {
                        if let Some(col) = input_batch.column(*var) {
                            columns[col_idx].push(col[row_idx].clone());
                        }
                    }
                }
            }

            return Ok(Some(Batch::new(self.schema.clone(), columns)?));
        }
    }

    fn close(&mut self) {
        self.child.close();
        self.state = OperatorState::Closed;
    }

    fn estimated_rows(&self) -> Option<usize> {
        // Could be 0 to child rows, return child estimate as upper bound
        self.child.estimated_rows()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::triple::{Ref, Term, TriplePattern};
    use fluree_db_core::Sid;

    #[test]
    fn test_exists_schema_preserved() {
        let child_schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1)].into_boxed_slice());
        let child: BoxedOperator = Box::new(TestEmptyWithSchema {
            schema: child_schema.clone(),
        });

        let op = ExistsOperator::new(
            child,
            vec![],
            false,
            None,
            crate::temporal_mode::PlanningContext::current(),
        );

        // Output schema should match child schema (EXISTS doesn't add vars)
        assert_eq!(op.schema(), &*child_schema);
    }

    #[test]
    fn test_not_exists_constructor() {
        let child_schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let child: BoxedOperator = Box::new(TestEmptyWithSchema {
            schema: child_schema,
        });

        let patterns = vec![Pattern::Triple(TriplePattern::new(
            Ref::Var(VarId(0)),
            Ref::Sid(Sid::new(100, "age")),
            Term::Var(VarId(1)),
        ))];

        let op = ExistsOperator::not_exists(
            child,
            patterns,
            None,
            crate::temporal_mode::PlanningContext::current(),
        );

        assert!(op.negated);
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
}
