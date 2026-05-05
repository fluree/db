//! BIND operator - evaluates expression and binds result to variable
//!
//! The BindOperator evaluates a filter expression and binds the result
//! to a variable with clobber prevention:
//!
//! - If variable is Unbound: bind the computed value
//! - If variable has the SAME value: pass through (compatible)
//! - If variable has a DIFFERENT value: drop the row (clobber prevention)
//!
//! Evaluation errors produce `Binding::Unbound` (NOT `Binding::Poisoned`).
//! Poisoned is reserved strictly for OPTIONAL semantics.
//!
//! For example, `BIND(?x + 10 AS ?y)` evaluates the expression for each input row and binds the result to `?y`.

use crate::binding::{Batch, Binding, RowAccess};
use crate::context::ExecutionContext;
use crate::error::Result;
use crate::eval::{passes_filters, PreparedBoolExpression};
use crate::ir::Expression;
use crate::operator::{
    compute_trimmed_vars, effective_schema, trim_batch, BoxedOperator, Operator, OperatorState,
};
use crate::var_registry::VarId;
use async_trait::async_trait;
use std::sync::Arc;

/// BIND operator - evaluates expression and binds to variable
///
/// Implements SPARQL BIND semantics with clobber prevention:
/// - New variable: bind computed value
/// - Same value: pass through
/// - Different value: drop row
pub struct BindOperator {
    /// Child operator providing input solutions
    child: BoxedOperator,
    /// Variable to bind the result to
    var: VarId,
    /// Expression to evaluate
    expr: Expression,
    /// Inline filters evaluated after computing the BIND value.
    /// Rows that fail any filter are dropped before materialization,
    /// eliminating the overhead of a separate FilterOperator.
    filters: Vec<PreparedBoolExpression>,
    /// Output schema (child schema with var added if new)
    in_schema: Arc<[VarId]>,
    /// Position of var in output schema
    var_position: usize,
    /// Whether this is a new variable (not in child schema)
    is_new_var: bool,
    /// Operator state
    state: OperatorState,
    /// Variables required by downstream operators; if set, output is trimmed.
    out_schema: Option<Arc<[VarId]>>,
}

impl BindOperator {
    /// Create a new BIND operator
    ///
    /// # Arguments
    ///
    /// * `child` - Child operator providing input solutions
    /// * `var` - Variable to bind the computed value to
    /// * `expr` - Expression to evaluate
    /// * `filters` - Inline filter expressions evaluated after computing the
    ///   BIND value; rows failing any filter are dropped before materialization
    pub fn new(
        child: BoxedOperator,
        var: VarId,
        expr: Expression,
        filters: Vec<Expression>,
    ) -> Self {
        let child_schema = child.schema();

        // Check if var already exists in child schema
        let existing_pos = child_schema.iter().position(|&v| v == var);

        let (schema, var_position, is_new_var): (Arc<[VarId]>, usize, bool) = match existing_pos {
            Some(pos) => {
                // Variable exists - schema stays the same
                (
                    Arc::from(child_schema.to_vec().into_boxed_slice()),
                    pos,
                    false,
                )
            }
            None => {
                // New variable - add to schema
                let mut new_schema = child_schema.to_vec();
                let pos = new_schema.len();
                new_schema.push(var);
                (Arc::from(new_schema.into_boxed_slice()), pos, true)
            }
        };

        Self {
            child,
            var,
            expr,
            filters: filters
                .into_iter()
                .map(PreparedBoolExpression::new)
                .collect(),
            in_schema: schema,
            var_position,
            is_new_var,
            state: OperatorState::Created,
            out_schema: None,
        }
    }

    /// Trim output to only the specified downstream variables.
    pub fn with_out_schema(mut self, downstream_vars: Option<&[VarId]>) -> Self {
        self.out_schema = compute_trimmed_vars(&self.in_schema, downstream_vars);
        self
    }
}

#[async_trait]
impl Operator for BindOperator {
    fn schema(&self) -> &[VarId] {
        effective_schema(&self.out_schema, &self.in_schema)
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
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
                Some(b) => b,
                None => {
                    self.state = OperatorState::Exhausted;
                    return Ok(None);
                }
            };

            if input_batch.is_empty() {
                continue;
            }

            let child_num_cols = self.child.schema().len();
            let num_output_cols = self.in_schema.len();

            // Build output columns
            let mut output_columns: Vec<Vec<Binding>> = (0..num_output_cols)
                .map(|_| Vec::with_capacity(input_batch.len()))
                .collect();

            // Reusable buffer for filter evaluation (avoids per-row allocation)
            let filter_row_cap = if self.filters.is_empty() {
                0
            } else {
                num_output_cols
            };
            let mut filter_row = Vec::with_capacity(filter_row_cap);

            // Process each row
            for row_idx in 0..input_batch.len() {
                let row_view = input_batch.row_view(row_idx).unwrap();

                // Evaluate expression. Non-strict mode still propagates fatal
                // execution issues such as dictionary lookup failures.
                let computed = if ctx.strict_bind_errors {
                    self.expr.try_eval_to_binding(&row_view, Some(ctx))?
                } else {
                    self.expr
                        .try_eval_to_binding_non_strict(&row_view, Some(ctx))?
                };

                // Check clobber prevention if variable already exists
                let keep_row = if self.is_new_var {
                    // New variable - always keep
                    true
                } else {
                    // Existing variable - check compatibility
                    let existing = row_view.get(self.var);
                    match existing {
                        Some(Binding::Unbound) | None => true, // Unbound - can bind
                        Some(existing_val) => {
                            // Check if same value
                            existing_val == &computed || matches!(computed, Binding::Unbound)
                        }
                    }
                };

                if !keep_row {
                    // Clobber detected - drop this row
                    continue;
                }

                // Evaluate inline filters against a row that includes the BIND output
                if !self.filters.is_empty() {
                    filter_row.clear();
                    for col in 0..child_num_cols {
                        filter_row.push(input_batch.get_by_col(row_idx, col).clone());
                    }
                    if self.is_new_var {
                        filter_row.push(computed.clone());
                    } else {
                        filter_row[self.var_position] = computed.clone();
                    }
                    if !passes_filters(&self.filters, &self.in_schema, &filter_row, Some(ctx))? {
                        continue;
                    }
                }

                // Copy child columns
                for (col_idx, output_col) in
                    output_columns.iter_mut().enumerate().take(child_num_cols)
                {
                    let binding = input_batch.get_by_col(row_idx, col_idx).clone();
                    output_col.push(binding);
                }

                // Add bound value
                if self.is_new_var {
                    // New column at the end
                    output_columns[self.var_position].push(computed);
                } else {
                    // Existing variable:
                    // - If computed is Unbound (e.g. eval error), do NOT clobber an existing bound value.
                    // - Otherwise, overwrite the existing column for this row with the computed value.
                    if matches!(computed, Binding::Unbound) {
                        // Keep the existing value we already pushed.
                    } else {
                        // Replace the last pushed value for this column.
                        output_columns[self.var_position].pop();
                        output_columns[self.var_position].push(computed);
                    }
                }
            }

            // Check if any rows remain after filtering
            if output_columns
                .first()
                .map(std::vec::Vec::is_empty)
                .unwrap_or(true)
            {
                continue;
            }

            let batch = Batch::new(self.in_schema.clone(), output_columns)?;
            return Ok(trim_batch(&self.out_schema, batch));
        }
    }

    fn close(&mut self) {
        self.child.close();
        self.state = OperatorState::Closed;
    }

    fn estimated_rows(&self) -> Option<usize> {
        // Some rows may be dropped due to clobber prevention
        // Estimate same as child (upper bound)
        self.child.estimated_rows()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binding::Batch;
    use fluree_db_core::value::FlakeValue;

    #[test]
    fn test_bind_operator_new_var_schema() {
        // Child has ?a, BIND adds ?b
        let child_schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let child = Box::new(TestEmptyWithSchema {
            schema: child_schema,
        });

        let expr = Expression::Const(FlakeValue::Long(42));
        let op = BindOperator::new(child, VarId(1), expr, vec![]);

        // Output schema should be [?a, ?b]
        assert_eq!(op.schema().len(), 2);
        assert_eq!(op.schema()[0], VarId(0));
        assert_eq!(op.schema()[1], VarId(1));
        assert!(op.is_new_var);
    }

    #[test]
    fn test_bind_operator_existing_var_schema() {
        // Child has ?a ?b, BIND to ?a (existing)
        let child_schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1)].into_boxed_slice());
        let child = Box::new(TestEmptyWithSchema {
            schema: child_schema,
        });

        let expr = Expression::Const(FlakeValue::Long(42));
        let op = BindOperator::new(child, VarId(0), expr, vec![]);

        // Schema should stay [?a, ?b]
        assert_eq!(op.schema().len(), 2);
        assert_eq!(op.schema()[0], VarId(0));
        assert_eq!(op.schema()[1], VarId(1));
        assert!(!op.is_new_var);
        assert_eq!(op.var_position, 0);
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
