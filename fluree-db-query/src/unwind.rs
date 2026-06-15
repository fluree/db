//! UNWIND operator — explode a runtime list-valued expression into rows.
//!
//! For each input row, evaluates the list expression to a [`Binding::List`] and
//! emits one output row per element with `var` bound to it (the input row's
//! other bindings are preserved). An empty or unbound list drops the row
//! (Cypher semantics: `UNWIND []` / `UNWIND null` yields nothing).
//!
//! Cypher `UNWIND <expr> AS var` over a *non-constant* list — e.g.
//! `UNWIND nodes(path) AS n`. A constant list (`UNWIND [1,2,3] AS x`) lowers to
//! `Pattern::Values` at compile time and never reaches this operator.

use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::Result;
use crate::ir::Expression;
use crate::operator::{
    compute_trimmed_vars, effective_schema, trim_batch, BoxedOperator, Operator, OperatorState,
};
use crate::var_registry::VarId;
use async_trait::async_trait;
use std::sync::Arc;

/// UNWIND operator — fans each input row out over a list-valued expression.
pub struct UnwindOperator {
    child: BoxedOperator,
    /// List-valued expression evaluated per input row.
    list: Expression,
    /// Output schema (child schema + var if new).
    in_schema: Arc<[VarId]>,
    /// True if `var` is new (appended), false if it shadows a child column.
    is_new_var: bool,
    /// Position of `var` in the output schema.
    var_position: usize,
    state: OperatorState,
    out_schema: Option<Arc<[VarId]>>,
}

impl UnwindOperator {
    /// Create a new UNWIND operator.
    pub fn new(child: BoxedOperator, var: VarId, list: Expression) -> Self {
        let child_schema = child.schema();
        let (schema, var_position, is_new_var): (Arc<[VarId]>, usize, bool) =
            match child_schema.iter().position(|&v| v == var) {
                Some(pos) => (Arc::from(child_schema.to_vec().into_boxed_slice()), pos, false),
                None => {
                    let mut s = child_schema.to_vec();
                    let pos = s.len();
                    s.push(var);
                    (Arc::from(s.into_boxed_slice()), pos, true)
                }
            };
        Self {
            child,
            list,
            in_schema: schema,
            is_new_var,
            var_position,
            state: OperatorState::Created,
            out_schema: None,
        }
    }

    /// Trim output to only the specified downstream variables.
    pub fn with_out_schema(mut self, downstream_vars: Option<&[VarId]>) -> Self {
        self.out_schema = compute_trimmed_vars(&self.in_schema, downstream_vars);
        self
    }

    fn child_cols(&self) -> usize {
        self.child.schema().len()
    }
}

#[async_trait]
impl Operator for UnwindOperator {
    fn plan_children(&self) -> Vec<crate::plan_node::PlanChild<'_>> {
        vec![crate::plan_node::PlanChild::child(self.child.as_ref())]
    }

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
            let input_batch = match self.child.next_batch(ctx).await? {
                Some(b) if !b.is_empty() => b,
                Some(_) => continue,
                None => {
                    self.state = OperatorState::Exhausted;
                    return Ok(None);
                }
            };

            let num_cols = self.in_schema.len();
            let mut columns: Vec<Vec<Binding>> = (0..num_cols).map(|_| Vec::new()).collect();

            for row_idx in 0..input_batch.len() {
                let row_view = input_batch.row_view(row_idx).unwrap();
                // A list element evaluates through the binding-producing path.
                let elements = match self.list.try_eval_to_binding(&row_view, Some(ctx))? {
                    Binding::List(items) => items,
                    // `UNWIND null` / non-list / empty → no rows for this input.
                    _ => continue,
                };

                let child_cols = self.child_cols();
                for element in elements {
                    for (col, column) in columns.iter_mut().take(child_cols).enumerate() {
                        column.push(input_batch.get_by_col(row_idx, col).clone());
                    }
                    if self.is_new_var {
                        columns[self.var_position].push(element);
                    } else {
                        // Shadowing an existing column: overwrite the just-copied
                        // child value at the var's position.
                        columns[self.var_position].pop();
                        columns[self.var_position].push(element);
                    }
                }
            }

            if columns[0].is_empty() && num_cols > 0 {
                // Every input row had an empty/unbound list — fetch the next batch.
                continue;
            }

            let batch = Batch::new(self.in_schema.clone(), columns)?;
            return Ok(trim_batch(&self.out_schema, batch));
        }
    }

    fn close(&mut self) {
        self.child.close();
        self.state = OperatorState::Closed;
    }

    fn estimated_rows(&self) -> Option<usize> {
        None
    }
}
