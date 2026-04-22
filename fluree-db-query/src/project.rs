//! Project operator - selects and reorders columns
//!
//! The ProjectOperator takes a child operator and produces batches
//! containing only the specified variables in the specified order.

use crate::binding::Batch;
use crate::context::ExecutionContext;
use crate::error::{QueryError, Result};
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::var_registry::VarId;
use async_trait::async_trait;

/// Project operator - selects and reorders columns from child
pub struct ProjectOperator {
    /// Child operator
    child: BoxedOperator,
    /// Variables to project (in output order)
    vars: Vec<VarId>,
    /// Operator state
    state: OperatorState,
}

impl ProjectOperator {
    /// Create a new project operator
    pub fn new(child: BoxedOperator, vars: Vec<VarId>) -> Self {
        Self {
            child,
            vars,
            state: OperatorState::Created,
        }
    }
}

#[async_trait]
impl Operator for ProjectOperator {
    fn schema(&self) -> &[VarId] {
        &self.vars
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
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if !self.state.can_next() {
            if self.state == OperatorState::Created {
                return Err(QueryError::OperatorNotOpened);
            }
            return Ok(None);
        }

        // Get batch from child
        let batch = match self.child.next_batch(ctx).await? {
            Some(b) => b,
            None => {
                self.state = OperatorState::Exhausted;
                return Ok(None);
            }
        };

        // Project to requested variables
        // If the projection matches exactly, return as-is
        if batch.schema() == self.vars.as_slice() {
            return Ok(Some(batch));
        }

        // Otherwise project
        match batch.project(&self.vars) {
            Some(projected) => Ok(Some(projected)),
            None => {
                // Variable not found in batch schema
                Err(QueryError::VariableNotFound(
                    "Projected variable not in child schema".to_string(),
                ))
            }
        }
    }

    fn close(&mut self) {
        self.child.close();
        self.state = OperatorState::Closed;
    }

    fn estimated_rows(&self) -> Option<usize> {
        // Same as child (projection doesn't change row count)
        self.child.estimated_rows()
    }
}

#[cfg(test)]
mod tests {
    // Tests require a mock operator - will be added with integration tests
}
