//! Seed operators for providing initial solutions
//!
//! This module provides operators that seed the execution pipeline with
//! initial solutions:
//!
//! - `SeedOperator`: Yields a single row from a batch (for correlated subqueries)
//! - `EmptyOperator`: Yields a single empty solution (for queries starting with non-triple patterns)
//!
//! These are used when the query doesn't start with a triple pattern (e.g., VALUES, BIND, UNION)
//! or when executing correlated branches in UNION/OPTIONAL.

use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::Result;
use crate::operator::{Operator, OperatorState};
use crate::var_registry::VarId;
use async_trait::async_trait;
use std::sync::Arc;

/// Operator that yields exactly one row from a source batch
///
/// Used for correlated subqueries where each input row needs to be
/// propagated into a nested operator tree (e.g., UNION branches).
///
/// Yields a single row extracted from a source batch, then exhausts.
pub struct SeedOperator {
    /// Schema of the output (same as source batch)
    schema: Arc<[VarId]>,
    /// The single row to emit
    row: Vec<Binding>,
    /// Whether we've emitted the row yet
    emitted: bool,
    /// Operator state
    state: OperatorState,
}

impl SeedOperator {
    /// Create a seed operator from a specific row in a batch
    ///
    /// # Panics
    ///
    /// Panics if `row_idx` is out of bounds for the batch.
    pub fn from_batch_row(batch: &Batch, row_idx: usize) -> Self {
        let schema = Arc::from(batch.schema().to_vec().into_boxed_slice());
        let row = batch
            .row_view(row_idx)
            .expect("row_idx must be valid for batch")
            .to_vec();

        Self {
            schema,
            row,
            emitted: false,
            state: OperatorState::Created,
        }
    }

    /// Create a seed operator from explicit schema and row
    pub fn from_row(schema: Arc<[VarId]>, row: Vec<Binding>) -> Self {
        debug_assert_eq!(
            schema.len(),
            row.len(),
            "schema length must match row length"
        );

        Self {
            schema,
            row,
            emitted: false,
            state: OperatorState::Created,
        }
    }

    /// Get the output schema (inherent method for use without trait bounds)
    pub fn schema(&self) -> &[VarId] {
        &self.schema
    }

    /// Get the estimated row count (inherent method for use without trait bounds)
    pub fn estimated_rows(&self) -> Option<usize> {
        Some(1)
    }
}

#[async_trait]
impl Operator for SeedOperator {
    fn schema(&self) -> &[VarId] {
        &self.schema
    }

    async fn open(&mut self, _ctx: &ExecutionContext<'_>) -> Result<()> {
        self.state = OperatorState::Open;
        self.emitted = false;
        Ok(())
    }

    async fn next_batch(&mut self, _ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if self.state != OperatorState::Open {
            return Ok(None);
        }

        if self.emitted {
            self.state = OperatorState::Exhausted;
            return Ok(None);
        }

        self.emitted = true;

        // Build a batch with just this one row.
        //
        // IMPORTANT: An empty schema still represents a *single* empty solution (len=1),
        // so we must not use `Batch::new` (it infers len=0 when there are no columns).
        if self.schema.is_empty() {
            return Ok(Some(Batch::single_empty()));
        }

        let columns: Vec<Vec<Binding>> = self.row.iter().map(|b| vec![b.clone()]).collect();
        let batch = Batch::new(self.schema.clone(), columns)?;

        Ok(Some(batch))
    }

    fn close(&mut self) {
        self.state = OperatorState::Closed;
    }

    fn estimated_rows(&self) -> Option<usize> {
        Some(1)
    }
}

/// Operator that yields a single empty solution
///
/// Used when a query starts with a non-triple pattern (VALUES, BIND, UNION, FILTER).
/// Produces one row with zero columns, allowing downstream operators to receive
/// an initial solution to work with.
///
/// Produces one row with zero columns, providing an initial solution for downstream operators.
pub struct EmptyOperator {
    /// Operator state
    state: OperatorState,
    /// Whether we've emitted the empty solution
    emitted: bool,
}

impl EmptyOperator {
    /// Create a new empty operator
    pub fn new() -> Self {
        Self {
            state: OperatorState::Created,
            emitted: false,
        }
    }

    /// Get the output schema (inherent method for use without trait bounds)
    ///
    /// Always returns empty slice since EmptyOperator has no columns.
    pub fn schema(&self) -> &[VarId] {
        &[]
    }

    /// Get the estimated row count (inherent method for use without trait bounds)
    pub fn estimated_rows(&self) -> Option<usize> {
        Some(1)
    }
}

impl Default for EmptyOperator {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Operator for EmptyOperator {
    fn schema(&self) -> &[VarId] {
        // Empty schema - no columns
        &[]
    }

    async fn open(&mut self, _ctx: &ExecutionContext<'_>) -> Result<()> {
        self.state = OperatorState::Open;
        self.emitted = false;
        Ok(())
    }

    async fn next_batch(&mut self, _ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if self.state != OperatorState::Open {
            return Ok(None);
        }

        if self.emitted {
            self.state = OperatorState::Exhausted;
            return Ok(None);
        }

        self.emitted = true;

        // Return a single empty solution (1 row, 0 columns)
        Ok(Some(Batch::single_empty()))
    }

    fn close(&mut self) {
        self.state = OperatorState::Closed;
    }

    fn estimated_rows(&self) -> Option<usize> {
        Some(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binding::Binding;
    use fluree_db_core::{FlakeValue, Sid};

    fn xsd_long() -> Sid {
        Sid::new(2, "long")
    }

    fn xsd_string() -> Sid {
        Sid::new(2, "string")
    }

    fn make_test_batch() -> Batch {
        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1)].into_boxed_slice());
        let columns = vec![
            vec![
                Binding::lit(FlakeValue::Long(10), xsd_long()),
                Binding::lit(FlakeValue::Long(20), xsd_long()),
                Binding::lit(FlakeValue::Long(30), xsd_long()),
            ],
            vec![
                Binding::lit(FlakeValue::String("a".into()), xsd_string()),
                Binding::lit(FlakeValue::String("b".into()), xsd_string()),
                Binding::lit(FlakeValue::String("c".into()), xsd_string()),
            ],
        ];
        Batch::new(schema, columns).unwrap()
    }

    // Note: We can't easily run async tests without a real ExecutionContext,
    // but we can test the construction and state management.

    #[test]
    fn test_seed_operator_from_batch_row() {
        let batch = make_test_batch();
        let seed = SeedOperator::from_batch_row(&batch, 1);

        assert_eq!(seed.schema().len(), 2);
        assert_eq!(seed.schema()[0], VarId(0));
        assert_eq!(seed.schema()[1], VarId(1));

        // Row 1 should have value 20 and "b"
        let (val, _) = seed.row[0].as_lit().unwrap();
        assert_eq!(*val, FlakeValue::Long(20));

        let (val, _) = seed.row[1].as_lit().unwrap();
        assert_eq!(*val, FlakeValue::String("b".into()));
    }

    #[test]
    fn test_seed_operator_from_row() {
        let schema: Arc<[VarId]> = Arc::from(vec![VarId(5)].into_boxed_slice());
        let row = vec![Binding::lit(FlakeValue::Long(42), xsd_long())];

        let seed = SeedOperator::from_row(schema.clone(), row);

        assert_eq!(seed.schema().len(), 1);
        assert_eq!(seed.schema()[0], VarId(5));
    }

    #[test]
    fn test_empty_operator_schema() {
        let empty = EmptyOperator::new();
        assert_eq!(empty.schema().len(), 0);
    }

    #[test]
    fn test_empty_operator_estimated_rows() {
        let empty = EmptyOperator::new();
        assert_eq!(empty.estimated_rows(), Some(1));
    }

    #[test]
    fn test_seed_operator_estimated_rows() {
        let batch = make_test_batch();
        let seed = SeedOperator::from_batch_row(&batch, 0);
        assert_eq!(seed.estimated_rows(), Some(1));
    }
}
