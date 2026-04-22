//! Limit operator for query result pagination
//!
//! The `LimitOperator` stops producing rows after a specified number have been emitted.
//! It tracks rows across batches and truncates the final batch if needed.

use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::Result;
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::var_registry::VarId;
use async_trait::async_trait;
use std::sync::Arc;

/// Limit operator - stops after emitting N rows
///
/// Wraps a child operator and emits at most N rows total, even if the child produces more.
pub struct LimitOperator {
    /// Child operator
    child: BoxedOperator,
    /// Maximum rows to emit
    limit: usize,
    /// Rows emitted so far
    emitted: usize,
    /// Output schema (same as child)
    schema: Arc<[VarId]>,
    /// Operator state
    state: OperatorState,
}

impl LimitOperator {
    /// Create a new limit operator
    ///
    /// # Arguments
    ///
    /// * `child` - The child operator to limit
    /// * `limit` - Maximum number of rows to emit
    pub fn new(child: BoxedOperator, limit: usize) -> Self {
        let schema = Arc::from(child.schema().to_vec().into_boxed_slice());
        Self {
            child,
            limit,
            emitted: 0,
            schema,
            state: OperatorState::Created,
        }
    }

    /// Get the limit value
    pub fn limit(&self) -> usize {
        self.limit
    }

    /// Get the number of rows emitted so far
    pub fn emitted(&self) -> usize {
        self.emitted
    }
}

#[async_trait]
impl Operator for LimitOperator {
    fn schema(&self) -> &[VarId] {
        &self.schema
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        if !self.state.can_open() {
            if self.state.is_closed() {
                return Err(crate::error::QueryError::OperatorClosed);
            }
            return Err(crate::error::QueryError::OperatorAlreadyOpened);
        }

        self.child.open(ctx).await?;
        self.emitted = 0;
        self.state = OperatorState::Open;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if !self.state.can_next() {
            if self.state == OperatorState::Created {
                return Err(crate::error::QueryError::OperatorNotOpened);
            }
            return Ok(None);
        }

        // Already hit limit
        if self.emitted >= self.limit {
            return Ok(None);
        }

        // Get next batch from child
        let batch = match self.child.next_batch(ctx).await? {
            Some(b) => b,
            None => {
                self.state = OperatorState::Exhausted;
                return Ok(None);
            }
        };

        let remaining = self.limit - self.emitted;

        if batch.len() <= remaining {
            // Can emit entire batch
            self.emitted += batch.len();
            if self.emitted >= self.limit {
                self.state = OperatorState::Exhausted;
            }
            Ok(Some(batch))
        } else {
            // Need to truncate batch
            self.emitted = self.limit;
            self.state = OperatorState::Exhausted;

            // Build truncated batch
            let num_cols = self.schema.len();
            let mut columns: Vec<Vec<Binding>> = Vec::with_capacity(num_cols);

            for col_idx in 0..num_cols {
                let col = batch
                    .column_by_idx(col_idx)
                    .expect("column index should be valid");
                columns.push(col.iter().take(remaining).cloned().collect());
            }

            Ok(Some(Batch::new(self.schema.clone(), columns)?))
        }
    }

    fn close(&mut self) {
        self.child.close();
        self.state = OperatorState::Closed;
    }

    fn estimated_rows(&self) -> Option<usize> {
        self.child.estimated_rows().map(|r| r.min(self.limit))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::QueryError;
    use crate::var_registry::VarRegistry;
    use fluree_db_core::{FlakeValue, LedgerSnapshot, Sid};

    /// Mock operator that emits predefined batches
    struct MockOperator {
        batches: Vec<Batch>,
        idx: usize,
        schema: Arc<[VarId]>,
        state: OperatorState,
    }

    impl MockOperator {
        fn new(batches: Vec<Batch>) -> Self {
            let schema = batches
                .first()
                .map(|b| Arc::from(b.schema().to_vec().into_boxed_slice()))
                .unwrap_or_else(|| Arc::from(Vec::new().into_boxed_slice()));
            Self {
                batches,
                idx: 0,
                schema,
                state: OperatorState::Created,
            }
        }
    }

    #[async_trait]
    impl Operator for MockOperator {
        fn schema(&self) -> &[VarId] {
            &self.schema
        }

        async fn open(&mut self, _ctx: &ExecutionContext<'_>) -> Result<()> {
            self.idx = 0;
            self.state = OperatorState::Open;
            Ok(())
        }

        async fn next_batch(&mut self, _ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
            if self.state != OperatorState::Open {
                return Ok(None);
            }

            if self.idx < self.batches.len() {
                let batch = self.batches[self.idx].clone();
                self.idx += 1;
                Ok(Some(batch))
            } else {
                self.state = OperatorState::Exhausted;
                Ok(None)
            }
        }

        fn close(&mut self) {
            self.state = OperatorState::Closed;
        }

        fn estimated_rows(&self) -> Option<usize> {
            Some(
                self.batches
                    .iter()
                    .map(super::super::binding::Batch::len)
                    .sum(),
            )
        }
    }

    fn make_test_batch(schema: Arc<[VarId]>, num_rows: usize) -> Batch {
        let num_cols = schema.len();
        let columns: Vec<Vec<Binding>> = (0..num_cols)
            .map(|col| {
                (0..num_rows)
                    .map(|row| {
                        Binding::lit(
                            FlakeValue::Long((col * 100 + row) as i64),
                            Sid::new(1, "long"),
                        )
                    })
                    .collect()
            })
            .collect();
        Batch::new(schema, columns).unwrap()
    }

    #[tokio::test]
    async fn test_limit_exact_batch_size() {
        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);

        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let batch = make_test_batch(schema.clone(), 5);
        let mock = MockOperator::new(vec![batch]);

        let mut limit_op = LimitOperator::new(Box::new(mock), 5);
        limit_op.open(&ctx).await.unwrap();

        let result = limit_op.next_batch(&ctx).await.unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().len(), 5);

        // Should be exhausted now
        let result2 = limit_op.next_batch(&ctx).await.unwrap();
        assert!(result2.is_none());
    }

    #[tokio::test]
    async fn test_limit_smaller_than_batch() {
        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);

        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let batch = make_test_batch(schema.clone(), 10);
        let mock = MockOperator::new(vec![batch]);

        let mut limit_op = LimitOperator::new(Box::new(mock), 3);
        limit_op.open(&ctx).await.unwrap();

        let result = limit_op.next_batch(&ctx).await.unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().len(), 3);

        // Should be exhausted after truncation
        let result2 = limit_op.next_batch(&ctx).await.unwrap();
        assert!(result2.is_none());
    }

    #[tokio::test]
    async fn test_limit_larger_than_input() {
        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);

        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let batch = make_test_batch(schema.clone(), 5);
        let mock = MockOperator::new(vec![batch]);

        let mut limit_op = LimitOperator::new(Box::new(mock), 100);
        limit_op.open(&ctx).await.unwrap();

        let result = limit_op.next_batch(&ctx).await.unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().len(), 5);

        // Child exhausted
        let result2 = limit_op.next_batch(&ctx).await.unwrap();
        assert!(result2.is_none());
    }

    #[tokio::test]
    async fn test_limit_zero() {
        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);

        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let batch = make_test_batch(schema.clone(), 10);
        let mock = MockOperator::new(vec![batch]);

        let mut limit_op = LimitOperator::new(Box::new(mock), 0);
        limit_op.open(&ctx).await.unwrap();

        // Limit 0 should return nothing
        let result = limit_op.next_batch(&ctx).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_limit_spans_batches() {
        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);

        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let batch1 = make_test_batch(schema.clone(), 5);
        let batch2 = make_test_batch(schema.clone(), 5);
        let batch3 = make_test_batch(schema.clone(), 5);
        let mock = MockOperator::new(vec![batch1, batch2, batch3]);

        // Limit 7 should get all of batch1 (5) + partial batch2 (2)
        let mut limit_op = LimitOperator::new(Box::new(mock), 7);
        limit_op.open(&ctx).await.unwrap();

        let result1 = limit_op.next_batch(&ctx).await.unwrap();
        assert!(result1.is_some());
        assert_eq!(result1.unwrap().len(), 5);
        assert_eq!(limit_op.emitted(), 5);

        let result2 = limit_op.next_batch(&ctx).await.unwrap();
        assert!(result2.is_some());
        assert_eq!(result2.unwrap().len(), 2); // Truncated from 5 to 2
        assert_eq!(limit_op.emitted(), 7);

        // Should be exhausted
        let result3 = limit_op.next_batch(&ctx).await.unwrap();
        assert!(result3.is_none());
    }

    #[tokio::test]
    async fn test_limit_preserves_schema() {
        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);

        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1), VarId(2)].into_boxed_slice());
        let batch = make_test_batch(schema.clone(), 5);
        let mock = MockOperator::new(vec![batch]);

        let mut limit_op = LimitOperator::new(Box::new(mock), 3);
        assert_eq!(limit_op.schema(), &[VarId(0), VarId(1), VarId(2)]);

        limit_op.open(&ctx).await.unwrap();

        let result = limit_op.next_batch(&ctx).await.unwrap().unwrap();
        assert_eq!(result.schema(), &[VarId(0), VarId(1), VarId(2)]);
        assert_eq!(result.len(), 3);
    }

    #[tokio::test]
    async fn test_limit_estimated_rows() {
        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let batch = make_test_batch(schema.clone(), 100);
        let mock = MockOperator::new(vec![batch]);

        // Limit less than estimated
        let limit_op = LimitOperator::new(Box::new(mock), 10);
        assert_eq!(limit_op.estimated_rows(), Some(10));

        // Limit more than estimated
        let schema2: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let batch2 = make_test_batch(schema2.clone(), 5);
        let mock2 = MockOperator::new(vec![batch2]);
        let limit_op2 = LimitOperator::new(Box::new(mock2), 100);
        assert_eq!(limit_op2.estimated_rows(), Some(5));
    }

    #[tokio::test]
    async fn test_limit_state_transitions() {
        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);

        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let batch = make_test_batch(schema.clone(), 5);
        let mock = MockOperator::new(vec![batch]);

        let mut limit_op = LimitOperator::new(Box::new(mock), 3);

        // Can't call next_batch before open
        let err = limit_op.next_batch(&ctx).await;
        assert!(matches!(err, Err(QueryError::OperatorNotOpened)));

        // Open successfully
        limit_op.open(&ctx).await.unwrap();

        // Can't open twice
        let err = limit_op.open(&ctx).await;
        assert!(matches!(err, Err(QueryError::OperatorAlreadyOpened)));

        // Get result
        let _ = limit_op.next_batch(&ctx).await.unwrap();

        // Close
        limit_op.close();

        // Can't open after close
        let err = limit_op.open(&ctx).await;
        assert!(matches!(err, Err(QueryError::OperatorClosed)));
    }
}
