//! Offset operator for query result pagination
//!
//! The `OffsetOperator` skips the first N rows before producing output.
//! It tracks rows across batches and handles partial batch skipping.

use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::Result;
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::var_registry::VarId;
use async_trait::async_trait;
use std::sync::Arc;

/// Offset operator - skips the first N rows
///
/// Wraps a child operator and skips the first N rows before emitting the remainder.
pub struct OffsetOperator {
    /// Child operator
    child: BoxedOperator,
    /// Number of rows to skip
    offset: usize,
    /// Rows skipped so far
    skipped: usize,
    /// Output schema (same as child)
    schema: Arc<[VarId]>,
    /// Operator state
    state: OperatorState,
}

impl OffsetOperator {
    /// Create a new offset operator
    ///
    /// # Arguments
    ///
    /// * `child` - The child operator to offset
    /// * `offset` - Number of rows to skip
    pub fn new(child: BoxedOperator, offset: usize) -> Self {
        let schema = Arc::from(child.schema().to_vec().into_boxed_slice());
        Self {
            child,
            offset,
            skipped: 0,
            schema,
            state: OperatorState::Created,
        }
    }

    /// Get the offset value
    pub fn offset(&self) -> usize {
        self.offset
    }

    /// Get the number of rows skipped so far
    pub fn skipped(&self) -> usize {
        self.skipped
    }
}

#[async_trait]
impl Operator for OffsetOperator {
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
        self.skipped = 0;
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

        loop {
            // Get next batch from child
            let batch = match self.child.next_batch(ctx).await? {
                Some(b) => b,
                None => {
                    self.state = OperatorState::Exhausted;
                    return Ok(None);
                }
            };

            let remaining_to_skip = self.offset.saturating_sub(self.skipped);

            if remaining_to_skip == 0 {
                // Offset satisfied, pass through entire batch
                return Ok(Some(batch));
            }

            if batch.len() <= remaining_to_skip {
                // Skip entire batch
                self.skipped += batch.len();
                continue;
            }

            // Partial skip - emit remaining rows after offset
            self.skipped = self.offset;
            let start_idx = remaining_to_skip;

            let num_cols = self.schema.len();
            let mut columns: Vec<Vec<Binding>> = Vec::with_capacity(num_cols);

            for col_idx in 0..num_cols {
                let col = batch
                    .column_by_idx(col_idx)
                    .expect("column index should be valid");
                columns.push(col.iter().skip(start_idx).cloned().collect());
            }

            return Ok(Some(Batch::new(self.schema.clone(), columns)?));
        }
    }

    fn close(&mut self) {
        self.child.close();
        self.state = OperatorState::Closed;
    }

    fn estimated_rows(&self) -> Option<usize> {
        self.child
            .estimated_rows()
            .map(|r| r.saturating_sub(self.offset))
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

    fn make_test_batch(schema: Arc<[VarId]>, num_rows: usize, start_value: usize) -> Batch {
        let num_cols = schema.len();
        let columns: Vec<Vec<Binding>> = (0..num_cols)
            .map(|col| {
                (0..num_rows)
                    .map(|row| {
                        Binding::lit(
                            FlakeValue::Long((start_value + col * 100 + row) as i64),
                            Sid::new(1, "long"),
                        )
                    })
                    .collect()
            })
            .collect();
        Batch::new(schema, columns).unwrap()
    }

    #[tokio::test]
    async fn test_offset_within_first_batch() {
        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);

        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let batch = make_test_batch(schema.clone(), 10, 0);
        let mock = MockOperator::new(vec![batch]);

        // Offset 3 within a batch of 10 should give 7 rows
        let mut offset_op = OffsetOperator::new(Box::new(mock), 3);
        offset_op.open(&ctx).await.unwrap();

        let result = offset_op.next_batch(&ctx).await.unwrap();
        assert!(result.is_some());
        let batch = result.unwrap();
        assert_eq!(batch.len(), 7);

        // Check that values are offset correctly (should start at 3, not 0)
        let first_val = batch.get_by_col(0, 0);
        assert!(matches!(
            first_val,
            Binding::Lit {
                val: FlakeValue::Long(3),
                ..
            }
        ));

        // Should be exhausted
        let result2 = offset_op.next_batch(&ctx).await.unwrap();
        assert!(result2.is_none());
    }

    #[tokio::test]
    async fn test_offset_skips_entire_batch() {
        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);

        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let batch1 = make_test_batch(schema.clone(), 5, 0);
        let batch2 = make_test_batch(schema.clone(), 5, 100);
        let mock = MockOperator::new(vec![batch1, batch2]);

        // Offset 5 should skip entire first batch
        let mut offset_op = OffsetOperator::new(Box::new(mock), 5);
        offset_op.open(&ctx).await.unwrap();

        let result = offset_op.next_batch(&ctx).await.unwrap();
        assert!(result.is_some());
        let batch = result.unwrap();
        assert_eq!(batch.len(), 5);

        // Should be from second batch (values start at 100)
        let first_val = batch.get_by_col(0, 0);
        assert!(matches!(
            first_val,
            Binding::Lit {
                val: FlakeValue::Long(100),
                ..
            }
        ));
    }

    #[tokio::test]
    async fn test_offset_spans_batches() {
        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);

        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let batch1 = make_test_batch(schema.clone(), 5, 0);
        let batch2 = make_test_batch(schema.clone(), 5, 100);
        let batch3 = make_test_batch(schema.clone(), 5, 200);
        let mock = MockOperator::new(vec![batch1, batch2, batch3]);

        // Offset 7 should skip batch1 (5) + partial batch2 (2), leave 3 of batch2
        let mut offset_op = OffsetOperator::new(Box::new(mock), 7);
        offset_op.open(&ctx).await.unwrap();

        let result1 = offset_op.next_batch(&ctx).await.unwrap();
        assert!(result1.is_some());
        let batch = result1.unwrap();
        assert_eq!(batch.len(), 3); // 5 - 2 skipped = 3 remaining

        // Values should start at 102 (skipped 100, 101)
        let first_val = batch.get_by_col(0, 0);
        assert!(matches!(
            first_val,
            Binding::Lit {
                val: FlakeValue::Long(102),
                ..
            }
        ));

        // Next batch should be full batch3
        let result2 = offset_op.next_batch(&ctx).await.unwrap();
        assert!(result2.is_some());
        assert_eq!(result2.unwrap().len(), 5);

        // Should be exhausted
        let result3 = offset_op.next_batch(&ctx).await.unwrap();
        assert!(result3.is_none());
    }

    #[tokio::test]
    async fn test_offset_larger_than_input() {
        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);

        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let batch = make_test_batch(schema.clone(), 5, 0);
        let mock = MockOperator::new(vec![batch]);

        // Offset 100 is larger than input (5)
        let mut offset_op = OffsetOperator::new(Box::new(mock), 100);
        offset_op.open(&ctx).await.unwrap();

        let result = offset_op.next_batch(&ctx).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_offset_zero() {
        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);

        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let batch = make_test_batch(schema.clone(), 5, 0);
        let mock = MockOperator::new(vec![batch]);

        // Offset 0 should pass through all rows
        let mut offset_op = OffsetOperator::new(Box::new(mock), 0);
        offset_op.open(&ctx).await.unwrap();

        let result = offset_op.next_batch(&ctx).await.unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().len(), 5);
    }

    #[tokio::test]
    async fn test_offset_preserves_schema() {
        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);

        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1), VarId(2)].into_boxed_slice());
        let batch = make_test_batch(schema.clone(), 10, 0);
        let mock = MockOperator::new(vec![batch]);

        let mut offset_op = OffsetOperator::new(Box::new(mock), 3);
        assert_eq!(offset_op.schema(), &[VarId(0), VarId(1), VarId(2)]);

        offset_op.open(&ctx).await.unwrap();

        let result = offset_op.next_batch(&ctx).await.unwrap().unwrap();
        assert_eq!(result.schema(), &[VarId(0), VarId(1), VarId(2)]);
        assert_eq!(result.len(), 7);
    }

    #[tokio::test]
    async fn test_offset_estimated_rows() {
        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let batch = make_test_batch(schema.clone(), 100, 0);
        let mock = MockOperator::new(vec![batch]);

        // Offset less than total
        let offset_op = OffsetOperator::new(Box::new(mock), 30);
        assert_eq!(offset_op.estimated_rows(), Some(70));

        // Offset more than total
        let schema2: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let batch2 = make_test_batch(schema2.clone(), 5, 0);
        let mock2 = MockOperator::new(vec![batch2]);
        let offset_op2 = OffsetOperator::new(Box::new(mock2), 100);
        assert_eq!(offset_op2.estimated_rows(), Some(0));
    }

    #[tokio::test]
    async fn test_offset_state_transitions() {
        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);

        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let batch = make_test_batch(schema.clone(), 5, 0);
        let mock = MockOperator::new(vec![batch]);

        let mut offset_op = OffsetOperator::new(Box::new(mock), 2);

        // Can't call next_batch before open
        let err = offset_op.next_batch(&ctx).await;
        assert!(matches!(err, Err(QueryError::OperatorNotOpened)));

        // Open successfully
        offset_op.open(&ctx).await.unwrap();

        // Can't open twice
        let err = offset_op.open(&ctx).await;
        assert!(matches!(err, Err(QueryError::OperatorAlreadyOpened)));

        // Get result
        let _ = offset_op.next_batch(&ctx).await.unwrap();

        // Close
        offset_op.close();

        // Can't open after close
        let err = offset_op.open(&ctx).await;
        assert!(matches!(err, Err(QueryError::OperatorClosed)));
    }
}
