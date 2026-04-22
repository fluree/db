//! Distinct operator for removing duplicate rows
//!
//! The `DistinctOperator` deduplicates rows based on all columns in the schema.
//! It uses a HashSet to track seen rows, processing batches incrementally.

use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::Result;
use crate::operator::{BoxedOperator, Operator, OperatorState};
use crate::var_registry::VarId;
use async_trait::async_trait;
use hashbrown::HashMap;
use rustc_hash::{FxBuildHasher, FxHasher};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// Row signature type for deduplication
/// `Binding` implements `Hash` and `Eq`, so `Vec<Binding>` can be used as a hash key.
type RowSignature = Vec<Binding>;

/// Distinct operator - removes duplicate rows
///
/// Uses a streaming approach: processes one batch at a time, maintaining a
/// HashSet of seen row signatures. Memory usage grows with the number of
/// unique rows seen.
///
/// Wraps a child operator and emits each unique row combination exactly once.
pub struct DistinctOperator {
    /// Child operator
    child: BoxedOperator,
    /// Set of seen row signatures
    seen: HashMap<RowSignature, (), FxBuildHasher>,
    /// Output schema (same as child)
    schema: Arc<[VarId]>,
    /// Operator state
    state: OperatorState,
}

impl DistinctOperator {
    /// Create a new distinct operator
    ///
    /// # Arguments
    ///
    /// * `child` - The child operator to deduplicate
    pub fn new(child: BoxedOperator) -> Self {
        let schema = Arc::from(child.schema().to_vec().into_boxed_slice());
        Self {
            child,
            seen: HashMap::with_hasher(FxBuildHasher),
            schema,
            state: OperatorState::Created,
        }
    }

    /// Get the number of unique rows seen so far
    pub fn unique_count(&self) -> usize {
        self.seen.len()
    }

    /// Extract row signature from batch at given row index.
    #[inline]
    fn extract_signature_with_len(batch: &Batch, row_idx: usize, cols: usize) -> RowSignature {
        let mut sig = Vec::with_capacity(cols);
        for col in 0..cols {
            sig.push(batch.get_by_col(row_idx, col).clone());
        }
        sig
    }

    #[inline]
    fn row_hash_with_len(batch: &Batch, row_idx: usize, cols: usize) -> u64 {
        let mut h = FxHasher::default();
        // Match `Vec<T>` / slice hashing which incorporates length.
        cols.hash(&mut h);
        for col in 0..cols {
            batch.get_by_col(row_idx, col).hash(&mut h);
        }
        h.finish()
    }
}

#[async_trait]
impl Operator for DistinctOperator {
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
        self.seen.clear();
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

            // Find rows that are not duplicates.
            //
            // IMPORTANT: avoid allocating/cloning a `Vec<Binding>` per row. We compute
            // the hash directly from the batch row, probe the set with a borrowed
            // equality check, and only allocate the full signature for rows that
            // are genuinely new.
            let num_cols = self.schema.len();
            let mut columns: Vec<Vec<Binding>> = (0..num_cols).map(|_| Vec::new()).collect();

            for row_idx in 0..batch.len() {
                let hash = Self::row_hash_with_len(&batch, row_idx, num_cols);
                let entry = self.seen.raw_entry_mut().from_hash(hash, |sig| {
                    if sig.len() != num_cols {
                        return false;
                    }
                    for col in 0..num_cols {
                        let Some(b) = sig.get(col) else {
                            return false;
                        };
                        if b != batch.get_by_col(row_idx, col) {
                            return false;
                        }
                    }
                    true
                });

                if let hashbrown::hash_map::RawEntryMut::Vacant(v) = entry {
                    let signature = Self::extract_signature_with_len(&batch, row_idx, num_cols);
                    v.insert_hashed_nocheck(hash, signature, ());

                    for (col_idx, col) in columns.iter_mut().enumerate() {
                        col.push(batch.get_by_col(row_idx, col_idx).clone());
                    }
                }
            }

            if columns.first().map(std::vec::Vec::is_empty).unwrap_or(true) {
                // All rows were duplicates, try next batch
                continue;
            }

            return Ok(Some(Batch::new(self.schema.clone(), columns)?));
        }
    }

    fn close(&mut self) {
        self.child.close();
        self.seen.clear(); // Release memory
        self.state = OperatorState::Closed;
    }

    fn estimated_rows(&self) -> Option<usize> {
        // Can't estimate deduplication impact without data statistics
        // Return child estimate as upper bound
        self.child.estimated_rows()
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

    fn make_batch_with_values(schema: Arc<[VarId]>, values: Vec<i64>) -> Batch {
        let columns = vec![values
            .into_iter()
            .map(|v| Binding::lit(FlakeValue::Long(v), Sid::new(1, "long")))
            .collect()];
        Batch::new(schema, columns).unwrap()
    }

    fn make_batch_2col(schema: Arc<[VarId]>, col1: Vec<i64>, col2: Vec<i64>) -> Batch {
        assert_eq!(col1.len(), col2.len());
        let columns = vec![
            col1.into_iter()
                .map(|v| Binding::lit(FlakeValue::Long(v), Sid::new(1, "long")))
                .collect(),
            col2.into_iter()
                .map(|v| Binding::lit(FlakeValue::Long(v), Sid::new(1, "long")))
                .collect(),
        ];
        Batch::new(schema, columns).unwrap()
    }

    #[tokio::test]
    async fn test_distinct_all_unique() {
        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);

        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let batch = make_batch_with_values(schema.clone(), vec![1, 2, 3, 4, 5]);
        let mock = MockOperator::new(vec![batch]);

        let mut distinct_op = DistinctOperator::new(Box::new(mock));
        distinct_op.open(&ctx).await.unwrap();

        let result = distinct_op.next_batch(&ctx).await.unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().len(), 5);
        assert_eq!(distinct_op.unique_count(), 5);

        // Should be exhausted
        let result2 = distinct_op.next_batch(&ctx).await.unwrap();
        assert!(result2.is_none());
    }

    #[tokio::test]
    async fn test_distinct_all_duplicates() {
        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);

        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let batch = make_batch_with_values(schema.clone(), vec![1, 1, 1, 1, 1]);
        let mock = MockOperator::new(vec![batch]);

        let mut distinct_op = DistinctOperator::new(Box::new(mock));
        distinct_op.open(&ctx).await.unwrap();

        let result = distinct_op.next_batch(&ctx).await.unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().len(), 1);
        assert_eq!(distinct_op.unique_count(), 1);
    }

    #[tokio::test]
    async fn test_distinct_mixed() {
        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);

        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        // 1, 2, 1, 3, 2, 1, 4 => unique: 1, 2, 3, 4
        let batch = make_batch_with_values(schema.clone(), vec![1, 2, 1, 3, 2, 1, 4]);
        let mock = MockOperator::new(vec![batch]);

        let mut distinct_op = DistinctOperator::new(Box::new(mock));
        distinct_op.open(&ctx).await.unwrap();

        let result = distinct_op.next_batch(&ctx).await.unwrap();
        assert!(result.is_some());
        let batch = result.unwrap();
        assert_eq!(batch.len(), 4);
        assert_eq!(distinct_op.unique_count(), 4);

        // Verify the values are 1, 2, 3, 4 (first occurrences)
        let vals: Vec<i64> = (0..batch.len())
            .filter_map(|i| {
                if let Binding::Lit {
                    val: FlakeValue::Long(v),
                    ..
                } = batch.get_by_col(i, 0)
                {
                    Some(*v)
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(vals, vec![1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn test_distinct_across_batches() {
        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);

        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let batch1 = make_batch_with_values(schema.clone(), vec![1, 2, 3]);
        let batch2 = make_batch_with_values(schema.clone(), vec![2, 3, 4]); // 2, 3 are dupes
        let batch3 = make_batch_with_values(schema.clone(), vec![1, 5, 1]); // 1 is dupe
        let mock = MockOperator::new(vec![batch1, batch2, batch3]);

        let mut distinct_op = DistinctOperator::new(Box::new(mock));
        distinct_op.open(&ctx).await.unwrap();

        // First batch: all unique (1, 2, 3)
        let result1 = distinct_op.next_batch(&ctx).await.unwrap();
        assert!(result1.is_some());
        assert_eq!(result1.unwrap().len(), 3);

        // Second batch: only 4 is new
        let result2 = distinct_op.next_batch(&ctx).await.unwrap();
        assert!(result2.is_some());
        assert_eq!(result2.unwrap().len(), 1);

        // Third batch: only 5 is new
        let result3 = distinct_op.next_batch(&ctx).await.unwrap();
        assert!(result3.is_some());
        assert_eq!(result3.unwrap().len(), 1);

        assert_eq!(distinct_op.unique_count(), 5);
    }

    #[tokio::test]
    async fn test_distinct_batch_all_dupes_skipped() {
        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);

        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let batch1 = make_batch_with_values(schema.clone(), vec![1, 2, 3]);
        let batch2 = make_batch_with_values(schema.clone(), vec![1, 2, 3]); // all dupes
        let batch3 = make_batch_with_values(schema.clone(), vec![4, 5]); // new values
        let mock = MockOperator::new(vec![batch1, batch2, batch3]);

        let mut distinct_op = DistinctOperator::new(Box::new(mock));
        distinct_op.open(&ctx).await.unwrap();

        // First batch: all unique
        let result1 = distinct_op.next_batch(&ctx).await.unwrap();
        assert!(result1.is_some());
        assert_eq!(result1.unwrap().len(), 3);

        // Second batch is all dupes, should skip to third
        let result2 = distinct_op.next_batch(&ctx).await.unwrap();
        assert!(result2.is_some());
        assert_eq!(result2.unwrap().len(), 2); // batch3's rows

        assert_eq!(distinct_op.unique_count(), 5);
    }

    #[tokio::test]
    async fn test_distinct_multi_column() {
        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);

        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1)].into_boxed_slice());
        // (1, 1), (1, 2), (1, 1) - third is dupe of first
        let batch = make_batch_2col(schema.clone(), vec![1, 1, 1], vec![1, 2, 1]);
        let mock = MockOperator::new(vec![batch]);

        let mut distinct_op = DistinctOperator::new(Box::new(mock));
        distinct_op.open(&ctx).await.unwrap();

        let result = distinct_op.next_batch(&ctx).await.unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().len(), 2); // (1,1) and (1,2)
        assert_eq!(distinct_op.unique_count(), 2);
    }

    #[tokio::test]
    async fn test_distinct_with_unbound() {
        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);

        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let columns = vec![vec![
            Binding::lit(FlakeValue::Long(1), Sid::new(1, "long")),
            Binding::Unbound,
            Binding::lit(FlakeValue::Long(1), Sid::new(1, "long")), // dupe of first
            Binding::Unbound,                                       // dupe of second Unbound
        ]];
        let batch = Batch::new(schema.clone(), columns).unwrap();
        let mock = MockOperator::new(vec![batch]);

        let mut distinct_op = DistinctOperator::new(Box::new(mock));
        distinct_op.open(&ctx).await.unwrap();

        let result = distinct_op.next_batch(&ctx).await.unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().len(), 2); // Long(1) and Unbound
        assert_eq!(distinct_op.unique_count(), 2);
    }

    #[tokio::test]
    async fn test_distinct_with_poisoned() {
        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);

        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let columns = vec![vec![
            Binding::lit(FlakeValue::Long(1), Sid::new(1, "long")),
            Binding::Poisoned,
            Binding::Poisoned, // dupe of first Poisoned
            Binding::lit(FlakeValue::Long(1), Sid::new(1, "long")), // dupe of first
        ]];
        let batch = Batch::new(schema.clone(), columns).unwrap();
        let mock = MockOperator::new(vec![batch]);

        let mut distinct_op = DistinctOperator::new(Box::new(mock));
        distinct_op.open(&ctx).await.unwrap();

        let result = distinct_op.next_batch(&ctx).await.unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().len(), 2); // Long(1) and Poisoned
        assert_eq!(distinct_op.unique_count(), 2);
    }

    #[tokio::test]
    async fn test_distinct_preserves_schema() {
        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);

        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1), VarId(2)].into_boxed_slice());
        let columns: Vec<Vec<Binding>> = (0..3)
            .map(|col| {
                (0..5)
                    .map(|row| {
                        Binding::lit(
                            FlakeValue::Long((col * 10 + row) as i64),
                            Sid::new(1, "long"),
                        )
                    })
                    .collect()
            })
            .collect();
        let batch = Batch::new(schema.clone(), columns).unwrap();
        let mock = MockOperator::new(vec![batch]);

        let mut distinct_op = DistinctOperator::new(Box::new(mock));
        assert_eq!(distinct_op.schema(), &[VarId(0), VarId(1), VarId(2)]);

        distinct_op.open(&ctx).await.unwrap();

        let result = distinct_op.next_batch(&ctx).await.unwrap().unwrap();
        assert_eq!(result.schema(), &[VarId(0), VarId(1), VarId(2)]);
    }

    #[tokio::test]
    async fn test_distinct_state_transitions() {
        let snapshot = LedgerSnapshot::genesis("test/main");
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);

        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let batch = make_batch_with_values(schema.clone(), vec![1, 2, 3]);
        let mock = MockOperator::new(vec![batch]);

        let mut distinct_op = DistinctOperator::new(Box::new(mock));

        // Can't call next_batch before open
        let err = distinct_op.next_batch(&ctx).await;
        assert!(matches!(err, Err(QueryError::OperatorNotOpened)));

        // Open successfully
        distinct_op.open(&ctx).await.unwrap();

        // Can't open twice
        let err = distinct_op.open(&ctx).await;
        assert!(matches!(err, Err(QueryError::OperatorAlreadyOpened)));

        // Get result
        let _ = distinct_op.next_batch(&ctx).await.unwrap();

        // Close
        distinct_op.close();
        assert_eq!(distinct_op.unique_count(), 0); // Cleared on close

        // Can't open after close
        let err = distinct_op.open(&ctx).await;
        assert!(matches!(err, Err(QueryError::OperatorClosed)));
    }
}
