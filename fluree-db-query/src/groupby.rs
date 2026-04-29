//! GROUP BY operator - partitions solutions by group key
//!
//! Implements SPARQL GROUP BY semantics:
//! - Partitions solutions by group key variables
//! - Non-grouped variables become `Grouped(Vec<Binding>)` containing all values within the group
//! - Grouped values are consumed by aggregate functions
//!
//! # Example
//!
//! ```text
//! Input (from WHERE):
//!   ?person  ?age  ?city
//!   alice    30    NYC
//!   bob      25    NYC
//!   carol    35    LA
//!
//! GROUP BY ?city:
//!   ?city  ?person             ?age
//!   NYC    Grouped([alice,bob]) Grouped([30,25])
//!   LA     Grouped([carol])     Grouped([35])
//! ```
//!
//! This is a **blocking** operator: it must consume all input before producing output.

use crate::binding::{Batch, Binding};
use crate::context::ExecutionContext;
use crate::error::Result;
use crate::operator::{
    compute_trimmed_vars, effective_schema, trim_batch, BoxedOperator, Operator, OperatorState,
};
use crate::var_registry::VarId;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tracing::Instrument;

/// Type alias for a group entry: (group_key, rows_in_group)
type GroupEntry = (Vec<Binding>, Vec<Vec<Binding>>);

/// GROUP BY operator - partitions solutions by group key variables.
///
/// For each unique combination of group key values, collects all rows into a single
/// output row where:
/// - Group key variables retain their single value
/// - Non-grouped variables become `Grouped(Vec<Binding>)` containing all values
pub struct GroupByOperator {
    /// Child operator providing input solutions
    child: BoxedOperator,
    /// Output schema (same as input schema)
    in_schema: Arc<[VarId]>,
    /// Operator state
    state: OperatorState,
    /// Accumulated groups: group_key -> list of complete rows
    groups: HashMap<Vec<Binding>, Vec<Vec<Binding>>>,
    /// Iterator for emitting grouped results
    emit_iter: Option<std::vec::IntoIter<GroupEntry>>,
    /// Indices of group key columns in the schema
    group_key_indices: Vec<usize>,
    /// Variables required by downstream operators; if set, output is trimmed.
    out_schema: Option<Arc<[VarId]>>,
}

impl GroupByOperator {
    /// Create a new GROUP BY operator.
    ///
    /// # Arguments
    ///
    /// * `child` - Input solutions operator
    /// * `group_vars` - Variables to group by (if empty, all rows become one group)
    ///
    /// # Panics
    ///
    /// Panics if any group variable is not in the child schema.
    pub fn new(child: BoxedOperator, group_vars: Vec<VarId>) -> Self {
        let schema: Arc<[VarId]> = Arc::from(child.schema().to_vec().into_boxed_slice());

        // Compute indices for group key columns
        let group_key_indices: Vec<usize> = group_vars
            .iter()
            .map(|v| {
                schema
                    .iter()
                    .position(|sv| sv == v)
                    .unwrap_or_else(|| panic!("GROUP BY variable {v:?} not in schema"))
            })
            .collect();

        Self {
            child,
            in_schema: schema,
            state: OperatorState::Created,
            groups: HashMap::new(),
            emit_iter: None,
            group_key_indices,
            out_schema: None,
        }
    }

    /// Trim output to only the specified downstream variables.
    pub fn with_out_schema(mut self, downstream_vars: Option<&[VarId]>) -> Self {
        self.out_schema = compute_trimmed_vars(&self.in_schema, downstream_vars);
        self
    }

    /// Extract group key from a row
    fn extract_group_key(&self, row: &[Binding]) -> Vec<Binding> {
        self.group_key_indices
            .iter()
            .map(|&idx| row[idx].clone())
            .collect()
    }

    /// Transform accumulated groups into output rows.
    ///
    /// For each group:
    /// - Group key columns keep their single value
    /// - Non-grouped columns become Grouped(Vec<Binding>)
    fn transform_groups(&self) -> Vec<(Vec<Binding>, Vec<Vec<Binding>>)> {
        self.groups
            .iter()
            .map(|(key, rows)| (key.clone(), rows.clone()))
            .collect()
    }

    /// Build an output row from a group
    fn build_output_row(
        schema_len: usize,
        group_key_indices: &[usize],
        group_key: &[Binding],
        group_rows: &[Vec<Binding>],
    ) -> Vec<Binding> {
        let mut output = Vec::with_capacity(schema_len);

        for col_idx in 0..schema_len {
            if group_key_indices.contains(&col_idx) {
                // Group key column - find the value from the group key
                let key_pos = group_key_indices
                    .iter()
                    .position(|&idx| idx == col_idx)
                    .unwrap();
                output.push(group_key[key_pos].clone());
            } else {
                // Non-grouped column - collect all values into Grouped
                let values: Vec<Binding> =
                    group_rows.iter().map(|row| row[col_idx].clone()).collect();
                output.push(Binding::Grouped(values));
            }
        }

        output
    }
}

#[async_trait]
impl Operator for GroupByOperator {
    fn schema(&self) -> &[VarId] {
        effective_schema(&self.out_schema, &self.in_schema)
    }

    async fn open(&mut self, ctx: &ExecutionContext<'_>) -> Result<()> {
        self.child.open(ctx).await?;
        self.state = OperatorState::Open;
        self.groups.clear();
        self.emit_iter = None;
        Ok(())
    }

    async fn next_batch(&mut self, ctx: &ExecutionContext<'_>) -> Result<Option<Batch>> {
        if self.state != OperatorState::Open {
            return Ok(None);
        }

        // If we haven't consumed all input yet, do so now
        if self.emit_iter.is_none() {
            let span = tracing::debug_span!(
                "groupby_blocking",
                group_key_cols = self.group_key_indices.len(),
                schema_cols = self.in_schema.len(),
                input_batches = tracing::field::Empty,
                input_rows = tracing::field::Empty,
                groups = tracing::field::Empty,
                drain_ms = tracing::field::Empty,
                child_next_ms = tracing::field::Empty,
                process_rows_ms = tracing::field::Empty
            );
            // Use an async block with .instrument() so the span is NOT held
            // across .await via a thread-local guard (which would cause cross-request
            // trace contamination in tokio's multi-threaded runtime).
            async {
                let span = tracing::Span::current();
                let drain_start = Instant::now();
                let mut input_batches: u64 = 0;
                let mut input_rows: u64 = 0;
                let mut child_next_ms: u64 = 0;
                let mut process_rows_ms: u64 = 0;

                // Drain all input from child
                loop {
                    let next_start = Instant::now();
                    let next = self
                        .child
                        .next_batch(ctx)
                        .instrument(tracing::trace_span!("groupby_child_next_batch"))
                        .await?;
                    child_next_ms += (next_start.elapsed().as_secs_f64() * 1000.0) as u64;

                    let Some(batch) = next else {
                        break;
                    };
                    input_batches += 1;
                    if batch.is_empty() {
                        continue;
                    }

                    // Process each row
                    let proc_span = tracing::trace_span!(
                        "groupby_process_batch",
                        rows = batch.len(),
                        schema_cols = self.in_schema.len()
                    );
                    let proc_start = Instant::now();
                    let _pg = proc_span.enter();
                    for row_idx in 0..batch.len() {
                        input_rows += 1;
                        let row: Vec<Binding> = (0..self.in_schema.len())
                            .map(|col| batch.get_by_col(row_idx, col).clone())
                            .collect();

                        let group_key = self.extract_group_key(&row);
                        self.groups.entry(group_key).or_default().push(row);
                    }
                    process_rows_ms += (proc_start.elapsed().as_secs_f64() * 1000.0) as u64;
                }

                span.record("input_batches", input_batches);
                span.record("input_rows", input_rows);
                span.record("groups", self.groups.len() as u64);
                span.record("child_next_ms", child_next_ms);
                span.record("process_rows_ms", process_rows_ms);
                span.record(
                    "drain_ms",
                    (drain_start.elapsed().as_secs_f64() * 1000.0) as u64,
                );

                Ok::<_, crate::error::QueryError>(())
            }
            .instrument(span)
            .await?;

            // Transform groups into output format
            let groups_vec = self.transform_groups();
            self.emit_iter = Some(groups_vec.into_iter());
        }

        // Emit batches from the accumulated groups
        let batch_size = ctx.batch_size;
        let schema_len = self.in_schema.len();
        let group_key_indices = self.group_key_indices.clone();
        let mut output_columns: Vec<Vec<Binding>> = (0..schema_len)
            .map(|_| Vec::with_capacity(batch_size))
            .collect();
        let mut rows_added = 0;

        if let Some(ref mut iter) = self.emit_iter {
            while rows_added < batch_size {
                match iter.next() {
                    Some((group_key, group_rows)) => {
                        let output_row = Self::build_output_row(
                            schema_len,
                            &group_key_indices,
                            &group_key,
                            &group_rows,
                        );
                        for (col, val) in output_row.into_iter().enumerate() {
                            output_columns[col].push(val);
                        }
                        rows_added += 1;
                    }
                    None => {
                        self.state = OperatorState::Exhausted;
                        break;
                    }
                }
            }
        }

        if rows_added == 0 {
            return Ok(None);
        }

        let batch = Batch::new(self.in_schema.clone(), output_columns)?;
        Ok(trim_batch(&self.out_schema, batch))
    }

    fn close(&mut self) {
        self.child.close();
        self.groups.clear();
        self.emit_iter = None;
        self.state = OperatorState::Closed;
    }

    fn estimated_rows(&self) -> Option<usize> {
        // Worst case: every input row is its own group
        // Best case (no group vars): 1 row
        // We don't know without running, so return None
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::seed::SeedOperator;
    use fluree_db_core::{FlakeValue, LedgerSnapshot, Sid};

    fn xsd_long() -> Sid {
        Sid::new(2, "long")
    }

    fn xsd_string() -> Sid {
        Sid::new(2, "string")
    }

    fn make_test_snapshot() -> LedgerSnapshot {
        LedgerSnapshot::genesis("test/main")
    }

    #[test]
    fn test_group_by_schema() {
        // Create a seed with schema [?city, ?person, ?age]
        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1), VarId(2)].into_boxed_slice());
        let columns = vec![
            vec![Binding::lit(FlakeValue::String("NYC".into()), xsd_string())],
            vec![Binding::sid(Sid::new(100, "alice"))],
            vec![Binding::lit(FlakeValue::Long(30), xsd_long())],
        ];
        let batch = Batch::new(schema.clone(), columns).unwrap();
        let seed: BoxedOperator = Box::new(SeedOperator::from_batch_row(&batch, 0));

        // GROUP BY ?city
        let op = GroupByOperator::new(seed, vec![VarId(0)]);

        // Schema should remain the same
        assert_eq!(op.schema().len(), 3);
        assert_eq!(op.schema()[0], VarId(0));
        assert_eq!(op.schema()[1], VarId(1));
        assert_eq!(op.schema()[2], VarId(2));
    }

    #[test]
    fn test_group_key_indices() {
        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1), VarId(2)].into_boxed_slice());
        let columns = vec![
            vec![Binding::Unbound],
            vec![Binding::Unbound],
            vec![Binding::Unbound],
        ];
        let batch = Batch::new(schema.clone(), columns).unwrap();
        let seed: BoxedOperator = Box::new(SeedOperator::from_batch_row(&batch, 0));

        // GROUP BY ?city (?city is VarId(0), column 0)
        let op = GroupByOperator::new(seed, vec![VarId(0)]);

        // Check that group_key_indices is computed correctly
        assert!(op.group_key_indices.contains(&0));
        assert!(!op.group_key_indices.contains(&1));
        assert!(!op.group_key_indices.contains(&2));
    }

    #[test]
    fn test_extract_group_key() {
        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1), VarId(2)].into_boxed_slice());
        let columns = vec![
            vec![Binding::lit(FlakeValue::String("NYC".into()), xsd_string())],
            vec![Binding::sid(Sid::new(100, "alice"))],
            vec![Binding::lit(FlakeValue::Long(30), xsd_long())],
        ];
        let batch = Batch::new(schema.clone(), columns).unwrap();
        let seed: BoxedOperator = Box::new(SeedOperator::from_batch_row(&batch, 0));

        let op = GroupByOperator::new(seed, vec![VarId(0)]);

        let row = vec![
            Binding::lit(FlakeValue::String("NYC".into()), xsd_string()),
            Binding::sid(Sid::new(100, "alice")),
            Binding::lit(FlakeValue::Long(30), xsd_long()),
        ];

        let key = op.extract_group_key(&row);
        assert_eq!(key.len(), 1);
        assert_eq!(
            key[0],
            Binding::lit(FlakeValue::String("NYC".into()), xsd_string())
        );
    }

    #[tokio::test]
    async fn test_group_by_single_group() {
        use crate::context::ExecutionContext;
        use crate::var_registry::VarRegistry;

        let snapshot = make_test_snapshot();
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);

        // Create input with 3 rows, all same city
        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1), VarId(2)].into_boxed_slice());
        let columns = vec![
            vec![
                Binding::lit(FlakeValue::String("NYC".into()), xsd_string()),
                Binding::lit(FlakeValue::String("NYC".into()), xsd_string()),
                Binding::lit(FlakeValue::String("NYC".into()), xsd_string()),
            ],
            vec![
                Binding::sid(Sid::new(100, "alice")),
                Binding::sid(Sid::new(100, "bob")),
                Binding::sid(Sid::new(100, "carol")),
            ],
            vec![
                Binding::lit(FlakeValue::Long(30), xsd_long()),
                Binding::lit(FlakeValue::Long(25), xsd_long()),
                Binding::lit(FlakeValue::Long(35), xsd_long()),
            ],
        ];
        let batch = Batch::new(schema.clone(), columns).unwrap();

        // Use a custom operator that yields this batch
        struct BatchOperator {
            schema: Arc<[VarId]>,
            batch: Option<Batch>,
        }
        #[async_trait]
        impl Operator for BatchOperator {
            fn schema(&self) -> &[VarId] {
                &self.schema
            }
            async fn open(&mut self, _: &ExecutionContext<'_>) -> Result<()> {
                Ok(())
            }
            async fn next_batch(&mut self, _: &ExecutionContext<'_>) -> Result<Option<Batch>> {
                Ok(self.batch.take())
            }
            fn close(&mut self) {}
        }

        let child: BoxedOperator = Box::new(BatchOperator {
            schema: schema.clone(),
            batch: Some(batch),
        });

        // GROUP BY ?city
        let mut op = GroupByOperator::new(child, vec![VarId(0)]);
        op.open(&ctx).await.unwrap();

        let result = op.next_batch(&ctx).await.unwrap();
        assert!(result.is_some());

        let result = result.unwrap();
        // Should have 1 group (all NYC)
        assert_eq!(result.len(), 1);

        // City column should be a single value
        let city = result.get_by_col(0, 0);
        assert!(!city.is_grouped());
        assert_eq!(
            *city,
            Binding::lit(FlakeValue::String("NYC".into()), xsd_string())
        );

        // Person and age columns should be Grouped
        let person = result.get_by_col(0, 1);
        assert!(person.is_grouped());
        let persons = person.as_grouped().unwrap();
        assert_eq!(persons.len(), 3);

        let age = result.get_by_col(0, 2);
        assert!(age.is_grouped());
        let ages = age.as_grouped().unwrap();
        assert_eq!(ages.len(), 3);

        op.close();
    }

    #[tokio::test]
    async fn test_group_by_multiple_groups() {
        use crate::context::ExecutionContext;
        use crate::var_registry::VarRegistry;

        let snapshot = make_test_snapshot();
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);

        // Create input with 4 rows, 2 cities
        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1), VarId(2)].into_boxed_slice());
        let columns = vec![
            vec![
                Binding::lit(FlakeValue::String("NYC".into()), xsd_string()),
                Binding::lit(FlakeValue::String("LA".into()), xsd_string()),
                Binding::lit(FlakeValue::String("NYC".into()), xsd_string()),
                Binding::lit(FlakeValue::String("LA".into()), xsd_string()),
            ],
            vec![
                Binding::sid(Sid::new(100, "alice")),
                Binding::sid(Sid::new(100, "bob")),
                Binding::sid(Sid::new(100, "carol")),
                Binding::sid(Sid::new(100, "dan")),
            ],
            vec![
                Binding::lit(FlakeValue::Long(30), xsd_long()),
                Binding::lit(FlakeValue::Long(25), xsd_long()),
                Binding::lit(FlakeValue::Long(35), xsd_long()),
                Binding::lit(FlakeValue::Long(40), xsd_long()),
            ],
        ];
        let batch = Batch::new(schema.clone(), columns).unwrap();

        struct BatchOperator {
            schema: Arc<[VarId]>,
            batch: Option<Batch>,
        }
        #[async_trait]
        impl Operator for BatchOperator {
            fn schema(&self) -> &[VarId] {
                &self.schema
            }
            async fn open(&mut self, _: &ExecutionContext<'_>) -> Result<()> {
                Ok(())
            }
            async fn next_batch(&mut self, _: &ExecutionContext<'_>) -> Result<Option<Batch>> {
                Ok(self.batch.take())
            }
            fn close(&mut self) {}
        }

        let child: BoxedOperator = Box::new(BatchOperator {
            schema: schema.clone(),
            batch: Some(batch),
        });

        let mut op = GroupByOperator::new(child, vec![VarId(0)]);
        op.open(&ctx).await.unwrap();

        // Collect all output
        let mut total_rows = 0;
        while let Some(batch) = op.next_batch(&ctx).await.unwrap() {
            total_rows += batch.len();
            for row_idx in 0..batch.len() {
                // City column should be a single value
                let city = batch.get_by_col(row_idx, 0);
                assert!(!city.is_grouped());

                // Person and age columns should be Grouped
                let person = batch.get_by_col(row_idx, 1);
                assert!(person.is_grouped());
                let persons = person.as_grouped().unwrap();
                assert_eq!(persons.len(), 2); // 2 persons per city

                let age = batch.get_by_col(row_idx, 2);
                assert!(age.is_grouped());
            }
        }

        // Should have 2 groups (NYC and LA)
        assert_eq!(total_rows, 2);

        op.close();
    }

    #[tokio::test]
    async fn test_group_by_empty_input() {
        use crate::context::ExecutionContext;
        use crate::var_registry::VarRegistry;

        let snapshot = make_test_snapshot();
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);

        // Create an operator that produces zero rows
        struct NoRowsOperator {
            schema: Arc<[VarId]>,
        }
        #[async_trait]
        impl Operator for NoRowsOperator {
            fn schema(&self) -> &[VarId] {
                &self.schema
            }
            async fn open(&mut self, _: &ExecutionContext<'_>) -> Result<()> {
                Ok(())
            }
            async fn next_batch(&mut self, _: &ExecutionContext<'_>) -> Result<Option<Batch>> {
                Ok(None) // Truly no rows
            }
            fn close(&mut self) {}
        }

        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
        let child: BoxedOperator = Box::new(NoRowsOperator {
            schema: schema.clone(),
        });

        // GROUP BY ?x
        let mut op = GroupByOperator::new(child, vec![VarId(0)]);
        op.open(&ctx).await.unwrap();

        let result = op.next_batch(&ctx).await.unwrap();
        // Empty input should produce no output
        assert!(result.is_none());

        op.close();
    }

    #[tokio::test]
    async fn test_group_by_no_group_vars() {
        use crate::context::ExecutionContext;
        use crate::var_registry::VarRegistry;

        let snapshot = make_test_snapshot();
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);

        // Create input with 3 rows
        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1)].into_boxed_slice());
        let columns = vec![
            vec![
                Binding::lit(FlakeValue::Long(1), xsd_long()),
                Binding::lit(FlakeValue::Long(2), xsd_long()),
                Binding::lit(FlakeValue::Long(3), xsd_long()),
            ],
            vec![
                Binding::lit(FlakeValue::Long(10), xsd_long()),
                Binding::lit(FlakeValue::Long(20), xsd_long()),
                Binding::lit(FlakeValue::Long(30), xsd_long()),
            ],
        ];
        let batch = Batch::new(schema.clone(), columns).unwrap();

        struct BatchOperator {
            schema: Arc<[VarId]>,
            batch: Option<Batch>,
        }
        #[async_trait]
        impl Operator for BatchOperator {
            fn schema(&self) -> &[VarId] {
                &self.schema
            }
            async fn open(&mut self, _: &ExecutionContext<'_>) -> Result<()> {
                Ok(())
            }
            async fn next_batch(&mut self, _: &ExecutionContext<'_>) -> Result<Option<Batch>> {
                Ok(self.batch.take())
            }
            fn close(&mut self) {}
        }

        let child: BoxedOperator = Box::new(BatchOperator {
            schema: schema.clone(),
            batch: Some(batch),
        });

        // GROUP BY (nothing) - all rows become one implicit group
        let mut op = GroupByOperator::new(child, vec![]);
        op.open(&ctx).await.unwrap();

        let result = op.next_batch(&ctx).await.unwrap();
        assert!(result.is_some());

        let result = result.unwrap();
        // Should have 1 row (the implicit single group)
        assert_eq!(result.len(), 1);

        // All columns should be Grouped (no group key)
        let col0 = result.get_by_col(0, 0);
        assert!(col0.is_grouped());
        let values0 = col0.as_grouped().unwrap();
        assert_eq!(values0.len(), 3);

        let col1 = result.get_by_col(0, 1);
        assert!(col1.is_grouped());
        let values1 = col1.as_grouped().unwrap();
        assert_eq!(values1.len(), 3);

        op.close();
    }

    #[test]
    #[should_panic(expected = "GROUP BY variable")]
    fn test_group_by_invalid_var() {
        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1)].into_boxed_slice());
        let columns = vec![vec![Binding::Unbound], vec![Binding::Unbound]];
        let batch = Batch::new(schema.clone(), columns).unwrap();
        let seed: BoxedOperator = Box::new(SeedOperator::from_batch_row(&batch, 0));

        // GROUP BY ?unknown (VarId(99) not in schema)
        let _op = GroupByOperator::new(seed, vec![VarId(99)]);
    }
}
