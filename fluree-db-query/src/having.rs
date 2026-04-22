//! HAVING operator - filters grouped/aggregated results
//!
//! Implements SPARQL HAVING semantics:
//! - Filters rows after GROUP BY and aggregation
//! - Uses the same filter expression evaluation as WHERE FILTER
//! - Operates on aggregate results (not Grouped values)
//! - Rows where the expression evaluates to `false` or encounters an error
//!   (type mismatch, unbound var) are filtered out (two-valued logic)
//!
//! # Example
//!
//! ```text
//! SELECT ?city (COUNT(?person) AS ?count)
//! WHERE { ?person :city ?city }
//! GROUP BY ?city
//! HAVING (COUNT(?person) > 10)
//! ```
//!
//! The HAVING clause filters out cities with 10 or fewer people.

use crate::binding::Batch;
use crate::context::ExecutionContext;
use crate::error::Result;
use crate::expression::PreparedBoolExpression;
use crate::filter::filter_batch;
use crate::ir::Expression;
use crate::operator::{
    compute_trimmed_vars, effective_schema, trim_batch, BoxedOperator, Operator, OperatorState,
};
use crate::var_registry::VarId;
use async_trait::async_trait;
use std::sync::Arc;

/// HAVING operator - filters rows after GROUP BY/aggregation
///
/// This is functionally identical to FilterOperator but conceptually
/// applies to grouped/aggregated results.
pub struct HavingOperator {
    /// Child operator (typically AggregateOperator or GroupByOperator)
    child: BoxedOperator,
    /// Prepared filter expression to evaluate
    prepared_expr: PreparedBoolExpression,
    /// Output schema (same as child)
    in_schema: Arc<[VarId]>,
    /// Operator state
    state: OperatorState,
    /// Variables required by downstream operators; if set, output is trimmed.
    out_schema: Option<Arc<[VarId]>>,
}

impl HavingOperator {
    /// Create a new HAVING operator
    ///
    /// # Arguments
    ///
    /// * `child` - Child operator (typically GroupByOperator or AggregateOperator)
    /// * `expr` - Filter expression to evaluate
    pub fn new(child: BoxedOperator, expr: Expression) -> Self {
        let schema: Arc<[VarId]> = Arc::from(child.schema().to_vec().into_boxed_slice());
        let prepared_expr = PreparedBoolExpression::new(expr);
        Self {
            child,
            prepared_expr,
            in_schema: schema,
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
impl Operator for HavingOperator {
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
            let batch = match self.child.next_batch(ctx).await? {
                Some(b) => b,
                None => {
                    self.state = OperatorState::Exhausted;
                    return Ok(None);
                }
            };

            if batch.is_empty() {
                continue;
            }

            if let Some(filtered) = filter_batch(&batch, &self.prepared_expr, &self.in_schema, ctx)?
            {
                return Ok(trim_batch(&self.out_schema, filtered));
            }
        }
    }

    fn close(&mut self) {
        self.child.close();
        self.state = OperatorState::Closed;
    }

    fn estimated_rows(&self) -> Option<usize> {
        // HAVING typically reduces row count, but we can't know by how much
        self.child.estimated_rows()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binding::Binding;
    use crate::seed::SeedOperator;
    use fluree_db_core::{FlakeValue, LedgerSnapshot, Sid};

    fn xsd_long() -> Sid {
        Sid::new(2, "long")
    }

    fn make_test_snapshot() -> LedgerSnapshot {
        LedgerSnapshot::genesis("test/main")
    }

    #[tokio::test]
    async fn test_having_filters_rows() {
        use crate::context::ExecutionContext;
        use crate::ir::FilterValue;
        use crate::var_registry::VarRegistry;

        let snapshot = make_test_snapshot();
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);

        // Create a batch with counts: 5, 15, 8, 20
        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1)].into_boxed_slice());
        let columns = vec![
            vec![
                Binding::lit(FlakeValue::String("NYC".into()), Sid::new(2, "string")),
                Binding::lit(FlakeValue::String("LA".into()), Sid::new(2, "string")),
                Binding::lit(FlakeValue::String("CHI".into()), Sid::new(2, "string")),
                Binding::lit(FlakeValue::String("SF".into()), Sid::new(2, "string")),
            ],
            vec![
                Binding::lit(FlakeValue::Long(5), xsd_long()),
                Binding::lit(FlakeValue::Long(15), xsd_long()),
                Binding::lit(FlakeValue::Long(8), xsd_long()),
                Binding::lit(FlakeValue::Long(20), xsd_long()),
            ],
        ];
        let batch = Batch::new(schema.clone(), columns).unwrap();

        // Create an operator that yields this batch
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

        // HAVING ?count > 10
        let expr = Expression::gt(
            Expression::Var(VarId(1)), // ?count
            Expression::Const(FilterValue::Long(10)),
        );

        let mut op = HavingOperator::new(child, expr);
        op.open(&ctx).await.unwrap();

        let result = op.next_batch(&ctx).await.unwrap();
        assert!(result.is_some());

        let result = result.unwrap();
        // Should only have 2 rows (LA=15, SF=20)
        assert_eq!(result.len(), 2);

        // Verify the cities
        let city0 = result.get_by_col(0, 0);
        let city1 = result.get_by_col(1, 0);
        if let Binding::Lit { val, .. } = city0 {
            assert_eq!(*val, FlakeValue::String("LA".into()));
        } else {
            panic!("Expected Lit binding");
        }
        if let Binding::Lit { val, .. } = city1 {
            assert_eq!(*val, FlakeValue::String("SF".into()));
        } else {
            panic!("Expected Lit binding");
        }

        op.close();
    }

    #[tokio::test]
    async fn test_having_no_matches() {
        use crate::context::ExecutionContext;
        use crate::ir::FilterValue;
        use crate::var_registry::VarRegistry;

        let snapshot = make_test_snapshot();
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);

        // Create a batch with all small counts
        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1)].into_boxed_slice());
        let columns = vec![
            vec![
                Binding::lit(FlakeValue::String("A".into()), Sid::new(2, "string")),
                Binding::lit(FlakeValue::String("B".into()), Sid::new(2, "string")),
            ],
            vec![
                Binding::lit(FlakeValue::Long(1), xsd_long()),
                Binding::lit(FlakeValue::Long(2), xsd_long()),
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

        // HAVING ?count > 100 (no rows match)
        let expr = Expression::gt(
            Expression::Var(VarId(1)),
            Expression::Const(FilterValue::Long(100)),
        );

        let mut op = HavingOperator::new(child, expr);
        op.open(&ctx).await.unwrap();

        let result = op.next_batch(&ctx).await.unwrap();
        // No rows should match
        assert!(result.is_none());

        op.close();
    }

    #[tokio::test]
    async fn test_having_schema_preserved() {
        use crate::context::ExecutionContext;
        use crate::ir::FilterValue;
        use crate::var_registry::VarRegistry;

        let snapshot = make_test_snapshot();
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);

        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1), VarId(2)].into_boxed_slice());
        let columns = vec![
            vec![Binding::lit(FlakeValue::Long(1), xsd_long())],
            vec![Binding::lit(FlakeValue::Long(2), xsd_long())],
            vec![Binding::lit(FlakeValue::Long(3), xsd_long())],
        ];
        let batch = Batch::new(schema.clone(), columns).unwrap();
        let seed: BoxedOperator = Box::new(SeedOperator::from_batch_row(&batch, 0));

        // Any expression that passes
        let expr = Expression::Const(FilterValue::Bool(true));

        let mut op = HavingOperator::new(seed, expr);
        op.open(&ctx).await.unwrap();

        // Schema should be preserved
        assert_eq!(op.schema().len(), 3);
        assert_eq!(op.schema()[0], VarId(0));
        assert_eq!(op.schema()[1], VarId(1));
        assert_eq!(op.schema()[2], VarId(2));

        op.close();
    }

    #[tokio::test]
    async fn test_having_type_mismatch_filters_out_row() {
        use crate::context::ExecutionContext;
        use crate::ir::FilterValue;
        use crate::var_registry::VarRegistry;

        let snapshot = make_test_snapshot();
        let vars = VarRegistry::new();
        let ctx = ExecutionContext::new(&snapshot, &vars);

        // Batch: city=NYC count=15, city=LA count="not_a_number"
        // HAVING ?count > 10 should keep NYC, filter out LA (type mismatch → filtered, not error)
        let schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1)].into_boxed_slice());
        let columns = vec![
            vec![
                Binding::lit(FlakeValue::String("NYC".into()), Sid::new(2, "string")),
                Binding::lit(FlakeValue::String("LA".into()), Sid::new(2, "string")),
            ],
            vec![
                Binding::lit(FlakeValue::Long(15), xsd_long()),
                Binding::lit(
                    FlakeValue::String("not_a_number".into()),
                    Sid::new(2, "string"),
                ),
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

        // HAVING ?count > 10
        let expr = Expression::gt(
            Expression::Var(VarId(1)),
            Expression::Const(FilterValue::Long(10)),
        );

        let mut op = HavingOperator::new(child, expr);
        op.open(&ctx).await.unwrap();

        // Should succeed (not error) and return only NYC
        let result = op.next_batch(&ctx).await.unwrap();
        assert!(result.is_some());

        let result = result.unwrap();
        assert_eq!(result.len(), 1);

        if let Binding::Lit { val, .. } = result.get_by_col(0, 0) {
            assert_eq!(*val, FlakeValue::String("NYC".into()));
        } else {
            panic!("Expected Lit binding");
        }

        op.close();
    }
}
