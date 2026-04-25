//! Correctness-focused integration tests for fluree-db-query.
//!
//! These tests are designed to validate end-to-end operator semantics without
//! requiring the on-disk `test-database` fixture.

use std::sync::Arc;

use fluree_db_core::{LedgerSnapshot, Sid};

use fluree_db_query::binary_scan::EmitMask;
use fluree_db_query::binding::{Batch, Binding};
use fluree_db_query::context::ExecutionContext;
use fluree_db_query::expression::PreparedBoolExpression;
use fluree_db_query::ir::{Expression, FilterValue};
use fluree_db_query::join::NestedLoopJoinOperator;
use fluree_db_query::operator::inline::InlineOperator;
use fluree_db_query::operator::Operator;
use fluree_db_query::optional::{OptionalBuilder, OptionalOperator};
use fluree_db_query::triple::{Ref, Term, TriplePattern};
use fluree_db_query::var_registry::{VarId, VarRegistry};

/// A simple operator that yields a single batch then exhausts.
struct SingleBatchOp {
    batch: Option<Batch>,
}

#[async_trait::async_trait]
impl Operator for SingleBatchOp {
    fn schema(&self) -> &[VarId] {
        self.batch
            .as_ref()
            .map(fluree_db_query::Batch::schema)
            .unwrap_or(&[])
    }

    async fn open(&mut self, _: &ExecutionContext<'_>) -> fluree_db_query::Result<()> {
        Ok(())
    }

    async fn next_batch(
        &mut self,
        _: &ExecutionContext<'_>,
    ) -> fluree_db_query::Result<Option<Batch>> {
        Ok(self.batch.take())
    }

    fn close(&mut self) {}
}

/// Builder that always yields no matches, forcing OPTIONAL to emit Poisoned for optional-only vars.
struct NoMatchOptionalBuilder {
    schema: Arc<[VarId]>,
    optional_only: Vec<VarId>,
}

impl NoMatchOptionalBuilder {
    fn new(optional_only: VarId) -> Self {
        Self {
            schema: Arc::from(vec![optional_only].into_boxed_slice()),
            optional_only: vec![optional_only],
        }
    }
}

impl OptionalBuilder for NoMatchOptionalBuilder {
    fn build(
        &self,
        _: &Batch,
        _: usize,
        _: &fluree_db_query::ExecutionContext<'_>,
    ) -> fluree_db_query::Result<Option<fluree_db_query::BoxedOperator>> {
        Ok(None)
    }

    fn schema(&self) -> &[VarId] {
        &self.schema
    }

    fn optional_only_vars(&self) -> &[VarId] {
        &self.optional_only
    }

    fn unify_instructions(&self) -> &[fluree_db_query::UnifyInstruction] {
        &[]
    }
}

/// Verifies Poisoned vars block subsequent pattern matching:
/// A left join emits Poisoned for an optional-only var, then a subsequent BindJoin
/// that needs that var for binding must produce no rows (and must not attempt a scan).
#[tokio::test]
async fn test_optional_poison_blocks_subsequent() {
    // Minimal db/context.
    let snapshot = LedgerSnapshot::genesis("test/main");
    let mut vars = VarRegistry::new();
    let s = vars.get_or_insert("?s");
    let opt = vars.get_or_insert("?opt");
    let o = vars.get_or_insert("?o");
    let ctx = ExecutionContext::new(&snapshot, &vars);

    // Required batch: one row with ?s bound.
    let required_schema: Arc<[VarId]> = Arc::from(vec![s].into_boxed_slice());
    let required_batch = Batch::new(
        required_schema.clone(),
        vec![vec![Binding::sid(Sid::new(100, "alice"))]],
    )
    .unwrap();

    // Left join with builder that forces no matches => emits Poisoned for ?opt.
    let required_op = SingleBatchOp {
        batch: Some(required_batch),
    };
    let builder: Box<dyn OptionalBuilder> = Box::new(NoMatchOptionalBuilder::new(opt));

    let mut left_join =
        OptionalOperator::with_builder(Box::new(required_op), required_schema.clone(), builder);
    left_join.open(&ctx).await.unwrap();
    let out_batch = left_join
        .next_batch(&ctx)
        .await
        .unwrap()
        .expect("Expected a left-join batch");
    left_join.close();

    // Ensure the optional-only column is Poisoned.
    assert_eq!(out_batch.schema(), &[s, opt]);
    assert!(out_batch.get_by_col(0, 1).is_poisoned());

    // Now feed that batch into a NestedLoopJoinOperator whose right pattern needs ?opt for binding.
    // If poison blocking works, the join will skip the row before executing any scan and return None.
    let left_op = SingleBatchOp {
        batch: Some(out_batch),
    };
    let left_schema: Arc<[VarId]> = Arc::from(vec![s, opt].into_boxed_slice());

    let right_pattern = TriplePattern::new(
        Ref::Var(opt), // correlation var (poisoned) used for binding
        Ref::Sid(Sid::new(100, "p")),
        Term::Var(o),
    );

    let mut join = NestedLoopJoinOperator::new(
        Box::new(left_op),
        left_schema,
        right_pattern,
        None, // No object bounds
        Vec::new(),
        EmitMask::ALL,
    );

    join.open(&ctx).await.unwrap();
    let join_out = join.next_batch(&ctx).await.unwrap();
    assert!(
        join_out.is_none(),
        "Expected no results when a poisoned var is required for binding"
    );
}

/// Regression guard: empty-schema scan must still apply inline filters.
#[tokio::test]
async fn test_range_scan_empty_schema_respects_inline_filter() {
    // Fully-bound triple patterns have empty output schema (existence semantics).
    // The critical behavior is that inline filters are still applied even when
    // there are no bindings (empty schema).
    let schema: Vec<VarId> = vec![];
    let mut bindings: Vec<Binding> = vec![];

    let inline_ops = vec![InlineOperator::Filter(PreparedBoolExpression::new(
        Expression::Const(FilterValue::Bool(false)),
    ))];
    let keep =
        fluree_db_query::operator::inline::apply_inline(&inline_ops, &schema, &mut bindings, None)
            .unwrap();

    assert!(
        !keep,
        "expected FILTER(false) to drop the match even for empty-schema rows"
    );
}
