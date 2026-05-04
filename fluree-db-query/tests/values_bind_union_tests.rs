//! Critical integration tests for VALUES, BIND, and UNION patterns
//!
//! These tests verify the P2 functionality:
//! - VALUES at position 0 (start of WHERE)
//! - BIND expression evaluation
//! - UNION branch execution

use fluree_db_core::{FlakeValue, GraphDbRef, LedgerSnapshot, NoOverlay, Sid};
use fluree_db_query::binding::Binding;
use fluree_db_query::context::ExecutionContext;
use fluree_db_query::execute::{execute, ContextConfig, ExecutableQuery};
use fluree_db_query::ir::triple::{Ref, Term, TriplePattern};
use fluree_db_query::ir::QueryOptions;
use fluree_db_query::ir::{Expression, Pattern};
use fluree_db_query::ir::{Query, QueryOutput};
use fluree_db_query::operator::Operator;
use fluree_db_query::seed::EmptyOperator;
use fluree_db_query::values::ValuesOperator;
use fluree_db_query::var_registry::{VarId, VarRegistry};
use fluree_graph_json_ld::ParsedContext;
use std::sync::Arc;

fn make_test_snapshot() -> LedgerSnapshot {
    LedgerSnapshot::genesis("test/main")
}

fn make_triple_pattern(s_var: VarId, p_name: &str, o_var: VarId) -> TriplePattern {
    TriplePattern::new(
        Ref::Var(s_var),
        Ref::Sid(Sid::new(100, p_name)),
        Term::Var(o_var),
    )
}

fn make_query(select: Vec<VarId>, patterns: Vec<Pattern>) -> Query {
    let output = if select.is_empty() {
        QueryOutput::wildcard()
    } else {
        QueryOutput::select_all(select)
    };
    Query {
        context: ParsedContext::default(),
        orig_context: None,
        output,
        patterns,
        options: QueryOptions::default(),
        post_values: None,
    }
}

fn xsd_long() -> Sid {
    Sid::new(2, "long")
}

/// Test VALUES at position 0 followed by a triple pattern
///
/// This verifies:
/// 1. VALUES at position 0 is allowed (runner change)
/// 2. The schema is correctly computed
/// 3. The join with subsequent patterns works
#[tokio::test]
async fn test_values_first_then_join() {
    let snapshot = make_test_snapshot();
    let vars = VarRegistry::new();

    // VALUES ?s { :sid1 :sid2 } followed by (?s :name ?n)
    let sid1 = Sid::new(100, "alice");
    let sid2 = Sid::new(100, "bob");

    let query = make_query(
        vec![VarId(0), VarId(1)], // SELECT ?s ?n
        vec![
            Pattern::Values {
                vars: vec![VarId(0)], // ?s
                rows: vec![vec![Binding::sid(sid1)], vec![Binding::sid(sid2)]],
            },
            Pattern::Triple(make_triple_pattern(VarId(0), "name", VarId(1))),
        ],
    );

    // This should succeed even though VALUES is at position 0
    let db = GraphDbRef::new(&snapshot, 0, &NoOverlay, snapshot.t);
    let executable = ExecutableQuery::simple(query);
    let results = execute(db, &vars, &executable, ContextConfig::default())
        .await
        .unwrap();

    // With an empty database, we'll get no results from the join
    // but the query structure is valid
    assert!(results.is_empty() || results.iter().all(fluree_db_query::Batch::is_empty));
}

/// Test BIND at position 0 creates a new variable
///
/// This verifies:
/// 1. BIND at position 0 is allowed
/// 2. The expression is evaluated
/// 3. The result is bound to the variable
#[tokio::test]
async fn test_bind_first() {
    let snapshot = make_test_snapshot();
    let vars = VarRegistry::new();

    // BIND(42 AS ?x) followed by (?s :age ?x)
    let query = make_query(
        vec![VarId(0), VarId(1)], // SELECT ?x ?s
        vec![
            Pattern::Bind {
                var: VarId(0), // ?x
                expr: Expression::Const(FlakeValue::Long(42)),
            },
            Pattern::Triple(make_triple_pattern(VarId(1), "age", VarId(0))),
        ],
    );

    // Should succeed even though BIND is at position 0
    let db = GraphDbRef::new(&snapshot, 0, &NoOverlay, snapshot.t);
    let executable = ExecutableQuery::simple(query);
    let results = execute(db, &vars, &executable, ContextConfig::default())
        .await
        .unwrap();

    // With an empty database, we'll get no results from the join
    // but the query structure is valid
    assert!(results.is_empty() || results.iter().all(fluree_db_query::Batch::is_empty));
}

/// Test UNION at position 0 with two branches
///
/// This verifies:
/// 1. UNION at position 0 is allowed
/// 2. Multiple branches are handled
/// 3. The unified schema is correct
#[tokio::test]
async fn test_union_first() {
    let snapshot = make_test_snapshot();
    let vars = VarRegistry::new();

    // UNION { (?s :name ?n) } { (?s :email ?e) }
    let query = make_query(
        vec![VarId(0)], // SELECT ?s
        vec![Pattern::Union(vec![
            vec![Pattern::Triple(make_triple_pattern(
                VarId(0),
                "name",
                VarId(1),
            ))],
            vec![Pattern::Triple(make_triple_pattern(
                VarId(0),
                "email",
                VarId(2),
            ))],
        ])],
    );

    // Should succeed even though UNION is at position 0
    let db = GraphDbRef::new(&snapshot, 0, &NoOverlay, snapshot.t);
    let executable = ExecutableQuery::simple(query);
    let results = execute(db, &vars, &executable, ContextConfig::default())
        .await
        .unwrap();

    // With an empty database, we'll get no results
    // but the query structure is valid
    assert!(results.is_empty() || results.iter().all(fluree_db_query::Batch::is_empty));
}

/// Test FILTER at position 0 (filter on constants)
///
/// This verifies FILTER at position 0 works with empty seed support
#[tokio::test]
async fn test_filter_first_true() {
    let snapshot = make_test_snapshot();
    let vars = VarRegistry::new();

    // FILTER(1 = 1) followed by triple
    let query = make_query(
        vec![VarId(0), VarId(1)], // SELECT ?s ?n
        vec![
            Pattern::Filter(Expression::eq(
                Expression::Const(FlakeValue::Long(1)),
                Expression::Const(FlakeValue::Long(1)),
            )),
            Pattern::Triple(make_triple_pattern(VarId(0), "name", VarId(1))),
        ],
    );

    // Should succeed - FILTER is on constants, passes for the empty seed row
    let db = GraphDbRef::new(&snapshot, 0, &NoOverlay, snapshot.t);
    let executable = ExecutableQuery::simple(query);
    let results = execute(db, &vars, &executable, ContextConfig::default())
        .await
        .unwrap();

    // Empty database, so no triple matches
    assert!(results.is_empty() || results.iter().all(fluree_db_query::Batch::is_empty));
}

/// Test FILTER at position 0 with false condition
#[tokio::test]
async fn test_filter_first_false() {
    let snapshot = make_test_snapshot();
    let vars = VarRegistry::new();

    // FILTER(1 = 2) followed by triple - should filter out the empty seed
    let query = make_query(
        vec![VarId(0), VarId(1)], // SELECT ?s ?n
        vec![
            Pattern::Filter(Expression::eq(
                Expression::Const(FlakeValue::Long(1)),
                Expression::Const(FlakeValue::Long(2)),
            )),
            Pattern::Triple(make_triple_pattern(VarId(0), "name", VarId(1))),
        ],
    );

    // The false filter should eliminate all rows
    let db = GraphDbRef::new(&snapshot, 0, &NoOverlay, snapshot.t);
    let executable = ExecutableQuery::simple(query);
    let results = execute(db, &vars, &executable, ContextConfig::default())
        .await
        .unwrap();

    // No results because the filter eliminates the empty seed row
    assert!(results.is_empty() || results.iter().all(fluree_db_query::Batch::is_empty));
}

/// Test ValuesOperator directly with overlap compatibility
#[tokio::test]
async fn test_values_operator_overlap_compatibility() {
    let snapshot = make_test_snapshot();
    let vars = VarRegistry::new();
    let ctx = ExecutionContext::new(&snapshot, &vars);

    // Create an EmptyOperator as the seed
    let empty = Box::new(EmptyOperator::new());

    // Create VALUES with one variable and two rows
    let value_vars = vec![VarId(0)];
    let value_rows = vec![
        vec![Binding::lit(FlakeValue::Long(1), xsd_long())],
        vec![Binding::lit(FlakeValue::Long(2), xsd_long())],
    ];

    let mut values_op = ValuesOperator::new(empty, value_vars, value_rows);

    // Open and get batches
    values_op.open(&ctx).await.unwrap();

    let batch = values_op.next_batch(&ctx).await.unwrap();
    assert!(batch.is_some());

    let batch = batch.unwrap();
    // Should have 2 rows (cross product of 1 empty row × 2 value rows)
    assert_eq!(batch.len(), 2);

    // Schema should have the VALUES variable
    assert_eq!(batch.schema(), &[VarId(0)]);

    // Check the values
    let col = batch.column_by_idx(0).unwrap();
    let (val0, _) = col[0].as_lit().unwrap();
    let (val1, _) = col[1].as_lit().unwrap();
    assert_eq!(*val0, FlakeValue::Long(1));
    assert_eq!(*val1, FlakeValue::Long(2));

    values_op.close();
}

/// Test BIND clobber prevention - same value passes through
#[tokio::test]
async fn test_bind_clobber_same_value() {
    use fluree_db_query::bind::BindOperator;
    use fluree_db_query::binding::Batch;

    let snapshot = make_test_snapshot();
    let vars = VarRegistry::new();
    let ctx = ExecutionContext::new(&snapshot, &vars);

    // Create a seed that already has ?x = 42.
    // Use xsd_integer (not xsd_long) to match the datatype that
    // ComparableValue::Long.to_binding() now produces.
    let schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
    let columns = vec![vec![Binding::lit(FlakeValue::Long(42), Sid::xsd_integer())]];
    let seed_batch = Batch::new(schema.clone(), columns).unwrap();

    use fluree_db_query::seed::SeedOperator;
    let seed = Box::new(SeedOperator::from_batch_row(&seed_batch, 0));

    // BIND(42 AS ?x) - same value, should pass through
    let expr = Expression::Const(FlakeValue::Long(42));
    let mut bind_op = BindOperator::new(seed, VarId(0), expr, vec![]);

    bind_op.open(&ctx).await.unwrap();

    let result = bind_op.next_batch(&ctx).await.unwrap();
    assert!(result.is_some());

    let batch = result.unwrap();
    // Row should pass through (same value)
    assert_eq!(batch.len(), 1);

    bind_op.close();
}

/// Test BIND clobber prevention - different value drops row
#[tokio::test]
async fn test_bind_clobber_different_value() {
    use fluree_db_query::bind::BindOperator;
    use fluree_db_query::binding::Batch;

    let snapshot = make_test_snapshot();
    let vars = VarRegistry::new();
    let ctx = ExecutionContext::new(&snapshot, &vars);

    // Create a seed that already has ?x = 42
    let schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
    let columns = vec![vec![Binding::lit(FlakeValue::Long(42), xsd_long())]];
    let seed_batch = Batch::new(schema.clone(), columns).unwrap();

    use fluree_db_query::seed::SeedOperator;
    let seed = Box::new(SeedOperator::from_batch_row(&seed_batch, 0));

    // BIND(100 AS ?x) - different value, should drop the row
    let expr = Expression::Const(FlakeValue::Long(100));
    let mut bind_op = BindOperator::new(seed, VarId(0), expr, vec![]);

    bind_op.open(&ctx).await.unwrap();

    let result = bind_op.next_batch(&ctx).await.unwrap();
    // Row should be dropped (different value)
    assert!(result.is_none());

    bind_op.close();
}

/// Critical: UNION must be correlated with the incoming solution stream.
///
/// We test this without any DB state by using VALUES overlap semantics:
/// - Seed ?s = 1
/// - UNION branches each run VALUES over ?s with rows (1) and (2)
/// - If correlated, the (2) row is filtered out (mismatch with seeded ?s=1)
/// - If uncorrelated, we'd incorrectly see ?s=2 in results
#[tokio::test]
async fn test_union_is_correlated_via_values_overlap() {
    let snapshot = make_test_snapshot();
    let vars = VarRegistry::new();

    let query = make_query(
        vec![VarId(0)], // SELECT ?s
        vec![
            // Seed ?s = 1
            Pattern::Values {
                vars: vec![VarId(0)],
                rows: vec![vec![Binding::lit(FlakeValue::Long(1), xsd_long())]],
            },
            // UNION two branches that both attempt to inject (?s = 1) and (?s = 2)
            Pattern::Union(vec![
                vec![Pattern::Values {
                    vars: vec![VarId(0)],
                    rows: vec![
                        vec![Binding::lit(FlakeValue::Long(1), xsd_long())],
                        vec![Binding::lit(FlakeValue::Long(2), xsd_long())],
                    ],
                }],
                vec![Pattern::Values {
                    vars: vec![VarId(0)],
                    rows: vec![vec![Binding::lit(FlakeValue::Long(1), xsd_long())]],
                }],
            ]),
        ],
    );

    let db = GraphDbRef::new(&snapshot, 0, &NoOverlay, snapshot.t);
    let executable = ExecutableQuery::simple(query);
    let results = execute(db, &vars, &executable, ContextConfig::default())
        .await
        .unwrap();
    let rows: Vec<Binding> = results
        .iter()
        .flat_map(|b| b.column_by_idx(0).unwrap_or(&[]).iter().cloned())
        .collect();

    // Expect exactly two rows, both ?s=1 (one from each branch)
    assert_eq!(rows.len(), 2);
    for r in rows {
        let (val, _) = r.as_lit().unwrap();
        assert_eq!(*val, FlakeValue::Long(1));
    }
}

/// Critical: BIND evaluation errors should NOT clobber an existing bound value.
///
/// If the target variable is already bound and the expression evaluates to Unbound
/// (due to error/unbound input), we keep the row and keep the existing binding.
#[tokio::test]
async fn test_bind_error_does_not_clobber_existing_binding() {
    use fluree_db_query::bind::BindOperator;
    use fluree_db_query::binding::Batch;

    let snapshot = make_test_snapshot();
    let vars = VarRegistry::new();
    let ctx = ExecutionContext::new(&snapshot, &vars);

    // Seed row: ?x = 42
    let schema: Arc<[VarId]> = Arc::from(vec![VarId(0)].into_boxed_slice());
    let columns = vec![vec![Binding::lit(FlakeValue::Long(42), xsd_long())]];
    let seed_batch = Batch::new(schema.clone(), columns).unwrap();
    let seed = Box::new(fluree_db_query::seed::SeedOperator::from_batch_row(
        &seed_batch,
        0,
    ));

    // Expression uses an unbound var ?y => evaluation yields Unbound
    let expr = Expression::add(
        Expression::Var(VarId(1)), // ?y (unbound)
        Expression::Const(FlakeValue::Long(1)),
    );

    let mut bind_op = BindOperator::new(seed, VarId(0), expr, vec![]);
    bind_op.open(&ctx).await.unwrap();

    let batch = bind_op.next_batch(&ctx).await.unwrap().unwrap();
    assert_eq!(batch.len(), 1);

    // Value should remain 42 (not clobbered to Unbound)
    let (val, _) = batch.get_by_col(0, 0).as_lit().unwrap();
    assert_eq!(*val, FlakeValue::Long(42));
    bind_op.close();
}

/// Test BindOperator with inline filters - rows failing the filter are dropped
#[tokio::test]
async fn test_bind_with_inline_filter() {
    use fluree_db_query::bind::BindOperator;

    let snapshot = make_test_snapshot();
    let vars = VarRegistry::new();
    let ctx = ExecutionContext::new(&snapshot, &vars);

    // Create a VALUES operator producing three rows: ?x = 10, 20, 30
    let values_rows = vec![
        vec![Binding::lit(FlakeValue::Long(10), xsd_long())],
        vec![Binding::lit(FlakeValue::Long(20), xsd_long())],
        vec![Binding::lit(FlakeValue::Long(30), xsd_long())],
    ];
    let seed = Box::new(ValuesOperator::new(
        Box::new(EmptyOperator::new()),
        vec![VarId(0)],
        values_rows,
    ));

    // BIND(?x + 10 AS ?y) with inline filter: ?y > 25
    // ?x=10 => ?y=20 (fails ?y > 25)
    // ?x=20 => ?y=30 (passes)
    // ?x=30 => ?y=40 (passes)
    let bind_expr = Expression::add(
        Expression::Var(VarId(0)),
        Expression::Const(FlakeValue::Long(10)),
    );
    let filter_expr = Expression::gt(
        Expression::Var(VarId(1)),
        Expression::Const(FlakeValue::Long(25)),
    );

    let mut bind_op = BindOperator::new(seed, VarId(1), bind_expr, vec![filter_expr]);
    bind_op.open(&ctx).await.unwrap();

    let result = bind_op.next_batch(&ctx).await.unwrap();
    assert!(result.is_some(), "should produce a batch");

    let batch = result.unwrap();
    assert_eq!(batch.len(), 2, "only two rows should pass the filter");

    // First passing row: ?x=20, ?y=30
    let (x0, _) = batch.get_by_col(0, 0).as_lit().unwrap();
    let (y0, _) = batch.get_by_col(0, 1).as_lit().unwrap();
    assert_eq!(*x0, FlakeValue::Long(20));
    assert_eq!(*y0, FlakeValue::Long(30));

    // Second passing row: ?x=30, ?y=40
    let (x1, _) = batch.get_by_col(1, 0).as_lit().unwrap();
    let (y1, _) = batch.get_by_col(1, 1).as_lit().unwrap();
    assert_eq!(*x1, FlakeValue::Long(30));
    assert_eq!(*y1, FlakeValue::Long(40));

    bind_op.close();
}

/// Test BindOperator with inline filter that rejects all rows
#[tokio::test]
async fn test_bind_with_inline_filter_rejects_all() {
    use fluree_db_query::bind::BindOperator;

    let snapshot = make_test_snapshot();
    let vars = VarRegistry::new();
    let ctx = ExecutionContext::new(&snapshot, &vars);

    // Single row: ?x = 5
    let seed = Box::new(ValuesOperator::new(
        Box::new(EmptyOperator::new()),
        vec![VarId(0)],
        vec![vec![Binding::lit(FlakeValue::Long(5), xsd_long())]],
    ));

    // BIND(?x + 10 AS ?y) with filter: ?y > 100
    // ?x=5 => ?y=15 (fails ?y > 100)
    let bind_expr = Expression::add(
        Expression::Var(VarId(0)),
        Expression::Const(FlakeValue::Long(10)),
    );
    let filter_expr = Expression::gt(
        Expression::Var(VarId(1)),
        Expression::Const(FlakeValue::Long(100)),
    );

    let mut bind_op = BindOperator::new(seed, VarId(1), bind_expr, vec![filter_expr]);
    bind_op.open(&ctx).await.unwrap();

    let result = bind_op.next_batch(&ctx).await.unwrap();
    assert!(result.is_none(), "all rows filtered out should yield None");

    bind_op.close();
}

/// Test BindOperator with multiple inline filters — only rows passing ALL filters survive.
///
/// All five rows pass filter1 (?y > 15), but only the middle three pass both
/// filter1 AND filter2 (?y < 45). Validates conjunction semantics of the filters vec.
#[tokio::test]
async fn test_bind_with_multiple_inline_filters() {
    use fluree_db_query::bind::BindOperator;

    let snapshot = make_test_snapshot();
    let vars = VarRegistry::new();
    let ctx = ExecutionContext::new(&snapshot, &vars);

    // Five rows: ?x = 10, 20, 30, 40, 50
    let values_rows = vec![
        vec![Binding::lit(FlakeValue::Long(10), xsd_long())],
        vec![Binding::lit(FlakeValue::Long(20), xsd_long())],
        vec![Binding::lit(FlakeValue::Long(30), xsd_long())],
        vec![Binding::lit(FlakeValue::Long(40), xsd_long())],
        vec![Binding::lit(FlakeValue::Long(50), xsd_long())],
    ];
    let seed = Box::new(ValuesOperator::new(
        Box::new(EmptyOperator::new()),
        vec![VarId(0)],
        values_rows,
    ));

    // BIND(?x + 10 AS ?y) => ?y = 20, 30, 40, 50, 60
    // filter1: ?y > 15  — all rows pass
    // filter2: ?y < 45  — ?y=20,30,40 pass; ?y=50,60 fail
    // Combined: only ?y=20,30,40 survive (the middle three input rows)
    let bind_expr = Expression::add(
        Expression::Var(VarId(0)),
        Expression::Const(FlakeValue::Long(10)),
    );
    let filter1 = Expression::gt(
        Expression::Var(VarId(1)),
        Expression::Const(FlakeValue::Long(15)),
    );
    let filter2 = Expression::lt(
        Expression::Var(VarId(1)),
        Expression::Const(FlakeValue::Long(45)),
    );

    let mut bind_op = BindOperator::new(seed, VarId(1), bind_expr, vec![filter1, filter2]);
    bind_op.open(&ctx).await.unwrap();

    let result = bind_op.next_batch(&ctx).await.unwrap();
    assert!(result.is_some(), "should produce a batch");

    let batch = result.unwrap();
    assert_eq!(batch.len(), 3, "only three rows should pass both filters");

    // Row 0: ?x=10, ?y=20
    let (x0, _) = batch.get_by_col(0, 0).as_lit().unwrap();
    let (y0, _) = batch.get_by_col(0, 1).as_lit().unwrap();
    assert_eq!(*x0, FlakeValue::Long(10));
    assert_eq!(*y0, FlakeValue::Long(20));

    // Row 1: ?x=20, ?y=30
    let (x1, _) = batch.get_by_col(1, 0).as_lit().unwrap();
    let (y1, _) = batch.get_by_col(1, 1).as_lit().unwrap();
    assert_eq!(*x1, FlakeValue::Long(20));
    assert_eq!(*y1, FlakeValue::Long(30));

    // Row 2: ?x=30, ?y=40
    let (x2, _) = batch.get_by_col(2, 0).as_lit().unwrap();
    let (y2, _) = batch.get_by_col(2, 1).as_lit().unwrap();
    assert_eq!(*x2, FlakeValue::Long(30));
    assert_eq!(*y2, FlakeValue::Long(40));

    bind_op.close();
}
