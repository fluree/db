//! Integration tests for GROUP BY and Aggregates
//!
//! These tests verify the P3 functionality:
//! - GROUP BY partitioning
//! - Aggregate functions (COUNT, SUM, AVG, MIN, MAX, etc.)
//! - HAVING filter on aggregated results
//! - Full pipeline execution

use fluree_db_core::{FlakeValue, GraphDbRef, LedgerSnapshot, NoOverlay, NonEmpty, Sid};
use fluree_db_query::aggregate::{AggregateFn, AggregateSpec};
use fluree_db_query::binding::Binding;
use fluree_db_query::context::ExecutionContext;
use fluree_db_query::execute::{execute, ContextConfig, ExecutableQuery};
use fluree_db_query::groupby::GroupByOperator;
use fluree_db_query::ir::QueryOptions;
use fluree_db_query::ir::{Expression, Grouping, Pattern};
use fluree_db_query::ir::{Query, QueryOutput};
use fluree_db_query::operator::Operator;
use fluree_db_query::var_registry::{VarId, VarRegistry};
use fluree_graph_json_ld::ParsedContext;
use std::sync::Arc;

fn explicit_grouping(by: Vec<VarId>, aggregates: Vec<AggregateSpec>) -> Grouping {
    Grouping::Explicit {
        group_by: NonEmpty::try_from_vec(by).expect("non-empty group_by"),
        aggregates,
        having: None,
    }
}

fn explicit_grouping_having(
    by: Vec<VarId>,
    aggregates: Vec<AggregateSpec>,
    having: Expression,
) -> Grouping {
    Grouping::Explicit {
        group_by: NonEmpty::try_from_vec(by).expect("non-empty group_by"),
        aggregates,
        having: Some(having),
    }
}

fn implicit_grouping(aggregates: Vec<AggregateSpec>) -> Grouping {
    Grouping::Implicit {
        aggregates: NonEmpty::try_from_vec(aggregates).expect("non-empty aggregates"),
        having: None,
    }
}

fn make_test_snapshot() -> LedgerSnapshot {
    LedgerSnapshot::genesis("test/main")
}

fn xsd_long() -> Sid {
    Sid::new(2, "long")
}

fn xsd_string() -> Sid {
    Sid::new(2, "string")
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
        grouping: None,
        options: QueryOptions::default(),
        post_values: None,
    }
}

/// Test GROUP BY with COUNT aggregate via execute pipeline
///
/// Simulates: SELECT ?city (COUNT(?person) AS ?count)
///            WHERE { ... }
///            GROUP BY ?city
#[tokio::test]
async fn test_group_by_with_count() {
    let snapshot = make_test_snapshot();
    let vars = VarRegistry::new();

    // Create input data via VALUES:
    // ?city, ?person
    // NYC, alice
    // NYC, bob
    // LA, carol
    // LA, dan
    // LA, eve
    let mut query = make_query(
        vec![VarId(0), VarId(1)], // SELECT ?city, ?count (where ?count will replace ?person)
        vec![Pattern::Values {
            vars: vec![VarId(0), VarId(1)], // ?city, ?person
            rows: vec![
                vec![
                    Binding::lit(FlakeValue::String("NYC".into()), xsd_string()),
                    Binding::sid(Sid::new(100, "alice")),
                ],
                vec![
                    Binding::lit(FlakeValue::String("NYC".into()), xsd_string()),
                    Binding::sid(Sid::new(100, "bob")),
                ],
                vec![
                    Binding::lit(FlakeValue::String("LA".into()), xsd_string()),
                    Binding::sid(Sid::new(100, "carol")),
                ],
                vec![
                    Binding::lit(FlakeValue::String("LA".into()), xsd_string()),
                    Binding::sid(Sid::new(100, "dan")),
                ],
                vec![
                    Binding::lit(FlakeValue::String("LA".into()), xsd_string()),
                    Binding::sid(Sid::new(100, "eve")),
                ],
            ],
        }],
    );

    query.grouping = Some(explicit_grouping(
        vec![VarId(0)], // GROUP BY ?city
        vec![AggregateSpec {
            function: AggregateFn::Count,
            input_var: Some(VarId(1)), // COUNT(?person)
            output_var: VarId(1),      // AS ?count (replaces ?person col)
            distinct: false,
        }],
    ));

    let db = GraphDbRef::new(&snapshot, 0, &NoOverlay, snapshot.t);
    let executable = ExecutableQuery::new(query, QueryOptions::default());
    let results = execute(db, &vars, &executable, ContextConfig::default())
        .await
        .unwrap();

    // Expect 2 groups: NYC (count=2), LA (count=3)
    let total_rows: usize = results.iter().map(fluree_db_query::Batch::len).sum();
    assert_eq!(total_rows, 2);

    // Collect results
    let mut counts: Vec<(String, i64)> = Vec::new();
    for batch in &results {
        for row_idx in 0..batch.len() {
            let city = batch.get_by_col(row_idx, 0);
            let count = batch.get_by_col(row_idx, 1);

            if let (Binding::Lit { val: city_val, .. }, Binding::Lit { val: count_val, .. }) =
                (city, count)
            {
                if let (FlakeValue::String(city_str), FlakeValue::Long(count_num)) =
                    (city_val, count_val)
                {
                    counts.push((city_str.clone(), *count_num));
                }
            }
        }
    }

    counts.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(counts.len(), 2);
    assert_eq!(counts[0], ("LA".to_string(), 3));
    assert_eq!(counts[1], ("NYC".to_string(), 2));
}

/// Test GROUP BY with SUM aggregate
#[tokio::test]
async fn test_group_by_with_sum() {
    let snapshot = make_test_snapshot();
    let vars = VarRegistry::new();

    // Create input data via VALUES:
    // ?city, ?amount
    // NYC, 100
    // NYC, 200
    // LA, 50
    let mut query = make_query(
        vec![VarId(0), VarId(1)],
        vec![Pattern::Values {
            vars: vec![VarId(0), VarId(1)],
            rows: vec![
                vec![
                    Binding::lit(FlakeValue::String("NYC".into()), xsd_string()),
                    Binding::lit(FlakeValue::Long(100), xsd_long()),
                ],
                vec![
                    Binding::lit(FlakeValue::String("NYC".into()), xsd_string()),
                    Binding::lit(FlakeValue::Long(200), xsd_long()),
                ],
                vec![
                    Binding::lit(FlakeValue::String("LA".into()), xsd_string()),
                    Binding::lit(FlakeValue::Long(50), xsd_long()),
                ],
            ],
        }],
    );

    query.grouping = Some(explicit_grouping(
        vec![VarId(0)],
        vec![AggregateSpec {
            function: AggregateFn::Sum,
            input_var: Some(VarId(1)),
            output_var: VarId(1),
            distinct: false,
        }],
    ));

    let db = GraphDbRef::new(&snapshot, 0, &NoOverlay, snapshot.t);
    let executable = ExecutableQuery::new(query, QueryOptions::default());
    let results = execute(db, &vars, &executable, ContextConfig::default())
        .await
        .unwrap();

    // Collect results
    let mut sums: Vec<(String, i64)> = Vec::new();
    for batch in &results {
        for row_idx in 0..batch.len() {
            let city = batch.get_by_col(row_idx, 0);
            let sum = batch.get_by_col(row_idx, 1);

            if let (Binding::Lit { val: city_val, .. }, Binding::Lit { val: sum_val, .. }) =
                (city, sum)
            {
                if let (FlakeValue::String(city_str), FlakeValue::Long(sum_num)) =
                    (city_val, sum_val)
                {
                    sums.push((city_str.clone(), *sum_num));
                }
            }
        }
    }

    sums.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(sums.len(), 2);
    assert_eq!(sums[0], ("LA".to_string(), 50));
    assert_eq!(sums[1], ("NYC".to_string(), 300));
}

/// Test GROUP BY with HAVING filter
///
/// Simulates: SELECT ?city (COUNT(?person) AS ?count)
///            WHERE { ... }
///            GROUP BY ?city
///            HAVING (COUNT(?person) > 2)
#[tokio::test]
async fn test_group_by_with_having() {
    let snapshot = make_test_snapshot();
    let vars = VarRegistry::new();

    // Same data as test_group_by_with_count
    let mut query = make_query(
        vec![VarId(0), VarId(1)],
        vec![Pattern::Values {
            vars: vec![VarId(0), VarId(1)],
            rows: vec![
                vec![
                    Binding::lit(FlakeValue::String("NYC".into()), xsd_string()),
                    Binding::sid(Sid::new(100, "alice")),
                ],
                vec![
                    Binding::lit(FlakeValue::String("NYC".into()), xsd_string()),
                    Binding::sid(Sid::new(100, "bob")),
                ],
                vec![
                    Binding::lit(FlakeValue::String("LA".into()), xsd_string()),
                    Binding::sid(Sid::new(100, "carol")),
                ],
                vec![
                    Binding::lit(FlakeValue::String("LA".into()), xsd_string()),
                    Binding::sid(Sid::new(100, "dan")),
                ],
                vec![
                    Binding::lit(FlakeValue::String("LA".into()), xsd_string()),
                    Binding::sid(Sid::new(100, "eve")),
                ],
            ],
        }],
    );

    // HAVING ?count > 2 (only LA with count=3 passes)
    query.grouping = Some(explicit_grouping_having(
        vec![VarId(0)],
        vec![AggregateSpec {
            function: AggregateFn::Count,
            input_var: Some(VarId(1)),
            output_var: VarId(1),
            distinct: false,
        }],
        Expression::gt(
            Expression::Var(VarId(1)),
            Expression::Const(FlakeValue::Long(2)),
        ),
    ));

    let db = GraphDbRef::new(&snapshot, 0, &NoOverlay, snapshot.t);
    let executable = ExecutableQuery::new(query, QueryOptions::default());
    let results = execute(db, &vars, &executable, ContextConfig::default())
        .await
        .unwrap();

    // Only LA should remain (count=3 > 2)
    let total_rows: usize = results.iter().map(fluree_db_query::Batch::len).sum();
    assert_eq!(total_rows, 1);

    // Verify it's LA
    let city = results[0].get_by_col(0, 0);
    if let Binding::Lit { val, .. } = city {
        assert_eq!(*val, FlakeValue::String("LA".into()));
    } else {
        panic!("Expected Lit binding for city");
    }
}

/// Test no GROUP BY but with aggregates (implicit single group)
#[tokio::test]
async fn test_aggregates_without_group_by() {
    let snapshot = make_test_snapshot();
    let vars = VarRegistry::new();

    // Input: 3 values
    let mut query = make_query(
        vec![VarId(0)],
        vec![Pattern::Values {
            vars: vec![VarId(0)],
            rows: vec![
                vec![Binding::lit(FlakeValue::Long(10), xsd_long())],
                vec![Binding::lit(FlakeValue::Long(20), xsd_long())],
                vec![Binding::lit(FlakeValue::Long(30), xsd_long())],
            ],
        }],
    );

    // No GROUP BY, just SUM — implicit single-group aggregation.
    query.grouping = Some(implicit_grouping(vec![AggregateSpec {
        function: AggregateFn::Sum,
        input_var: Some(VarId(0)),
        output_var: VarId(0),
        distinct: false,
    }]));

    let db = GraphDbRef::new(&snapshot, 0, &NoOverlay, snapshot.t);
    let executable = ExecutableQuery::new(query, QueryOptions::default());
    let results = execute(db, &vars, &executable, ContextConfig::default())
        .await
        .unwrap();

    // Should have 1 row with sum=60
    let total_rows: usize = results.iter().map(fluree_db_query::Batch::len).sum();
    assert_eq!(total_rows, 1);

    let sum = results[0].get_by_col(0, 0);
    if let Binding::Lit { val, .. } = sum {
        assert_eq!(*val, FlakeValue::Long(60));
    } else {
        panic!("Expected Lit binding for sum");
    }
}

/// Test GROUP BY operator directly with multiple group keys
#[tokio::test]
async fn test_group_by_multiple_keys() {
    let snapshot = make_test_snapshot();
    let vars = VarRegistry::new();
    let ctx = ExecutionContext::new(&snapshot, &vars);

    // Create batch: ?region, ?city, ?amount
    // East, NYC, 100
    // East, NYC, 200
    // East, BOS, 50
    // West, LA, 75
    let schema: Arc<[VarId]> = Arc::from(vec![VarId(0), VarId(1), VarId(2)].into_boxed_slice());
    let columns = vec![
        vec![
            Binding::lit(FlakeValue::String("East".into()), xsd_string()),
            Binding::lit(FlakeValue::String("East".into()), xsd_string()),
            Binding::lit(FlakeValue::String("East".into()), xsd_string()),
            Binding::lit(FlakeValue::String("West".into()), xsd_string()),
        ],
        vec![
            Binding::lit(FlakeValue::String("NYC".into()), xsd_string()),
            Binding::lit(FlakeValue::String("NYC".into()), xsd_string()),
            Binding::lit(FlakeValue::String("BOS".into()), xsd_string()),
            Binding::lit(FlakeValue::String("LA".into()), xsd_string()),
        ],
        vec![
            Binding::lit(FlakeValue::Long(100), xsd_long()),
            Binding::lit(FlakeValue::Long(200), xsd_long()),
            Binding::lit(FlakeValue::Long(50), xsd_long()),
            Binding::lit(FlakeValue::Long(75), xsd_long()),
        ],
    ];
    let batch = fluree_db_query::binding::Batch::new(schema.clone(), columns).unwrap();

    // Use a batch-producing operator
    struct BatchOperator {
        schema: Arc<[VarId]>,
        batch: Option<fluree_db_query::binding::Batch>,
    }
    use async_trait::async_trait;
    #[async_trait]
    impl fluree_db_query::operator::Operator for BatchOperator {
        fn schema(&self) -> &[VarId] {
            &self.schema
        }
        async fn open(&mut self, _: &ExecutionContext<'_>) -> fluree_db_query::error::Result<()> {
            Ok(())
        }
        async fn next_batch(
            &mut self,
            _: &ExecutionContext<'_>,
        ) -> fluree_db_query::error::Result<Option<fluree_db_query::binding::Batch>> {
            Ok(self.batch.take())
        }
        fn close(&mut self) {}
    }

    let child: fluree_db_query::operator::BoxedOperator = Box::new(BatchOperator {
        schema: schema.clone(),
        batch: Some(batch),
    });

    // GROUP BY ?region, ?city
    let mut op = GroupByOperator::new(child, vec![VarId(0), VarId(1)]);
    op.open(&ctx).await.unwrap();

    // Collect all groups
    let mut total_groups = 0;
    while let Some(batch) = op.next_batch(&ctx).await.unwrap() {
        total_groups += batch.len();
        for row_idx in 0..batch.len() {
            // Region and city should be single values
            let region = batch.get_by_col(row_idx, 0);
            let city = batch.get_by_col(row_idx, 1);
            assert!(!region.is_grouped());
            assert!(!city.is_grouped());

            // Amount should be Grouped
            let amount = batch.get_by_col(row_idx, 2);
            assert!(amount.is_grouped());
        }
    }

    // Should have 3 groups: (East, NYC), (East, BOS), (West, LA)
    assert_eq!(total_groups, 3);

    op.close();
}

/// Test AVG aggregate
#[tokio::test]
async fn test_aggregate_avg() {
    let snapshot = make_test_snapshot();
    let vars = VarRegistry::new();

    let mut query = make_query(
        vec![VarId(0), VarId(1)],
        vec![Pattern::Values {
            vars: vec![VarId(0), VarId(1)],
            rows: vec![
                vec![
                    Binding::lit(FlakeValue::String("A".into()), xsd_string()),
                    Binding::lit(FlakeValue::Long(10), xsd_long()),
                ],
                vec![
                    Binding::lit(FlakeValue::String("A".into()), xsd_string()),
                    Binding::lit(FlakeValue::Long(20), xsd_long()),
                ],
                vec![
                    Binding::lit(FlakeValue::String("A".into()), xsd_string()),
                    Binding::lit(FlakeValue::Long(30), xsd_long()),
                ],
            ],
        }],
    );

    query.grouping = Some(explicit_grouping(
        vec![VarId(0)],
        vec![AggregateSpec {
            function: AggregateFn::Avg,
            input_var: Some(VarId(1)),
            output_var: VarId(1),
            distinct: false,
        }],
    ));

    let db = GraphDbRef::new(&snapshot, 0, &NoOverlay, snapshot.t);
    let executable = ExecutableQuery::new(query, QueryOptions::default());
    let results = execute(db, &vars, &executable, ContextConfig::default())
        .await
        .unwrap();

    // Should have 1 row with avg=20.0
    let avg = results[0].get_by_col(0, 1);
    if let Binding::Lit { val, .. } = avg {
        assert_eq!(*val, FlakeValue::Double(20.0));
    } else {
        panic!("Expected Lit binding for avg");
    }
}

/// Test MIN/MAX aggregates
#[tokio::test]
async fn test_aggregate_min_max() {
    let snapshot = make_test_snapshot();
    let vars = VarRegistry::new();

    let mut query = make_query(
        vec![VarId(0), VarId(1), VarId(2)], // ?city, ?min, ?max
        vec![Pattern::Values {
            vars: vec![VarId(0), VarId(1), VarId(2)], // ?city, ?val1, ?val2 (we'll aggregate both)
            rows: vec![
                vec![
                    Binding::lit(FlakeValue::String("A".into()), xsd_string()),
                    Binding::lit(FlakeValue::Long(50), xsd_long()),
                    Binding::lit(FlakeValue::Long(50), xsd_long()),
                ],
                vec![
                    Binding::lit(FlakeValue::String("A".into()), xsd_string()),
                    Binding::lit(FlakeValue::Long(10), xsd_long()),
                    Binding::lit(FlakeValue::Long(10), xsd_long()),
                ],
                vec![
                    Binding::lit(FlakeValue::String("A".into()), xsd_string()),
                    Binding::lit(FlakeValue::Long(30), xsd_long()),
                    Binding::lit(FlakeValue::Long(30), xsd_long()),
                ],
            ],
        }],
    );

    query.grouping = Some(explicit_grouping(
        vec![VarId(0)],
        vec![
            AggregateSpec {
                function: AggregateFn::Min,
                input_var: Some(VarId(1)),
                output_var: VarId(1),
                distinct: false,
            },
            AggregateSpec {
                function: AggregateFn::Max,
                input_var: Some(VarId(2)),
                output_var: VarId(2),
                distinct: false,
            },
        ],
    ));

    let db = GraphDbRef::new(&snapshot, 0, &NoOverlay, snapshot.t);
    let executable = ExecutableQuery::new(query, QueryOptions::default());
    let results = execute(db, &vars, &executable, ContextConfig::default())
        .await
        .unwrap();

    // Should have 1 row with min=10, max=50
    let min = results[0].get_by_col(0, 1);
    let max = results[0].get_by_col(0, 2);

    if let Binding::Lit { val, .. } = min {
        assert_eq!(*val, FlakeValue::Long(10));
    } else {
        panic!("Expected Lit binding for min");
    }

    if let Binding::Lit { val, .. } = max {
        assert_eq!(*val, FlakeValue::Long(50));
    } else {
        panic!("Expected Lit binding for max");
    }
}

/// ORDER BY on a grouped (non-key, non-aggregate) var should error.
#[tokio::test]
async fn test_order_by_on_grouped_var_errors() {
    let snapshot = make_test_snapshot();
    let vars = VarRegistry::new();

    // VALUES ?city ?person ...
    let mut query = make_query(
        vec![VarId(0), VarId(1)],
        vec![Pattern::Values {
            vars: vec![VarId(0), VarId(1)],
            rows: vec![
                vec![
                    Binding::lit(FlakeValue::String("NYC".into()), xsd_string()),
                    Binding::sid(Sid::new(100, "alice")),
                ],
                vec![
                    Binding::lit(FlakeValue::String("NYC".into()), xsd_string()),
                    Binding::sid(Sid::new(100, "bob")),
                ],
            ],
        }],
    );

    // GROUP BY ?city, no aggregates: ?person becomes Grouped(...)
    // ORDER BY ?person is undefined -> should error.
    query.grouping = Some(explicit_grouping(vec![VarId(0)], vec![]));
    let options = QueryOptions::new()
        .with_order_by(vec![fluree_db_query::sort::SortSpec::asc(VarId(1))]);

    let db = GraphDbRef::new(&snapshot, 0, &NoOverlay, snapshot.t);
    let executable = ExecutableQuery::new(query, options);
    let err = execute(db, &vars, &executable, ContextConfig::default())
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("Cannot ORDER BY"),
        "unexpected error: {err}"
    );
}

/// Aggregating a GROUP BY key var should error (it is not Grouped in this model).
#[tokio::test]
async fn test_aggregate_on_group_by_key_errors() {
    let snapshot = make_test_snapshot();
    let vars = VarRegistry::new();

    let mut query = make_query(
        vec![VarId(0), VarId(1)],
        vec![Pattern::Values {
            vars: vec![VarId(0), VarId(1)],
            rows: vec![
                vec![
                    Binding::lit(FlakeValue::String("NYC".into()), xsd_string()),
                    Binding::sid(Sid::new(100, "alice")),
                ],
                vec![
                    Binding::lit(FlakeValue::String("NYC".into()), xsd_string()),
                    Binding::sid(Sid::new(100, "bob")),
                ],
            ],
        }],
    );

    // Attempt to COUNT(?city) while also GROUP BY ?city.
    query.grouping = Some(explicit_grouping(
        vec![VarId(0)],
        vec![AggregateSpec {
            function: AggregateFn::Count,
            input_var: Some(VarId(0)), // key var
            output_var: VarId(0),
            distinct: false,
        }],
    ));
    let options = QueryOptions::default();

    let db = GraphDbRef::new(&snapshot, 0, &NoOverlay, snapshot.t);
    let executable = ExecutableQuery::new(query, options);
    let err = execute(db, &vars, &executable, ContextConfig::default())
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("GROUP BY key"),
        "unexpected error: {err}"
    );
}
