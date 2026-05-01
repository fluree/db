//! Query execution engine
//!
//! This module provides the query runner that builds operator trees from
//! `ParsedQuery` and executes them with optional solution modifiers.
//!
//! # Architecture
//!
//! The execution pipeline applies operators in this order:
//! ```text
//! WHERE patterns → GROUP BY → Aggregates → HAVING → ORDER BY → PROJECT → DISTINCT → OFFSET → LIMIT
//! ```
//!
//! - GROUP BY partitions solutions and creates Grouped values for non-key vars
//! - Aggregates compute COUNT, SUM, AVG, etc. on Grouped values
//! - HAVING filters grouped/aggregated results
//! - ORDER BY before PROJECT because sort may reference vars not in SELECT
//! - DISTINCT applies to projected output (post-select)
//! - OFFSET/LIMIT apply last for pagination
//!
//! # Module Organization
//!
//! The execution engine is split into focused submodules:
//!
//! - `reasoning_prep`: Schema hierarchy, reasoning modes, derived facts
//! - `rewrite_glue`: Pattern rewriting for RDFS/OWL expansion
//! - `pushdown`: Filter bounds extraction for index-level filtering
//! - `where_plan`: WHERE clause operator building
//! - `operator_tree`: Complete operator tree construction
//! - `runner`: Unified execution runner (eliminates duplication)
//!
//! Use `execute_query` for simple execution or build an `ExecutableQuery` with custom `QueryOptions` for full control.

mod dependency;
pub(crate) mod operator_tree;
mod pushdown;
mod reasoning_prep;
mod rewrite_glue;
mod runner;
mod where_plan;

// Re-export public types
pub use runner::execute;
pub use runner::execute_prepared;
pub use runner::ContextConfig;
pub use runner::ExecutableQuery;
pub use runner::QueryContextParams;

// Re-export internal helpers for use in lib.rs
pub use where_plan::build_where_operators_seeded;
pub(crate) use where_plan::{analyze_property_join_plan, collect_inner_join_block};

// Re-export operator tree builder and runner for custom execution pipelines
pub use operator_tree::build_operator_tree;
pub use runner::run_operator;

// Re-export pushdown utilities for tests
pub use pushdown::{
    count_filter_vars, extract_bounds_from_filters, extract_lookahead_bounds_with_consumption,
    merge_lower_bound, merge_object_bounds, merge_upper_bound,
};

use crate::binding::Batch;
use crate::dataset::DataSet;
use crate::error::Result;
use crate::var_registry::VarRegistry;
use fluree_db_core::{GraphDbRef, Tracker};

use runner::{
    execute_prepared_with_dataset, execute_prepared_with_dataset_and_bm25,
    execute_prepared_with_dataset_and_policy, execute_prepared_with_dataset_and_policy_and_bm25,
    execute_prepared_with_dataset_and_policy_and_providers,
    execute_prepared_with_dataset_and_providers, execute_prepared_with_dataset_history,
    execute_prepared_with_overlay, execute_prepared_with_overlay_tracked,
    execute_prepared_with_policy, execute_prepared_with_r2rml,
};
pub use runner::{
    prepare_execution, prepare_execution_with_binary_store, prepare_execution_with_config,
    PrepareConfig,
};

/// Execute a query with an overlay
///
/// This is the primary execution path for queries over indexed databases
/// with uncommitted changes (novelty). The `GraphDbRef` bundles the snapshot,
/// graph ID, overlay, and time bound.
///
/// # Returns
///
/// Vector of result batches.
pub async fn execute_with_overlay(
    db: GraphDbRef<'_>,
    vars: &VarRegistry,
    query: &ExecutableQuery,
) -> Result<Vec<Batch>> {
    let prepared = prepare_execution(db, query).await?;
    execute_prepared_with_overlay(db, vars, prepared).await
}

/// Execute a query with an overlay and optional tracking.
///
/// This mirrors `execute_with_overlay`, but attaches `tracker` to the execution context.
pub async fn execute_with_overlay_tracked<'a>(
    db: GraphDbRef<'a>,
    vars: &VarRegistry,
    query: &ExecutableQuery,
    tracker: Option<&'a Tracker>,
) -> Result<Vec<Batch>> {
    let prepared = prepare_execution(db, query).await?;
    execute_prepared_with_overlay_tracked(db, vars, prepared, tracker).await
}

/// Execute a query with policy enforcement
///
/// This function applies access control policies during query execution,
/// filtering results based on the provided `PolicyContext`.
pub async fn execute_with_policy<'a>(
    db: GraphDbRef<'a>,
    vars: &VarRegistry,
    query: &ExecutableQuery,
    policy: &'a fluree_db_policy::PolicyContext,
) -> Result<Vec<Batch>> {
    let prepared = prepare_execution(db, query).await?;
    execute_prepared_with_policy(db, vars, prepared, policy, None).await
}

/// Execute a query with policy enforcement, with optional tracking.
///
/// This mirrors `execute_with_policy`, but attaches `tracker` to the execution context.
pub async fn execute_with_policy_tracked<'a>(
    db: GraphDbRef<'a>,
    vars: &VarRegistry,
    query: &ExecutableQuery,
    policy: &'a fluree_db_policy::PolicyContext,
    tracker: &'a Tracker,
) -> Result<Vec<Batch>> {
    let prepared = prepare_execution(db, query).await?;
    execute_prepared_with_policy(db, vars, prepared, policy, Some(tracker)).await
}

/// Execute a query with R2RML providers (for mapped graph source support).
pub async fn execute_with_r2rml<'a, 'b>(
    db: GraphDbRef<'a>,
    vars: &VarRegistry,
    query: &ExecutableQuery,
    tracker: &'a Tracker,
    r2rml_provider: &'b dyn crate::r2rml::R2rmlProvider,
    r2rml_table_provider: &'b dyn crate::r2rml::R2rmlTableProvider,
) -> Result<Vec<Batch>> {
    let prepared = prepare_execution(db, query).await?;
    execute_prepared_with_r2rml(
        db,
        vars,
        prepared,
        tracker,
        r2rml_provider,
        r2rml_table_provider,
    )
    .await
}

/// Execute a query against a dataset (multi-graph query)
pub async fn execute_with_dataset<'a>(
    db: GraphDbRef<'a>,
    vars: &VarRegistry,
    query: &ExecutableQuery,
    dataset: &'a DataSet<'a>,
) -> Result<Vec<Batch>> {
    let prepared = prepare_execution(db, query).await?;
    execute_prepared_with_dataset(db, vars, prepared, dataset, None).await
}

/// Execute a query against a dataset (multi-graph), with optional tracking.
pub async fn execute_with_dataset_tracked<'a>(
    db: GraphDbRef<'a>,
    vars: &VarRegistry,
    query: &ExecutableQuery,
    dataset: &'a DataSet<'a>,
    tracker: &'a Tracker,
) -> Result<Vec<Batch>> {
    let prepared = prepare_execution(db, query).await?;
    execute_prepared_with_dataset(db, vars, prepared, dataset, Some(tracker)).await
}

/// Execute a query against a dataset in history mode.
///
/// The `_history` suffix is load-bearing: this helper prepares the operator
/// tree with [`PrepareConfig::history`] so scan operators emit asserts +
/// retracts and fast paths are gated off at planner-time. Callers that want
/// current-state semantics should use [`execute_with_dataset`] instead.
///
/// The view layer (`view::dataset_query::query_dataset`) has its own
/// detection logic and goes through `prepare_execution_with_config`
/// directly; this top-level helper is for callers that already know they
/// want history-range semantics without going through the view layer.
pub async fn execute_with_dataset_history<'a>(
    db: GraphDbRef<'a>,
    vars: &VarRegistry,
    query: &ExecutableQuery,
    dataset: &'a DataSet<'a>,
    tracker: Option<&'a Tracker>,
) -> Result<Vec<Batch>> {
    let prepared = prepare_execution_with_config(db, query, &PrepareConfig::history(None)).await?;
    execute_prepared_with_dataset_history(db, vars, prepared, dataset, tracker).await
}

/// Execute a query against a dataset (multi-graph) with policy enforcement
pub async fn execute_with_dataset_and_policy<'a>(
    db: GraphDbRef<'a>,
    vars: &VarRegistry,
    query: &ExecutableQuery,
    dataset: &'a DataSet<'a>,
    policy: &'a fluree_db_policy::PolicyContext,
) -> Result<Vec<Batch>> {
    let prepared = prepare_execution(db, query).await?;
    execute_prepared_with_dataset_and_policy(db, vars, prepared, dataset, policy, None).await
}

/// Execute a query against a dataset (multi-graph) with policy enforcement, with optional tracking.
pub async fn execute_with_dataset_and_policy_tracked<'a>(
    db: GraphDbRef<'a>,
    vars: &VarRegistry,
    query: &ExecutableQuery,
    dataset: &'a DataSet<'a>,
    policy: &'a fluree_db_policy::PolicyContext,
    tracker: &'a Tracker,
) -> Result<Vec<Batch>> {
    let prepared = prepare_execution(db, query).await?;
    execute_prepared_with_dataset_and_policy(db, vars, prepared, dataset, policy, Some(tracker))
        .await
}

/// Execute a query against a dataset with BM25 provider (for index graph source queries)
///
/// This combines dataset execution (multiple default/named graphs) with BM25 index
/// provider support, enabling `f:searchText` patterns in queries to resolve against
/// graph source BM25 indexes.
pub async fn execute_with_dataset_and_bm25<'a>(
    db: GraphDbRef<'a>,
    vars: &VarRegistry,
    query: &ExecutableQuery,
    dataset: &'a DataSet<'a>,
    bm25_provider: &dyn crate::bm25::Bm25IndexProvider,
    tracker: Option<&'a Tracker>,
) -> Result<Vec<Batch>> {
    let prepared = prepare_execution(db, query).await?;
    execute_prepared_with_dataset_and_bm25(db, vars, prepared, dataset, bm25_provider, tracker)
        .await
}

/// Execute a query against a dataset with policy enforcement and BM25 provider
///
/// This combines dataset execution (multiple default/named graphs) with policy
/// enforcement and BM25 index provider support.
pub async fn execute_with_dataset_and_policy_and_bm25<'a>(
    db: GraphDbRef<'a>,
    vars: &VarRegistry,
    query: &ExecutableQuery,
    dataset: &'a DataSet<'a>,
    policy: &'a fluree_db_policy::PolicyContext,
    bm25_provider: &dyn crate::bm25::Bm25IndexProvider,
    tracker: Option<&'a Tracker>,
) -> Result<Vec<Batch>> {
    let prepared = prepare_execution(db, query).await?;
    execute_prepared_with_dataset_and_policy_and_bm25(
        db,
        vars,
        prepared,
        dataset,
        policy,
        bm25_provider,
        tracker,
    )
    .await
}

/// Execute a query against a dataset with both BM25 and vector providers (for index graph source queries)
///
/// This combines dataset execution (multiple default/named graphs) with both BM25 and
/// vector index provider support, enabling both `f:searchText` and `f:queryVector` patterns
/// in queries.
pub async fn execute_with_dataset_and_providers<'a, 'b>(
    db: GraphDbRef<'a>,
    vars: &VarRegistry,
    query: &ExecutableQuery,
    dataset: &'a DataSet<'a>,
    bm25_provider: &'b dyn crate::bm25::Bm25IndexProvider,
    vector_provider: &'b dyn crate::vector::VectorIndexProvider,
    tracker: Option<&'a Tracker>,
) -> Result<Vec<Batch>> {
    let prepared = prepare_execution(db, query).await?;
    execute_prepared_with_dataset_and_providers(
        db,
        vars,
        prepared,
        dataset,
        bm25_provider,
        vector_provider,
        tracker,
    )
    .await
}

/// Execute a query against a dataset with policy enforcement and both providers
///
/// This combines dataset execution with policy enforcement and both BM25 and
/// vector index provider support.
#[allow(
    clippy::elidable_lifetime_names,
    reason = "named lifetimes document the 'a/'b relationship between the db ref and context params"
)]
pub async fn execute_with_dataset_and_policy_and_providers<'a, 'b>(
    db: GraphDbRef<'a>,
    vars: &VarRegistry,
    query: &ExecutableQuery,
    params: QueryContextParams<'a, 'b>,
) -> Result<Vec<Batch>> {
    let prepared = prepare_execution(db, query).await?;
    execute_prepared_with_dataset_and_policy_and_providers(db, vars, prepared, params).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::{Expression, FilterValue, Pattern};
    use crate::options::QueryOptions;
    use crate::parse::{ParsedQuery, QueryOutput};
    use crate::planner::reorder_patterns;
    use crate::sort::SortSpec;
    use crate::triple::{Ref, Term, TriplePattern};
    use crate::var_registry::VarId;
    use fluree_db_core::{FlakeValue, LedgerSnapshot, NoOverlay, PropertyStatData, Sid, StatsView};
    use fluree_graph_json_ld::ParsedContext;
    use std::collections::HashSet;
    use where_plan::collect_inner_join_block;

    fn make_test_snapshot() -> LedgerSnapshot {
        LedgerSnapshot::genesis("test/main")
    }

    fn make_pattern(s_var: VarId, p_name: &str, o_var: VarId) -> TriplePattern {
        TriplePattern::new(
            Ref::Var(s_var),
            Ref::Sid(Sid::new(100, p_name)),
            Term::Var(o_var),
        )
    }

    #[tokio::test]
    async fn test_empty_patterns_returns_one_row() {
        let snapshot = make_test_snapshot();
        let vars = VarRegistry::new();
        let db = GraphDbRef::new(&snapshot, 0, &NoOverlay, snapshot.t);

        let query = ParsedQuery {
            context: ParsedContext::default(),
            orig_context: None,
            output: QueryOutput::Wildcard,
            patterns: vec![],
            options: QueryOptions::default(),
            graph_select: None,
            post_values: None,
        };
        let executable = ExecutableQuery::simple(query);
        let results = execute_with_overlay(db, &vars, &executable).await.unwrap();

        // Empty WHERE returns 1 batch with a single empty solution
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].schema().len(), 0); // Empty schema
        assert_eq!(results[0].len(), 1);
    }

    #[tokio::test]
    async fn test_query_options_builder() {
        let opts = QueryOptions::new()
            .with_limit(10)
            .with_offset(5)
            .with_distinct()
            .with_order_by(vec![SortSpec::asc(VarId(0))]);

        assert_eq!(opts.limit, Some(10));
        assert_eq!(opts.offset, Some(5));
        assert!(opts.distinct);
        assert_eq!(opts.order_by.len(), 1);
        assert!(opts.has_modifiers());
    }

    #[tokio::test]
    async fn test_query_options_default_no_modifiers() {
        let opts = QueryOptions::default();
        assert!(!opts.has_modifiers());
    }

    #[test]
    fn test_build_operator_tree_validates_select_vars() {
        let query = ParsedQuery {
            context: ParsedContext::default(),
            orig_context: None,
            output: QueryOutput::select(vec![VarId(99)]), // Variable not in pattern
            patterns: vec![Pattern::Triple(make_pattern(VarId(0), "name", VarId(1)))],
            options: QueryOptions::default(),
            graph_select: None,
            post_values: None,
        };

        let result = build_operator_tree(
            &query,
            &QueryOptions::default(),
            None,
            &crate::temporal_mode::PlanningContext::current(),
        );
        match result {
            Err(e) => assert!(e.to_string().contains("not found")),
            Ok(_) => panic!("Expected error for invalid select var"),
        }
    }

    #[test]
    fn test_build_operator_tree_validates_sort_vars() {
        let query = ParsedQuery {
            context: ParsedContext::default(),
            orig_context: None,
            output: QueryOutput::select(vec![VarId(0)]),
            patterns: vec![Pattern::Triple(make_pattern(VarId(0), "name", VarId(1)))],
            options: QueryOptions::default(),
            graph_select: None,
            post_values: None,
        };

        let options = QueryOptions::new().with_order_by(vec![SortSpec::asc(VarId(99))]); // Invalid var

        let result = build_operator_tree(
            &query,
            &options,
            None,
            &crate::temporal_mode::PlanningContext::current(),
        );
        match result {
            Err(e) => assert!(e.to_string().contains("Sort variable")),
            Ok(_) => panic!("Expected error for invalid sort var"),
        }
    }

    #[test]
    fn test_build_where_operators_single_triple() {
        let patterns = vec![Pattern::Triple(make_pattern(VarId(0), "name", VarId(1)))];

        let result = where_plan::build_where_operators(
            &patterns,
            None,
            None,
            &crate::temporal_mode::PlanningContext::current(),
        );
        assert!(result.is_ok());

        let op = result.unwrap();
        assert_eq!(op.schema(), &[VarId(0), VarId(1)]);
    }

    #[test]
    fn test_build_where_operators_with_filter() {
        let patterns = vec![
            Pattern::Triple(make_pattern(VarId(0), "age", VarId(1))),
            Pattern::Filter(Expression::gt(
                Expression::Var(VarId(1)),
                Expression::Const(FilterValue::Long(18)),
            )),
        ];

        let result = where_plan::build_where_operators(
            &patterns,
            None,
            None,
            &crate::temporal_mode::PlanningContext::current(),
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_collects_and_reorders_triples_across_safe_filter_boundary_with_stats() {
        let score = VarId(0);
        let score_v = VarId(1);
        let concept = VarId(2);

        let patterns = vec![
            Pattern::Triple(make_pattern(score, "hasScore", score_v)),
            Pattern::Filter(Expression::gt(
                Expression::Var(score_v),
                Expression::Const(FilterValue::Double(0.4)),
            )),
            Pattern::Triple(TriplePattern::new(
                Ref::Var(score),
                Ref::Sid(Sid::new(100, "refersInstance")),
                Term::Var(concept),
            )),
            Pattern::Triple(TriplePattern::new(
                Ref::Var(concept),
                Ref::Sid(Sid::new(100, "notation")),
                Term::Value(FlakeValue::String("LVL1".to_string())),
            )),
        ];

        let block = collect_inner_join_block(&patterns, 0);
        assert_eq!(
            block.end_index,
            patterns.len(),
            "block should consume all patterns"
        );
        assert_eq!(block.values.len(), 0, "expected 0 VALUES in the block");
        assert_eq!(block.binds.len(), 0, "expected 0 BINDs in the block");
        assert_eq!(block.triples.len(), 3, "expected 3 triples in the block");
        assert_eq!(block.filters.len(), 1, "expected 1 filter in the block");

        let mut stats = StatsView::default();
        stats.properties.insert(
            Sid::new(100, "notation"),
            PropertyStatData {
                count: 1_000_000,
                ndv_values: 1_000_000,
                ndv_subjects: 1_000_000,
            },
        );
        stats.properties.insert(
            Sid::new(100, "hasScore"),
            PropertyStatData {
                count: 1_000_000_000,
                ndv_values: 900_000_000,
                ndv_subjects: 900_000_000,
            },
        );
        stats.properties.insert(
            Sid::new(100, "refersInstance"),
            PropertyStatData {
                count: 800_000_000,
                ndv_values: 700_000_000,
                ndv_subjects: 700_000_000,
            },
        );

        let patterns: Vec<Pattern> = block.triples.into_iter().map(Pattern::Triple).collect();
        let reordered = reorder_patterns(&patterns, Some(&stats), &HashSet::new());
        let first_triple = match &reordered[0] {
            Pattern::Triple(tp) => tp,
            _ => panic!("expected Triple pattern"),
        };
        let first_pred = first_triple.p.as_sid().expect("predicate should be Sid");
        assert_eq!(
            &*first_pred.name, "notation",
            "expected optimizer to start from the most selective triple"
        );
    }

    #[test]
    fn test_extract_lookahead_bounds_simple_range() {
        let triples = vec![make_pattern(VarId(0), "age", VarId(1))];
        let remaining = vec![Pattern::Filter(Expression::and(vec![
            Expression::gt(
                Expression::Var(VarId(1)),
                Expression::Const(FilterValue::Long(18)),
            ),
            Expression::lt(
                Expression::Var(VarId(1)),
                Expression::Const(FilterValue::Long(65)),
            ),
        ]))];

        let (bounds, _consumed) = extract_lookahead_bounds_with_consumption(&triples, &remaining);

        assert!(bounds.contains_key(&VarId(1)));
        let obj_bounds = bounds.get(&VarId(1)).unwrap();

        assert!(obj_bounds.lower.is_some());
        let (lower_val, lower_inclusive) = obj_bounds.lower.as_ref().unwrap();
        assert_eq!(*lower_val, FlakeValue::Long(18));
        assert!(!lower_inclusive);

        assert!(obj_bounds.upper.is_some());
        let (upper_val, upper_inclusive) = obj_bounds.upper.as_ref().unwrap();
        assert_eq!(*upper_val, FlakeValue::Long(65));
        assert!(!upper_inclusive);
    }

    #[test]
    fn test_merge_lower_bound_takes_higher_value() {
        let a = Some((FlakeValue::Long(10), false));
        let b = Some((FlakeValue::Long(20), false));

        let merged = merge_lower_bound(a.as_ref(), b.as_ref());

        assert!(merged.is_some());
        let (val, _inclusive) = merged.unwrap();
        assert_eq!(val, FlakeValue::Long(20));
    }

    #[test]
    fn test_merge_object_bounds_full() {
        use fluree_db_core::ObjectBounds;

        let a = ObjectBounds {
            lower: Some((FlakeValue::Long(10), false)),
            upper: Some((FlakeValue::Long(100), true)),
        };
        let b = ObjectBounds {
            lower: Some((FlakeValue::Long(20), true)),
            upper: Some((FlakeValue::Long(80), false)),
        };

        let merged = merge_object_bounds(&a, &b);

        let (lower_val, _) = merged.lower.as_ref().unwrap();
        assert_eq!(*lower_val, FlakeValue::Long(20));

        let (upper_val, _) = merged.upper.as_ref().unwrap();
        assert_eq!(*upper_val, FlakeValue::Long(80));
    }
}
