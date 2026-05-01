//! # Fluree DB Query
//!
//! Query execution engine for Fluree DB.
//!
//! This crate provides:
//! - Columnar batch-based execution model
//! - Operator trait with `open/next_batch/close` lifecycle
//! - DatasetOperator for dataset-aware triple-pattern evaluation (multi-graph fanout)
//! - BinaryScanOperator for single-graph scanning (binary cursor + range fallback)
//! - Variable registry for compact binding indices
//!
//! ## Quick Start
//!
//! Build a `TriplePattern` with a `VarRegistry`, then call `execute_pattern` with a `GraphDbRef` to get result batches.

pub mod aggregate;
pub mod binary_history;
pub mod binary_range;
pub mod binary_scan;
pub mod bind;
pub mod binding;
pub mod bm25;
pub mod context;
pub(crate) mod count_plan;
pub(crate) mod count_plan_exec;
pub(crate) mod count_rows;
pub mod datalog_rules;
pub mod dataset;
pub mod dataset_operator;
pub mod dict_overlay;
pub mod distinct;
pub mod error;
pub mod execute;
pub mod exists;
pub mod explain;
pub mod expression;
pub(crate) mod fast_count;
pub(crate) mod fast_exists_join_count_distinct_object;
pub(crate) mod fast_fused_scan_sum;
pub(crate) mod fast_group_count_firsts;
pub(crate) mod fast_label_regex_type;
pub(crate) mod fast_min_max_string;
pub(crate) mod fast_multicolumn_join_count_all;
pub(crate) mod fast_optional_chain_head_count_all;
pub(crate) mod fast_path_common;
pub(crate) mod fast_property_path_plus_count_all;
pub(crate) mod fast_star_const_order_topk;
pub(crate) mod fast_string_prefix_count_all;
pub(crate) mod fast_sum_strlen_group_concat;
pub(crate) mod fast_transitive_path_plus_count_all;
pub(crate) mod fast_union_star_count_all;
pub mod filter;
pub mod geo_rewrite;
pub mod geo_search;
pub mod graph;
pub mod group_aggregate;
pub mod groupby;
pub mod having;
pub mod ir;
pub mod join;
pub mod limit;
pub mod materializer;
pub mod minus;
pub(crate) mod object_binding;
pub mod offset;
pub mod operator;
pub mod optional;
pub mod options;
pub mod parse;
pub mod planner;
pub mod policy;
pub mod project;
pub mod property_join;
pub mod property_path;
pub mod r2rml;
pub mod reasoning;
pub mod remote_service;
pub mod rewrite;
pub mod rewrite_owl_ql;
pub mod s2_search;
pub mod schema_bundle;
pub mod seed;
pub(crate) mod semijoin;
pub mod service;
pub(crate) mod sid_iri;
pub mod sort;
pub mod sparql_results;
pub(crate) mod stats_cache;
pub mod stats_query;
pub mod subquery;
pub mod temporal_mode;
pub mod triple;
pub mod union;
pub mod values;
pub mod var_registry;
pub mod vector;

// Re-exports
pub use aggregate::{apply_aggregate, AggregateFn, AggregateOperator, AggregateSpec};
pub use binary_history::BinaryHistoryScanOperator;
pub use binary_range::BinaryRangeProvider;
pub use binary_scan::BinaryScanOperator;
pub use bind::BindOperator;
pub use binding::{Batch, BatchError, BatchView, Binding, RowAccess, RowView};
pub use context::{ExecutionContext, WellKnownDatatypes};
pub use dataset::{ActiveGraph, ActiveGraphs, DataSet, GraphRef};
pub use dataset_operator::{DatasetBuilder, DatasetOperator, ScanDatasetBuilder};
pub use distinct::DistinctOperator;
pub use error::{QueryError, Result};
pub use execute::{
    build_operator_tree, execute_with_dataset, execute_with_dataset_and_bm25,
    execute_with_dataset_and_policy, execute_with_dataset_and_policy_and_bm25,
    execute_with_dataset_and_policy_and_providers, execute_with_dataset_and_policy_tracked,
    execute_with_dataset_and_providers, execute_with_dataset_history, execute_with_dataset_tracked,
    execute_with_overlay, execute_with_overlay_tracked, execute_with_policy,
    execute_with_policy_tracked, execute_with_r2rml, run_operator, ExecutableQuery,
    QueryContextParams,
};
pub use exists::ExistsOperator;
pub use explain::{
    explain_execution_hints, explain_patterns, ExecutionStrategyHint, ExplainPlan, FallbackReason,
    OptimizationStatus, PatternDisplay, SelectivityInputs,
};
pub use filter::FilterOperator;
pub use geo_rewrite::rewrite_geo_patterns;
pub use graph::GraphOperator;
pub use group_aggregate::{GroupAggregateOperator, StreamingAggSpec};
pub use groupby::GroupByOperator;
pub use having::HavingOperator;
pub use ir::{
    CompareOp, Expression, FilterValue, Function, PathModifier, Pattern, PropertyPathPattern,
    Query, R2rmlPattern, ServiceEndpoint, ServicePattern, SubqueryPattern,
};
pub use join::{BindInstruction, NestedLoopJoinOperator, PatternPosition, UnifyInstruction};
pub use limit::LimitOperator;
pub use materializer::{ComparableValue, JoinKey, Materializer};
pub use minus::MinusOperator;
pub use offset::OffsetOperator;
pub use operator::{BoxedOperator, Operator, OperatorState};
pub use optional::OptionalOperator;
pub use options::QueryOptions;
pub use planner::{
    extract_object_bounds_for_var, extract_range_constraints, is_property_join, PatternType,
    RangeConstraint, RangeValue,
};
pub use policy::{QueryPolicyEnforcer, QueryPolicyExecutor};
pub use project::ProjectOperator;
pub use property_join::PropertyJoinOperator;
pub use property_path::{PropertyPathOperator, DEFAULT_MAX_VISITED};
pub use r2rml::{NoOpR2rmlProvider, R2rmlProvider, R2rmlScanOperator, R2rmlTableProvider};
pub use reasoning::{global_reasoning_cache, ReasoningOverlay};
pub use rewrite::{
    rewrite_patterns, Diagnostics as RewriteDiagnostics, PlanContext, PlanLimits, ReasoningModes,
};
pub use rewrite_owl_ql::{rewrite_owl_ql_patterns, Ontology, OwlQlContext};
pub use seed::{EmptyOperator, SeedOperator};
pub use sort::{compare_bindings, compare_flake_values, SortDirection, SortOperator, SortSpec};
pub use stats_query::StatsCountByPredicateOperator;
pub use subquery::SubqueryOperator;
pub use temporal_mode::{PlanningContext, TemporalMode};
pub use triple::{Ref, Term, TriplePattern};

// Re-export DatatypeConstraint from fluree-db-core for convenience
pub use fluree_db_core::DatatypeConstraint;
pub use union::UnionOperator;
pub use values::ValuesOperator;

// Re-export from fluree-db-core for convenience
pub use fluree_db_core::ObjectBounds;
pub use var_registry::{VarId, VarRegistry};

// Re-export parse types for query parsing
pub use parse::{parse_query, ParsedQuery, QueryOutput};

use execute::build_where_operators_seeded;
use fluree_db_core::GraphDbRef;
use std::sync::Arc;

/// Execute a single triple pattern query
///
/// Returns all batches of results for the pattern.
///
/// # Arguments
///
/// * `db` - Bundled database reference (snapshot + graph id + overlay + as-of time)
/// * `vars` - Variable registry containing the pattern's variables
/// * `pattern` - Triple pattern to match
///
pub async fn execute_pattern(
    db: GraphDbRef<'_>,
    vars: &VarRegistry,
    pattern: TriplePattern,
) -> Result<Vec<Batch>> {
    let ctx = ExecutionContext::from_graph_db_ref(db, vars);
    // Current-state helper: BinaryHistoryScanOperator is single-purpose since the
    // planner-mode refactor (always emits asserts + retracts), so use the
    // current-state scan directly here.
    let mut scan = BinaryScanOperator::new(pattern, None, Vec::new());

    scan.open(&ctx).await?;

    let mut batches = Vec::new();
    while let Some(batch) = scan.next_batch(&ctx).await? {
        batches.push(batch);
    }

    scan.close();
    Ok(batches)
}

/// Execute a pattern and collect all results into a single batch
///
/// Convenience function when you want all results at once.
pub async fn execute_pattern_all(
    db: GraphDbRef<'_>,
    vars: &VarRegistry,
    pattern: TriplePattern,
) -> Result<Option<Batch>> {
    let batches = execute_pattern(db, vars, pattern).await?;

    if batches.is_empty() {
        return Ok(None);
    }

    if batches.len() == 1 {
        return Ok(batches.into_iter().next());
    }

    // Merge multiple batches into one
    let schema_vec = batches[0].schema().to_vec();
    let schema: Arc<[VarId]> = Arc::from(schema_vec.into_boxed_slice());
    let num_cols = schema.len();

    // Merge columns across all batches
    let columns: Vec<Vec<Binding>> = (0..num_cols)
        .map(|col_idx| {
            batches
                .iter()
                .filter_map(|batch| batch.column_by_idx(col_idx))
                .flat_map(|src_col| src_col.iter().cloned())
                .collect()
        })
        .collect();

    Ok(Some(Batch::new(schema, columns)?))
}

/// Execute a pattern with a `GraphDbRef` and an optional `from_t` for history queries.
///
/// `to_t` comes from `db.t`; `from_t` is the lower time bound for history.
pub async fn execute_pattern_at(
    db: GraphDbRef<'_>,
    vars: &VarRegistry,
    pattern: TriplePattern,
    from_t: Option<i64>,
) -> Result<Vec<Batch>> {
    let ctx = ExecutionContext::from_graph_db_ref_with_from_t(db, vars, from_t);
    // Current-state helper: BinaryHistoryScanOperator is single-purpose since the
    // planner-mode refactor (always emits asserts + retracts), so use the
    // current-state scan directly here.
    let mut scan = BinaryScanOperator::new(pattern, None, Vec::new());

    scan.open(&ctx).await?;

    let mut batches = Vec::new();
    while let Some(batch) = scan.next_batch(&ctx).await? {
        batches.push(batch);
    }

    scan.close();
    Ok(batches)
}

/// Execute a pattern against a `GraphDbRef`.
///
/// The `db` bundles snapshot, graph id, overlay (novelty), and as-of time.
/// This replaces the old `execute_pattern_with_overlay` and
/// `execute_pattern_with_overlay_at` functions.
pub async fn execute_pattern_with_overlay(
    db: GraphDbRef<'_>,
    vars: &VarRegistry,
    pattern: TriplePattern,
) -> Result<Vec<Batch>> {
    let ctx = ExecutionContext::from_graph_db_ref(db, vars);
    // Current-state helper: BinaryHistoryScanOperator is single-purpose since the
    // planner-mode refactor (always emits asserts + retracts), so use the
    // current-state scan directly here.
    let mut scan = BinaryScanOperator::new(pattern, None, Vec::new());

    scan.open(&ctx).await?;

    let mut batches = Vec::new();
    while let Some(batch) = scan.next_batch(&ctx).await? {
        batches.push(batch);
    }

    scan.close();
    Ok(batches)
}

/// Execute a pattern with a `GraphDbRef` and an optional `from_t` for history queries.
///
/// `to_t` comes from `db.t`; `from_t` is the lower time bound for history.
pub async fn execute_pattern_with_overlay_at(
    db: GraphDbRef<'_>,
    vars: &VarRegistry,
    pattern: TriplePattern,
    from_t: Option<i64>,
) -> Result<Vec<Batch>> {
    let ctx = ExecutionContext::from_graph_db_ref_with_from_t(db, vars, from_t);
    // Current-state helper: BinaryHistoryScanOperator is single-purpose since the
    // planner-mode refactor (always emits asserts + retracts), so use the
    // current-state scan directly here.
    let mut scan = BinaryScanOperator::new(pattern, None, Vec::new());

    scan.open(&ctx).await?;

    let mut batches = Vec::new();
    while let Some(batch) = scan.next_batch(&ctx).await? {
        batches.push(batch);
    }

    scan.close();
    Ok(batches)
}

/// Execute WHERE patterns with overlay and time-travel support
///
/// This is the entry point for transaction WHERE clause execution.
/// Returns all matching bindings as batches.
///
/// # Arguments
///
/// * `db` - Bundled database reference (snapshot + graph id + overlay + as-of time)
/// * `vars` - Variable registry for the patterns
/// * `patterns` - WHERE patterns to execute
/// * `from_t` - Optional lower time bound for history queries
///
/// # Returns
///
/// Vector of result batches. If patterns is empty, returns a single batch
/// with one empty solution (row with no columns).
///
pub async fn execute_where_with_overlay_at(
    db: GraphDbRef<'_>,
    vars: &VarRegistry,
    patterns: &[Pattern],
    from_t: Option<i64>,
) -> Result<Vec<Batch>> {
    if patterns.is_empty() {
        // Empty WHERE = single empty solution (one row, zero columns)
        let schema: Arc<[VarId]> = Arc::new([]);
        return Ok(vec![Batch::empty(schema)?]);
    }

    let ctx = ExecutionContext::from_graph_db_ref_with_from_t(db, vars, from_t);
    // Root: this is a transaction WHERE-clause executor. Even when `from_t` is
    // set, semantics are "current state at `to_t` with from_t time bound for
    // novelty visibility" — not a history-range query that emits asserts +
    // retracts. Always plan as `Current`.
    let mut operator = build_where_operators_seeded(
        None,
        patterns,
        None,
        None,
        &temporal_mode::PlanningContext::current(),
    )?;

    operator.open(&ctx).await?;
    let mut batches = Vec::new();
    while let Some(batch) = operator.next_batch(&ctx).await? {
        batches.push(batch);
    }
    operator.close();

    Ok(batches)
}

/// Execute WHERE patterns with strict bind error handling.
pub async fn execute_where_with_overlay_at_strict(
    db: GraphDbRef<'_>,
    vars: &VarRegistry,
    patterns: &[Pattern],
    from_t: Option<i64>,
) -> Result<Vec<Batch>> {
    if patterns.is_empty() {
        let schema: Arc<[VarId]> = Arc::new([]);
        return Ok(vec![Batch::empty(schema)?]);
    }

    let ctx =
        ExecutionContext::from_graph_db_ref_with_from_t(db, vars, from_t).with_strict_bind_errors();
    // Root: same as `execute_where_with_overlay_at` — transaction WHERE clause,
    // current-state semantics regardless of `from_t`.
    let mut operator = build_where_operators_seeded(
        None,
        patterns,
        None,
        None,
        &temporal_mode::PlanningContext::current(),
    )?;

    operator.open(&ctx).await?;
    let mut batches = Vec::new();
    while let Some(batch) = operator.next_batch(&ctx).await? {
        batches.push(batch);
    }
    operator.close();

    Ok(batches)
}

/// Execute WHERE patterns with strict bind error handling, optionally providing a runtime dataset.
///
/// When `dataset` is provided, GRAPH patterns like `GRAPH <iri> { ... }` can resolve named
/// graphs by IRI (via `DataSet::named_graph` / `DataSet::has_named_graph`). Without a dataset,
/// GRAPH patterns only execute for:
/// - R2RML graph sources (if configured via `ExecutionContext`)
/// - a graph IRI that matches the db's `ledger_id` alias
pub async fn execute_where_with_overlay_at_strict_in_dataset<'a>(
    db: GraphDbRef<'a>,
    vars: &VarRegistry,
    patterns: &[Pattern],
    from_t: Option<i64>,
    dataset: Option<&'a DataSet<'a>>,
) -> Result<Vec<Batch>> {
    if patterns.is_empty() {
        let schema: Arc<[VarId]> = Arc::new([]);
        return Ok(vec![Batch::empty(schema)?]);
    }

    let mut ctx =
        ExecutionContext::from_graph_db_ref_with_from_t(db, vars, from_t).with_strict_bind_errors();
    if let Some(ds) = dataset {
        ctx = ctx.with_dataset(ds);
    }
    // Root: transaction WHERE clause executor (dataset-aware variant).
    // History-range planning is detected at the dataset/view layer in
    // `view::dataset_query` before prepare runs; this transaction-side path
    // is always current-state.
    let mut operator = build_where_operators_seeded(
        None,
        patterns,
        None,
        None,
        &temporal_mode::PlanningContext::current(),
    )?;

    operator.open(&ctx).await?;
    let mut batches = Vec::new();
    while let Some(batch) = operator.next_batch(&ctx).await? {
        batches.push(batch);
    }
    operator.close();

    Ok(batches)
}

/// Cursor handle over a WHERE operator tree that yields result batches
/// incrementally via [`WhereCursor::next_batch`].
///
/// This is the streaming counterpart to
/// [`execute_where_with_overlay_at_strict_in_dataset`]. Instead of driving
/// the operator to completion and collecting a `Vec<Batch>`, the caller
/// pulls one batch at a time — typically to process (materialize, generate
/// flakes, push into an accumulator) and drop before pulling the next.
/// That keeps transact-side peak memory bounded by one batch plus whatever
/// the caller chooses to retain, independent of total WHERE result size.
///
/// Non-blocking operators (BGP scans, filters, inner joins, union,
/// optional, distinct, limit/offset, project) naturally emit incrementally
/// and benefit directly. Blocking operators (GROUP BY, SORT, some
/// aggregates) still materialize their input internally, but the top-level
/// cursor only surfaces batches as they become available — in the worst
/// case this is equivalent to the eager path.
///
/// The operator is `open`ed on construction and `close`d when the cursor
/// is dropped, or explicitly via [`WhereCursor::close`]. Calling
/// `next_batch` after a `None` return is safe and continues to return
/// `None`.
pub struct WhereCursor<'a> {
    inner: CursorInner<'a>,
}

enum CursorInner<'a> {
    /// Normal case: a real operator tree to drive. Boxed because
    /// `ExecutionContext` is ~300 bytes and dwarfs the other variant;
    /// keeping the enum body small avoids pessimizing every `WhereCursor`.
    Operator(Box<WhereCursorOperator<'a>>),
    /// Empty-patterns case: mirror the eager function's
    /// `vec![Batch::empty(Arc::new([]))]` return by emitting one empty
    /// batch and then signalling end-of-stream.
    SingleEmpty { schema: Arc<[VarId]>, emitted: bool },
}

struct WhereCursorOperator<'a> {
    operator: BoxedOperator,
    ctx: ExecutionContext<'a>,
    closed: bool,
}

impl WhereCursor<'_> {
    /// Pull the next result batch. Returns `Ok(None)` when the operator is
    /// exhausted; further calls continue to return `Ok(None)`.
    pub async fn next_batch(&mut self) -> Result<Option<Batch>> {
        match &mut self.inner {
            CursorInner::Operator(state) => {
                if state.closed {
                    return Ok(None);
                }
                let result = state.operator.next_batch(&state.ctx).await?;
                if result.is_none() {
                    state.operator.close();
                    state.closed = true;
                }
                Ok(result)
            }
            CursorInner::SingleEmpty { schema, emitted } => {
                if *emitted {
                    Ok(None)
                } else {
                    *emitted = true;
                    Ok(Some(Batch::empty(schema.clone())?))
                }
            }
        }
    }

    /// Close the cursor. After this call, `next_batch` returns `Ok(None)`
    /// for every subsequent invocation. Idempotent; called automatically on
    /// drop.
    ///
    /// For the `Operator` variant this closes the underlying operator tree.
    /// For the `SingleEmpty` variant it marks the empty batch as already
    /// emitted, so a caller that closes before pulling the first batch
    /// observes end-of-stream rather than a late empty batch.
    pub fn close(&mut self) {
        match &mut self.inner {
            CursorInner::Operator(state) => {
                if !state.closed {
                    state.operator.close();
                    state.closed = true;
                }
            }
            CursorInner::SingleEmpty { emitted, .. } => {
                *emitted = true;
            }
        }
    }
}

impl Drop for WhereCursor<'_> {
    fn drop(&mut self) {
        self.close();
    }
}

/// Open a streaming WHERE cursor. Same input shape and execution semantics
/// as [`execute_where_with_overlay_at_strict_in_dataset`], but returns a
/// [`WhereCursor`] for per-batch consumption instead of buffering the
/// entire result into a `Vec<Batch>`.
pub async fn execute_where_streaming_in_dataset<'a>(
    db: GraphDbRef<'a>,
    vars: &'a VarRegistry,
    patterns: &[Pattern],
    from_t: Option<i64>,
    dataset: Option<&'a DataSet<'a>>,
) -> Result<WhereCursor<'a>> {
    if patterns.is_empty() {
        let schema: Arc<[VarId]> = Arc::new([]);
        return Ok(WhereCursor {
            inner: CursorInner::SingleEmpty {
                schema,
                emitted: false,
            },
        });
    }

    let mut ctx =
        ExecutionContext::from_graph_db_ref_with_from_t(db, vars, from_t).with_strict_bind_errors();
    if let Some(ds) = dataset {
        ctx = ctx.with_dataset(ds);
    }
    // Root: transaction WHERE clause executor (dataset-aware variant).
    // History-range planning is detected at the dataset/view layer in
    // `view::dataset_query` before prepare runs; this transaction-side path
    // is always current-state.
    let mut operator = build_where_operators_seeded(
        None,
        patterns,
        None,
        None,
        &temporal_mode::PlanningContext::current(),
    )?;
    operator.open(&ctx).await?;
    Ok(WhereCursor {
        inner: CursorInner::Operator(Box::new(WhereCursorOperator {
            operator,
            ctx,
            closed: false,
        })),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_public_api() {
        // Ensure public API is accessible
        let mut vars = VarRegistry::new();
        let s = vars.get_or_insert("?s");
        let o = vars.get_or_insert("?o");

        let _pattern = TriplePattern::new(
            Ref::Var(s),
            Ref::Sid(fluree_db_core::Sid::new(100, "name")),
            Term::Var(o),
        );
    }

    /// Empty-patterns cursor yields one empty batch then end-of-stream,
    /// matching the eager `execute_where_with_overlay_at_strict_in_dataset`
    /// behavior of returning `vec![Batch::empty(schema)?]`.
    #[tokio::test]
    async fn test_streaming_cursor_empty_patterns() {
        use crate::binding::Batch;
        use fluree_db_core::{overlay::NoOverlay, GraphDbRef, LedgerSnapshot};

        let vars = VarRegistry::new();
        let snapshot = LedgerSnapshot::genesis("test:main");
        let overlay = NoOverlay;
        let db = GraphDbRef::new(&snapshot, 0, &overlay, 0);

        let mut cursor = execute_where_streaming_in_dataset(db, &vars, &[], None, None)
            .await
            .expect("cursor");

        let first: Option<Batch> = cursor.next_batch().await.expect("first");
        assert!(first.is_some(), "empty-patterns cursor emits one batch");
        let b = first.unwrap();
        assert_eq!(b.len(), 0);
        assert_eq!(b.schema().len(), 0);

        let second = cursor.next_batch().await.expect("second");
        assert!(second.is_none(), "second call returns None");

        let third = cursor.next_batch().await.expect("third");
        assert!(third.is_none(), "further calls keep returning None");
    }

    /// Explicit close is idempotent and leaves the cursor returning `None`.
    #[tokio::test]
    async fn test_streaming_cursor_close_is_idempotent() {
        use fluree_db_core::{overlay::NoOverlay, GraphDbRef, LedgerSnapshot};

        let vars = VarRegistry::new();
        let snapshot = LedgerSnapshot::genesis("test:main");
        let overlay = NoOverlay;
        let db = GraphDbRef::new(&snapshot, 0, &overlay, 0);

        let mut cursor = execute_where_streaming_in_dataset(db, &vars, &[], None, None)
            .await
            .expect("cursor");

        let _ = cursor.next_batch().await.expect("first");
        cursor.close();
        cursor.close(); // idempotent
        let after = cursor.next_batch().await.expect("after close");
        assert!(after.is_none());
    }

    /// Regression: calling `close()` on a fresh `SingleEmpty` cursor must
    /// suppress the pending empty batch — `next_batch` should return `None`
    /// rather than emitting the empty batch after close. Contradicts an
    /// earlier version that only terminated the `Operator` variant on close.
    #[tokio::test]
    async fn test_streaming_cursor_close_terminates_single_empty() {
        use fluree_db_core::{overlay::NoOverlay, GraphDbRef, LedgerSnapshot};

        let vars = VarRegistry::new();
        let snapshot = LedgerSnapshot::genesis("test:main");
        let overlay = NoOverlay;
        let db = GraphDbRef::new(&snapshot, 0, &overlay, 0);

        let mut cursor = execute_where_streaming_in_dataset(db, &vars, &[], None, None)
            .await
            .expect("cursor");

        // Close BEFORE consuming the pending empty batch.
        cursor.close();
        let after = cursor.next_batch().await.expect("after close");
        assert!(
            after.is_none(),
            "close() must suppress the pending empty batch for SingleEmpty cursors"
        );
    }
}
