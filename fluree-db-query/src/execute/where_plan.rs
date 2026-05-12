//! WHERE clause planning and operator building
//!
//! Builds operators for WHERE clause patterns including:
//! - Triple patterns (DatasetOperator / BinaryScanOperator, NestedLoopJoinOperator)
//! - Filter patterns (FilterOperator)
//! - Optional patterns (OptionalOperator)
//! - Values patterns (ValuesOperator)
//! - Bind patterns (BindOperator)
//! - Union patterns (UnionOperator)
//! - And more...

use crate::binary_scan::EmitMask;
use crate::bind::BindOperator;
use crate::bm25::Bm25SearchOperator;
use crate::distinct::DistinctOperator;
use crate::error::{QueryError, Result};
use crate::eval::PreparedBoolExpression;
use crate::exists::ExistsOperator;
use crate::filter::{contains_exists, FilterOperator};
use crate::ir::triple::{Ref, Term, TriplePattern};
use crate::ir::{Expression, Pattern};
use crate::join::NestedLoopJoinOperator;
use crate::minus::MinusOperator;
use crate::operator::inline::InlineOperator;
use crate::operator::BoxedOperator;
use crate::optional::{GroupedPatternOptionalBuilder, OptionalOperator, PlanTreeOptionalBuilder};
use crate::planner::{analyze_property_join, is_property_join, reorder_patterns};
use crate::property_join::PropertyJoinOperator;
use crate::property_path::{PropertyPathOperator, DEFAULT_MAX_VISITED};
use crate::seed::EmptyOperator;
use crate::semijoin::SemijoinOperator;
use crate::subquery::SubqueryOperator;
use crate::temporal_mode::PlanningContext;
use crate::union::UnionOperator;
use crate::values::ValuesOperator;
use crate::var_registry::VarId;
use fluree_db_core::{IndexType, ObjectBounds, StatsView};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::pushdown::extract_bounds_from_filters;

#[inline]
fn filter_not_bound_var(expr: &Expression) -> Option<VarId> {
    match expr {
        Expression::Call { func, args } if *func == crate::ir::Function::Not && args.len() == 1 => {
            match &args[0] {
                Expression::Call { func, args }
                    if *func == crate::ir::Function::Bound && args.len() == 1 =>
                {
                    match &args[0] {
                        Expression::Var(v) => Some(*v),
                        _ => None,
                    }
                }
                _ => None,
            }
        }
        _ => None,
    }
}

#[inline]
fn pattern_list_contains_var(patterns: &[Pattern], v: VarId) -> bool {
    patterns.iter().any(|p| p.referenced_vars().contains(&v))
}

/// Strategy for executing an EXISTS / NOT EXISTS pattern.
///
/// Picked by [`choose_exists_strategy`] from the outer schema and the inner
/// patterns alone — no operator construction, no stats, no I/O. Kept as a
/// distinct type so unit tests can pin the decision without standing up a
/// full operator tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ExistsStrategy {
    /// Build the inner solution once into a hash set keyed by `key_vars`,
    /// probe per outer row. Used when the inner is correlated to the outer
    /// only via vars the inner itself produces.
    Semijoin { key_vars: Vec<VarId> },
    /// Rebuild and re-execute the inner per outer row. Used when the inner
    /// is uncorrelated (no shared produced vars with outer) or references
    /// outer-only consumed vars (e.g. `FILTER(?p = ?q)` where `?p` is
    /// produced only by the outer).
    Exists {
        /// Reason recorded for tracing / debugging.
        reason: ExistsFallbackReason,
    },
}

/// Why a strategy fell back to per-row `Exists` rather than `Semijoin`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExistsFallbackReason {
    /// Inner shares no produced vars with the outer schema — fully uncorrelated.
    Uncorrelated,
    /// Inner references an outer var it doesn't produce, so it can't run standalone.
    OuterOnlyConsumed,
}

/// Pure decision: pick an [`ExistsStrategy`] from the outer schema and the
/// inner patterns. No side effects, no I/O — unit-testable in isolation.
pub(crate) fn choose_exists_strategy(
    outer_schema: &[VarId],
    inner_patterns: &[Pattern],
) -> ExistsStrategy {
    // Vars PRODUCED by inner patterns (Triple, Bind, Values, etc.).
    // Vars only consumed (e.g. in FILTER expressions) cannot serve as semijoin keys.
    let inner_produced_vars: HashSet<VarId> = inner_patterns
        .iter()
        .flat_map(Pattern::produced_vars)
        .collect();

    // Key vars in outer schema order (stable, matches column layout).
    let key_vars: Vec<VarId> = outer_schema
        .iter()
        .copied()
        .filter(|v| inner_produced_vars.contains(v))
        .collect();

    // Inner references an outer var it doesn't produce (e.g., FILTER(?p = ?q)
    // where ?p is outer-only): inner cannot run standalone → must seed per row.
    let outer_set: HashSet<VarId> = outer_schema.iter().copied().collect();
    let outer_only_consumed = inner_patterns
        .iter()
        .flat_map(Pattern::referenced_vars)
        .any(|v| outer_set.contains(&v) && !inner_produced_vars.contains(&v));

    if outer_only_consumed {
        ExistsStrategy::Exists {
            reason: ExistsFallbackReason::OuterOnlyConsumed,
        }
    } else if key_vars.is_empty() {
        ExistsStrategy::Exists {
            reason: ExistsFallbackReason::Uncorrelated,
        }
    } else {
        ExistsStrategy::Semijoin { key_vars }
    }
}

/// Build the operator for an EXISTS / NOT EXISTS using [`choose_exists_strategy`].
///
/// Picks `SemijoinOperator` (build-once + hash probe) when the inner pattern is
/// correlated to the outer solely via produced vars, otherwise `ExistsOperator`
/// (per-row correlated rebuild). Shared by the pattern-level `Pattern::Exists`
/// / `Pattern::NotExists` dispatch and the `OPTIONAL { ... } FILTER(!bound(?v))`
/// rewrite — the latter relied on `ExistsOperator` unconditionally before this
/// helper existed, which timed out on large outer streams.
fn build_exists_strategy(
    child: BoxedOperator,
    inner_patterns: &[Pattern],
    negated: bool,
    stats: Option<Arc<StatsView>>,
    planning: PlanningContext,
) -> BoxedOperator {
    let strategy = choose_exists_strategy(child.schema(), inner_patterns);
    match strategy {
        ExistsStrategy::Semijoin { key_vars } => {
            tracing::debug!(
                strategy = "semijoin",
                negated,
                key_var_count = key_vars.len(),
                inner_pattern_count = inner_patterns.len(),
                "exists dispatch",
            );
            Box::new(SemijoinOperator::new(
                child,
                inner_patterns.to_vec(),
                key_vars,
                negated,
                stats,
                planning,
            ))
        }
        ExistsStrategy::Exists { reason } => {
            tracing::debug!(
                strategy = "exists",
                negated,
                reason = ?reason,
                inner_pattern_count = inner_patterns.len(),
                "exists dispatch",
            );
            Box::new(ExistsOperator::new(
                child,
                inner_patterns.to_vec(),
                negated,
                stats,
                planning,
            ))
        }
    }
}

fn collect_grouped_single_triple_optionals(
    patterns: &[Pattern],
    start: usize,
    required_schema: &[VarId],
) -> (Vec<TriplePattern>, usize) {
    let mut triples = Vec::new();
    let mut seen_object_vars = HashSet::new();
    let mut i = start;
    let mut subject_var: Option<VarId> = None;

    while let Some(Pattern::Optional(inner_patterns)) = patterns.get(i) {
        if inner_patterns.len() != 1 {
            break;
        }
        let Some(triple) = inner_patterns[0].as_triple().cloned() else {
            break;
        };
        let Some(s_var) = triple.s.as_var() else {
            break;
        };
        let Some(o_var) = triple.o.as_var() else {
            break;
        };
        if triple.dtc.is_some() || !triple.p_bound() {
            break;
        }
        if !required_schema.contains(&s_var) || required_schema.contains(&o_var) {
            break;
        }
        if subject_var.is_none() {
            subject_var = Some(s_var);
        }
        if subject_var != Some(s_var) || !seen_object_vars.insert(o_var) {
            break;
        }
        triples.push(triple);
        i += 1;
    }

    (triples, i)
}

// ============================================================================
// Variable statistics (needed-vars + join participation)
// ============================================================================

#[inline]
fn bump_count(counts: &mut HashMap<VarId, usize>, v: VarId) {
    *counts.entry(v).or_insert(0) += 1;
}

/// Collect variable usage statistics for a pattern list.
///
/// - `counts`: number of times each var appears across patterns (used to keep join vars).
/// - `vars`: set of all vars mentioned anywhere in WHERE (useful for wrappers/tests).
pub fn collect_var_stats(
    patterns: &[Pattern],
    counts: &mut HashMap<VarId, usize>,
    vars: &mut HashSet<VarId>,
) {
    fn walk(patterns: &[Pattern], counts: &mut HashMap<VarId, usize>, vars: &mut HashSet<VarId>) {
        for p in patterns {
            match p {
                Pattern::Triple(tp) => {
                    for v in tp.referenced_vars() {
                        bump_count(counts, v);
                        vars.insert(v);
                    }
                }
                Pattern::Values { vars: vs, .. } => {
                    for v in vs {
                        bump_count(counts, *v);
                        vars.insert(*v);
                    }
                }
                Pattern::Bind { var, expr } => {
                    bump_count(counts, *var);
                    vars.insert(*var);
                    for v in expr.referenced_vars() {
                        bump_count(counts, v);
                        vars.insert(v);
                    }
                }
                Pattern::Filter(expr) => {
                    for v in expr.referenced_vars() {
                        bump_count(counts, v);
                        vars.insert(v);
                    }
                }
                Pattern::Optional(inner)
                | Pattern::Minus(inner)
                | Pattern::Exists(inner)
                | Pattern::NotExists(inner) => walk(inner, counts, vars),
                Pattern::Union(branches) => {
                    for b in branches {
                        walk(b, counts, vars);
                    }
                }
                Pattern::Graph { name, patterns } => {
                    if let crate::ir::GraphName::Var(v) = name {
                        bump_count(counts, *v);
                        vars.insert(*v);
                    }
                    walk(patterns, counts, vars);
                }
                Pattern::Subquery(sq) => {
                    for v in &sq.select {
                        bump_count(counts, *v);
                        vars.insert(*v);
                    }
                    walk(&sq.patterns, counts, vars);
                }
                Pattern::PropertyPath(pp) => {
                    for v in pp.referenced_vars() {
                        bump_count(counts, v);
                        vars.insert(v);
                    }
                }
                _ => {}
            }
        }
    }

    walk(patterns, counts, vars);
}

/// Compute WHERE-level variable stats used for emission pruning.
///
/// Returns:
/// - `var_counts`: how many times each var appears across patterns.
/// - `protected_vars`: vars that must not be pruned from scan output.
#[inline]
fn compute_where_var_stats(
    patterns: &[Pattern],
    needed_vars: &HashSet<VarId>,
) -> (HashMap<VarId, usize>, HashSet<VarId>) {
    let mut counts: HashMap<VarId, usize> = HashMap::new();
    let mut all_vars: HashSet<VarId> = HashSet::new();
    collect_var_stats(patterns, &mut counts, &mut all_vars);

    // Today we treat "needed after WHERE" vars as protected.
    // (Join vars are protected via `var_counts` in `emit_mask_for_triple`.)
    (counts, needed_vars.clone())
}

// ============================================================================
// Inner join block types
// ============================================================================

/// Results of filter bounds pushdown: object bounds extracted from filters and
/// the indices of filters consumed by the pushdown.
///
/// Computed by [`extract_bounds_from_filters`] and threaded into block builders
/// so that consumed filters are not re-applied and object bounds are pushed
/// down into scan operators.
pub struct FilterPushdown {
    pub object_bounds: HashMap<VarId, ObjectBounds>,
    pub consumed_indices: Vec<usize>,
}

/// Augment required_where_vars with a precomputed suffix variable set.
///
/// When projection pushdown is active, each operator must keep variables
/// that are either required by the post-WHERE pipeline (`required_where_vars`)
/// or referenced by subsequent patterns (suffix). This union ensures that
/// operators don't trim variables that later patterns need for correlation.
fn augment_with_suffix(
    required_where_vars: Option<&[VarId]>,
    suffix_vars: &HashSet<VarId>,
) -> Option<Vec<VarId>> {
    required_where_vars.map(|rwv| {
        let mut combined: HashSet<VarId> = rwv.iter().copied().collect();
        combined.extend(suffix_vars);
        combined.into_iter().collect()
    })
}

/// Precompute cumulative suffix variable sets for each pattern position.
///
/// `suffix_vars[j]` = union of all variables from `patterns[j..]`.
/// `suffix_vars[patterns.len()]` = empty (no suffix).
///
/// Only computed when `required_where_vars` is `Some` (projection pushdown active).
fn precompute_suffix_vars(patterns: &[Pattern]) -> Vec<HashSet<VarId>> {
    let n = patterns.len();
    let mut sets = vec![HashSet::new(); n + 1];
    for j in (0..n).rev() {
        sets[j] = sets[j + 1].clone();
        sets[j].extend(patterns[j].referenced_vars());
    }
    sets
}

/// A single VALUES pattern: its bound variables and constant rows.
#[derive(Debug, Clone)]
pub struct ValuesPattern {
    pub vars: Vec<VarId>,
    pub rows: Vec<Vec<crate::binding::Binding>>,
}

impl ValuesPattern {
    pub fn new(vars: Vec<VarId>, rows: Vec<Vec<crate::binding::Binding>>) -> Self {
        Self { vars, rows }
    }
}

/// A single BIND pattern: target variable, defining expression, and the set of
/// variables the expression depends on.
///
/// `required_vars` is computed at construction time and used during operator
/// building to determine when this BIND can be applied (all dependencies bound).
#[derive(Debug, Clone)]
pub struct BindPattern {
    /// Variables that must be bound before this BIND can execute
    pub required_vars: HashSet<VarId>,
    /// The variable being bound by this expression
    pub var: VarId,
    /// The expression to evaluate
    pub expr: Expression,
}

impl BindPattern {
    /// Construct a `BindPattern` only when all the expression's variables are
    /// already present in `bound_vars`. Returns `None` when the expression
    /// depends on variables not yet bound, avoiding the `expr.clone()` in
    /// that case.
    ///
    /// `required_vars` is computed once from the expression and, on success,
    /// moved directly into the resulting `BindPattern`.
    pub fn when_eligible(
        var: VarId,
        expr: &Expression,
        bound_vars: &HashSet<VarId>,
    ) -> Option<Self> {
        let required_vars: HashSet<VarId> = expr.referenced_vars().into_iter().collect();
        required_vars.is_subset(bound_vars).then(|| Self {
            required_vars,
            var,
            expr: expr.clone(),
        })
    }
}

/// A single FILTER pattern: expression, its required variables, and its
/// original index within the block (used for pushdown tracking).
///
/// `required_vars` is computed at construction time and used during operator
/// building to determine when this filter can be applied (all dependencies bound).
/// `original_idx` tracks which filters were consumed by bound pushdown so they
/// can be skipped during operator application.
#[derive(Debug, Clone)]
pub struct FilterPattern {
    /// Original index in the block's filter list (for pushdown tracking)
    pub original_idx: usize,
    /// Variables that must be bound before this filter can execute
    pub required_vars: HashSet<VarId>,
    /// The filter expression to evaluate
    pub expr: Expression,
}

impl FilterPattern {
    pub fn new(original_idx: usize, expr: Expression) -> Self {
        let required_vars = expr.referenced_vars().into_iter().collect();
        Self {
            original_idx,
            required_vars,
            expr,
        }
    }
}

/// Result of collecting an inner-join block from a pattern list.
///
/// Contains all the components needed to build a joined block of patterns.
pub struct InnerJoinBlock {
    /// Index past the last consumed pattern
    pub end_index: usize,
    /// VALUES patterns
    pub values: Vec<ValuesPattern>,
    /// Triple patterns
    pub triples: Vec<TriplePattern>,
    /// BIND patterns
    pub binds: Vec<BindPattern>,
    /// FILTER patterns
    pub filters: Vec<FilterPattern>,
}

#[derive(Default, Clone)]
pub(crate) struct PropertyJoinTail {
    end_index: usize,
    pub optional_triples: Vec<TriplePattern>,
    pub binds: Vec<BindPattern>,
    pub filters: Vec<FilterPattern>,
}

#[derive(Debug, Clone)]
pub(crate) struct PropertyJoinPlanDecision {
    pub analysis: crate::planner::PropertyJoinAnalysis,
    pub width_score: f32,
    pub optional_bonus: f32,
    pub meets_width_threshold: bool,
    pub has_upstream_seed: bool,
    pub can_property_join: bool,
    pub tail_optional_triples: usize,
    pub tail_filters: usize,
    pub tail_binds: usize,
}

/// Minimum weighted star width required before choosing the property-join path.
///
/// Required triples count as 1.0 each. Adjacent same-subject OPTIONAL triples
/// count partially because they still represent future subject-correlated work,
/// but they do not shrink the required-side candidate set.
pub(crate) const PROPERTY_JOIN_MIN_WIDTH_SCORE: f32 = 3.0;
const OPTIONAL_STAR_WIDTH_WEIGHT: f32 = 0.5;

fn optional_star_width_bonus(inner_patterns: &[Pattern], subject_var: VarId) -> f32 {
    let mut triple_count = 0usize;
    for pattern in inner_patterns {
        match pattern {
            Pattern::Triple(tp) if tp.s.as_var() == Some(subject_var) && tp.p_bound() => {
                triple_count += 1;
            }
            Pattern::Filter(_) | Pattern::Bind { .. } | Pattern::Values { .. } => {}
            _ => return 0.0,
        }
    }
    OPTIONAL_STAR_WIDTH_WEIGHT * triple_count as f32
}

pub(crate) fn property_join_width_score(
    required_triples: &[TriplePattern],
    trailing_patterns: &[Pattern],
) -> (f32, f32) {
    let required_score = required_triples.len() as f32;
    let Some(subject_var) = required_triples.first().and_then(|tp| tp.s.as_var()) else {
        return (required_score, 0.0);
    };

    let mut optional_bonus = 0.0;
    for pattern in trailing_patterns {
        match pattern {
            Pattern::Optional(inner_patterns) => {
                optional_bonus += optional_star_width_bonus(inner_patterns, subject_var);
            }
            _ => break,
        }
    }

    (required_score + optional_bonus, optional_bonus)
}

pub(crate) fn collect_property_join_tail(
    patterns: &[Pattern],
    start: usize,
    required_triples: &[TriplePattern],
) -> PropertyJoinTail {
    let Some(subject_var) = required_triples.first().and_then(|tp| tp.s.as_var()) else {
        return PropertyJoinTail {
            end_index: start,
            ..PropertyJoinTail::default()
        };
    };

    let mut out = PropertyJoinTail {
        end_index: start,
        ..PropertyJoinTail::default()
    };
    let mut i = start;
    let mut combined = required_triples.to_vec();
    let mut available_required_vars: HashSet<VarId> = required_triples
        .iter()
        .flat_map(crate::ir::triple::TriplePattern::produced_vars)
        .collect();

    while let Some(Pattern::Optional(inner_patterns)) = patterns.get(i) {
        if inner_patterns.len() != 1 {
            break;
        }
        let Some(optional_triple) = inner_patterns[0].as_triple().cloned() else {
            break;
        };
        if optional_triple.s.as_var() != Some(subject_var) || !optional_triple.p_bound() {
            break;
        }

        combined.push(optional_triple.clone());
        if !analyze_property_join(&combined).eligible() {
            break;
        }

        out.optional_triples.push(optional_triple);
        i += 1;
    }

    while i < patterns.len() {
        match &patterns[i] {
            Pattern::Filter(expr) => {
                let required: HashSet<VarId> = expr.referenced_vars().into_iter().collect();
                if !required.is_subset(&available_required_vars) {
                    break;
                }
                out.filters.push(FilterPattern::new(
                    usize::MAX - out.filters.len(),
                    expr.clone(),
                ));
                i += 1;
            }
            Pattern::Bind { var, expr } => {
                let Some(bind) = BindPattern::when_eligible(*var, expr, &available_required_vars)
                else {
                    break;
                };
                available_required_vars.insert(*var);
                out.binds.push(bind);
                i += 1;
            }
            _ => break,
        }
    }

    out.end_index = i;
    out
}

pub(crate) fn analyze_property_join_plan(
    patterns: &[Pattern],
    block_end_index: usize,
    triples_for_exec: &[TriplePattern],
    has_upstream_seed: bool,
) -> (PropertyJoinPlanDecision, PropertyJoinTail) {
    let analysis = analyze_property_join(triples_for_exec);
    let (width_score, optional_bonus) =
        property_join_width_score(triples_for_exec, &patterns[block_end_index..]);
    let meets_width_threshold = width_score >= PROPERTY_JOIN_MIN_WIDTH_SCORE;
    let can_property_join = !has_upstream_seed && analysis.eligible() && meets_width_threshold;
    let tail = if can_property_join {
        collect_property_join_tail(patterns, block_end_index, triples_for_exec)
    } else {
        PropertyJoinTail {
            end_index: block_end_index,
            ..PropertyJoinTail::default()
        }
    };
    let decision = PropertyJoinPlanDecision {
        analysis,
        width_score,
        optional_bonus,
        meets_width_threshold,
        has_upstream_seed,
        can_property_join,
        tail_optional_triples: tail.optional_triples.len(),
        tail_filters: tail.filters.len(),
        tail_binds: tail.binds.len(),
    };
    (decision, tail)
}

/// Require a child operator, returning an error if None.
#[inline]
fn require_child(operator: Option<BoxedOperator>, pattern_name: &str) -> Result<BoxedOperator> {
    operator
        .ok_or_else(|| QueryError::InvalidQuery(format!("{pattern_name} has no input operator")))
}

/// Get an operator or create an empty seed if None.
///
/// Used for patterns that can appear at position 0 and need an initial solution.
#[inline]
fn get_or_empty_seed(operator: Option<BoxedOperator>) -> BoxedOperator {
    operator.unwrap_or_else(|| Box::new(EmptyOperator::new()))
}

/// Get bound variables from an operator's schema.
#[inline]
fn bound_vars_from_operator(operator: &Option<BoxedOperator>) -> HashSet<VarId> {
    operator
        .as_ref()
        .map(|op| op.schema().iter().copied().collect())
        .unwrap_or_default()
}

/// Apply VALUES patterns on top of an existing operator.
///
/// Each VALUES pattern wraps the current operator with a `ValuesOperator`,
/// creating an empty seed if no operator exists yet.
fn apply_values(
    operator: Option<BoxedOperator>,
    block_values: Vec<ValuesPattern>,
) -> Option<BoxedOperator> {
    let mut operator = operator;
    for vp in block_values {
        let child = get_or_empty_seed(operator.take());
        operator = Some(Box::new(ValuesOperator::new(child, vp.vars, vp.rows)));
    }
    operator
}

/// Partition filters into those eligible for inline evaluation and those still waiting.
///
/// Filters consumed by pushdown are silently dropped. Filters whose required
/// variables are all in `bound` are returned as ready expressions (first element);
/// the rest are returned as-is (second element).
fn partition_eligible_filters(
    filters: Vec<FilterPattern>,
    bound: &HashSet<VarId>,
    filter_idxs_consumed: &[usize],
) -> (Vec<Expression>, Vec<FilterPattern>) {
    let mut ready = Vec::new();
    let mut pending = Vec::new();
    for pf in filters {
        if filter_idxs_consumed.contains(&pf.original_idx) {
            continue;
        }
        // Filters containing EXISTS subexpressions cannot be inlined because
        // inline evaluation is synchronous. EXISTS requires async evaluation
        // via FilterOperator's filter_batch_with_exists path.
        if pf.required_vars.is_subset(bound) && !contains_exists(&pf.expr) {
            ready.push(pf.expr);
        } else {
            pending.push(pf);
        }
    }
    (ready, pending)
}

/// Apply eligible BINDs whose required variables are all bound.
///
/// Each ready BIND is fused with any filters that become ready once the BIND's
/// variable enters `bound`.  Returns the updated operator, any BINDs whose
/// dependencies are not yet satisfied, and the remaining filters.
fn apply_eligible_binds(
    mut child: BoxedOperator,
    bound: &mut HashSet<VarId>,
    pending_binds: Vec<BindPattern>,
    mut pending_filters: Vec<FilterPattern>,
    filter_idxs_consumed: &[usize],
) -> (BoxedOperator, Vec<BindPattern>, Vec<FilterPattern>) {
    let mut remaining_binds = Vec::new();

    for pending in pending_binds {
        if pending.required_vars.is_subset(bound) {
            bound.insert(pending.var);

            let (bind_filters, still_pending) =
                partition_eligible_filters(pending_filters, bound, filter_idxs_consumed);
            pending_filters = still_pending;

            child = Box::new(BindOperator::new(
                child,
                pending.var,
                pending.expr,
                bind_filters,
            ));
        } else {
            remaining_binds.push(pending);
        }
    }

    (child, remaining_binds, pending_filters)
}

/// Apply BINDs and FILTERs whose required variables are all bound.
///
/// Returns the updated operator and the remaining items.
#[allow(clippy::too_many_arguments)]
fn apply_deferred_patterns(
    child: BoxedOperator,
    bound: &mut HashSet<VarId>,
    pending_binds: Vec<BindPattern>,
    pending_filters: Vec<FilterPattern>,
    filter_idxs_consumed: &[usize],
    planning: &PlanningContext,
) -> (BoxedOperator, Vec<BindPattern>, Vec<FilterPattern>) {
    let (mut child, remaining_binds, pending_filters) = apply_eligible_binds(
        child,
        bound,
        pending_binds,
        pending_filters,
        filter_idxs_consumed,
    );

    let (ready, remaining_filters) =
        partition_eligible_filters(pending_filters, bound, filter_idxs_consumed);
    for expr in ready {
        child = Box::new(FilterOperator::new_with_planning(child, expr, *planning));
    }

    (child, remaining_binds, remaining_filters)
}

/// Apply all remaining BINDs and FILTERs at the end of a block.
///
/// Filters are fused into each BindOperator when the BIND's variable is the
/// last dependency the filter was waiting on.  Any filters still remaining
/// after all BINDs are applied as standalone FilterOperators.
fn apply_all_remaining(
    child: BoxedOperator,
    pending_binds: Vec<BindPattern>,
    pending_filters: Vec<FilterPattern>,
    filter_idxs_consumed: &[usize],
    planning: &PlanningContext,
) -> BoxedOperator {
    let mut bound: HashSet<VarId> = child.schema().iter().copied().collect();

    let (mut child, _, remaining_filters) = apply_eligible_binds(
        child,
        &mut bound,
        pending_binds,
        pending_filters,
        filter_idxs_consumed,
    );
    for pending in remaining_filters {
        if !filter_idxs_consumed.contains(&pending.original_idx) {
            child = Box::new(FilterOperator::new_with_planning(
                child,
                pending.expr,
                *planning,
            ));
        }
    }
    child
}

/// Build an operator for a single non-block pattern (BIND, VALUES, or Triple).
///
/// Used when `collect_inner_join_block` consumes zero patterns because the
/// current pattern is not safe to hoist. Processes one pattern and returns.
fn build_single_pattern(
    operator: Option<BoxedOperator>,
    pattern: &Pattern,
    var_counts: &HashMap<VarId, usize>,
    protected_vars: &HashSet<VarId>,
    group_by: &[VarId],
    planning: &PlanningContext,
) -> Option<BoxedOperator> {
    match pattern {
        Pattern::Bind { var, expr } => {
            let child = get_or_empty_seed(operator);
            Some(Box::new(BindOperator::new(
                child,
                *var,
                expr.clone(),
                vec![],
            )))
        }
        Pattern::Values { vars, rows } => {
            let child = get_or_empty_seed(operator);
            Some(Box::new(ValuesOperator::new(
                child,
                vars.clone(),
                rows.clone(),
            )))
        }
        Pattern::Triple(tp) => Some(build_scan_or_join(
            operator,
            tp,
            &HashMap::new(),
            Vec::new(),
            None,
            emit_mask_for_triple(tp, var_counts, protected_vars),
            group_by,
            planning,
        )),
        _ => operator,
    }
}

/// Build an operator tree for a property-join-eligible block of triples.
///
/// Constructs a `PropertyJoinOperator` for the triples, then layers deferred
/// VALUES and any ready BINDs/FILTERs on top.
#[allow(clippy::too_many_arguments)]
fn build_property_join_block(
    triples: &[TriplePattern],
    optional_triples: &[TriplePattern],
    block_values: Vec<ValuesPattern>,
    pending_binds: Vec<BindPattern>,
    pending_filters: Vec<FilterPattern>,
    pushdown: &FilterPushdown,
    required_where_vars: Option<&[VarId]>,
    var_counts: &HashMap<VarId, usize>,
    protected_vars: &HashSet<VarId>,
    planning: &PlanningContext,
) -> Result<Option<BoxedOperator>> {
    let mut needed: HashSet<VarId> = HashSet::new();
    if let Some(rwv) = required_where_vars {
        needed.extend(rwv.iter().copied());
    }
    for (v, c) in var_counts {
        if *c > 1 || protected_vars.contains(v) {
            needed.insert(*v);
        }
    }

    let mut available_vars: HashSet<VarId> = triples
        .iter()
        .flat_map(crate::ir::triple::TriplePattern::produced_vars)
        .collect();
    available_vars.extend(
        optional_triples
            .iter()
            .flat_map(crate::ir::triple::TriplePattern::produced_vars),
    );

    let (inline_ops, pending_binds, pending_filters) = build_inline_ops(
        pending_binds,
        pending_filters,
        &available_vars,
        &pushdown.consumed_indices,
    );
    for op in &inline_ops {
        match op {
            InlineOperator::Filter(expr) => needed.extend(expr.referenced_vars()),
            InlineOperator::Bind { var, expr } => {
                needed.insert(*var);
                needed.extend(expr.referenced_vars());
            }
        }
    }

    let property_join = PropertyJoinOperator::new_with_options(
        triples,
        optional_triples,
        pushdown.object_bounds.clone(),
        Some(&needed),
        inline_ops,
        planning.mode(),
    )?;
    let mut operator: Option<BoxedOperator> = Some(Box::new(property_join));

    if !block_values.is_empty() {
        operator = apply_values(operator, block_values);
    }

    let mut bound = bound_vars_from_operator(&operator);
    if let Some(child) = operator.take() {
        let (child, _, _) = apply_deferred_patterns(
            child,
            &mut bound,
            pending_binds,
            pending_filters,
            &pushdown.consumed_indices,
            planning,
        );
        operator = Some(child);
    }

    Ok(operator)
}

/// Build an optimally-ordered sequence of inline operators.
///
/// First inlines any filters already eligible, then delegates to
/// [`inline_chain`] to iteratively inline binds and the filters they unlock.
/// The resulting sequence drops rows at the earliest possible point.
///
/// Returns (inline_operators, remaining_binds, remaining_filters) — remaining
/// items are those whose dependencies are not yet satisfied by `available_vars`.
fn build_inline_ops(
    pending_binds: Vec<BindPattern>,
    pending_filters: Vec<FilterPattern>,
    available_vars: &HashSet<VarId>,
    filter_idxs_consumed: &[usize],
) -> (Vec<InlineOperator>, Vec<BindPattern>, Vec<FilterPattern>) {
    let mut ops = Vec::new();
    let mut available = available_vars.clone();

    let remaining_filters =
        inline_eligible_filters(&mut ops, pending_filters, &available, filter_idxs_consumed);

    let (remaining_binds, remaining_filters) = inline_chain(
        &mut ops,
        pending_binds,
        remaining_filters,
        &mut available,
        filter_idxs_consumed,
    );

    (ops, remaining_binds, remaining_filters)
}

/// Inline filters whose required variables are already available.
fn inline_eligible_filters(
    ops: &mut Vec<InlineOperator>,
    pending_filters: Vec<FilterPattern>,
    available: &HashSet<VarId>,
    filter_idxs_consumed: &[usize],
) -> Vec<FilterPattern> {
    let (ready, remaining) =
        partition_eligible_filters(pending_filters, available, filter_idxs_consumed);
    for expr in ready {
        ops.push(InlineOperator::Filter(PreparedBoolExpression::new(expr)));
    }
    remaining
}

/// Iteratively inline binds and the filters they unlock.
///
/// Loops until no more binds can be inlined. Each eligible bind is added,
/// then [`inline_eligible_filters`] runs to pick up any filters the new
/// variable made eligible. Returns the binds and filters that remain.
fn inline_chain(
    ops: &mut Vec<InlineOperator>,
    pending_binds: Vec<BindPattern>,
    pending_filters: Vec<FilterPattern>,
    available: &mut HashSet<VarId>,
    filter_idxs_consumed: &[usize],
) -> (Vec<BindPattern>, Vec<FilterPattern>) {
    let mut remaining_binds = pending_binds;
    let mut remaining_filters = pending_filters;
    let mut changed = true;
    while changed {
        changed = false;
        let mut still_pending = Vec::new();
        for bind in remaining_binds {
            if bind.required_vars.is_subset(available) {
                available.insert(bind.var);
                ops.push(InlineOperator::Bind {
                    var: bind.var,
                    expr: bind.expr,
                });
                changed = true;

                remaining_filters = inline_eligible_filters(
                    ops,
                    remaining_filters,
                    available,
                    filter_idxs_consumed,
                );
            } else {
                still_pending.push(bind);
            }
        }
        remaining_binds = still_pending;
    }

    (remaining_binds, remaining_filters)
}

/// Build an operator tree for a sequential scan/join block of triples.
///
/// Applies VALUES first (if any), then iterates triples building scan/join
/// operators, inlining eligible filters and binds into each step and applying
/// deferred BINDs/FILTERs as their dependencies become bound.
#[allow(clippy::too_many_arguments)]
fn build_sequential_join_block(
    operator: Option<BoxedOperator>,
    triples: &[TriplePattern],
    block_values: Vec<ValuesPattern>,
    pending_binds: Vec<BindPattern>,
    pending_filters: Vec<FilterPattern>,
    pushdown: &FilterPushdown,
    required_where_vars: Option<&[VarId]>,
    var_counts: &HashMap<VarId, usize>,
    protected_vars: &HashSet<VarId>,
    group_by: &[VarId],
    distinct_query: bool,
    planning: &PlanningContext,
) -> Result<Option<BoxedOperator>> {
    let mut operator = operator;

    if !block_values.is_empty() {
        operator = apply_values(operator, block_values);
    }

    let mut bound = bound_vars_from_operator(&operator);
    let mut pending_binds = pending_binds;
    let mut pending_filters = pending_filters;

    // Base required vars from the post-WHERE pipeline (SELECT, ORDER BY, etc.).
    // Computed once; filter/bind vars are added per-step using only remaining items.
    let base_vars: Option<HashSet<VarId>> =
        required_where_vars.map(|rwv| rwv.iter().copied().collect());

    for (k, tp) in triples.iter().enumerate() {
        let mut vars_after: HashSet<VarId> = bound.clone();
        for v in tp.produced_vars() {
            vars_after.insert(v);
        }

        // Build interleaved inline operators: eligible filters first, then binds
        // whose required vars are all available after this triple, interleaved
        // with the filters each bind unlocks.
        let (inline_ops, remaining_binds, remaining_filters) = build_inline_ops(
            pending_binds,
            pending_filters,
            &vars_after,
            &pushdown.consumed_indices,
        );
        pending_binds = remaining_binds;
        pending_filters = remaining_filters;

        // Compute live vars using only REMAINING filters and binds (after inline
        // consumption). Inline-consumed and previously deferred/fused items no
        // longer contribute to liveness.
        let live_vars = base_vars.as_ref().map(|base| {
            let mut live: HashSet<VarId> = base.clone();
            live.extend(
                triples[k + 1..]
                    .iter()
                    .flat_map(crate::ir::triple::TriplePattern::referenced_vars),
            );
            live.extend(
                pending_filters
                    .iter()
                    .flat_map(|f| f.expr.referenced_vars()),
            );
            live.extend(pending_binds.iter().flat_map(|b| b.expr.referenced_vars()));
            live.into_iter().collect::<Vec<VarId>>()
        });

        let pruned_vars: Option<HashSet<VarId>> = if distinct_query {
            live_vars.as_ref().map(|live| {
                let live_set: HashSet<VarId> = live.iter().copied().collect();
                vars_after
                    .iter()
                    .copied()
                    .filter(|v| !live_set.contains(v))
                    .collect()
            })
        } else {
            None
        };

        let emit = emit_mask_for_triple(tp, var_counts, protected_vars);
        let op = build_scan_or_join(
            operator,
            tp,
            &pushdown.object_bounds,
            inline_ops,
            live_vars.as_deref(),
            emit,
            group_by,
            planning,
        );
        bound.extend(op.schema().iter().copied());
        operator = Some(op);

        if let Some(child) = operator.take() {
            let (child, new_binds, new_filters) = apply_deferred_patterns(
                child,
                &mut bound,
                pending_binds,
                pending_filters,
                &pushdown.consumed_indices,
                planning,
            );
            pending_binds = new_binds;
            pending_filters = new_filters;
            operator = if distinct_query && pruned_vars.as_ref().is_some_and(|s| !s.is_empty()) {
                Some(Box::new(DistinctOperator::new(child)))
            } else {
                Some(child)
            };
        }
    }

    if !pending_binds.is_empty() || !pending_filters.is_empty() {
        let child = require_child(operator, "Filters")?;
        operator = Some(apply_all_remaining(
            child,
            pending_binds,
            pending_filters,
            &pushdown.consumed_indices,
            planning,
        ));
    }

    Ok(operator)
}

/// Test-only convenience wrapper for WHERE planning.
///
/// Treats all WHERE-bound variables as needed and does not provide GROUP BY hints.
#[cfg(test)]
pub fn build_where_operators(
    patterns: &[Pattern],
    stats: Option<Arc<StatsView>>,
    required_where_vars: Option<&[VarId]>,
    planning: &PlanningContext,
) -> Result<BoxedOperator> {
    let mut needed: HashSet<VarId> = HashSet::new();
    let mut counts: HashMap<VarId, usize> = HashMap::new();
    collect_var_stats(patterns, &mut counts, &mut needed);
    needed.extend(counts.keys().copied());
    build_where_operators_with_needed(
        patterns,
        stats,
        &needed,
        &[],
        false,
        required_where_vars,
        planning,
    )
}

/// Build WHERE operators with explicit needed-vars and GROUP BY keys.
///
/// `needed_vars` are the variables that must survive the WHERE stage (because they
/// are used by GROUP BY, aggregates, HAVING, ORDER BY, projection, etc.).
pub fn build_where_operators_with_needed(
    patterns: &[Pattern],
    stats: Option<Arc<StatsView>>,
    needed_vars: &HashSet<VarId>,
    group_by: &[VarId],
    distinct_query: bool,
    required_where_vars: Option<&[VarId]>,
    planning: &PlanningContext,
) -> Result<BoxedOperator> {
    build_where_operators_seeded_with_needed(
        None,
        patterns,
        stats,
        needed_vars,
        group_by,
        distinct_query,
        required_where_vars,
        planning,
    )
}

/// Collect an optimizable inner-join block consisting of:
/// - `VALUES` (SPARQL VALUES)
/// - `Triple`
/// - `BIND` (when safe - all referenced variables already bound)
/// - `FILTER`s (all filters, regardless of variable binding status)
///
/// FILTERs are collected unconditionally. Filters referencing variables not yet bound
/// in left-to-right order will be applied later when their required variables become
/// bound. This allows users to write FILTER patterns anywhere in the WHERE clause —
/// the system automatically moves each filter to execute immediately after all of its
/// required variables are bound.
///
/// A BIND is considered safe to include in the block if **all** variables referenced
/// by its expression are already bound by preceding patterns (original order). BINDs
/// with unbound inputs cannot be deferred - they must fail at their original position.
///
/// `VALUES` is always safe to include because it is an inner-join constraint/seed.
pub fn collect_inner_join_block(patterns: &[Pattern], start: usize) -> InnerJoinBlock {
    let mut i = start;
    let mut values: Vec<ValuesPattern> = Vec::new();
    let mut triples: Vec<TriplePattern> = Vec::new();
    let mut binds: Vec<BindPattern> = Vec::new();
    let mut filters: Vec<FilterPattern> = Vec::new();
    let mut bound_vars: HashSet<VarId> = HashSet::new();

    while i < patterns.len() {
        match &patterns[i] {
            Pattern::Values { vars, rows } => {
                // VALUES binds its vars immediately (join seed/constraint).
                bound_vars.extend(vars.iter().copied());
                values.push(ValuesPattern::new(vars.clone(), rows.clone()));
                i += 1;
            }
            Pattern::Triple(tp) => {
                // Triples add bindings (subject/object vars) to the local bound set.
                bound_vars.extend(tp.produced_vars());
                triples.push(tp.clone());
                i += 1;
            }
            Pattern::Bind { var, expr } => {
                if let Some(bind) = BindPattern::when_eligible(*var, expr, &bound_vars) {
                    bound_vars.insert(*var);
                    binds.push(bind);
                    i += 1;
                } else {
                    // Unsafe to move this BIND: it depends on vars not yet bound.
                    break;
                }
            }
            Pattern::Filter(expr) => {
                // Collect all filters unconditionally. Filters referencing variables
                // not yet bound will be applied later when their required variables
                // become bound (after subsequent triples). This allows users to write
                // FILTER patterns anywhere in the WHERE clause — the system automatically
                // moves each filter to execute immediately after all of its required
                // variables are bound.
                filters.push(FilterPattern::new(filters.len(), expr.clone()));
                i += 1;
            }
            _ => break,
        }
    }

    InnerJoinBlock {
        end_index: i,
        values,
        triples,
        binds,
        filters,
    }
}

/// Build WHERE operators with an optional initial seed operator (back-compat wrapper).
///
/// Treats all WHERE-bound vars as needed and does not provide GROUP BY hints.
pub fn build_where_operators_seeded(
    seed: Option<BoxedOperator>,
    patterns: &[Pattern],
    stats: Option<Arc<StatsView>>,
    required_where_vars: Option<&[VarId]>,
    planning: &PlanningContext,
) -> Result<BoxedOperator> {
    let mut needed: HashSet<VarId> = HashSet::new();
    let mut counts: HashMap<VarId, usize> = HashMap::new();
    collect_var_stats(patterns, &mut counts, &mut needed);
    needed.extend(counts.keys().copied());
    build_where_operators_seeded_with_needed(
        seed,
        patterns,
        stats,
        &needed,
        &[],
        false,
        required_where_vars,
        planning,
    )
}

/// Build WHERE operators with an optional initial seed operator and explicit needed-vars + GROUP BY keys.
///
/// Handles all pattern types: Triple, VALUES, BIND, FILTER, OPTIONAL, UNION,
/// MINUS, EXISTS, PropertyPath, Subquery, and search patterns.
///
/// Contiguous runs of Triple/VALUES/BIND/FILTER patterns are collected into
/// inner-join blocks by [`collect_inner_join_block`], then built as either a
/// `PropertyJoinOperator` or a sequential scan/join chain. All other patterns
/// (OPTIONAL, UNION, MINUS, etc.) are processed one at a time.
///
/// Pattern reordering is applied upfront via [`reorder_patterns`] using
/// selectivity-based cost estimation.
///
/// - If `seed` is `Some`, it is used as the starting operator for the pattern list.
/// - If `seed` is `None` and the first pattern is non-triple, an `EmptyOperator` is used.
/// - `stats` provides property/class statistics for selectivity-based pattern reordering.
#[allow(clippy::too_many_arguments)]
pub fn build_where_operators_seeded_with_needed(
    seed: Option<BoxedOperator>,
    patterns: &[Pattern],
    stats: Option<Arc<StatsView>>,
    needed_vars: &HashSet<VarId>,
    group_by: &[VarId],
    distinct_query: bool,
    required_where_vars: Option<&[VarId]>,
    planning: &PlanningContext,
) -> Result<BoxedOperator> {
    // `planning` is captured here and threaded into every operator that
    // builds late subplans (UNION, OPTIONAL, EXISTS, MINUS, GRAPH, SERVICE,
    // SUBQUERY, FILTER EXISTS, scan/join chain). The invariant: late
    // builders never call `PlanningContext::current()` themselves —
    // they capture the value passed here.
    if patterns.is_empty() {
        // Empty patterns = one row with empty schema
        return Ok(seed.unwrap_or_else(|| Box::new(EmptyOperator::new())));
    }

    // Apply generalized pattern reordering upfront for all pattern lists.
    //
    // reorder_patterns determines optimal placement of all patterns
    // (triples, compound patterns like UNION/OPTIONAL/MINUS/EXISTS/Subquery)
    // using selectivity-based cost estimation. This subsumes the per-block
    // reorder_patterns_seeded calls that previously handled triple-only blocks.
    let initial_bound = seed
        .as_ref()
        .map(|op| op.schema().iter().copied().collect::<HashSet<_>>())
        .unwrap_or_default();
    let reordered_storage = reorder_patterns(patterns, stats.as_deref(), &initial_bound);
    let patterns = &reordered_storage;

    // Compute variable stats for emission pruning and join heuristics.
    let (var_counts, protected_vars) = compute_where_var_stats(patterns, needed_vars);

    // If no explicit seed, determine if we need an empty seed.
    //
    // We only need a synthetic empty seed when the first pattern *requires* an upstream
    // operator. Triples/VALUES/BIND can build their own empty seed on demand via
    // `get_or_empty_seed(...)`, and pre-inserting an EmptyOperator would incorrectly
    // block PropertyJoinOperator selection (because `operator.is_none()` would be false).
    let needs_empty_seed = seed.is_none()
        && !matches!(
            patterns.first(),
            Some(Pattern::Triple(_) | Pattern::Values { .. } | Pattern::Bind { .. })
        );

    // Start with provided seed, else start with empty operator if needed
    let mut operator: Option<BoxedOperator> = if let Some(seed) = seed {
        Some(seed)
    } else if needs_empty_seed {
        Some(Box::new(EmptyOperator::new()))
    } else {
        None
    };

    // Precompute suffix variable sets for O(1) augmentation at each pattern.
    let empty_suffix = HashSet::new();
    let suffix_vars = required_where_vars
        .map(|_| precompute_suffix_vars(patterns))
        .unwrap_or_default();
    // Helper: compute augmented required vars for position j.
    let augmented_at = |pos: usize| -> Option<Vec<VarId>> {
        augment_with_suffix(
            required_where_vars,
            suffix_vars.get(pos).unwrap_or(&empty_suffix),
        )
    };

    let mut i = 0;
    while i < patterns.len() {
        match &patterns[i] {
            Pattern::Triple(_) | Pattern::Values { .. } | Pattern::Bind { .. } => {
                let start = i;
                let block = collect_inner_join_block(patterns, start);
                let end = block.end_index;

                // `collect_inner_join_block` may consume *zero* patterns when the
                // current pattern is not safe to hoist (e.g. BIND with unbound vars).
                // Fall back to processing one pattern to ensure `i` advances.
                if end == start {
                    operator = build_single_pattern(
                        operator.take(),
                        &patterns[start],
                        &var_counts,
                        &protected_vars,
                        group_by,
                        planning,
                    );
                    i = start + 1;
                    continue;
                }
                i = end;

                // Hot path: triples only (no BIND/FILTER).
                // Skip dependency bookkeeping entirely.
                if block.binds.is_empty() && block.filters.is_empty() {
                    let augmented_rwv = augmented_at(end);
                    let augmented_ref = augmented_rwv.as_deref();

                    // Preserve property-join eligibility when a top-level VALUES precedes
                    // a pure star block. Wrapping VALUES first seeds the schema/operator and
                    // prevents `build_triple_operators()` from taking the property-join path.
                    let values_after_triples = operator.is_none()
                        && !block.values.is_empty()
                        && block.triples.len() >= 2
                        && is_property_join(&block.triples);

                    let mut built = build_triple_operators(
                        if values_after_triples {
                            operator.take()
                        } else {
                            apply_values(operator.take(), block.values.clone())
                        },
                        &block.triples,
                        &HashMap::new(),
                        augmented_ref,
                        &var_counts,
                        &protected_vars,
                        group_by,
                        distinct_query,
                        planning,
                    )?;
                    if values_after_triples {
                        built = apply_values(Some(built), block.values)
                            .expect("apply_values should preserve operator");
                    }
                    operator = Some(built);
                    continue;
                }

                let pending_binds = block.binds;
                let pending_filters = block.filters;

                // Push down range-safe filters into object bounds (when possible).
                let filters_for_pushdown: Vec<Expression> =
                    pending_filters.iter().map(|f| f.expr.clone()).collect();
                let (object_bounds, consumed_indices) =
                    extract_bounds_from_filters(&block.triples, &filters_for_pushdown);
                let pushdown = FilterPushdown {
                    object_bounds,
                    consumed_indices,
                };

                // Prefer starting from a range-bounded triple *only when it is not
                // materially less selective than the current first triple*.
                //
                // `reorder_patterns(...)` runs before bounds extraction, so it cannot
                // account for FILTER-derived object bounds (e.g. `?year <= 1940`).
                // When such bounds exist, starting from the bounded triple can reduce
                // the join domain by orders of magnitude and prevent ORDER BY from
                // buffering a huge intermediate.
                //
                // However, naively forcing a bounded triple to the front can be
                // catastrophic when the bounded triple is still a broad predicate scan
                // and another triple is already highly selective (e.g. a bound-object
                // lookup that would bind the join domain first).
                let mut triples_for_exec: Vec<TriplePattern> = block.triples.clone();

                if !pushdown.object_bounds.is_empty() && triples_for_exec.len() >= 2 {
                    // Existing static-bounds nudge: promote bounded triple to the front only
                    // when it is not less selective than the current first triple.
                    let first_has_bounds = triples_for_exec[0]
                        .o
                        .as_var()
                        .is_some_and(|v| pushdown.object_bounds.contains_key(&v));

                    if !first_has_bounds {
                        let bound_vars: HashSet<VarId> = operator
                            .as_ref()
                            .map(|op| op.schema().iter().copied().collect())
                            .unwrap_or_default();
                        let stats_ref = stats.as_deref();
                        let first_est = crate::planner::estimate_triple_row_count(
                            &triples_for_exec[0],
                            &bound_vars,
                            stats_ref,
                        );

                        let mut best_idx: Option<usize> = None;
                        let mut best_est: f64 = f64::INFINITY;
                        for (idx, tp) in triples_for_exec.iter().enumerate().skip(1) {
                            let has_bounds =
                                tp.o.as_var()
                                    .is_some_and(|v| pushdown.object_bounds.contains_key(&v));
                            if !has_bounds {
                                continue;
                            }
                            let est = crate::planner::estimate_triple_row_count(
                                tp,
                                &bound_vars,
                                stats_ref,
                            );
                            if est < best_est {
                                best_est = est;
                                best_idx = Some(idx);
                            }
                        }

                        if let Some(i) = best_idx {
                            if best_est <= first_est {
                                triples_for_exec.swap(0, i);
                            }
                        }
                    }
                }

                // Ensure a concrete seed when only BIND/FILTER remain (no triples, no upstream).
                if block.triples.is_empty() && operator.is_none() {
                    operator = Some(Box::new(EmptyOperator::new()));
                }

                let has_upstream_seed = operator.is_some();
                let (property_join_plan, property_join_tail) =
                    analyze_property_join_plan(patterns, end, &triples_for_exec, has_upstream_seed);
                let property_join_end = property_join_tail.end_index;
                let augmented_rwv = augmented_at(property_join_end);
                let augmented_ref = augmented_rwv.as_deref();

                if triples_for_exec.len() >= 2 {
                    tracing::debug!(
                        block_start = start,
                        triple_count = triples_for_exec.len(),
                        has_upstream_seed = property_join_plan.has_upstream_seed,
                        has_values = !block.values.is_empty(),
                        has_object_bounds = !pushdown.object_bounds.is_empty(),
                        has_bound_object_triples =
                            triples_for_exec.iter().any(TriplePattern::o_bound),
                        property_join_eligible = property_join_plan.analysis.eligible(),
                        property_join_enough_patterns = property_join_plan.analysis.enough_patterns,
                        property_join_subject_is_var = property_join_plan.analysis.subject_is_var,
                        property_join_same_subject = property_join_plan.analysis.same_subject,
                        property_join_predicates_bound =
                            property_join_plan.analysis.predicates_bound,
                        property_join_object_modes_supported =
                            property_join_plan.analysis.object_modes_supported,
                        property_join_object_vars_distinct =
                            property_join_plan.analysis.object_vars_distinct,
                        property_join_has_bound_objects =
                            property_join_plan.analysis.has_bound_objects,
                        property_join_predicates_distinct =
                            property_join_plan.analysis.predicates_distinct,
                        property_join_width_score = property_join_plan.width_score,
                        property_join_optional_bonus = property_join_plan.optional_bonus,
                        property_join_meets_width_threshold =
                            property_join_plan.meets_width_threshold,
                        property_join_optional_triples = property_join_plan.tail_optional_triples,
                        property_join_tail_filters = property_join_plan.tail_filters,
                        property_join_tail_binds = property_join_plan.tail_binds,
                        chosen_strategy = if property_join_plan.can_property_join {
                            "property_join"
                        } else {
                            "sequential_join"
                        },
                        "planned inner join block"
                    );
                }

                if property_join_plan.can_property_join {
                    i = property_join_end;
                    let mut property_join_binds = pending_binds;
                    property_join_binds.extend(property_join_tail.binds);
                    let mut property_join_filters = pending_filters;
                    property_join_filters.extend(property_join_tail.filters);
                    operator = build_property_join_block(
                        &triples_for_exec,
                        &property_join_tail.optional_triples,
                        block.values,
                        property_join_binds,
                        property_join_filters,
                        &pushdown,
                        augmented_ref,
                        &var_counts,
                        &protected_vars,
                        planning,
                    )?;
                } else {
                    operator = build_sequential_join_block(
                        operator,
                        &triples_for_exec,
                        block.values,
                        pending_binds,
                        pending_filters,
                        &pushdown,
                        augmented_ref,
                        &var_counts,
                        &protected_vars,
                        group_by,
                        distinct_query,
                        planning,
                    )?;
                }
            }

            Pattern::Filter(expr) => {
                // Wrap current operator with filter; FILTER EXISTS subplans
                // inherit the captured planning context.
                let child = require_child(operator, "Filter pattern")?;
                operator = Some(Box::new(FilterOperator::new_with_planning(
                    child,
                    expr.clone(),
                    *planning,
                )));
                i += 1;
            }

            Pattern::Optional(_inner_patterns) => {
                // OPTIONAL with conjunctive semantics: all inner patterns must match together
                // (SPARQL 1.1 §8.1 LeftJoin).
                //
                // Two paths:
                // 1. Fast path: single triple pattern uses PatternOptionalBuilder (direct scan)
                // 2. General path: multi-pattern uses PlanTreeOptionalBuilder (full operator tree)
                let child = require_child(operator, "OPTIONAL pattern")?;

                let augmented_rwv = augmented_at(i + 1);
                let augmented_ref = augmented_rwv.as_deref();

                if let Pattern::Optional(inner_patterns) = &patterns[i] {
                    // Optimization: OPTIONAL { ... binds ?v ... } FILTER(!bound(?v))
                    // behaves like NOT EXISTS { ... } for the inner conjunctive block.
                    // Dispatch through the shared strategy helper so it picks
                    // SemijoinOperator (build-once + hash probe) when the inner shares
                    // produced key vars with the outer schema — the per-row
                    // ExistsOperator path is reserved for uncorrelated / outer-only-
                    // consumed cases the helper detects.
                    if let Some(Pattern::Filter(next_filter)) = patterns.get(i + 1) {
                        if let Some(v) = filter_not_bound_var(next_filter) {
                            let v_bound_in_outer = child.schema().contains(&v);
                            let v_appears_later = pattern_list_contains_var(&patterns[i + 2..], v);
                            let v_bound_by_inner = pattern_list_contains_var(inner_patterns, v);
                            if !v_bound_in_outer && !v_appears_later && v_bound_by_inner {
                                operator = Some(build_exists_strategy(
                                    child,
                                    inner_patterns,
                                    true,
                                    stats.clone(),
                                    *planning,
                                ));
                                i += 2;
                                continue;
                            }
                        }
                    }

                    let required_schema = Arc::from(child.schema().to_vec().into_boxed_slice());

                    let (grouped_optional_triples, grouped_end) =
                        collect_grouped_single_triple_optionals(patterns, i, &required_schema);
                    if grouped_optional_triples.len() >= 2 {
                        let builder = GroupedPatternOptionalBuilder::new(
                            required_schema.clone(),
                            grouped_optional_triples,
                            *planning,
                        )?;
                        operator = Some(Box::new(
                            OptionalOperator::with_builder(
                                child,
                                required_schema,
                                Box::new(builder),
                            )
                            .with_out_schema(augmented_ref),
                        ));
                        i = grouped_end;
                        continue;
                    }

                    // Fast path: single triple pattern
                    if inner_patterns.len() == 1 {
                        if let Some(inner_triple) = inner_patterns[0].as_triple().cloned() {
                            operator = Some(Box::new(
                                OptionalOperator::new(
                                    child,
                                    required_schema,
                                    inner_triple,
                                    *planning,
                                )
                                .with_out_schema(augmented_ref),
                            ));
                            i += 1;
                            continue;
                        }
                    }

                    // General path: use PlanTreeOptionalBuilder for multi-pattern or
                    // non-triple single patterns (VALUES, BIND, subquery, etc.)
                    let builder = PlanTreeOptionalBuilder::new(
                        required_schema.clone(),
                        inner_patterns.clone(),
                        stats.clone(),
                        *planning,
                    );
                    operator = Some(Box::new(
                        OptionalOperator::with_builder(child, required_schema, Box::new(builder))
                            .with_out_schema(augmented_ref),
                    ));
                    i += 1;
                    continue;
                }

                unreachable!("match arm ensures Pattern::Optional")
            }

            Pattern::Union(branches) => {
                let child = require_child(operator, "UNION pattern")?;
                if branches.is_empty() {
                    return Err(QueryError::InvalidQuery(
                        "UNION requires at least one branch".to_string(),
                    ));
                }

                let augmented_rwv = augmented_at(i + 1);
                let augmented_ref = augmented_rwv.as_deref();

                // Correlated UNION: execute each branch per input row (seeded from child).
                operator = Some(Box::new(
                    UnionOperator::new(child, branches.clone(), stats.clone(), *planning)
                        .with_out_schema(augmented_ref),
                ));
                i += 1;
            }

            Pattern::Minus(inner_patterns) => {
                // MINUS - anti-join semantics (set difference)
                let child = require_child(operator, "MINUS pattern")?;
                operator = Some(Box::new(MinusOperator::new(
                    child,
                    inner_patterns.clone(),
                    stats.clone(),
                    *planning,
                )));
                i += 1;
            }

            Pattern::Exists(inner_patterns) | Pattern::NotExists(inner_patterns) => {
                let child = require_child(operator, "EXISTS pattern")?;
                let negated = matches!(&patterns[i], Pattern::NotExists(_));
                operator = Some(build_exists_strategy(
                    child,
                    inner_patterns,
                    negated,
                    stats.clone(),
                    *planning,
                ));
                i += 1;
            }

            Pattern::PropertyPath(pp) => {
                // Property path - transitive graph traversal
                // Pass existing operator as child for correlation
                let augmented_rwv = augmented_at(i + 1);
                let augmented_ref = augmented_rwv.as_deref();

                operator = Some(Box::new(
                    PropertyPathOperator::new(operator, pp.clone(), DEFAULT_MAX_VISITED)
                        .with_out_schema(augmented_ref),
                ));
                i += 1;
            }

            Pattern::Subquery(sq) => {
                // Subquery - execute nested query and merge results
                let child = require_child(operator, "SUBQUERY pattern")?;
                let augmented_rwv = augmented_at(i + 1);
                let augmented_ref = augmented_rwv.as_deref();

                operator = Some(Box::new(
                    SubqueryOperator::new(child, sq.clone(), stats.clone(), *planning)
                        .with_out_schema(augmented_ref),
                ));
                i += 1;
            }

            Pattern::IndexSearch(isp) => {
                // BM25 full-text search against a graph source
                // If no child operator, use EmptyOperator as seed (allows IndexSearch at position 0)
                let child = get_or_empty_seed(operator.take());
                let augmented_rwv = augmented_at(i + 1);
                let augmented_ref = augmented_rwv.as_deref();

                operator = Some(Box::new(
                    Bm25SearchOperator::new(child, isp.clone()).with_out_schema(augmented_ref),
                ));
                i += 1;
            }

            Pattern::VectorSearch(vsp) => {
                // Vector similarity search against a vector graph source
                // If no child operator, use EmptyOperator as seed (allows VectorSearch at position 0)
                let child = get_or_empty_seed(operator.take());
                let augmented_rwv = augmented_at(i + 1);
                let augmented_ref = augmented_rwv.as_deref();

                operator = Some(Box::new(
                    crate::vector::VectorSearchOperator::new(child, vsp.clone())
                        .with_out_schema(augmented_ref),
                ));
                i += 1;
            }

            Pattern::R2rml(r2rml_pattern) => {
                // R2RML scan against an Iceberg graph source
                let child = require_child(operator, "R2RML pattern")?;
                operator = Some(Box::new(crate::r2rml::R2rmlScanOperator::new(
                    child,
                    r2rml_pattern.clone(),
                )));
                i += 1;
            }

            Pattern::GeoSearch(gsp) => {
                // Geographic proximity search against binary index
                let child = get_or_empty_seed(operator.take());
                let augmented_rwv = augmented_at(i + 1);
                let augmented_ref = augmented_rwv.as_deref();

                operator = Some(Box::new(
                    crate::geo_search::GeoSearchOperator::new(child, gsp.clone())
                        .with_out_schema(augmented_ref),
                ));
                i += 1;
            }

            Pattern::S2Search(s2p) => {
                // S2 spatial search against spatial index sidecar
                let child = get_or_empty_seed(operator.take());
                let augmented_rwv = augmented_at(i + 1);
                let augmented_ref = augmented_rwv.as_deref();

                operator = Some(Box::new(
                    crate::s2_search::S2SearchOperator::new(child, s2p.clone())
                        .with_out_schema(augmented_ref),
                ));
                i += 1;
            }

            Pattern::Graph {
                name,
                patterns: inner_patterns,
            } => {
                // GRAPH pattern - scope inner patterns to a named graph
                let child = require_child(operator, "GRAPH pattern")?;
                operator = Some(Box::new(crate::graph::GraphOperator::new(
                    child,
                    name.clone(),
                    inner_patterns.clone(),
                    *planning,
                )));
                i += 1;
            }

            Pattern::Service(service_pattern) => {
                // SERVICE pattern - execute patterns against another ledger
                let child = require_child(operator, "SERVICE pattern")?;
                operator = Some(Box::new(crate::service::ServiceOperator::new(
                    child,
                    service_pattern.clone(),
                    *planning,
                )));
                i += 1;
            }
        }
    }

    operator.ok_or_else(|| QueryError::InvalidQuery("No patterns produced an operator".to_string()))
}

/// Create a first-pattern scan operator.
///
/// Wraps the scan in a [`DatasetOperator`] so that multi-graph fanout and
/// provenance stamping are handled transparently. In single-graph mode the
/// `DatasetOperator` passes through to the inner scan with negligible overhead.
fn make_first_scan(
    tp: &TriplePattern,
    object_bounds: &HashMap<VarId, ObjectBounds>,
    inline_ops: Vec<InlineOperator>,
    emit: EmitMask,
    group_by: &[VarId],
    planning: &PlanningContext,
) -> BoxedOperator {
    let obj_bounds = tp.o.as_var().and_then(|v| object_bounds.get(&v).cloned());
    let index_hint = scan_index_hint_for_triple(tp, group_by, &inline_ops);
    Box::new(crate::dataset_operator::DatasetOperator::scan(
        tp.clone(),
        obj_bounds,
        inline_ops,
        emit,
        index_hint,
        planning.mode(),
    ))
}

/// Build a single scan or join operator for a triple pattern
///
/// This is the extracted helper that eliminates the duplication between
/// `build_where_operators_seeded` (incremental path) and `build_triple_operators`.
///
/// - If `left` is None, creates a `DatasetOperator` scan for the first pattern with inline operators
/// - If `left` is Some, creates a NestedLoopJoinOperator joining to the existing operator
/// - Applies object bounds from filters when available
///
/// The `inline_ops` are evaluated inline on the operator: baked into the scan
/// for the first pattern, or evaluated per combined row in `NestedLoopJoinOperator` for joins.
#[allow(clippy::too_many_arguments)]
pub fn build_scan_or_join(
    left: Option<BoxedOperator>,
    tp: &TriplePattern,
    object_bounds: &HashMap<VarId, ObjectBounds>,
    inline_ops: Vec<InlineOperator>,
    downstream_vars: Option<&[VarId]>,
    emit: EmitMask,
    group_by: &[VarId],
    planning: &PlanningContext,
) -> BoxedOperator {
    match left {
        None => make_first_scan(tp, object_bounds, inline_ops, emit, group_by, planning),
        Some(left) => {
            // Subsequent patterns: use NestedLoopJoinOperator with optional bounds pushdown
            let left_schema: Arc<[VarId]> = Arc::from(left.schema().to_vec().into_boxed_slice());

            // Extract object bounds if available for this pattern's object variable
            let bounds = tp.o.as_var().and_then(|v| object_bounds.get(&v).cloned());

            Box::new(
                NestedLoopJoinOperator::new(
                    left,
                    left_schema,
                    tp.clone(),
                    bounds,
                    inline_ops,
                    EmitMask::ALL,
                    planning.mode(),
                )
                .with_out_schema(downstream_vars),
            )
        }
    }
}

#[inline]
fn emit_mask_for_triple(
    tp: &TriplePattern,
    var_counts: &HashMap<VarId, usize>,
    protected_vars: &HashSet<VarId>,
) -> EmitMask {
    let emit_var = |v: VarId| {
        let count = var_counts.get(&v).copied().unwrap_or(0);
        count > 1 || protected_vars.contains(&v)
    };

    let emit_s = match &tp.s {
        Ref::Var(v) => emit_var(*v),
        _ => true,
    };
    let emit_p = match &tp.p {
        Ref::Var(v) => emit_var(*v),
        _ => true,
    };
    let emit_o = match &tp.o {
        Term::Var(v) => emit_var(*v),
        _ => true,
    };

    EmitMask {
        s: emit_s,
        p: emit_p,
        o: emit_o,
    }
}

#[inline]
fn scan_index_hint_for_triple(
    tp: &TriplePattern,
    group_by: &[VarId],
    inline_ops: &[InlineOperator],
) -> Option<IndexType> {
    if let Some(hint) = crate::binary_scan::preferred_index_hint_for_prefix_filters(tp, inline_ops)
    {
        return Some(hint);
    }
    if group_by.len() != 1 {
        return None;
    }
    let gb = group_by[0];

    // Hint only on patterns of the form: ?s <p> ?o (fixed predicate, both s/o vars).
    // This preserves semantics while allowing a physical order aligned with GROUP BY.
    if !tp.p_bound() {
        return None;
    }
    let Ref::Var(sv) = tp.s else {
        return None;
    };
    let Term::Var(ov) = tp.o else {
        return None;
    };

    if gb == ov {
        Some(IndexType::Post)
    } else if gb == sv {
        Some(IndexType::Psot)
    } else {
        None
    }
}

/// Build operators for a sequence of triple patterns
///
/// Uses property join optimization when applicable.
/// When `object_bounds` is provided, range constraints are pushed down to the scan operator
/// for the first pattern, enabling index-level filtering.
#[allow(clippy::too_many_arguments)]
pub fn build_triple_operators(
    existing: Option<BoxedOperator>,
    triples: &[TriplePattern],
    object_bounds: &HashMap<VarId, ObjectBounds>,
    required_where_vars: Option<&[VarId]>,
    var_counts: &HashMap<VarId, usize>,
    protected_vars: &HashSet<VarId>,
    group_by: &[VarId],
    distinct_query: bool,
    planning: &PlanningContext,
) -> Result<BoxedOperator> {
    if triples.is_empty() {
        return existing
            .ok_or_else(|| QueryError::InvalidQuery("No triple patterns to process".to_string()));
    }

    let mut operator = existing;
    let triples_for_exec: Vec<TriplePattern> = triples.to_vec();

    // Check for property join optimization
    //
    // PropertyJoinOperator scans each predicate independently and applies per-predicate
    // object bounds during its scan phase, then intersects subjects. This is far cheaper
    // than falling back to NestedLoopJoin which does correlated novelty traversals.
    if operator.is_none() && triples_for_exec.len() >= 2 && is_property_join(&triples_for_exec) {
        // Use PropertyJoinOperator for multi-property patterns.
        //
        // If an object var is not needed downstream (not in required_where_vars and not
        // otherwise protected), treat that predicate as existence-only (semijoin) to avoid
        // cartesian-product blowups.
        let mut needed: HashSet<VarId> = HashSet::new();
        if let Some(rwv) = required_where_vars {
            needed.extend(rwv.iter().copied());
        }
        for (v, c) in var_counts {
            if *c > 1 || protected_vars.contains(v) {
                needed.insert(*v);
            }
        }

        let pj = PropertyJoinOperator::new_with_needed_vars(
            &triples_for_exec,
            object_bounds.clone(),
            Some(&needed),
            planning.mode(),
        )?;
        return Ok(Box::new(pj));
    }

    // Build chain of scan/join operators using the shared helper
    let rwv_set: Option<HashSet<VarId>> = required_where_vars.map(|v| v.iter().copied().collect());

    let mut seen_vars: HashSet<VarId> = HashSet::new();
    for (k, pattern) in triples_for_exec.iter().enumerate() {
        seen_vars.extend(pattern.produced_vars());
        // Compute live vars: required_where_vars ∪ vars from subsequent triples
        let live_vars = rwv_set.as_ref().map(|base| {
            let suffix_vars: HashSet<VarId> = triples_for_exec[k + 1..]
                .iter()
                .flat_map(crate::ir::triple::TriplePattern::referenced_vars)
                .collect();
            base.union(&suffix_vars).copied().collect::<Vec<VarId>>()
        });

        let emit = emit_mask_for_triple(pattern, var_counts, protected_vars);
        operator = Some(build_scan_or_join(
            operator,
            pattern,
            object_bounds,
            Vec::new(),
            live_vars.as_deref(),
            emit,
            group_by,
            planning,
        ));

        // DISTINCT query optimization: if any variables seen so far are no longer live,
        // collapse duplicates early to avoid downstream join blowups.
        if distinct_query {
            if let Some(live) = live_vars.as_ref() {
                let live_set: HashSet<VarId> = live.iter().copied().collect();
                let dead = seen_vars
                    .iter()
                    .copied()
                    .filter(|v| !live_set.contains(v))
                    .count();
                if dead > 0 {
                    if let Some(op) = operator.take() {
                        operator = Some(Box::new(DistinctOperator::new(op)));
                    }
                }
            }
        }
    }

    Ok(operator.unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::triple::{Ref, Term};
    use crate::ir::{Expression, Pattern};
    use fluree_db_core::{FlakeValue, PropertyStatData, Sid, StatsView};
    use std::collections::HashSet;
    use std::sync::Arc;

    // Back-compat helpers for tests: most unit tests don't care about post-WHERE
    // needed-vars pruning, so default to "no extra needed vars".
    fn build_where_operators(
        patterns: &[Pattern],
        stats: Option<Arc<StatsView>>,
    ) -> Result<BoxedOperator> {
        // Preserve historical test behavior: treat ALL vars in the pattern list
        // as "needed", so WHERE planning does not prune schema outputs.
        let mut needed: HashSet<VarId> = HashSet::new();
        let mut counts: HashMap<VarId, usize> = HashMap::new();
        collect_var_stats(patterns, &mut counts, &mut needed);
        needed.extend(counts.keys().copied());
        super::build_where_operators_with_needed(
            patterns,
            stats,
            &needed,
            &[],
            false,
            None,
            &crate::temporal_mode::PlanningContext::current(),
        )
    }

    fn build_triple_operators(
        existing: Option<BoxedOperator>,
        triples: &[TriplePattern],
        object_bounds: &HashMap<VarId, ObjectBounds>,
    ) -> Result<BoxedOperator> {
        // Preserve historical test behavior: keep all triple vars.
        let needed: HashSet<VarId> = triples
            .iter()
            .flat_map(crate::ir::triple::TriplePattern::produced_vars)
            .collect();
        let (counts, protected) = compute_where_var_stats(
            &triples
                .iter()
                .cloned()
                .map(Pattern::Triple)
                .collect::<Vec<_>>(),
            &needed,
        );
        super::build_triple_operators(
            existing,
            triples,
            object_bounds,
            None,
            &counts,
            &protected,
            &[],
            false,
            &crate::temporal_mode::PlanningContext::current(),
        )
    }

    fn make_pattern(s_var: VarId, p_name: &str, o_var: VarId) -> TriplePattern {
        TriplePattern::new(
            Ref::Var(s_var),
            Ref::Sid(Sid::new(100, p_name)),
            Term::Var(o_var),
        )
    }

    #[test]
    fn test_build_where_operators_single_triple() {
        let patterns = vec![Pattern::Triple(make_pattern(VarId(0), "name", VarId(1)))];

        let result = build_where_operators(&patterns, None);
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
                Expression::Const(FlakeValue::Long(18)),
            )),
        ];

        let result = build_where_operators(&patterns, None);
        assert!(result.is_ok());
    }

    /// Regression: top-level VALUES must not block PropertyJoinOperator selection.
    ///
    /// Previously, we eagerly inserted an EmptyOperator seed whenever the first pattern
    /// was non-triple. With VALUES at position 0, that made `operator.is_none()` false
    /// and disabled PropertyJoinOperator, causing a catastrophic fallback to nested-loop
    /// joins on multi-property patterns (e.g. vector score + date filter).
    #[test]
    fn test_values_does_not_block_property_join_schema_order() {
        use crate::binding::Binding;

        // VALUES ?queryVec { 0 } .
        // ?article :date ?date .
        // ?article :vec ?vec .
        // ?article :title ?title .
        // ?article :status ?status .
        //
        // We assert schema order to distinguish the plan shape:
        // - Bad (VALUES first): schema starts with ?queryVec
        // - Good (PropertyJoin first, then VALUES wrap): schema starts with ?article
        let patterns = vec![
            Pattern::Values {
                vars: vec![VarId(9)],
                rows: vec![vec![Binding::lit(FlakeValue::Long(0), Sid::new(2, "long"))]],
            },
            Pattern::Triple(make_pattern(VarId(0), "date", VarId(1))),
            Pattern::Triple(make_pattern(VarId(0), "vec", VarId(2))),
            Pattern::Triple(make_pattern(VarId(0), "title", VarId(3))),
            Pattern::Triple(make_pattern(VarId(0), "status", VarId(4))),
        ];

        let op = build_where_operators(&patterns, None).unwrap();
        let schema = op.schema();
        assert!(
            !schema.is_empty(),
            "schema should be non-empty for VALUES + triples"
        );
        assert_eq!(
            schema[0],
            VarId(0),
            "expected plan to start from subject var (?article), not VALUES var"
        );
        assert!(
            schema.contains(&VarId(9)),
            "expected VALUES var (?queryVec) to appear in schema"
        );
    }

    #[test]
    fn test_collects_and_reorders_triples_across_safe_filter_boundary_with_stats() {
        // Roughly matches the user's SPARQL shape:
        //   ?score :hasScore ?scoreV .
        //   FILTER(?scoreV > 0.4)
        //   ?score :refersInstance ?concept .
        //   ?concept :notation "LVL1" .
        //
        // The key expectation: we can treat (Triple + FILTER + Triple + Triple) as a single block
        // and reorder the triples (with stats) to start from the most selective predicate ("notation").
        let score = VarId(0);
        let score_v = VarId(1);
        let concept = VarId(2);

        let patterns = vec![
            Pattern::Triple(make_pattern(score, "hasScore", score_v)),
            Pattern::Filter(Expression::gt(
                Expression::Var(score_v),
                Expression::Const(FlakeValue::Double(0.4)),
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

        // Stats: make "notation" look far more selective than the score predicates.
        let mut stats = StatsView::default();
        stats.properties.insert(
            Sid::new(100, "notation"),
            PropertyStatData {
                count: 1_000_000,
                ndv_values: 1_000_000, // selectivity ~ 1
                ndv_subjects: 1_000_000,
            },
        );
        stats.properties.insert(
            Sid::new(100, "hasScore"),
            PropertyStatData {
                count: 1_000_000_000, // very unselective property scan
                ndv_values: 900_000_000,
                ndv_subjects: 900_000_000,
            },
        );
        stats.properties.insert(
            Sid::new(100, "refersInstance"),
            PropertyStatData {
                count: 800_000_000, // unselective property scan
                ndv_values: 700_000_000,
                ndv_subjects: 700_000_000,
            },
        );

        let as_patterns: Vec<Pattern> = block.triples.into_iter().map(Pattern::Triple).collect();
        let ordered = reorder_patterns(&as_patterns, Some(&stats), &HashSet::new());
        let first_triple = ordered[0]
            .as_triple()
            .expect("first reordered pattern should be a triple");
        let first_pred = first_triple.p.as_sid().expect("predicate should be Sid");
        assert_eq!(
            &*first_pred.name, "notation",
            "expected optimizer to start from the most selective triple"
        );
    }

    #[test]
    fn test_collect_block_includes_values_and_marks_filter_safe() {
        use crate::binding::Binding;

        // VALUES ?x { 1 } . FILTER(?x = 1) . ?s :p ?x
        let patterns = vec![
            Pattern::Values {
                vars: vec![VarId(0)],
                rows: vec![vec![Binding::lit(FlakeValue::Long(1), Sid::new(2, "long"))]],
            },
            Pattern::Filter(Expression::eq(
                Expression::Var(VarId(0)),
                Expression::Const(FlakeValue::Long(1)),
            )),
            Pattern::Triple(TriplePattern::new(
                Ref::Var(VarId(1)),
                Ref::Sid(Sid::new(100, "p")),
                Term::Var(VarId(0)),
            )),
        ];

        let block = collect_inner_join_block(&patterns, 0);
        assert_eq!(block.end_index, patterns.len());
        assert_eq!(block.values.len(), 1, "VALUES should be included in block");
        assert_eq!(block.binds.len(), 0, "expected 0 BINDs in the block");
        assert_eq!(block.triples.len(), 1);
        assert_eq!(
            block.filters.len(),
            1,
            "FILTER referencing VALUES var should be safe"
        );
    }

    #[test]
    fn test_collect_block_includes_safe_bind() {
        // ?s :age ?age . BIND(?age + 1 AS ?age2) . FILTER(?age2 > 0)
        //
        // BIND is "safe" here because ?age is bound by the preceding triple in original order.
        let patterns = vec![
            Pattern::Triple(make_pattern(VarId(0), "age", VarId(1))),
            Pattern::Bind {
                var: VarId(2),
                expr: Expression::add(
                    Expression::Var(VarId(1)),
                    Expression::Const(FlakeValue::Long(1)),
                ),
            },
            Pattern::Filter(Expression::gt(
                Expression::Var(VarId(2)),
                Expression::Const(FlakeValue::Long(0)),
            )),
        ];

        let block = collect_inner_join_block(&patterns, 0);
        assert_eq!(block.end_index, patterns.len());
        assert_eq!(block.values.len(), 0);
        assert_eq!(block.triples.len(), 1);
        assert_eq!(
            block.binds.len(),
            1,
            "expected BIND to be included in inner-join block"
        );
        assert_eq!(block.filters.len(), 1);
    }

    #[test]
    fn test_build_where_operators_filter_before_triple_allowed() {
        // FILTER at position 0 is now allowed with empty seed support
        let patterns = vec![
            Pattern::Filter(Expression::eq(
                Expression::Const(FlakeValue::Long(1)),
                Expression::Const(FlakeValue::Long(1)),
            )),
            Pattern::Triple(make_pattern(VarId(0), "name", VarId(1))),
        ];

        let result = build_where_operators(&patterns, None);
        // Now succeeds - empty seed provides initial solution
        assert!(result.is_ok());
    }

    #[test]
    fn test_values_pattern_builds() {
        use crate::binding::Binding;

        // VALUES at position 0 should work
        // xsd:long is namespace code 2
        let patterns = vec![Pattern::Values {
            vars: vec![VarId(0)],
            rows: vec![vec![Binding::lit(
                FlakeValue::Long(42),
                Sid::new(2, "long"),
            )]],
        }];
        let result = build_where_operators(&patterns, None);
        assert!(result.is_ok());

        let op = result.unwrap();
        // Schema should include the VALUES var
        assert_eq!(op.schema(), &[VarId(0)]);
    }

    #[test]
    fn test_bind_pattern_builds() {
        // BIND at position 0 should work
        let patterns = vec![Pattern::Bind {
            var: VarId(0),
            expr: Expression::Const(FlakeValue::Long(42)),
        }];
        let result = build_where_operators(&patterns, None);
        assert!(result.is_ok());

        let op = result.unwrap();
        // Schema should include the BIND var
        assert_eq!(op.schema(), &[VarId(0)]);
    }

    #[test]
    fn test_union_pattern_builds() {
        // UNION at position 0 should work
        let patterns = vec![Pattern::Union(vec![
            vec![Pattern::Triple(make_pattern(VarId(0), "name", VarId(1)))],
            vec![Pattern::Triple(make_pattern(VarId(0), "email", VarId(2)))],
        ])];
        let result = build_where_operators(&patterns, None);
        assert!(result.is_ok());

        let op = result.unwrap();
        // Unified schema should have all vars: ?0, ?1, ?2
        assert_eq!(op.schema().len(), 3);
    }

    #[test]
    fn test_values_then_triple_pattern() {
        use crate::binding::Binding;

        // VALUES followed by triple pattern
        // xsd:long is namespace code 2
        let patterns = vec![
            Pattern::Values {
                vars: vec![VarId(0)],
                rows: vec![
                    vec![Binding::lit(FlakeValue::Long(1), Sid::new(2, "long"))],
                    vec![Binding::lit(FlakeValue::Long(2), Sid::new(2, "long"))],
                ],
            },
            Pattern::Triple(make_pattern(VarId(0), "name", VarId(1))),
        ];
        let result = build_where_operators(&patterns, None);
        assert!(result.is_ok());

        let op = result.unwrap();
        // Schema should have both vars
        assert_eq!(op.schema(), &[VarId(0), VarId(1)]);
    }

    #[test]
    fn test_empty_operator_schema() {
        use crate::seed::EmptyOperator;

        let op = EmptyOperator::new();
        // EmptyOperator has empty schema
        assert_eq!(op.schema().len(), 0);
        assert_eq!(op.estimated_rows(), Some(1));
    }

    /// Regression: PropertyJoinOperator must be used even when object bounds are
    /// non-empty (bounds are applied per-predicate inside the property-join scan).
    /// Previously, `build_triple_operators` fell back to NestedLoopJoin whenever
    /// `object_bounds` was non-empty, causing 1000x+ slowdowns on vector queries
    /// with FILTER clauses.
    #[test]
    fn test_property_join_used_with_object_bounds() {
        use fluree_db_core::ObjectBounds;

        let tp1 = make_pattern(VarId(0), "date", VarId(1));
        let tp2 = make_pattern(VarId(0), "vec", VarId(2));
        let triples = vec![tp1, tp2];

        // Non-empty bounds on one of the object variables
        let mut bounds = HashMap::new();
        bounds.insert(
            VarId(1),
            ObjectBounds {
                lower: Some((FlakeValue::String("2026-01-01".to_string()), true)),
                upper: None,
            },
        );

        let op = build_triple_operators(None, &triples, &bounds).unwrap();

        // PropertyJoinOperator schema is [subject, obj1, obj2] in declaration order.
        // If NestedLoopJoin were used instead, all three vars would still appear but
        // the operator would be a chain rather than a single PropertyJoinOperator.
        assert_eq!(
            op.schema(),
            &[VarId(0), VarId(1), VarId(2)],
            "PropertyJoinOperator should be used (schema = [subject, obj1, obj2])"
        );
    }

    /// Verify that `build_where_operators` uses PropertyJoinOperator for a multi-property
    /// pattern with a FILTER that produces object bounds.
    #[test]
    fn test_where_operators_property_join_with_filter_bounds() {
        // Pattern: ?s :date ?date . ?s :vec ?vec . FILTER(?date >= "2026-01-01")
        // This should use PropertyJoinOperator (with bounds on ?date pushed into its
        // per-predicate scan) rather than falling back to NestedLoopJoin.
        let patterns = vec![
            Pattern::Triple(make_pattern(VarId(0), "date", VarId(1))),
            Pattern::Triple(make_pattern(VarId(0), "vec", VarId(2))),
            Pattern::Filter(Expression::ge(
                Expression::Var(VarId(1)),
                Expression::Const(FlakeValue::String("2026-01-01".to_string())),
            )),
        ];

        let result = build_where_operators(&patterns, None);
        assert!(result.is_ok(), "should build successfully");

        let op = result.unwrap();
        // All three variables should be in the schema
        let schema = op.schema();
        assert!(schema.contains(&VarId(0)), "subject var present");
        assert!(schema.contains(&VarId(1)), "date var present");
        assert!(schema.contains(&VarId(2)), "vec var present");
    }

    #[test]
    fn test_build_scan_or_join_first_pattern() {
        let tp = make_pattern(VarId(0), "name", VarId(1));
        let bounds = HashMap::new();

        let op: BoxedOperator = build_scan_or_join(
            None,
            &tp,
            &bounds,
            Vec::new(),
            None,
            EmitMask::ALL,
            &[],
            &crate::temporal_mode::PlanningContext::current(),
        );

        assert_eq!(op.schema(), &[VarId(0), VarId(1)]);
    }

    #[test]
    fn test_build_scan_or_join_with_left() {
        let tp1 = make_pattern(VarId(0), "name", VarId(1));
        let tp2 = make_pattern(VarId(0), "age", VarId(2));
        let bounds = HashMap::new();

        let first: BoxedOperator = build_scan_or_join(
            None,
            &tp1,
            &bounds,
            Vec::new(),
            None,
            EmitMask::ALL,
            &[],
            &crate::temporal_mode::PlanningContext::current(),
        );
        let second = build_scan_or_join(
            Some(first),
            &tp2,
            &bounds,
            Vec::new(),
            None,
            EmitMask::ALL,
            &[],
            &crate::temporal_mode::PlanningContext::current(),
        );

        // Schema should include all vars from both patterns
        assert_eq!(second.schema().len(), 3);
        assert!(second.schema().contains(&VarId(0)));
        assert!(second.schema().contains(&VarId(1)));
        assert!(second.schema().contains(&VarId(2)));
    }

    // ========================================================================
    // Filter optimization tests - Phase 1: dependency-based filter injection
    // ========================================================================

    #[test]
    fn test_filter_before_triple_collected_in_block() {
        // FILTER(?x > 0) . ?s :p ?x
        // Filter references ?x which is bound by the subsequent triple.
        // The filter should be collected and applied after ?x is bound.
        let patterns = vec![
            Pattern::Filter(Expression::gt(
                Expression::Var(VarId(0)),
                Expression::Const(FlakeValue::Long(0)),
            )),
            Pattern::Triple(make_pattern(VarId(1), "value", VarId(0))),
        ];

        let block = collect_inner_join_block(&patterns, 0);
        assert_eq!(block.end_index, 2, "should consume both filter and triple");
        assert_eq!(block.filters.len(), 1, "should include the filter");
        assert_eq!(block.triples.len(), 1, "should include the triple");
    }

    #[test]
    fn test_filter_referencing_later_pattern_vars() {
        // ?s :age ?age . FILTER(?age > 18 AND ?name != "") . ?s :name ?name
        // Filter references both ?age (already bound) and ?name (bound later).
        // All patterns should be collected in the same block.
        let age = VarId(0);
        let name = VarId(1);
        let s = VarId(2);

        let patterns = vec![
            Pattern::Triple(make_pattern(s, "age", age)),
            Pattern::Filter(Expression::and(vec![
                Expression::gt(
                    Expression::Var(age),
                    Expression::Const(FlakeValue::Long(18)),
                ),
                Expression::ne(
                    Expression::Var(name),
                    Expression::Const(FlakeValue::String(String::new())),
                ),
            ])),
            Pattern::Triple(make_pattern(s, "name", name)),
        ];

        let block = collect_inner_join_block(&patterns, 0);
        assert_eq!(
            block.end_index,
            patterns.len(),
            "should consume all patterns"
        );
        assert_eq!(block.triples.len(), 2, "should include both triples");
        assert_eq!(block.filters.len(), 1, "should include the filter");
    }

    #[test]
    fn test_filter_only_patterns_are_collected() {
        // Multiple filters before any triple - all should be collected
        let patterns = vec![
            Pattern::Filter(Expression::gt(
                Expression::Var(VarId(0)),
                Expression::Const(FlakeValue::Long(0)),
            )),
            Pattern::Filter(Expression::lt(
                Expression::Var(VarId(0)),
                Expression::Const(FlakeValue::Long(100)),
            )),
            Pattern::Triple(make_pattern(VarId(1), "value", VarId(0))),
        ];

        let block = collect_inner_join_block(&patterns, 0);
        assert_eq!(block.end_index, 3, "should consume all patterns");
        assert_eq!(block.filters.len(), 2, "should include both filters");
        assert_eq!(block.triples.len(), 1, "should include the triple");
    }

    #[test]
    fn test_build_operators_filter_before_triple_is_valid() {
        // FILTER(?x > 18) before the triple that binds ?x should now succeed
        let patterns = vec![
            Pattern::Filter(Expression::gt(
                Expression::Var(VarId(0)),
                Expression::Const(FlakeValue::Long(18)),
            )),
            Pattern::Triple(make_pattern(VarId(1), "age", VarId(0))),
        ];

        let result = build_where_operators(&patterns, None);
        assert!(
            result.is_ok(),
            "Filter before its bound variable should be allowed: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_filter_with_multiple_unbound_vars_collected() {
        // FILTER(?x > ?y) where both vars are bound by later triples
        let x = VarId(0);
        let y = VarId(1);
        let s = VarId(2);

        let patterns = vec![
            Pattern::Filter(Expression::gt(Expression::Var(x), Expression::Var(y))),
            Pattern::Triple(make_pattern(s, "valueX", x)),
            Pattern::Triple(make_pattern(s, "valueY", y)),
        ];

        let block = collect_inner_join_block(&patterns, 0);
        assert_eq!(
            block.end_index,
            patterns.len(),
            "should consume all patterns"
        );
        assert_eq!(block.filters.len(), 1);
        assert_eq!(block.triples.len(), 2);

        // Verify the operator tree builds successfully
        let result = build_where_operators(&patterns, None);
        assert!(
            result.is_ok(),
            "Should build successfully: {:?}",
            result.err()
        );
    }

    // =========================================================================
    // Generalized reordering integration tests
    // =========================================================================

    #[test]
    fn test_build_operators_with_union_builds_successfully() {
        // Triple followed by UNION should build successfully
        let patterns = vec![
            Pattern::Triple(make_pattern(VarId(0), "name", VarId(1))),
            Pattern::Union(vec![
                vec![Pattern::Triple(make_pattern(VarId(0), "type", VarId(2)))],
                vec![Pattern::Triple(make_pattern(VarId(0), "class", VarId(3)))],
            ]),
        ];

        let result = build_where_operators(&patterns, None);
        assert!(result.is_ok(), "Should build: {:?}", result.err());

        let op = result.unwrap();
        assert!(op.schema().contains(&VarId(0)));
    }

    #[test]
    fn test_build_operators_optional_after_triples() {
        // OPTIONAL should work after triple patterns
        let patterns = vec![
            Pattern::Triple(make_pattern(VarId(0), "name", VarId(1))),
            Pattern::Triple(make_pattern(VarId(0), "age", VarId(2))),
            Pattern::Optional(vec![Pattern::Triple(make_pattern(
                VarId(0),
                "email",
                VarId(3),
            ))]),
        ];

        let result = build_where_operators(&patterns, None);
        assert!(result.is_ok(), "Should build: {:?}", result.err());

        let op = result.unwrap();
        let schema = op.schema();
        assert!(schema.contains(&VarId(0)), "subject var present");
        assert!(schema.contains(&VarId(1)), "name var present");
        assert!(schema.contains(&VarId(2)), "age var present");
        assert!(
            schema.contains(&VarId(3)),
            "email var from OPTIONAL present"
        );
    }

    #[test]
    fn test_property_join_width_score_counts_same_subject_optionals_partially() {
        let s = VarId(0);
        let required = vec![
            make_pattern(s, "name", VarId(1)),
            make_pattern(s, "amount", VarId(2)),
            make_pattern(s, "stage", VarId(3)),
        ];
        let trailing = vec![
            Pattern::Optional(vec![Pattern::Triple(make_pattern(
                s,
                "probability",
                VarId(4),
            ))]),
            Pattern::Optional(vec![Pattern::Triple(make_pattern(s, "closedAt", VarId(5)))]),
        ];

        let (score, optional_bonus) = property_join_width_score(&required, &trailing);
        assert_eq!(optional_bonus, 1.0);
        assert_eq!(score, 4.0);
        assert!(score > PROPERTY_JOIN_MIN_WIDTH_SCORE);
    }

    #[test]
    fn test_property_join_plan_accepts_exact_width_threshold() {
        let s = VarId(0);
        let triples = vec![
            make_pattern(s, "type", VarId(1)),
            make_pattern(s, "text", VarId(2)),
            make_pattern(s, "vector", VarId(3)),
        ];
        let patterns: Vec<Pattern> = triples.iter().cloned().map(Pattern::Triple).collect();

        let (decision, _tail) =
            analyze_property_join_plan(&patterns, patterns.len(), &triples, false);

        assert_eq!(decision.width_score, PROPERTY_JOIN_MIN_WIDTH_SCORE);
        assert!(decision.meets_width_threshold);
        assert!(decision.can_property_join);
    }

    #[test]
    fn test_property_join_width_score_ignores_non_star_optional_shapes() {
        let s = VarId(0);
        let required = vec![
            make_pattern(s, "name", VarId(1)),
            make_pattern(s, "amount", VarId(2)),
        ];
        let trailing = vec![Pattern::Optional(vec![Pattern::Triple(
            TriplePattern::new(
                Ref::Var(VarId(9)),
                Ref::Sid(Sid::new(100, "other")),
                Term::Var(VarId(10)),
            ),
        )])];

        let (score, optional_bonus) = property_join_width_score(&required, &trailing);
        assert_eq!(optional_bonus, 0.0);
        assert_eq!(score, 2.0);
        assert!(score <= PROPERTY_JOIN_MIN_WIDTH_SCORE);
    }

    #[test]
    fn test_collect_property_join_tail_fuses_simple_optionals_and_required_filter() {
        let s = VarId(0);
        let required = vec![
            make_pattern(s, "name", VarId(1)),
            make_pattern(s, "amount", VarId(2)),
            make_pattern(s, "stage", VarId(3)),
        ];
        let patterns = vec![
            Pattern::Optional(vec![Pattern::Triple(make_pattern(
                s,
                "probability",
                VarId(4),
            ))]),
            Pattern::Optional(vec![Pattern::Triple(make_pattern(s, "closedAt", VarId(5)))]),
            Pattern::Filter(Expression::not(Expression::Call {
                func: crate::ir::Function::StrStarts,
                args: vec![
                    Expression::Call {
                        func: crate::ir::Function::Str,
                        args: vec![Expression::Var(VarId(3))],
                    },
                    Expression::Const(FlakeValue::String("Closed".to_string())),
                ],
            })),
        ];

        let tail = collect_property_join_tail(&patterns, 0, &required);
        assert_eq!(tail.end_index, patterns.len());
        assert_eq!(tail.optional_triples.len(), 2);
        assert_eq!(tail.filters.len(), 1);
    }

    #[test]
    fn test_fast_path_triple_only_unchanged() {
        // Triple-only patterns should work identically to before
        // (reorder_patterns handles all pattern types uniformly)
        let patterns = vec![
            Pattern::Triple(make_pattern(VarId(0), "name", VarId(1))),
            Pattern::Triple(make_pattern(VarId(0), "age", VarId(2))),
            Pattern::Filter(Expression::gt(
                Expression::Var(VarId(2)),
                Expression::Const(FlakeValue::Long(18)),
            )),
        ];

        let result = build_where_operators(&patterns, None);
        assert!(result.is_ok(), "Should build: {:?}", result.err());

        let op = result.unwrap();
        assert!(op.schema().contains(&VarId(0)));
        assert!(op.schema().contains(&VarId(1)));
        assert!(op.schema().contains(&VarId(2)));
    }

    #[test]
    fn test_property_join_still_works_with_compound_patterns_elsewhere() {
        // When compound patterns exist but triples still qualify for property join,
        // the fast path should detect property join within the triple block.
        let patterns = vec![
            Pattern::Triple(make_pattern(VarId(0), "date", VarId(1))),
            Pattern::Triple(make_pattern(VarId(0), "vec", VarId(2))),
            Pattern::Optional(vec![Pattern::Triple(make_pattern(
                VarId(0),
                "email",
                VarId(3),
            ))]),
        ];

        let result = build_where_operators(&patterns, None);
        assert!(result.is_ok(), "Should build: {:?}", result.err());

        let op = result.unwrap();
        let schema = op.schema();
        assert!(schema.contains(&VarId(0)));
        assert!(schema.contains(&VarId(1)));
        assert!(schema.contains(&VarId(2)));
    }

    #[test]
    fn test_build_operators_minus_after_triple() {
        // MINUS after a triple should work
        let patterns = vec![
            Pattern::Triple(make_pattern(VarId(0), "name", VarId(1))),
            Pattern::Minus(vec![Pattern::Triple(make_pattern(
                VarId(0),
                "deleted",
                VarId(2),
            ))]),
        ];

        let result = build_where_operators(&patterns, None);
        assert!(result.is_ok(), "Should build: {:?}", result.err());
    }

    #[test]
    fn test_build_operators_exists_after_triple() {
        // EXISTS after a triple should work
        let patterns = vec![
            Pattern::Triple(make_pattern(VarId(0), "name", VarId(1))),
            Pattern::Exists(vec![Pattern::Triple(make_pattern(
                VarId(0),
                "verified",
                VarId(2),
            ))]),
        ];

        let result = build_where_operators(&patterns, None);
        assert!(result.is_ok(), "Should build: {:?}", result.err());
    }

    #[test]
    fn test_build_operators_complex_mix() {
        // Complex mix: Triple, UNION, Triple, MINUS, OPTIONAL
        let patterns = vec![
            Pattern::Triple(make_pattern(VarId(0), "name", VarId(1))),
            Pattern::Union(vec![
                vec![Pattern::Triple(make_pattern(VarId(0), "type", VarId(2)))],
                vec![Pattern::Triple(make_pattern(VarId(0), "class", VarId(3)))],
            ]),
            Pattern::Triple(make_pattern(VarId(0), "age", VarId(4))),
            Pattern::Minus(vec![Pattern::Triple(make_pattern(
                VarId(0),
                "deleted",
                VarId(5),
            ))]),
            Pattern::Optional(vec![Pattern::Triple(make_pattern(
                VarId(0),
                "email",
                VarId(6),
            ))]),
        ];

        let result = build_where_operators(&patterns, None);
        assert!(
            result.is_ok(),
            "Complex mix should build: {:?}",
            result.err()
        );

        let op = result.unwrap();
        let schema = op.schema();
        assert!(schema.contains(&VarId(0)));
        assert!(schema.contains(&VarId(1)));
    }

    #[test]
    fn test_bind_with_post_filter_builds_successfully() {
        // ?s :age ?x . BIND(?x + 10 AS ?y) . FILTER(?y > 25)
        // The filter depends on ?y which is the BIND output.
        // It should be fused into the BindOperator.
        let patterns = vec![
            Pattern::Triple(make_pattern(VarId(0), "age", VarId(1))),
            Pattern::Bind {
                var: VarId(2),
                expr: Expression::add(
                    Expression::Var(VarId(1)),
                    Expression::Const(FlakeValue::Long(10)),
                ),
            },
            Pattern::Filter(Expression::gt(
                Expression::Var(VarId(2)),
                Expression::Const(FlakeValue::Long(25)),
            )),
        ];

        let result = build_where_operators(&patterns, None);
        assert!(
            result.is_ok(),
            "BIND + post-BIND FILTER should build: {:?}",
            result.err()
        );

        let op = result.unwrap();
        let schema = op.schema();
        assert!(schema.contains(&VarId(0)), "subject var present");
        assert!(schema.contains(&VarId(1)), "?x var present");
        assert!(schema.contains(&VarId(2)), "?y (BIND output) var present");
    }

    // ========================================================================
    // Inline operator tests — verify that build_inline_ops produces the right
    // sequence of InlineOperator values for representative scenarios.
    // ========================================================================

    /// Helper: build a BindPattern from a var and expression.
    fn make_bind(var: VarId, expr: Expression) -> BindPattern {
        let required_vars = expr.referenced_vars().into_iter().collect();
        BindPattern {
            required_vars,
            var,
            expr,
        }
    }

    #[test]
    fn test_inline_filter_on_single_triple() {
        // ?s :age ?age . FILTER(?age > 18)
        // The filter's required var (?age) is bound by the triple,
        // so it should be inlined.
        let age = VarId(1);
        let filter_expr = Expression::gt(
            Expression::Var(age),
            Expression::Const(FlakeValue::Long(18)),
        );
        let available: HashSet<VarId> = [VarId(0), age].into();

        let (ops, remaining_binds, remaining_filters) = build_inline_ops(
            Vec::new(),
            vec![FilterPattern::new(0, filter_expr.clone())],
            &available,
            &[],
        );

        assert_eq!(ops.len(), 1, "filter should be inlined");
        assert!(
            matches!(&ops[0], InlineOperator::Filter(_)),
            "should be a Filter"
        );
        assert!(remaining_binds.is_empty());
        assert!(remaining_filters.is_empty());
    }

    #[test]
    fn test_inline_bind_on_single_triple() {
        // ?s :age ?age . BIND(?age + 1 AS ?age2)
        // The bind's required var (?age) is bound by the triple,
        // so it should be inlined.
        let age = VarId(1);
        let age2 = VarId(2);
        let bind_expr =
            Expression::add(Expression::Var(age), Expression::Const(FlakeValue::Long(1)));
        let available: HashSet<VarId> = [VarId(0), age].into();

        let (ops, remaining_binds, remaining_filters) = build_inline_ops(
            vec![make_bind(age2, bind_expr)],
            Vec::new(),
            &available,
            &[],
        );

        assert_eq!(ops.len(), 1, "bind should be inlined");
        assert!(
            matches!(&ops[0], InlineOperator::Bind { var, .. } if *var == age2),
            "should be a Bind targeting ?age2"
        );
        assert!(remaining_binds.is_empty());
        assert!(remaining_filters.is_empty());
    }

    #[test]
    fn test_inline_bind_unlocks_filter() {
        // ?s :age ?age . BIND(?age + 10 AS ?y) . FILTER(?y > 25)
        // The bind is eligible (depends on ?age), and the filter depends on ?y
        // which isn't available until the bind executes. Both should be inlined,
        // with the filter after the bind.
        let age = VarId(1);
        let y = VarId(2);
        let bind_expr = Expression::add(
            Expression::Var(age),
            Expression::Const(FlakeValue::Long(10)),
        );
        let filter_expr =
            Expression::gt(Expression::Var(y), Expression::Const(FlakeValue::Long(25)));
        let available: HashSet<VarId> = [VarId(0), age].into();

        let (ops, remaining_binds, remaining_filters) = build_inline_ops(
            vec![make_bind(y, bind_expr)],
            vec![FilterPattern::new(0, filter_expr)],
            &available,
            &[],
        );

        assert_eq!(ops.len(), 2, "bind + unlocked filter should be inlined");
        assert!(
            matches!(&ops[0], InlineOperator::Bind { var, .. } if *var == y),
            "bind should come first"
        );
        assert!(
            matches!(&ops[1], InlineOperator::Filter(_)),
            "filter should follow the bind that unlocked it"
        );
        assert!(remaining_binds.is_empty());
        assert!(remaining_filters.is_empty());
    }

    #[test]
    fn test_inline_chained_binds() {
        // ?s :age ?age . BIND(?age + 1 AS ?a) . BIND(?a * 2 AS ?b)
        // ?b depends on ?a which depends on ?age. Both should be inlined
        // in dependency order.
        let age = VarId(1);
        let a = VarId(2);
        let b = VarId(3);
        let bind_a = make_bind(
            a,
            Expression::add(Expression::Var(age), Expression::Const(FlakeValue::Long(1))),
        );
        let bind_b = make_bind(
            b,
            Expression::mul(Expression::Var(a), Expression::Const(FlakeValue::Long(2))),
        );
        let available: HashSet<VarId> = [VarId(0), age].into();

        let (ops, remaining_binds, remaining_filters) =
            build_inline_ops(vec![bind_a, bind_b], Vec::new(), &available, &[]);

        assert_eq!(ops.len(), 2, "both binds should be inlined");
        assert!(
            matches!(&ops[0], InlineOperator::Bind { var, .. } if *var == a),
            "?a should be first (no dependencies beyond ?age)"
        );
        assert!(
            matches!(&ops[1], InlineOperator::Bind { var, .. } if *var == b),
            "?b should be second (depends on ?a)"
        );
        assert!(remaining_binds.is_empty());
        assert!(remaining_filters.is_empty());
    }

    #[test]
    fn test_non_inlinable_bind_remains() {
        // ?s :age ?age . BIND(?name AS ?alias)
        // ?name is not in the available vars, so the bind can't be inlined.
        let name = VarId(3);
        let alias = VarId(4);
        let bind_expr = Expression::Var(name);
        let available: HashSet<VarId> = [VarId(0), VarId(1)].into();

        let (ops, remaining_binds, remaining_filters) = build_inline_ops(
            vec![make_bind(alias, bind_expr)],
            Vec::new(),
            &available,
            &[],
        );

        assert!(ops.is_empty(), "nothing should be inlined");
        assert_eq!(remaining_binds.len(), 1, "bind should remain");
        assert!(remaining_filters.is_empty());
    }

    #[test]
    fn test_inline_filter_into_join() {
        // ?s :name ?name . ?s :age ?age . FILTER(?age > 18)
        // After the second triple, ?age is available, so the filter
        // should be inlined into the join operator.
        let s = VarId(0);
        let name = VarId(1);
        let age = VarId(2);

        let patterns = vec![
            Pattern::Triple(make_pattern(s, "name", name)),
            Pattern::Triple(make_pattern(s, "age", age)),
            Pattern::Filter(Expression::gt(
                Expression::Var(age),
                Expression::Const(FlakeValue::Long(18)),
            )),
        ];

        let op = build_where_operators(&patterns, None).unwrap();

        // The filter should be inlined into the join, not wrapped as a
        // separate FilterOperator. If it were a wrapper, the outermost
        // operator's schema would still contain the same vars, but the
        // schema should include ?s, ?name, ?age with no extra wrapper layer.
        let schema = op.schema();
        assert_eq!(schema.len(), 3);
        assert!(schema.contains(&s));
        assert!(schema.contains(&name));
        assert!(schema.contains(&age));
    }

    #[test]
    fn test_inline_bind_into_join() {
        // ?s :name ?name . ?s :age ?age . BIND(?age + 10 AS ?y)
        // After the second triple, ?age is available, so the bind should be
        // inlined into the join. The schema should include ?y without a
        // separate BindOperator wrapper.
        let s = VarId(0);
        let name = VarId(1);
        let age = VarId(2);
        let y = VarId(3);

        let patterns = vec![
            Pattern::Triple(make_pattern(s, "name", name)),
            Pattern::Triple(make_pattern(s, "age", age)),
            Pattern::Bind {
                var: y,
                expr: Expression::add(
                    Expression::Var(age),
                    Expression::Const(FlakeValue::Long(10)),
                ),
            },
        ];

        let op = build_where_operators(&patterns, None).unwrap();

        // ?y should appear in the schema — it was inlined into the join,
        // extending the join's output schema rather than requiring a
        // BindOperator wrapper.
        let schema = op.schema();
        assert_eq!(schema.len(), 4, "schema should have s, name, age, y");
        assert!(schema.contains(&s));
        assert!(schema.contains(&name));
        assert!(schema.contains(&age));
        assert!(schema.contains(&y));
    }

    #[test]
    fn test_inline_bind_into_scan_extends_schema() {
        // ?s :age ?age . BIND(?age + 10 AS ?y)
        // Single triple + bind: the bind should be inlined into the scan,
        // and the scan's schema should include ?y.
        let s = VarId(0);
        let age = VarId(1);
        let y = VarId(2);

        let patterns = vec![
            Pattern::Triple(make_pattern(s, "age", age)),
            Pattern::Bind {
                var: y,
                expr: Expression::add(
                    Expression::Var(age),
                    Expression::Const(FlakeValue::Long(10)),
                ),
            },
        ];

        let op = build_where_operators(&patterns, None).unwrap();

        // If the bind were a separate BindOperator, the top-level operator
        // would be a BindOperator wrapping a scan operator. But since it's
        // inlined, the scan operator itself has the extended schema.
        let schema = op.schema();
        assert_eq!(schema.len(), 3, "schema should have s, age, y");
        assert!(schema.contains(&s));
        assert!(schema.contains(&age));
        assert!(schema.contains(&y));
    }

    #[test]
    fn test_scan_index_hint_prefers_opst_for_prefix_filter() {
        let tp = TriplePattern::new(
            Ref::Var(VarId(0)),
            Ref::Sid(fluree_db_core::Sid::new(100, "name")),
            Term::Var(VarId(1)),
        );
        let filter = InlineOperator::Filter(PreparedBoolExpression::new(Expression::call(
            crate::ir::Function::StrStarts,
            vec![
                Expression::Var(VarId(1)),
                Expression::Const(FlakeValue::String("Ali".to_string())),
            ],
        )));

        let hint = scan_index_hint_for_triple(&tp, &[VarId(1)], &[filter]);
        assert_eq!(hint, Some(IndexType::Opst));
    }

    #[test]
    fn test_scan_index_hint_prefers_opst_for_str_wrapper_with_string_dtc() {
        let tp = TriplePattern {
            s: Ref::Var(VarId(0)),
            p: Ref::Sid(fluree_db_core::Sid::new(100, "name")),
            o: Term::Var(VarId(1)),
            dtc: Some(fluree_db_core::DatatypeConstraint::Explicit(
                fluree_db_core::Sid::new(
                    fluree_vocab::namespaces::XSD,
                    fluree_vocab::xsd_names::STRING,
                ),
            )),
        };
        let filter = InlineOperator::Filter(PreparedBoolExpression::new(Expression::call(
            crate::ir::Function::Regex,
            vec![
                Expression::call(crate::ir::Function::Str, vec![Expression::Var(VarId(1))]),
                Expression::Const(FlakeValue::String("^Ali".to_string())),
            ],
        )));

        let hint = scan_index_hint_for_triple(&tp, &[], &[filter]);
        assert_eq!(hint, Some(IndexType::Opst));
    }

    #[test]
    fn test_scan_index_hint_rejects_str_wrapper_without_string_dtc() {
        let tp = TriplePattern::new(
            Ref::Var(VarId(0)),
            Ref::Sid(fluree_db_core::Sid::new(100, "name")),
            Term::Var(VarId(1)),
        );
        let filter = InlineOperator::Filter(PreparedBoolExpression::new(Expression::call(
            crate::ir::Function::Regex,
            vec![
                Expression::call(crate::ir::Function::Str, vec![Expression::Var(VarId(1))]),
                Expression::Const(FlakeValue::String("^Ali".to_string())),
            ],
        )));

        let hint = scan_index_hint_for_triple(&tp, &[], &[filter]);
        assert_eq!(hint, None);
    }

    // ========================================================================
    // EXISTS / NOT EXISTS strategy decision
    //
    // These pin the dispatch decision independent of operator construction —
    // a regression to per-row ExistsOperator for the root-level absence shape
    // would silently slow real workloads (see the OPTIONAL+!bound rewrite at
    // `Pattern::Optional` dispatch) without changing query results.
    // ========================================================================

    /// Outer triple binds `?f`; inner triple `?f <parent> ?p` shares `?f` via
    /// a produced var and introduces fresh `?p`. This is the root-level
    /// absence shape (`["not-exists", {"@id":"?f","storage:parent":"?p"}]`
    /// after `{"@id":"?f","@type":"storage:File"}`) — must pick Semijoin so
    /// the inner is built once instead of rebuilt per outer row.
    #[test]
    fn choose_exists_strategy_semijoin_for_root_level_absence_shape() {
        let f = VarId(0);
        let p = VarId(1);
        let outer_schema = [f];
        let inner = [Pattern::Triple(make_pattern(f, "parent", p))];

        let strategy = choose_exists_strategy(&outer_schema, &inner);

        assert_eq!(
            strategy,
            ExistsStrategy::Semijoin { key_vars: vec![f] },
            "expected Semijoin for shared-produced-var inner, got {strategy:?}"
        );
    }

    /// Inner references no outer var: fully uncorrelated → per-row Exists
    /// is correct (and Semijoin would have an empty key set).
    #[test]
    fn choose_exists_strategy_exists_when_uncorrelated() {
        let outer_schema = [VarId(0)];
        let inner = [Pattern::Triple(make_pattern(VarId(10), "p", VarId(11)))];

        let strategy = choose_exists_strategy(&outer_schema, &inner);

        assert_eq!(
            strategy,
            ExistsStrategy::Exists {
                reason: ExistsFallbackReason::Uncorrelated,
            }
        );
    }

    /// Inner references an outer-only var via FILTER (not via a producing
    /// pattern): the inner cannot run standalone, so the strategy must fall
    /// back to per-row Exists even though there is a shared produced var.
    #[test]
    fn choose_exists_strategy_exists_when_outer_only_consumed() {
        let f = VarId(0);
        let p = VarId(1);
        let q = VarId(2); // outer-only — consumed by inner FILTER, not produced

        let outer_schema = [f, q];
        let filter_eq = Expression::call(
            crate::ir::Function::Eq,
            vec![Expression::Var(p), Expression::Var(q)],
        );
        let inner = [
            Pattern::Triple(make_pattern(f, "parent", p)),
            Pattern::Filter(filter_eq),
        ];

        let strategy = choose_exists_strategy(&outer_schema, &inner);

        assert_eq!(
            strategy,
            ExistsStrategy::Exists {
                reason: ExistsFallbackReason::OuterOnlyConsumed,
            },
            "outer-only consumed must dominate over the shared produced var, got {strategy:?}"
        );
    }

    /// Empty outer schema (NotExists at root with no preceding sources) →
    /// no key vars → per-row Exists.
    #[test]
    fn choose_exists_strategy_exists_when_outer_schema_empty() {
        let outer_schema: [VarId; 0] = [];
        let inner = [Pattern::Triple(make_pattern(VarId(0), "p", VarId(1)))];

        let strategy = choose_exists_strategy(&outer_schema, &inner);

        assert_eq!(
            strategy,
            ExistsStrategy::Exists {
                reason: ExistsFallbackReason::Uncorrelated,
            }
        );
    }

    /// Key var order tracks the outer schema order (column layout), not the
    /// order the inner happens to produce them. This is load-bearing for
    /// `SemijoinOperator`'s column lookup at open time.
    #[test]
    fn choose_exists_strategy_semijoin_key_vars_follow_outer_schema_order() {
        let a = VarId(0);
        let b = VarId(1);
        let outer_schema = [a, b];
        // Inner triple is (?b <p> ?a) — produces both ?b and ?a, but in the
        // opposite order from the outer schema.
        let inner = [Pattern::Triple(make_pattern(b, "p", a))];

        let strategy = choose_exists_strategy(&outer_schema, &inner);

        assert_eq!(
            strategy,
            ExistsStrategy::Semijoin {
                key_vars: vec![a, b],
            },
        );
    }
}
