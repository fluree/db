//! Operator tree building
//!
//! Builds the complete operator tree for a query including:
//! WHERE patterns → GROUP BY → Aggregates → HAVING → ORDER BY → PROJECT → DISTINCT → OFFSET → LIMIT

use crate::ir::AggregateFn;
use crate::aggregate::AggregateOperator;
use crate::ir::AggregateSpec;
use crate::binary_scan::EmitMask;
use crate::count_rows::CountRowsOperator;
use crate::distinct::DistinctOperator;
use crate::error::{QueryError, Result};
use crate::eval::PreparedBoolExpression;
use crate::fast_count::{
    count_blank_node_subjects_operator, count_distinct_object_operator,
    count_distinct_objects_operator, count_distinct_predicates_operator,
    count_distinct_subjects_operator, count_literal_objects_operator,
    count_rows_lang_filter_operator, count_rows_numeric_compare_operator, count_rows_operator,
    count_triples_operator, NumericCompareOp,
};
use crate::fast_exists_join_count_distinct_object::exists_join_count_distinct_object_operator;
use crate::fast_fused_scan_sum::{
    fused_scan_sum_i64_operator, DateComponentFn, NumericUnaryFn, SumExprI64,
};
use crate::fast_group_count_firsts::{
    GroupByObjectStarTopKOperator, PredicateGroupCountFirstsOperator,
    PredicateObjectCountFirstsOperator,
};
use crate::fast_label_regex_type::label_regex_type_operator;
use crate::fast_min_max_string::{
    predicate_avg_numeric_operator, predicate_min_max_string_operator, MinMaxMode,
};
use crate::fast_multicolumn_join_count_all::multicolumn_join_count_all_operator;
use crate::fast_optional_chain_head_count_all::predicate_optional_chain_head_count_all;
use crate::fast_property_path_plus_count_all::property_path_plus_count_all_operator;
use crate::fast_star_const_order_topk::star_const_ordered_limit_operator;
use crate::fast_string_prefix_count_all::{
    string_prefix_count_all_operator, string_prefix_sum_strstarts_operator,
};
use crate::fast_sum_strlen_group_concat::sum_strlen_group_concat_operator;
use crate::fast_transitive_path_plus_count_all::transitive_path_plus_count_all_operator;
use crate::fast_union_star_count_all::{UnionCountMode, UnionStarCountAllOperator};
use crate::group_aggregate::{GroupAggregateOperator, StreamingAggSpec};
use crate::groupby::GroupByOperator;
use crate::having::HavingOperator;
use crate::ir::triple::{Ref, Term, TriplePattern};
use crate::ir::Aggregation;
use crate::ir::Expression;
use crate::ir::Grouping;
use crate::ir::QueryOptions;
use crate::ir::{PathModifier, Pattern};
use crate::ir::{Query, QueryOutput};
use crate::limit::LimitOperator;
use crate::offset::OffsetOperator;
use crate::operator::inline::InlineOperator;
use crate::operator::BoxedOperator;
use crate::project::ProjectOperator;
use crate::sort::SortDirection;
use crate::sort::SortOperator;
use crate::stats_query::StatsCountByPredicateOperator;
use crate::temporal_mode::PlanningContext;
use crate::var_registry::VarId;
use fluree_db_core::StatsView;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::dependency::compute_variable_deps;
use super::where_plan::build_where_operators_with_needed;
use super::where_plan::collect_var_stats;

// ---------------------------------------------------------------------------
// Shared detection helpers
// ---------------------------------------------------------------------------

/// Extract a bound predicate (IRI or SID) from a triple pattern's predicate position.
/// Returns `None` if the predicate is a variable or other non-bound form.
pub(crate) fn extract_bound_predicate(p: &Ref) -> Option<Ref> {
    p.is_bound().then(|| p.clone())
}

/// Validate a triple pattern as `?s <bound_pred> ?o` with no datatype constraint.
/// Returns `(subject_var, bound_predicate, object_var)`.
pub(crate) fn validate_simple_triple(tp: &TriplePattern) -> Option<(VarId, Ref, VarId)> {
    let Ref::Var(sv) = &tp.s else { return None };
    let pred = extract_bound_predicate(&tp.p)?;
    let Term::Var(ov) = &tp.o else { return None };
    if tp.dtc.is_some() {
        return None;
    }
    Some((*sv, pred, *ov))
}

/// Find a 2-hop chain pattern `?a <p1> ?b . ?b <p2> ?c` in two triples (trying both orderings).
///
/// Returns `(a_var, pred1, b_var, pred2, c_var)` or `None` if neither ordering forms a chain.
/// Both triples must be simple (var subject, bound pred, var object, no dtc).
fn find_two_hop_chain(
    t1: &TriplePattern,
    t2: &TriplePattern,
) -> Option<(VarId, Ref, VarId, Ref, VarId)> {
    let try_order =
        |x: &TriplePattern, y: &TriplePattern| -> Option<(VarId, Ref, VarId, Ref, VarId)> {
            let (a, p1, b1) = validate_simple_triple(x)?;
            let (b2, p2, c) = validate_simple_triple(y)?;
            if b1 != b2 {
                return None;
            }
            Some((a, p1, b1, p2, c))
        };
    try_order(t1, t2).or_else(|| try_order(t2, t1))
}

#[derive(Clone)]
struct StarConstOrderTopKSpec {
    subject_var: VarId,
    label_var: VarId,
    label_pred: Ref,
    const_constraints: Vec<(Ref, Term)>,
    numeric_pred: Ref,
    numeric_threshold: crate::ir::triple::Term, // stored as Term::Value for convenience
    limit: usize,
}

/// Detect a common benchmark shape:
/// - Same-subject star constraints with **constant IRI-ref objects**: `?s <p> <o>`
/// - One numeric predicate with `FILTER(?v > K)` used only as an existence constraint
/// - One label predicate whose object var is ORDER BY key
/// - SELECT DISTINCT of exactly `(?s, ?label)` plus `ORDER BY ?label LIMIT k`
fn detect_star_const_numeric_label_order_limit(
    query: &Query,
    options: &QueryOptions,
) -> Option<StarConstOrderTopKSpec> {
    if !query.output.is_distinct()
        || options.offset.is_some()
        || query.grouping.is_some()
    {
        return None;
    }
    let limit = options.limit?;
    if limit == 0 {
        return None;
    }
    if query.ordering.len() != 1 {
        return None;
    }
    let ob = &query.ordering[0];
    if ob.direction != SortDirection::Ascending {
        return None;
    }
    let label_var = ob.var;

    // Must select exactly (?s, ?label) in any order.
    let select_vars = query.output.projected_vars()?;
    if select_vars.len() != 2 || !select_vars.contains(&label_var) {
        return None;
    }

    // Only triples + a single FILTER.
    let mut triples: Vec<&TriplePattern> = Vec::new();
    let mut filters: Vec<&crate::ir::Expression> = Vec::new();
    for p in &query.patterns {
        match p {
            crate::ir::Pattern::Triple(tp) => triples.push(tp),
            crate::ir::Pattern::Filter(expr) => filters.push(expr),
            _ => return None,
        }
    }
    if triples.len() < 3 || filters.len() != 1 {
        return None;
    }

    // All triples share the same subject var.
    let mut subject_var: Option<VarId> = None;
    for tp in &triples {
        let Ref::Var(sv) = &tp.s else { return None };
        match subject_var {
            None => subject_var = Some(*sv),
            Some(x) if x != *sv => return None,
            _ => {}
        }
        if tp.dtc.is_some() {
            return None;
        }
        if !tp.p_bound() {
            return None;
        }
    }
    let subject_var = subject_var?;
    if !select_vars.contains(&subject_var) {
        return None;
    }

    // Find the label triple: ?s <p_label> ?label where ?label is ORDER BY var.
    let mut label_pred: Option<Ref> = None;
    for tp in &triples {
        if matches!(&tp.o, Term::Var(v) if *v == label_var) {
            if label_pred.is_some() {
                return None;
            }
            label_pred = Some(tp.p.clone());
        }
    }
    let label_pred = label_pred?;

    // Extract numeric threshold from FILTER(?v > K) and find matching triple ?s <p_num> ?v.
    let (value_var, thr_value) = extract_simple_gt_threshold(filters[0])?;
    let mut numeric_pred: Option<Ref> = None;
    for tp in &triples {
        if matches!(&tp.o, Term::Var(v) if *v == value_var) {
            if numeric_pred.is_some() {
                return None;
            }
            numeric_pred = Some(tp.p.clone());
        }
    }
    let numeric_pred = numeric_pred?;
    // Numeric var must not be selected.
    if select_vars.contains(&value_var) {
        return None;
    }

    // All remaining triples must be constant IRI-ref object constraints: ?s <p> <oRef>
    let mut const_constraints: Vec<(Ref, Term)> = Vec::new();
    for tp in &triples {
        if tp.p == label_pred && matches!(&tp.o, Term::Var(v) if *v == label_var) {
            continue;
        }
        if tp.p == numeric_pred && matches!(&tp.o, Term::Var(v) if *v == value_var) {
            continue;
        }
        match &tp.o {
            Term::Sid(_) | Term::Iri(_) => const_constraints.push((tp.p.clone(), tp.o.clone())),
            _ => return None,
        }
    }
    if const_constraints.is_empty() {
        return None;
    }

    Some(StarConstOrderTopKSpec {
        subject_var,
        label_var,
        label_pred,
        const_constraints,
        numeric_pred,
        numeric_threshold: Term::Value(thr_value),
        limit,
    })
}

fn extract_simple_gt_threshold(
    expr: &crate::ir::Expression,
) -> Option<(VarId, fluree_db_core::FlakeValue)> {
    let (var, op, threshold) = extract_simple_numeric_compare_threshold(expr)?;
    if op != NumericCompareOp::Gt {
        return None;
    }
    Some((var, threshold))
}

fn extract_simple_numeric_compare_threshold(
    expr: &crate::ir::Expression,
) -> Option<(VarId, NumericCompareOp, fluree_db_core::FlakeValue)> {
    use crate::ir::{Expression, FlakeValue, Function};
    let Expression::Call { func, args } = expr else {
        return None;
    };
    if args.len() != 2 {
        return None;
    }
    let const_to_flake = |c: &FlakeValue| match c {
        FlakeValue::Long(n) => Some(fluree_db_core::FlakeValue::Long(*n)),
        FlakeValue::Double(d) => Some(fluree_db_core::FlakeValue::Double(*d)),
        _ => None,
    };
    let direct_op = match *func {
        Function::Gt => NumericCompareOp::Gt,
        Function::Ge => NumericCompareOp::Ge,
        Function::Lt => NumericCompareOp::Lt,
        Function::Le => NumericCompareOp::Le,
        _ => return None,
    };
    let reverse_op = match direct_op {
        NumericCompareOp::Gt => NumericCompareOp::Lt,
        NumericCompareOp::Ge => NumericCompareOp::Le,
        NumericCompareOp::Lt => NumericCompareOp::Gt,
        NumericCompareOp::Le => NumericCompareOp::Ge,
    };

    match (&args[0], &args[1]) {
        (Expression::Var(v), Expression::Const(c)) => Some((*v, direct_op, const_to_flake(c)?)),
        (Expression::Const(c), Expression::Var(v)) => Some((*v, reverse_op, const_to_flake(c)?)),
        _ => None,
    }
}

#[derive(Clone)]
struct LabelRegexTypeSpec {
    subject_var: VarId,
    label_var: VarId,
    label_pred: Ref,
    class_term: Term,
    regex_pattern: Arc<str>,
    regex_flags: Arc<str>,
}

/// Detect:
/// `?s rdfs:label ?label . ?s rdf:type <Class> . FILTER regex(?label, "pat"[, "flags"])`
/// with plain SELECT of exactly `(?s, ?label)` (no ORDER BY/LIMIT/DISTINCT).
fn detect_label_regex_type(query: &Query, options: &QueryOptions) -> Option<LabelRegexTypeSpec> {
    if query.output.is_distinct()
        || options.limit.is_some()
        || options.offset.is_some()
        || !query.ordering.is_empty()
        || query.grouping.is_some()
    {
        return None;
    }

    let select = query.output.projected_vars()?;
    if select.len() != 2 {
        return None;
    }

    let mut triples: Vec<&TriplePattern> = Vec::new();
    let mut filters: Vec<&crate::ir::Expression> = Vec::new();
    for p in &query.patterns {
        match p {
            Pattern::Triple(tp) => triples.push(tp),
            Pattern::Filter(expr) => filters.push(expr),
            _ => return None,
        }
    }
    if triples.len() != 2 || filters.len() != 1 {
        return None;
    }

    // Determine subject var (must be same in both).
    let Ref::Var(sv0) = &triples[0].s else {
        return None;
    };
    let Ref::Var(sv1) = &triples[1].s else {
        return None;
    };
    if sv0 != sv1 {
        return None;
    }
    let subject_var = *sv0;
    if !select.contains(&subject_var) {
        return None;
    }

    // Find label triple (?s <p> ?label) and type triple (?s rdf:type <Class>).
    let mut label_pred: Option<Ref> = None;
    let mut label_var: Option<VarId> = None;
    let mut class_term: Option<Term> = None;
    for tp in &triples {
        if tp.dtc.is_some() || !tp.p_bound() {
            return None;
        }
        if matches!(&tp.o, Term::Var(_)) {
            // Candidate label triple.
            let Term::Var(lv) = &tp.o else { unreachable!() };
            if label_pred.is_some() {
                return None;
            }
            label_pred = Some(tp.p.clone());
            label_var = Some(*lv);
        } else if tp.p.is_rdf_type() {
            // Candidate type triple.
            class_term = Some(tp.o.clone());
        } else {
            return None;
        }
    }
    let (label_pred, label_var, class_term) = (label_pred?, label_var?, class_term?);
    if !select.contains(&label_var) {
        return None;
    }

    // Filter must be regex(?label, "pat"[, "flags"]) with constant strings.
    let (pattern, flags) = extract_regex_const_pattern(filters[0], label_var)?;

    Some(LabelRegexTypeSpec {
        subject_var,
        label_var,
        label_pred,
        class_term,
        regex_pattern: pattern,
        regex_flags: flags,
    })
}

fn extract_regex_const_pattern(
    expr: &crate::ir::Expression,
    label_var: VarId,
) -> Option<(Arc<str>, Arc<str>)> {
    use crate::ir::{Expression, FlakeValue, Function};
    let Expression::Call { func, args } = expr else {
        return None;
    };
    if *func != Function::Regex {
        return None;
    }
    if args.len() != 2 && args.len() != 3 {
        return None;
    }
    if !matches!(&args[0], Expression::Var(v) if *v == label_var) {
        return None;
    }
    let Expression::Const(FlakeValue::String(pat)) = &args[1] else {
        return None;
    };
    let flags: Arc<str> = if args.len() == 3 {
        let Expression::Const(FlakeValue::String(f)) = &args[2] else {
            return None;
        };
        Arc::from(f.as_str())
    } else {
        Arc::from("")
    };
    Some((Arc::from(pat.as_str()), flags))
}

/// Returns the sole aggregate when the query is in the canonical implicit
/// single-aggregate fast-path shape:
/// - `Grouping::Implicit` with exactly one aggregate, no `having`, no
///   post-aggregation binds.
/// - No `order_by`, no `offset`, no `DISTINCT`, and `limit != Some(0)`.
///
/// Detectors that look for a specific aggregate function start here, then
/// inspect the spec's function/distinct/input_var fields.
fn implicit_single_aggregate<'a>(
    query: &'a Query,
    options: &QueryOptions,
) -> Option<&'a AggregateSpec> {
    let Some(Grouping::Implicit {
        aggregation: Aggregation { aggregates, binds },
        having: None,
    }) = &query.grouping
    else {
        return None;
    };
    if aggregates.len() != 1
        || !binds.is_empty()
        || !query.ordering.is_empty()
        || options.offset.is_some()
        || query.output.is_distinct()
        || options.limit == Some(0)
    {
        return None;
    }
    Some(aggregates.first())
}

/// Validate that a query has a single `COUNT(*)` aggregate with standard constraints.
///
/// Returns `Some(output_var)` if the query has:
/// - SELECT output (not CONSTRUCT/BOOLEAN/WILDCARD)
/// - Exactly one aggregate: `COUNT(*)` (not distinct, no input var)
/// - No group_by, having, post-aggregation binds, order_by, offset, or DISTINCT
/// - LIMIT >= 1 (or no limit)
/// - SELECT vars == `[agg.output_var]`
pub(crate) fn detect_count_all_aggregate(query: &Query, options: &QueryOptions) -> Option<VarId> {
    let agg = implicit_single_aggregate(query, options)?;
    if agg.distinct || !matches!(agg.function, AggregateFn::CountAll) || agg.input_var.is_some() {
        return None;
    }
    let select_vars = query.output.projected_vars()?;
    if select_vars.len() != 1 || select_vars[0] != agg.output_var {
        return None;
    }
    Some(agg.output_var)
}

/// Validate that a query has a single `COUNT(DISTINCT ?var)` aggregate with standard constraints.
///
/// Returns `Some((input_var, output_var))` if the query has:
/// - SELECT output (not CONSTRUCT/BOOLEAN/WILDCARD)
/// - Exactly one aggregate: `COUNT(DISTINCT ?var)`
/// - No group_by, having, post-aggregation binds, order_by, offset, or DISTINCT
/// - LIMIT >= 1 (or no limit)
/// - SELECT vars == `[agg.output_var]`
fn detect_count_distinct_aggregate(
    query: &Query,
    options: &QueryOptions,
) -> Option<(VarId, VarId)> {
    let agg = implicit_single_aggregate(query, options)?;
    if agg.distinct || !matches!(agg.function, AggregateFn::CountDistinct) {
        return None;
    }
    let in_var = agg.input_var?;
    let select_vars = query.output.projected_vars()?;
    if select_vars.len() != 1 || select_vars[0] != agg.output_var {
        return None;
    }
    Some((in_var, agg.output_var))
}

/// Validate that a query has a single `COUNT(*)` or `COUNT(?var)` aggregate.
///
/// Returns `Some((input_var, output_var))` where `input_var` is `None` for `COUNT(*)`.
/// Same standard constraints as [`detect_count_all_aggregate`].
fn detect_count_aggregate(query: &Query, options: &QueryOptions) -> Option<(Option<VarId>, VarId)> {
    let agg = implicit_single_aggregate(query, options)?;
    if agg.distinct {
        return None;
    }
    let input_var = match agg.function {
        AggregateFn::CountAll if agg.input_var.is_none() => None,
        AggregateFn::Count => Some(agg.input_var?),
        _ => return None,
    };
    let select_vars = query.output.projected_vars()?;
    if select_vars.len() != 1 || select_vars[0] != agg.output_var {
        return None;
    }
    Some((input_var, agg.output_var))
}

fn detect_partitioned_group_by(query: &Query, _options: &QueryOptions) -> bool {
    let Some(Grouping::Explicit { group_by, .. }) = &query.grouping else {
        return false;
    };
    if group_by.len() != 1 {
        return false;
    }
    let gb = *group_by.first();

    // Strict: only a single triple pattern plus order-preserving operators (FILTER/BIND).
    let mut triple: Option<&crate::ir::triple::TriplePattern> = None;
    for p in &query.patterns {
        match p {
            Pattern::Triple(tp) => {
                if triple.is_some() {
                    return false;
                }
                triple = Some(tp);
            }
            Pattern::Filter(_) | Pattern::Bind { .. } => {}
            _ => return false,
        }
    }
    let Some(tp) = triple else {
        return false;
    };

    // Must be ?s <p> ?o and group key must be either ?s or ?o.
    if !tp.p_bound() {
        return false;
    }
    let Ref::Var(sv) = &tp.s else {
        return false;
    };
    let Term::Var(ov) = &tp.o else {
        return false;
    };
    gb == *sv || gb == *ov
}

fn detect_predicate_group_by_object_count_topk(
    query: &Query,
    options: &QueryOptions,
) -> Option<(Ref, VarId, VarId, VarId, usize)> {
    if matches!(query.output, QueryOutput::Construct(_) | QueryOutput::Ask) {
        return None;
    }
    if query.patterns.len() != 1 {
        return None;
    }
    let Pattern::Triple(tp) = &query.patterns[0] else {
        return None;
    };
    let (s_var, pred, o_var) = validate_simple_triple(tp)?;

    // GROUP BY ?object with exactly one aggregate, no HAVING.
    let Some(Grouping::Explicit {
        group_by,
        aggregation: Some(Aggregation { aggregates, binds }),
        having: None,
    }) = &query.grouping
    else {
        return None;
    };
    if group_by.len() != 1 || *group_by.first() != o_var {
        return None;
    }
    if aggregates.len() != 1 {
        return None;
    }
    let agg = aggregates.first();
    if agg.distinct {
        return None;
    }
    let is_count = matches!(agg.function, AggregateFn::Count | AggregateFn::CountAll);
    if !is_count {
        return None;
    }
    if matches!(agg.function, AggregateFn::Count) && agg.input_var != Some(s_var) {
        return None;
    }
    if !binds.is_empty() {
        return None;
    }
    // ORDER BY DESC(?count) and LIMIT k required so we can do top-k directly.
    let limit = options.limit?;
    if query.ordering.len() != 1 {
        return None;
    }
    let ob = &query.ordering[0];
    if ob.var != agg.output_var || ob.direction != crate::sort::SortDirection::Descending {
        return None;
    }
    Some((pred, s_var, o_var, agg.output_var, limit))
}

/// Detect `GROUP BY ?o` top-k where WHERE is a same-subject star join:
/// `?s <p_group> ?o . ?s <p_filter1> ?x1 . ...`
///
/// Supports subject aggregates: MIN(?s), MAX(?s), SAMPLE(?s) in addition to COUNT.
#[allow(clippy::type_complexity)]
fn detect_group_by_object_star_topk(
    query: &Query,
    options: &QueryOptions,
) -> Option<(
    Ref,
    Vec<Ref>,
    Arc<[VarId]>,
    VarId,
    VarId,
    VarId,
    Option<VarId>,
    Option<VarId>,
    Option<VarId>,
    usize,
)> {
    let select_vars: Arc<[VarId]> = Arc::from(query.output.projected_vars()?.into_boxed_slice());
    let Some(Grouping::Explicit {
        group_by,
        aggregation: Some(Aggregation { aggregates, binds }),
        having: None,
    }) = &query.grouping
    else {
        return None;
    };
    if group_by.len() != 1 {
        return None;
    }
    let group_var = *group_by.first();
    if query.output.is_distinct() || !binds.is_empty() {
        return None;
    }
    if options.offset.is_some() {
        return None;
    }
    let limit = options.limit?;
    if query.ordering.len() != 1 {
        return None;
    }
    if query.patterns.len() < 2 {
        return None;
    }

    // All patterns must be triples with the same subject var.
    let mut subj_var: Option<VarId> = None;
    let mut group_tp: Option<&TriplePattern> = None;
    let mut filter_preds: Vec<Ref> = Vec::new();
    for p in &query.patterns {
        let Pattern::Triple(tp) = p else {
            return None;
        };
        let (sv, pred, ov) = validate_simple_triple(tp)?;
        if subj_var.is_none() {
            subj_var = Some(sv);
        } else if subj_var != Some(sv) {
            return None;
        }
        if ov == group_var {
            if group_tp.is_some() {
                return None;
            }
            group_tp = Some(tp);
        } else {
            filter_preds.push(pred);
        }
    }
    let subj_var = subj_var?;
    let group_tp = group_tp?;
    let group_pred = extract_bound_predicate(&group_tp.p)?;
    if filter_preds.is_empty() {
        return None;
    }

    // Aggregates: require COUNT (or COUNT(*)) and allow MIN/MAX/SAMPLE on ?s.
    let mut count_out: Option<VarId> = None;
    let mut min_out: Option<VarId> = None;
    let mut max_out: Option<VarId> = None;
    let mut sample_out: Option<VarId> = None;
    for agg in aggregates.iter() {
        if agg.distinct {
            return None;
        }
        match agg.function {
            AggregateFn::CountAll => {
                if count_out.is_some() {
                    return None;
                }
                count_out = Some(agg.output_var);
            }
            AggregateFn::Count => {
                if count_out.is_some() {
                    return None;
                }
                if agg.input_var != Some(subj_var) {
                    return None;
                }
                count_out = Some(agg.output_var);
            }
            AggregateFn::Min => {
                if min_out.is_some() || agg.input_var != Some(subj_var) {
                    return None;
                }
                min_out = Some(agg.output_var);
            }
            AggregateFn::Max => {
                if max_out.is_some() || agg.input_var != Some(subj_var) {
                    return None;
                }
                max_out = Some(agg.output_var);
            }
            AggregateFn::Sample => {
                if sample_out.is_some() || agg.input_var != Some(subj_var) {
                    return None;
                }
                sample_out = Some(agg.output_var);
            }
            _ => return None,
        }
    }
    let count_out = count_out?;

    // ORDER BY DESC(?count).
    let ob = &query.ordering[0];
    if ob.var != count_out || ob.direction != crate::sort::SortDirection::Descending {
        return None;
    }

    // SELECT vars must be exactly the group var + the aggregate output vars (in any order).
    let mut expected: Vec<VarId> = vec![group_var, count_out];
    if let Some(v) = min_out {
        expected.push(v);
    }
    if let Some(v) = max_out {
        expected.push(v);
    }
    if let Some(v) = sample_out {
        expected.push(v);
    }
    expected.sort_unstable();
    let mut actual: Vec<VarId> = select_vars.iter().copied().collect();
    actual.sort_unstable();
    if actual != expected {
        return None;
    }

    Some((
        group_pred,
        filter_preds,
        select_vars,
        subj_var,
        group_var,
        count_out,
        min_out,
        max_out,
        sample_out,
        limit,
    ))
}

fn detect_sum_strlen_group_concat_subquery(
    query: &Query,
    options: &QueryOptions,
) -> Option<(Ref, Arc<str>, VarId)> {
    use crate::ir::{Expression, Function, Pattern};

    // Outer aggregate must be SUM(?v) (where ?v is the STRLEN bind var).
    let outer_agg = implicit_single_aggregate(query, options)?;
    if outer_agg.distinct || outer_agg.function != AggregateFn::Sum {
        return None;
    }
    let strlen_var = outer_agg.input_var?;

    // Patterns: one Subquery + one Bind(strlen_var = STRLEN(?cat)).
    if query.patterns.len() != 2 {
        return None;
    }
    let mut subq: Option<&crate::ir::SubqueryPattern> = None;
    let mut bind: Option<(VarId, &Expression)> = None;
    for p in &query.patterns {
        match p {
            Pattern::Subquery(sq) => subq = Some(sq),
            Pattern::Bind { var, expr } => bind = Some((*var, expr)),
            _ => return None,
        }
    }
    let (Some(sq), Some((bind_var, bind_expr))) = (subq, bind) else {
        return None;
    };
    if bind_var != strlen_var {
        return None;
    }
    let Expression::Call { func, args } = bind_expr else {
        return None;
    };
    if *func != Function::Strlen || args.len() != 1 {
        return None;
    }
    let Expression::Var(cat_var) = &args[0] else {
        return None;
    };

    // Inner subquery must be GROUP BY ?s with GROUP_CONCAT(?o; sep) AS ?cat.
    let Some(Grouping::Explicit {
        group_by: sq_group_by,
        aggregation: Some(Aggregation { aggregates: sq_aggregates, binds: _ }),
        having: None,
    }) = &sq.grouping
    else {
        return None;
    };
    if sq_group_by.len() != 1 {
        return None;
    }
    if sq_aggregates.len() != 1 {
        return None;
    }
    let inner_agg = sq_aggregates.first();
    let (sep, input_var) = match &inner_agg.function {
        AggregateFn::GroupConcat { separator } => (separator.as_str(), inner_agg.input_var?),
        _ => return None,
    };
    if inner_agg.distinct || inner_agg.output_var != *cat_var {
        return None;
    }

    // Inner WHERE must be a single triple: ?s <p> ?o (predicate bound).
    if sq.patterns.len() != 1 {
        return None;
    }
    let Pattern::Triple(tp) = &sq.patterns[0] else {
        return None;
    };
    let Ref::Var(s_var) = &tp.s else {
        return None;
    };
    let pred = match &tp.p {
        Ref::Sid(_) | Ref::Iri(_) => tp.p.clone(),
        _ => return None,
    };
    let Term::Var(o_var) = &tp.o else {
        return None;
    };
    if tp.dtc.is_some() {
        return None;
    }
    if *sq_group_by.first() != *s_var {
        return None;
    }
    if input_var != *o_var {
        return None;
    }

    // SELECT must be exactly the aggregate output var.
    let select_vars = query.output.projected_vars()?;
    if select_vars.len() != 1 || select_vars[0] != outer_agg.output_var {
        return None;
    }

    Some((pred, Arc::from(sep), outer_agg.output_var))
}

fn detect_predicate_object_count(
    query: &Query,
    options: &QueryOptions,
) -> Option<(Ref, VarId, crate::ir::triple::Term, VarId)> {
    let (input_var, out_var) = detect_count_aggregate(query, options)?;

    if query.patterns.len() != 1 {
        return None;
    }
    let Pattern::Triple(tp) = &query.patterns[0] else {
        return None;
    };

    // Must be ?s <p> <o> (subject var, predicate bound, object bound).
    let Ref::Var(s_var) = &tp.s else {
        return None;
    };
    let pred = extract_bound_predicate(&tp.p)?;
    if matches!(&tp.o, Term::Var(_)) {
        return None;
    }
    if tp.dtc.is_some() {
        return None;
    }

    // COUNT(?var) must reference ?s (the only var in this single-triple pattern).
    if let Some(v) = input_var {
        if v != *s_var {
            return None;
        }
    }

    Some((pred, *s_var, tp.o.clone(), out_var))
}

fn detect_predicate_count_rows(query: &Query, options: &QueryOptions) -> Option<(Ref, VarId)> {
    let (input_var, out_var) = detect_count_aggregate(query, options)?;

    if query.patterns.len() != 1 {
        return None;
    }
    let Pattern::Triple(tp) = &query.patterns[0] else {
        return None;
    };
    let (s_var, pred, o_var) = validate_simple_triple(tp)?;

    // COUNT(?var) must reference ?s or ?o (both always bound in a single triple).
    if let Some(v) = input_var {
        if v != s_var && v != o_var {
            return None;
        }
    }

    Some((pred, out_var))
}

fn detect_predicate_count_rows_lang_filter(
    query: &Query,
    options: &QueryOptions,
) -> Option<(Ref, String, VarId)> {
    let (input_var, out_var) = detect_count_aggregate(query, options)?;

    if query.patterns.len() != 2 {
        return None;
    }
    let (tp, filter) = match (&query.patterns[0], &query.patterns[1]) {
        (Pattern::Triple(tp), Pattern::Filter(expr)) => (tp, expr),
        _ => return None,
    };
    let (s_var, pred, o_var) = validate_simple_triple(tp)?;

    // COUNT(?var) must reference ?s or ?o (both always bound in a single triple).
    if let Some(v) = input_var {
        if v != s_var && v != o_var {
            return None;
        }
    }

    let is_lang_o = |e: &crate::ir::Expression| match e {
        crate::ir::Expression::Call { func, args } => {
            *func == crate::ir::Function::Lang
                && args.len() == 1
                && matches!(&args[0], crate::ir::Expression::Var(v) if *v == o_var)
        }
        _ => false,
    };
    let crate::ir::Expression::Call { func, args } = filter else {
        return None;
    };
    if *func != crate::ir::Function::Eq || args.len() != 2 {
        return None;
    }

    if is_lang_o(&args[0]) {
        if let crate::ir::Expression::Const(crate::ir::FlakeValue::String(tag)) = &args[1] {
            return Some((pred, tag.clone(), out_var));
        }
    }
    if is_lang_o(&args[1]) {
        if let crate::ir::Expression::Const(crate::ir::FlakeValue::String(tag)) = &args[0] {
            return Some((pred, tag.clone(), out_var));
        }
    }

    None
}

fn detect_predicate_count_distinct_object(
    query: &Query,
    options: &QueryOptions,
) -> Option<(Ref, VarId)> {
    let (in_var, out_var) = detect_count_distinct_aggregate(query, options)?;

    if query.patterns.len() != 1 {
        return None;
    }
    let Pattern::Triple(tp) = &query.patterns[0] else {
        return None;
    };
    let (_s_var, pred, o_var) = validate_simple_triple(tp)?;

    // COUNT(DISTINCT ?o) must reference the object var.
    if in_var != o_var {
        return None;
    }

    Some((pred, out_var))
}

fn detect_predicate_minmax_string(
    query: &Query,
    options: &QueryOptions,
) -> Option<(Ref, MinMaxMode, VarId)> {
    // Must be a single implicit aggregate with no grouping/having/binds/etc.
    let agg = implicit_single_aggregate(query, options)?;
    // WHERE must be a single triple.
    if query.patterns.len() != 1 {
        return None;
    }
    let Pattern::Triple(tp) = &query.patterns[0] else {
        return None;
    };
    let (_s_var, pred, o_var) = validate_simple_triple(tp)?;

    // Aggregate must be MIN(?o) or MAX(?o) (not distinct).
    if agg.distinct {
        return None;
    }
    let mode = match agg.function {
        AggregateFn::Min => MinMaxMode::Min,
        AggregateFn::Max => MinMaxMode::Max,
        _ => return None,
    };
    if agg.input_var? != o_var {
        return None;
    }

    // SELECT must be exactly the aggregate output var.
    let select_vars = query.output.projected_vars()?;
    if select_vars.len() != 1 || select_vars[0] != agg.output_var {
        return None;
    }

    Some((pred, mode, agg.output_var))
}

fn detect_predicate_avg_numeric(query: &Query, options: &QueryOptions) -> Option<(Ref, VarId)> {
    let agg = implicit_single_aggregate(query, options)?;
    if query.patterns.len() != 1 {
        return None;
    }
    let Pattern::Triple(tp) = &query.patterns[0] else {
        return None;
    };
    let (_s_var, pred, o_var) = validate_simple_triple(tp)?;
    if agg.distinct || !matches!(agg.function, AggregateFn::Avg) || agg.input_var? != o_var {
        return None;
    }
    let select_vars = query.output.projected_vars()?;
    if select_vars.len() != 1 || select_vars[0] != agg.output_var {
        return None;
    }
    Some((pred, agg.output_var))
}

fn detect_count_rows_with_encoded_filters(
    query: &Query,
    options: &QueryOptions,
) -> Option<(
    crate::ir::triple::TriplePattern,
    Vec<crate::ir::Expression>,
    VarId,
)> {
    // Must be single COUNT aggregate, no grouping/having/binds/etc.
    let agg = implicit_single_aggregate(query, options)?;
    if agg.distinct || !matches!(agg.function, AggregateFn::Count | AggregateFn::CountAll) {
        return None;
    }

    // WHERE must be: one triple + one or more FILTER(...) patterns, and nothing else.
    if query.patterns.len() < 2 {
        return None;
    }
    let mut triple: Option<&crate::ir::triple::TriplePattern> = None;
    let mut filters: Vec<&crate::ir::Expression> = Vec::new();
    for p in &query.patterns {
        match p {
            Pattern::Triple(tp) => triple = Some(tp),
            Pattern::Filter(expr) => filters.push(expr),
            _ => return None,
        }
    }
    let tp = triple?;
    if filters.is_empty() {
        return None;
    }

    let (s_var, _pred, o_var) = validate_simple_triple(tp)?;

    // COUNT(?s) or COUNT(*) only.
    if matches!(agg.function, AggregateFn::Count) && agg.input_var != Some(s_var) {
        return None;
    }

    // All FILTERs must be compilable as encoded prefilters.
    let is_s =
        |e: &crate::ir::Expression| matches!(e, crate::ir::Expression::Var(v) if *v == s_var);
    let is_o =
        |e: &crate::ir::Expression| matches!(e, crate::ir::Expression::Var(v) if *v == o_var);
    let is_lang_call = |e: &crate::ir::Expression| match e {
        crate::ir::Expression::Call { func, args } => {
            *func == crate::ir::Function::Lang
                && args.len() == 1
                && matches!(&args[0], crate::ir::Expression::Var(v) if *v == o_var)
        }
        _ => false,
    };
    let is_lang_eq_const = |expr: &crate::ir::Expression| match expr {
        crate::ir::Expression::Call { func, args } => {
            if *func != crate::ir::Function::Eq || args.len() != 2 {
                return false;
            }
            let has_lang = is_lang_call(&args[0]) || is_lang_call(&args[1]);
            let has_const = matches!(
                (&args[0], &args[1]),
                (
                    crate::ir::Expression::Const(crate::ir::FlakeValue::String(_)),
                    _
                ) | (
                    _,
                    crate::ir::Expression::Const(crate::ir::FlakeValue::String(_))
                )
            );
            has_lang && has_const
        }
        _ => false,
    };
    for expr in &filters {
        match expr {
            crate::ir::Expression::Call { func, args }
                if *func == crate::ir::Function::IsBlank
                    && args.len() == 1
                    && matches!(&args[0], crate::ir::Expression::Var(v) if *v == o_var) =>
            {
                continue;
            }
            crate::ir::Expression::Call { func, args } if args.len() == 2 => {
                if matches!(func, crate::ir::Function::Eq | crate::ir::Function::Ne)
                    && ((is_s(&args[0]) && is_o(&args[1])) || (is_o(&args[0]) && is_s(&args[1])))
                {
                    continue;
                }
                if is_lang_eq_const(expr) {
                    continue;
                }
                return None;
            }
            _ => return None,
        }
    }

    // SELECT must be exactly the count output var.
    let select_vars = query.output.projected_vars()?;
    if select_vars.len() != 1 || select_vars[0] != agg.output_var {
        return None;
    }

    Some((
        tp.clone(),
        filters.into_iter().cloned().collect(),
        agg.output_var,
    ))
}

fn detect_predicate_count_rows_numeric_compare(
    query: &Query,
    options: &QueryOptions,
) -> Option<(Ref, NumericCompareOp, fluree_db_core::FlakeValue, VarId)> {
    let agg = implicit_single_aggregate(query, options)?;
    if query.patterns.len() != 2 {
        return None;
    }
    if agg.distinct || !matches!(agg.function, AggregateFn::Count | AggregateFn::CountAll) {
        return None;
    }

    let (tp, filter) = match (&query.patterns[0], &query.patterns[1]) {
        (Pattern::Triple(tp), Pattern::Filter(expr)) => (tp, expr),
        (Pattern::Filter(expr), Pattern::Triple(tp)) => (tp, expr),
        _ => return None,
    };

    let (s_var, pred, o_var) = validate_simple_triple(tp)?;
    if matches!(agg.function, AggregateFn::Count) && agg.input_var != Some(s_var) {
        return None;
    }

    let (filter_var, compare, threshold) = extract_simple_numeric_compare_threshold(filter)?;
    if filter_var != o_var {
        return None;
    }

    let select_vars = query.output.projected_vars()?;
    if select_vars.len() != 1 || select_vars[0] != agg.output_var {
        return None;
    }

    Some((pred, compare, threshold, agg.output_var))
}

fn detect_string_prefix_count_all(
    query: &Query,
    options: &QueryOptions,
) -> Option<(Ref, Arc<str>, VarId)> {
    let out_var = detect_count_all_aggregate(query, options)?;
    if query.patterns.len() != 2 {
        return None;
    }

    let (tp, filter) = match (&query.patterns[0], &query.patterns[1]) {
        (Pattern::Triple(tp), Pattern::Filter(expr)) => (tp, expr),
        (Pattern::Filter(expr), Pattern::Triple(tp)) => (tp, expr),
        _ => return None,
    };

    let (_s_var, pred, o_var) = validate_simple_triple(tp)?;
    let prefix = extract_string_prefix_filter(filter, o_var)?;
    Some((pred, prefix, out_var))
}

fn detect_string_prefix_sum_strstarts(
    query: &Query,
    options: &QueryOptions,
) -> Option<(Ref, Arc<str>, VarId)> {
    use crate::ir::{Expression, FlakeValue, Function};

    let agg = implicit_single_aggregate(query, options)?;
    if agg.distinct || !matches!(agg.function, AggregateFn::Sum) {
        return None;
    }
    let select_vars = query.output.projected_vars()?;
    if select_vars.len() != 1 || select_vars[0] != agg.output_var {
        return None;
    }

    if query.patterns.len() != 2 {
        return None;
    }
    let (tp, bind_var, bind_expr) = match (&query.patterns[0], &query.patterns[1]) {
        (Pattern::Triple(tp), Pattern::Bind { var, expr }) => (tp, *var, expr),
        _ => return None,
    };
    let (_s_var, pred, o_var) = validate_simple_triple(tp)?;
    if agg.input_var != Some(bind_var) {
        return None;
    }

    let Expression::Call {
        func: Function::XsdInteger,
        args,
    } = bind_expr
    else {
        return None;
    };
    if args.len() != 1 {
        return None;
    }
    let Expression::Call {
        func: Function::StrStarts,
        args,
    } = &args[0]
    else {
        return None;
    };
    if args.len() != 2 {
        return None;
    }
    if !matches!(&args[0], Expression::Var(v) if *v == o_var) {
        return None;
    }
    let Expression::Const(FlakeValue::String(prefix)) = &args[1] else {
        return None;
    };
    if prefix.is_empty() {
        return None;
    }

    Some((pred, Arc::from(prefix.as_str()), agg.output_var))
}

fn extract_string_prefix_filter(
    expr: &crate::ir::Expression,
    object_var: VarId,
) -> Option<Arc<str>> {
    use crate::ir::{Expression, FlakeValue, Function};

    let is_object_var = |expr: &Expression| matches!(expr, Expression::Var(v) if *v == object_var);

    match expr {
        Expression::Call { func, args } if *func == Function::Regex => {
            if args.len() != 2 && args.len() != 3 {
                return None;
            }
            if !is_object_var(&args[0]) {
                return None;
            }
            let Expression::Const(FlakeValue::String(pattern)) = &args[1] else {
                return None;
            };
            if args.len() == 3 {
                let Expression::Const(FlakeValue::String(flags)) = &args[2] else {
                    return None;
                };
                if !flags.is_empty() {
                    return None;
                }
            }
            anchored_literal_regex_prefix(pattern)
        }
        Expression::Call { func, args } if *func == Function::StrStarts && args.len() == 2 => {
            if !is_object_var(&args[0]) {
                return None;
            }
            let Expression::Const(FlakeValue::String(prefix)) = &args[1] else {
                return None;
            };
            if prefix.is_empty() {
                return None;
            }
            Some(Arc::from(prefix.as_str()))
        }
        _ => None,
    }
}

fn anchored_literal_regex_prefix(pattern: &str) -> Option<Arc<str>> {
    let prefix = pattern.strip_prefix('^')?;
    if prefix.is_empty() {
        return None;
    }
    if prefix.bytes().any(|b| {
        matches!(
            b,
            b'.' | b'+'
                | b'*'
                | b'?'
                | b'('
                | b')'
                | b'['
                | b']'
                | b'{'
                | b'}'
                | b'|'
                | b'\\'
                | b'^'
                | b'$'
        )
    }) {
        return None;
    }
    Some(Arc::from(prefix))
}

/// Detect if this is a stats fast-path query: `SELECT ?p (COUNT(?x) as ?c) WHERE { ?s ?p ?o } GROUP BY ?p`
///
/// Returns `Some((predicate_var, count_output_var))` if the query matches the pattern.
fn detect_stats_count_by_predicate(
    query: &Query,
    _options: &QueryOptions,
) -> Option<(VarId, VarId)> {
    // Must have stats available (checked by caller)
    // Must have exactly one triple pattern with all variables
    if query.patterns.len() != 1 {
        return None;
    }
    let Pattern::Triple(tp) = &query.patterns[0] else {
        return None;
    };

    // All three positions must be variables
    let Ref::Var(s_var) = &tp.s else {
        return None;
    };
    let Ref::Var(p_var) = &tp.p else {
        return None;
    };
    let Term::Var(o_var) = &tp.o else {
        return None;
    };

    // GROUP BY must be exactly the predicate variable, with a single COUNT
    // aggregate, no HAVING.
    let Some(Grouping::Explicit {
        group_by,
        aggregation: Some(Aggregation { aggregates, binds }),
        having: None,
    }) = &query.grouping
    else {
        return None;
    };
    if group_by.len() != 1 || *group_by.first() != *p_var {
        return None;
    }
    if aggregates.len() != 1 {
        return None;
    }
    let agg = aggregates.first();
    if !matches!(agg.function, AggregateFn::Count) {
        return None;
    }

    // COUNT input must be a non-predicate variable (subject or object)
    let input_var = agg.input_var?;
    if input_var != *s_var && input_var != *o_var {
        return None;
    }

    // No post-aggregation binds (for simplicity)
    if !binds.is_empty() {
        return None;
    }

    tracing::debug!(
        predicate_var = ?p_var,
        count_var = ?agg.output_var,
        "detected stats count-by-predicate fast-path"
    );

    Some((*p_var, agg.output_var))
}

fn detect_fused_scan_sum_i64(
    query: &Query,
    options: &QueryOptions,
) -> Option<(Ref, SumExprI64, VarId)> {
    // Must be single aggregate, no grouping/having/binds/etc.
    let agg = implicit_single_aggregate(query, options)?;

    // SELECT must be exactly the aggregate output var.
    let select_vars = query.output.projected_vars()?;
    if select_vars.len() != 1 || select_vars[0] != agg.output_var {
        return None;
    }
    if agg.distinct || !matches!(agg.function, AggregateFn::Sum) {
        return None;
    }

    match query.patterns.as_slice() {
        [Pattern::Triple(tp)] => {
            let pred = extract_bound_predicate(&tp.p)?;
            let Term::Var(o_var) = &tp.o else {
                return None;
            };
            if agg.input_var != Some(*o_var) {
                return None;
            }
            Some((pred, SumExprI64::Identity, agg.output_var))
        }
        [Pattern::Triple(tp), Pattern::Bind { var, expr }] => {
            let pred = extract_bound_predicate(&tp.p)?;
            let Term::Var(o_var) = &tp.o else {
                return None;
            };

            // Bind must define the aggregate input var, and SUM must use it.
            if agg.input_var != Some(*var) {
                return None;
            }

            let scalar = match expr {
                crate::ir::Expression::Call { func, args }
                    if args.len() == 1
                        && matches!(&args[0], crate::ir::Expression::Var(v) if v == o_var) =>
                {
                    match func {
                        crate::ir::Function::Year => {
                            SumExprI64::DateComponent(DateComponentFn::Year)
                        }
                        crate::ir::Function::Month => {
                            SumExprI64::DateComponent(DateComponentFn::Month)
                        }
                        crate::ir::Function::Day => SumExprI64::DateComponent(DateComponentFn::Day),
                        crate::ir::Function::Abs => SumExprI64::NumericUnary(NumericUnaryFn::Abs),
                        crate::ir::Function::Ceil => SumExprI64::NumericUnary(NumericUnaryFn::Ceil),
                        crate::ir::Function::Floor => {
                            SumExprI64::NumericUnary(NumericUnaryFn::Floor)
                        }
                        crate::ir::Function::Round => {
                            SumExprI64::NumericUnary(NumericUnaryFn::Round)
                        }
                        _ => return None,
                    }
                }
                crate::ir::Expression::Call { func, args }
                    if *func == crate::ir::Function::Add
                        && args.len() == 2
                        && matches!(&args[0], crate::ir::Expression::Var(v) if v == o_var)
                        && matches!(&args[1], crate::ir::Expression::Var(v) if v == o_var) =>
                {
                    SumExprI64::AddSelf
                }
                _ => return None,
            };

            Some((pred, scalar, agg.output_var))
        }
        _ => None,
    }
}

fn detect_exists_join_count_distinct_object(
    query: &Query,
    options: &QueryOptions,
) -> Option<(Ref, Ref, VarId)> {
    let (in_var, out_var) = detect_count_distinct_aggregate(query, options)?;

    // WHERE must be exactly two triples: ?s <p1> ?o1 . ?s <p2> ?o2 .
    if query.patterns.len() != 2 {
        return None;
    }
    let Pattern::Triple(a) = &query.patterns[0] else {
        return None;
    };
    let Pattern::Triple(b) = &query.patterns[1] else {
        return None;
    };

    let (sv_a, pred_a, ov_a) = validate_simple_triple(a)?;
    let (sv_b, pred_b, ov_b) = validate_simple_triple(b)?;
    if sv_a != sv_b {
        return None;
    }

    // One of the object vars must be the COUNT DISTINCT input. Accept either
    // triple order so callers don't miss the fused fast path just because the
    // existence predicate appears before the counted predicate in the query.
    if ov_a == in_var {
        Some((pred_a, pred_b, out_var))
    } else if ov_b == in_var {
        Some((pred_b, pred_a, out_var))
    } else {
        None
    }
}

fn detect_multicolumn_join_count_all(
    query: &Query,
    options: &QueryOptions,
) -> Option<(Ref, Ref, VarId)> {
    let out_var = detect_count_all_aggregate(query, options)?;

    // WHERE must be exactly two triple patterns and nothing else.
    if query.patterns.len() != 2 {
        return None;
    }
    let Pattern::Triple(t1) = &query.patterns[0] else {
        return None;
    };
    let Pattern::Triple(t2) = &query.patterns[1] else {
        return None;
    };

    // Must be ?s p1 ?o . ?s p2 ?o (same subject var, same object var).
    let (s1, p1, o1) = validate_simple_triple(t1)?;
    let (s2, p2, o2) = validate_simple_triple(t2)?;
    if s1 != s2 {
        return None;
    }
    if o1 != o2 {
        return None;
    }
    if o1 == s1 {
        return None;
    }

    Some((p1, p2, out_var))
}

fn detect_count_blank_node_subjects(query: &Query, options: &QueryOptions) -> Option<VarId> {
    let (input_var, out_var) = detect_count_aggregate(query, options)?;

    // Pattern shape: one Triple + one Filter(ISBLANK(?s)) in canonical order.
    if query.patterns.len() != 2 {
        return None;
    }
    let (tp, filter) = match (&query.patterns[0], &query.patterns[1]) {
        (Pattern::Triple(tp), Pattern::Filter(expr)) => (tp, expr),
        _ => return None,
    };
    let Ref::Var(sv) = &tp.s else { return None };
    let Ref::Var(_pv) = &tp.p else { return None };
    let Term::Var(_ov) = &tp.o else { return None };
    if tp.dtc.is_some() {
        return None;
    }

    // COUNT(?var) must reference ?s.
    if let Some(v) = input_var {
        if v != *sv {
            return None;
        }
    }

    let crate::ir::Expression::Call { func, args } = filter else {
        return None;
    };
    if *func != crate::ir::Function::IsBlank || args.len() != 1 {
        return None;
    }
    if !matches!(&args[0], crate::ir::Expression::Var(v) if *v == *sv) {
        return None;
    }

    Some(out_var)
}

fn detect_count_literal_objects(query: &Query, options: &QueryOptions) -> Option<VarId> {
    let (input_var, out_var) = detect_count_aggregate(query, options)?;

    // Pattern shape: one Triple + one Filter(ISLITERAL(?o)) in canonical order.
    if query.patterns.len() != 2 {
        return None;
    }
    let (tp, filter) = match (&query.patterns[0], &query.patterns[1]) {
        (Pattern::Triple(tp), Pattern::Filter(expr)) => (tp, expr),
        _ => return None,
    };
    let Ref::Var(_sv) = &tp.s else { return None };
    let Ref::Var(_pv) = &tp.p else { return None };
    let Term::Var(ov) = &tp.o else { return None };
    if tp.dtc.is_some() {
        return None;
    }

    // COUNT(?var) must reference ?o.
    if let Some(v) = input_var {
        if v != *ov {
            return None;
        }
    }

    let crate::ir::Expression::Call { func, args } = filter else {
        return None;
    };
    if *func != crate::ir::Function::IsLiteral || args.len() != 1 {
        return None;
    }
    if !matches!(&args[0], crate::ir::Expression::Var(v) if *v == *ov) {
        return None;
    }

    Some(out_var)
}

fn detect_count_distinct_objects(query: &Query, options: &QueryOptions) -> Option<VarId> {
    let (in_var, out_var) = detect_count_distinct_aggregate(query, options)?;

    // Pattern shape: exactly one triple with all vars.
    if query.patterns.len() != 1 {
        return None;
    }
    let Pattern::Triple(tp) = &query.patterns[0] else {
        return None;
    };
    let Ref::Var(_sv) = &tp.s else { return None };
    let Ref::Var(_pv) = &tp.p else { return None };
    let Term::Var(ov) = &tp.o else { return None };
    if tp.dtc.is_some() {
        return None;
    }

    // COUNT(DISTINCT ?o) specifically.
    if in_var != *ov {
        return None;
    }

    Some(out_var)
}

fn detect_count_distinct_subjects(query: &Query, options: &QueryOptions) -> Option<VarId> {
    let (in_var, out_var) = detect_count_distinct_aggregate(query, options)?;

    // Pattern shape: exactly one triple with all vars.
    if query.patterns.len() != 1 {
        return None;
    }
    let Pattern::Triple(tp) = &query.patterns[0] else {
        return None;
    };
    let Ref::Var(sv) = &tp.s else { return None };
    let Ref::Var(_pv) = &tp.p else { return None };
    let Term::Var(_ov) = &tp.o else { return None };
    if tp.dtc.is_some() {
        return None;
    }

    // COUNT(DISTINCT ?s) specifically.
    if in_var != *sv {
        return None;
    }

    Some(out_var)
}

fn detect_count_distinct_predicates(query: &Query, options: &QueryOptions) -> Option<VarId> {
    let (in_var, out_var) = detect_count_distinct_aggregate(query, options)?;

    // Pattern shape: exactly one triple with all vars.
    if query.patterns.len() != 1 {
        return None;
    }
    let Pattern::Triple(tp) = &query.patterns[0] else {
        return None;
    };
    let Ref::Var(_sv) = &tp.s else { return None };
    let Ref::Var(pv) = &tp.p else { return None };
    let Term::Var(_ov) = &tp.o else { return None };
    if tp.dtc.is_some() {
        return None;
    }

    // COUNT(DISTINCT ?p) specifically.
    if in_var != *pv {
        return None;
    }

    Some(out_var)
}

fn detect_count_triples(query: &Query, options: &QueryOptions) -> Option<VarId> {
    let (input_var, out_var) = detect_count_aggregate(query, options)?;

    // Pattern shape: exactly one triple with all vars.
    if query.patterns.len() != 1 {
        return None;
    }
    let Pattern::Triple(tp) = &query.patterns[0] else {
        return None;
    };
    let Ref::Var(sv) = &tp.s else { return None };
    let Ref::Var(pv) = &tp.p else { return None };
    let Term::Var(ov) = &tp.o else { return None };
    if tp.dtc.is_some() {
        return None;
    }

    // COUNT(?var) must reference one of the triple's vars; all are always bound.
    if let Some(v) = input_var {
        if v != *sv && v != *pv && v != *ov {
            return None;
        }
    }

    Some(out_var)
}

fn detect_optional_chain_head_join_count_all(
    query: &Query,
    options: &QueryOptions,
) -> Option<(Ref, Ref, Ref, VarId)> {
    let out_var = detect_count_all_aggregate(query, options)?;

    // Pattern shape: one required triple + OPTIONAL with two triples (order-independent).
    if query.patterns.len() != 2 {
        return None;
    }

    let mut req: Option<&crate::ir::triple::TriplePattern> = None;
    let mut inner: Option<&[Pattern]> = None;
    for p in &query.patterns {
        match p {
            Pattern::Triple(tp) => req = Some(tp),
            Pattern::Optional(v) => inner = Some(v),
            _ => return None,
        }
    }
    let req = req?;
    let inner = inner?;
    if inner.len() != 2 {
        return None;
    }
    let (t1, t2) = match (&inner[0], &inner[1]) {
        (Pattern::Triple(a), Pattern::Triple(b)) => (a, b),
        _ => return None,
    };

    // Required: ?a <p1> ?b
    let (_a, p1, b_var) = validate_simple_triple(req)?;

    // Optional must be a 2-hop chain starting at ?b: ?b <p2> ?c . ?c <p3> ?d (either order).
    let (b1, p2, _c, p3, _d) = find_two_hop_chain(t1, t2)?;
    if b1 != b_var {
        return None;
    }

    tracing::debug!(
        "detected optional chain-head COUNT(*) fast-path (p1={:?}, p2={:?}, p3={:?})",
        p1,
        p2,
        p3
    );
    Some((p1, p2, p3, out_var))
}

fn detect_transitive_path_plus_count_all(
    query: &Query,
    options: &QueryOptions,
) -> Option<(Ref, Ref, VarId)> {
    let out_var = detect_count_all_aggregate(query, options)?;
    if query.patterns.len() != 2 {
        return None;
    }
    let Pattern::Triple(t1) = &query.patterns[0] else {
        return None;
    };
    let Pattern::PropertyPath(pp) = &query.patterns[1] else {
        return None;
    };

    // ?s <p1> ?x
    let (_s, p1, x1) = validate_simple_triple(t1)?;

    // ?x <p2>+ ?o
    let Ref::Var(x2) = &pp.subject else {
        return None;
    };
    if *x2 != x1 {
        return None;
    }
    if pp.modifier != PathModifier::OneOrMore {
        return None;
    }
    let Ref::Var(_o) = &pp.object else {
        return None;
    };

    Some((p1, Ref::Sid(pp.predicate.clone()), out_var))
}

fn detect_property_path_plus_fixed_subject_count_all(
    query: &Query,
    options: &QueryOptions,
) -> Option<(fluree_db_core::Sid, Ref, VarId)> {
    let out_var = detect_count_all_aggregate(query, options)?;
    if query.patterns.len() != 1 {
        return None;
    }
    let Pattern::PropertyPath(pp) = &query.patterns[0] else {
        return None;
    };
    if pp.modifier != PathModifier::OneOrMore {
        return None;
    }
    if !pp.subject.is_bound() {
        return None;
    }
    let Ref::Var(_o) = &pp.object else {
        return None;
    };
    Some((pp.predicate.clone(), pp.subject.clone(), out_var))
}

fn detect_union_star_count_all(
    query: &Query,
    options: &QueryOptions,
) -> Option<(Vec<Ref>, Vec<Ref>, UnionCountMode, VarId)> {
    use crate::ir::{Expression, Function};
    let out_var = detect_count_all_aggregate(query, options)?;

    // Find exactly one UNION pattern.
    let mut union: Option<&Vec<Vec<Pattern>>> = None;
    let mut other: Vec<&Pattern> = Vec::new();
    for p in &query.patterns {
        match p {
            Pattern::Union(branches) => {
                if union.is_some() {
                    return None;
                }
                union = Some(branches);
            }
            _ => other.push(p),
        }
    }
    let branches = union?;
    if branches.len() < 2 {
        return None;
    }

    // Each branch must be exactly one triple: ?s <p> ?o1 (same ?s and same ?o1 across branches).
    let mut subj: Option<VarId> = None;
    let mut obj: Option<VarId> = None;
    let mut union_preds: Vec<Ref> = Vec::with_capacity(branches.len());
    for b in branches {
        if b.len() != 1 {
            return None;
        }
        let Pattern::Triple(tp) = &b[0] else {
            return None;
        };
        let (s, pred, o) = validate_simple_triple(tp)?;
        match subj {
            None => subj = Some(s),
            Some(x) if x != s => return None,
            _ => {}
        }
        match obj {
            None => obj = Some(o),
            Some(x) if x != o => return None,
            _ => {}
        }
        union_preds.push(pred);
    }
    let subj = subj?;
    let obj = obj?;

    // Optional FILTER(?s = ?o1) (either arg order).
    let mut mode = UnionCountMode::AllRows;
    let mut extra_preds: Vec<Ref> = Vec::new();

    for p in other {
        match p {
            Pattern::Filter(expr) => {
                let Expression::Call { func, args } = expr else {
                    return None;
                };
                if *func != Function::Eq || args.len() != 2 {
                    return None;
                }
                let is_s_o = |a: &Expression, b: &Expression| {
                    matches!(a, Expression::Var(v) if *v == subj)
                        && matches!(b, Expression::Var(v) if *v == obj)
                };
                if !(is_s_o(&args[0], &args[1]) || is_s_o(&args[1], &args[0])) {
                    return None;
                }
                mode = UnionCountMode::SubjectEqObject;
            }
            Pattern::Triple(tp) => {
                // Extra required same-subject star predicate(s): ?s <p> ?o2
                let (s, pred, o2) = validate_simple_triple(tp)?;
                if s != subj {
                    return None;
                }
                if o2 == subj || o2 == obj {
                    return None;
                }
                extra_preds.push(pred);
            }
            _ => return None,
        }
    }

    tracing::debug!(
        union_pred_count = union_preds.len(),
        extra_pred_count = extra_preds.len(),
        ?mode,
        "detected UNION-star COUNT(*) fast-path"
    );
    Some((union_preds, extra_preds, mode, out_var))
}

/// Build the complete operator tree for a query
///
/// Constructs operators in the order:
/// WHERE patterns → GROUP BY → Aggregates → HAVING → ORDER BY → PROJECT → DISTINCT → OFFSET → LIMIT
pub fn build_operator_tree(
    query: &Query,
    options: &QueryOptions,
    stats: Option<Arc<StatsView>>,
    planning: &PlanningContext,
) -> Result<BoxedOperator> {
    build_operator_tree_inner(query, options, stats, true, planning)
}

fn build_operator_tree_inner(
    query: &Query,
    options: &QueryOptions,
    stats: Option<Arc<StatsView>>,
    enable_fused_fast_paths: bool,
    planning: &PlanningContext,
) -> Result<BoxedOperator> {
    // Phase 5 of the planner-mode refactor: fast paths emit current-state
    // bindings (no `op` channel, no retract events) and don't consult the
    // history sidecar. In `History` mode they're semantically wrong, so the
    // planner declines to construct them at all — this collapses the
    // optimistic-then-fallback pattern in each operator's `open()` into a
    // single planner-time decision.
    let enable_fused_fast_paths = enable_fused_fast_paths && !planning.is_history();

    if enable_fused_fast_paths {
        tracing::debug!(
            patterns = ?query.patterns,
            grouping = ?query.grouping,
            "operator_tree: considering fused fast paths"
        );
    }

    // Fast-path: `SELECT (SUM(DAY(?o)) AS ?sum) WHERE { ?s <p> ?o }` and friends.
    //
    // These are lowered as: Triple + Bind(expr) + SUM(synthetic_var).
    // This operator scans the predicate's POST range and aggregates directly from encoded values.
    if enable_fused_fast_paths {
        if let Some((pred, scalar, out_var)) = detect_fused_scan_sum_i64(query, options) {
            // Build fallback operator tree without this fast path to preserve correctness in
            // pre-index / history / policy contexts.
            let fallback =
                build_operator_tree_inner(query, options, stats.clone(), false, planning)?;
            return Ok(Box::new(fused_scan_sum_i64_operator(
                pred,
                scalar,
                out_var,
                Some(fallback),
            )));
        }
    }

    // Fast-path: `SELECT (AVG(?o) AS ?avg) WHERE { ?s <p> ?o }`
    // for homogeneous numeric predicates, scanning only POST `o_key` values.
    if enable_fused_fast_paths {
        if let Some((pred, out_var)) = detect_predicate_avg_numeric(query, options) {
            let fallback =
                build_operator_tree_inner(query, options, stats.clone(), false, planning)?;
            return Ok(Box::new(predicate_avg_numeric_operator(
                pred,
                out_var,
                Some(fallback),
            )));
        }
    }

    // Fast-path: `SELECT (COUNT(DISTINCT ?o) AS ?c) WHERE { ?s <p> ?o }`
    // by scanning POST and counting distinct encoded object IDs.
    if enable_fused_fast_paths {
        if let Some((pred, out_var)) = detect_predicate_count_distinct_object(query, options) {
            let fallback =
                build_operator_tree_inner(query, options, stats.clone(), false, planning)?;
            return Ok(Box::new(count_distinct_object_operator(
                pred,
                out_var,
                Some(fallback),
            )));
        }
    }

    // Fast-path: `SELECT (MIN(?o) AS ?min) WHERE { ?s <p> ?o }` and MAX(...)
    // when the object is string-dict-backed. This inspects only POST leaflet directory keys.
    if enable_fused_fast_paths {
        if let Some((pred, mode, out_var)) = detect_predicate_minmax_string(query, options) {
            let fallback =
                build_operator_tree_inner(query, options, stats.clone(), false, planning)?;
            return Ok(Box::new(predicate_min_max_string_operator(
                pred,
                mode,
                out_var,
                Some(fallback),
            )));
        }
    }

    // Fast-path: `COUNT(*)` over one triple with an anchored string-prefix filter.
    if enable_fused_fast_paths {
        if let Some((pred, prefix, out_var)) = detect_string_prefix_count_all(query, options) {
            let fallback =
                build_operator_tree_inner(query, options, stats.clone(), false, planning)?;
            return Ok(Box::new(string_prefix_count_all_operator(
                pred,
                prefix,
                out_var,
                Some(fallback),
            )));
        }
    }

    // Fast-path: `SUM(xsd:integer(STRSTARTS(?o, "...")))` over one triple.
    if enable_fused_fast_paths {
        if let Some((pred, prefix, out_var)) = detect_string_prefix_sum_strstarts(query, options) {
            let fallback =
                build_operator_tree_inner(query, options, stats.clone(), false, planning)?;
            return Ok(Box::new(string_prefix_sum_strstarts_operator(
                pred,
                prefix,
                out_var,
                Some(fallback),
            )));
        }
    }

    // Fast-path: `COUNT(?s)` / `COUNT(*)` on a single predicate with FILTERs that
    // can be pushed down to encoded pre-filters in `BinaryScanOperator`:
    // - FILTER(?s = ?o)
    // - FILTER(?s != ?o)
    // - FILTER(LANG(?o) = "en")
    //
    // We build a scan that emits no bindings (empty schema) and counts rows.
    if enable_fused_fast_paths {
        if let Some((pred, lang_tag, out_var)) =
            detect_predicate_count_rows_lang_filter(query, options)
        {
            let fallback =
                build_operator_tree_inner(query, options, stats.clone(), false, planning)?;
            return Ok(Box::new(count_rows_lang_filter_operator(
                pred,
                lang_tag,
                out_var,
                Some(fallback),
            )));
        }
    }

    if enable_fused_fast_paths {
        if let Some((pred, compare, threshold, out_var)) =
            detect_predicate_count_rows_numeric_compare(query, options)
        {
            let fallback =
                build_operator_tree_inner(query, options, stats.clone(), false, planning)?;
            return Ok(Box::new(count_rows_numeric_compare_operator(
                pred,
                compare,
                threshold,
                out_var,
                Some(fallback),
            )));
        }
    }

    if enable_fused_fast_paths {
        if let Some((tp, filters, out_var)) = detect_count_rows_with_encoded_filters(query, options)
        {
            let fallback =
                build_operator_tree_inner(query, options, stats.clone(), false, planning)?;
            let inline_ops: Vec<InlineOperator> = filters
                .into_iter()
                .map(|expr| InlineOperator::Filter(PreparedBoolExpression::new(expr)))
                .collect();
            let emit = EmitMask {
                s: false,
                p: false,
                o: false,
            };
            let scan: BoxedOperator = Box::new(crate::dataset_operator::DatasetOperator::scan(
                tp,
                None,
                inline_ops,
                emit,
                None,
                planning.mode(),
            ));
            return Ok(Box::new(CountRowsOperator::new(
                scan,
                out_var,
                Some(fallback),
            )));
        }
    }

    // Fast-path: `SELECT (COUNT(?x) AS ?c) WHERE { ?s <p> ?o }` (and COUNT(*))
    // answered from PSOT leaflet directory row counts (no scan / no decoding).
    if enable_fused_fast_paths {
        if let Some((pred, out_var)) = detect_predicate_count_rows(query, options) {
            let fallback =
                build_operator_tree_inner(query, options, stats.clone(), false, planning)?;
            return Ok(Box::new(count_rows_operator(pred, out_var, Some(fallback))));
        }
    }

    // Count-only plan: generic join-aware count planner that handles star joins, chains,
    // and modifier combinations (OPTIONAL, MINUS, EXISTS, object-chain patterns).
    // Fires after trivial metadata-only counts but before the remaining specialized fast paths.
    if enable_fused_fast_paths {
        if let Some(plan) = crate::count_plan::try_build_count_plan(query, options) {
            let fallback =
                build_operator_tree_inner(query, options, stats.clone(), false, planning)?;
            return Ok(crate::count_plan_exec::count_plan_operator(
                plan,
                Some(fallback),
            ));
        }
    }

    // Fast-path: `COUNT(*)` for a 2-pattern multicolumn join `?s p1 ?o . ?s p2 ?o`.
    if enable_fused_fast_paths {
        if let Some((p1, p2, out_var)) = detect_multicolumn_join_count_all(query, options) {
            let fallback =
                build_operator_tree_inner(query, options, stats.clone(), false, planning)?;
            return Ok(Box::new(multicolumn_join_count_all_operator(
                p1,
                p2,
                out_var,
                Some(fallback),
            )));
        }
    }

    // Fast-path: `COUNT(DISTINCT ?o1)` with an existence-only same-subject join.
    if enable_fused_fast_paths {
        if let Some((count_pred, exists_pred, out_var)) =
            detect_exists_join_count_distinct_object(query, options)
        {
            let fallback =
                build_operator_tree_inner(query, options, stats.clone(), false, planning)?;
            return Ok(Box::new(exists_join_count_distinct_object_operator(
                count_pred,
                exists_pred,
                out_var,
                Some(fallback),
            )));
        }
    }

    // Fast-path: `SELECT (COUNT(?s) AS ?c) WHERE { ?s ?p ?o FILTER ISBLANK(?s) }`
    // answered from SPOT leaflet metadata by scanning the blank-node SubjectId range.
    if enable_fused_fast_paths {
        if let Some(out_var) = detect_count_blank_node_subjects(query, options) {
            let fallback =
                build_operator_tree_inner(query, options, stats.clone(), false, planning)?;
            return Ok(Box::new(count_blank_node_subjects_operator(
                out_var,
                Some(fallback),
            )));
        }
    }

    // Fast-path: `SELECT (COUNT(?o) AS ?c) WHERE { ?s ?p ?o FILTER ISLITERAL(?o) }`
    // answered from PSOT leaflet metadata by counting non-node-ref `o_type` rows.
    if enable_fused_fast_paths {
        if let Some(out_var) = detect_count_literal_objects(query, options) {
            let fallback =
                build_operator_tree_inner(query, options, stats.clone(), false, planning)?;
            return Ok(Box::new(count_literal_objects_operator(
                out_var,
                Some(fallback),
            )));
        }
    }

    // Fast-path: `SELECT (COUNT(DISTINCT ?s) AS ?c) WHERE { ?s ?p ?o }`
    // answered metadata-only from SPOT leaflet `lead_group_count` + boundary correction.
    if enable_fused_fast_paths {
        if let Some(out_var) = detect_count_distinct_subjects(query, options) {
            let fallback =
                build_operator_tree_inner(query, options, stats.clone(), false, planning)?;
            return Ok(Box::new(count_distinct_subjects_operator(
                out_var,
                Some(fallback),
            )));
        }
    }

    // Fast-path: `SELECT (COUNT(DISTINCT ?p) AS ?c) WHERE { ?s ?p ?o }`
    // answered metadata-only from PSOT leaflet `p_const` transitions.
    if enable_fused_fast_paths {
        if let Some(out_var) = detect_count_distinct_predicates(query, options) {
            let fallback =
                build_operator_tree_inner(query, options, stats.clone(), false, planning)?;
            return Ok(Box::new(count_distinct_predicates_operator(
                out_var,
                Some(fallback),
            )));
        }
    }

    // Fast-path: `SELECT (COUNT(?s) AS ?c) WHERE { ?s ?p ?o }`
    // answered metadata-only by summing leaf row_count across a branch manifest.
    if enable_fused_fast_paths {
        if let Some(out_var) = detect_count_triples(query, options) {
            let fallback =
                build_operator_tree_inner(query, options, stats.clone(), false, planning)?;
            return Ok(Box::new(count_triples_operator(out_var, Some(fallback))));
        }
    }

    // Fast-path: `SELECT (COUNT(*) AS ?c) WHERE { ?a <p1> ?b . OPTIONAL { ?b <p2> ?c . ?c <p3> ?d } }`
    // answered by streaming group counts and an `n3(c)` map.
    if enable_fused_fast_paths {
        if let Some((p1, p2, p3, out_var)) =
            detect_optional_chain_head_join_count_all(query, options)
        {
            let fallback =
                build_operator_tree_inner(query, options, stats.clone(), false, planning)?;
            return Ok(Box::new(predicate_optional_chain_head_count_all(
                p1,
                p2,
                p3,
                out_var,
                Some(fallback),
            )));
        }
    }

    // Fast-path: `SELECT (COUNT(*) AS ?c) WHERE { <S> <p>+ ?o }`
    // Avoids repeated range scans by building adjacency once and traversing.
    if enable_fused_fast_paths {
        if let Some((pred_sid, subject, out_var)) =
            detect_property_path_plus_fixed_subject_count_all(query, options)
        {
            let fallback =
                build_operator_tree_inner(query, options, stats.clone(), false, planning)?;
            return Ok(Box::new(property_path_plus_count_all_operator(
                pred_sid,
                subject,
                out_var,
                Some(fallback),
            )));
        }
    }

    // Fast-path: UNION-of-triples optionally constrained by same-subject star joins and/or FILTER(?s = ?o).
    if enable_fused_fast_paths {
        if let Some((union_preds, extra_preds, mode, out_var)) =
            detect_union_star_count_all(query, options)
        {
            let fallback =
                build_operator_tree_inner(query, options, stats.clone(), false, planning)?;
            return Ok(Box::new(UnionStarCountAllOperator::new(
                union_preds,
                extra_preds,
                mode,
                out_var,
                Some(fallback),
            )));
        }
    }

    // Fast-path: `SELECT (COUNT(*) AS ?c) WHERE { ?s <p1> ?x . ?x <p2>+ ?o }`
    // Avoids closure materialization by counting reachability.
    if enable_fused_fast_paths {
        if let Some((p1, p2, out_var)) = detect_transitive_path_plus_count_all(query, options) {
            let fallback =
                build_operator_tree_inner(query, options, stats.clone(), false, planning)?;
            return Ok(Box::new(transitive_path_plus_count_all_operator(
                p1,
                p2,
                out_var,
                Some(fallback),
            )));
        }
    }

    // Fast-path: `SELECT (COUNT(DISTINCT ?o) AS ?c) WHERE { ?s ?p ?o }`
    // answered metadata-only from OPST leaflet `lead_group_count` + boundary correction.
    if enable_fused_fast_paths {
        if let Some(out_var) = detect_count_distinct_objects(query, options) {
            let fallback =
                build_operator_tree_inner(query, options, stats.clone(), false, planning)?;
            return Ok(Box::new(count_distinct_objects_operator(
                out_var,
                Some(fallback),
            )));
        }
    }

    // Fast-path: constant-object star constraints + numeric existence filter + label ORDER BY + LIMIT.
    if enable_fused_fast_paths {
        if let Some(spec) = detect_star_const_numeric_label_order_limit(query, options) {
            let fallback =
                build_operator_tree_inner(query, options, stats.clone(), false, planning)?;
            let threshold = match spec.numeric_threshold {
                Term::Value(v) => v,
                _ => return Ok(fallback),
            };
            return Ok(star_const_ordered_limit_operator(
                spec.subject_var,
                spec.label_var,
                spec.label_pred,
                spec.const_constraints,
                spec.numeric_pred,
                threshold,
                spec.limit,
                Some(fallback),
            ));
        }
    }

    // Fast-path: label scan + regex filter + rdf:type membership check.
    if enable_fused_fast_paths {
        if let Some(spec) = detect_label_regex_type(query, options) {
            let fallback =
                build_operator_tree_inner(query, options, stats.clone(), false, planning)?;
            return Ok(label_regex_type_operator(
                spec.subject_var,
                spec.label_var,
                spec.label_pred,
                spec.class_term,
                spec.regex_pattern,
                spec.regex_flags,
                Some(fallback),
            ));
        }
    }

    // Fast-path: `?s <p_group> ?o GROUP BY ?o` top-k with same-subject star constraints:
    // `?s <p_group> ?o . ?s <p_filter1> ?x1 . ...`
    //
    // Avoids join materialization and generic group-by for common benchmark shapes.
    if enable_fused_fast_paths {
        if let Some((
            group_pred,
            filter_preds,
            select_schema,
            _s_var,
            o_var,
            count_var,
            min_var,
            max_var,
            sample_var,
            limit,
        )) = detect_group_by_object_star_topk(query, options)
        {
            let fallback =
                build_operator_tree_inner(query, options, stats.clone(), false, planning)?;
            return Ok(Box::new(GroupByObjectStarTopKOperator::new(
                group_pred,
                filter_preds,
                o_var,
                count_var,
                min_var,
                max_var,
                sample_var,
                limit,
                select_schema,
                Some(fallback),
            )));
        }
    }

    // Fast-path: `?s <p> ?o GROUP BY ?o COUNT(?s)` top-k using leaflet FIRST headers.
    //
    // This avoids decoding leaflets for long (p,o) runs that span leaflet boundaries.
    // Skipped in `History` mode for the same reason as the fused fast paths above:
    // the path emits current-state counts and ignores retracts.
    if !planning.is_history() {
        if let Some((pred, s_var, o_var, count_var, limit)) =
            detect_predicate_group_by_object_count_topk(query, options)
        {
            return Ok(Box::new(PredicateGroupCountFirstsOperator::new(
                s_var,
                o_var,
                count_var,
                pred,
                limit,
                planning.mode(),
            )));
        }
    }

    // Fast-path: SUM(STRLEN(GROUP_CONCAT(...))) over a single predicate.
    if enable_fused_fast_paths {
        if let Some((pred, sep, out_var)) = detect_sum_strlen_group_concat_subquery(query, options)
        {
            let fallback =
                build_operator_tree_inner(query, options, stats.clone(), false, planning)?;
            return Ok(Box::new(sum_strlen_group_concat_operator(
                pred,
                sep,
                out_var,
                Some(fallback),
            )));
        }
    }

    // Fast-path: `SELECT (COUNT(?s) AS ?c) WHERE { ?s <p> <o> }` using leaflet FIRST headers.
    // Skipped in `History` mode (current-state count semantics).
    if !planning.is_history() {
        if let Some((pred, s_var, obj, count_var)) = detect_predicate_object_count(query, options) {
            let mut operator: BoxedOperator = Box::new(PredicateObjectCountFirstsOperator::new(
                pred,
                s_var,
                obj,
                count_var,
                planning.mode(),
            ));

            // ORDER BY
            if !query.ordering.is_empty() {
                operator = Box::new(SortOperator::new(operator, query.ordering.clone()));
            }

            // PROJECT
            if let Some(vars) = query.output.projected_vars() {
                if !vars.is_empty() {
                    operator = Box::new(ProjectOperator::new(operator, vars.to_vec()));
                }
            }

            // DISTINCT
            if query.output.is_distinct() {
                operator = Box::new(DistinctOperator::new(operator));
            }

            // OFFSET
            if let Some(offset) = options.offset {
                if offset > 0 {
                    operator = Box::new(OffsetOperator::new(operator, offset));
                }
            }

            // LIMIT
            if let Some(limit) = options.limit {
                operator = Box::new(LimitOperator::new(operator, limit));
            }

            return Ok(operator);
        }
    }

    // Fast-path: stats-based count-by-predicate query
    // This avoids scanning all triples when we can answer directly from IndexStats.
    // Skipped in `History` mode — IndexStats reflects current-state cardinality,
    // not the asserts + retracts a history-range query needs.
    if !planning.is_history() {
        if let Some(ref stats_view) = stats {
            if let Some((pred_var, count_var)) = detect_stats_count_by_predicate(query, options) {
                let mut operator: BoxedOperator = Box::new(StatsCountByPredicateOperator::new(
                    Arc::clone(stats_view),
                    pred_var,
                    count_var,
                ));

                // ORDER BY (on predicate or count)
                if !query.ordering.is_empty() {
                    operator = Box::new(SortOperator::new(operator, query.ordering.clone()));
                }

                // PROJECT (select specific columns)
                if let Some(vars) = query.output.projected_vars() {
                    if !vars.is_empty() {
                        operator = Box::new(ProjectOperator::new(operator, vars.to_vec()));
                    }
                }

                // DISTINCT
                if query.output.is_distinct() {
                    operator = Box::new(crate::distinct::DistinctOperator::new(operator));
                }

                // OFFSET
                if let Some(offset) = options.offset {
                    if offset > 0 {
                        operator = Box::new(OffsetOperator::new(operator, offset));
                    }
                }

                // LIMIT
                if let Some(limit) = options.limit {
                    operator = Box::new(LimitOperator::new(operator, limit));
                }

                return Ok(operator);
            }
        }
    }

    // Compute per-operator downstream dependency sets for trimming.
    // Done before building WHERE operators so we can push projection into the WHERE clause.
    let variable_deps = compute_variable_deps(query);

    // Build WHERE clause operators with projection pushdown
    let required_where_vars = variable_deps
        .as_ref()
        .map(|d| d.required_where_vars.as_slice());
    // needed-vars for WHERE planning: derived from variable_deps when available,
    // otherwise treat all WHERE-bound vars as needed (wildcard/boolean/construct cases).
    let mut needed_where_vars: HashSet<VarId> = HashSet::new();
    if let Some(req) = required_where_vars {
        needed_where_vars.extend(req.iter().copied());
    } else {
        let mut counts: HashMap<VarId, usize> = HashMap::new();
        let mut vars: HashSet<VarId> = HashSet::new();
        collect_var_stats(&query.patterns, &mut counts, &mut vars);
        vars.extend(counts.keys().copied());
        needed_where_vars = vars;
    }

    // Flatten the grouping phase's data for consumption below. The variant
    // distinction has already done its structural work at the IR boundary;
    // the operator-tree builder treats both variants uniformly. Cloning is
    // cheap here — both vectors are short (typically a handful of items)
    // and this is one-shot setup, not per-row work.
    let group_by_vec: Vec<VarId> = match &query.grouping {
        Some(Grouping::Explicit { group_by, .. }) => group_by.iter().copied().collect(),
        _ => Vec::new(),
    };
    let aggregates_vec: Vec<AggregateSpec> = query
        .grouping
        .as_ref()
        .map(|g| g.aggregates().cloned().collect())
        .unwrap_or_default();
    let post_binds_vec: Vec<(VarId, Expression)> = query
        .grouping
        .as_ref()
        .map(|g| g.binds().cloned().collect())
        .unwrap_or_default();
    let having_expr: Option<&Expression> = query.grouping.as_ref().and_then(Grouping::having);

    let mut operator = build_where_operators_with_needed(
        &query.patterns,
        stats,
        &needed_where_vars,
        &group_by_vec,
        query.output.is_distinct(),
        required_where_vars,
        planning,
    )?;

    // Apply post-query VALUES clause after the WHERE tree is fully built.
    // This is kept separate from `patterns` so the WHERE-clause planner cannot
    // reorder it relative to OPTIONAL/UNION (which would change semantics).
    if let Some(Pattern::Values { vars, rows }) = &query.post_values {
        operator = Box::new(crate::values::ValuesOperator::new(
            operator,
            vars.clone(),
            rows.clone(),
        ));
    }

    // Get the schema after WHERE (before grouping)
    let where_schema: Arc<[VarId]> = Arc::from(operator.schema().to_vec().into_boxed_slice());

    // GROUP BY + Aggregates
    // We use streaming GroupAggregateOperator when all aggregates are streamable
    // (COUNT, SUM, AVG, MIN, MAX). This is O(groups) memory instead of O(rows).
    let needs_grouping = !group_by_vec.is_empty() || !aggregates_vec.is_empty();
    if needs_grouping {
        // Validate group vars exist in where schema
        for var in &group_by_vec {
            if !where_schema.contains(var) {
                return Err(QueryError::VariableNotFound(format!(
                    "GROUP BY variable {var:?} not found in query schema"
                )));
            }
        }

        // Validate aggregates
        let current_schema = operator.schema();
        let group_by_set: HashSet<VarId> =
            group_by_vec.iter().copied().collect();
        let mut seen_output_vars: HashSet<VarId> =
            HashSet::new();

        for spec in &aggregates_vec {
            if let Some(input_var) = spec.input_var {
                if !current_schema.contains(&input_var) {
                    return Err(QueryError::VariableNotFound(format!(
                        "Aggregate input variable {input_var:?} not found in schema"
                    )));
                }
                if !group_by_vec.is_empty() && group_by_set.contains(&input_var) {
                    return Err(QueryError::InvalidQuery(format!(
                        "Aggregate input variable {input_var:?} is a GROUP BY key and will not be grouped"
                    )));
                }
                if spec.output_var != input_var && current_schema.contains(&spec.output_var) {
                    return Err(QueryError::InvalidQuery(format!(
                        "Aggregate output variable {:?} already exists in schema",
                        spec.output_var
                    )));
                }
            } else if current_schema.contains(&spec.output_var) {
                return Err(QueryError::InvalidQuery(format!(
                    "Aggregate output variable {:?} already exists in schema",
                    spec.output_var
                )));
            }
            if !seen_output_vars.insert(spec.output_var) {
                return Err(QueryError::InvalidQuery(format!(
                    "Duplicate aggregate output variable {:?}",
                    spec.output_var
                )));
            }
        }

        // Try streaming path: GroupAggregateOperator replaces both GroupBy + Aggregate
        // when all aggregates are streamable (COUNT, SUM, AVG, MIN, MAX).
        let streaming_specs: Vec<StreamingAggSpec> = aggregates_vec
            .iter()
            .map(|spec| {
                let input_col = spec
                    .input_var
                    .and_then(|v| current_schema.iter().position(|&sv| sv == v));
                StreamingAggSpec {
                    function: spec.function.clone(),
                    input_col,
                    output_var: spec.output_var,
                    distinct: spec.distinct,
                }
            })
            .collect();

        // The streaming GroupAggregateOperator only outputs GROUP BY keys + aggregate outputs.
        // If the SELECT projects any *grouped* variables (non-key, non-aggregate),
        // we must use the traditional GroupByOperator path so those vars become
        // `Binding::Grouped(Vec<Binding>)` and remain selectable.
        let select_needs_grouped_vars = query.output.projected_vars().is_some_and(|vars| {
            vars.iter().any(|v| {
                !group_by_vec.contains(v)
                    && !aggregates_vec.iter().any(|a| a.output_var == *v)
            })
        });

        let use_streaming = !aggregates_vec.is_empty()
            && GroupAggregateOperator::all_streamable(&streaming_specs)
            && !select_needs_grouped_vars;

        if use_streaming {
            // Streaming path: O(groups) memory
            let partitioned = detect_partitioned_group_by(query, options);
            tracing::debug!(
                group_by_count = group_by_vec.len(),
                agg_count = streaming_specs.len(),
                partitioned,
                "using streaming GroupAggregateOperator"
            );
            // GroupAggregateOperator replaces both GroupBy and Aggregate,
            // so use required_aggregate_vars (what the combined output must contain).
            operator = Box::new(
                GroupAggregateOperator::new(
                    operator,
                    group_by_vec.clone(),
                    streaming_specs,
                    None, // graph_view - will be set from context if needed
                    partitioned,
                )
                .with_out_schema(
                    variable_deps
                        .as_ref()
                        .map(|d| d.required_aggregate_vars.as_slice()),
                ),
            );
        } else {
            // Traditional path: GroupByOperator + AggregateOperator
            operator = Box::new(
                GroupByOperator::new(operator, group_by_vec.clone()).with_out_schema(
                    variable_deps
                        .as_ref()
                        .map(|d| d.required_groupby_vars.as_slice()),
                ),
            );
            if !aggregates_vec.is_empty() {
                operator = Box::new(
                    AggregateOperator::new(operator, aggregates_vec.clone()).with_out_schema(
                        variable_deps
                            .as_ref()
                            .map(|d| d.required_aggregate_vars.as_slice()),
                    ),
                );
            }
        }
    }

    // HAVING (filter on aggregated results)
    if let Some(expr) = having_expr {
        operator = Box::new(
            HavingOperator::new(operator, expr.clone()).with_out_schema(
                variable_deps
                    .as_ref()
                    .map(|d| d.required_having_vars.as_slice()),
            ),
        );
    }

    // Post-aggregation BINDs (e.g., SELECT (CEIL(?avg) AS ?ceil))
    if !post_binds_vec.is_empty() {
        for (i, (var, expr)) in post_binds_vec.iter().enumerate() {
            operator = Box::new(
                crate::bind::BindOperator::new(operator, *var, expr.clone(), vec![])
                    .with_out_schema(
                        variable_deps
                            .as_ref()
                            .and_then(|d| d.required_bind_vars.get(i))
                            .map(std::vec::Vec::as_slice),
                    ),
            );
        }
    }

    // Get the schema after grouping/aggregation/binds (for validation)
    let post_group_schema: Arc<[VarId]> = Arc::from(operator.schema().to_vec().into_boxed_slice());

    // ORDER BY, PROJECT, DISTINCT, OFFSET, LIMIT
    //
    // SPARQL algebra conversion order is: ORDER BY → PROJECT → DISTINCT → SLICE.
    //
    // Generic optimization for DISTINCT SELECT queries:
    // If ORDER BY references ONLY projected variables, we can safely perform
    // PROJECT + DISTINCT before ORDER BY. This can drastically reduce sort input
    // size (and allow top-k truncation) while preserving semantics:
    // duplicates eliminated by DISTINCT have identical sort keys, so removing
    // them before sorting does not change the ordered set of unique solutions.
    let select_vars_opt: Option<Vec<VarId>> = query.output.projected_vars();
    let can_project_distinct_before_sort = query.output.is_distinct()
        && !query.ordering.is_empty()
        && select_vars_opt.as_ref().is_some_and(|vars| {
            !vars.is_empty() && query.ordering.iter().all(|s| vars.contains(&s.var))
        });

    // Validate SELECT vars (when present) exist in the post-group schema.
    if let Some(vars) = &select_vars_opt {
        if !vars.is_empty() {
            for var in vars {
                if !post_group_schema.contains(var) {
                    return Err(QueryError::VariableNotFound(format!(
                        "Selected variable {var:?} not found in query schema"
                    )));
                }
            }
        }
    }

    // Validate ORDER BY vars exist in the post-group schema and are allowed under grouping.
    if !query.ordering.is_empty() {
        // Disallow sorting on Grouped variables (non-key, non-aggregated) because comparison is undefined.
        let mut allowed_sort_vars: Option<HashSet<VarId>> = None;
        if needs_grouping {
            let mut allowed = HashSet::new();
            for v in &group_by_vec {
                allowed.insert(*v);
            }
            for spec in &aggregates_vec {
                allowed.insert(spec.output_var);
            }
            allowed_sort_vars = Some(allowed);
        }
        for spec in &query.ordering {
            if !post_group_schema.contains(&spec.var) {
                return Err(QueryError::VariableNotFound(format!(
                    "Sort variable {:?} not found in query schema",
                    spec.var
                )));
            }
            if let Some(ref allowed) = allowed_sort_vars {
                if !allowed.contains(&spec.var) {
                    return Err(QueryError::InvalidQuery(format!(
                        "Cannot ORDER BY variable {:?} because it is grouped (non-key, non-aggregate)",
                        spec.var
                    )));
                }
            }
        }
    }

    if can_project_distinct_before_sort {
        // PROJECT
        if let Some(vars) = select_vars_opt {
            operator = Box::new(ProjectOperator::new(operator, vars.to_vec()));
        }
        // DISTINCT (pre-sort)
        operator = Box::new(DistinctOperator::new(operator));

        // ORDER BY (post-distinct, projected vars only)
        let k = match (options.limit, options.offset) {
            (Some(limit), Some(offset)) => limit.saturating_add(offset),
            (Some(limit), None) => limit,
            _ => 0,
        };
        let can_topk = options.limit.is_some();
        let mut sort_op = if can_topk {
            SortOperator::new_topk(operator, query.ordering.clone(), k)
        } else {
            SortOperator::new(operator, query.ordering.clone())
        };
        sort_op = sort_op.with_out_schema(
            variable_deps
                .as_ref()
                .map(|d| d.required_sort_vars.as_slice()),
        );
        operator = Box::new(sort_op);
    } else {
        // ORDER BY (before projection - may reference vars not in SELECT)
        if !query.ordering.is_empty() {
            // Safe top-k: ORDER BY + (OFFSET o) + LIMIT l can keep only (o + l) rows.
            //
            // This is safe when DISTINCT is not in play because slicing happens after sorting.
            let can_topk = options.limit.is_some() && !query.output.is_distinct();
            let k = match (options.limit, options.offset) {
                (Some(limit), Some(offset)) => limit.saturating_add(offset),
                (Some(limit), None) => limit,
                _ => 0,
            };
            let mut sort_op = if can_topk {
                SortOperator::new_topk(operator, query.ordering.clone(), k)
            } else {
                SortOperator::new(operator, query.ordering.clone())
            };
            sort_op = sort_op.with_out_schema(
                variable_deps
                    .as_ref()
                    .map(|d| d.required_sort_vars.as_slice()),
            );
            operator = Box::new(sort_op);
        }

        // PROJECT
        if let Some(vars) = select_vars_opt {
            if !vars.is_empty() {
                operator = Box::new(ProjectOperator::new(operator, vars.to_vec()));
            }
        }

        // DISTINCT (after projection)
        if query.output.is_distinct() {
            operator = Box::new(DistinctOperator::new(operator));
        }
    }

    // OFFSET
    if let Some(offset) = options.offset {
        if offset > 0 {
            operator = Box::new(OffsetOperator::new(operator, offset));
        }
    }

    // LIMIT
    if let Some(limit) = options.limit {
        operator = Box::new(LimitOperator::new(operator, limit));
    }

    Ok(operator)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::triple::{Ref, Term, TriplePattern};
    use crate::ir::Pattern;
    use crate::ir::QueryOptions;
    use crate::ir::{Query, QueryOutput};
    use crate::sort::SortSpec;
    use fluree_db_core::Sid;
    use fluree_graph_json_ld::ParsedContext;

    fn make_pattern(s_var: VarId, p_name: &str, o_var: VarId) -> TriplePattern {
        TriplePattern::new(
            Ref::Var(s_var),
            Ref::Sid(Sid::new(100, p_name)),
            Term::Var(o_var),
        )
    }

    fn make_simple_query(select: Vec<VarId>, patterns: Vec<Pattern>) -> Query {
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
            grouping: None,
            ordering: Vec::new(),
            post_values: None,
        }
    }

    #[test]
    fn test_detect_star_const_numeric_label_order_limit() {
        let s = VarId(0);
        let label = VarId(1);
        let v = VarId(2);

        let p_label = Ref::Sid(Sid::new(100, "label"));
        let p_num = Ref::Sid(Sid::new(100, "num"));
        let p_c1 = Ref::Sid(Sid::new(100, "c1"));
        let p_c2 = Ref::Sid(Sid::new(100, "c2"));

        let patterns = vec![
            Pattern::Triple(TriplePattern::new(
                Ref::Var(s),
                p_label.clone(),
                Term::Var(label),
            )),
            Pattern::Triple(TriplePattern::new(
                Ref::Var(s),
                p_c1.clone(),
                Term::Sid(Sid::new(100, "o1")),
            )),
            Pattern::Triple(TriplePattern::new(
                Ref::Var(s),
                p_c2.clone(),
                Term::Sid(Sid::new(100, "o2")),
            )),
            Pattern::Triple(TriplePattern::new(Ref::Var(s), p_num.clone(), Term::Var(v))),
            Pattern::Filter(crate::ir::Expression::gt(
                crate::ir::Expression::Var(v),
                crate::ir::Expression::Const(crate::ir::FlakeValue::Long(50)),
            )),
        ];

        let query = Query {
            context: ParsedContext::default(),
            orig_context: None,
            output: QueryOutput::select_distinct(vec![s, label]),
            patterns,
            options: QueryOptions::default(),
            grouping: None,
            ordering: vec![SortSpec::asc(label)],
            post_values: None,
        };

        let opts = QueryOptions::new().with_limit(10);

        let spec = detect_star_const_numeric_label_order_limit(&query, &opts)
            .expect("should detect shape");
        assert_eq!(spec.subject_var, s);
        assert_eq!(spec.label_var, label);
        assert_eq!(spec.limit, 10);
        assert_eq!(spec.const_constraints.len(), 2);
        assert_eq!(spec.numeric_pred, p_num);
        assert_eq!(spec.label_pred, p_label);
    }

    #[test]
    fn test_build_operator_tree_validates_select_vars() {
        let query = Query {
            context: ParsedContext::default(),
            orig_context: None,
            output: QueryOutput::select_all(vec![VarId(99)]), // Variable not in pattern
            patterns: vec![Pattern::Triple(make_pattern(VarId(0), "name", VarId(1)))],
            options: QueryOptions::default(),
            grouping: None,
            ordering: Vec::new(),
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
        let query = Query {
            context: ParsedContext::default(),
            orig_context: None,
            output: QueryOutput::select_all(vec![VarId(0)]),
            patterns: vec![Pattern::Triple(make_pattern(VarId(0), "name", VarId(1)))],
            options: QueryOptions::default(),
            grouping: None,
            ordering: vec![SortSpec::asc(VarId(99))], // Invalid var
            post_values: None,
        };

        let result = build_operator_tree(
            &query,
            &QueryOptions::default(),
            None,
            &crate::temporal_mode::PlanningContext::current(),
        );
        match result {
            Err(e) => assert!(e.to_string().contains("Sort variable")),
            Ok(_) => panic!("Expected error for invalid sort var"),
        }
    }

    #[test]
    fn test_build_operator_tree_empty_patterns() {
        let query = make_simple_query(vec![], vec![]);
        let result = build_operator_tree(
            &query,
            &QueryOptions::default(),
            None,
            &crate::temporal_mode::PlanningContext::current(),
        );
        assert!(result.is_ok());

        let op = result.unwrap();
        // Empty patterns should produce EmptyOperator with empty schema
        assert_eq!(op.schema().len(), 0);
    }

    #[test]
    fn test_detect_exists_join_count_distinct_object_accepts_either_pattern_order() {
        let s = VarId(0);
        let exists_o = VarId(1);
        let counted_o = VarId(2);
        let out = VarId(3);
        let exists_pred = Ref::Sid(Sid::new(100, "rdf:type"));
        let count_pred = Ref::Sid(Sid::new(100, "sourceLink"));

        let make_grouping = || {
            Some(Grouping::Implicit {
                aggregation: Aggregation {
                    aggregates: fluree_db_core::NonEmpty::try_from_vec(vec![
                        crate::ir::AggregateSpec {
                            function: crate::ir::AggregateFn::CountDistinct,
                            input_var: Some(counted_o),
                            output_var: out,
                            distinct: false,
                        },
                    ])
                    .unwrap(),
                    binds: Vec::new(),
                },
                having: None,
            })
        };
        let counted_first = Query {
            context: ParsedContext::default(),
            orig_context: None,
            output: QueryOutput::select_all(vec![out]),
            patterns: vec![
                Pattern::Triple(TriplePattern::new(
                    Ref::Var(s),
                    count_pred.clone(),
                    Term::Var(counted_o),
                )),
                Pattern::Triple(TriplePattern::new(
                    Ref::Var(s),
                    exists_pred.clone(),
                    Term::Var(exists_o),
                )),
            ],
            options: QueryOptions::default(),
            grouping: make_grouping(),
            ordering: Vec::new(),
            post_values: None,
        };
        let reversed = Query {
            context: ParsedContext::default(),
            orig_context: None,
            output: QueryOutput::select_all(vec![out]),
            patterns: vec![
                Pattern::Triple(TriplePattern::new(
                    Ref::Var(s),
                    exists_pred.clone(),
                    Term::Var(exists_o),
                )),
                Pattern::Triple(TriplePattern::new(
                    Ref::Var(s),
                    count_pred.clone(),
                    Term::Var(counted_o),
                )),
            ],
            options: QueryOptions::default(),
            grouping: make_grouping(),
            ordering: Vec::new(),
            post_values: None,
        };
        let options = QueryOptions::default();

        assert_eq!(
            detect_exists_join_count_distinct_object(&counted_first, &options),
            Some((count_pred.clone(), exists_pred.clone(), out))
        );
        assert_eq!(
            detect_exists_join_count_distinct_object(&reversed, &options),
            Some((count_pred, exists_pred, out))
        );
    }
}
